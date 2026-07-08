"""Web terminal: browser (xterm.js) <-> WebSocket <-> ``kubectl exec`` PTY.

The desktop viewer reverse-proxies the browser to the in-pod Selkies server; a
terminal needs none of that.
We open a local PTY, spawn ``kubectl exec -it <pod> -n <ns> -- <shell>`` with its
stdio on the slave end, and pump bytes between the PTY master and the WebSocket.
Resize is carried out-of-band: the browser sends a small JSON control frame and
we ``TIOCSWINSZ`` the master (kubectl forwards SIGWINCH to the remote TTY).

This is the same mechanism the SSH server uses in ``server._run_pod_shell``; the
pure command-assembly and control-frame parsing live here so they're unit-
testable without a cluster. Works for any container/kata-backed Session (ssh or
desktop mode) — VM-runtime sessions have no pod to exec into and are gated out by
the caller.
"""
import asyncio
import fcntl
import json
import logging
import os
import pty
import struct
import termios

from aiohttp import web

logger = logging.getLogger("whistler.portal")

# Browser viewport is tiny at connect; xterm's fit addon sends a resize control
# frame immediately after open, so this default is only briefly visible.
_DEFAULT_COLS, _DEFAULT_ROWS = 80, 24
_MAX_DIM = 1000  # clamp absurd values before they reach the kernel ioctl


def build_exec_command(pod_name: str, namespace: str, shell: str = "/bin/bash") -> list[str]:
    """The ``kubectl exec`` argv for an interactive PTY shell into a pod.

    ``-c main`` pins the workload container: every Session pod names it "main"
    (see ``_build_pod_spec``), and without the flag kubectl both prints a
    "Defaulted container ..." banner into the terminal and would pick the
    streamer sidecar if the container order ever changed.

    Desktop workload containers start as root (their entrypoint needs it for
    the system bus) and ``su`` down to the image's ``DESKTOP_USER`` for the
    actual session — so a bare exec lands a root shell. Mirror the entrypoint:
    if we're root and ``DESKTOP_USER`` names a real account, drop into their
    login shell, re-exporting DISPLAY/PULSE_SERVER (``su -`` scrubs them) so
    GUI apps launched from the terminal appear on the streamed desktop.
    Otherwise (ssh pods, images without the convention) keep the plain shell.

    ``-it`` allocates a remote TTY (so line editing, job control, and SIGWINCH
    work); ``--`` ends kubectl's own flag parsing. Falls back to ``sh`` if the
    requested shell is missing, mirroring how interactive shells degrade."""
    inner = (f"env DISPLAY='$DISPLAY' PULSE_SERVER='$PULSE_SERVER' {shell} -l "
             f"|| env DISPLAY='$DISPLAY' PULSE_SERVER='$PULSE_SERVER' sh -l")
    script = (
        'if [ "$(id -u)" = "0" ] && [ -n "$DESKTOP_USER" ] '
        '&& id "$DESKTOP_USER" >/dev/null 2>&1; then '
        # su -c leaves the shell a non-session-leader on the exec'd TTY ("no
        # job control", Ctrl-C/Ctrl-Z dead). util-linux su --pty gives the
        # child its own pty session (and forwards resize), fixing that; probe
        # for it since exec'ing an unsupported flag would kill the session.
        "if su --help 2>&1 | grep -q -- --pty; then "
        f'exec su -P - "$DESKTOP_USER" -c "{inner}"; fi; '
        f'exec su - "$DESKTOP_USER" -c "{inner}"; '
        f"fi; exec {shell} || exec /bin/sh"
    )
    return [
        "kubectl", "exec", "-it", pod_name, "-n", namespace, "-c", "main", "--",
        "sh", "-c", script,
    ]


def parse_resize(text: str) -> tuple[int, int] | None:
    """Decode a client control frame into ``(cols, rows)``, or ``None`` if it is
    not a resize message. Frames are JSON ``{"resize": [cols, rows]}``; anything
    else (ordinary keystrokes) returns ``None`` so the caller forwards it as
    terminal input. Values are clamped to a sane range."""
    if not text or text[0] != "{":
        return None
    try:
        msg = json.loads(text)
    except (ValueError, TypeError):
        return None
    dims = msg.get("resize") if isinstance(msg, dict) else None
    if not (isinstance(dims, (list, tuple)) and len(dims) == 2):
        return None
    try:
        cols, rows = int(dims[0]), int(dims[1])
    except (ValueError, TypeError):
        return None
    cols = max(1, min(_MAX_DIM, cols))
    rows = max(1, min(_MAX_DIM, rows))
    return cols, rows


def set_winsize(fd: int, cols: int, rows: int) -> None:
    """Apply a terminal window size to a PTY master fd via ``TIOCSWINSZ``."""
    winsize = struct.pack("HHHH", rows, cols, 0, 0)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, winsize)


async def relay_terminal(wsr: web.WebSocketResponse, pod_name: str, namespace: str,
                         *, shell: str = "/bin/bash",
                         cols: int = _DEFAULT_COLS, rows: int = _DEFAULT_ROWS) -> None:
    """Bridge a prepared WebSocket to a ``kubectl exec`` PTY until either side
    closes. Text frames are terminal input unless they parse as a resize control
    frame; PTY output is sent back as binary frames."""
    master, slave = pty.openpty()
    try:
        set_winsize(master, cols, rows)
    except OSError as e:
        logger.warning(f"terminal: initial winsize failed: {e}")
    os.set_blocking(master, False)

    cmd = build_exec_command(pod_name, namespace, shell)
    logger.info(f"terminal: exec {pod_name} in {namespace} ({cols}x{rows})")
    process = await asyncio.create_subprocess_exec(
        *cmd, stdin=slave, stdout=slave, stderr=slave, preexec_fn=os.setsid,
    )
    os.close(slave)  # parent keeps only the master end

    loop = asyncio.get_running_loop()
    closed = loop.create_future()

    def on_pty_readable():
        try:
            data = os.read(master, 65536)
        except BlockingIOError:
            return
        except OSError:
            data = b""
        if not data:
            if not closed.done():
                closed.set_result(True)
            return
        # send_bytes is a coroutine; schedule it so the reader callback stays sync.
        if not wsr.closed:
            asyncio.ensure_future(wsr.send_bytes(data))

    loop.add_reader(master, on_pty_readable)

    async def ws_to_pty():
        try:
            async for msg in wsr:
                if msg.type == web.WSMsgType.TEXT:
                    resize = parse_resize(msg.data)
                    if resize:
                        try:
                            set_winsize(master, *resize)
                        except OSError:
                            pass
                    else:
                        os.write(master, msg.data.encode("utf-8"))
                elif msg.type == web.WSMsgType.BINARY:
                    os.write(master, msg.data)
                else:
                    break
        except Exception as e:
            logger.debug(f"terminal: ws->pty ended: {e}")
        finally:
            if not closed.done():
                closed.set_result(True)

    pump = asyncio.ensure_future(ws_to_pty())
    try:
        await asyncio.wait(
            [asyncio.ensure_future(process.wait()), closed, pump],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        loop.remove_reader(master)
        pump.cancel()
        os.close(master)
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
        if not wsr.closed:
            await wsr.close()
