"""Periodic desktop screenshots — one current thumbnail per running session.

**The H.264 stream is deliberately not the source.** Selkies only encodes while
a browser is attached (capture starts on ``START_VIDEO`` and stops when the last
client disconnects), so tapping the relay in ``whistler.portal.proxy`` would
only ever see sessions somebody is already watching — the least interesting
ones. The X display, by contrast, exists for the whole life of the session.
Grabbing from there covers every running desktop, watched or not, and costs the
session nothing when nobody is looking.

Grab side, both runtimes, one command: ``xwd -root`` piped through ``gzip``.
``xwd`` (from **x11-apps**) is the one X grabber that needs no image encoder in
the session — it dumps the raw framebuffer, and the portal does the downscale
and PNG encode with nothing but the stdlib (``xwd_to_rgb`` / ``encode_png``
below). That keeps the streamer sidecar and the VM guests at one extra apt
package instead of imagemagick/ffmpeg, and puts the per-shot CPU on the idle
portal rather than in the user's session.

  * **pods** — ``kubectl exec -c streamer``: the sidecar owns Xvfb (the
    workload container shares the socket but need not carry the tool).
  * **VMs** — SSH as the session user with the portal's per-user VM access key,
    the same credential the web terminal uses (``kubevirt.relay_ssh``). Xvfb
    runs with ``-ac``, so ``$DISPLAY`` is reachable without an xauth cookie.

Storage is a process-wide dict, newest-only, never written to disk: one PNG per
(user, session), replaced on each pass and dropped when the session goes away.
A portal restart loses them all and the next pass refills — which is the whole
lifetime worth having, since a screenshot that outlives its session is worse
than no screenshot at all. **This assumes a single portal replica**: the store
is per-process, so N replicas each run their own capture loop (N grabs into
every session) and answer /screenshot from their own store. See
``portal.replicaCount`` in values.yaml; the loop warns at startup if it is >1.

## This is monitoring — and it is not the only monitoring here

Say it plainly: a periodic capture of a user's screen is surveillance, and
``max_width`` is the only thing deciding which kind. At the 320px default a
1920px desktop is downscaled 6x — window shapes, colours and layout survive,
body text does not. That is an *activity overview*: you can see a session is
being used, not what is being done in it. Raise it toward the native width and
the same mechanism becomes readable monitoring. The setting is the policy, so
it is documented as such rather than buried as a rendering detail. **The stored
width is the boundary, not the CSS the dashboard displays it at** — anything
this module holds is retrievable at full stored resolution from
``/screenshot/<id>``.

Nor is this the first such capability in the stack, only the first one whistler
actually uses. Selkies is multi-client by design: one capture pipeline
broadcast to every attached websocket (``_broadcast_to_clients``), with a
``controller``/``viewer`` role split where ``viewer`` is read-only spectating
of the live stream. The in-session server runs with no master token and
``--enable-basic-auth=false``, so that role comes straight off the query string
and anything able to reach ``:8082`` can attach. The only access control is the
portal's proxy, which resolves sessions in the requesting user's namespace
(``_resolve_desktop_base``). So the substrate can already put a second pair of
eyes on a live desktop; what is missing is an admin surface for it, not a
mechanism. Worth knowing before anyone concludes that turning screenshots off
makes sessions unobservable.

Only *desktop* sessions have an X display; ssh-mode instances are skipped.

Since groups landed, the loop also skips a session whose owner is not granted
the ``screenshots`` channel in that session's zone — checked before the grab,
so those pixels never reach this process at all.
"""
import asyncio
import contextlib
import logging
import os
import re
import struct
import time
import zlib

from whistler.config import CHANNEL_SCREENSHOTS

logger = logging.getLogger("whistler.portal")

DEFAULT_INTERVAL_SECONDS = 300
# 320px is an activity overview, not a readable screen (see the module
# docstring): it is the privacy posture, deliberately the default, and the
# knob that turns this into real monitoring when raised.
DEFAULT_MAX_WIDTH = 320
DEFAULT_DISPLAY = ":0"
# The streamer sidecar owns Xvfb in a desktop pod (see _build_pod_spec); the
# workload container is "main" and is deliberately left without the grab tool.
STREAMER_CONTAINER = "streamer"

_GRAB_TIMEOUT_SECONDS = 30
_MAX_CONCURRENT_GRABS = 4
# Decompressed ceiling for one dump: 8K RGBA is ~140 MB, a 4K desktop ~35 MB.
# Bounded so a hostile guest can't answer a grab with a zip bomb.
_MAX_XWD_BYTES = 192 * 1024 * 1024
# $DISPLAY goes into a shell command; keep it to the one shape X allows.
_DISPLAY_RE = re.compile(r"^:\d+(\.\d+)?$")

# XWD file format (X11R7 xwd(1)): a 25-field u32 header, the window name, an
# XWDColor table (12 bytes/entry, present even for TrueColor visuals), then the
# raw pixel rows.
_XWD_HEADER_BYTES = 100
_XWD_HEADER_FIELDS = 25
_XWD_FILE_VERSION = 7
_XWD_ZPIXMAP = 2
_XWD_COLOR_ENTRY_BYTES = 12


class ScreenshotError(Exception):
    """One session's grab or decode failed. The capture loop logs it and moves
    on to the next session — a screenshot is never worth failing a pass over."""


# --------------------------------------------------------------------------- #
# Pure decode/encode. No cluster, no I/O — unit-tested in                       #
# tests/unit/test_screenshots.py.                                              #
# --------------------------------------------------------------------------- #

def gunzip(blob: bytes, limit: int = _MAX_XWD_BYTES) -> bytes:
    """Inflate the gzip stream the grab command produces, refusing anything
    that expands past ``limit``."""
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        out = decompressor.decompress(blob, limit)
    except zlib.error as e:
        raise ScreenshotError(f"grab output is not gzip: {e}") from e
    if decompressor.unconsumed_tail:
        raise ScreenshotError(f"xwd dump expands past {limit} bytes")
    return out


def _xwd_header(blob: bytes) -> tuple[int, ...]:
    """The 25 header fields. ``xwd`` normalises them to MSB-first, but detect
    rather than assume: ``file_version`` is the self-describing field."""
    if len(blob) < _XWD_HEADER_BYTES:
        raise ScreenshotError(f"xwd dump truncated ({len(blob)} bytes)")
    for order in (">", "<"):
        fields = struct.unpack(f"{order}{_XWD_HEADER_FIELDS}I", blob[:_XWD_HEADER_BYTES])
        if fields[1] == _XWD_FILE_VERSION:
            return fields
    raise ScreenshotError("not an xwd dump (bad file_version)")


def _mask_channel(mask: int) -> tuple[int, int]:
    """``(shift, width_in_bits)`` for one of a TrueColor visual's colour masks."""
    if not mask:
        raise ScreenshotError("xwd dump has no TrueColor masks")
    shift = (mask & -mask).bit_length() - 1
    return shift, bin(mask >> shift).count("1")


def _scale8(value: int, bits: int) -> int:
    """Widen/narrow a channel of ``bits`` to 8 bits. Identity for the usual
    depth-24 case; the arithmetic only matters for 16bpp (5/6/5) visuals."""
    if bits == 8:
        return value
    if bits > 8:
        return value >> (bits - 8)
    return value * 255 // ((1 << bits) - 1)


def xwd_to_rgb(blob: bytes, max_width: int = DEFAULT_MAX_WIDTH) -> tuple[int, int, bytes]:
    """Decode an ``xwd`` dump to ``(width, height, packed RGB)``, subsampling to
    at most ``max_width`` on the way.

    Downscaling happens *during* decode, not after: at an integer pixel step we
    only ever touch the pixels that survive, so a 4K desktop costs a 640-wide
    thumbnail's worth of work rather than 8M per-pixel iterations. Nearest
    neighbour is visibly coarse on text at large ratios, which is the right
    trade for a thumbnail nobody reads.
    """
    fields = _xwd_header(blob)
    header_size, pixmap_format = fields[0], fields[2]
    width, height = fields[4], fields[5]
    byte_order = fields[7]
    bits_per_pixel, bytes_per_line = fields[11], fields[12]
    red_mask, green_mask, blue_mask = fields[14], fields[15], fields[16]
    ncolors = fields[19]

    if pixmap_format != _XWD_ZPIXMAP:
        raise ScreenshotError(
            f"unsupported xwd pixmap_format {pixmap_format} (need ZPixmap)")
    if bits_per_pixel not in (16, 24, 32):
        raise ScreenshotError(f"unsupported xwd bits_per_pixel {bits_per_pixel}")
    if not width or not height:
        raise ScreenshotError("xwd dump has zero extent")

    offset = header_size + ncolors * _XWD_COLOR_ENTRY_BYTES
    needed = offset + bytes_per_line * height
    if len(blob) < needed:
        raise ScreenshotError(f"xwd dump truncated: {len(blob)} < {needed} bytes")

    step = max(1, -(-width // max_width)) if max_width > 0 else 1
    out_w, out_h = -(-width // step), -(-height // step)

    endian = "little" if byte_order == 0 else "big"
    stride = bits_per_pixel // 8
    r_shift, r_bits = _mask_channel(red_mask)
    g_shift, g_bits = _mask_channel(green_mask)
    b_shift, b_bits = _mask_channel(blue_mask)

    rgb = bytearray(out_w * out_h * 3)
    i = 0
    for y in range(0, height, step):
        base = offset + y * bytes_per_line
        for x in range(0, width, step):
            start = base + x * stride
            pixel = int.from_bytes(blob[start:start + stride], endian)
            rgb[i] = _scale8((pixel & red_mask) >> r_shift, r_bits)
            rgb[i + 1] = _scale8((pixel & green_mask) >> g_shift, g_bits)
            rgb[i + 2] = _scale8((pixel & blue_mask) >> b_shift, b_bits)
            i += 3
    return out_w, out_h, bytes(rgb)


def _png_chunk(tag: bytes, data: bytes) -> bytes:
    return (struct.pack(">I", len(data)) + tag + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))


def encode_png(width: int, height: int, rgb: bytes) -> bytes:
    """Pack RGB into a PNG (8-bit truecolour, filter 0 on every row).

    Deliberately dependency-free: a thumbnail is not worth adding Pillow to an
    image that also carries the SSH server and the operator. Filter 0 costs
    some compression versus an adaptive filter, and zlib does the rest."""
    stride = width * 3
    if len(rgb) != stride * height:
        raise ScreenshotError(
            f"rgb buffer is {len(rgb)} bytes, expected {stride * height}")
    raw = bytearray()
    for y in range(height):
        raw.append(0)
        raw += rgb[y * stride:(y + 1) * stride]
    return (b"\x89PNG\r\n\x1a\n"
            + _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + _png_chunk(b"IDAT", zlib.compress(bytes(raw), 6))
            + _png_chunk(b"IEND", b""))


def render_screenshot(gzipped_xwd: bytes, max_width: int = DEFAULT_MAX_WIDTH) -> bytes:
    """gzipped xwd dump -> downscaled PNG. CPU-bound; callers run it in an
    executor so a 4K decode doesn't stall the portal's event loop."""
    width, height, rgb = xwd_to_rgb(gunzip(gzipped_xwd), max_width)
    return encode_png(width, height, rgb)


# --------------------------------------------------------------------------- #
# Grab commands + transports                                                   #
# --------------------------------------------------------------------------- #

def grab_script(display: str = DEFAULT_DISPLAY) -> str:
    """The shell one-liner run inside the session. ``-silent`` keeps xwd from
    beeping the display; ``gzip -1`` is worth it because the raw dump is
    width*height*4 bytes (~8 MB at 1080p) and a desktop framebuffer is mostly
    flat colour — cheap CPU for a large cut in bytes over the exec/SSH channel."""
    if not _DISPLAY_RE.match(display):
        raise ScreenshotError(f"refusing suspicious DISPLAY {display!r}")
    return f"DISPLAY={display} xwd -root -silent | gzip -1"


def build_pod_grab_command(pod_name: str, namespace: str,
                           display: str = DEFAULT_DISPLAY,
                           container: str = STREAMER_CONTAINER) -> list[str]:
    """``kubectl exec`` argv for a one-shot grab. No ``-it``: this is a binary
    pipe, and a TTY would mangle it."""
    return ["kubectl", "exec", pod_name, "-n", namespace, "-c", container, "--",
            "sh", "-c", grab_script(display)]


async def capture_pod(pod_name: str, namespace: str,
                      display: str = DEFAULT_DISPLAY) -> bytes:
    cmd = build_pod_grab_command(pod_name, namespace, display)
    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(process.communicate(), _GRAB_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise ScreenshotError(f"grab in {pod_name} timed out")
    if process.returncode != 0:
        raise ScreenshotError(
            f"kubectl exec {pod_name} rc={process.returncode}: "
            f"{err.decode('utf-8', 'replace').strip()[:200]}")
    return out


async def capture_vm(host: str, username: str, private_key_pem: str,
                     display: str = DEFAULT_DISPLAY) -> bytes:
    """Grab over SSH into the guest. Unlike the web terminal there is no retry
    budget: a VM whose sshd isn't up yet simply has no screenshot this pass."""
    import asyncssh  # deferred: only VM sessions need an SSH client

    key = asyncssh.import_private_key(private_key_pem)
    try:
        async with asyncssh.connect(
            host, username=username, client_keys=[key],
            # config=None for the same reason as the relay: the portal's own
            # connections must not be steerable by a ~/.ssh/config in its home.
            known_hosts=None, config=None, connect_timeout=10,
        ) as conn:
            result = await asyncio.wait_for(
                conn.run(grab_script(display), encoding=None, check=False),
                _GRAB_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        raise ScreenshotError(f"grab on {host} timed out")
    except (OSError, asyncssh.Error) as e:
        raise ScreenshotError(f"ssh to {host} failed: {e}") from e
    if result.exit_status != 0:
        stderr = (result.stderr or b"").decode("utf-8", "replace").strip()[:200]
        raise ScreenshotError(f"grab on {host} exited {result.exit_status}: {stderr}")
    return result.stdout


# --------------------------------------------------------------------------- #
# Store + capture loop                                                         #
# --------------------------------------------------------------------------- #

class ScreenshotStore:
    """Newest-only PNG per (user, session), in memory.

    There is no history and no disk: keeping older shots would turn a liveness
    aid into a recording of what a user did, with retention and storage
    questions attached. One frame, overwritten, gone when the session is."""

    def __init__(self):
        self._shots: dict[tuple[str, str], tuple[float, bytes]] = {}

    def put(self, user: str, name: str, png: bytes) -> None:
        self._shots[(user, name)] = (time.time(), png)

    def get(self, user: str, name: str) -> tuple[float, bytes] | None:
        """``(captured_at, png)`` or None if this session has none yet."""
        return self._shots.get((user, name))

    def keep_only(self, keys) -> int:
        """Drop shots whose session no longer exists. Returns the number
        dropped."""
        stale = set(self._shots) - set(keys)
        for key in stale:
            del self._shots[key]
        return len(stale)

    def __len__(self) -> int:
        return len(self._shots)


# Process-wide: the viewer app (aiohttp) runs the capture loop and serves
# /screenshot/<id>, the management app (FastAPI) renders the thumbnails, and
# whistler.portal.__main__ runs both in one event loop.
STORE = ScreenshotStore()


def settings() -> tuple[int, int, str]:
    """``(interval_seconds, max_width, display)`` from the environment.
    A non-positive interval disables capture entirely."""
    try:
        interval = int(os.environ.get("WHISTLER_SCREENSHOT_INTERVAL",
                                      DEFAULT_INTERVAL_SECONDS))
    except ValueError:
        interval = DEFAULT_INTERVAL_SECONDS
    try:
        max_width = int(os.environ.get("WHISTLER_SCREENSHOT_WIDTH", DEFAULT_MAX_WIDTH))
    except ValueError:
        max_width = DEFAULT_MAX_WIDTH
    display = os.environ.get("WHISTLER_SCREENSHOT_DISPLAY", DEFAULT_DISPLAY)
    return interval, max(1, max_width), display


async def _capture_one(cm, store: ScreenshotStore, session: dict,
                       display: str, max_width: int,
                       semaphore: asyncio.Semaphore) -> bool:
    user, name = session["user"], session["name"]
    loop = asyncio.get_running_loop()

    # The channel grant is checked at *capture*, not at serve time, and that
    # is the point: a session whose owner isn't granted the screenshots
    # channel never has its display read at all, so the image never enters
    # portal memory. Gating only the HTTP route would still have carried the
    # pixels out of the zone (design/security.md, "Access channels").
    channels = await loop.run_in_executor(None, cm.session_channels, user, name)
    if CHANNEL_SCREENSHOTS not in channels:
        logger.debug(f"screenshot {user}/{name}: screenshots channel not granted")
        return False

    async with semaphore:
        try:
            if session.get("runtime") == "vm":
                address, private_key = await asyncio.gather(
                    loop.run_in_executor(None, cm.get_vmi_address, user, name),
                    loop.run_in_executor(None, cm.get_vm_access_private_key, user),
                )
                if not address:
                    raise ScreenshotError("VM has no address yet")
                if not private_key:
                    raise ScreenshotError("no VM access key for this user")
                blob = await capture_vm(address, user, private_key, display)
            else:
                if not session.get("podName"):
                    raise ScreenshotError("session has no pod")
                blob = await capture_pod(session["podName"], session["namespace"], display)
            png = await loop.run_in_executor(None, render_screenshot, blob, max_width)
        except ScreenshotError as e:
            # Expected and frequent (booting guest, session going away
            # mid-pass); debug so a busy cluster doesn't fill the log.
            logger.debug(f"screenshot {user}/{name}: {e}")
            return False
        except Exception as e:
            logger.warning(f"screenshot {user}/{name} failed: {e}")
            return False
    store.put(user, name, png)
    return True


async def capture_all(cm, store: ScreenshotStore, *, display: str, max_width: int) -> int:
    """One pass over every desktop session in the cluster. Returns the number
    of screenshots taken."""
    loop = asyncio.get_running_loop()
    sessions = await loop.run_in_executor(None, cm.list_all_desktop_sessions)
    # Prune against *every* session, not just the Ready ones: a session that is
    # briefly restarting should keep its last screenshot rather than flicker.
    store.keep_only((s["user"], s["name"]) for s in sessions)

    ready = [s for s in sessions if s.get("phase") == "Ready"]
    if not ready:
        return 0
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_GRABS)
    results = await asyncio.gather(*(
        _capture_one(cm, store, s, display, max_width, semaphore) for s in ready
    ))
    return sum(results)


async def run_forever(cm, store: ScreenshotStore, *, interval: int,
                      display: str, max_width: int) -> None:
    while True:
        started = time.monotonic()
        try:
            taken = await capture_all(cm, store, display=display, max_width=max_width)
            logger.info(f"Screenshot pass: {taken} captured, {len(store)} held "
                        f"({time.monotonic() - started:.1f}s)")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Screenshot pass failed: {e}")
        await asyncio.sleep(interval)


def screenshot_ctx(app):
    """aiohttp cleanup_ctx: run the capture loop for the life of the app."""
    interval, max_width, display = settings()
    if interval <= 0:
        logger.info("Session screenshots disabled (WHISTLER_SCREENSHOT_INTERVAL<=0)")

        async def _noop(_app):
            yield

        return _noop(app)

    async def _ctx(app):
        logger.info(f"Session screenshots: every {interval}s, "
                    f"max width {max_width}px, DISPLAY={display}")
        # The store is per-process. With more than one replica each one grabs
        # every session on its own schedule and answers /screenshot only for
        # what it captured, so thumbnails appear and vanish as requests are
        # balanced around — and every session gets grabbed N times.
        try:
            replicas = int(os.environ.get("PORTAL_REPLICAS", "1"))
        except ValueError:
            replicas = 1
        if replicas > 1:
            logger.warning(
                f"Session screenshots are in-memory per portal process, but "
                f"portal.replicaCount is {replicas}: each replica will grab "
                f"every session and serve only its own captures. Run one "
                f"portal replica, or disable screenshots.")
        task = asyncio.create_task(
            run_forever(app["cm"], STORE, interval=interval,
                        display=display, max_width=max_width))
        yield
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    return _ctx(app)
