"""Web-terminal heartbeat and idle timeout (terminal.ActivityTrackingSocket,
terminal.run_with_idle_timeout), driven over a real aiohttp WebSocket with
sub-second timeouts. The relay here is a stand-in for kubectl exec / SSH: it
reads the socket like the real ones do and can be told to print, which
must not count as activity: idle means nobody typed."""
import asyncio

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer

from whistler.portal import terminal


def _app(idle, *, heartbeat=None, chatty=None):
    """A /ws endpoint wired the way app._serve_terminal wires /ws-term.
    ``chatty``: seconds between server-side output frames (a busy shell)."""
    result = {}

    async def relay(sock):
        async def printer():
            while chatty:
                await asyncio.sleep(chatty)
                await sock.send_bytes(b".")
        p = asyncio.ensure_future(printer())
        try:
            async for msg in sock:
                if msg.type not in (web.WSMsgType.TEXT, web.WSMsgType.BINARY):
                    break
        finally:
            p.cancel()
            result["relay_ended"] = True

    async def handler(request):
        wsr = web.WebSocketResponse(heartbeat=heartbeat)
        await wsr.prepare(request)
        sock = terminal.ActivityTrackingSocket(wsr)
        result["idle_closed"] = await terminal.run_with_idle_timeout(
            sock, relay(sock), idle)
        return wsr

    app = web.Application()
    app.router.add_get("/ws", handler)
    return app, result


async def _session(app, client_fn):
    server = TestServer(app)
    await server.start_server()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(server.make_url("/ws")) as ws:
                return await client_fn(ws)
    finally:
        await server.close()


async def _wait_close(ws, budget):
    """Read until the server closes or ``budget`` passes; the close message,
    or None if it was still open. The budget is overall, not per message, or
    a chatty server would never let it end."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + budget
    try:
        while True:
            msg = await asyncio.wait_for(ws.receive(), deadline - loop.time())
            if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                return msg.extra or ""
    except asyncio.TimeoutError:
        return None


def test_idle_terminal_is_closed_with_a_reason():
    app, result = _app(0.3)
    reason = asyncio.run(_session(app, lambda ws: _wait_close(ws, 2)))
    assert reason == "idle for 0.3 seconds"
    assert result == {"idle_closed": True, "relay_ended": True}


def test_heartbeat_does_not_count_as_activity():
    # Pings every 0.05s keep the socket open through proxies; they must not
    # also keep it open past the idle limit.
    app, result = _app(0.4, heartbeat=0.05)
    reason = asyncio.run(_session(app, lambda ws: _wait_close(ws, 2)))
    assert reason is not None and reason.startswith("idle for")
    assert result["idle_closed"] is True


def test_typing_keeps_it_open():
    app, result = _app(0.3)

    async def client(ws):
        for _ in range(8):  # 0.8s of typing, well past the 0.3s limit
            await ws.send_str("x")
            await asyncio.sleep(0.1)
        return await _wait_close(ws, 0.05)

    assert asyncio.run(_session(app, client)) is None


def test_output_does_not_keep_it_open():
    # Idle means nobody typed: a shell printing every 0.1s to an unattended
    # terminal is still closed.
    app, result = _app(0.3, chatty=0.1)
    reason = asyncio.run(_session(app, lambda ws: _wait_close(ws, 2)))
    assert reason == "idle for 0.3 seconds"
    assert result["idle_closed"] is True


def test_resizing_does_not_keep_it_open():
    app, result = _app(0.3)

    async def client(ws):
        async def resize():  # resize frames every 0.1s, no keystrokes
            while True:
                await ws.send_str('{"resize": [120, 40]}')
                await asyncio.sleep(0.1)

        sender = asyncio.ensure_future(resize())
        try:
            return await _wait_close(ws, 2)
        finally:
            sender.cancel()

    assert asyncio.run(_session(app, client)) == "idle for 0.3 seconds"


def test_binary_input_counts_as_typing():
    app, result = _app(0.3)

    async def client(ws):
        for _ in range(8):
            await ws.send_bytes(b"x")
            await asyncio.sleep(0.1)
        return await _wait_close(ws, 0.05)

    assert asyncio.run(_session(app, client)) is None


def test_disabled_never_closes():
    app, result = _app(0)
    assert asyncio.run(_session(app, lambda ws: _wait_close(ws, 0.5))) is None


def test_whole_minutes_read_as_minutes():
    # Only the wording, on a fake clock; nothing waits half an hour.
    clock = [0.0]

    class FakeWs:
        closed = False
        message = None

        async def close(self, message=b""):
            self.message = message
            self.closed = True

    ws = FakeWs()
    sock = terminal.ActivityTrackingSocket(ws, clock=lambda: clock[0])
    clock[0] = 1800  # already past the limit when the watchdog first looks

    async def relay():
        while not ws.closed:
            await asyncio.sleep(0.01)

    assert asyncio.run(terminal.run_with_idle_timeout(sock, relay(), 1800)) is True
    assert ws.message == b"idle for 30 minutes"


def test_idle_timeout_env(monkeypatch):
    monkeypatch.delenv("WHISTLER_TERMINAL_IDLE_TIMEOUT", raising=False)
    assert terminal.idle_timeout_seconds() == 1800
    monkeypatch.setenv("WHISTLER_TERMINAL_IDLE_TIMEOUT", "0")
    assert terminal.idle_timeout_seconds() == 0
    # Malformed falls back to the default, never to "disabled".
    monkeypatch.setenv("WHISTLER_TERMINAL_IDLE_TIMEOUT", "half an hour")
    assert terminal.idle_timeout_seconds() == 1800
