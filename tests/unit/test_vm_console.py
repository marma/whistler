"""KubeVirt subresource plumbing (whistler.portal.kubevirt) — pure parts plus
the console relay driven with stub websockets. The API-server dial itself is
integration territory."""
import asyncio

import pytest
from aiohttp import WSMsgType, web

from whistler.portal import kubevirt


# --- subresource_ws_url ---------------------------------------------------- #

def test_console_url_shape_and_wss_upgrade():
    url = kubevirt.subresource_ws_url(
        "https://10.0.0.1:6443", "whistler-user-alice", "alice-desk", "console")
    assert url == ("wss://10.0.0.1:6443/apis/subresources.kubevirt.io/v1"
                   "/namespaces/whistler-user-alice"
                   "/virtualmachineinstances/alice-desk/console")


def test_vnc_url_and_plain_http():
    url = kubevirt.subresource_ws_url("http://localhost:8001", "ns", "vm", "vnc")
    assert url.startswith("ws://localhost:8001/")
    assert url.endswith("/virtualmachineinstances/vm/vnc")


def test_subprotocol_constant():
    assert kubevirt.SUBPROTOCOL == "plain.kubevirt.io"


# --- relay_console ---------------------------------------------------------- #

class _Msg:
    def __init__(self, type_, data):
        self.type = type_
        self.data = data


class _StubWS:
    """Minimal stand-in for both the browser WebSocketResponse and the upstream
    ClientWebSocketResponse: iterates over scripted incoming messages and
    records what was sent to it."""

    def __init__(self, incoming=()):
        self._incoming = list(incoming)
        self.sent = []
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._incoming:
            # Block forever (until cancelled) once the script is exhausted,
            # like a websocket with no traffic.
            await asyncio.Event().wait()
        return self._incoming.pop(0)

    async def send_bytes(self, data):
        self.sent.append(data)

    async def close(self, **kwargs):
        self.closed = True


async def test_text_input_reaches_console_as_bytes():
    browser = _StubWS([_Msg(web.WSMsgType.TEXT, "ls -la\n"),
                       _Msg(web.WSMsgType.CLOSE, None)])
    console = _StubWS()
    await kubevirt.relay_console(browser, console)
    assert console.sent == [b"ls -la\n"]
    assert console.closed and browser.closed


async def test_resize_control_frames_are_dropped():
    # A serial console has no resize ioctl; forwarding the JSON frame would
    # type it into the guest as keystrokes.
    browser = _StubWS([_Msg(web.WSMsgType.TEXT, '{"resize": [120, 40]}'),
                       _Msg(web.WSMsgType.TEXT, "echo hi\n"),
                       _Msg(web.WSMsgType.CLOSE, None)])
    console = _StubWS()
    await kubevirt.relay_console(browser, console)
    assert console.sent == [b"echo hi\n"]


async def test_console_output_reaches_browser_as_bytes():
    browser = _StubWS()
    console = _StubWS([_Msg(WSMsgType.BINARY, b"login: "),
                       _Msg(WSMsgType.CLOSE, None)])
    await kubevirt.relay_console(browser, console)
    assert browser.sent == [b"login: "]
    assert browser.closed and console.closed
