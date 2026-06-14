"""guacd handshake + connection-param helpers (whistler.portal.guacd)."""
import asyncio

from whistler.portal.guacd import (
    _build_guacd_params,
    build_connect_args,
    handshake,
    resolve_session,
)
from whistler.portal.protocol import Decoder, encode


# ---- pure helpers --------------------------------------------------------- #

def test_build_connect_args_positions_and_version_echo():
    arg_names = ["VERSION_1_5_0", "hostname", "port", "password"]
    params = {"hostname": "h", "port": "3389"}
    assert build_connect_args(arg_names, params, "VERSION_1_5_0") == \
        ["VERSION_1_5_0", "h", "3389", ""]


def test_build_connect_args_without_version_pseudo_arg():
    assert build_connect_args(["hostname", "port"], {"hostname": "h", "port": "5900"}) == \
        ["h", "5900"]


def test_build_guacd_params_merges_template_and_target():
    template_spec = {"connectionParams": {"username": "abc", "security": "any"}}
    params = _build_guacd_params(template_spec, "svc.ns.svc.cluster.local", 3389)
    assert params == {
        "username": "abc", "security": "any",
        "hostname": "svc.ns.svc.cluster.local", "port": "3389",
    }


def test_build_guacd_params_target_wins_over_template():
    template_spec = {"connectionParams": {"hostname": "ignored", "port": "1"}}
    params = _build_guacd_params(template_spec, "real", 3389)
    assert params["hostname"] == "real"
    assert params["port"] == "3389"


def test_resolve_session_by_short_name():
    sessions = [{"name": "a"}, {"name": "b"}]
    assert resolve_session(sessions, "b") == {"name": "b"}
    assert resolve_session(sessions, "missing") is None


# ---- handshake against an in-memory fake guacd ---------------------------- #

class _FakeWriter:
    def __init__(self):
        self.data = bytearray()
        self.closed = False

    def write(self, b):
        self.data += b

    async def drain(self):
        pass

    def close(self):
        self.closed = True


def _reader_with(*chunks):
    r = asyncio.StreamReader()
    for c in chunks:
        r.feed_data(c)
    r.feed_eof()
    return r


def _instructions(data):
    return Decoder().feed(bytes(data))


async def test_handshake_drives_full_sequence_and_returns_id():
    reader = _reader_with(
        encode("args", "VERSION_1_5_0", "hostname", "port", "password"),
        encode("ready", "$conn-1"),
    )
    writer = _FakeWriter()

    conn_id, leftover = await handshake(
        reader, writer, protocol="rdp",
        params={"hostname": "h", "port": "3389", "password": "abc"},
        width=1024, height=768, dpi=96,
    )

    assert conn_id == "$conn-1"
    assert leftover == b""

    sent = _instructions(writer.data)
    opcodes = [i[0] for i in sent]
    assert opcodes == ["select", "size", "audio", "video", "image", "connect"]
    assert sent[0] == ["select", "rdp"]
    assert sent[1] == ["size", "1024", "768", "96"]
    assert sent[-1] == ["connect", "VERSION_1_5_0", "h", "3389", "abc"]


async def test_handshake_preserves_bytes_glued_to_ready():
    # guacd may start drawing immediately; data past `ready` must survive.
    reader = _reader_with(
        encode("args", "VERSION_1_5_0", "hostname", "port")
        + encode("ready", "x")
        + encode("sync", "12345"),
    )
    writer = _FakeWriter()

    conn_id, leftover = await handshake(
        reader, writer, protocol="rdp", params={"hostname": "h", "port": "3389"},
    )

    assert conn_id == "x"
    assert leftover == encode("sync", "12345")
