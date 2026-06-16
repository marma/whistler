"""guacd connection: the server-side Guacamole handshake (aiohttp-free).

The portal speaks to a shared guacd over TCP, performing the full client
handshake (``select`` / ``args`` / ``size``/``audio``/``video``/``image`` /
``connect`` / ``ready``) using connection params resolved from the
DesktopSession + its template — exactly the role guacamole-lite plays. The
browser's ``Guacamole.Client`` only streams after ``ready``.

Kept free of aiohttp so the pure helpers and the handshake (which needs only
asyncio streams) are unit-testable against an in-memory fake guacd. The
WebSocket relay lives in ``app.py``.
"""
import asyncio
import logging
import os
from typing import Dict, List, Optional, Tuple

from whistler.portal.protocol import Decoder, ProtocolError, encode

logger = logging.getLogger("whistler.portal")

GUACD_HOST = os.environ.get("GUACD_HOST", "whistler-guacd")
GUACD_PORT = int(os.environ.get("GUACD_PORT", "4822"))

# guacd's `args` reply lists parameter names; in guacd >= 1.3 the first element
# is a protocol-version pseudo-arg. The client echoes back the version IT
# supports and guacd operates at min(its version, ours). This MUST match the
# version of the browser's guacamole-common-js: advertise too high and guacd
# emits drawing instructions the client can't render (black screen). We now
# vendor guacamole-common-js 1.6.0 (whistler/portal/static/, served by the
# portal) instead of the npm-capped 1.5.0, because 1.5.0's renderer mishandles
# guacd's save-under copy ops (the selection-rectangle / drag artifacts). So
# advertise 1.6.0 to match the vendored client and guacd 1.6.x.
_VERSION_PREFIX = "VERSION_"
_CLIENT_PROTOCOL_VERSION = "VERSION_1_6_0"

# Image formats we advertise to guacd on the browser's behalf (like
# guacamole-lite), so guacd encodes screen updates in a format the browser
# renders. Declaring none yields "source image could not be decoded" for every
# frame.
#
# NOTE: this does NOT control lossiness. PNG and JPEG are baseline formats guacd
# assumes every client supports, so even advertising only image/png, guacd still
# emits JPEG for regions its lossy heuristic deems photographic (visible ringing
# around edges). Lossless is forced by the connection param force-lossless=true
# (set on the DesktopTemplate), not here. We still list only image/png (and omit
# WebP, which decoded unreliably in browsers).
_IMAGE_MIMETYPES = ("image/png",)


def build_connect_args(arg_names: List[str], params: Dict[str, str],
                       version: Optional[str] = None) -> List[str]:
    """Positional values for the ``connect`` instruction, one per name guacd
    listed in ``args`` (order preserved). The version pseudo-arg is echoed;
    named params come from ``params``; anything unknown is empty."""
    values = []
    for name in arg_names:
        if name.startswith(_VERSION_PREFIX):
            values.append(version or name)
        else:
            values.append(params.get(name, ""))
    return values


def resolve_session(sessions: List[Dict], short_name: str) -> Optional[Dict]:
    """Find a session by its short (per-user) name in a list from
    ``get_user_desktop_sessions``."""
    return next((s for s in sessions if s.get("name") == short_name), None)


def _build_guacd_params(template_spec: Dict, address: str, display_port) -> Dict[str, str]:
    """Merge a template's ``connectionParams`` with the resolved target. The CR
    is the source of truth: hostname/port always come from the live session, so
    they win over anything in the template. ``rdpSecurity`` (a first-class field)
    is injected as the guacd ``security`` param and takes precedence over a raw
    ``security`` key in ``connectionParams``."""
    params = {str(k): str(v) for k, v in (template_spec.get("connectionParams") or {}).items()}
    if rdp_security := template_spec.get("rdpSecurity"):
        params["security"] = rdp_security
    params["hostname"] = address
    params["port"] = str(display_port)
    return params


async def handshake(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *,
                    protocol: str, params: Dict[str, str],
                    width: int = 1024, height: int = 768,
                    dpi: int = 96) -> Tuple[str, bytes]:
    """Drive the guacd handshake; return ``(connection_id, leftover_bytes)``.

    ``leftover_bytes`` is any stream data that arrived glued to ``ready`` (guacd
    may start drawing immediately); the caller must flush it to the browser
    before the raw relay so nothing is dropped."""
    decoder = Decoder()
    pending: List[List[str]] = []

    async def read_instruction() -> List[str]:
        while not pending:
            data = await reader.read(65536)
            if not data:
                raise ProtocolError("guacd closed the connection during handshake")
            pending.extend(decoder.feed(data))
        return pending.pop(0)

    writer.write(encode("select", protocol))
    await writer.drain()

    args = await read_instruction()
    if not args or args[0] != "args":
        raise ProtocolError(f"expected 'args' from guacd, got {args!r}")
    arg_names = args[1:]
    # Advertise the vendored browser client's protocol version (not guacd's) so
    # guacd emits instructions our guacamole-common-js (1.6.0) can render.
    version = _CLIENT_PROTOCOL_VERSION

    writer.write(encode("size", str(width), str(height), str(dpi)))
    writer.write(encode("audio"))            # no audio
    writer.write(encode("video"))            # no video
    writer.write(encode("image", *_IMAGE_MIMETYPES))
    await writer.drain()

    writer.write(encode("connect", *build_connect_args(arg_names, params, version)))
    await writer.drain()

    ready = await read_instruction()
    if not ready or ready[0] != "ready":
        raise ProtocolError(f"expected 'ready' from guacd, got {ready!r}")
    connection_id = ready[1] if len(ready) > 1 else ""

    # Recover any complete instructions and the partial tail read past `ready`.
    # `pending_bytes` (not `pending`) so a multibyte char split across the read
    # that delivered `ready` isn't dropped — that loss desyncs the stream and
    # surfaces in the browser as "could not be decoded" / "Incomplete instruction".
    leftover = b"".join(encode(*instr) for instr in pending) + decoder.pending_bytes
    return connection_id, leftover
