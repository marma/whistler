"""Desktop reverse proxy: carries ``/desktop/<id>/...`` to the in-pod Selkies
2.x server (the streamer sidecar) over the per-session ClusterIP Service.

The Selkies 2.x web client is fully page-relative — the Vite build uses
``base: ''`` and the WebSocket / API URLs are derived from the *directory* of
the page URL (``.../websockets``, ``.../status``, …). So proxying it under a
path prefix needs no HTML/JS rewriting at all: strip ``/desktop/<id>`` from the
incoming path and forward the remainder verbatim. The two relay flavors here
are deliberately protocol-transparent:

  * ``relay_http`` — any method, streamed request/response bodies, hop-by-hop
    headers dropped, payload bytes untouched (the shared ``ClientSession`` must
    be created with ``auto_decompress=False`` so Content-Encoding survives).
  * ``relay_ws`` — the H.264/input WebSocket (``/websockets``): dial upstream
    first (so a dead pod is a clean 502, not a half-open browser socket), then
    pump text/binary frames both ways until either side closes.

Session resolution / authorization stays in ``app.py`` — this module only ever
sees a target URL that is already known to belong to the requesting user.
"""
import asyncio
import logging

import aiohttp
from aiohttp import WSMsgType, web

logger = logging.getLogger("whistler.portal")

# Hop-by-hop headers (RFC 9110 §7.6.1) are per-connection and must not be
# forwarded; aiohttp manages its own on each leg. Host is recomputed for the
# upstream leg, and Content-Length/-Encoding pass through untouched because the
# body bytes do too.
_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade",
})

_WS_CLOSED_TYPES = (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED, WSMsgType.ERROR)

# Selkies serves its web root with ETag/Last-Modified but NO Cache-Control, so
# browsers fall back to heuristic freshness (~10% of the file's age — days for
# a bundle whose mtime is the image build). The entry points are not
# content-hashed (/index.html, /src/selkies-core.js) and the /desktop/<id>/ URL
# is stable across image updates, so a Mac that loaded a session weeks ago can
# keep running a weeks-stale viewer without ever revalidating. Stamp no-cache
# (NOT no-store: revalidation is a cheap 304 thanks to the upstream ETag) on
# the types the viewer boots from. Content-hashed assets get revalidated too —
# a 304 apiece is a fair price for never wedging a browser on a stale client.
# Upstream Cache-Control, if Selkies ever starts sending one, wins untouched.
_REVALIDATE_TYPES = ("text/html", "javascript", "application/json")


def _forward_headers(headers, *, drop_host=False):
    skip = _HOP_BY_HOP | ({"host"} if drop_host else set())
    return [(k, v) for k, v in headers.items() if k.lower() not in skip]


def is_websocket_upgrade(request: web.Request) -> bool:
    return (
        "upgrade" in request.headers.get("Connection", "").lower()
        and request.headers.get("Upgrade", "").lower() == "websocket"
    )


async def relay_http(request: web.Request, target_url: str,
                     session: aiohttp.ClientSession) -> web.StreamResponse:
    """Forward one plain-HTTP request to ``target_url`` and stream the response
    back. Transparent: method, query (already inside target_url), headers minus
    hop-by-hop, and body bytes all pass through unchanged."""
    body = request.content if request.body_exists else None
    try:
        async with session.request(
            request.method, target_url,
            headers=_forward_headers(request.headers, drop_host=True),
            data=body, allow_redirects=False,
        ) as upstream:
            response = web.StreamResponse(status=upstream.status)
            for k, v in _forward_headers(upstream.headers):
                response.headers.add(k, v)
            if ("Cache-Control" not in response.headers
                    and any(t in response.headers.get("Content-Type", "")
                            for t in _REVALIDATE_TYPES)):
                response.headers["Cache-Control"] = "no-cache"
            await response.prepare(request)
            async for chunk in upstream.content.iter_chunked(64 * 1024):
                await response.write(chunk)
            await response.write_eof()
            return response
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"Desktop proxy: upstream {target_url} failed: {e}")
        return web.Response(status=502, text="desktop backend unreachable")


async def _pump(src, dst):
    """One direction of the WebSocket relay. Returns when src stops yielding
    data frames; the caller closes both ends."""
    async for msg in src:
        if msg.type == WSMsgType.TEXT:
            await dst.send_str(msg.data)
        elif msg.type == WSMsgType.BINARY:
            await dst.send_bytes(msg.data)
        elif msg.type in _WS_CLOSED_TYPES:
            break
        # PING/PONG are handled per-leg by aiohttp itself.


async def relay_ws(request: web.Request, target_ws_url: str,
                   session: aiohttp.ClientSession) -> web.StreamResponse:
    """Bridge the browser's WebSocket to the in-pod Selkies WebSocket.

    max_msg_size=0 (unlimited) on both legs: the H.264 stream is framed by
    Selkies, not by us, and a clamped frame kills the video mid-session."""
    try:
        upstream = await session.ws_connect(
            target_ws_url, max_msg_size=0, autoping=True,
        )
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"Desktop proxy: WS upstream {target_ws_url} failed: {e}")
        return web.Response(status=502, text="desktop backend unreachable")

    downstream = web.WebSocketResponse(max_msg_size=0)
    await downstream.prepare(request)
    try:
        # Either side closing ends its pump; cancel the other and close both.
        done, pending = await asyncio.wait(
            [asyncio.ensure_future(_pump(downstream, upstream)),
             asyncio.ensure_future(_pump(upstream, downstream))],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.warning(f"Desktop proxy: WS relay error: {exc}")
    finally:
        await upstream.close()
        if not downstream.closed:
            await downstream.close()
    return downstream
