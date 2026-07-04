"""WebRTC (Selkies) viewer support for the portal — the parallel of ``guacd.py``.

Where the guacd viewer relays the Guacamole wire protocol to a shared guacd that
re-rasterizes frames, the webrtc viewer relays only **signaling** to an in-pod
Selkies server; the H.264/VP8 media flows browser <-> coturn <-> pod over a real
WebRTC peer connection (so the codec reaches the browser's hardware decoder).

This module holds the pure, unit-testable pieces:
  - ``mint_turn_credentials`` / ``ice_servers`` — coturn's use-auth-secret
    (time-limited HMAC) scheme, handed to the browser via ``/ice``.
  - ``signal_relay`` — an opaque WS<->WS pump between the browser and the in-pod
    Selkies signaling socket (JSON frames; no framing rules like guacd's, so a
    whole-frame relay is enough).

Env (rendered from whistler.coturn values onto the portal Deployment):
  TURN_EXTERNAL_HOST  browser-reachable coturn host (empty => no TURN, host
                      candidates only — dev/host-network only)
  TURN_PORT           default 3478
  TURN_PROTOCOL       udp | tcp
  TURN_SHARED_SECRET  shared static-auth-secret (same one coturn validates)
"""
import asyncio
import base64
import hashlib
import hmac
import logging
import os
import time

import aiohttp
from aiohttp import web

logger = logging.getLogger("whistler.portal")

# In-pod Selkies signaling WS path the portal dials (ws://<address>:<signalPort><path>).
# Selkies-gstreamer serves signaling at /webrtc/signalling/ (verified: that path
# returns HTTP 426 Upgrade Required, i.e. expects a WS upgrade). Overridable so the
# relay can match a different server build.
SIGNAL_PATH = os.environ.get("SELKIES_SIGNAL_PATH", "/webrtc/signalling/")

# Default lifetime of a minted TURN credential. Long enough to outlast a session
# launch; the browser only needs it valid at ICE-gathering time.
_DEFAULT_TTL = 24 * 3600


def mint_turn_credentials(secret: str, ttl: int = _DEFAULT_TTL, label: str = "",
                          now: int | None = None) -> dict:
    """coturn use-auth-secret (a.k.a. TURN REST API) time-limited credential.

    ``username`` is the unix expiry (optionally ``<expiry>:<label>``) and
    ``credential`` is base64(HMAC-SHA1(secret, username)). coturn recomputes the
    same HMAC from its shared secret to validate — no per-user state, no DB. Pure
    function of its inputs (``now`` injectable) so it is unit-tested.
    """
    expiry = int((now if now is not None else time.time()) + ttl)
    username = f"{expiry}:{label}" if label else str(expiry)
    digest = hmac.new(secret.encode(), username.encode(), hashlib.sha1).digest()
    return {"username": username, "credential": base64.b64encode(digest).decode()}


def ice_servers(*, host: str, port: int, protocol: str, secret: str,
                ttl: int = _DEFAULT_TTL, label: str = "", now: int | None = None) -> list:
    """RTCPeerConnection ``iceServers`` for the browser: a STUN + a TURN entry on
    the same coturn, the TURN entry carrying freshly-minted time-limited creds.
    Returns ``[]`` when no TURN host is configured (host-candidate fallback —
    only works with host networking, i.e. dev)."""
    if not host:
        return []
    stun = {"urls": [f"stun:{host}:{port}"]}
    turn = {"urls": [f"turn:{host}:{port}?transport={protocol}"]}
    turn.update(mint_turn_credentials(secret, ttl=ttl, label=label, now=now))
    return [stun, turn]


def ice_servers_from_env(label: str = "") -> list:
    """``ice_servers`` sourced from the portal's TURN_* env (see module docstring)."""
    host = os.environ.get("TURN_EXTERNAL_HOST", "")
    return ice_servers(
        host=host,
        port=int(os.environ.get("TURN_PORT", "3478") or "3478"),
        protocol=os.environ.get("TURN_PROTOCOL", "udp"),
        secret=os.environ.get("TURN_SHARED_SECRET", ""),
        label=label,
    )


def signal_url(address: str, signal_port: int) -> str:
    return f"ws://{address}:{signal_port}{SIGNAL_PATH}"


async def signal_relay(wsr: web.WebSocketResponse, upstream_url: str) -> None:
    """Opaque WS<->WS pump between the browser (``wsr``) and the in-pod Selkies
    signaling socket. Selkies signaling is self-delimited JSON frames, so — unlike
    the guacd relay — there is no instruction-boundary rule to honor; each frame is
    forwarded whole. Closing either side tears down both."""
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(upstream_url) as upstream:

            async def browser_to_pod():
                async for msg in wsr:
                    if msg.type == web.WSMsgType.TEXT:
                        await upstream.send_str(msg.data)
                    elif msg.type == web.WSMsgType.BINARY:
                        await upstream.send_bytes(msg.data)
                    else:
                        break

            async def pod_to_browser():
                async for msg in upstream:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await wsr.send_str(msg.data)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        await wsr.send_bytes(msg.data)
                    else:
                        break

            # Stop as soon as *either* side ends and tear down both — otherwise a
            # browser that closes leaves the upstream-read side blocked forever
            # (a leaked signaling connection per departed session).
            tasks = [asyncio.create_task(browser_to_pod()),
                     asyncio.create_task(pod_to_browser())]
            try:
                _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                if not upstream.closed:
                    await upstream.close()
                if not wsr.closed:
                    await wsr.close()
