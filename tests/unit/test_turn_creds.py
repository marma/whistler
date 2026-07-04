"""TURN credential minting + ICE config + signaling relay (whistler.portal.webrtc).

The cred scheme is coturn's use-auth-secret (TURN REST API): the credential is a
deterministic HMAC of the expiry-based username, so coturn validates it without
shared state. These tests pin that contract — a drift here silently breaks every
webrtc session (the browser would fail ICE against coturn)."""
import asyncio
import base64
import hashlib
import hmac

import aiohttp
import pytest
from aiohttp import web

from whistler.portal import webrtc


# ---- pure: credential minting -------------------------------------------- #

def test_mint_credentials_match_coturn_hmac():
    secret, now, ttl = "s3cr3t", 1_000_000, 3600
    creds = webrtc.mint_turn_credentials(secret, ttl=ttl, now=now)
    assert creds["username"] == str(now + ttl)
    expected = base64.b64encode(
        hmac.new(secret.encode(), creds["username"].encode(), hashlib.sha1).digest()
    ).decode()
    assert creds["credential"] == expected


def test_mint_credentials_label_prefixes_username():
    creds = webrtc.mint_turn_credentials("x", ttl=60, label="alice-d1", now=0)
    assert creds["username"] == "60:alice-d1"
    # credential is the HMAC of the *full* username including the label
    expected = base64.b64encode(
        hmac.new(b"x", b"60:alice-d1", hashlib.sha1).digest()
    ).decode()
    assert creds["credential"] == expected


def test_mint_credentials_deterministic_for_same_inputs():
    a = webrtc.mint_turn_credentials("k", ttl=10, now=5)
    b = webrtc.mint_turn_credentials("k", ttl=10, now=5)
    assert a == b


# ---- pure: ICE server list ----------------------------------------------- #

def test_ice_servers_empty_without_host():
    assert webrtc.ice_servers(host="", port=3478, protocol="udp", secret="s") == []


def test_ice_servers_stun_plus_turn_with_creds():
    servers = webrtc.ice_servers(host="turn.example", port=3478, protocol="udp",
                                 secret="s", ttl=60, now=0)
    stun, turn = servers
    assert stun == {"urls": ["stun:turn.example:3478"]}
    assert turn["urls"] == ["turn:turn.example:3478?transport=udp"]
    assert turn["username"] == "60"
    assert "credential" in turn


def test_ice_servers_from_env(monkeypatch):
    monkeypatch.setenv("TURN_EXTERNAL_HOST", "turn.host")
    monkeypatch.setenv("TURN_PORT", "3478")
    monkeypatch.setenv("TURN_PROTOCOL", "tcp")
    monkeypatch.setenv("TURN_SHARED_SECRET", "abc")
    servers = webrtc.ice_servers_from_env(label="x")
    assert servers[1]["urls"] == ["turn:turn.host:3478?transport=tcp"]


def test_ice_servers_from_env_disabled_when_unset(monkeypatch):
    monkeypatch.delenv("TURN_EXTERNAL_HOST", raising=False)
    assert webrtc.ice_servers_from_env() == []


def test_signal_url_uses_signal_path():
    url = webrtc.signal_url("10.0.0.5", 8082)
    assert url == f"ws://10.0.0.5:8082{webrtc.SIGNAL_PATH}"


# ---- signaling relay: opaque whole-frame WS<->WS pump -------------------- #

def test_signal_relay_round_trips_frames():
    """The relay must forward browser->pod and pod->upstream frames intact, both
    text and binary, without interpreting them (Selkies signaling is self-
    delimited JSON). Stand up a fake in-pod Selkies WS that echoes a greeting and
    mirrors what it receives; drive it through the real relay with a client WS."""

    async def scenario():
        # Fake in-pod Selkies signaling server: greet, then echo with a prefix.
        async def upstream_handler(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_str("HELLO")
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await ws.send_str("echo:" + msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    await ws.send_bytes(b"bin:" + msg.data)
            return ws

        up_app = web.Application()
        up_app.add_routes([web.get("/ws", upstream_handler)])
        up_runner = web.AppRunner(up_app)
        await up_runner.setup()
        up_site = web.TCPSite(up_runner, "127.0.0.1", 0)
        await up_site.start()
        up_port = up_runner.addresses[0][1]

        # Portal-side endpoint that hands the browser WS to the real relay.
        async def portal_handler(request):
            wsr = web.WebSocketResponse()
            await wsr.prepare(request)
            await webrtc.signal_relay(wsr, f"ws://127.0.0.1:{up_port}/ws")
            return wsr

        portal_app = web.Application()
        portal_app.add_routes([web.get("/ws-signal", portal_handler)])
        portal_runner = web.AppRunner(portal_app)
        await portal_runner.setup()
        portal_site = web.TCPSite(portal_runner, "127.0.0.1", 0)
        await portal_site.start()
        portal_port = portal_runner.addresses[0][1]

        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(f"ws://127.0.0.1:{portal_port}/ws-signal") as ws:
                    assert (await ws.receive()).data == "HELLO"
                    await ws.send_str('{"sdp":"offer"}')
                    assert (await ws.receive()).data == 'echo:{"sdp":"offer"}'
                    await ws.send_bytes(b"\x01\x02")
                    assert (await ws.receive()).data == b"bin:\x01\x02"
        finally:
            await portal_runner.cleanup()
            await up_runner.cleanup()

    asyncio.run(scenario())
