"""The desktop reverse proxy (/desktop/<id>/* -> in-pod Selkies server):
HTTP + WebSocket relaying, session resolution/authorization, the trailing-slash
redirect, and the dev-auth cookie that carries identity onto the proxied
requests. A local aiohttp app stands in for the sidecar's Selkies server; no
cluster."""
import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from whistler.portal.app import build_app


# --------------------------------------------------------------------------- #
# Fakes: the Selkies server the sidecar would run, and a ConfigManager whose   #
# sessions point at it.                                                        #
# --------------------------------------------------------------------------- #

def _fake_selkies_app():
    """The upstream surface the proxy must carry: index + assets (GET),
    /status (GET JSON), /tokens (POST), and the /websockets data channel."""
    async def index(request):
        return web.Response(text="SELKIES INDEX", content_type="text/html")

    async def status(request):
        return web.json_response({"current_mode": "websockets"})

    async def tokens(request):
        body = await request.text()
        return web.Response(text=f"tokens:{body}")

    async def websockets(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_str("MODE websockets")
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                await ws.send_str(f"echo:{msg.data}")
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await ws.send_bytes(b"echo:" + msg.data)
        return ws

    app = web.Application()
    app.add_routes([
        web.get("/", index),
        web.get("/status", status),
        web.post("/tokens", tokens),
        web.get("/websockets", websockets),
    ])
    return app


class FakeCM:
    """Just enough ConfigManager for the viewer app: per-user desktop-session
    lists (the proxy's authorization boundary), a create that mimics the CR
    409-on-existing behavior, and a no-op reconcile nudge."""

    def __init__(self):
        self.sessions = {}

    def get_user_desktop_sessions(self, username):
        return self.sessions.get(username, [])

    def add_desktop_session(self, username, template_name, session_name):
        mine = self.sessions.setdefault(username, [])
        if any(s["name"] == session_name for s in mine):
            return False  # KubeConfigManager returns False on the 409
        mine.append({**_session(session_name, 1), "template": template_name})
        return True

    def trigger_instance_start(self, username, name):
        return True


def _session(name, port, phase="Ready", address="127.0.0.1"):
    return {"name": name, "namespace": "ns", "phase": phase, "runtime": "container",
            "podName": f"pod-{name}", "address": address, "displayPort": port}


@pytest.fixture
async def backend():
    server = TestServer(_fake_selkies_app())
    await server.start_server()
    yield server
    await server.close()


@pytest.fixture
async def portal(backend, monkeypatch):
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")
    cm = FakeCM()
    cm.sessions["alice"] = [
        _session("d1", backend.port),
        _session("booting", backend.port, phase="Booting"),
    ]
    client = TestClient(TestServer(build_app(cm)))
    await client.start_server()
    client.cm = cm
    yield client
    await client.close()


# --------------------------------------------------------------------------- #
# HTTP relay                                                                   #
# --------------------------------------------------------------------------- #

async def test_proxies_index_html(portal):
    resp = await portal.get("/desktop/d1/", params={"user": "alice"})
    assert resp.status == 200
    assert await resp.text() == "SELKIES INDEX"


async def test_proxies_get_json(portal):
    resp = await portal.get("/desktop/d1/status", params={"user": "alice"})
    assert resp.status == 200
    assert (await resp.json())["current_mode"] == "websockets"


async def test_proxies_post_body(portal):
    resp = await portal.post("/desktop/d1/tokens", data=b'{"t":1}',
                             params={"user": "alice"})
    assert resp.status == 200
    assert await resp.text() == 'tokens:{"t":1}'


async def test_missing_slash_redirects_to_directory_url(portal):
    # The Selkies client resolves everything relative to the page directory, so
    # /desktop/<id> must become /desktop/<id>/ before it loads.
    resp = await portal.get("/desktop/d1", params={"user": "alice"},
                            allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/desktop/d1/?user=alice"


# --------------------------------------------------------------------------- #
# WebSocket relay                                                              #
# --------------------------------------------------------------------------- #

async def test_relays_websocket_both_ways(portal):
    ws = await portal.ws_connect("/desktop/d1/websockets?user=alice")
    assert (await ws.receive_str()) == "MODE websockets"
    await ws.send_str("hi")
    assert (await ws.receive_str()) == "echo:hi"
    await ws.send_bytes(b"\x00\x01")
    assert (await ws.receive_bytes()) == b"echo:\x00\x01"
    await ws.close()


# --------------------------------------------------------------------------- #
# Resolution / authorization                                                   #
# --------------------------------------------------------------------------- #

async def test_unknown_session_is_404(portal):
    resp = await portal.get("/desktop/nope/status", params={"user": "alice"})
    assert resp.status == 404


async def test_other_users_session_is_404(portal):
    # bob's namespace has no session named d1 — the per-user session list is
    # the authorization boundary.
    resp = await portal.get("/desktop/d1/status", params={"user": "bob"})
    assert resp.status == 404


async def test_not_ready_session_is_409(portal):
    resp = await portal.get("/desktop/booting/status", params={"user": "alice"})
    assert resp.status == 409
    assert "Booting" in await resp.text()


async def test_unreachable_backend_is_502(portal, backend):
    # A Ready session whose pod is gone: connection refused -> clean 502.
    portal.cm.sessions["alice"].append(_session("dead", 1))  # port 1: refused
    resp = await portal.get("/desktop/dead/status", params={"user": "alice"})
    assert resp.status == 502


# --------------------------------------------------------------------------- #
# /launch idempotence: re-launching an existing session reconnects to it;      #
# only a name collision with a different template is a conflict.               #
# --------------------------------------------------------------------------- #

async def test_launch_new_session_redirects_to_connect(portal):
    resp = await portal.post("/launch", params={"user": "alice"},
                             data={"template": "t1", "name": "fresh"},
                             allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/connect/fresh?user=alice"


async def test_relaunch_same_template_reconnects(portal):
    data = {"template": "t1", "name": "dup"}
    await portal.post("/launch", params={"user": "alice"}, data=data)
    resp = await portal.post("/launch", params={"user": "alice"}, data=data,
                             allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/connect/dup?user=alice"


async def test_relaunch_different_template_conflicts(portal):
    await portal.post("/launch", params={"user": "alice"},
                      data={"template": "t1", "name": "dup2"})
    resp = await portal.post("/launch", params={"user": "alice"},
                             data={"template": "t2", "name": "dup2"},
                             allow_redirects=False)
    assert resp.status == 409
    assert "t1" in await resp.text()


# --------------------------------------------------------------------------- #
# Dev-auth cookie: the proxied asset/WS requests carry no ?user=, so the       #
# identity that loaded the page must ride a cookie.                            #
# --------------------------------------------------------------------------- #

async def test_connect_page_sets_identity_cookie_used_by_proxy(portal):
    resp = await portal.get("/connect/d1", params={"user": "alice"})
    assert resp.status == 200
    assert any(c.key == "whistler_user" and c.value == "alice"
               for c in portal.session.cookie_jar)
    # No ?user= here: only the cookie identifies alice.
    resp = await portal.get("/desktop/d1/status")
    assert resp.status == 200


async def test_without_any_identity_falls_back_to_tester(portal):
    # Fresh client state: no cookie, no query -> "tester", who owns nothing.
    resp = await portal.get("/desktop/d1/status")
    assert resp.status == 404
