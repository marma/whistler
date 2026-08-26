"""The desktop reverse proxy (/desktop/<id>/* -> in-pod Selkies server):
HTTP + WebSocket relaying, session resolution/authorization, the trailing-slash
redirect, the dev-auth cookie that carries identity onto the proxied requests,
and the VM viewer/terminal paths (noVNC page, /ws-vnc + /ws-term bridged to a
fake KubeVirt subresource endpoint). A local aiohttp app stands in for the
sidecar's Selkies server and the API-server subresources; no cluster."""
import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from whistler.config import CHANNELS, CHANNEL_TERMINAL
from whistler.portal import kubevirt, screenshots
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

    async def core_js(request):
        # Like the real web root: ETag/Last-Modified, no Cache-Control.
        return web.Response(text="// core", content_type="text/javascript",
                            headers={"Etag": '"abc"'})

    async def png(request):
        return web.Response(body=b"\x89PNG", content_type="image/png")

    async def cached_js(request):
        # Upstream that DOES set Cache-Control keeps its own policy.
        return web.Response(text="// cached", content_type="text/javascript",
                            headers={"Cache-Control": "max-age=3600"})

    async def framed(request):
        # Some in-session servers stamp X-Frame-Options on their web root.
        return web.Response(text="FRAMED", content_type="text/html",
                            headers={"X-Frame-Options": "DENY"})

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

    async def subresource(request):
        # Stands in for the KubeVirt console/vnc subresources: raw bytes over
        # binary frames (greeting first, then echo).
        ws = web.WebSocketResponse(protocols=(kubevirt.SUBPROTOCOL,))
        await ws.prepare(request)
        await ws.send_bytes(b"HELLO:" + request.match_info["kind"].encode())
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                await ws.send_bytes(b"echo:" + msg.data)
        return ws

    app = web.Application()
    app.add_routes([
        web.get("/", index),
        web.get("/src/selkies-core.js", core_js),
        web.get("/vite.svg", png),
        web.get("/cached.js", cached_js),
        web.get("/framed.html", framed),
        web.get("/status", status),
        web.post("/tokens", tokens),
        web.get("/websockets", websockets),
        web.get("/subresource/{kind}", subresource),
    ])
    return app


class FakeCM:
    """Just enough ConfigManager for the viewer app: per-user desktop-session
    lists (the proxy's authorization boundary), a create that mimics the CR
    409-on-existing behavior, and a no-op reconcile nudge."""

    def __init__(self):
        self.sessions = {}
        self.admins = set()
        # Channels granted per user; the default is everything, which is what
        # an ungrouped user in an unrestricted zone gets.
        self.channels = {}

    def is_user_admin(self, username):
        return username in self.admins

    def may_enter(self, username, entry_point):
        """Entry points this double does not restrict — the binding has its own
        tests (test_entry_points.py); here every user holds every door."""
        return True

    def session_channels(self, username, name):
        return self.channels.get(username, set(CHANNELS))

    def get_user_desktop_sessions(self, username):
        return self.sessions.get(username, [])

    def get_user_instances(self, username):
        return []  # ssh instances are irrelevant to the viewer tests

    def get_vmi_address(self, username, name):
        return "10.42.0.99"

    def get_vm_access_private_key(self, username):
        return "FAKE-PRIVATE-KEY"

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
            "podName": f"pod-{name}", "vmiName": None, "address": address,
            "displayPort": port, "viewer": "websockets"}


def _vm_session(name, phase="Ready", vmi="vmi"):
    return {"name": name, "namespace": "ns", "phase": phase, "runtime": "vm",
            "podName": None, "vmiName": f"{vmi}-{name}" if vmi else None,
            "address": None, "displayPort": None, "viewer": "vnc"}


@pytest.fixture
async def backend():
    server = TestServer(_fake_selkies_app())
    await server.start_server()
    yield server
    await server.close()


@pytest.fixture
async def portal(backend, monkeypatch):
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")
    # The screenshot loop is a background task on the app; these tests exercise
    # the routes, so keep it from grabbing at a FakeCM (0 = disabled).
    monkeypatch.setenv("WHISTLER_SCREENSHOT_INTERVAL", "0")
    cm = FakeCM()
    # The machine console is admin-only, and most VNC tests are about the
    # relay rather than the gate; a separate test covers the refusal.
    cm.admins.add("alice")
    cm.sessions["alice"] = [
        _session("d1", backend.port),
        _session("booting", backend.port, phase="Booting"),
        _vm_session("v1"),
        _vm_session("vbooting", phase="Booting", vmi=None),
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


async def test_upstream_x_frame_options_is_dropped(portal):
    """Framing is the portal's call, not the guest's. The kiosk session page
    embeds /desktop/<id>/ in a same-origin iframe so it can keep an idle
    watcher over the desktop; an upstream X-Frame-Options would make that a
    blank rectangle, and it is an opinion about a URL the guest does not
    serve."""
    resp = await portal.get("/desktop/d1/framed.html", params={"user": "alice"})
    assert resp.status == 200
    assert "X-Frame-Options" not in resp.headers


async def test_viewer_entry_points_get_no_cache_stamped(portal):
    # Selkies serves the (unhashed) entry points with no Cache-Control, and the
    # stable /desktop/<id>/ URL makes heuristic caching serve weeks-stale
    # viewer bundles. The proxy stamps no-cache so browsers revalidate (cheap:
    # upstream sends an ETag).
    for path in ("", "src/selkies-core.js", "status"):
        resp = await portal.get(f"/desktop/d1/{path}", params={"user": "alice"})
        assert resp.headers["Cache-Control"] == "no-cache", path


async def test_non_boot_content_and_upstream_policy_pass_through(portal):
    # Media keeps heuristic caching; an explicit upstream Cache-Control wins.
    resp = await portal.get("/desktop/d1/vite.svg", params={"user": "alice"})
    assert "Cache-Control" not in resp.headers
    resp = await portal.get("/desktop/d1/cached.js", params={"user": "alice"})
    assert resp.headers["Cache-Control"] == "max-age=3600"


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


# --------------------------------------------------------------------------- #
# VM sessions: /status viewer, the noVNC page, and the /ws-vnc + /ws-term      #
# bridges to the (faked) KubeVirt subresources.                                #
# --------------------------------------------------------------------------- #

def _fake_subresource_opener(backend):
    async def fake_open(session, namespace, name, kind):
        assert namespace == "ns"
        return await session.ws_connect(
            f"http://127.0.0.1:{backend.port}/subresource/{kind}")
    return fake_open


async def test_status_reports_viewer(portal):
    for name, viewer in (("d1", "websockets"), ("v1", "vnc")):
        resp = await portal.get(f"/status/{name}", params={"user": "alice"})
        assert (await resp.json())["viewer"] == viewer


async def test_vnc_page_serves_novnc_client(portal):
    resp = await portal.get("/vnc/v1", params={"user": "alice"})
    assert resp.status == 200
    body = await resp.text()
    assert "/static/novnc/core/rfb.js" in body
    assert "/ws-vnc/" in body


async def test_novnc_module_served_statically(portal):
    resp = await portal.get("/static/novnc/core/rfb.js")
    assert resp.status == 200


async def test_machine_console_is_admin_only(portal):
    """Both the page and the websocket, because the socket is reachable
    without ever loading the page — hiding the dashboard button is not an
    access control."""
    portal.cm.admins.discard("alice")
    assert (await portal.get("/console/v1", params={"user": "alice"})).status == 403
    assert (await portal.get("/ws-console/v1", params={"user": "alice"})).status == 403


async def test_machine_console_admin_check_precedes_session_lookup(portal):
    """A non-admin gets the same 403 for a session that doesn't exist, so the
    console can't be used to probe which sessions a user has."""
    portal.cm.admins.discard("alice")
    assert (await portal.get("/ws-console/nope", params={"user": "alice"})).status == 403


async def test_vnc_desktop_is_not_admin_only(portal):
    """/connect redirects viewer:vnc VMs to /vnc, so gating it on admin would
    take the desktop away from every VM that uses the noVNC viewer — which is
    the default for VM sessions. Regression guard."""
    portal.cm.admins.discard("alice")
    assert (await portal.get("/vnc/v1", params={"user": "alice"})).status == 200


async def test_console_attaches_before_the_session_is_ready(portal, backend, monkeypatch):
    """The whole point of the console: firmware and kernel messages happen
    long before Ready, so waiting for it would mean the boot output has
    already scrolled past."""
    monkeypatch.setattr(kubevirt, "open_subresource_ws",
                        _fake_subresource_opener(backend))
    booting = _vm_session("vbooting2", phase="Provisioning")
    portal.cm.sessions["alice"].append(booting)
    ws = await portal.ws_connect("/ws-console/vbooting2?user=alice")
    assert (await ws.receive_bytes()) == b"HELLO:vnc"
    await ws.close()


async def test_console_still_needs_a_vm_instance(portal):
    """Nothing to attach to before qemu exists — reported as a wait, not a
    failure."""
    assert (await portal.get("/ws-console/vbooting",
                             params={"user": "alice"})).status == 409


async def test_vnc_desktop_still_waits_for_ready(portal):
    """The desktop keeps its Ready gate: a desktop you cannot use yet is not
    worth showing, and that is the opposite of what the console is for."""
    assert (await portal.get("/ws-vnc/vbooting", params={"user": "alice"})).status == 409


async def test_ws_vnc_authorization(portal):
    # Unknown session, non-vm session, not-Ready vm session.
    assert (await portal.get("/ws-vnc/nope", params={"user": "alice"})).status == 404
    assert (await portal.get("/ws-vnc/d1", params={"user": "alice"})).status == 400
    assert (await portal.get("/ws-vnc/vbooting", params={"user": "alice"})).status == 409


async def test_ws_vnc_relays_rfb_bytes(portal, backend, monkeypatch):
    monkeypatch.setattr(kubevirt, "open_subresource_ws",
                        _fake_subresource_opener(backend))
    ws = await portal.ws_connect("/ws-vnc/v1?user=alice")
    assert (await ws.receive_bytes()) == b"HELLO:vnc"
    await ws.send_bytes(b"RFB 003.008\n")
    assert (await ws.receive_bytes()) == b"echo:RFB 003.008\n"
    await ws.close()


async def test_ws_term_vm_uses_ssh_relay(portal, monkeypatch):
    # The default VM terminal is an SSH session into the guest; assert the
    # route resolves address/key from the CM and hands the socket to relay_ssh.
    seen = {}

    async def fake_relay_ssh(browser_ws, host, username, private_key, **kw):
        seen.update(host=host, username=username, key=private_key)
        await browser_ws.send_bytes(b"SSH-SHELL")
        await browser_ws.close()

    monkeypatch.setattr(kubevirt, "relay_ssh", fake_relay_ssh)
    ws = await portal.ws_connect("/ws-term/v1?user=alice")
    assert (await ws.receive_bytes()) == b"SSH-SHELL"
    await ws.close()
    assert seen == {"host": "10.42.0.99", "username": "alice",
                    "key": "FAKE-PRIVATE-KEY"}


async def test_ws_term_is_refused_without_the_terminal_channel(portal, monkeypatch):
    """The portal is one of the doors a channel grant has to hold at — a grant
    missed at one entry point leaks the whole identity half of the border."""
    portal.cm.channels["alice"] = {c for c in CHANNELS if c != CHANNEL_TERMINAL}

    async def explode(*args, **kwargs):
        raise AssertionError("the relay must not be reached")

    monkeypatch.setattr(kubevirt, "relay_ssh", explode)
    resp = await portal.get("/ws-term/v1", params={"user": "alice"})
    assert resp.status == 403
    # And the page that opens it refuses too, before nudging the session awake.
    assert (await portal.get("/term/v1", params={"user": "alice"})).status == 403


async def test_ws_term_vm_console_mode(portal, backend, monkeypatch):
    # WHISTLER_VM_TERMINAL=console falls back to the serial-console relay.
    monkeypatch.setenv("WHISTLER_VM_TERMINAL", "console")
    monkeypatch.setattr(kubevirt, "open_subresource_ws",
                        _fake_subresource_opener(backend))
    ws = await portal.ws_connect("/ws-term/v1?user=alice")
    assert (await ws.receive_bytes()) == b"HELLO:console"
    # Text keystrokes reach the console as bytes; resize frames are dropped.
    await ws.send_str('{"resize": [120, 40]}')
    await ws.send_str("ls\n")
    assert (await ws.receive_bytes()) == b"echo:ls\n"
    await ws.close()


# --------------------------------------------------------------------------- #
# /screenshot/<id> — serves the newest thumbnail the capture loop stored.      #
# The capture path itself is tested in test_screenshots.py.                    #
# --------------------------------------------------------------------------- #

async def test_screenshot_serves_the_stored_png(portal):
    screenshots.STORE.put("alice", "d1", b"\x89PNG-fake")
    try:
        resp = await portal.get("/screenshot/d1?user=alice")
        assert resp.status == 200
        assert resp.content_type == "image/png"
        assert resp.headers["Cache-Control"] == "no-store"
        assert await resp.read() == b"\x89PNG-fake"
    finally:
        screenshots.STORE.keep_only([])


async def test_screenshot_404s_before_the_first_capture(portal):
    screenshots.STORE.keep_only([])
    resp = await portal.get("/screenshot/d1?user=alice")
    assert resp.status == 404


async def test_screenshot_is_scoped_to_the_requesting_user(portal):
    # The store is keyed by (user, session), so bob asking for alice's session
    # name resolves to *his* key and misses — no cross-user read.
    screenshots.STORE.put("alice", "d1", b"alice-png")
    try:
        resp = await portal.get("/screenshot/d1?user=bob")
        assert resp.status == 404
    finally:
        screenshots.STORE.keep_only([])
