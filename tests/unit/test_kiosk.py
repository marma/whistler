"""The kiosk surface (whistler.portal.kiosk): who gets in, what they are shown,
and what the surface deliberately does not offer.

The kiosk is Whistler's half of the "kiosk situation" (design/security.md,
"Closing the fourth axis"): a user bound to it should reach their desktops and
nothing else. So these tests care as much about what is *absent* — no template
picker, no launch form, no terminal, no admin — as about what renders.
"""
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from whistler.portal import kiosk
from whistler.portal.app import build_app
from whistler.status import STATUS_GROUPS


class FakeCM:
    def __init__(self):
        self.sessions = {}

    def get_user_desktop_sessions(self, username):
        return self.sessions.get(username, [])

    def get_user_instances(self, username):
        return []

    def is_user_admin(self, username):
        return False

    def trigger_instance_start(self, username, name):
        return True


def _desktop(name, phase="Ready", template="xfce"):
    return {"name": name, "template": template, "namespace": "ns",
            "phase": phase, "runtime": "vm", "viewer": "vnc",
            "podName": None, "vmiName": f"vmi-{name}", "address": None,
            "displayPort": None}


@pytest.fixture
async def portal(monkeypatch):
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")
    monkeypatch.setenv("WHISTLER_SCREENSHOT_INTERVAL", "0")
    cm = FakeCM()
    cm.sessions["alice"] = [_desktop("desk"), _desktop("off", phase="Stopped")]
    cm.sessions["bob"] = [_desktop("bobs-box")]
    client = TestClient(TestServer(build_app(cm)))
    await client.start_server()
    client.cm = cm
    yield client
    await client.close()


# --------------------------------------------------------------------------- #
# Getting in                                                                   #
# --------------------------------------------------------------------------- #

async def test_bare_kiosk_shows_the_login_form(portal):
    """No cookie, no ?user= — and specifically NOT the middleware's fallback
    identity, which would mean nobody is ever logged out of a kiosk."""
    resp = await portal.get("/kiosk")
    body = await resp.text()
    assert resp.status == 200
    assert "/kiosk/login" in body and 'name=password' in body
    assert "alice" not in body


async def test_dev_user_param_signs_in_and_redirects_to_the_bare_path(portal):
    """The dev auto-forward. The redirect matters as much as the cookies: the
    URL a kiosk browser is parked on must not carry an identity."""
    resp = await portal.get("/kiosk", params={"user": "alice"},
                            allow_redirects=False)
    assert resp.status == 303
    assert resp.headers["Location"] == "/kiosk"
    assert resp.cookies[kiosk.KIOSK_COOKIE].value == "1"
    assert resp.cookies["whistler_user"].value == "alice"


async def test_login_sets_cookies_and_redirects(portal):
    resp = await portal.post("/kiosk/login",
                             data={"user": "alice", "password": "anything"},
                             allow_redirects=False)
    assert resp.status == 303
    assert resp.headers["Location"] == "/kiosk"
    assert resp.cookies[kiosk.KIOSK_COOKIE].value == "1"


async def test_login_refuses_an_empty_user(portal):
    resp = await portal.post("/kiosk/login", data={"user": "", "password": "x"})
    assert resp.status == 401
    assert "Sign-in failed" in await resp.text()


async def test_login_is_refused_when_dev_auth_is_off(portal, monkeypatch):
    """There is no password store yet, so outside dev mode there is nothing a
    credential could be checked against and the answer must be no — never a
    default yes."""
    monkeypatch.delenv("WHISTLER_AUTH_ALLOW_ANY")
    assert kiosk._verify_credentials("alice", "hunter2") is False


async def test_signed_in_browser_gets_the_grid(portal):
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    body = await (await portal.get("/kiosk")).text()
    assert "/kiosk/sessions" in body
    assert "alice" in body


async def test_logout_clears_the_cookies_and_returns_to_the_form(portal):
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    resp = await portal.post("/kiosk/logout", allow_redirects=False)
    assert resp.status == 303
    portal.session.cookie_jar.update_cookies(resp.cookies)
    assert 'name=password' in await (await portal.get("/kiosk")).text()


async def test_a_stale_user_cookie_alone_is_not_a_kiosk_login(portal):
    """Someone who used the ordinary portal has a whistler_user cookie. That is
    an identity, not a kiosk sign-in, and must still meet the login screen."""
    portal.session.cookie_jar.update_cookies({"whistler_user": "alice"})
    assert 'name=password' in await (await portal.get("/kiosk")).text()


# --------------------------------------------------------------------------- #
# The grid                                                                     #
# --------------------------------------------------------------------------- #

async def test_sessions_json_is_the_users_own_desktops(portal):
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    rows = await (await portal.get("/kiosk/sessions")).json()
    assert [r["name"] for r in rows] == ["desk", "off"]
    assert [r["running"] for r in rows] == [True, False]
    assert [r["state"] for r in rows] == ["Running", "Stopped"]


async def test_sessions_json_never_spans_users(portal):
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    rows = await (await portal.get("/kiosk/sessions")).json()
    assert "bobs-box" not in [r["name"] for r in rows]


async def test_sessions_json_needs_a_kiosk_login(portal):
    """401, not the middleware's fallback identity — the grid script keys off
    this status to bounce back to the login screen."""
    assert (await portal.get("/kiosk/sessions")).status == 401


async def test_every_status_group_has_a_colour():
    """A new user-facing state must not render as a colourless dot; the kiosk
    reads whistler/status.py and this is the seam where the two could drift."""
    missing = set(STATUS_GROUPS.values()) - set(kiosk._STATE_COLORS)
    assert not missing, missing


async def test_ssh_sessions_are_not_offered(portal):
    """A card exists to open a desktop. An ssh-mode session has none, and the
    only thing the portal could offer instead is a web terminal — precisely the
    channel a kiosk-bound user is not supposed to have."""
    called = []
    portal.cm.get_user_instances = lambda u: called.append(u) or []
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    await portal.get("/kiosk/sessions")
    assert called == []


# --------------------------------------------------------------------------- #
# The session page                                                             #
# --------------------------------------------------------------------------- #

async def test_session_page_frames_connect_for_an_owned_session(portal):
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    body = await (await portal.get("/kiosk/session/desk")).text()
    assert 'src="/connect/desk"' in body
    assert 'href="/kiosk"' in body          # a way back that is not the idle timer


async def test_session_page_refuses_another_users_session(portal):
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    assert (await portal.get("/kiosk/session/bobs-box")).status == 404


async def test_session_page_refuses_an_unknown_name(portal):
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    assert (await portal.get("/kiosk/session/nope")).status == 404


async def test_session_page_without_a_login_goes_to_the_kiosk(portal):
    resp = await portal.get("/kiosk/session/desk", allow_redirects=False)
    assert resp.status == 303
    assert resp.headers["Location"] == "/kiosk"


# --------------------------------------------------------------------------- #
# The idle timer                                                               #
# --------------------------------------------------------------------------- #

async def test_idle_timeout_is_baked_into_both_pages(portal, monkeypatch):
    monkeypatch.setenv("WHISTLER_KIOSK_IDLE_TIMEOUT", "60")
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    for path in ("/kiosk", "/kiosk/session/desk"):
        assert "IDLE_MS = 60000" in await (await portal.get(path)).text(), path


async def test_idle_timeout_can_be_disabled(portal, monkeypatch):
    monkeypatch.setenv("WHISTLER_KIOSK_IDLE_TIMEOUT", "0")
    await portal.post("/kiosk/login", data={"user": "alice", "password": "x"})
    assert "IDLE_MS = 0" in await (await portal.get("/kiosk/session/desk")).text()


async def test_a_nonsense_idle_timeout_falls_back_to_the_default(monkeypatch):
    """A typo in an env var must not disable the timer — that is the one
    failure mode of this setting that leaves a desktop on screen forever."""
    monkeypatch.setenv("WHISTLER_KIOSK_IDLE_TIMEOUT", "fifteen")
    assert kiosk._idle_seconds() == kiosk._DEFAULT_IDLE_SECONDS


# --------------------------------------------------------------------------- #
# The lock                                                                     #
#                                                                              #
# The thin client decides the person has gone and navigates to /kiosk/lock.    #
# What makes that a lock rather than a screensaver is that the identity cookie #
# stops being sufficient: these tests are mostly about what a locked browser   #
# is refused, not about what the lock screen looks like.                       #
# --------------------------------------------------------------------------- #

async def _signed_in(portal, user="alice"):
    await portal.post("/kiosk/login", data={"user": user, "password": "x"})


async def _locked(portal, next_="/kiosk/session/desk"):
    await _signed_in(portal)
    await portal.get("/kiosk/lock", params={"next": next_})


async def test_lock_consumes_next_into_a_cookie_and_parks_on_a_bare_url(portal):
    """The parked URL must not say which session is behind the lock, and a
    reload must not be able to change where unlocking goes."""
    await _signed_in(portal)
    resp = await portal.get("/kiosk/lock", params={"next": "/kiosk/session/desk"},
                            allow_redirects=False)
    assert resp.status == 303
    assert resp.headers["Location"] == "/kiosk/lock"
    assert resp.cookies[kiosk.LOCK_COOKIE].value == "/kiosk/session/desk"


async def test_the_lock_cookie_is_httponly(portal):
    """The page it guards must not be able to clear it — that is the whole
    difference between this and a ?locked=1 parameter."""
    await _signed_in(portal)
    resp = await portal.get("/kiosk/lock", params={"next": "/kiosk"},
                            allow_redirects=False)
    assert resp.cookies[kiosk.LOCK_COOKIE]["httponly"]


async def test_lock_screen_names_the_locked_user_and_offers_no_username_field(portal):
    await _locked(portal)
    body = await (await portal.get("/kiosk/lock")).text()
    assert "Locked" in body and "alice" in body
    assert "name=user" not in body          # unlocking is not a login
    assert "/kiosk/logout" in body          # ...but a different person can escape


async def test_locked_browser_cannot_reach_the_grid(portal):
    await _locked(portal)
    resp = await portal.get("/kiosk", headers={"Accept": "text/html"},
                            allow_redirects=False)
    assert resp.status == 303
    assert resp.headers["Location"] == "/kiosk/lock"


async def test_locked_browser_cannot_reach_a_session_page(portal):
    """The bypass this exists to close: the identity cookie is still valid, so
    without the lock cookie check this URL would serve a desktop."""
    await _locked(portal)
    resp = await portal.get("/kiosk/session/desk", headers={"Accept": "text/html"},
                            allow_redirects=False)
    assert resp.status == 303
    assert resp.headers["Location"] == "/kiosk/lock"


async def test_locked_browser_cannot_reach_connect_or_the_desktop_relay(portal):
    """The lock has to cover the viewer app, not just the kiosk's own pages —
    /connect and /desktop are where the pixels actually are."""
    await _locked(portal)
    for path in ("/connect/desk", "/desktop/desk/", "/vnc/desk", "/term/desk"):
        resp = await portal.get(path, headers={"Accept": "text/html"},
                                allow_redirects=False)
        assert resp.status == 303, path
        assert resp.headers["Location"] == "/kiosk/lock", path


async def test_locked_browser_cannot_fetch_a_screenshot(portal):
    """An <img> is not a navigation, so it gets 423 rather than a redirect into
    an HTML page — but it is refused either way."""
    await _locked(portal)
    resp = await portal.get("/screenshot/desk", headers={"Accept": "image/png"})
    assert resp.status == 423


async def test_locked_sessions_json_is_refused(portal):
    await _locked(portal)
    resp = await portal.get("/kiosk/sessions", headers={"Accept": "application/json"})
    assert resp.status == 423


async def test_a_websocket_style_request_is_refused_rather_than_redirected(portal):
    await _locked(portal)
    resp = await portal.get("/ws-vnc/v1", headers={"Sec-Fetch-Mode": "websocket"})
    assert resp.status == 423


async def test_static_assets_and_healthz_survive_the_lock(portal):
    """The lock screen is made of the former; the latter belongs to the kubelet
    and locking a screen must not take a pod out of service."""
    await _locked(portal)
    assert (await portal.get("/healthz")).status == 200
    assert (await portal.get("/static/style.css")).status == 200


async def test_unlocking_returns_to_where_the_lock_came_from(portal):
    await _locked(portal, "/kiosk/session/desk")
    resp = await portal.post("/kiosk/login", data={"password": "x"},
                             allow_redirects=False)
    assert resp.status == 303
    assert resp.headers["Location"] == "/kiosk/session/desk"


async def test_unlocking_clears_the_lock(portal):
    await _locked(portal)
    await portal.post("/kiosk/login", data={"password": "x"})
    assert (await portal.get("/kiosk/session/desk")).status == 200


async def test_a_failed_unlock_leaves_the_lock_in_place(portal, monkeypatch):
    """The one thing a wrong password must never do."""
    await _locked(portal)
    monkeypatch.setattr(kiosk, "_verify_credentials", lambda u, p: False)
    resp = await portal.post("/kiosk/login", data={"password": "nope"})
    assert resp.status == 401
    assert "Locked" in await resp.text()
    # No monkeypatch.undo() here: the portal fixture shares this function-scoped
    # monkeypatch, so undoing would also clear WHISTLER_AUTH_ALLOW_ANY and the
    # 401 below would be the auth gate rather than the lock. The patched
    # verifier is not consulted on this path anyway — lock_middleware refuses it
    # before any handler runs, which is the point being asserted.
    resp = await portal.get("/kiosk/session/desk", headers={"Accept": "text/html"},
                            allow_redirects=False)
    assert resp.status == 303
    assert resp.headers["Location"] == "/kiosk/lock"


async def test_unlocking_cannot_switch_user(portal):
    """Unlocking returns one person to their own screen. A username in the form
    is ignored — becoming someone else is Sign out, which ends the session."""
    await _locked(portal, "/kiosk")
    await portal.post("/kiosk/login", data={"user": "bob", "password": "x"})
    rows = await (await portal.get("/kiosk/sessions")).json()
    assert [r["name"] for r in rows] == ["desk", "off"]      # still alice's


async def test_signing_out_from_the_lock_screen_clears_everything(portal):
    await _locked(portal)
    await portal.post("/kiosk/logout")
    body = await (await portal.get("/kiosk")).text()
    assert "name=password" in body and "Locked" not in body


async def test_locking_without_a_session_just_shows_the_login(portal):
    resp = await portal.get("/kiosk/lock", params={"next": "/kiosk"},
                            allow_redirects=False)
    assert resp.status == 303
    assert resp.headers["Location"] == "/kiosk"


async def test_lock_without_next_still_locks(portal):
    """"Show me the lock screen" must never mean "and stay unlocked"."""
    await _signed_in(portal)
    resp = await portal.get("/kiosk/lock", allow_redirects=False)
    assert resp.status == 303
    assert resp.cookies[kiosk.LOCK_COOKIE].value == "/kiosk"


@pytest.mark.parametrize("given", [
    "https://evil.example/steal",     # absolute URL
    "//evil.example/steal",           # protocol-relative
    "/admin/users",                   # a real path, but not the kiosk's
    "/kiosk/lock",                    # would loop
    "/kiosk\r\nSet-Cookie: x=1",      # header splitting
    "x" * 600,                        # absurd
    "",
    None,
])
def test_next_that_is_not_a_kiosk_path_falls_back_to_the_grid(given):
    """?next= is attacker-supplied in the sense that matters — it is whatever
    landed in a URL. Only a kiosk path survives, which also disposes of open
    redirects without a separate check for them."""
    assert kiosk._safe_return(given) == "/kiosk"


@pytest.mark.parametrize("given", ["/kiosk", "/kiosk/session/desk",
                                   "/kiosk/session/a-b_c"])
def test_a_kiosk_path_is_kept(given):
    assert kiosk._safe_return(given) == given
