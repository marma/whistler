"""The kiosk binding: which door a user may come in through.

This is the identity half of design/security.md's "kiosk situation" — the half
Whistler can actually promise. The document names its own failure mode: *a
grant missed at one entry point, the classic being the SSH gateway, which is a
different door on a different port from the portal*. So these tests are
organised by door, and the point of them is that all three are shut for the
same user.
"""
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer
from starlette.requests import Request

from whistler.config import ENTRY_GATEWAY, ENTRY_KIOSK, ENTRY_POINTS, ENTRY_PORTAL
from whistler.portal.app import _required_entry_point, build_app
from whistler.portal.management import (EntryPointDenied, _entry_point_denied,
                                        login_submit, require_user)
from whistler.server import SSHServer

import asyncssh


# --------------------------------------------------------------------------- #
# The rule                                                                     #
# --------------------------------------------------------------------------- #

def test_no_list_means_no_door(make_config):
    """Every allow is explicit (2026-08-25). This used to be the opposite —
    an empty list meant every door — which made the User CR nobody had
    configured the one that could go anywhere. The portal and the operator
    seed the grant on accounts they create, so this state is now something
    somebody wrote, not something somebody forgot."""
    cm = make_config(users={"alice": {"name": "alice"}})
    assert cm.get_user_entry_points("alice") == []
    for entry in ENTRY_POINTS:
        assert cm.may_enter("alice", entry) is False


def test_a_list_bounds_the_user(make_config):
    cm = make_config(users={"alice": {"name": "alice", "entryPoints": ["kiosk"]}})
    assert cm.may_enter("alice", ENTRY_KIOSK) is True
    assert cm.may_enter("alice", ENTRY_PORTAL) is False
    assert cm.may_enter("alice", ENTRY_GATEWAY) is False


def test_an_unknown_user_holds_no_grant_and_so_no_door(make_config):
    """A name with no User CR has been granted nothing, and nothing is what it
    gets. Authentication is still a separate question that refuses the same
    person earlier; this is the answer for the paths that get past it — the
    dev `?user=` shortcut above all, which is an identity shortcut and never
    was a grant."""
    cm = make_config(users={})
    assert cm.may_enter("nobody", ENTRY_PORTAL) is False


def test_a_group_widens_a_bound_user(make_config):
    """Deliberate, and the one thing to know when binding somebody: entry
    points compose like allowedZones, so a group grant is a grant."""
    cm = make_config(
        users={"alice": {"name": "alice", "entryPoints": ["kiosk"]}},
        groups={"staff": {"members": ["alice"], "entryPoints": ["portal"]}})
    assert cm.get_user_entry_points("alice") == ["kiosk", "portal"]
    assert cm.may_enter("alice", ENTRY_PORTAL) is True
    assert cm.may_enter("alice", ENTRY_GATEWAY) is False


def test_a_group_can_bound_a_user_who_had_no_list(make_config):
    cm = make_config(
        users={"alice": {"name": "alice"}},
        groups={"kiosk-users": {"members": ["alice"], "entryPoints": ["kiosk"]}})
    assert cm.may_enter("alice", ENTRY_KIOSK) is True
    assert cm.may_enter("alice", ENTRY_GATEWAY) is False


def test_setting_the_grant_orders_it_canonically(make_config):
    cm = make_config(users={"alice": {"name": "alice"}})
    cm.set_user_entry_points("alice", ["gateway", "kiosk"])
    assert cm.get_user_entry_points("alice") == ["gateway", "kiosk"]
    # Clearing it is a real lockout now, not a reset to "everything".
    cm.set_user_entry_points("alice", [])
    assert cm.may_enter("alice", ENTRY_PORTAL) is False


# --------------------------------------------------------------------------- #
# Door 1: the SSH gateway                                                      #
# --------------------------------------------------------------------------- #

def _keypair():
    key = asyncssh.generate_private_key("ssh-ed25519")
    return key, key.export_public_key().decode().strip() + " user@host"


def test_a_kiosk_bound_user_cannot_authenticate_to_the_gateway(make_config):
    """Refused at auth, not at a channel: the launcher, the relay and the jump
    are three doors on one port, and a check at any one of them is a check the
    other two could outlive."""
    key, line = _keypair()
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": [line],
                                      "entryPoints": ["kiosk"]}})
    srv = SSHServer(config_manager=cm)
    assert srv.validate_public_key("alice", key) is False
    # And nothing was resolved on the way out.
    assert srv.username is None


def test_the_gateway_grant_lets_the_same_user_in(make_config):
    key, line = _keypair()
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": [line],
                                      "entryPoints": ["kiosk", "gateway"]}})
    srv = SSHServer(config_manager=cm)
    assert srv.validate_public_key("alice", key) is True
    assert srv.username == "alice"


def test_a_user_granted_no_door_cannot_authenticate_either(make_config):
    """The same refusal for the account nobody configured. A valid key is not
    the question the gateway asks last."""
    key, line = _keypair()
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": [line]}})
    srv = SSHServer(config_manager=cm)
    assert srv.validate_public_key("alice", key) is False
    assert srv.username is None


def test_dev_mode_does_not_open_the_gateway_to_a_bound_user(monkeypatch, make_config):
    """WHISTLER_AUTH_ALLOW_ANY skips the *key* check. A binding is not a
    credential, so it still holds — otherwise the one flag every dev cluster
    sets would quietly undo the control."""
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")
    key, _line = _keypair()
    cm = make_config(users={"alice": {"name": "alice", "entryPoints": ["kiosk"]}})
    srv = SSHServer(config_manager=cm)
    assert srv.validate_public_key("alice", key) is False
    assert srv.validate_password("alice", "anything") is False


# --------------------------------------------------------------------------- #
# Door 2: the viewer app (both surfaces live on it, so it is a path question)   #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("path,expected", [
    ("/kiosk", ENTRY_KIOSK),
    # Signing in and out of the kiosk is always reachable: a browser holding a
    # cookie for an account the surface now refuses has to be able to sign out,
    # or nobody else can use that machine.
    ("/kiosk/login", None),
    ("/kiosk/logout", None),
    ("/kiosk/session/desk", ENTRY_KIOSK),
    ("/kiosk/lock", ENTRY_KIOSK),
    ("/", ENTRY_PORTAL),
    ("/launch", ENTRY_PORTAL),
    ("/term/desk", ENTRY_PORTAL),
    ("/ws-term/desk", ENTRY_PORTAL),
    ("/console/desk", ENTRY_PORTAL),
    ("/ws-console/desk", ENTRY_PORTAL),
    ("/something-added-later", ENTRY_PORTAL),   # fail-closed by default
    # The desktop itself: both surfaces end up here, so it carries no entry
    # point of its own — which is also what keeps the User CR read off the
    # Selkies asset and WebSocket paths.
    ("/connect/desk", None),
    ("/desktop/desk/", None),
    ("/vnc/desk", None),
    ("/ws-vnc/desk", None),
    ("/status/desk", None),
    ("/screenshot/desk", None),
    ("/static/style.css", None),
    ("/healthz", None),
])
def test_which_paths_need_which_entry_point(path, expected):
    assert _required_entry_point(path) == expected


class _CM:
    def __init__(self, entry_points=None, sessions=None):
        self.entry_points = entry_points or {}
        self.sessions = sessions or {}

    def get_user_entry_points(self, username):
        return list(self.entry_points.get(username, []))

    def may_enter(self, username, entry_point):
        return entry_point in self.get_user_entry_points(username)

    def get_user_desktop_sessions(self, username):
        return self.sessions.get(username, [])

    def get_user_instances(self, username):
        return []

    def get_user_templates(self, username):
        return []

    def get_user_desktop_templates(self, username):
        return []

    def is_user_admin(self, username):
        return False


@pytest.fixture
async def viewer(monkeypatch):
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")
    monkeypatch.setenv("WHISTLER_SCREENSHOT_INTERVAL", "0")
    cm = _CM(entry_points={"kiosky": ["kiosk"], "worker": ["portal", "gateway"],
                           "everyone": list(ENTRY_POINTS)})
    client = TestClient(TestServer(build_app(cm)))
    await client.start_server()
    yield client
    await client.close()


_NAV = {"Sec-Fetch-Mode": "navigate", "Accept": "text/html"}


@pytest.mark.parametrize("path", ["/", "/term/desk", "/console/desk"])
async def test_a_kiosk_bound_user_is_refused_the_portal_pages(viewer, path):
    resp = await viewer.get(f"{path}?user=kiosky", headers=_NAV)
    body = await resp.text()
    assert resp.status == 403
    # A page with a way out, not a redirect: the other surface is only
    # same-origin behind the bundled proxy.
    assert "Kiosk only" in body and 'href="/kiosk"' in body


async def test_a_kiosk_bound_user_still_reaches_their_desktop(viewer):
    """The binding must not break the surface it binds someone to. /connect is
    what the kiosk session page frames."""
    resp = await viewer.get("/kiosk?user=kiosky", headers=_NAV)
    assert resp.status == 200
    for path in ("/connect/desk", "/screenshot/desk", "/status/desk"):
        resp = await viewer.get(f"{path}?user=kiosky")
        assert resp.status != 403, path


async def test_a_non_kiosk_user_is_refused_the_kiosk(viewer):
    resp = await viewer.get("/kiosk?user=worker", headers=_NAV)
    body = await resp.text()
    assert resp.status == 403
    assert "Not a kiosk account" in body and 'href="/"' in body


async def test_a_stale_portal_cookie_still_gets_the_kiosk_login_form(viewer):
    """The trap this avoids: on a shared machine the auth middleware's cookie
    fallback would name whoever used the portal there last, and the kiosk would
    refuse its own login screen to the person standing in front of it."""
    resp = await viewer.get("/kiosk", headers=_NAV,
                            cookies={"whistler_user": "worker"})
    body = await resp.text()
    assert resp.status == 200
    assert "/kiosk/login" in body and "Not a kiosk account" not in body


async def test_the_kiosk_form_refuses_a_non_kiosk_account(viewer):
    """The middleware is the boundary; the form saying so is the difference
    between an explanation and a bounce off the grid."""
    resp = await viewer.post("/kiosk/login",
                             data={"user": "worker", "password": "x"})
    assert resp.status == 403
    assert "cannot use the kiosk" in await resp.text()
    # And no identity was handed out on the way.
    assert not resp.cookies.get("whistler_kiosk")


async def test_a_fetch_gets_a_status_not_a_page(viewer):
    """An htmx poll or a WebSocket handshake cannot render an explanation, and
    a 403 body of HTML would only produce a parse error at the far end."""
    resp = await viewer.get("/term-status/desk?user=kiosky",
                            headers={"Accept": "*/*"})
    assert resp.status == 403
    assert "Kiosk only" not in await resp.text()


async def test_a_user_granted_both_doors_reaches_both(viewer):
    for path in ("/", "/kiosk"):
        resp = await viewer.get(f"{path}?user=everyone", headers=_NAV)
        assert resp.status == 200


async def test_a_user_granted_nothing_reaches_neither(viewer):
    """The other half of the same rule. Before 2026-08-25 this account — the
    one nobody has configured — was the one that could open both surfaces."""
    for path in ("/", "/kiosk"):
        resp = await viewer.get(f"{path}?user=ungranted", headers=_NAV)
        assert resp.status == 403


# --------------------------------------------------------------------------- #
# Door 3: the management portal                                                 #
# --------------------------------------------------------------------------- #

def _mgmt_request(user="alice", entry_points=None, headers=None, path="/"):
    cm = _CM(entry_points=entry_points or {})

    async def run(func, *args):
        return func(*args)

    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request({"type": "http", "http_version": "1.1", "method": "GET",
                    "scheme": "http", "server": ("portal", 80), "root_path": "",
                    "path": path, "raw_path": path.encode(),
                    "query_string": f"user={user}".encode(), "headers": raw,
                    "app": SimpleNamespace(
                        state=SimpleNamespace(cm=cm, run=run))})


def test_require_user_refuses_a_kiosk_bound_user(monkeypatch):
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")
    with pytest.raises(EntryPointDenied):
        require_user(_mgmt_request(entry_points={"alice": ["kiosk"]}))
    # The check is on the whole surface, so the dev ?user= shortcut does not
    # get around it either — it is an identity shortcut, not a grant.
    assert require_user(_mgmt_request(entry_points={"alice": ["portal"]})) == "alice"


async def test_the_refusal_names_the_kiosk(monkeypatch):
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")
    response = await _entry_point_denied(_mgmt_request(headers=_NAV),
                                         EntryPointDenied())
    assert response.status_code == 403
    body = response.body.decode()
    assert "Kiosk only" in body and "/kiosk" in body
    assert response.headers["cache-control"] == "no-store"


async def test_the_login_form_refuses_a_kiosk_bound_user_after_a_good_password(
        monkeypatch):
    """The password is right and the door is still wrong. Nothing is signed in:
    a cookie for a surface this account cannot use would only produce the same
    refusal one navigation later."""
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")
    request = _mgmt_request(entry_points={"alice": ["kiosk"]})
    response = await login_submit(request, user="alice", password="x")
    assert response.status_code == 403
    assert "Kiosk only" in response.body.decode()
    assert not response.headers.getlist("set-cookie")
