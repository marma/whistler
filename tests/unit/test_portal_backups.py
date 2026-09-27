"""The portal's side of backups (whistler/portal/backups.py and the Backups
routes in management.py; design/backup.md Phase 4).

The client is tested against the real backup service app (with the fake
cluster behind it) over a real socket, so both ends of the wire are the code
that ships. The offer middleware is driven with genuine Starlette requests.
"""
import datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from aiohttp.test_utils import TestServer
from starlette.requests import Request
from starlette.responses import PlainTextResponse

from whistler.backup import archive
from whistler.backup.service import BackupService, make_app
from whistler.backup.store import Store
from whistler.portal import management as mgmt
from whistler.portal.backups import (BackupClient, BackupRefused,
                                     BackupUnavailable, in_cluster_storage)

from backup_fakes import FakeCM, _populated
from test_backup_service import Clock, FakeState, at

PORTAL = "system:serviceaccount:whistler:whistler-portal"


@pytest.fixture
async def live(tmp_path, monkeypatch):
    """A real backup service on a socket, and a portal client pointed at it."""
    monkeypatch.setenv("WHISTLER_HOST_KEY_SECRET_NAME",
                       "whistler-server-host-key")
    monkeypatch.delenv("WHISTLER_BACKUP_CLAIM", raising=False)
    cluster = _populated()
    state = FakeState()
    service = BackupService(FakeCM(cluster), Store(tmp_path), state,
                            Clock(at(27, 4)))

    async def auth(token):
        return {"portal-token": PORTAL}.get(token)
    server = TestServer(make_app(service, auth))
    with patch("kubernetes.client.CoreV1Api", return_value=cluster):
        await server.start_server()
        client = BackupClient(str(server.make_url("")).rstrip("/"),
                              token="portal-token")
        yield SimpleNamespace(client=client, service=service, state=state,
                              cluster=cluster, server=server)
        await server.close()


# --- the client, end to end ------------------------------------------------------- #

async def test_back_up_list_download_restore_through_the_client(live):
    c = live.client
    made = await c.create(by="root")
    name = made["backup"]["file"]
    assert [b["file"] for b in await c.list()] == [name]
    data, filename = await c.download(name)
    assert filename == name and archive.read(data).objects
    del live.cluster.crs[("users", "whistler", "alice")]
    preview = await c.preview(name)
    assert preview["counts"].get("create") == 1
    result = await c.restore(name, by="root")
    assert result["written"] == 1
    assert ("users", "whistler", "alice") in live.cluster.crs
    assert "root (via " + PORTAL + ")" == live.state.recorded["decidedBy"]


async def test_a_refusal_carries_the_services_reason(live):
    with pytest.raises(BackupRefused, match="mode"):
        await live.client.save_settings({"mode": "hourly"})
    with pytest.raises(BackupRefused):
        await live.client.upload(b"not a backup")


async def test_a_token_the_service_refuses_reads_as_unavailable(live):
    stranger = BackupClient(live.client.url, token="someone-else")
    with pytest.raises(BackupUnavailable, match="ALLOWED_SERVICEACCOUNTS"):
        await stranger.status()


async def test_an_unreachable_service_is_unavailable_not_an_error():
    client = BackupClient("http://127.0.0.1:9", token="x")
    with pytest.raises(BackupUnavailable, match="not reachable"):
        await client.status()


async def test_backups_off_means_a_disabled_client():
    client = BackupClient("", token="x")
    assert not client.enabled
    with pytest.raises(BackupUnavailable):
        await client.status()


def test_the_token_is_read_from_the_pod_each_time(tmp_path):
    token = tmp_path / "token"
    token.write_text("first\n")
    client = BackupClient("http://x", token_path=str(token))
    assert client._headers() == {"Authorization": "Bearer first"}
    token.write_text("rotated")
    assert client._headers() == {"Authorization": "Bearer rotated"}


@pytest.mark.parametrize("driver,inside", [
    ("local", True), ("rancher.io/local-path", True),
    ("driver.longhorn.io", True), ("rook-ceph.rbd.csi.ceph.com", True),
    ("nfs.csi.k8s.io", False), ("nfs", False), ("ebs.csi.aws.com", False),
    (None, False)])
def test_in_cluster_storage_is_flagged(driver, inside):
    assert in_cluster_storage(driver) is inside


# --- the first-login offer ---------------------------------------------------------- #

class FakeBackups:
    enabled = True

    def __init__(self, offer=True, install="install-B", fail=False):
        self.offer, self.install, self.fail = offer, install, fail
        self.calls = 0

    async def status(self):
        self.calls += 1
        if self.fail:
            raise BackupUnavailable("down")
        return {"offer": self.offer, "install": {"installId": self.install},
                "offers": [], "stale": None}


def _app(backups, admins=("root",)):
    cm = SimpleNamespace(is_user_admin=lambda u: u in admins,
                         may_enter=lambda u, e: True)

    async def run(fn, *args):
        return fn(*args)
    return SimpleNamespace(state=SimpleNamespace(
        backups=backups, offer_cache=None, cm=cm, run=run))


def _request(app, path="/", user="root", cookies="", navigation=True,
             method="GET", htmx=False):
    headers = []
    if navigation:
        headers.append((b"sec-fetch-mode", b"navigate"))
        headers.append((b"accept", b"text/html"))
    if cookies:
        headers.append((b"cookie", cookies.encode()))
    if htmx:
        headers.append((b"hx-request", b"true"))
    return Request({"type": "http", "method": method, "path": path,
                    "headers": headers, "app": app,
                    "query_string": f"user={user}".encode() if user else b""})


async def _passes(request):
    return PlainTextResponse("passed")


@pytest.fixture(autouse=True)
def dev_identity(monkeypatch):
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")


async def test_an_admins_navigation_lands_on_the_offer():
    app = _app(FakeBackups())
    resp = await mgmt.restore_offer_middleware(
        _request(app, "/admin/users"), _passes)
    assert resp.status_code == 303
    assert resp.headers["location"].startswith("/admin/backups/offer?")
    assert "next=%2Fadmin%2Fusers" in resp.headers["location"]


@pytest.mark.parametrize("kw", [
    {"user": "alice"},                              # not an admin
    {"navigation": False},                          # a fetch
    {"htmx": True},                                 # an htmx swap
    {"method": "POST"},
    {"path": "/admin/backups"},                     # the backup pages
    {"path": "/admin/backups/file/x/restore"},
    {"path": "/static/style.css"},
    {"path": "/login"},
    {"cookies": f"{mgmt.OFFER_DISMISSED_COOKIE}=install-B"},   # not now
])
async def test_everything_else_passes_straight_through(kw):
    resp = await mgmt.restore_offer_middleware(
        _request(_app(FakeBackups()), **kw), _passes)
    assert resp.body == b"passed"


async def test_not_now_for_an_earlier_install_does_not_hide_this_ones():
    app = _app(FakeBackups(install="install-C"))
    resp = await mgmt.restore_offer_middleware(
        _request(app, cookies=f"{mgmt.OFFER_DISMISSED_COOKIE}=install-B"),
        _passes)
    assert resp.status_code == 303


async def test_a_settled_install_is_not_redirected():
    resp = await mgmt.restore_offer_middleware(
        _request(_app(FakeBackups(offer=False))), _passes)
    assert resp.body == b"passed"


async def test_a_down_backup_service_never_blocks_the_portal():
    resp = await mgmt.restore_offer_middleware(
        _request(_app(FakeBackups(fail=True))), _passes)
    assert resp.body == b"passed"


async def test_the_answer_is_cached_between_navigations():
    backups = FakeBackups(offer=False)
    app = _app(backups)
    for _ in range(3):
        await mgmt.restore_offer_middleware(_request(app), _passes)
    assert backups.calls == 1


# --- the pages ------------------------------------------------------------------------ #

def _render(template, **context):
    request = SimpleNamespace(url=SimpleNamespace(path="/admin/backups"))
    return mgmt.templates.env.get_template(template).render(
        request=request, current_user="root", is_admin=True, **context)


STATUS = {
    "volume": {"capacityBytes": 2 ** 30, "usedBytes": 2 ** 28,
               "persistentVolume": "pv-1", "storageClassName": "local-path",
               "driver": "local", "reclaimPolicy": "Retain"},
    "install": {"installId": "install-B", "decision": "declined",
                "decidedBy": "root"},
    "offer": False, "offers": [], "paused": False,
    "settings": {"mode": "daily", "hours": 24, "at": "03:00", "retain": 14},
    "nextRun": "2026-09-28T03:00:00Z", "lastSuccess": None, "lastError": None,
    "warnings": [], "stale": None, "passphraseSet": False, "backups": 1}
BACKUP = {"file": "whistler-backup-20260927-030000-scheduled.tar.gz",
          "size": 2048, "createdAt": "2026-09-27T03:00:00Z",
          "trigger": "scheduled", "installId": "install-A",
          "counts": {"User": 2}, "secrets": "plain", "problem": None}


def test_the_backups_page_renders_with_everything():
    html = _render("admin/backups.html", enabled=True, status=STATUS,
                   backups=[BACKUP], in_cluster=True, unavailable=None,
                   hint=None, notice="Saved.", error=None)
    assert BACKUP["file"] in html
    assert "/restore?user=root" in html and "/download?user=root" in html
    assert "inside the cluster" in html            # local-path warning
    assert "not encrypted" in html                 # no passphrase
    assert "earlier" in html                       # install-A != install-B


def test_the_backups_page_says_when_the_service_is_down():
    html = _render("admin/backups.html", enabled=True, status=None,
                   backups=[], in_cluster=False,
                   unavailable="not reachable", hint="claim is Pending",
                   notice=None, error=None)
    assert "not reachable" in html and "claim is Pending" in html


def test_the_backups_page_when_backups_are_off():
    html = _render("admin/backups.html", enabled=False, status=None,
                   backups=[], in_cluster=False, unavailable=None, hint=None,
                   notice=None, error=None)
    assert "turned off" in html


def test_the_restore_page_previews_and_then_reports():
    preview = {"counts": {"create": 1, "unchanged": 3},
               "entries": [{"action": "create", "kind": "User",
                            "namespace": "whistler", "name": "alice",
                            "reason": None}],
               "warnings": [], "secrets": "encrypted", "secretsReadable": False}
    html = _render("admin/backup_restore.html", name=BACKUP["file"],
                   preview=preview, result=None, error=None,
                   include_secrets=True)
    assert "alice" in html and "passphrase this install does" in html
    result = {"written": 1, "pruned": ["HomeVolume a/b: spec.pvName"],
              "prunedHint": "apply the CRDs", "warnings": [],
              "preRestoreBackup": "whistler-backup-x-pre-restore.tar.gz"}
    html = _render("admin/backup_restore.html", name=BACKUP["file"],
                   preview=None, result=result, error=None,
                   include_secrets=True)
    assert "spec.pvName" in html and "pre-restore" in html


def test_the_offer_page_offers_the_newest_and_the_choices():
    status = {**STATUS, "offer": True,
              "install": {"installId": "install-B", "decision": "pending"}}
    html = _render("admin/backup_offer.html", status=status,
                   offers=[BACKUP, {**BACKUP, "file": "older.tar.gz"}],
                   next_to="/admin")
    assert f"/admin/backups/file/{BACKUP['file']}/restore" in html
    assert "/admin/backups/decline" in html
    assert 'value="install-B"' in html              # "not now" is per install
    assert "older.tar.gz" in html


# --- routes ------------------------------------------------------------------------------ #

class RouteBackups:
    enabled = True

    def __init__(self):
        self.passphrases = []

    async def set_passphrase(self, value):
        self.passphrases.append(value)


def _route_request(backups):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        backups=backups, offer_cache=None)))


async def test_a_mistyped_passphrase_is_not_saved():
    backups = RouteBackups()
    resp = await mgmt.admin_backup_passphrase(
        _route_request(backups), "root", action="set", passphrase="one",
        confirm="two")
    assert "error=" in resp.headers["location"] and backups.passphrases == []
    await mgmt.admin_backup_passphrase(
        _route_request(backups), "root", action="set", passphrase="one",
        confirm="one")
    await mgmt.admin_backup_passphrase(_route_request(backups), "root",
                                       action="clear")
    assert backups.passphrases == ["one", None]


async def test_a_download_of_a_non_backup_name_is_a_404():
    with pytest.raises(mgmt.HTTPException) as err:
        await mgmt.admin_backup_download(_route_request(RouteBackups()),
                                         "root", "../../etc/passwd")
    assert err.value.status_code == 404


async def test_dismissing_the_offer_is_a_session_cookie_for_this_install():
    resp = await mgmt.admin_backup_offer_dismiss(
        _route_request(RouteBackups()), "root", install_id="install-B",
        next_to="/admin")
    cookie = resp.headers["set-cookie"]
    assert f"{mgmt.OFFER_DISMISSED_COOKIE}=install-B" in cookie
    assert "max-age" not in cookie.lower() and "expires" not in cookie.lower()
    assert "httponly" in cookie.lower()
