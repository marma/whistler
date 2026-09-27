"""The backup service (whistler/backup/service.py, schedule.py, store.py;
design/backup.md Phase 3).

The rules that make repeated reinstalls harmless are the ones tested hardest:
retention only ever touches this install's scheduled backups, and the
schedule pauses while another install's backup is on offer.
"""
import datetime
from unittest.mock import patch

import pytest
from aiohttp.test_utils import TestClient, TestServer

from whistler.backup import BackupError, archive, schedule
from whistler.backup.service import BackupService, TokenReviewAuth, make_app
from whistler.backup.store import Store

from backup_fakes import FakeCluster, FakeCM, _populated

UTC = datetime.timezone.utc


def at(day, hh=0, mm=0):
    return datetime.datetime(2026, 9, day, hh, mm, tzinfo=UTC)


# --- file names ------------------------------------------------------------------- #

def test_names_carry_when_and_why():
    name = schedule.filename_for(at(27, 3), "pre-restore")
    assert name == "whistler-backup-20260927-030000-pre-restore.tar.gz"
    assert schedule.parse_filename(name) == {"stamp": at(27, 3),
                                             "trigger": "pre-restore"}


def test_a_taken_name_gets_a_suffix():
    first = schedule.filename_for(at(27), "manual")
    second = schedule.filename_for(at(27), "manual", {first})
    assert second == "whistler-backup-20260927-000000-manual.2.tar.gz"
    assert schedule.parse_filename(second)["trigger"] == "manual"


@pytest.mark.parametrize("name", [
    "../whistler-backup-20260927-000000-manual.tar.gz",
    "whistler-backup-20260927-000000-manual.tar.gz/../../etc/passwd",
    "whistler-backup-20260927-000000-bogus.tar.gz",
    ".incoming-abc", "", "whistler-backup-20261399-000000-manual.tar.gz"])
def test_anything_else_is_not_a_backup_name(name):
    assert schedule.parse_filename(name) is None


# --- settings and the clock ---------------------------------------------------------- #

@pytest.mark.parametrize("raw", [{"mode": "hourly"}, {"hours": 0},
                                 {"retain": 0}, {"at": "25:00"},
                                 {"at": "3am"}, {"hours": "many"}])
def test_bad_settings_say_why(raw):
    with pytest.raises(ValueError):
        schedule.parse_settings(raw)


def test_daily_is_due_once_per_slot():
    s = schedule.parse_settings({"mode": "daily", "at": "03:00"})
    assert schedule.is_due(s, None, at(27, 12))                 # never ran
    assert not schedule.is_due(s, at(27, 3, 1), at(27, 12))     # ran today
    assert schedule.next_run(s, at(27, 3, 1), at(27, 12)) == at(28, 3)
    assert schedule.is_due(s, at(26, 3), at(27, 3, 0))          # today's slot
    assert not schedule.is_due(s, at(26, 3), at(27, 2, 59))     # not yet


def test_interval_counts_from_the_last_attempt():
    s = schedule.parse_settings({"mode": "interval", "hours": 6})
    assert schedule.next_run(s, at(27, 1), at(27, 2)) == at(27, 7)
    assert schedule.is_due(s, at(27, 1), at(27, 7))


def test_off_is_never_due():
    s = schedule.parse_settings({"mode": "off"})
    assert schedule.next_run(s, None, at(27)) is None


def test_a_quietly_stopped_schedule_is_flagged():
    s = schedule.parse_settings({"mode": "daily"})
    assert schedule.stale(s, at(20), False, at(27)) is not None
    assert schedule.stale(s, at(26, 3), False, at(27)) is None
    assert "failed" in schedule.stale(s, at(26, 3), True, at(27))
    assert schedule.stale(s, None, False, at(27)) is None


# --- retention and the offer ---------------------------------------------------------- #

def _e(name, day, trigger, install):
    return {"file": name, "createdAt": at(day), "trigger": trigger,
            "installId": install}


def test_retention_only_prunes_this_installs_scheduled_backups():
    entries = [_e(f"s{d}", d, "scheduled", "B") for d in range(1, 6)] + [
        _e("m1", 1, "manual", "B"), _e("u1", 1, "uninstall", "B"),
        _e("old", 1, "scheduled", "A"), _e("up", 1, "uploaded", "A")]
    assert sorted(schedule.prunable(entries, "B", 2)) == ["s1", "s2", "s3"]


def test_without_an_install_id_nothing_is_pruned():
    assert schedule.prunable([_e("s", 1, "scheduled", None)], None, 0) == []


def test_another_installs_backup_is_an_offer_and_pauses():
    entries = [_e("mine", 26, "scheduled", "B"), _e("theirs", 25, "manual", "A")]
    assert schedule.offer_state("pending", entries, "B") == "offer"
    assert [e["file"] for e in schedule.from_other_installs(entries, "B")] == \
        ["theirs"]


def test_only_my_own_backups_means_there_is_nothing_to_offer():
    entries = [_e("mine", 26, "scheduled", "B")]
    assert schedule.offer_state("pending", entries, "B") == "fresh"
    assert schedule.offer_state("declined", entries, "B") == "settled"


# --- the store ----------------------------------------------------------------------- #

@pytest.fixture
def backup_bytes(monkeypatch):
    monkeypatch.setenv("WHISTLER_HOST_KEY_SECRET_NAME",
                       "whistler-server-host-key")
    c = _populated()
    from whistler.backup.export import export
    with patch("kubernetes.client.CoreV1Api", return_value=c):
        return export(FakeCM(c), created=at(27), install_id="A")[0]


def test_store_saves_lists_reads_and_deletes(tmp_path, backup_bytes):
    store = Store(tmp_path)
    entry = store.save(backup_bytes, at(27), "manual")
    assert entry["installId"] == "A" and entry["trigger"] == "manual"
    assert [e["file"] for e in store.list()] == [entry["file"]]
    assert store.read(entry["file"]) == backup_bytes
    assert not list(tmp_path.glob(".incoming-*"))
    store.delete(entry["file"])
    assert store.list() == []


def test_store_will_not_touch_other_names(tmp_path):
    (tmp_path / "secret.txt").write_text("x")
    store = Store(tmp_path)
    for name in ("secret.txt", "../secret.txt"):
        with pytest.raises(BackupError):
            store.read(name)
        with pytest.raises(BackupError):
            store.delete(name)
    assert store.list() == []


def test_a_damaged_backup_is_listed_with_its_problem(tmp_path):
    (tmp_path / "whistler-backup-20260927-000000-manual.tar.gz").write_bytes(
        b"not a backup")
    [entry] = Store(tmp_path).list()
    assert entry["problem"]


# --- the service ------------------------------------------------------------------------ #

class FakeState:
    def __init__(self, install_id="B", decision="pending", settings=None):
        self._id = install_id
        self.recorded = None if decision == "pending" else {
            "installId": install_id, "decision": decision}
        self._settings = schedule.parse_settings(settings or {"mode": "daily"})
        self.pass_ = None

    def install_id(self):
        return self._id

    def decision(self, install_id):
        if self.recorded and self.recorded["installId"] == install_id:
            return self.recorded
        return {"installId": install_id, "decision": "pending"}

    def record_decision(self, install_id, decision, by, backup=None):
        self.recorded = {"installId": install_id, "decision": decision,
                         "decidedBy": by, "backup": backup}
        return self.recorded

    def settings(self):
        return self._settings

    def save_settings(self, raw):
        self._settings = schedule.parse_settings(raw)
        return self._settings

    def passphrase(self):
        return self.pass_

    def set_passphrase(self, p):
        self.pass_ = p


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setenv("WHISTLER_HOST_KEY_SECRET_NAME",
                       "whistler-server-host-key")
    monkeypatch.delenv("WHISTLER_BACKUP_CLAIM", raising=False)
    cluster = _populated()
    state = FakeState()
    clock = Clock(at(27, 4))
    service = BackupService(FakeCM(cluster), Store(tmp_path), state, clock)
    with patch("kubernetes.client.CoreV1Api", return_value=cluster):
        yield service, cluster, state, clock


def _files(service):
    return [e["file"] for e in service.store.list()]


async def test_a_fresh_install_with_nothing_to_offer_backs_up(world):
    service, _, state, _ = world
    assert await service.tick() == "backed-up"
    assert state.recorded["decision"] == "fresh"
    [entry] = service.store.list()
    assert entry["trigger"] == "scheduled" and entry["installId"] == "B"


async def test_the_schedule_pauses_while_another_install_is_on_offer(world,
                                                                     backup_bytes):
    service, _, state, _ = world
    service.store.save(backup_bytes, at(26), "uninstall")      # install A's
    assert await service.tick() == "paused"
    assert len(_files(service)) == 1
    status = await service.status()
    assert status["offer"] and status["paused"]
    assert [o["installId"] for o in status["offers"]] == ["A"]


async def test_unchanged_state_is_not_written_again(world):
    service, _, _, clock = world
    assert await service.tick() == "backed-up"
    clock.now = at(28, 4)
    assert await service.tick() == "unchanged"
    assert len(_files(service)) == 1
    assert await service.tick() == "idle"         # the attempt counts


async def test_retention_runs_after_each_scheduled_backup(world, backup_bytes):
    service, cluster, state, clock = world
    state._settings = schedule.parse_settings({"mode": "daily", "retain": 2})
    service.store.save(backup_bytes, at(1), "uninstall")       # install A's
    state.record_decision("B", "declined", "root")
    for day in (24, 25, 26, 27):
        clock.now = at(day, 4)
        cluster.put("zones", "whistler", f"z{day}", {})          # a change
        assert await service.tick() == "backed-up"
    kept = service.store.list()
    assert [e["trigger"] for e in kept].count("scheduled") == 2
    assert any(e["installId"] == "A" for e in kept)            # never pruned


async def test_restore_takes_a_pre_restore_backup_and_settles_the_offer(
        world, backup_bytes):
    service, cluster, state, _ = world
    entry = service.store.save(backup_bytes, at(26), "uninstall")
    del cluster.crs[("users", "whistler", "alice")]
    result = await service.restore(entry["file"], by="root")
    assert result["written"] == 1 and result["pruned"] == []
    assert result["preRestoreBackup"] in _files(service)
    assert state.recorded["decision"] == "restored"
    assert state.recorded["backup"] == entry["file"]
    assert ("users", "whistler", "alice") in cluster.crs


async def test_a_restore_with_nothing_to_do_takes_no_pre_restore_backup(
        world, backup_bytes):
    service, _, _, _ = world
    entry = service.store.save(backup_bytes, at(26), "manual")
    result = await service.restore(entry["file"], by="root")
    assert result["written"] == 0 and result["preRestoreBackup"] is None


async def test_an_upload_is_checked_before_it_is_kept(world, backup_bytes):
    service, _, _, _ = world
    with pytest.raises(BackupError):
        await service.upload(b"garbage")
    entry = await service.upload(backup_bytes)
    assert entry["trigger"] == "uploaded" and entry["installId"] == "A"


async def test_declining_is_recorded_and_restored_is_not_settable(world):
    service, _, state, _ = world
    await service.decide("declined", "root")
    assert state.recorded["decision"] == "declined"
    with pytest.raises(BackupError):
        await service.decide("restored", "root")


# --- HTTP ------------------------------------------------------------------------------- #

async def _client(service):
    async def auth(token):
        return {"portal-token": "system:serviceaccount:whistler:whistler-portal"
                }.get(token)
    client = TestClient(TestServer(make_app(service, auth)))
    await client.start_server()
    return client


H = {"Authorization": "Bearer portal-token"}


async def test_the_api_needs_an_allowed_token(world):
    service = world[0]
    client = await _client(service)
    try:
        assert (await client.get("/healthz")).status == 200
        assert (await client.get("/v1/status")).status == 401
        bad = {"Authorization": "Bearer nope"}
        assert (await client.get("/v1/status", headers=bad)).status == 403
        assert (await client.get("/v1/status", headers=H)).status == 200
    finally:
        await client.close()


async def test_back_up_list_download_delete_over_http(world):
    service = world[0]
    client = await _client(service)
    try:
        r = await client.post("/v1/backups", headers=H, json={"by": "root"})
        assert r.status == 201
        name = (await r.json())["backup"]["file"]
        listing = await (await client.get("/v1/backups", headers=H)).json()
        assert [b["file"] for b in listing] == [name]
        r = await client.get(f"/v1/backups/{name}", headers=H)
        assert r.status == 200
        assert name in r.headers["Content-Disposition"]
        archive.read(await r.read())
        assert (await client.delete(f"/v1/backups/{name}",
                                    headers=H)).status == 200
    finally:
        await client.close()


async def test_a_path_is_never_a_file_name_over_http(world):
    client = await _client(world[0])
    try:
        r = await client.get("/v1/backups/..%2F..%2Fetc%2Fpasswd", headers=H)
        assert r.status in (400, 404)
    finally:
        await client.close()


async def test_settings_are_validated_over_http(world):
    client = await _client(world[0])
    try:
        r = await client.put("/v1/settings", headers=H, json={"mode": "hourly"})
        assert r.status == 400 and "mode" in (await r.json())["error"]
        r = await client.put("/v1/settings", headers=H,
                             json={"mode": "interval", "hours": 6})
        assert (await r.json())["hours"] == 6
    finally:
        await client.close()


async def test_the_caller_is_recorded_with_the_decision(world):
    service, _, state, _ = world
    client = await _client(service)
    try:
        await client.post("/v1/install/decision", headers=H,
                          json={"decision": "declined", "by": "root"})
        assert state.recorded["decidedBy"] == \
            "root (via system:serviceaccount:whistler:whistler-portal)"
    finally:
        await client.close()


# --- TokenReview ------------------------------------------------------------------------- #

async def test_token_review_allows_only_listed_service_accounts():
    reviews = []

    def review(token):
        reviews.append(token)
        return {"a": "system:serviceaccount:whistler:whistler-portal",
                "b": "system:serviceaccount:whistler:someone-else"}.get(token)
    auth = TokenReviewAuth(["system:serviceaccount:whistler:whistler-portal"],
                           review=review)
    assert await auth("a") == "system:serviceaccount:whistler:whistler-portal"
    assert await auth("b") is None
    assert await auth("c") is None
    await auth("a")
    assert reviews.count("a") == 1        # cached
