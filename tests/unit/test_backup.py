"""Backup and restore of state (whistler/backup, design/backup.md Phase 2).

The round trip is tested against an in-memory fake of the two Kubernetes
APIs the code uses, so the rules that matter — what is in a backup, restored
where, idempotently, without deleting, without stopping what runs — are
checked end to end without a cluster.
"""
import datetime
import gzip
import io
import json
import tarfile
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from whistler.backup import BackupError, archive, restore
from whistler.backup.export import export

from backup_fakes import HELM, KIND_OF, FakeCluster, FakeCM, _populated

T0 = datetime.datetime(2026, 9, 27, 12, 0, tzinfo=datetime.timezone.utc)
T1 = datetime.datetime(2026, 9, 28, 3, 0, tzinfo=datetime.timezone.utc)
@pytest.fixture
def host_key_env(monkeypatch):
    monkeypatch.setenv("WHISTLER_HOST_KEY_SECRET_NAME",
                       "whistler-server-host-key")


def _export(cluster, **kw):
    cm = FakeCM(cluster, **{k: kw.pop(k) for k in ("namespace", "release")
                            if k in kw})
    with patch("kubernetes.client.CoreV1Api", return_value=cluster):
        return export(cm, created=kw.pop("created", T0), **kw)


def _restore(cluster, data, apply=True, passphrase=None, **cmkw):
    cm = FakeCM(cluster, **cmkw)
    with patch("kubernetes.client.CoreV1Api", return_value=cluster):
        backup = archive.read(data, passphrase=passphrase)
        the_plan = restore.plan(cm, backup)
        written = restore.apply(cm, the_plan) if apply else []
        pruned = restore.readback(cm, written) if apply else []
    return the_plan, written, pruned


# --- what is in a backup --------------------------------------------------------- #

def test_the_backup_holds_state_and_nothing_derived(host_key_env):
    data, manifest, warnings = _export(_populated())
    backup = archive.read(data)
    names = {(o["kind"], o["metadata"].get("namespace"), o["metadata"]["name"])
             for o in backup.objects}
    assert names == {
        ("Zone", None, "restricted"), ("User", None, "alice"),
        ("Group", None, "lab"), ("Template", None, "custom"),
        ("Dataset", None, "corpus"),
        ("HomeVolume", "whistler-user-alice", "desk"),
        ("HomeVolume", "whistler-archive", "bob-old-20260101"),
        ("Session", "whistler-user-alice", "alice-box")}
    # Helm-rendered (in git), being-deleted, and foreign namespaces: out.
    roles = sorted(s["metadata"]["annotations"][archive.ROLE_ANNOTATION]
                   for s in backup.secrets)
    assert roles == ["dataset-credentials", "server-host-key", "ssh-ca"]
    assert manifest["secrets"] == "plain"
    assert any("unencrypted" in w for w in warnings)


def test_a_session_is_backed_up_without_its_run_state(host_key_env):
    backup = archive.read(_export(_populated())[0])
    session = next(o for o in backup.objects if o["kind"] == "Session")
    assert "runOverrides" not in session["spec"]
    assert "annotations" not in session["metadata"]   # last-connect dropped
    assert session["spec"]["homeVolume"] == "desk"


def test_live_only_metadata_is_not_in_the_backup(host_key_env):
    backup = archive.read(_export(_populated())[0])
    for obj in backup.objects:
        meta = obj["metadata"]
        assert not {"uid", "resourceVersion", "managedFields"} & set(meta)
        assert "status" not in obj
        assert not any(k.startswith("kopf.zalando.org/")
                       for k in meta.get("annotations") or {})


def test_the_uid_a_home_is_owned_by_survives(host_key_env):
    backup = archive.read(_export(_populated())[0])
    alice = next(o for o in backup.objects if o["kind"] == "User")
    assert alice["spec"]["uid"] == 1234


def test_no_host_key_name_is_a_warning_not_a_failure(monkeypatch):
    monkeypatch.delenv("WHISTLER_HOST_KEY_SECRET_NAME", raising=False)
    data, manifest, warnings = _export(_populated())
    assert manifest["secretCount"] == 2
    assert any("host key" in w for w in warnings)


def test_state_is_byte_stable_and_hash_ignores_the_clock(host_key_env):
    c = _populated()
    a, ma, _ = _export(c, created=T0)
    b, mb, _ = _export(c, created=T1)
    assert ma["contentHash"] == mb["contentHash"]
    assert ma["sha256"][archive.STATE] == mb["sha256"][archive.STATE]
    c.put("zones", "whistler", "restricted", {"dns": {"clusterOnly": False}})
    _, mc, _ = _export(c, created=T1)
    assert mc["contentHash"] != ma["contentHash"]


def test_filename_says_when_and_why(host_key_env):
    _, manifest, _ = _export(_populated(), trigger="scheduled")
    assert archive.filename(manifest) == \
        "whistler-backup-20260927-120000-scheduled.tar.gz"


# --- reading a file as untrusted input ------------------------------------------ #

def _repack(data, mutate):
    raw = gzip.decompress(data)
    members = {}
    with tarfile.open(fileobj=io.BytesIO(raw)) as tar:
        for info in tar:
            members[info.name] = tar.extractfile(info).read()
    mutate(members)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, body in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    return gzip.compress(buf.getvalue())


def test_a_damaged_member_is_caught(host_key_env):
    data = _export(_populated())[0]
    bad = _repack(data, lambda m: m.__setitem__(
        archive.STATE, m[archive.STATE] + b"\n"))
    with pytest.raises(BackupError, match="checksum"):
        archive.read(bad)


def test_an_unexpected_member_is_refused(host_key_env):
    data = _export(_populated())[0]
    bad = _repack(data, lambda m: m.__setitem__("../../etc/passwd", b"x"))
    with pytest.raises(BackupError, match="Unexpected member"):
        archive.read(bad)


def _with_special_member(info):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tar:
        manifest = tarfile.TarInfo(archive.MANIFEST)
        manifest.size = 2
        tar.addfile(manifest, io.BytesIO(b"{}"))
        tar.addfile(info)
    return gzip.compress(buf.getvalue())


def _special(type_, linkname=""):
    info = tarfile.TarInfo(archive.STATE)     # a valid name, on purpose
    info.type, info.linkname = type_, linkname
    return info


@pytest.mark.parametrize("info,what", [
    (_special(tarfile.SYMTYPE, "/etc/passwd"), "symbolic link"),
    (_special(tarfile.SYMTYPE, "../../var/run/secrets/kubernetes.io/"
                               "serviceaccount/token"), "symbolic link"),
    (_special(tarfile.LNKTYPE, archive.MANIFEST), "hard link"),
    (_special(tarfile.DIRTYPE), "directory"),
    (_special(tarfile.CHRTYPE), "device"),
    (_special(tarfile.FIFOTYPE), "device"),
])
def test_links_and_special_files_are_refused_even_under_a_valid_name(info,
                                                                      what):
    # Nothing is extracted to disk, so a link could never overwrite a file;
    # refusing it also rules out one being followed to read one.
    with pytest.raises(BackupError, match=what):
        archive.read(_with_special_member(info))


def test_a_newer_format_is_refused(host_key_env):
    data = _export(_populated())[0]

    def bump(m):
        manifest = json.loads(m[archive.MANIFEST])
        manifest["format"] = archive.FORMAT + 1
        m[archive.MANIFEST] = json.dumps(manifest).encode()
    with pytest.raises(BackupError, match="format"):
        archive.read(_repack(data, bump))


def test_an_oversized_archive_is_refused(monkeypatch, host_key_env):
    data = _export(_populated())[0]
    monkeypatch.setattr(archive, "MAX_ARCHIVE_BYTES", 100)
    with pytest.raises(BackupError, match="larger"):
        archive.read(data)


def test_garbage_is_not_a_backup():
    with pytest.raises(BackupError):
        archive.read(b"definitely not gzip")


# --- encrypted secrets ------------------------------------------------------------- #

def test_encrypted_secrets_need_the_passphrase(host_key_env):
    data, manifest, warnings = _export(_populated(), passphrase="hunter2")
    assert manifest["secrets"] == "encrypted"
    assert not any("unencrypted" in w for w in warnings)
    assert b"Q0E=" not in gzip.decompress(data)          # the CA key's bytes
    assert archive.read(data).secrets is None
    assert len(archive.read(data, passphrase="hunter2").secrets) == 3
    with pytest.raises(BackupError, match="Wrong passphrase"):
        archive.read(data, passphrase="hunter3")


def test_restoring_without_the_passphrase_says_what_it_skips(host_key_env):
    data = _export(_populated(), passphrase="hunter2")[0]
    the_plan, _, _ = _restore(FakeCluster(), data, apply=False)
    assert not any(e.kind == "Secret" for e in the_plan.entries)
    assert any("NOT restored" in w for w in the_plan.warnings)


# --- restoring ------------------------------------------------------------------------ #

def test_round_trip_into_an_empty_cluster(host_key_env):
    source = _populated()
    data, first, _ = _export(source)
    target = FakeCluster()
    the_plan, written, pruned = _restore(target, data)
    assert set(the_plan.counts()) == {"create"}
    assert pruned == []
    assert "whistler-user-alice" in target.namespaces
    assert "whistler-archive" in target.namespaces
    # What the target holds now backs up to exactly what was restored.
    _, second, _ = _export(target)
    assert second["contentHash"] == first["contentHash"]


def test_restoring_twice_writes_nothing_the_second_time(host_key_env):
    data = _export(_populated())[0]
    target = FakeCluster()
    _restore(target, data)
    the_plan, written, _ = _restore(target, data)
    assert set(the_plan.counts()) == {"unchanged"}
    assert written == []


def test_restoring_onto_its_own_cluster_changes_nothing(host_key_env):
    c = _populated()
    the_plan, written, _ = _restore(c, _export(c)[0])
    assert written == []


def test_a_running_session_keeps_running_through_a_restore(host_key_env):
    c = _populated()
    data = _export(c)[0]
    # Change it after the backup, so the restore has to replace it.
    live = c.crs[("sessions", "whistler-user-alice", "alice-box")]
    live["spec"]["homeVolume"] = "elsewhere"
    the_plan, written, _ = _restore(c, data)
    assert [e.kind for e in written] == ["Session"]
    after = c.crs[("sessions", "whistler-user-alice", "alice-box")]
    assert after["spec"]["homeVolume"] == "desk"
    assert after["metadata"]["annotations"]["whistler/last-connect"] == \
        "1790000000.0"
    assert after["spec"]["runOverrides"] == {"gpuType": "A100"}


def test_restore_never_deletes(host_key_env):
    data = _export(_populated())[0]
    target = FakeCluster()
    target.put("users", "whistler", "newcomer", {"entryPoints": ["portal"]})
    _restore(target, data)
    assert ("users", "whistler", "newcomer") in target.crs


def test_a_helm_managed_object_here_is_left_to_helm(host_key_env):
    data = _export(_populated())[0]
    target = FakeCluster()
    target.put("zones", "whistler", "restricted", {"dns": {}}, labels=HELM)
    the_plan, _, _ = _restore(target, data)
    skipped = [e for e in the_plan.entries if e.action == "skip"]
    assert [(e.kind, e.name) for e in skipped] == [("Zone", "restricted")]
    assert target.crs[("zones", "whistler", "restricted")]["spec"] == {"dns": {}}


def test_secrets_restore_under_this_installs_names(host_key_env, monkeypatch):
    data = _export(_populated())[0]
    monkeypatch.setenv("WHISTLER_HOST_KEY_SECRET_NAME", "prod-server-host-key")
    target = FakeCluster()
    _restore(target, data, release="prod")
    assert target.secrets[("whistler", "prod-ssh-ca")]["data"] == \
        {"ca_key": "Q0E="}
    assert target.secrets[("whistler", "prod-server-host-key")]["data"] == \
        {"host_key": "SEs="}
    assert ("whistler", "whistler-dataset-corpus-creds") in target.secrets
    assert ("whistler", "whistler-ssh-ca") not in target.secrets


def test_the_release_namespace_is_whatever_it_is_called_here(host_key_env):
    data = _export(_populated())[0]
    target = FakeCluster()
    _restore(target, data, namespace="whistler-prod")
    assert ("users", "whistler-prod", "alice") in target.crs
    assert ("homevolumes", "whistler-user-alice", "desk") in target.crs


def test_a_missing_crd_refuses_the_whole_restore(host_key_env):
    data = _export(_populated())[0]
    target = FakeCluster(crds=set(KIND_OF) - {"homevolumes"})
    with pytest.raises(BackupError, match="homevolumes"):
        _restore(target, data)
    assert target.crs == {}


def test_a_field_an_older_crd_prunes_is_reported(host_key_env):
    data = _export(_populated())[0]
    target = FakeCluster(prune={"homevolumes": {"pvName"}})
    _, _, pruned = _restore(target, data)
    assert sorted(pruned) == [
        "HomeVolume whistler-archive/bob-old-20260101: spec.pvName",
        "HomeVolume whistler-user-alice/desk: spec.pvName"]


def test_an_external_dataset_credential_that_is_missing_is_a_warning(
        host_key_env):
    c = _populated()
    c.put("datasets", "whistler", "external", {"credentialsSecret": "theirs"})
    the_plan, _, _ = _restore(FakeCluster(), _export(c)[0], apply=False)
    assert any("theirs" in w for w in the_plan.warnings)


def test_order_is_users_before_their_sessions(host_key_env):
    data = _export(_populated())[0]
    _, written, _ = _restore(FakeCluster(), data)
    kinds = [e.kind for e in written]
    assert kinds.index("User") < kinds.index("Secret") < \
        kinds.index("HomeVolume") < kinds.index("Session")


# --- the CLI --------------------------------------------------------------------------- #

def test_the_cli_previews_unless_told_to_apply(host_key_env, monkeypatch,
                                                capsys):
    from whistler.backup import __main__ as cli
    data = _export(_populated())[0]
    target = FakeCluster()
    monkeypatch.setattr(cli, "_config_manager", lambda: FakeCM(target))
    monkeypatch.setattr("sys.stdin", SimpleNamespace(
        buffer=io.BytesIO(data)))
    with patch("kubernetes.client.CoreV1Api", return_value=target):
        assert cli.main(["restore"]) == 0
    assert target.crs == {}
    assert "Nothing written" in capsys.readouterr().out
