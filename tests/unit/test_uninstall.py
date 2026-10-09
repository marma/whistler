"""The pre-delete hook (whistler/uninstall.py; design/backup.md Phase 5).

What matters is the order and the failure rules: nothing is deleted until
the disks are retained and — unless told otherwise — a final backup exists,
and the hook does not return while the user namespaces are still going.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from whistler import reattach, uninstall


# --- the order and the failure rules ------------------------------------------------ #

@pytest.fixture
def steps(monkeypatch):
    log = []
    monkeypatch.setattr(uninstall, "retain_user_data",
                        lambda cm: log.append("retain") or ["pv-1"])
    monkeypatch.setattr(uninstall, "final_backup",
                        lambda b: log.append("backup") or "b.tar.gz")
    monkeypatch.setattr(uninstall, "delete_namespaces",
                        lambda timeout: log.append("namespaces"))
    monkeypatch.setattr(uninstall, "delete_release_leftovers",
                        lambda cm: log.append("leftovers") or [])
    monkeypatch.setattr(uninstall, "release_backup_claim",
                        lambda cm: log.append("claim"))
    return log


CM = SimpleNamespace(namespace="whistler")
ON = SimpleNamespace(enabled=True)


def test_retain_then_backup_then_delete(steps):
    uninstall.run(CM, ON)
    assert steps == ["retain", "backup", "namespaces", "leftovers", "claim"]


def test_a_failed_retain_deletes_nothing(steps, monkeypatch):
    def boom(cm):
        raise ApiException(status=403, reason="Forbidden")
    monkeypatch.setattr(uninstall, "retain_user_data", boom)
    with pytest.raises(ApiException):
        uninstall.run(CM, ON)
    assert steps == []


def test_a_failed_final_backup_deletes_nothing_by_default(steps, monkeypatch):
    def boom(b):
        raise RuntimeError("backup service down")
    monkeypatch.setattr(uninstall, "final_backup", boom)
    with pytest.raises(uninstall.UninstallError, match="Nothing has been deleted"):
        uninstall.run(CM, ON)
    assert steps == ["retain"]


def test_without_require_final_backup_the_uninstall_goes_on(steps, monkeypatch):
    def boom(b):
        raise RuntimeError("backup service down")
    monkeypatch.setattr(uninstall, "final_backup", boom)
    uninstall.run(CM, ON, require_final_backup=False)
    assert steps == ["retain", "namespaces", "leftovers", "claim"]


def test_backups_off_means_no_final_backup(steps):
    uninstall.run(CM, SimpleNamespace(enabled=False))
    assert "backup" not in steps and steps[-1] == "claim"


def test_the_final_backup_is_an_uninstall_backup():
    seen = {}

    class Backups:
        async def create(self, by, trigger="manual"):
            seen.update(by=by, trigger=trigger)
            return {"skipped": False, "backup": {"file": "x.tar.gz"}}
    assert uninstall.final_backup(Backups()) == "x.tar.gz"
    assert seen["trigger"] == "uninstall"


# --- waiting for the namespaces ---------------------------------------------------- #

def _ns(name):
    return SimpleNamespace(metadata=SimpleNamespace(name=name))


class Namespaces:
    """User/archive namespaces that take ``polls`` list calls to go."""

    def __init__(self, names, polls):
        self.names, self.polls, self.deleted = set(names), polls, []

    def list_namespace(self, label_selector=None):
        items = [_ns(n) for n in sorted(self.names)
                 if (n == "whistler-archive") == ("archive" in label_selector)]
        return SimpleNamespace(items=items)

    def delete_namespace(self, name):
        self.deleted.append(name)

    def tick(self):
        self.polls -= 1
        if self.polls <= 0:
            self.names.clear()


def test_the_hook_waits_until_the_namespaces_are_gone():
    core = Namespaces(["whistler-user-alice", "whistler-archive"], polls=3)
    with patch("whistler.uninstall.client.CoreV1Api", return_value=core):
        uninstall.delete_namespaces(60, poll=0, sleep=lambda s: core.tick())
    assert sorted(core.deleted) == ["whistler-archive", "whistler-user-alice"]
    assert not core.names


def test_namespaces_that_will_not_go_fail_the_uninstall():
    core = Namespaces(["whistler-user-alice"], polls=10 ** 6)
    now = [0.0]

    def sleep(s):
        now[0] += 10
    with patch("whistler.uninstall.client.CoreV1Api", return_value=core):
        with pytest.raises(uninstall.UninstallError,
                           match="whistler-user-alice"):
            uninstall.delete_namespaces(30, poll=10, clock=lambda: now[0],
                                        sleep=sleep)


# --- what counts as user data -------------------------------------------------------- #

@pytest.mark.parametrize("ns,name,kind", [
    ("whistler-user-alice", "whistler-home-desk", reattach.KIND_HOME),
    ("whistler-user-alice", "whistler-data-alice", reattach.KIND_POD_HOME),
    ("whistler-archive", "whistler-home-alice-desk-1", reattach.KIND_ARCHIVED),
    ("whistler-user-alice", "alice-box-rootdisk", None),
])
def test_which_claims_are_retained(ns, name, kind):
    assert uninstall._claim_kind(ns, name, "whistler-archive") == kind


def test_retain_covers_homes_the_archive_the_backup_and_dataset_claims(
        monkeypatch):
    monkeypatch.setenv("WHISTLER_BACKUP_CLAIM", "whistler-backups")
    core = MagicMock()
    core.list_namespace.side_effect = lambda label_selector=None: \
        SimpleNamespace(items=[_ns("whistler-archive")]
                        if "archive" in label_selector
                        else [_ns("whistler-user-alice")])

    def claims(ns, label_selector=None):
        if ns == "whistler":
            # Only the managed datasets' claims are asked for here, by label.
            assert label_selector == "app=whistler-dataset-server"
            return SimpleNamespace(items=[_ns("whistler-dataset-corpus")])
        names = {"whistler-user-alice": ["whistler-home-desk",
                                         "alice-box-rootdisk"],
                 "whistler-archive": ["whistler-home-bob-old-1"]}[ns]
        return SimpleNamespace(items=[_ns(n) for n in names])
    core.list_namespaced_persistent_volume_claim.side_effect = claims
    secured = []
    cm = SimpleNamespace(namespace="whistler",
                         secure_claim=lambda ns, c, kind, u: secured.append(
                             (ns, c, kind, u)) or f"pv-{c}")
    with patch("whistler.uninstall.client.CoreV1Api", return_value=core):
        pvs = uninstall.retain_user_data(cm)
    assert secured == [
        ("whistler-archive", "whistler-home-bob-old-1", "archived", ""),
        ("whistler-user-alice", "whistler-home-desk", "home", "alice"),
        ("whistler", "whistler-backups", "backups", ""),
        ("whistler", "whistler-dataset-corpus", "dataset", "")]
    assert len(pvs) == 4


# --- the release namespace ---------------------------------------------------------------- #

def _obj(name, labels=None):
    return SimpleNamespace(metadata=SimpleNamespace(name=name,
                                                    labels=labels or {}))


def test_leftovers_are_removed_and_helms_own_are_not(monkeypatch):
    monkeypatch.setenv("WHISTLER_HOST_KEY_SECRET_NAME", "wh-server-host-key")
    helm = {"app.kubernetes.io/managed-by": "Helm"}
    api = MagicMock()
    api.list_namespaced_custom_object.side_effect = \
        lambda g, v, ns, plural, **kw: {"items": {
            "users": [{"metadata": {"name": "alice"}}],
            "zones": [{"metadata": {"name": "open", "labels": helm}},
                      {"metadata": {"name": "restricted"}}],
            "middlewares": [{"metadata": {
                "name": "whistler-dataset-corpus-ext"}}]}.get(plural, [])}
    cm = SimpleNamespace(namespace="whistler", api=api, group="g", version="v",
                         ssh_ca_secret_name="wh-ssh-ca")
    core, apps, net = MagicMock(), MagicMock(), MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(
        items=[_obj("whistler-s3-corpus-ro")])
    core.list_namespaced_service.return_value = SimpleNamespace(
        items=[_obj("whistler-s3-corpus-ro")])
    net.list_namespaced_network_policy.return_value = SimpleNamespace(items=[])
    core.list_namespaced_secret.side_effect = \
        lambda ns, label_selector: SimpleNamespace(items={
            "app=whistler-dataset": [_obj("whistler-dataset-corpus-creds")],
            "app=whistler-backup": [_obj("wh-backup-passphrase")]}.get(
                label_selector, []))
    core.list_namespaced_config_map.return_value = SimpleNamespace(items=[
        _obj("wh-install", helm), _obj("wh-install-state")])
    core.delete_namespaced_secret.side_effect = None
    with patch("whistler.uninstall.client.CoreV1Api", return_value=core), \
            patch("whistler.uninstall.client.AppsV1Api", return_value=apps), \
            patch("whistler.uninstall.client.NetworkingV1Api", return_value=net):
        deleted = uninstall.delete_release_leftovers(cm)
    assert "User/alice" in deleted and "Zone/restricted" in deleted
    assert "Zone/open" not in deleted                    # Helm's
    assert "ConfigMap/wh-install-state" in deleted
    assert "ConfigMap/wh-install" not in deleted         # Helm's: the install id
    for name in ("whistler-dataset-corpus-creds", "wh-backup-passphrase",
                 "wh-ssh-ca", "wh-server-host-key"):
        assert f"Secret/{name}" in deleted
    assert "Deployment/whistler-s3-corpus-ro" in deleted
    assert "Middleware/whistler-dataset-corpus-ext" in deleted


def test_leftovers_that_are_already_gone_are_fine():
    api = MagicMock()
    api.list_namespaced_custom_object.return_value = {
        "items": [{"metadata": {"name": "alice"}}]}
    api.delete_namespaced_custom_object.side_effect = ApiException(status=404)
    cm = SimpleNamespace(namespace="whistler", api=api, group="g", version="v",
                         ssh_ca_secret_name="wh-ssh-ca")
    core = MagicMock()
    core.delete_namespaced_secret.side_effect = ApiException(status=404)
    for fn in ("list_namespaced_service", "list_namespaced_secret",
               "list_namespaced_config_map"):
        getattr(core, fn).return_value = SimpleNamespace(items=[])
    apps, net = MagicMock(), MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(items=[])
    net.list_namespaced_network_policy.return_value = SimpleNamespace(items=[])
    with patch("whistler.uninstall.client.CoreV1Api", return_value=core), \
            patch("whistler.uninstall.client.AppsV1Api", return_value=apps), \
            patch("whistler.uninstall.client.NetworkingV1Api", return_value=net):
        assert uninstall.delete_release_leftovers(cm) == []
