"""Archived homes (config.py, "Archived homes"; reattach.py).

A claim cannot change namespace; a retained PV can change claims. Archiving
deletes the user's claim and binds the PV to one in the archive namespace,
beside a HomeVolume record with no user. Restoring does the reverse for any
user. The PV carries the provenance, so a reinstall rebuilds the archive.
"""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from whistler import reattach
from whistler.config import (ARCHIVE_ANNOTATION, ARCHIVE_NAMESPACE,
                             KubeConfigManager, PolicyError)

NS = "whistler-user-alice"


def _pv(name="pv-desk", phase="Bound", policy="Retain", claim_ns=NS,
        claim="whistler-home-desk", labels=None, annotations=None):
    return {"metadata": {"name": name, "labels": labels or {},
                         "annotations": annotations or {}},
            "spec": {"persistentVolumeReclaimPolicy": policy,
                     "accessModes": ["ReadWriteOnce"],
                     "capacity": {"storage": "20Gi"},
                     "storageClassName": "nfs",
                     "claimRef": {"namespace": claim_ns, "name": claim}},
            "status": {"phase": phase}}


RECORD = {"record": "alice-desk-20260927120000", "user": "alice",
          "name": "desk", "archivedAt": "2026-09-27T12:00:00Z",
          "archivedBy": "root"}


def _archived_pv(**kw):
    return _pv(labels={reattach.USER_DATA_LABEL: "archived"},
               annotations={reattach.ARCHIVED_ANNOTATION: json.dumps(RECORD)},
               **kw)


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.home_disk_size = "20Gi"
    cm._get_user_namespace = lambda u: f"whistler-user-{u}"
    cm.api = MagicMock()
    return cm


# --- the PV side (pure) ------------------------------------------------------ #

def test_archiving_disconnects_the_pv_from_its_user():
    patch_ = reattach.archive_patch(_pv(), RECORD)
    labels = patch_["metadata"]["labels"]
    assert labels[reattach.USER_DATA_LABEL] == "archived"
    assert labels[reattach.USER_LABEL] is None        # merge-patch removes it
    assert patch_["spec"]["persistentVolumeReclaimPolicy"] == "Retain"
    stored = patch_["metadata"]["annotations"][reattach.ARCHIVED_ANNOTATION]
    assert reattach.archive_record(
        {"metadata": {"annotations": {reattach.ARCHIVED_ANNOTATION: stored}}}
    ) == RECORD


def test_restoring_gives_the_pv_to_its_new_user():
    patch_ = reattach.unarchive_patch("bob")
    assert patch_["metadata"]["labels"] == {reattach.USER_DATA_LABEL: "home",
                                            reattach.USER_LABEL: "bob"}
    assert patch_["metadata"]["annotations"] == {
        reattach.ARCHIVED_ANNOTATION: None}


def test_a_garbled_record_is_no_record():
    assert reattach.archive_record({"metadata": {"annotations": {
        reattach.ARCHIVED_ANNOTATION: "{not json"}}}) is None
    assert reattach.archive_record({"metadata": {}}) is None


def test_a_new_volume_of_the_same_name_does_not_pick_up_the_archived_disk():
    # The archived PV's stale claimRef still names the claim it came from.
    pv = _archived_pv(phase="Released")
    assert reattach.find_reattachable([pv], NS, "whistler-home-desk") == (
        None, None)


def test_a_recorded_but_archived_disk_is_refused():
    pv, problem = reattach.find_reattachable(
        [_archived_pv(phase="Released")], NS, "whistler-home-desk",
        recorded="pv-desk")
    assert pv is None and "archived" in problem


def test_the_archives_own_moves_may_bind_it():
    pv, problem = reattach.find_reattachable(
        [_archived_pv(phase="Released")], ARCHIVE_NAMESPACE, "x",
        recorded="pv-desk", allow_archived=True)
    assert problem is None and pv["metadata"]["name"] == "pv-desk"


def test_a_pv_being_deleted_is_never_bound_back():
    # Deleting with data sets Delete and drops the claim; for a moment the PV
    # is Released and about to be reclaimed.
    doomed = _pv(phase="Released", policy="Delete")
    assert reattach.find_reattachable([doomed], NS, "whistler-home-desk") == (
        None, None)
    pv, problem = reattach.find_reattachable(
        [doomed], NS, "whistler-home-desk", recorded="pv-desk")
    assert pv is None and "being deleted" in problem


# --- refusing ----------------------------------------------------------------- #

def _refusal(holder=None, sessions=(), bound=True, recorded=None):
    cm = _manager()
    cm.get_home_volume = lambda u, n: {"name": n, "pvName": recorded} \
        if recorded else {"name": n}
    cm.home_volume_holder = lambda u, v: holder
    cm.api.list_namespaced_custom_object.return_value = {"items": [
        {"metadata": {"name": n}, "spec": spec} for n, spec in sessions]}
    core = MagicMock()
    if bound:
        core.read_namespaced_persistent_volume_claim.return_value = \
            SimpleNamespace(spec=SimpleNamespace(volume_name="pv-desk"))
    else:
        core.read_namespaced_persistent_volume_claim.side_effect = \
            ApiException(status=404)
    with patch("whistler.config.CoreV1Api", return_value=core):
        return cm.archive_refusal("alice", "desk")


def test_a_free_used_home_can_be_archived():
    assert _refusal(sessions=[("alice-other", {"homeVolume": "scratch"})]) is None


def test_an_attached_home_is_refused():
    assert "alice-box" in _refusal(holder="alice-box")


def test_a_home_an_instance_names_is_refused():
    assert "alice-box" in _refusal(sessions=[("alice-box",
                                              {"homeVolume": "desk"})])


def test_a_home_an_instance_would_default_to_is_refused():
    # Archived from under it, an instance with no home chosen would be given
    # a fresh EMPTY default home at its next start — never acceptable.
    assert "desk" in _refusal(sessions=[("desk", {})])


def test_a_home_never_used_has_nothing_to_archive():
    assert "never been used" in _refusal(bound=False)
    assert _refusal(bound=False, recorded="pv-desk") is None


def test_the_request_is_a_mark_on_the_volume():
    cm = _manager()
    cm.archive_refusal = lambda u, n: None
    ok, _msg = cm.request_archive_home_volume("alice", "desk", "root")
    assert ok
    body = cm.api.patch_namespaced_custom_object.call_args[0][5]
    assert body == {"metadata": {"annotations": {ARCHIVE_ANNOTATION: "root"}}}


def test_a_marked_volume_leaves_the_users_list_at_once():
    cm = _manager()
    cm.api.list_namespaced_custom_object.return_value = {"items": [
        {"metadata": {"name": "keep"}, "spec": {}},
        {"metadata": {"name": "desk",
                      "annotations": {ARCHIVE_ANNOTATION: "root"}},
         "spec": {}}]}
    assert [v["name"] for v in cm.get_home_volumes("alice")] == ["keep"]


# --- archiving (the operator) -------------------------------------------------- #

def _archiving(pv_phases, bind_ok=True):
    """Run archive_home_volume; return (result, ordered steps)."""
    cm = _manager()
    steps = []
    cm.api.get_namespaced_custom_object.return_value = {
        "metadata": {"name": "desk", "annotations": {ARCHIVE_ANNOTATION: "root"}},
        "spec": {"user": "alice", "pvName": "pv-desk", "size": "20Gi"}}
    cm.home_volume_holder = lambda u, v: None
    phases = iter(pv_phases)
    cm._read_pv = lambda name: _pv(phase=next(phases))
    cm._ensure_archive_namespace = lambda: steps.append("namespace")
    cm.api.create_namespaced_custom_object.side_effect = \
        lambda *a: steps.append(("record", a[4]["metadata"]["name"],
                                 a[4]["spec"]["archived"]["user"]))
    cm.api.delete_namespaced_custom_object.side_effect = \
        lambda *a: steps.append(("delete-record", a[2], a[4]))
    cm.revoke_own_volume_access = lambda u, n: steps.append(("revoke", u, n))
    cm.reattach_claim = lambda ns, claim, labels, recorded=None, \
        allow_archived=False, logger=None: steps.append(
            ("bind", ns, recorded, allow_archived)) or bind_ok
    core = MagicMock()
    core.patch_persistent_volume.side_effect = \
        lambda name, body: steps.append(
            ("pv", body["metadata"]["labels"][reattach.USER_DATA_LABEL]))
    core.read_namespaced_persistent_volume_claim.side_effect = \
        ApiException(status=404)
    core.delete_namespaced_persistent_volume_claim.side_effect = \
        lambda name, ns: steps.append(("delete-claim", ns, name))
    with patch("whistler.config.CoreV1Api", return_value=core):
        result = cm.archive_home_volume(NS, "desk")
    return result, steps


def test_archive_marks_the_pv_first_and_removes_the_user_last():
    result, steps = _archiving(["Bound", "Released"])
    assert result is True
    names = [s[0] if isinstance(s, tuple) else s for s in steps]
    assert names == ["pv", "namespace", "record", "delete-claim", "bind",
                     "delete-record", "revoke"]
    assert steps[0] == ("pv", "archived")
    assert steps[4] == ("bind", ARCHIVE_NAMESPACE, "pv-desk", True)
    assert steps[5] == ("delete-record", NS, "desk")
    assert steps[6] == ("revoke", "alice", "desk")


def test_archive_waits_while_the_pv_is_still_releasing():
    # The PV takes a moment to go Released once the user's claim is deleted;
    # until the archive's claim exists the user's record stays, so a retry
    # finishes the job.
    result, steps = _archiving(["Bound", "Bound"])
    assert result is False
    assert not any(isinstance(s, tuple) and s[0] == "delete-record"
                   for s in steps)


def test_archive_retry_reuses_the_record_already_on_the_pv():
    cm = _manager()
    cm.api.get_namespaced_custom_object.return_value = {
        "metadata": {"name": "desk", "annotations": {ARCHIVE_ANNOTATION: "root"}},
        "spec": {"user": "alice", "pvName": "pv-desk"}}
    cm.home_volume_holder = lambda u, v: None
    cm._read_pv = lambda name: _archived_pv()
    seen = []
    cm._ensure_archive_record = lambda pv, record: seen.append(record["record"])
    cm._bind_archive_claim = lambda pv, record: False
    with patch("whistler.config.CoreV1Api", return_value=MagicMock()):
        cm.archive_home_volume(NS, "desk")
    assert seen == [RECORD["record"]]


# --- restoring ------------------------------------------------------------------ #

def _restoring_request(existing=False):
    cm = _manager()
    cm.get_archived_home_volumes = lambda: [
        {"name": RECORD["record"], "size": "20Gi", "archived": RECORD}]
    cm.user_exists = lambda u: u == "bob"
    cm._ensure_user_namespace = lambda u: f"whistler-user-{u}"
    if existing:
        cm.api.get_namespaced_custom_object.return_value = {"spec": {}}
    else:
        cm.api.get_namespaced_custom_object.side_effect = ApiException(status=404)
    saved = []
    cm.save_home_volume = lambda u, v: saved.append((u, v)) or True
    cm.get_home_volume = lambda u, n: saved[-1][1] if saved else None
    core = MagicMock()
    core.read_namespaced_persistent_volume_claim.side_effect = \
        ApiException(status=404)
    with patch("whistler.config.CoreV1Api", return_value=core):
        result = cm.request_unarchive_home_volume(RECORD["record"], "bob", "desk")
    return result, saved


def test_restore_is_a_new_volume_naming_its_source():
    (ok, _msg), saved = _restoring_request()
    assert ok
    user, volume = saved[0]
    assert user == "bob"
    assert volume["name"] == "desk"
    assert volume["fromArchive"] == RECORD["record"]


def test_restore_never_overwrites_an_existing_volume():
    (ok, msg), saved = _restoring_request(existing=True)
    assert not ok and "already has" in msg and not saved


def test_a_crd_that_prunes_the_request_is_caught_not_left_empty():
    cm = _manager()
    cm.get_archived_home_volumes = lambda: [{"name": RECORD["record"]}]
    cm.user_exists = lambda u: True
    cm._ensure_user_namespace = lambda u: f"whistler-user-{u}"
    cm.api.get_namespaced_custom_object.side_effect = ApiException(status=404)
    cm.save_home_volume = lambda u, v: True
    cm.get_home_volume = lambda u, n: {"name": n}     # fromArchive pruned
    core = MagicMock()
    core.read_namespaced_persistent_volume_claim.side_effect = \
        ApiException(status=404)
    with patch("whistler.config.CoreV1Api", return_value=core):
        ok, msg = cm.request_unarchive_home_volume(RECORD["record"], "bob", "desk")
    assert not ok and "crds.yaml" in msg
    cm.api.delete_namespaced_custom_object.assert_called_once()


def test_restore_to_an_unknown_user_is_refused():
    cm = _manager()
    cm.get_archived_home_volumes = lambda: [{"name": RECORD["record"]}]
    cm.user_exists = lambda u: False
    ok, msg = cm.request_unarchive_home_volume(RECORD["record"], "eve", "desk")
    assert not ok and "eve" in msg


def test_a_home_being_restored_cannot_be_attached_yet():
    cm = _manager()
    core = MagicMock()
    core.read_namespaced_persistent_volume_claim.side_effect = \
        ApiException(status=404)
    with patch("whistler.config.client.CoreV1Api", return_value=core):
        with pytest.raises(PolicyError, match="restored"):
            cm.ensure_home_volume_pvc("bob", {"name": "desk",
                                              "fromArchive": "x"})
    core.create_namespaced_persistent_volume_claim.assert_not_called()


def test_unarchive_records_the_disk_then_moves_it_then_clears_the_request():
    cm = _manager()
    steps = []

    def get(group, version, ns, plural, name):
        if ns == ARCHIVE_NAMESPACE:
            return {"spec": {"pvName": "pv-desk",
                             "pvcName": f"whistler-home-{RECORD['record']}"}}
        return {"spec": {"user": "bob", "fromArchive": RECORD["record"]}}
    cm.api.get_namespaced_custom_object.side_effect = get
    cm.api.patch_namespaced_custom_object.side_effect = \
        lambda *a: steps.append(("patch-volume", a[5]))
    cm.api.delete_namespaced_custom_object.side_effect = \
        lambda *a: steps.append(("delete-record", a[2]))
    cm._read_pv = lambda name: _pv(phase="Released")
    cm.reattach_claim = lambda ns, claim, labels, recorded=None, **kw: \
        steps.append(("bind", ns, claim, recorded)) or True
    core = MagicMock()
    core.patch_persistent_volume.side_effect = \
        lambda name, body: steps.append(
            ("pv", body["metadata"]["labels"][reattach.USER_LABEL]))
    core.delete_namespaced_persistent_volume_claim.side_effect = \
        lambda name, ns: steps.append(("delete-claim", ns))
    core.read_namespaced_persistent_volume_claim.side_effect = \
        ApiException(status=404)
    with patch("whistler.config.CoreV1Api", return_value=core):
        assert cm.unarchive_home_volume("whistler-user-bob", "desk")
    assert steps == [
        ("patch-volume", {"spec": {"pvName": "pv-desk"}}),
        ("pv", "bob"),
        ("delete-claim", ARCHIVE_NAMESPACE),
        ("bind", "whistler-user-bob", "whistler-home-desk", "pv-desk"),
        ("delete-record", ARCHIVE_NAMESPACE),
        ("patch-volume", {"spec": {"fromArchive": None}}),
    ]


# --- surviving a reinstall ------------------------------------------------------- #

def test_recovery_rebuilds_the_archive_from_released_pvs():
    cm = _manager()
    cm._list_pvs = lambda: [
        _archived_pv(phase="Released"),
        _archived_pv(name="pv-doomed", phase="Released", policy="Delete"),
        _pv(name="pv-home", phase="Released"),                 # not archived
        _pv(name="pv-norecord", phase="Released",
            labels={reattach.USER_DATA_LABEL: "archived"}),     # no provenance
        _archived_pv(name="pv-live", phase="Bound"),            # already fine
    ]
    rebuilt = []
    cm._ensure_archive_record = lambda pv, record: rebuilt.append(pv)
    cm._bind_archive_claim = lambda pv, record: True
    assert cm.recover_archive() == 1
    assert rebuilt == ["pv-desk"]


# --- the page ----------------------------------------------------------------------- #

def test_the_admin_page_lists_the_archive_with_its_actions():
    from whistler.portal import management as mgmt
    request = SimpleNamespace(url=SimpleNamespace(path="/admin/homevolumes"))
    html = mgmt.templates.env.get_template("admin/home_volumes.html").render(
        request=request, current_user="root", is_admin=True,
        volumes=[{"user": "alice", "name": "desk", "pvcName": "c",
                  "zones": ["default"], "inUseBy": None}],
        archived=[{"name": RECORD["record"], "size": "20Gi",
                   "archived": RECORD}],
        usernames=["alice", "bob"])
    assert "/admin/homevolumes/alice/desk/archive" in html
    assert f"/admin/archive/homevolumes/{RECORD['record']}/restore" in html
    assert f"/admin/archive/homevolumes/{RECORD['record']}/delete" in html
    assert "alice/desk" in html


def test_an_archive_interrupted_after_deleting_the_claim_still_finds_its_disk():
    """Found live (k3d, 2026-09-27): the first attempt found the disk via the
    user's claim, deleted the claim, and had to wait for the PV to release;
    the retry looked for the claim, found none, and gave up — leaving an
    archived PV the user's volume no longer pointed at. The disk is now
    recorded before anything is deleted, and an archived PV's own record is
    the fallback."""
    cm = _manager()
    cm.api.get_namespaced_custom_object.return_value = {
        "metadata": {"name": "old", "annotations": {ARCHIVE_ANNOTATION: "root"}},
        "spec": {"user": "alice"}}                    # no pvName recorded
    cm.home_volume_holder = lambda u, v: None
    cm.secure_home_volume = lambda u, v: None         # the claim is gone
    record = {**RECORD, "name": "old", "record": "alice-old-1"}
    cm._list_pvs = lambda: [_pv(name="pv-old", phase="Released",
                                labels={reattach.USER_DATA_LABEL: "archived"},
                                annotations={reattach.ARCHIVED_ANNOTATION:
                                             json.dumps(record)})]
    cm._read_pv = lambda name: next(p for p in cm._list_pvs()
                                    if p["metadata"]["name"] == name)
    bound = []
    cm._ensure_archive_record = lambda pv, rec: None
    cm._bind_archive_claim = lambda pv, rec: bound.append(pv) or True
    cm.revoke_own_volume_access = lambda u, n: True
    with patch("whistler.config.CoreV1Api", return_value=MagicMock()):
        assert cm.archive_home_volume(NS, "old") is True
    assert bound == ["pv-old"]
    # And it did not give up: no un-marking patch was sent.
    for call in cm.api.patch_namespaced_custom_object.call_args_list:
        assert call[0][5] != {"metadata": {"annotations": {
            ARCHIVE_ANNOTATION: None}}}


def test_the_first_attempt_records_the_disk_before_deleting_the_claim():
    cm = _manager()
    cm.api.get_namespaced_custom_object.return_value = {
        "metadata": {"name": "desk", "annotations": {ARCHIVE_ANNOTATION: "root"}},
        "spec": {"user": "alice"}}
    cm.home_volume_holder = lambda u, v: None
    order = []
    cm.secure_home_volume = lambda u, v: order.append("record") or "pv-desk"
    cm._read_pv = lambda name: _pv(phase="Bound")
    cm._ensure_archive_record = lambda pv, rec: None
    cm._bind_archive_claim = lambda pv, rec: False
    core = MagicMock()
    core.delete_namespaced_persistent_volume_claim.side_effect = \
        lambda *a: order.append("delete-claim")
    with patch("whistler.config.CoreV1Api", return_value=core):
        cm.archive_home_volume(NS, "desk")
    assert order == ["record", "delete-claim"]
