"""User data outlives the install (whistler/reattach.py, design/backup.md
Phase 1): the pure decisions over PersistentVolumes.

The rule every test here serves: no empty home next to a real one.
"""
from whistler import reattach

NS, CLAIM = "whistler-user-alice", "whistler-home-desk"


def _pv(name="pv-1", phase="Released", policy="Retain", claim_ns=NS,
        claim=CLAIM, labels=None, storage_class="nfs", capacity="20Gi"):
    spec = {"persistentVolumeReclaimPolicy": policy,
            "accessModes": ["ReadWriteOnce"], "volumeMode": "Filesystem",
            "capacity": {"storage": capacity}}
    if storage_class is not None:
        spec["storageClassName"] = storage_class
    if claim:
        spec["claimRef"] = {"kind": "PersistentVolumeClaim",
                            "namespace": claim_ns, "name": claim,
                            "uid": "old-uid", "resourceVersion": "42"}
    return {"metadata": {"name": name, "labels": labels or {}},
            "spec": spec, "status": {"phase": phase}}


# --- securing ---------------------------------------------------------------- #

def test_a_delete_policy_pv_is_set_to_retain_and_labelled():
    patch = reattach.secure_patch(_pv(policy="Delete", phase="Bound"),
                                  reattach.KIND_HOME, "alice")
    assert patch["spec"] == {"persistentVolumeReclaimPolicy": "Retain"}
    assert patch["metadata"]["labels"] == {
        reattach.USER_DATA_LABEL: "home", reattach.USER_LABEL: "alice"}


def test_a_secured_pv_needs_nothing():
    pv = _pv(phase="Bound", labels={reattach.USER_DATA_LABEL: "home",
                                    reattach.USER_LABEL: "alice"})
    assert reattach.secure_patch(pv, reattach.KIND_HOME, "alice") is None


def test_a_retain_class_pv_still_gets_its_labels():
    # Retained by its storage class, but the uninstall hook selects on the
    # label, so it must still be marked.
    patch = reattach.secure_patch(_pv(phase="Bound"), reattach.KIND_POD_HOME,
                                  "alice")
    assert "spec" not in patch
    assert patch["metadata"]["labels"][reattach.USER_DATA_LABEL] == "pod-home"


def test_release_hands_the_disk_back_to_the_provisioner():
    assert reattach.release_patch() == {
        "spec": {"persistentVolumeReclaimPolicy": "Delete"}}


# --- finding the disk again -------------------------------------------------- #

def test_a_recorded_pv_is_given_back():
    pv, problem = reattach.find_reattachable([_pv()], NS, CLAIM,
                                             recorded="pv-1")
    assert problem is None and pv["metadata"]["name"] == "pv-1"


def test_a_recorded_pv_that_is_gone_is_an_error_never_a_new_disk():
    pv, problem = reattach.find_reattachable([], NS, CLAIM, recorded="pv-1")
    assert pv is None
    assert "pv-1" in problem and "no longer exists" in problem


def test_a_recorded_pv_bound_to_someone_else_is_refused():
    other = _pv(phase="Bound", claim_ns="whistler-user-bob", claim="theirs")
    pv, problem = reattach.find_reattachable([other], NS, CLAIM,
                                             recorded="pv-1")
    assert pv is None and "whistler-user-bob/theirs" in problem


def test_an_unrecorded_claim_finds_its_released_pv_by_claim_ref():
    # Covers a home secured before it was recorded, and the pod home, which
    # has no CR to record on.
    pvs = [_pv("pv-other", claim="whistler-home-other"), _pv("pv-mine")]
    pv, problem = reattach.find_reattachable(pvs, NS, CLAIM)
    assert problem is None and pv["metadata"]["name"] == "pv-mine"


def test_same_claim_name_in_another_namespace_is_not_ours():
    pv, problem = reattach.find_reattachable(
        [_pv(claim_ns="whistler-user-bob")], NS, CLAIM)
    assert (pv, problem) == (None, None)


def test_nothing_to_give_back_means_provision():
    assert reattach.find_reattachable([], NS, CLAIM) == (None, None)


def test_a_bound_pv_is_never_taken():
    # Bound means some live claim holds it; only Released/Available are free.
    assert reattach.find_reattachable([_pv(phase="Bound")], NS, CLAIM) == (
        None, None)


def test_two_candidates_are_not_guessed_between():
    pv, problem = reattach.find_reattachable([_pv("pv-a"), _pv("pv-b")],
                                             NS, CLAIM)
    assert pv is None and "pv-a" in problem and "pv-b" in problem


# --- binding it --------------------------------------------------------------- #

def test_rebind_points_the_claim_ref_at_the_new_claim_without_the_old_uid():
    # A claimRef that keeps the old claim's uid never matches the new claim,
    # and the PV stays Released forever.
    pv_patch, _pvc = reattach.rebind_manifests(_pv(), NS, CLAIM, {})
    ref = pv_patch["spec"]["claimRef"]
    assert (ref["namespace"], ref["name"]) == (NS, CLAIM)
    assert ref["uid"] is None and ref["resourceVersion"] is None


def test_rebind_claim_matches_what_the_binder_compares():
    _patch, pvc = reattach.rebind_manifests(
        _pv(capacity="50Gi"), NS, CLAIM, {"app": "whistler"})
    spec = pvc["spec"]
    assert spec["volumeName"] == "pv-1"
    assert spec["storageClassName"] == "nfs"
    assert spec["resources"]["requests"]["storage"] == "50Gi"
    assert spec["accessModes"] == ["ReadWriteOnce"]
    assert spec["volumeMode"] == "Filesystem"
    assert pvc["metadata"] == {"name": CLAIM, "namespace": NS,
                               "labels": {"app": "whistler"}}


def test_rebind_to_a_classless_pv_says_so_explicitly():
    # Absent, admission would default it to the cluster's default class, which
    # then does not match a PV with none, and the claim pends forever.
    _patch, pvc = reattach.rebind_manifests(_pv(storage_class=None), NS,
                                            CLAIM, {})
    assert pvc["spec"]["storageClassName"] == ""
