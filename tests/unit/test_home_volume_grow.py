"""Growing a home volume (config.grow_home_volume, request_home_volume_size).

A home is a disk.img on a claim, so what has to grow is the file, then the
ext4 inside it (the guest's resize2fs, tests/unit/test_cloud_init.py). Where
the claim can be expanded, KubeVirt grows the file itself; where it cannot
(local-path, NFS — whose claim size is nominal anyway), Whistler grows the
file in place with a short pod, while no running instance holds it.
"""
import json
import os
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from whistler.config import (ARCHIVE_ANNOTATION, DELETE_DATA_ANNOTATION,
                             DISK_SIZE_ANNOTATION, HOME_GROW_SCRIPT,
                             KubeConfigManager, QEMU_UID,
                             RESIZE_PROBLEM_ANNOTATION, home_disk_size,
                             home_grow_pod_manifest, home_volume_size_problem)

NS = "whistler-user-alice"
GiB = 2 ** 30


def _manager(holder=None):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.home_disk_size = "20Gi"
    cm.self_image = "whistler:test"
    cm.self_image_pull_policy = "IfNotPresent"
    cm._get_user_namespace = lambda u: f"whistler-user-{u}"
    cm.home_volume_holder = lambda u, v, ignore_instance=None: holder
    cm.api = MagicMock()
    return cm


def _item(size="50Gi", annotations=None, **spec):
    return {"metadata": {"name": "research", "namespace": NS,
                         "annotations": annotations or {}},
            "spec": {"user": "alice", "size": size, **spec}}


def _pvc(requested="20Gi", capacity="20Gi"):
    return SimpleNamespace(
        spec=SimpleNamespace(resources=SimpleNamespace(
            requests={"storage": requested})),
        status=SimpleNamespace(capacity={"storage": capacity}))


def _pod(phase, message=""):
    term = SimpleNamespace(message=message)
    return SimpleNamespace(status=SimpleNamespace(
        phase=phase,
        container_statuses=[SimpleNamespace(
            state=SimpleNamespace(terminated=term))]))


def _refused(status=422):
    err = ApiException(status=status)
    err.body = json.dumps({"message": "only dynamically provisioned pvc can "
                                      "be resized and the storageclass that "
                                      "provisions the pvc must support resize"})
    return err


def _grow(item, pvc=None, patch_error=None, pod=None, holder=None, **kwargs):
    cm = _manager(holder=holder)
    cm.api.get_namespaced_custom_object.return_value = item
    core = MagicMock()
    if pvc is None:
        core.read_namespaced_persistent_volume_claim.side_effect = \
            ApiException(status=404)
    else:
        core.read_namespaced_persistent_volume_claim.return_value = pvc
    if patch_error:
        core.patch_namespaced_persistent_volume_claim.side_effect = patch_error
    core.read_namespaced_pod.return_value = pod or _pod("Succeeded", "grew")
    with patch("whistler.config.client.CoreV1Api", return_value=core), \
            patch("whistler.config.time.sleep"):
        ok = cm.grow_home_volume(NS, "research", **kwargs)
    return ok, core, cm


def _recorded(cm):
    """The annotations the last CR patch wrote."""
    return cm.api.patch_namespaced_custom_object.call_args[0][5][
        "metadata"]["annotations"]


# --- pure pieces --------------------------------------------------------------- #

def test_size_only_grows():
    assert home_volume_size_problem("50Gi", "20Gi") is None
    assert "only grow" in home_volume_size_problem("10Gi", "20Gi")
    assert "already" in home_volume_size_problem("20480Mi", "20Gi")


def test_any_size_before_the_claim_exists():
    assert home_volume_size_problem("5Gi", None) is None


@pytest.mark.parametrize("bad", ["", None, "lots", "0", "-5Gi"])
def test_size_must_be_a_positive_quantity(bad):
    assert "quantity" in home_volume_size_problem(bad, "20Gi")


def test_disk_size_is_the_smaller_claim_figure_or_what_was_grown():
    # KubeVirt makes the file min(request, capacity).
    assert home_disk_size("50Gi", "20Gi") == "20Gi"
    assert home_disk_size("20Gi", "21474836480") == "20Gi"
    # A file grown directly is bigger than its unexpandable claim says.
    assert home_disk_size("20Gi", "20Gi", "100Gi") == "100Gi"
    assert home_disk_size(None, None) is None


def test_grow_pod_runs_as_the_disk_owner_with_nothing_else():
    pod = home_grow_pod_manifest("research", "whistler-home-research",
                                 50 * GiB, image="whistler:test",
                                 pull_policy="IfNotPresent")
    spec = pod["spec"]
    assert spec["securityContext"]["runAsUser"] == QEMU_UID
    assert spec["automountServiceAccountToken"] is False
    assert spec["restartPolicy"] == "Never"
    c = spec["containers"][0]
    assert c["command"][-2:] == ["/disk/disk.img", str(50 * GiB)]
    assert c["securityContext"]["readOnlyRootFilesystem"] is True
    assert spec["volumes"] == [{"name": "home", "persistentVolumeClaim": {
        "claimName": "whistler-home-research"}}]


# --- the script, against real files ------------------------------------------- #

def _script(tmp_path, path, size):
    script = tmp_path / "grow.py"
    script.write_text(HOME_GROW_SCRIPT)
    return subprocess.run([sys.executable, "-I", str(script), str(path),
                           str(size)], capture_output=True, text=True)


def test_script_grows_sparse_and_never_shrinks(tmp_path):
    disk = tmp_path / "disk.img"
    disk.write_bytes(b"\x01" * 4096)
    r = _script(tmp_path, disk, 64 * 2 ** 20)
    assert r.returncode == 0 and disk.stat().st_size == 64 * 2 ** 20
    assert disk.read_bytes()[:4096] == b"\x01" * 4096   # data untouched
    assert disk.stat().st_blocks * 512 < 2 ** 20          # still sparse
    r = _script(tmp_path, disk, 2 ** 20)
    assert r.returncode == 0 and disk.stat().st_size == 64 * 2 ** 20


def test_script_creates_a_missing_image(tmp_path):
    disk = tmp_path / "disk.img"
    assert _script(tmp_path, disk, 2 ** 20).returncode == 0
    assert disk.stat().st_size == 2 ** 20


def test_script_refuses_more_than_the_volume_can_hold(tmp_path):
    disk = tmp_path / "disk.img"
    disk.write_bytes(b"")
    free = os.statvfs(tmp_path).f_bavail * os.statvfs(tmp_path).f_frsize
    r = _script(tmp_path, disk, free * 2 + 2 ** 30)
    assert r.returncode == 1 and "room for" in r.stdout
    assert disk.stat().st_size == 0


# --- the operator's grow ------------------------------------------------------- #

def test_an_expandable_claim_is_grown_and_kubevirt_does_the_file():
    ok, core, _ = _grow(_item("50Gi"), pvc=_pvc("20Gi"))
    assert ok
    core.patch_namespaced_persistent_volume_claim.assert_called_once_with(
        "whistler-home-research", NS,
        {"spec": {"resources": {"requests": {"storage": "50Gi"}}}})
    core.create_namespaced_pod.assert_not_called()


@pytest.mark.parametrize("status", [403, 422])
def test_an_unexpandable_claim_has_its_file_grown(status):
    ok, core, cm = _grow(_item("50Gi"), pvc=_pvc("20Gi"),
                         patch_error=_refused(status))
    assert ok
    pod = core.create_namespaced_pod.call_args[0][1]
    assert pod["spec"]["containers"][0]["command"][-1] == str(50 * GiB)
    core.delete_namespaced_pod.assert_called_once()
    assert _recorded(cm) == {DISK_SIZE_ANNOTATION: "50Gi"}


def test_a_held_home_waits_for_its_next_start():
    ok, core, cm = _grow(_item("50Gi"), pvc=_pvc("20Gi"),
                         patch_error=_refused(), holder="alice-desk")
    assert ok   # not a failure: it happens at the next start
    core.create_namespaced_pod.assert_not_called()
    cm.api.patch_namespaced_custom_object.assert_not_called()


def test_a_failed_file_grow_is_recorded_and_the_pod_removed():
    ok, core, cm = _grow(_item("50Gi"), pvc=_pvc("20Gi"),
                         patch_error=_refused(),
                         pod=_pod("Failed", "the volume has room for 30 MiB"))
    assert not ok
    core.delete_namespaced_pod.assert_called_once()
    value = _recorded(cm)[RESIZE_PROBLEM_ANNOTATION]
    assert value.startswith("50Gi: ") and "room for" in value


def test_another_api_error_is_recorded_without_touching_the_file():
    ok, core, cm = _grow(_item("50Gi"), pvc=_pvc("20Gi"),
                         patch_error=ApiException(status=500))
    assert not ok
    core.create_namespaced_pod.assert_not_called()
    assert _recorded(cm)[RESIZE_PROBLEM_ANNOTATION].startswith("50Gi: ")


@pytest.mark.parametrize("size", ["20Gi", "10Gi"])
def test_nothing_happens_to_a_disk_already_that_big(size):
    ok, core, _ = _grow(_item(size), pvc=_pvc("20Gi"))
    assert ok
    core.patch_namespaced_persistent_volume_claim.assert_not_called()
    core.create_namespaced_pod.assert_not_called()


def test_a_file_already_grown_is_not_grown_again():
    ok, core, _ = _grow(_item("50Gi", {DISK_SIZE_ANNOTATION: "50Gi"}),
                        pvc=_pvc("20Gi"))
    assert ok
    core.read_namespaced_persistent_volume_claim.assert_not_called()


def test_a_claim_resize_in_progress_is_waited_for():
    # Request raised, capacity not yet: KubeVirt grows the file once it lands.
    ok, core, _ = _grow(_item("50Gi"), pvc=_pvc("50Gi", "20Gi"))
    assert ok
    core.patch_namespaced_persistent_volume_claim.assert_not_called()
    core.create_namespaced_pod.assert_not_called()


def test_no_claim_yet_needs_nothing():
    ok, core, _ = _grow(_item("50Gi"))
    assert ok
    core.patch_namespaced_persistent_volume_claim.assert_not_called()


@pytest.mark.parametrize("item", [
    _item(annotations={DELETE_DATA_ANNOTATION: "true"}),
    _item(annotations={ARCHIVE_ANNOTATION: "admin"}),
    _item(fromArchive="old"),
    _item(size=None),
])
def test_leaves_volumes_being_moved_or_unsized_alone(item):
    ok, core, _ = _grow(item, pvc=_pvc("20Gi"))
    assert ok
    core.read_namespaced_persistent_volume_claim.assert_not_called()


def test_the_sweep_does_not_retry_a_size_that_already_failed():
    item = _item("50Gi", annotations={RESIZE_PROBLEM_ANNOTATION: "50Gi: no"})
    cm = _manager()
    core = MagicMock()
    with patch("whistler.config.client.CoreV1Api", return_value=core):
        cm.grow_home_volume(NS, "research", item=item, retry_recorded=False)
    cm.api.get_namespaced_custom_object.assert_not_called()
    core.read_namespaced_persistent_volume_claim.assert_not_called()


def test_a_start_does_retry_it():
    # The admin may have fixed the storage since.
    item = _item("50Gi", annotations={RESIZE_PROBLEM_ANNOTATION: "50Gi: no"})
    ok, core, cm = _grow(item, pvc=_pvc("20Gi"))
    assert ok
    core.patch_namespaced_persistent_volume_claim.assert_called_once()
    assert _recorded(cm) == {RESIZE_PROBLEM_ANNOTATION: None}


# --- the portal's request ------------------------------------------------------ #

def _request(size, disk="20Gi", volume=None):
    cm = _manager()
    cm.get_home_volume = lambda u, n: volume if volume is not None else \
        {"name": n, "user": u}
    cm.home_volume_claim_size = lambda u, v: {"requested": "20Gi",
                                              "capacity": "20Gi",
                                              "disk": disk, "pending": False}
    return cm.request_home_volume_size("alice", "research", size), cm


def test_request_writes_the_size_and_clears_an_old_refusal():
    (ok, _msg), cm = _request("50Gi")
    assert ok
    args = cm.api.patch_namespaced_custom_object.call_args[0]
    assert args[2] == NS and args[4] == "research"
    assert args[5] == {
        "metadata": {"annotations": {RESIZE_PROBLEM_ANNOTATION: None}},
        "spec": {"size": "50Gi"}}


def test_request_is_checked_against_the_disk_not_the_claim():
    # A file grown past its claim cannot be "grown" back to the claim's size.
    (ok, msg), cm = _request("50Gi", disk="100Gi")
    assert not ok and "only grow" in msg
    cm.api.patch_namespaced_custom_object.assert_not_called()


def test_request_refused_while_restoring_from_the_archive():
    (ok, msg), _ = _request("50Gi", volume={"name": "research",
                                            "fromArchive": "old"})
    assert not ok and "archive" in msg
