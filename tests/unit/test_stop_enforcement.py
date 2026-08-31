"""The operator half of stop: reconciling a workload *toward* stopped.

Stop is an annotation write (tests/unit/test_run_intent.py); this is what acts
on it. Both directions live in the reconcile path so that no other process
needs a KubeVirt or pod-delete verb to stop something.

The bug this closes is not only about privilege. `_create_vm` used to flip
runStrategy to Always when a start was wanted and do *nothing* otherwise, so
the run state of an existing VM was only ever written by whoever asked for the
stop — and a stale start annotation (they were never cleared) could bring a
stopped session back up on any unrelated reconcile.

The running strategy is RerunOnFailure rather than Always so that a guest
which powers itself off stays off. That costs one thing, covered at the
bottom: KubeVirt starts such a VM on a strategy *change*, so a start that
finds the strategy already set has to ask explicitly.
"""
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException

from whistler.config import KUBEVIRT_VM_PLURAL, KubeConfigManager


def _manager(vm_before=None):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.zones = {"default": {}}
    cm.api = MagicMock()
    cm.get_gpu_catalog = lambda: []
    cm.get_user = lambda u: {"name": u, "uid": 1001, "publicKeys": []}
    cm.ensure_session_host_cert = lambda *a, **k: (None, None)
    cm.session_ssh_principals = lambda *a, **k: ["box.w"]
    cm._ensure_vm_access_key = lambda *a, **k: "ssh-ed25519 AAAA portal"
    cm._patch_vm_spec = lambda *a, **k: vm_before
    cm._get_user_namespace = lambda u: f"whistler-user-{u}"
    return cm


def _reconcile_existing_vm(start, vm_before=None):
    """Run _create_vm against a cluster where the VM already exists (409),
    and return the runStrategy patches it issued.

    `vm_before` is the VirtualMachine as _patch_vm_spec read it — the view the
    start-request decision is made from."""
    cm = _manager(vm_before)
    cm.api.create_namespaced_custom_object.side_effect = ApiException(status=409)
    with patch("whistler.config.client.CoreV1Api", return_value=MagicMock()):
        ok = cm._create_vm(
            "whistler-user-alice", "alice-box", "box", "alice", "uid-1",
            {"image": "example/devbase:latest"}, 5900, None, False,
            home_pvc="whistler-home-alice-box", start=start)
    patches = [c[0][5] for c in cm.api.patch_namespaced_custom_object.call_args_list
               if c[0][3] == KUBEVIRT_VM_PLURAL]
    return ok, [p["spec"]["runStrategy"] for p in patches
                if "runStrategy" in (p.get("spec") or {})]


def test_an_existing_vm_is_halted_when_the_intent_is_stopped():
    ok, strategies = _reconcile_existing_vm(start=False)
    assert ok is True
    assert strategies == ["Halted"]


def test_an_existing_vm_is_started_when_the_intent_is_running():
    ok, strategies = _reconcile_existing_vm(start=True)
    assert ok is True
    assert strategies == ["RerunOnFailure"]


def test_a_new_vm_is_created_halted_and_needs_no_patch():
    """Creation already encodes the intent in the manifest, so there is
    nothing to reconcile — and a session no one has connected to must not
    boot."""
    cm = _manager()
    with patch("whistler.config.client.CoreV1Api", return_value=MagicMock()):
        ok = cm._create_vm(
            "whistler-user-alice", "alice-box", "box", "alice", "uid-1",
            {"image": "example/devbase:latest"}, 5900, None, False,
            home_pvc="whistler-home-alice-box", start=False)
    body = cm.api.create_namespaced_custom_object.call_args[0][4]
    assert ok is True
    assert body["spec"]["runStrategy"] == "Halted"
    assert cm.api.patch_namespaced_custom_object.call_count == 0


# --- starting a VM the strategy patch will not start ------------------------ #
# virt-controller starts a RerunOnFailure VM with no VMI only when the strategy
# just changed, or when something asked outright. Whistler's ordinary start is
# a Halted -> RerunOnFailure patch and so is the former; a guest that powered
# ITSELF off leaves the spec already saying RerunOnFailure, where the same
# patch changes nothing and the play button would silently do nothing.


def _start_requests(cm):
    return [c for c in cm.api.api_client.call_api.call_args_list
            if c[0][0].endswith("/start")]


def test_a_start_after_a_guest_power_off_is_requested_explicitly():
    cm = _manager({"status": {"runStrategy": "RerunOnFailure",
                              "printableStatus": "Stopped"}})
    cm.api.create_namespaced_custom_object.side_effect = ApiException(status=409)
    with patch("whistler.config.client.CoreV1Api", return_value=MagicMock()):
        cm._create_vm(
            "whistler-user-alice", "alice-box", "box", "alice", "uid-1",
            {"image": "example/devbase:latest"}, 5900, None, False,
            home_pvc="whistler-home-alice-box", start=True)
    calls = _start_requests(cm)
    assert len(calls) == 1
    assert calls[0][0][1] == "PUT"
    assert calls[0][0][2]["name"] == "alice-box"


def test_an_ordinary_start_needs_no_start_request():
    """The VM was halted, so the strategy patch is itself the start. Asking
    again would be a durable mark in the VM's status for no reason."""
    cm = _manager({"status": {"runStrategy": "Halted",
                              "printableStatus": "Stopped"}})
    cm.api.create_namespaced_custom_object.side_effect = ApiException(status=409)
    with patch("whistler.config.client.CoreV1Api", return_value=MagicMock()):
        cm._create_vm(
            "whistler-user-alice", "alice-box", "box", "alice", "uid-1",
            {"image": "example/devbase:latest"}, 5900, None, False,
            home_pvc="whistler-home-alice-box", start=True)
    assert _start_requests(cm) == []


def test_a_running_vm_is_never_sent_a_start_request():
    """A start request written while the guest is on its way down (or already
    up) would restart the machine the user just powered off."""
    for status in ({"runStrategy": "RerunOnFailure", "printableStatus": "Running",
                    "created": True, "ready": True},
                   {"runStrategy": "RerunOnFailure", "printableStatus": "Starting",
                    "created": True}):
        cm = _manager({"status": status})
        cm.api.create_namespaced_custom_object.side_effect = ApiException(status=409)
        with patch("whistler.config.client.CoreV1Api", return_value=MagicMock()):
            cm._create_vm(
                "whistler-user-alice", "alice-box", "box", "alice", "uid-1",
                {"image": "example/devbase:latest"}, 5900, None, False,
                home_pvc="whistler-home-alice-box", start=True)
        assert _start_requests(cm) == []


def test_a_stop_never_sends_a_start_request():
    cm = _manager({"status": {"runStrategy": "RerunOnFailure",
                              "printableStatus": "Stopped"}})
    cm.api.create_namespaced_custom_object.side_effect = ApiException(status=409)
    with patch("whistler.config.client.CoreV1Api", return_value=MagicMock()):
        cm._create_vm(
            "whistler-user-alice", "alice-box", "box", "alice", "uid-1",
            {"image": "example/devbase:latest"}, 5900, None, False,
            home_pvc="whistler-home-alice-box", start=False)
    assert _start_requests(cm) == []


# --- the pod half ----------------------------------------------------------- #

def test_stopping_a_pod_session_deletes_the_pod():
    cm = _manager()
    core = MagicMock()
    with patch("whistler.config.client.CoreV1Api", return_value=core):
        assert cm._delete_session_pod("whistler-user-alice", "alice-scratch")
    core.delete_namespaced_pod.assert_called_once_with(
        "alice-scratch", "whistler-user-alice")


def test_an_already_absent_pod_is_the_desired_state():
    cm = _manager()
    core = MagicMock()
    core.delete_namespaced_pod.side_effect = ApiException(status=404)
    with patch("whistler.config.client.CoreV1Api", return_value=core):
        assert cm._delete_session_pod("whistler-user-alice", "alice-scratch")


def test_a_real_pod_delete_failure_is_reported():
    cm = _manager()
    core = MagicMock()
    core.delete_namespaced_pod.side_effect = ApiException(status=403)
    with patch("whistler.config.client.CoreV1Api", return_value=core):
        assert cm._delete_session_pod("whistler-user-alice", "alice-scratch") is False
