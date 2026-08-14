"""ssh-mode sessions whose runtime is a VM (images/devbase).

`mode: ssh` used to imply "a pod": get_user_instances matched sessions to pods
labelled user=/instance=, and a KubeVirt VM has neither (its launcher pod is
virt-launcher-* with KubeVirt's own labels). The result was a healthy, running,
SSH-reachable devbase VM permanently reported as "Stopped" — and, in
list_ssh_targets, as an unreachable container the launcher refused to connect
to. These tests pin both halves.
"""
import pytest

from whistler.config import KubeConfigManager


class _FakeApi:
    """Minimal CustomObjectsApi: Sessions per namespace + VMIs per namespace."""

    def __init__(self, sessions, vmis=()):
        self.sessions = list(sessions)
        self.vmis = list(vmis)
        self.calls = []

    def list_namespaced_custom_object(self, group, version, ns, plural, **kw):
        self.calls.append(plural)
        if plural == "virtualmachineinstances":
            return {"items": self.vmis}
        return {"items": self.sessions}


class _FakePod:
    def __init__(self, name, instance, user, phase="Running", ready=True):
        self.metadata = type("M", (), {
            "name": name,
            "labels": {"instance": instance, "user": user},
            "deletion_timestamp": None,
        })()
        cs = type("CS", (), {"ready": ready})()
        self.status = type("S", (), {
            "phase": phase, "pod_ip": "10.42.0.9", "container_statuses": [cs],
        })()
        self.spec = type("Sp", (), {"containers": []})()


def _manager(monkeypatch, sessions, vmis=(), pods=()):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.api = _FakeApi(sessions, vmis)
    cm.group, cm.version = "whistler.martinmalmsten.net", "v1"

    class _FakeCore:
        def list_namespaced_pod(self, ns, **kw):
            return type("L", (), {"items": list(pods)})()

    monkeypatch.setattr("whistler.config.client.CoreV1Api", lambda: _FakeCore())
    return cm


def _session(name, runtime, phase, **status):
    return {
        "metadata": {"name": name},
        "spec": {"templateRef": "devbase-cuda-dev"},
        "status": {"runtime": runtime, "phase": phase, **status},
    }


def test_ssh_vm_session_reports_its_operator_phase_not_stopped(monkeypatch):
    cm = _manager(monkeypatch, [
        _session("marma-devbox", "vm", "Ready", vmiName="marma-devbox"),
    ])
    (inst,) = cm.get_user_instances("marma")
    assert inst["status"] == "Ready"      # not "Stopped"
    assert inst["ready"] is True
    assert inst["runtime"] == "vm"
    assert inst["vmiName"] == "marma-devbox"


def test_ssh_container_session_still_derives_from_its_pod(monkeypatch):
    cm = _manager(
        monkeypatch,
        [_session("marma-box", "container", "Ready")],
        pods=[_FakePod("marma-box-abc", "marma-box", "marma")],
    )
    (inst,) = cm.get_user_instances("marma")
    assert inst["status"] == "Running"    # the live pod, not the CR phase
    assert inst["ready"] is True
    assert inst["runtime"] == "container"
    assert inst["podName"] == "marma-box-abc"


def test_no_vmi_list_when_no_vm_sessions(monkeypatch):
    """The extra API call is only worth making when a VM is actually present."""
    cm = _manager(monkeypatch, [_session("marma-box", "container", "Ready")])
    cm.get_user_instances("marma")
    assert "virtualmachineinstances" not in cm.api.calls


def test_deleting_vmi_shows_terminating(monkeypatch):
    cm = _manager(
        monkeypatch,
        [_session("marma-devbox", "vm", "Ready", vmiName="marma-devbox")],
        vmis=[{"metadata": {"name": "marma-devbox",
                            "deletionTimestamp": "2026-08-11T19:00:00Z"}}],
    )
    (inst,) = cm.get_user_instances("marma")
    assert inst["status"] == "Terminating"
    assert inst["ready"] is False


def test_deleting_session_cr_beats_the_reported_phase(monkeypatch):
    sess = _session("marma-devbox", "vm", "Ready", vmiName="marma-devbox")
    sess["metadata"]["deletionTimestamp"] = "2026-08-11T19:00:00Z"
    cm = _manager(monkeypatch, [sess])
    (inst,) = cm.get_user_instances("marma")
    assert inst["status"] == "Terminating"


@pytest.mark.parametrize("runtime,reachable", [("vm", True), ("container", False)])
def test_ssh_targets_reachability_follows_the_runtime(monkeypatch, runtime, reachable):
    """The launcher must offer the VM (it runs sshd) and refuse the container
    (no sshd since the kubectl-exec bridge was removed)."""
    cm = _manager(monkeypatch, [_session("marma-devbox", runtime, "Ready",
                                         vmiName="marma-devbox")])
    monkeypatch.setattr(cm, "get_user_desktop_sessions", lambda u: [])
    (target,) = cm.list_ssh_targets("marma")
    assert target["runtime"] == runtime
    assert target["sshReachable"] is reachable
