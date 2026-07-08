"""Operator helper logic that needs no cluster."""
import logging

from kubernetes import client as k8s_client
from kubernetes.client.rest import ApiException

from whistler import operator
from whistler.operator import (
    _instance_short_name,
    _map_pod_phase,
    _desktop_address,
    _probe_vmi,
)


def test_strips_user_prefix():
    assert _instance_short_name("alice-box", "alice") == "box"


def test_preserves_dashes_in_instance_name():
    assert _instance_short_name("alice-my-box", "alice") == "my-box"


def test_returns_name_unchanged_without_prefix():
    assert _instance_short_name("box", "alice") == "box"


def test_map_pod_phase_running_and_ready_is_ready():
    assert _map_pod_phase("Running", True) == "Ready"


def test_map_pod_phase_running_but_not_ready_is_booting():
    assert _map_pod_phase("Running", False) == "Booting"


def test_map_pod_phase_pending_is_booting():
    assert _map_pod_phase("Pending", False) == "Booting"


def test_map_pod_phase_failed_is_failed():
    assert _map_pod_phase("Failed", False) == "Failed"


def test_desktop_address_is_service_dns():
    assert _desktop_address("alice-desk", "whistler-user-alice") == \
        "alice-desk.whistler-user-alice.svc.cluster.local"


# --- _probe_vmi phase machine ---------------------------------------------- #
# Driven with a fake CustomObjectsApi: keys are (plural, name); a missing key
# raises 404 exactly like a cluster without the object (or without the CRDs).

_LOG = logging.getLogger("test")


class _FakeCustomObjects:
    objects = {}

    def get_namespaced_custom_object(self, group, version, namespace, plural, name):
        try:
            return self.objects[(plural, name)]
        except KeyError:
            raise ApiException(status=404)


def _probe(monkeypatch, objects):
    _FakeCustomObjects.objects = objects
    monkeypatch.setattr(k8s_client, "CustomObjectsApi", _FakeCustomObjects)
    return _probe_vmi("ns", "alice-desk", _LOG)


def test_probe_running_vmi_is_ready_with_address(monkeypatch):
    phase, name, address = _probe(monkeypatch, {
        ("virtualmachineinstances", "alice-desk"): {"status": {"phase": "Running"}},
    })
    assert (phase, name) == ("Ready", "alice-desk")
    assert address == "alice-desk.ns.svc.cluster.local"


def test_probe_failed_vmi_is_failed(monkeypatch):
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachineinstances", "alice-desk"): {"status": {"phase": "Failed"}},
    })
    assert phase == "Failed"


def test_probe_succeeded_vmi_is_stopped(monkeypatch):
    # Guest shut itself down under runStrategy Halted.
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachineinstances", "alice-desk"): {"status": {"phase": "Succeeded"}},
    })
    assert phase == "Stopped"


def test_probe_draining_vmi_is_terminating(monkeypatch):
    # A stopped/deleted VM's VMI keeps phase Running while the guest shuts
    # down; the deletionTimestamp is what marks the teardown.
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachineinstances", "alice-desk"): {
            "metadata": {"deletionTimestamp": "2026-07-08T10:00:00Z"},
            "status": {"phase": "Running"},
        },
    })
    assert phase == "Terminating"


def test_probe_scheduling_vmi_is_booting(monkeypatch):
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachineinstances", "alice-desk"): {"status": {"phase": "Scheduling"}},
    })
    assert phase == "Booting"


def test_probe_no_vmi_no_vm_is_stopped(monkeypatch):
    # Covers both "VM deleted" and "KubeVirt CRDs absent" — never crashes.
    phase, name, _ = _probe(monkeypatch, {})
    assert (phase, name) == ("Stopped", None)


def test_probe_halted_vm_is_stopped(monkeypatch):
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachines", "alice-desk"): {"spec": {"runStrategy": "Halted"}},
    })
    assert phase == "Stopped"


def test_probe_importing_data_volume_is_importing(monkeypatch):
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachines", "alice-desk"): {"spec": {"runStrategy": "Always"}},
        ("datavolumes", "alice-desk-root"): {"status": {"phase": "ImportInProgress"}},
    })
    assert phase == "Importing"


def test_probe_imported_data_volume_is_booting(monkeypatch):
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachines", "alice-desk"): {"spec": {"runStrategy": "Always"}},
        ("datavolumes", "alice-desk-root"): {"status": {"phase": "Succeeded"}},
    })
    assert phase == "Booting"


def test_probe_container_disk_vm_without_vmi_is_booting(monkeypatch):
    # No DataVolume at all (containerDisk boot, or CDI absent) -> Booting.
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachines", "alice-desk"): {"spec": {"runStrategy": "Always"}},
    })
    assert phase == "Booting"
