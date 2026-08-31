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
        ("virtualmachineinstances", "alice-desk"): {"status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        }},
    })
    assert (phase, name) == ("Ready", "alice-desk")
    assert address == "alice-desk.ns.svc.cluster.local"


def test_probe_running_vmi_without_ready_condition_is_booting(monkeypatch):
    # phase Running only means the domain booted. The readinessProbe on the VMI
    # (see _build_vm_spec) is what says the guest actually serves the display /
    # sshd, and it surfaces as the Ready condition — reporting Ready before it
    # flips sends the portal's connect page at a port nothing is listening on.
    phase, _, address = _probe(monkeypatch, {
        ("virtualmachineinstances", "alice-desk"): {"status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "False"}],
        }},
    })
    assert phase == "Booting"
    assert address is None


def test_probe_failed_vmi_is_failed(monkeypatch):
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachineinstances", "alice-desk"): {"status": {"phase": "Failed"}},
    })
    assert phase == "Failed"


def test_probe_succeeded_vmi_is_stopped(monkeypatch):
    # The domain exited gracefully: the guest shut itself down, or obeyed a
    # halt. Either way nothing is running.
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
        ("virtualmachines", "alice-desk"): {
            "spec": {"runStrategy": "RerunOnFailure"}},
        ("datavolumes", "alice-desk-root"): {"status": {"phase": "ImportInProgress"}},
    })
    assert phase == "Importing"


def test_probe_imported_data_volume_is_booting(monkeypatch):
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachines", "alice-desk"): {
            "spec": {"runStrategy": "RerunOnFailure"}},
        ("datavolumes", "alice-desk-root"): {"status": {"phase": "Succeeded"}},
    })
    assert phase == "Booting"


def test_probe_container_disk_vm_without_vmi_is_booting(monkeypatch):
    # No DataVolume at all (containerDisk boot, or CDI absent) -> Booting.
    phase, _, _ = _probe(monkeypatch, {
        ("virtualmachines", "alice-desk"): {
            "spec": {"runStrategy": "RerunOnFailure"}},
    })
    assert phase == "Booting"


# --- the guest's own power-off --------------------------------------------- #
# `Power off` in the desktop menu, or `sudo poweroff`. Under RerunOnFailure
# KubeVirt leaves such a VM stopped instead of respawning it, and the operator
# has to turn that into a stop the Session CR agrees with — otherwise the CR
# goes on saying "run", any later reconcile boots the guest back up, and the
# next deliberate start is a no-op patch that changes nothing.


def test_a_powered_off_guest_is_stopped_not_booting(monkeypatch):
    # Before this, a VM with no VMI that Whistler had not halted fell through
    # to Booting — and stayed there forever, because nothing was coming.
    phase, name, _ = _probe(monkeypatch, {
        ("virtualmachines", "alice-desk"): {
            "spec": {"runStrategy": "RerunOnFailure"},
            "status": {"runStrategy": "RerunOnFailure",
                       "printableStatus": "Stopped"},
        },
    })
    assert (phase, name) == ("Stopped", "alice-desk")


def test_guest_powered_off_reads_the_observed_strategy_not_the_asked_one():
    """A start in flight looks identical from the outside: no VMI, spec says
    run. `status.runStrategy` is the one field that separates them — it is
    what virt-controller has acted on, and it still says Halted until it
    catches up."""
    assert not operator._guest_powered_off({
        "spec": {"runStrategy": "RerunOnFailure"},
        "status": {"runStrategy": "Halted", "printableStatus": "Stopped"},
    })


def test_guest_powered_off_is_false_while_kubevirt_is_starting_it():
    assert not operator._guest_powered_off({
        "status": {"runStrategy": "RerunOnFailure",
                   "printableStatus": "Starting"},
    })


def test_a_crash_kubevirt_will_restart_is_not_a_power_off():
    """RerunOnFailure answers a failed VMI by queueing a start. Calling that a
    stop would take away the automatic restart the strategy exists for."""
    assert not operator._guest_powered_off({
        "status": {"runStrategy": "RerunOnFailure",
                   "printableStatus": "Stopped",
                   "stateChangeRequests": [{"action": "Start"}]},
    })


def test_guest_powered_off_survives_a_status_it_does_not_recognise():
    for status in ({}, {"runStrategy": "RerunOnFailure"}, {"printableStatus": "Stopped"}):
        assert not operator._guest_powered_off({"status": status})
    assert not operator._guest_powered_off({})
    assert not operator._guest_powered_off(None)


class _Patch(dict):
    """The slice of kopf's Patch these helpers touch."""
    def __init__(self):
        super().__init__()
        self.meta = {}
        self.status = {}


def _record(monkeypatch, objects):
    _FakeCustomObjects.objects = objects
    monkeypatch.setattr(k8s_client, "CustomObjectsApi", _FakeCustomObjects)
    patch = _Patch()
    operator._record_guest_shutdown("ns", "alice-desk", patch, _LOG)
    return patch


def test_a_power_off_is_recorded_as_a_stop(monkeypatch):
    patch = _record(monkeypatch, {
        ("virtualmachines", "alice-desk"): {
            "status": {"runStrategy": "RerunOnFailure",
                       "printableStatus": "Stopped"},
        },
    })
    stop = patch.meta["annotations"]["whistler/last-stop"]
    # An epoch float, the same units the start mark is written in — run_intent
    # compares the two directly.
    assert float(stop) > 0


def test_a_start_in_flight_is_not_recorded_as_a_stop(monkeypatch):
    """The phase probe says Stopped here too; only the VM itself can tell the
    difference, which is why this re-reads it rather than trusting the phase."""
    patch = _record(monkeypatch, {
        ("virtualmachines", "alice-desk"): {
            "spec": {"runStrategy": "RerunOnFailure"},
            "status": {"runStrategy": "Halted", "printableStatus": "Stopped"},
        },
    })
    assert patch.meta == {}


def test_a_vanished_vm_is_not_recorded_as_a_stop(monkeypatch):
    assert _record(monkeypatch, {}).meta == {}
