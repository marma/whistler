"""Operator helper logic that needs no cluster."""
from whistler.operator import _instance_short_name, _map_pod_phase, _desktop_address


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
