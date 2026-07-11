"""Portal create-instance form -> Session spec.overrides assembly
(whistler.portal.management._build_session_overrides)."""
from whistler.portal.management import _build_session_overrides


def test_nothing_filled_returns_none():
    assert _build_session_overrides() is None


def test_blank_strings_are_ignored():
    assert _build_session_overrides(cpu="", memory="", gpu_type="") is None


def test_only_cpu_filled_yields_resources_only():
    assert _build_session_overrides(cpu="500m") == {"resources": {"cpu": "500m"}}


def test_gpu_count_and_uid_gid_coerced_to_int():
    result = _build_session_overrides(gpu_count="2", uid="2000", gid="2001")
    assert result == {"gpuCount": 2, "uid": 2000, "gid": 2001}


def test_security_context_partial_fields():
    result = _build_session_overrides(run_as_user="2000")
    assert result == {"securityContext": {"runAsUser": 2000}}


def test_full_set_of_overrides():
    result = _build_session_overrides(
        volumes={"scratch": "/mnt/scratch"}, cpu="1", memory="2Gi",
        gpu_type="A100", gpu_count="2", uid="2000", gid="2001",
        run_as_user="2000", run_as_group="2001", fs_group="2001",
    )
    assert result == {
        "resources": {"cpu": "1", "memory": "2Gi"},
        "gpuType": "A100",
        "gpuCount": 2,
        "uid": 2000,
        "gid": 2001,
        "securityContext": {"runAsUser": 2000, "runAsGroup": 2001, "fsGroup": 2001},
        "volumes": {"scratch": "/mnt/scratch"},
    }
