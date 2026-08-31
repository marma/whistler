"""Portal create-instance form -> Session spec.overrides assembly
(whistler.portal.management._build_session_overrides)."""
from whistler.config import GPU_NONE
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
        cpu="1", memory="2Gi", zone="restricted",
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
        "zone": "restricted",
    }


# --- "No GPU" --------------------------------------------------------------- #

def test_no_gpu_is_a_gpu_type_value_not_a_zero_count():
    """It rides the gpuType field so it needs only the gpuType grant. Saying it
    as `gpuCount: 0` would have required the *other* grant to turn a card off,
    and the operator would still have had to translate the zero into an
    absence — resources.gpu: 0 attaches a GPU (see test_vm_spec)."""
    assert _build_session_overrides(gpu_type=GPU_NONE) == {"gpuType": GPU_NONE}


def test_no_gpu_discards_a_count_rather_than_writing_the_contradiction():
    # The dialog disables the count box when No GPU is picked; a submission
    # that slipped past that must not ask for a card and forbid one at once.
    # _apply_overrides resolves it the same way — this is just not writing it
    # down.
    assert _build_session_overrides(gpu_type=GPU_NONE, gpu_count="4") == {
        "gpuType": GPU_NONE}


def test_an_ordinary_type_still_carries_its_count():
    assert _build_session_overrides(gpu_type="A100", gpu_count="2") == {
        "gpuType": "A100", "gpuCount": 2}
