"""Per-session overrides (KubeConfigManager._apply_overrides).

Covers the merge of a session's spec.overrides into the template/user
details used to build its pod or VM, gated by the owning user's granted
override groups (users.yaml `overrides`)."""
import pytest

from whistler.config import GPU_NODE_LABEL, KubeConfigManager, PolicyError


def _manager(*, users=None):
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.users = users or {}
    return cm


def _user(name, **overrides):
    return {"name": name, "overrides": overrides}


# --- no overrides requested ----------------------------------------------- #

def test_no_overrides_returns_inputs_unchanged():
    cm = _manager()
    template_spec = {"image": "x", "resources": {"cpu": "1"}}
    user_details = {"name": "alice"}
    spec, user = cm._apply_overrides(template_spec, user_details, None, "alice")
    assert spec == template_spec
    assert user == user_details


# --- capability gating ----------------------------------------------------- #

def test_ungranted_resources_override_raises():
    cm = _manager(users={"alice": _user("alice")})
    with pytest.raises(PolicyError):
        cm._apply_overrides({"resources": {"cpu": "1"}}, {"name": "alice"},
                            {"resources": {"cpu": "4"}}, "alice")


def test_granted_resources_override_merges_cpu_and_memory():
    cm = _manager(users={"alice": _user("alice", resources=True)})
    spec, _ = cm._apply_overrides(
        {"resources": {"cpu": "1", "memory": "1Gi", "gpu": 1}},
        {"name": "alice"},
        {"resources": {"cpu": "4", "memory": "8Gi"}},
        "alice",
    )
    # gpu (count) is a separate group and must survive untouched.
    assert spec["resources"] == {"cpu": "4", "memory": "8Gi", "gpu": 1}


def test_ungranted_gpu_type_override_raises():
    cm = _manager(users={"alice": _user("alice")})
    with pytest.raises(PolicyError):
        cm._apply_overrides({}, {"name": "alice"}, {"gpuType": "A100"}, "alice")


def test_granted_gpu_type_override_sets_node_selector():
    cm = _manager(users={"alice": _user("alice", gpuType=True)})
    spec, _ = cm._apply_overrides(
        {"nodeSelector": {"other": "x"}}, {"name": "alice"},
        {"gpuType": "A100"}, "alice",
    )
    assert spec["nodeSelector"] == {"other": "x", GPU_NODE_LABEL: "A100"}


def test_ungranted_gpu_count_override_raises():
    cm = _manager(users={"alice": _user("alice")})
    with pytest.raises(PolicyError):
        cm._apply_overrides({}, {"name": "alice"}, {"gpuCount": 2}, "alice")


def test_granted_gpu_count_override_sets_resources_gpu():
    cm = _manager(users={"alice": _user("alice", gpuCount=True)})
    spec, _ = cm._apply_overrides(
        {"resources": {"cpu": "1"}}, {"name": "alice"}, {"gpuCount": 2}, "alice",
    )
    assert spec["resources"] == {"cpu": "1", "gpu": 2}


def test_ungranted_uid_gid_override_raises():
    cm = _manager(users={"alice": _user("alice")})
    with pytest.raises(PolicyError):
        cm._apply_overrides({}, {"name": "alice"}, {"uid": 2000}, "alice")
    with pytest.raises(PolicyError):
        cm._apply_overrides({}, {"name": "alice"}, {"gid": 2000}, "alice")


def test_granted_uid_gid_override_sets_user_details():
    cm = _manager(users={"alice": _user("alice", uidGid=True)})
    _, user = cm._apply_overrides(
        {}, {"name": "alice", "uid": 1001}, {"uid": 2000, "gid": 2001}, "alice",
    )
    assert user["uid"] == 2000
    assert user["gid"] == 2001


def test_ungranted_security_context_override_raises():
    cm = _manager(users={"alice": _user("alice")})
    with pytest.raises(PolicyError):
        cm._apply_overrides({}, {"name": "alice"},
                            {"securityContext": {"fsGroup": 2000}}, "alice")


def test_granted_security_context_override_merges():
    cm = _manager(users={"alice": _user("alice", securityContext=True)})
    _, user = cm._apply_overrides(
        {}, {"name": "alice", "securityContext": {"runAsUser": 1001, "fsGroup": 1001}},
        {"securityContext": {"fsGroup": 2000}}, "alice",
    )
    # fsGroup overridden, runAsUser untouched.
    assert user["securityContext"] == {"runAsUser": 1001, "fsGroup": 2000}


def test_ungranted_volumes_override_raises():
    cm = _manager(users={"alice": _user("alice")})
    with pytest.raises(PolicyError):
        cm._apply_overrides({}, {"name": "alice"},
                            {"volumes": {"scratch": "/mnt/scratch"}}, "alice")


def test_granted_volumes_override_replaces_template_volumes():
    cm = _manager(users={"alice": _user("alice", volumes=True)})
    spec, _ = cm._apply_overrides(
        {"volumes": {"data": "/mnt/data"}}, {"name": "alice"},
        {"volumes": {"scratch": "/mnt/scratch"}}, "alice",
    )
    assert spec["volumes"] == {"scratch": "/mnt/scratch"}


# --- inputs are not mutated ------------------------------------------------ #

def test_inputs_not_mutated():
    cm = _manager(users={"alice": _user("alice", resources=True)})
    template_spec = {"resources": {"cpu": "1"}}
    user_details = {"name": "alice"}
    cm._apply_overrides(template_spec, user_details,
                        {"resources": {"cpu": "4"}}, "alice")
    assert template_spec == {"resources": {"cpu": "1"}}
    assert user_details == {"name": "alice"}
