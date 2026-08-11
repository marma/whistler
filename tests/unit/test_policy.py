"""Operator-authoritative policy (KubeConfigManager._apply_policy).

Covers the image allow-list (enforced for desktop mode and vm runtime, skipped
for ssh container/kata), the privileged->kata coercion gated by
forceKataForPrivileged, and the per-user GPU-type/volumes allow-lists."""
import pytest

from whistler.config import GPU_NODE_LABEL, KubeConfigManager, PolicyError


def _manager(*, force_kata=False, images=None, users=None):
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.force_kata_for_privileged = force_kata
    cm.kata_runtime_class = "kata"
    cm.images = images or {"ssh": [], "desktop": [], "vm": []}
    cm.users = users or {}
    return cm


# --- image allow-list --------------------------------------------------- #

def test_ssh_container_allows_any_image():
    cm = _manager(images={"ssh": [], "desktop": [], "vm": []})
    # Empty ssh list means "allow any"; arbitrary image is accepted.
    assert cm._apply_policy({"image": "whatever:latest"}, "ssh", "container") == "container"
    assert cm._apply_policy({"image": "whatever:latest"}, "ssh", "kata") == "kata"


@pytest.mark.parametrize("runtime", ["container", "kata"])
def test_desktop_mode_needs_a_vm(runtime):
    """Container sessions are web-terminal only: the streamed desktop-in-a-pod
    is retired (design/container_workloads.md), so asking for one is a policy
    error naming the way forward rather than a pod that quietly has no
    display."""
    cm = _manager(images={"ssh": [], "desktop": ["good:1"], "vm": []})
    with pytest.raises(PolicyError) as e:
        cm._apply_policy({"image": "good:1"}, "desktop", runtime)
    assert "runtime 'vm'" in str(e.value)


def test_desktop_refusal_precedes_the_image_check():
    """Fail on the shape, not the image — otherwise a container desktop with
    an unlisted image reports the wrong reason."""
    cm = _manager(images={"ssh": [], "desktop": ["good:1"], "vm": []})
    with pytest.raises(PolicyError) as e:
        cm._apply_policy({"image": "bad:1"}, "desktop", "container")
    assert "runtime 'vm'" in str(e.value)


def test_ssh_container_is_unaffected():
    cm = _manager(images={"ssh": [], "desktop": ["good:1"], "vm": []})
    assert cm._apply_policy({"image": "anything:1"}, "ssh", "container") == "container"


def test_vm_image_must_be_in_vm_list():
    cm = _manager(images={"ssh": [], "desktop": ["d:1"], "vm": ["v:1"]})
    assert cm._apply_policy({"image": "v:1"}, "desktop", "vm") == "vm"
    # An image only in the desktop list is not valid for a vm.
    with pytest.raises(PolicyError):
        cm._apply_policy({"image": "d:1"}, "desktop", "vm")


def test_ssh_vm_still_restricted_to_vm_list():
    cm = _manager(images={"ssh": [], "desktop": [], "vm": ["v:1"]})
    # A VM is always restricted regardless of mode.
    with pytest.raises(PolicyError):
        cm._apply_policy({"image": "anything"}, "ssh", "vm")


def test_vm_image_url_checked_against_vm_list():
    url = "https://example.com/noble.img"
    cm = _manager(images={"ssh": [], "desktop": [], "vm": [url]})
    assert cm._apply_policy({"imageURL": url}, "desktop", "vm") == "vm"
    with pytest.raises(PolicyError):
        cm._apply_policy({"imageURL": "https://example.com/other.img"}, "desktop", "vm")


def test_vm_rejects_both_image_and_image_url():
    cm = _manager(images={"ssh": [], "desktop": [], "vm": ["v:1", "u"]})
    with pytest.raises(PolicyError):
        cm._apply_policy({"image": "v:1", "imageURL": "u"}, "desktop", "vm")


# --- privileged -> kata coercion --------------------------------------- #

def test_privileged_container_coerced_to_kata_when_enabled():
    cm = _manager(force_kata=True)
    assert cm._apply_policy({"image": "x", "privileged": True}, "ssh", "container") == "kata"
    # fuse implies privileged too.
    assert cm._apply_policy({"image": "x", "fuse": True}, "ssh", "container") == "kata"


def test_privileged_not_coerced_when_disabled():
    cm = _manager(force_kata=False)
    assert cm._apply_policy({"image": "x", "privileged": True}, "ssh", "container") == "container"


def test_already_kata_unchanged():
    cm = _manager(force_kata=True)
    assert cm._apply_policy({"image": "x", "privileged": True}, "ssh", "kata") == "kata"


def test_privileged_vm_unchanged():
    cm = _manager(force_kata=True, images={"ssh": [], "desktop": [], "vm": ["v:1"]})
    assert cm._apply_policy({"image": "v:1", "privileged": True}, "desktop", "vm") == "vm"


def test_coercion_to_kata_does_not_rescue_a_container_desktop():
    """Kata is still not a VM as far as displays go: coercion happens after
    the desktop check, so a privileged container desktop is refused for being
    a container desktop, not silently promoted into one that works."""
    cm = _manager(force_kata=True, images={"ssh": [], "desktop": ["good:1"], "vm": []})
    with pytest.raises(PolicyError) as e:
        cm._apply_policy({"image": "good:1", "privileged": True}, "desktop", "container")
    assert "runtime 'vm'" in str(e.value)


# --- GPU type allow-list -------------------------------------------------- #

def test_gpu_type_allowed_when_no_username_given():
    # No username (e.g. called without operator context) skips the check
    # entirely rather than failing closed.
    cm = _manager()
    spec = {"image": "x", "nodeSelector": {GPU_NODE_LABEL: "A100-SXM4-40GB"}}
    assert cm._apply_policy(spec, "ssh", "container") == "container"


def test_gpu_type_allowed_when_user_has_no_allow_list():
    # Absent/empty allowedGpuTypes means "no restriction".
    cm = _manager(users={"alice": {"name": "alice"}})
    spec = {"image": "x", "nodeSelector": {GPU_NODE_LABEL: "A100-SXM4-40GB"}}
    assert cm._apply_policy(spec, "ssh", "container", "alice") == "container"


def test_gpu_type_allowed_when_in_users_allow_list():
    cm = _manager(users={"alice": {"name": "alice", "allowedGpuTypes": ["A100-SXM4-40GB"]}})
    spec = {"image": "x", "nodeSelector": {GPU_NODE_LABEL: "A100-SXM4-40GB"}}
    assert cm._apply_policy(spec, "ssh", "container", "alice") == "container"


def test_gpu_type_rejected_when_not_in_users_allow_list():
    cm = _manager(users={"alice": {"name": "alice", "allowedGpuTypes": ["nvidia-tesla-p100"]}})
    spec = {"image": "x", "nodeSelector": {GPU_NODE_LABEL: "A100-SXM4-40GB"}}
    with pytest.raises(PolicyError):
        cm._apply_policy(spec, "ssh", "container", "alice")


def test_no_gpu_requested_skips_check_even_with_allow_list():
    cm = _manager(users={"alice": {"name": "alice", "allowedGpuTypes": ["nvidia-tesla-p100"]}})
    assert cm._apply_policy({"image": "x"}, "ssh", "container", "alice") == "container"


# --- volumes allow-list --------------------------------------------------- #

def test_volumes_allowed_when_no_username_given():
    cm = _manager()
    spec = {"image": "x", "volumes": {"scratch": "/mnt/scratch"}}
    assert cm._apply_policy(spec, "ssh", "container") == "container"


def test_volumes_allowed_when_user_has_no_allow_list():
    cm = _manager(users={"alice": {"name": "alice"}})
    spec = {"image": "x", "volumes": {"scratch": "/mnt/scratch"}}
    assert cm._apply_policy(spec, "ssh", "container", "alice") == "container"


def test_volumes_allowed_when_in_users_allow_list():
    cm = _manager(users={"alice": {"name": "alice", "allowedVolumes": ["scratch"]}})
    spec = {"image": "x", "volumes": {"scratch": "/mnt/scratch"}}
    assert cm._apply_policy(spec, "ssh", "container", "alice") == "container"


def test_volumes_rejected_when_not_in_users_allow_list():
    cm = _manager(users={"alice": {"name": "alice", "allowedVolumes": ["scratch"]}})
    spec = {"image": "x", "volumes": {"other": "/mnt/other"}}
    with pytest.raises(PolicyError):
        cm._apply_policy(spec, "ssh", "container", "alice")


def test_no_volumes_requested_skips_check_even_with_allow_list():
    cm = _manager(users={"alice": {"name": "alice", "allowedVolumes": ["scratch"]}})
    assert cm._apply_policy({"image": "x"}, "ssh", "container", "alice") == "container"
