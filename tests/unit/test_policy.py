"""Operator-authoritative policy (KubeConfigManager._apply_policy).

Covers the image allow-list (enforced for desktop mode and vm runtime, skipped
for ssh container/kata) and the privileged->kata coercion gated by
forceKataForPrivileged."""
import pytest

from whistler.config import KubeConfigManager, PolicyError


def _manager(*, force_kata=False, images=None):
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.force_kata_for_privileged = force_kata
    cm.kata_runtime_class = "kata"
    cm.images = images or {"ssh": [], "desktop": [], "vm": []}
    return cm


# --- image allow-list --------------------------------------------------- #

def test_ssh_container_allows_any_image():
    cm = _manager(images={"ssh": [], "desktop": [], "vm": []})
    # Empty ssh list means "allow any"; arbitrary image is accepted.
    assert cm._apply_policy({"image": "whatever:latest"}, "ssh", "container") == "container"
    assert cm._apply_policy({"image": "whatever:latest"}, "ssh", "kata") == "kata"


def test_desktop_image_must_be_in_list():
    cm = _manager(images={"ssh": [], "desktop": ["good:1"], "vm": []})
    assert cm._apply_policy({"image": "good:1"}, "desktop", "container") == "container"
    with pytest.raises(PolicyError):
        cm._apply_policy({"image": "bad:1"}, "desktop", "container")


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


def test_coercion_to_kata_keeps_desktop_image_check():
    # Coercion happens first; the desktop allow-list still applies to the image.
    cm = _manager(force_kata=True, images={"ssh": [], "desktop": ["good:1"], "vm": []})
    assert cm._apply_policy({"image": "good:1", "privileged": True}, "desktop", "container") == "kata"
    with pytest.raises(PolicyError):
        cm._apply_policy({"image": "bad:1", "privileged": True}, "desktop", "container")
