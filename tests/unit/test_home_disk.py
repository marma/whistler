"""Per-instance home disk PVC (KubeConfigManager._ensure_home_disk_pvc).

A VM cannot mount a PVC, so its home is a disk.img on an ordinary claim
attached as virtio-blk and formatted ext4 by the guest — see
design/storage.md, and tests/unit/test_cloud_init.py for the guest side.

The properties that matter here are structural: the claim is per INSTANCE
(not per user, or a home would carry data between zones on reboot), and it is
owner-referenced so it dies with its Session.
"""
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException

from whistler.config import KubeConfigManager


def _manager(home_disk_size="20Gi"):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.home_disk_size = home_disk_size
    return cm


def _created(**kwargs):
    """Run _ensure_home_disk_pvc against a cluster with no such PVC, and
    return the body it tried to create."""
    cm = _manager(**{k: v for k, v in kwargs.items()
                     if k == "home_disk_size"})
    api = MagicMock()
    api.read_namespaced_persistent_volume_claim.side_effect = ApiException(
        status=404)
    call = dict(session_name="alice-desk", namespace="whistler-user-alice",
                uid="uid-123")
    call.update({k: v for k, v in kwargs.items() if k != "home_disk_size"})
    with patch("whistler.config.client.CoreV1Api", return_value=api):
        name = cm._ensure_home_disk_pvc(**call)
    body = api.create_namespaced_persistent_volume_claim.call_args[0][1]
    return name, body


def test_named_and_labelled_per_instance_not_per_user():
    # The session name, not the username: two instances belonging to one user
    # get two homes, which is what keeps a home from crossing zones when the
    # user reboots into a different one.
    name, body = _created()
    assert name == "whistler-home-alice-desk"
    assert body["metadata"]["name"] == "whistler-home-alice-desk"
    assert body["metadata"]["labels"]["session"] == "alice-desk"
    assert "user" not in body["metadata"]["labels"]


def test_owner_referenced_so_it_dies_with_the_session():
    _name, body = _created()
    (owner,) = body["metadata"]["ownerReferences"]
    assert owner["kind"] == "Session"
    assert owner["name"] == "alice-desk"
    assert owner["uid"] == "uid-123"
    assert owner["controller"] is True


def test_filesystem_mode_and_rwo():
    _name, body = _created()
    # Filesystem (not Block) is what puts a disk.img on the share, and is the
    # only mode csi-driver-nfs can serve.
    assert body["spec"]["volumeMode"] == "Filesystem"
    # RWO: exactly one VM attaches this. Two writers on one block device
    # corrupt it, and nothing in the stack would notice.
    assert body["spec"]["accessModes"] == ["ReadWriteOnce"]


def test_size_defaults_and_is_overridable_per_template():
    _name, body = _created()
    assert body["spec"]["resources"]["requests"]["storage"] == "20Gi"
    _name, body = _created(size="100Gi")
    assert body["spec"]["resources"]["requests"]["storage"] == "100Gi"


def test_existing_claim_is_reused_not_recreated():
    # Reconcile runs repeatedly; a second call must return the existing claim
    # rather than trying to make another (which would 409 forever, and on a
    # different code path could mean a fresh empty home).
    cm = _manager()
    api = MagicMock()
    with patch("whistler.config.client.CoreV1Api", return_value=api):
        name = cm._ensure_home_disk_pvc(
            "alice-desk", "whistler-user-alice", "uid-123")
    assert name == "whistler-home-alice-desk"
    api.create_namespaced_persistent_volume_claim.assert_not_called()


def test_a_concurrent_creator_is_not_an_error():
    # Two reconciles can race. Losing the race means the claim exists, which
    # is the desired state — returning it beats failing the boot.
    cm = _manager()
    api = MagicMock()
    api.read_namespaced_persistent_volume_claim.side_effect = ApiException(
        status=404)
    api.create_namespaced_persistent_volume_claim.side_effect = ApiException(
        status=409)
    with patch("whistler.config.client.CoreV1Api", return_value=api):
        assert cm._ensure_home_disk_pvc(
            "alice-desk", "whistler-user-alice", "uid-123") == \
            "whistler-home-alice-desk"
