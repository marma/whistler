"""Named home volumes (KubeConfigManager.ensure_home_volume_pvc and friends).

A VM cannot mount a PVC, so its home is a disk.img on an ordinary claim
attached as virtio-blk and formatted ext4 by the guest — see
design/storage.md, and tests/unit/test_cloud_init.py for the guest side.

A home used to be created per instance and owner-referenced to its Session, so
the two died together. It is now a NAMED object the user owns and an instance
selects (design/security.md, "Core model: the access matrix"): the claim
outlives any one Session, and what keeps data from crossing zones is the
access matrix plus the one-live-attach rule, not the absence of a choice.
"""
import json
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from whistler.config import KubeConfigManager, PolicyError


def _manager(home_disk_size="20Gi"):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.home_disk_size = home_disk_size
    cm._get_user_namespace = lambda u: f"whistler-user-{u}"
    return cm


VOLUME = {"name": "alice-desk", "user": "alice",
          "pvcName": "whistler-home-alice-desk"}


def _created(volume=None, **kwargs):
    """Run ensure_home_volume_pvc against a cluster with no such PVC, and
    return the body it tried to create."""
    cm = _manager(**{k: v for k, v in kwargs.items() if k == "home_disk_size"})
    api = MagicMock()
    api.read_namespaced_persistent_volume_claim.side_effect = ApiException(
        status=404)
    with patch("whistler.config.client.CoreV1Api", return_value=api):
        name = cm.ensure_home_volume_pvc(
            "alice", volume or VOLUME,
            fallback_size=kwargs.get("fallback_size"))
    body = api.create_namespaced_persistent_volume_claim.call_args[0][1]
    return name, body


# --- the claim -------------------------------------------------------------- #

def test_claim_is_not_owner_referenced_so_it_outlives_the_session():
    # The inversion that makes named homes work at all. Under per-instance
    # homes the claim was owned by the Session and GC reaped the two together;
    # a named volume that vanished with the instance that happened to create
    # it would be exactly the data loss users fear from a dropdown.
    _name, body = _created()
    assert "ownerReferences" not in body["metadata"]


def test_claim_name_is_explicit_when_given_and_derived_otherwise():
    # Adopted volumes carry an explicit pvcName because their claim is named
    # after the session that created it, and a bound PVC cannot be renamed.
    name, _body = _created()
    assert name == "whistler-home-alice-desk"
    name, _body = _created({"name": "scratch", "user": "alice"})
    assert name == "whistler-home-scratch"


def test_filesystem_mode_and_rwo():
    _name, body = _created()
    # Filesystem (not Block) is what puts a disk.img on the share, and is the
    # only mode csi-driver-nfs can serve.
    assert body["spec"]["volumeMode"] == "Filesystem"
    # RWO is documentation here, not enforcement: it is per-NODE, so two VMs
    # on one node could both attach. home_volume_holder is the real check.
    assert body["spec"]["accessModes"] == ["ReadWriteOnce"]


def test_size_comes_from_the_volume_then_the_template_then_the_default():
    _name, body = _created()
    assert body["spec"]["resources"]["requests"]["storage"] == "20Gi"
    _name, body = _created(fallback_size="50Gi")
    assert body["spec"]["resources"]["requests"]["storage"] == "50Gi"
    _name, body = _created({**VOLUME, "size": "100Gi"}, fallback_size="50Gi")
    assert body["spec"]["resources"]["requests"]["storage"] == "100Gi"


def test_storage_class_is_only_set_when_asked_for():
    _name, body = _created()
    assert "storageClassName" not in body["spec"]
    _name, body = _created({**VOLUME, "storageClassName": "fast"})
    assert body["spec"]["storageClassName"] == "fast"


def test_existing_claim_is_reused_not_recreated():
    # Reconcile runs repeatedly; a second call must return the existing claim
    # rather than trying to make another (which would 409 forever, and on a
    # different code path could mean a fresh empty home).
    cm = _manager()
    api = MagicMock()
    with patch("whistler.config.client.CoreV1Api", return_value=api):
        name = cm.ensure_home_volume_pvc("alice", VOLUME)
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
        assert cm.ensure_home_volume_pvc("alice", VOLUME) == \
            "whistler-home-alice-desk"


# --- choosing one ----------------------------------------------------------- #

def test_the_default_home_grants_itself_in_the_instances_zone():
    # The matrix has no defaults, so a default home created without a cell is
    # refused at the very start that created it — i.e. every instance that has
    # not chosen a volume, which is most of them. Not a widening: the instance
    # is already permitted in that zone.
    cm = _manager()
    cm.get_home_volume = lambda u, n: None
    cm.save_home_volume = lambda u, v: True
    granted = []
    cm.grant_own_volume_access = lambda u, z, v, mode="allowed": granted.append(
        (u, z, v)) or True
    cm.resolve_session_home_volume("alice", "alice-desk", zone="restricted")
    assert granted == [("alice", "restricted", "alice-desk")]


def test_no_request_means_a_home_named_after_the_instance():
    # The pre-named-volumes behaviour, preserved exactly: the default volume
    # carries the LEGACY claim name, so an instance that predates named
    # volumes keeps the disk it already has instead of getting an empty one.
    cm = _manager()
    cm.get_home_volume = lambda u, n: None
    saved = {}
    cm.save_home_volume = lambda u, v: saved.update(v) or True
    vol = cm.resolve_session_home_volume("alice", "alice-desk")
    assert vol["name"] == "alice-desk"
    assert vol["pvcName"] == "whistler-home-alice-desk"
    assert saved["pvcName"] == "whistler-home-alice-desk"


def test_an_existing_default_is_reused_rather_than_recreated():
    cm = _manager()
    cm.get_home_volume = lambda u, n: dict(VOLUME) if n == "alice-desk" else None
    cm.save_home_volume = lambda u, v: pytest.fail("must not recreate")
    assert cm.resolve_session_home_volume("alice", "alice-desk")["name"] == \
        "alice-desk"


def test_the_resolved_default_is_recorded_on_the_session():
    """The CR has to say which home the instance is using, not just have one.

    Until this, `spec.homeVolume` stayed empty for every instance that did not
    pick a volume — so the edit form's picker showed "New home for this
    instance" for an instance that already had one, and the volumes page could
    not name the instance holding a disk. The disk was right all along; the CR
    simply did not record it."""
    cm = _manager()
    patched = []
    cm.api = MagicMock()
    cm.api.patch_namespaced_custom_object.side_effect = (
        lambda g, v, ns, plural, name, body: patched.append((ns, name, body)))
    cm._record_session_home_volume("whistler-user-alice", "alice-desk",
                                   "alice-desk")
    assert patched == [("whistler-user-alice", "alice-desk",
                        {"spec": {"homeVolume": "alice-desk"}})]


def test_recording_the_home_never_blocks_the_boot():
    # The session is already using the right disk; the only thing a failed
    # patch loses is the UI knowing about it.
    cm = _manager()
    cm.api = MagicMock()
    cm.api.patch_namespaced_custom_object.side_effect = ApiException(status=500)
    cm._record_session_home_volume("ns", "alice-desk", "alice-desk")  # no raise


def test_a_named_volume_that_does_not_exist_is_refused_not_created():
    # Silently creating one would turn a typo into a brand-new empty home,
    # which is indistinguishable from data loss to the person it happens to.
    cm = _manager()
    cm.get_home_volume = lambda u, n: None
    cm.save_home_volume = lambda u, v: pytest.fail("must not create")
    with pytest.raises(PolicyError, match="does not exist"):
        cm.resolve_session_home_volume("alice", "alice-desk",
                                       requested="typo")


# --- one live attach -------------------------------------------------------- #

def _holder(vms, running, ignore=None):
    cm = _manager()
    api = MagicMock()

    def _list(group, version, ns, plural, **kw):
        if plural == "virtualmachineinstances":
            return {"items": [{"metadata": {"name": n}} for n in running]}
        return {"items": vms}
    api.list_namespaced_custom_object.side_effect = _list
    cm.api = api
    return cm.home_volume_holder("alice", VOLUME, ignore_instance=ignore)


def _vm(name, claim):
    return {"metadata": {"name": name},
            "spec": {"template": {"spec": {"volumes": [
                {"name": "homedisk",
                 "persistentVolumeClaim": {"claimName": claim}}]}}}}


def test_a_running_instance_holding_the_volume_is_reported():
    assert _holder([_vm("alice-other", "whistler-home-alice-desk")],
                   running=["alice-other"]) == "alice-other"


def test_a_stopped_instance_does_not_hold_it():
    # The rule is one live attach, not one reference. A stopped VM still names
    # the claim in its spec, and blocking on that would make a volume
    # unusable by anything but the instance that last touched it.
    assert _holder([_vm("alice-other", "whistler-home-alice-desk")],
                   running=[]) is None


def test_the_instance_asking_does_not_block_itself():
    # Restart and reconcile both re-run this against a VM that is already
    # running with the volume; counting itself would refuse every reboot.
    assert _holder([_vm("alice-desk", "whistler-home-alice-desk")],
                   running=["alice-desk"], ignore="alice-desk") is None


def test_another_volume_on_a_running_instance_is_irrelevant():
    assert _holder([_vm("alice-other", "whistler-home-something-else")],
                   running=["alice-other"]) is None


def test_an_api_failure_fails_open():
    # Deliberate: this check prevents filesystem incoherence, not a security
    # boundary. An API blip that blocked every start would be worse than the
    # rare double attach it would prevent. The rule that must fail CLOSED is
    # the access matrix, which is a different check.
    cm = _manager()
    api = MagicMock()
    api.list_namespaced_custom_object.side_effect = ApiException(status=500)
    cm.api = api
    assert cm.home_volume_holder("alice", VOLUME) is None


# --- error paths ------------------------------------------------------------ #

def test_a_failed_save_logs_rather_than_raising():
    # Error paths only run when something is already wrong, so a mistake in
    # one hides until the worst moment: a wrong-arity crd_missing_hint() call
    # shipped here and turned "could not save" into a TypeError that took the
    # whole adoption run down with it. Exercise the path itself.
    cm = _manager()
    cm._ensure_user_namespace = lambda u: f"whistler-user-{u}"
    api = MagicMock()
    api.get_namespaced_custom_object.side_effect = ApiException(status=404)
    api.create_namespaced_custom_object.side_effect = ApiException(status=404)
    cm.api = api
    assert cm.save_home_volume("alice", {"name": "research"}) is False


def test_a_missing_crd_says_how_to_fix_it():
    from whistler.config import crd_missing_hint, HOME_VOLUME_PLURAL
    msg = crd_missing_hint(HOME_VOLUME_PLURAL, ApiException(status=404))
    assert "homevolumes.whistler.martinmalmsten.net" in msg
    assert "kubectl apply" in msg


def test_releasing_a_claim_nulls_owner_references_rather_than_emptying_them():
    # ownerReferences has a MERGE patch strategy, so patching it with [] is a
    # silent no-op: the claim keeps its Session owner and is still reaped with
    # the instance, while the HomeVolume CR beside it says it was adopted.
    # Measured on a live cluster 2026-08-18. `null` deletes the key.
    cm = _manager()
    cm.get_home_volumes = lambda u: []
    cm.save_home_volume = lambda u, v: True
    cm._instance_zone = lambda ns, inst: "restricted"
    cm._has_any_access_cell = lambda u, v: False
    granted = []
    cm.grant_own_volume_access = lambda u, z, v, mode="allowed": granted.append(
        (u, z, v)) or True
    core = MagicMock()
    ns = MagicMock()
    ns.metadata.name = "whistler-user-alice"
    ns.metadata.labels = {"whistler.martinmalmsten.net/user": "alice"}
    core.list_namespace.return_value = MagicMock(items=[ns])
    claim = MagicMock()
    claim.metadata.name = "whistler-home-alice-desk"
    claim.metadata.owner_references = [
        MagicMock(kind="Session")]
    claim.spec.resources.requests = {"storage": "20Gi"}
    core.list_namespaced_persistent_volume_claim.return_value = MagicMock(
        items=[claim])
    with patch("whistler.config.client.CoreV1Api", return_value=core):
        assert cm.adopt_legacy_home_disks() == 1
    body = core.patch_namespaced_persistent_volume_claim.call_args[0][2]
    assert body == {"metadata": {"ownerReferences": None}}
    # And it must be granted where its instance already runs: the matrix has
    # no defaults, so an adopted home with no cell would be refused at the
    # next start of a session the user has been using for weeks.
    assert granted == [("alice", "restricted", "alice-desk")]


def test_adoption_backfills_a_cell_for_an_already_adopted_volume():
    # Adoption is idempotent, so a home adopted BEFORE the access matrix
    # existed would never get an entry — and the matrix has no defaults, so
    # its owner's instances would all be refused at their next start. Caught
    # on a live cluster 2026-08-19, where the first matrix deploy left every
    # existing home ungranted.
    cm = _manager()
    cm.get_home_volumes = lambda u: [{"name": "alice-desk"}]   # already adopted
    cm.save_home_volume = lambda u, v: pytest.fail("must not recreate")
    cm._instance_zone = lambda ns, inst: "open"
    cm._has_any_access_cell = lambda u, v: False
    granted = []
    cm.grant_own_volume_access = lambda u, z, v, mode="allowed": granted.append(
        (u, z, v)) or True
    core = MagicMock()
    ns = MagicMock()
    ns.metadata.name = "whistler-user-alice"
    ns.metadata.labels = {"whistler.martinmalmsten.net/user": "alice"}
    core.list_namespace.return_value = MagicMock(items=[ns])
    claim = MagicMock()
    claim.metadata.name = "whistler-home-alice-desk"
    claim.metadata.owner_references = []          # already released
    core.list_namespaced_persistent_volume_claim.return_value = MagicMock(
        items=[claim])
    with patch("whistler.config.client.CoreV1Api", return_value=core):
        cm.adopt_legacy_home_disks()
    assert granted == [("alice", "open", "alice-desk")]


def test_a_volume_that_already_has_a_cell_is_left_alone():
    cm = _manager()
    cm.get_home_volumes = lambda u: [{"name": "alice-desk"}]
    cm._has_any_access_cell = lambda u, v: True
    cm.grant_own_volume_access = lambda *a, **kw: pytest.fail("must not regrant")
    core = MagicMock()
    ns = MagicMock()
    ns.metadata.name = "whistler-user-alice"
    ns.metadata.labels = {"whistler.martinmalmsten.net/user": "alice"}
    core.list_namespace.return_value = MagicMock(items=[ns])
    claim = MagicMock()
    claim.metadata.name = "whistler-home-alice-desk"
    claim.metadata.owner_references = []
    core.list_namespaced_persistent_volume_claim.return_value = MagicMock(
        items=[claim])
    with patch("whistler.config.client.CoreV1Api", return_value=core):
        cm.adopt_legacy_home_disks()


# --- why a home was refused ------------------------------------------------- #
#
# The text IS the remedy path: it surfaces only as status.statusMessage on a
# Failed session, where there is nothing else to go on.

def _refusing(access):
    cm = _manager()
    cm.namespace = "whistler"
    cm.users = {"alice": {"name": "alice", "volumeAccess": access}}
    cm.groups = {}
    return cm


def test_a_refused_home_names_the_zones_it_is_actually_granted_in():
    # A home is granted in the zone it was made for, so the usual cause is
    # that the instance has since moved. Naming where it moved FROM is the
    # difference between "add this cell" and "why is my machine broken".
    msg = _refusing({"green": {"alice-desk": "allowed"},
                     "red": {"alice-desk": "read-only"},
                     "default": {"other": "allowed"}}
                    ).home_volume_refusal("alice", "default", "alice-desk")
    assert "It is granted in: green, red." in msg
    assert "'default'" in msg


def test_a_home_granted_nowhere_says_so_rather_than_listing_nothing():
    # "It is granted in: ." would read as a rendering bug and send the reader
    # looking for a zone that is not there.
    msg = _refusing({}).home_volume_refusal("alice", "default", "alice-desk")
    assert "granted in no zone at all" in msg


def test_the_refusal_carries_a_command_that_actually_fixes_it():
    # Pasted straight from a Failed session's message, so it has to be valid
    # JSON naming the right zone and volume — not prose about a grid.
    msg = _refusing({}).home_volume_refusal("alice", "restricted", "alice-desk")
    patch = json.loads(msg.split("-p '")[1].rstrip("'"))
    assert patch == {"spec": {"volumeAccess": {
        "restricted": {"alice-desk": "allowed"}}}}
    assert "kubectl -n whistler patch usr alice" in msg


def test_a_group_granted_zone_counts_as_granted():
    # get_user_volume_access is the merged view, so a cell a project confers
    # must not be reported as missing.
    cm = _refusing({})
    cm.groups = {"lab": {"members": ["alice"],
                         "volumeAccess": {"green": {"alice-desk": "allowed"}}}}
    assert "granted in: green" in cm.home_volume_refusal(
        "alice", "default", "alice-desk")
