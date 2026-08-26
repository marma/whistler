"""The Group primitive: how a project's grants compose with a user's own.

Three rules are under test here, and they are the whole model
(design/security.md, "Group"):

  1. **Allow-lists union.** The union of the user's own list and every
     group's is what they hold, and nothing else — every allow is explicit
     (2026-08-25), so a group is the only thing that can widen a user who has
     been granted nothing of their own. Grants add up; empty is empty.
  2. **Volume access modes.** A group grants each member `rw` or `ro` per
     volume, with per-member exceptions; `ro` becomes a read-only mount.
  3. **Channels narrow, never widen.** A zone carries a ceiling, the union of
     the user's and their groups' grants narrows it, and the intersection is
     what the gateway and the portal decide on.
"""
import pytest
from kubernetes.client.rest import ApiException

from whistler.config import (
    CHANNEL_CLIPBOARD,
    CHANNEL_RELAY,
    CHANNEL_SCREENSHOTS,
    CHANNEL_SSH,
    CHANNEL_TERMINAL,
    ConfigWriteError,
    GPU_NODE_LABEL,
    KubeConfigManager,
    crd_missing_hint,
    PolicyError,
    SSH_POSTURE_NONE,
    SSH_POSTURE_RELAY,
    group_volume_grants,
    merge_allow_lists,
    merge_channel_grants,
    merge_override_grants,
    target_channels,
)


def _manager(*, users=None, groups=None, zones=None):
    """A KubeConfigManager with no cluster behind it — the catalogs are set
    directly, exactly as the zone and policy tests do."""
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.force_kata_for_privileged = False
    cm.kata_runtime_class = "kata"
    cm.images = {"ssh": [], "desktop": [], "vm": []}
    cm.users = users or {}
    cm.groups = groups or {}
    cm.zones = zones if zones is not None else {"default": {}}
    return cm


# --- The union rule, in the abstract --------------------------------------- #

def test_empty_everywhere_is_empty():
    # And empty is now no access at all, not "no opinion" — the enforcement
    # points below are where that shows.
    assert merge_allow_lists([], None, []) == []


def test_a_group_is_the_only_thing_that_widens_a_user_with_no_list():
    # The user's own empty list contributes nothing, so the union is the
    # group's — which is the whole of what they hold.
    assert merge_allow_lists([], ["restricted"]) == ["restricted"]


def test_grants_add_up_and_keep_the_users_own_first():
    assert merge_allow_lists(["open"], ["restricted"], ["open", "lab"]) == [
        "open", "restricted", "lab"]


def test_override_grants_are_or_ed_and_a_false_does_not_veto():
    merged = merge_override_grants({"zone": False}, {"zone": True, "volumes": True})
    assert merged == {"zone": True, "volumes": True}


# --- Resolution through the manager ---------------------------------------- #

def test_membership_is_read_off_the_group():
    cm = _manager(users={"alice": {"name": "alice"}, "bob": {"name": "bob"}},
                  groups={"lab": {"members": ["alice"]}})
    assert [g["name"] for g in cm.get_user_groups("alice")] == ["lab"]
    assert cm.get_user_groups("bob") == []


def test_zone_allow_list_is_the_union_of_user_and_groups():
    cm = _manager(
        users={"alice": {"name": "alice", "allowedZones": ["open"]}},
        groups={"lab": {"members": ["alice"], "allowedZones": ["restricted"]}},
        zones={"default": {}, "open": {}, "restricted": {}},
    )
    assert cm.get_user_allowed_zones("alice") == ["open", "restricted"]
    # Both are usable...
    for zone in ("open", "restricted"):
        assert cm._apply_policy({"image": "x", "zone": zone}, "ssh", "container",
                                "alice") == "container"


def test_a_group_zone_is_all_an_ungranted_user_has():
    cm = _manager(
        users={"alice": {"name": "alice"}},
        groups={"lab": {"members": ["alice"], "allowedZones": ["restricted"]}},
        zones={"default": {}, "open": {}, "restricted": {}},
    )
    assert cm._apply_policy({"image": "x", "zone": "restricted"}, "ssh",
                            "container", "alice") == "container"
    with pytest.raises(PolicyError, match="zone 'open' is not allowed"):
        cm._apply_policy({"image": "x", "zone": "open"}, "ssh", "container", "alice")


def test_a_non_member_gets_nothing_from_the_group():
    cm = _manager(
        users={"alice": {"name": "alice"}, "bob": {"name": "bob"}},
        groups={"lab": {"members": ["alice"], "allowedZones": ["restricted"]}},
        zones={"default": {}, "open": {}, "restricted": {}},
    )
    # bob has no list of his own and no group, so he holds nothing — neither
    # the group's zone nor any other. Before explicit access this same state
    # made him the one user who could start a session anywhere.
    assert cm.get_user_allowed_zones("bob") == []
    for zone in ("open", "restricted"):
        with pytest.raises(PolicyError, match="none granted"):
            cm._apply_policy({"image": "x", "zone": zone}, "ssh", "container", "bob")


def test_gpu_types_compose_the_same_way():
    cm = _manager(
        users={"alice": {"name": "alice", "allowedZones": ["default"]}},
        groups={"lab": {"members": ["alice"], "allowedGpuTypes": ["A100"]}},
    )
    spec = {"image": "x", "nodeSelector": {GPU_NODE_LABEL: "A100"}}
    assert cm._apply_policy(spec, "ssh", "container", "alice") == "container"
    spec = {"image": "x", "nodeSelector": {GPU_NODE_LABEL: "H100"}}
    with pytest.raises(PolicyError, match="not allowed"):
        cm._apply_policy(spec, "ssh", "container", "alice")


def test_override_grant_can_come_from_a_group():
    cm = _manager(
        users={"alice": {"name": "alice"}},
        groups={"lab": {"members": ["alice"], "overrides": {"zone": True}}},
        zones={"default": {}, "restricted": {}},
    )
    assert cm.get_user_overrides("alice")["zone"] is True
    spec, _ = cm._apply_overrides({"image": "x"}, {}, {"zone": "restricted"}, "alice")
    assert spec["zone"] == "restricted"


# --- Volumes: per-member access modes -------------------------------------- #

def test_volume_mode_defaults_to_rw_for_members():
    grants = group_volume_grants({"members": ["alice"],
                                  "volumes": [{"name": "data"}]}, "alice")
    assert grants == {"data": "rw"}


def test_per_member_access_overrides_the_default_mode():
    spec = {"members": ["alice", "carol"],
            "volumes": [{"name": "data", "mode": "rw", "access": {"carol": "ro"}}]}
    assert group_volume_grants(spec, "alice") == {"data": "rw"}
    assert group_volume_grants(spec, "carol") == {"data": "ro"}


def test_mode_none_grants_only_to_the_named_exceptions():
    spec = {"members": ["alice", "carol"],
            "volumes": [{"name": "secret", "mode": "none", "access": {"carol": "ro"}}]}
    assert group_volume_grants(spec, "alice") == {}
    assert group_volume_grants(spec, "carol") == {"secret": "ro"}


def test_someone_named_in_access_need_not_be_a_member():
    spec = {"members": ["alice"],
            "volumes": [{"name": "data", "mode": "rw", "access": {"dan": "ro"}}]}
    assert group_volume_grants(spec, "dan") == {"data": "ro"}


def test_rw_anywhere_beats_ro_elsewhere():
    cm = _manager(
        users={"alice": {"name": "alice"}},
        groups={
            "lab": {"members": ["alice"],
                    "volumes": [{"name": "data", "mode": "rw"}]},
            "guests": {"members": ["alice"],
                       "volumes": [{"name": "data", "mode": "ro"}]},
        },
    )
    assert cm.get_user_volume_modes("alice") == {"data": "rw"}


def test_a_users_own_volume_is_never_downgraded_by_a_group():
    cm = _manager(
        users={"alice": {"name": "alice", "allowedVolumes": ["data"]}},
        groups={"guests": {"members": ["alice"],
                           "volumes": [{"name": "data", "mode": "ro"}]}},
    )
    assert cm.get_user_volume_modes("alice") == {"data": "rw"}


def test_group_volume_appears_in_the_allow_list_and_gates_the_rest():
    cm = _manager(
        users={"alice": {"name": "alice", "allowedZones": ["default"]}},
        groups={"lab": {"members": ["alice"],
                        "volumes": [{"name": "project", "mode": "ro"}]}},
    )
    assert cm.get_user_allowed_volumes("alice") == ["project"]
    assert cm._apply_policy({"image": "x", "volumes": {"project": "/p"}},
                            "ssh", "container", "alice") == "container"
    with pytest.raises(PolicyError, match=r"volumes \['other'\] are not allowed"):
        cm._apply_policy({"image": "x", "volumes": {"other": "/o"}},
                         "ssh", "container", "alice")


def test_read_only_grant_becomes_a_read_only_mount():
    cm = _manager()
    pod_volumes, mounts = cm._build_volume_wiring(
        pvc_name="pvc-alice", personal_mount_path="/userdata",
        requested_volumes={"project": "/project", "scratch": "/scratch"},
        available_volumes={"project": {"name": "project",
                                       "persistentVolumeClaim": {"claimName": "p"}},
                           "scratch": {"name": "scratch",
                                       "persistentVolumeClaim": {"claimName": "s"}}},
        volume_modes={"project": "ro"},
    )
    by_name = {m["name"]: m for m in mounts}
    assert by_name["project"]["readOnly"] is True
    # Anything not named is read-write, which is what every volume was before
    # groups existed.
    assert "readOnly" not in by_name["scratch"]
    assert "readOnly" not in by_name["data"]


# --- Channels --------------------------------------------------------------- #

def test_absent_grants_do_not_narrow():
    assert merge_channel_grants(None, None) is None


def test_an_explicit_empty_grant_is_not_the_same_as_none():
    assert merge_channel_grants(None, []) == set()


def test_zone_ceiling_falls_back_to_the_legacy_ssh_posture():
    cm = _manager(zones={"relay-only": {"ssh": SSH_POSTURE_RELAY},
                         "no-ssh": {"ssh": SSH_POSTURE_NONE},
                         "default": {}})
    # `relay` closes the jump and nothing else; the other three channels were
    # never governed by that field.
    assert cm.zone_channel_ceiling("relay-only") == {
        CHANNEL_RELAY, CHANNEL_TERMINAL, CHANNEL_CLIPBOARD, CHANNEL_SCREENSHOTS}
    assert CHANNEL_SSH not in cm.zone_channel_ceiling("no-ssh")
    assert CHANNEL_RELAY not in cm.zone_channel_ceiling("no-ssh")
    # An unrestricted zone ceilings nothing.
    assert cm.zone_channel_ceiling("default") == {
        CHANNEL_SSH, CHANNEL_RELAY, CHANNEL_TERMINAL, CHANNEL_CLIPBOARD,
        CHANNEL_SCREENSHOTS}


def test_an_unknown_channel_in_a_zone_is_dropped_not_honored():
    cm = _manager(zones={"weird": {"channels": ["ssh", "telepathy"]}})
    assert cm.zone_channel_ceiling("weird") == {CHANNEL_SSH}


def test_a_group_grant_narrows_the_ceiling():
    cm = _manager(
        users={"alice": {"name": "alice"}, "carol": {"name": "carol"}},
        groups={"visitors": {"members": ["carol"], "channels": [CHANNEL_SCREENSHOTS]}},
        zones={"restricted": {}},
    )
    # Two people, same zone, same instance, different doors — the third axis.
    assert CHANNEL_SSH in cm.effective_channels("alice", "restricted")
    assert cm.effective_channels("carol", "restricted") == {CHANNEL_SCREENSHOTS}


def test_a_grant_can_never_widen_the_ceiling():
    cm = _manager(
        users={"carol": {"name": "carol", "channels": [CHANNEL_SSH]}},
        zones={"locked": {"channels": []}},
    )
    assert cm.effective_channels("carol", "locked") == set()


def test_grants_from_two_groups_union_before_the_ceiling_applies():
    cm = _manager(
        users={"alice": {"name": "alice"}},
        groups={"a": {"members": ["alice"], "channels": [CHANNEL_SSH]},
                "b": {"members": ["alice"], "channels": [CHANNEL_TERMINAL]}},
        zones={"restricted": {"channels": [CHANNEL_SSH, CHANNEL_RELAY]}},
    )
    # Union is {ssh, terminal}; the ceiling keeps only ssh.
    assert cm.effective_channels("alice", "restricted") == {CHANNEL_SSH}


def test_target_channels_falls_back_to_the_posture_rather_than_everything():
    # A resolver that predates the field must not read as "all channels".
    assert target_channels({"sshPosture": SSH_POSTURE_NONE}) == set()
    assert target_channels({"sshPosture": SSH_POSTURE_RELAY}) == {CHANNEL_RELAY}
    assert target_channels({"channels": [CHANNEL_SSH]}) == {CHANNEL_SSH}


# --- Failure reporting ------------------------------------------------------ #

def test_a_404_is_reported_as_the_missing_crd_it_almost_always_is():
    """`helm upgrade` never updates a chart's crds/, so a freshly added kind
    404s on a cluster that has had the chart installed for months. Naming that
    outright is worth more than relaying "404 Not Found"."""
    hint = crd_missing_hint("groups", ApiException(status=404, reason="Not Found"))
    assert "CRD is not installed" in hint
    assert "kubectl apply -f charts/whistler/crds/crds.yaml" in hint


def test_other_statuses_are_reported_as_themselves():
    assert crd_missing_hint("groups", ApiException(status=403, reason="Forbidden")) \
        == "403 Forbidden"


def test_save_group_raises_with_the_reason_rather_than_returning_false():
    cm = _manager()

    class _Api:
        def get_namespaced_custom_object(self, *a, **kw):
            raise ApiException(status=404, reason="Not Found")

        def create_namespaced_custom_object(self, *a, **kw):
            raise ApiException(status=404, reason="Not Found")

    cm.api = _Api()
    cm.namespace = "whistler"
    with pytest.raises(ConfigWriteError, match="CRD is not installed"):
        cm.save_group({"name": "lab", "members": ["alice"]})


def test_a_group_without_a_name_is_refused_without_touching_the_cluster():
    cm = _manager()
    cm.api = None  # any call would raise AttributeError
    assert cm.save_group({"members": ["alice"]}) is False
