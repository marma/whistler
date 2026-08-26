"""The access matrix: (subject, zone, volume) -> allowed | read-only.

An absent entry is NO ACCESS. This table got there first: it was the one
place in Whistler where empty meant nothing while every allow-list around it
meant "unrestricted". The allow-lists were brought into line on 2026-08-25, so
the rule is now uniform — but a cell is (zone, volume, mode) and not a name,
so the table is still not an allow-list. See design/security.md, "Core model:
the access matrix".
"""
import pytest

from whistler.config import (ACCESS_MODES, ENFORCED_ACCESS_KINDS,
                             KubeConfigManager, merge_volume_access)


def _manager(users=None, groups=None):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.namespace = "whistler"
    cm.users = users or {}
    cm.groups = groups or {}
    return cm


# --- composition ------------------------------------------------------------ #

def test_absent_is_no_access_and_there_is_nothing_below_it():
    cm = _manager({"alice": {"name": "alice"}})
    assert cm.volume_access("alice", "open", "anything") is None
    # An empty table grants nothing at all — the same explicit-allow rule
    # allowedVolumes now follows, arrived at from the other direction.
    assert cm.get_user_volume_access("alice") == {}


def test_groups_widen_and_the_most_permissive_cell_wins():
    cm = _manager(
        {"alice": {"name": "alice",
                   "volumeAccess": {"open": {"home": "read-only"}}}},
        {"proj": {"members": ["alice"],
                  "volumeAccess": {"open": {"home": "allowed",
                                            "corpus": "read-only"}}}},
    )
    # Joining a project is a deliberate act that confers access, so the group's
    # "allowed" beats her own "read-only" rather than being clamped by it.
    assert cm.volume_access("alice", "open", "home") == "allowed"
    assert cm.volume_access("alice", "open", "corpus") == "read-only"


def test_a_zone_nobody_granted_stays_empty():
    cm = _manager({"alice": {"name": "alice",
                             "volumeAccess": {"open": {"home": "allowed"}}}})
    assert cm.volume_access("alice", "restricted", "home") is None


def test_merge_ignores_modes_it_does_not_recognise():
    # A typo'd mode must not become a grant. Silence beats guessing here.
    merged = merge_volume_access({"open": {"a": "allowed", "b": "rw",
                                           "c": "yes please"}})
    assert merged == {"open": {"a": "allowed"}}


def test_merge_is_order_independent_for_the_same_cell():
    a = {"open": {"home": "read-only"}}
    b = {"open": {"home": "allowed"}}
    assert merge_volume_access(a, b) == merge_volume_access(b, a)


# --- writing ---------------------------------------------------------------- #

def test_saving_writes_only_the_users_own_cells():
    # Saving the merged view would copy a project's grants onto the member
    # permanently, and they would keep them after leaving the group.
    cm = _manager({"alice": {"name": "alice"}})
    saved = {}
    cm._save_user_spec = lambda u, spec: saved.update(spec) or True
    cm.set_user_volume_access("alice", {"open": {"home": "allowed",
                                                 "junk": "nonsense"},
                                        "empty": {}})
    assert saved["volumeAccess"] == {"open": {"home": "allowed"}}


def test_clearing_every_cell_removes_the_field_rather_than_writing_empty():
    # None removes the key: "this user states nothing", which keeps the CR
    # clean instead of accumulating an empty object.
    cm = _manager({"alice": {"name": "alice"}})
    saved = {}
    cm._save_user_spec = lambda u, spec: saved.update(spec) or True
    cm.set_user_volume_access("alice", {})
    assert saved == {"volumeAccess": None}


def test_granting_one_cell_leaves_the_rest_alone():
    # Self-service home creation writes exactly one cell; it must not clobber
    # what an admin set elsewhere.
    cm = _manager({"alice": {"name": "alice", "volumeAccess": {
        "open": {"old": "allowed"}, "restricted": {"secret": "read-only"}}}})
    saved = {}
    cm._save_user_spec = lambda u, spec: saved.update(spec) or True
    assert cm.grant_own_volume_access("alice", "open", "new") is True
    assert saved["volumeAccess"] == {
        "open": {"old": "allowed", "new": "allowed"},
        "restricted": {"secret": "read-only"}}


def test_an_unknown_mode_cannot_be_granted():
    cm = _manager({"alice": {"name": "alice"}})
    cm._save_user_spec = lambda u, spec: pytest.fail("must not write")
    assert cm.grant_own_volume_access("alice", "open", "v", "rw") is False


def test_deleting_a_volume_drops_it_from_every_zone():
    cm = _manager({"alice": {"name": "alice", "volumeAccess": {
        "open": {"gone": "allowed", "kept": "allowed"},
        "restricted": {"gone": "read-only"}}}})
    saved = {}
    cm._save_user_spec = lambda u, spec: saved.update(spec) or True
    cm.revoke_own_volume_access("alice", "gone")
    assert saved["volumeAccess"] == {"open": {"kept": "allowed"}}


# --- scope ------------------------------------------------------------------ #

def test_only_home_volumes_are_enforced_so_far():
    # Datasets and PVC volumes appear in the grid but are still governed by
    # allowedVolumes and the group grants. Recording that in code keeps the UI
    # honest — the same role ENFORCED_CHANNELS plays for `clipboard`.
    assert ENFORCED_ACCESS_KINDS == ("home",)
    assert ACCESS_MODES == ("allowed", "read-only")


# --- computed access -------------------------------------------------------- #

def test_computed_access_is_the_max_union_of_user_and_groups():
    # no access < read-only < read-write, per cell. Two groups and the user
    # each contributing a different level: the most permissive wins, and it
    # wins per cell rather than per volume or per zone.
    cm = _manager(
        {"alice": {"name": "alice", "volumeAccess": {
            "open": {"corpus": "read-only"},
            "restricted": {"corpus": "allowed"}}}},
        {"readers": {"members": ["alice"], "volumeAccess": {
            "open": {"corpus": "allowed", "extra": "read-only"}}},
         "guests": {"members": ["alice"], "volumeAccess": {
            "open": {"corpus": "read-only"},
            "restricted": {"corpus": "read-only"}}}},
    )
    assert cm.get_user_volume_access("alice") == {
        "open": {"corpus": "allowed", "extra": "read-only"},
        "restricted": {"corpus": "allowed"},
    }


def test_a_group_never_takes_access_away():
    # Union, not intersection: joining a project confers access. A group that
    # says read-only cannot clamp a user who already holds read-write.
    cm = _manager(
        {"alice": {"name": "alice",
                   "volumeAccess": {"open": {"corpus": "allowed"}}}},
        {"guests": {"members": ["alice"],
                    "volumeAccess": {"open": {"corpus": "read-only"}}}},
    )
    assert cm.volume_access("alice", "open", "corpus") == "allowed"


def test_a_cell_nobody_grants_stays_absent_in_the_union():
    cm = _manager({"alice": {"name": "alice"}},
                  {"g": {"members": ["alice"],
                         "volumeAccess": {"open": {"a": "allowed"}}}})
    assert cm.volume_access("alice", "open", "b") is None
    assert cm.volume_access("alice", "restricted", "a") is None
