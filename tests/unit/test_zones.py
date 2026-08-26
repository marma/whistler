"""Zone resolution, authorization, and workload stamping.

Covers the zone half of _apply_policy (fail-closed on unknown zones,
allowedZones gating), the "zone" override group in _apply_overrides, the
config loader's legacy fallback, and the zone label / config-hash annotation /
DNS steering on pod and VM manifests.
"""
import pytest

from whistler.config import (
    DEFAULT_ZONE,
    KubeConfigManager,
    PolicyError,
    ZONE_HASH_ANNOTATION,
    ZONE_LABEL,
)


def _manager(*, zones=None, users=None):
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.force_kata_for_privileged = False
    cm.kata_runtime_class = "kata"
    cm.images = {"ssh": [], "desktop": [], "vm": []}
    cm.users = users or {}
    cm.groups = {}
    cm.zones = zones if zones is not None else {"default": {}}
    return cm


# --- _apply_policy: existence + allowedZones ------------------------------- #

def test_unknown_zone_fails_closed():
    # An undefined zone must never fall back to default, whose posture may be
    # laxer than what the template intended.
    cm = _manager(zones={"default": {}, "green": {}})
    with pytest.raises(PolicyError, match="zone 'red' is not defined"):
        cm._apply_policy({"image": "x", "zone": "red"}, "ssh", "container")


def test_implicit_default_zone_is_gated_like_any_other():
    # A template with no `zone` lands in "default", so "default" is the grant
    # it is checked against. It used to skip the check entirely, which meant a
    # user allowed only "green" could still start a session in "default" —
    # the one hole left by explicit access (2026-08-25).
    cm = _manager(users={"alice": {"name": "alice", "allowedZones": ["green"]}},
                  zones={"default": {}, "green": {}})
    with pytest.raises(PolicyError,
                       match="zone 'default' is not allowed for user 'alice'"):
        cm._apply_policy({"image": "x"}, "ssh", "container", "alice")
    cm = _manager(users={"alice": {"name": "alice", "allowedZones": [DEFAULT_ZONE]}})
    assert cm._apply_policy({"image": "x"}, "ssh", "container", "alice") == "container"


def test_explicit_zone_requires_allow_list_membership():
    cm = _manager(zones={"default": {}, "green": {}, "red": {}},
                  users={"alice": {"name": "alice", "allowedZones": ["green"]}})
    assert cm._apply_policy(
        {"image": "x", "zone": "green"}, "ssh", "container", "alice") == "container"
    with pytest.raises(PolicyError, match="zone 'red' is not allowed for user 'alice'"):
        cm._apply_policy({"image": "x", "zone": "red"}, "ssh", "container", "alice")


def test_empty_allow_list_grants_no_zone_at_all():
    # Every allow is explicit: an unconfigured user starts nothing anywhere,
    # not everything everywhere.
    cm = _manager(zones={"default": {}, "red": {}},
                  users={"alice": {"name": "alice"}})
    with pytest.raises(PolicyError, match="none granted"):
        cm._apply_policy({"image": "x", "zone": "red"}, "ssh", "container", "alice")
    with pytest.raises(PolicyError, match="none granted"):
        cm._apply_policy({"image": "x"}, "ssh", "container", "alice")


# --- _apply_overrides: the "zone" grant ------------------------------------ #

def test_zone_override_requires_the_grant():
    cm = _manager(users={"alice": {"name": "alice", "overrides": {}}})
    with pytest.raises(PolicyError, match="not granted the 'zone' override"):
        cm._apply_overrides({"image": "x"}, {}, {"zone": "green"}, "alice")


def test_granted_zone_override_lands_on_the_spec():
    cm = _manager(users={"alice": {"name": "alice", "overrides": {"zone": True}}})
    spec, _ = cm._apply_overrides(
        {"image": "x", "zone": "red"}, {}, {"zone": "green"}, "alice")
    assert spec["zone"] == "green"


# --- loader ------------------------------------------------------------------ #
# Zone CRs are the source of truth; the file paths below are the fallback the
# __new__-built manager (no self.api) exercises via its AttributeError branch.

class _FakeCustomObjectsApi:
    def __init__(self, items):
        self._items = items

    def list_namespaced_custom_object(self, group, version, namespace, plural):
        assert plural == "zones"
        return {"items": self._items}


def test_load_zones_prefers_zone_crs():
    cm = _manager()
    cm.namespace = "whistler"
    cm.api = _FakeCustomObjectsApi([
        {"metadata": {"name": "green"},
         "spec": {"egress": {"blockCIDRs": ["10.0.0.0/8"]}}},
        {"metadata": {"name": "red"}, "spec": None},
    ])
    cm._load_zones()
    assert set(cm.zones) == {"default", "green", "red"}
    assert cm.zones["green"] == {"egress": {"blockCIDRs": ["10.0.0.0/8"]}}
    assert cm.zones["red"] == {}       # null spec normalized
    assert cm.zones["default"] == {}   # synthesized when no default CR


def test_delete_zone_refuses_default():
    # Guard fires before any API access — no self.api needed.
    assert _manager().delete_zone("default") is False


def test_save_zone_requires_a_name():
    assert _manager().save_zone({"name": "  ", "egress": {}}) is False


def test_load_zones_falls_back_to_legacy_networkpolicy(tmp_path, monkeypatch):
    import whistler.config as config_mod
    legacy = tmp_path / "networkpolicy.yaml"
    legacy.write_text("egress:\n  blockCIDRs: [10.0.0.0/8]\n")
    monkeypatch.setattr(config_mod, "ZONES_FILE", str(tmp_path / "zones.yaml"))
    monkeypatch.setattr(config_mod, "NETWORKPOLICY_FILE", str(legacy))
    cm = _manager()
    cm._load_zones()
    assert cm.zones == {"default": {"egress": {"blockCIDRs": ["10.0.0.0/8"]}}}


def test_load_zones_reads_catalog_and_guarantees_default(tmp_path, monkeypatch):
    import whistler.config as config_mod
    zones = tmp_path / "zones.yaml"
    zones.write_text("green:\n  egress:\n    blockCIDRs: [10.0.0.0/8]\n")
    monkeypatch.setattr(config_mod, "ZONES_FILE", str(zones))
    monkeypatch.setattr(config_mod, "NETWORKPOLICY_FILE", str(tmp_path / "missing.yaml"))
    cm = _manager()
    cm._load_zones()
    assert set(cm.zones) == {"default", "green"}
    assert cm.zones["default"] == {}


# --- workload stamping -------------------------------------------------------- #

def _pod(cm, template_spec):
    return cm._build_pod_spec(
        full_name="alice-dev-abc", hostname="dev", username="alice",
        uid="uid-1", mode="ssh", runtime="container",
        template_spec=template_spec, pvc_name="whistler-data-alice",
        available_volumes={}, user_details={"name": "alice"}, preemptible=False,
    )


def test_pod_is_stamped_with_zone_label_and_config_hash():
    cm = _manager(zones={"default": {}, "red": {"egress": {"allowCIDRs": []}}})
    pod = _pod(cm, {"image": "x", "zone": "red"})
    assert pod["metadata"]["labels"][ZONE_LABEL] == "red"
    assert pod["metadata"]["annotations"][ZONE_HASH_ANNOTATION] == cm._zone_config_hash("red")
    # Hash tracks the zone's config, not just its name.
    assert cm._zone_config_hash("red") != cm._zone_config_hash("default")


def test_pod_without_zone_lands_in_default():
    cm = _manager()
    pod = _pod(cm, {"image": "x"})
    assert pod["metadata"]["labels"][ZONE_LABEL] == DEFAULT_ZONE
    assert "dnsConfig" not in pod["spec"]  # no forced resolvers by default


def test_zone_dns_servers_steer_the_pod_resolv_conf():
    cm = _manager(zones={"default": {}, "z": {"dns": {"servers": ["10.0.0.53"]}}})
    pod = _pod(cm, {"image": "x", "zone": "z"})
    assert pod["spec"]["dnsPolicy"] == "None"
    assert pod["spec"]["dnsConfig"] == {"nameservers": ["10.0.0.53"]}


def test_vm_template_carries_zone_label_and_dns():
    cm = _manager(zones={"default": {}, "z": {"dns": {"servers": ["10.0.0.53"]}}})
    vm, _secret = cm._build_vm_spec(
        session_name="alice-desk", hostname="desk", username="alice",
        uid="uid-1", template_spec={"image": "q/x:1", "zone": "z"},
        display_port=5900, instancetype=None, preemptible=False,
        home_pvc="whistler-home-alice-desk",
        user_details={"name": "alice"},
    )
    tpl_meta = vm["spec"]["template"]["metadata"]
    # On the VMI template so the virt-launcher pod (which carries the guest's
    # masqueraded traffic) inherits it and the zone policy selects it.
    assert tpl_meta["labels"][ZONE_LABEL] == "z"
    assert tpl_meta["annotations"][ZONE_HASH_ANNOTATION] == cm._zone_config_hash("z")
    assert vm["spec"]["template"]["spec"]["dnsConfig"] == {"nameservers": ["10.0.0.53"]}
    assert vm["spec"]["template"]["spec"]["dnsPolicy"] == "None"
