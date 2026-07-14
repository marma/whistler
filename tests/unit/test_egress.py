"""Egress NetworkPolicy rule construction, per zone
(KubeConfigManager._build_egress_rules / _build_baseline_egress_rules).

The blockCIDRs path computes the explicit complement of the blocked ranges
(rather than ipBlock.except, which several CNIs ignore) — a security boundary
worth pinning down. Same for the DNS narrowing knobs: a "red" zone's posture
is only as tight as its port-53 rule.
"""
import ipaddress

from whistler.config import KubeConfigManager


def _manager(zones):
    # Bypass __init__ (which loads kube config and files); we only need the
    # zones dict for these pure methods.
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.zones = zones
    return cm


def _zone_manager(zone_cfg):
    return _manager({"default": {}, "z": zone_cfg})


def _dns_anywhere_rule(rule):
    ports = {(p["port"], p["protocol"]) for p in rule.get("ports", [])}
    return rule.get("to") is None and ports == {(53, "UDP"), (53, "TCP")}


GATEWAY_RULE = {
    "to": [{"podSelector": {"matchLabels": {"app": "whistler-storage-gateway"}}}],
    "ports": [{"port": 445, "protocol": "TCP"}],
}


# --- baseline (all pods, zone-independent) --------------------------------- #

def test_baseline_allows_only_the_storage_gateway():
    # NetworkPolicy allows are union'd across policies, so anything in the
    # baseline is irrevocable by a zone — it must stay minimal. DNS in
    # particular must NOT be here, or a zone could never narrow it.
    rules = _manager({"default": {}})._build_baseline_egress_rules()
    assert rules == [GATEWAY_RULE]


# --- per-zone rules --------------------------------------------------------- #

def test_empty_zone_allows_only_dns():
    rules = _zone_manager({})._build_egress_rules("z")
    assert len(rules) == 1
    assert _dns_anywhere_rule(rules[0])


def test_allow_cidr_with_ports():
    rules = _zone_manager({
        "egress": {
            "allowCIDRs": [{"cidr": "203.0.113.0/24",
                            "ports": [{"port": 443, "protocol": "TCP"}]}],
            "blockCIDRs": [],
        },
    })._build_egress_rules("z")

    assert _dns_anywhere_rule(rules[0])
    allow = rules[1]
    assert allow["to"] == [{"ipBlock": {"cidr": "203.0.113.0/24"}}]
    assert allow["ports"] == [{"port": 443, "protocol": "TCP"}]


def test_allow_cidr_without_ports_allows_all_ports():
    rules = _zone_manager({
        "egress": {"allowCIDRs": [{"cidr": "0.0.0.0/0"}]},
    })._build_egress_rules("z")
    allow = rules[1]
    assert allow["to"] == [{"ipBlock": {"cidr": "0.0.0.0/0"}}]
    assert "ports" not in allow  # no port restriction => all ports


def test_block_cidr_emits_complement_excluding_blocked_range():
    block = "10.0.0.0/8"
    rules = _zone_manager({
        "egress": {"allowCIDRs": [], "blockCIDRs": [block]},
    })._build_egress_rules("z")

    # DNS rule + one complement rule.
    assert _dns_anywhere_rule(rules[0])
    complement_cidrs = [ipb["ipBlock"]["cidr"] for ipb in rules[1]["to"]]

    expected = [
        str(n) for n in ipaddress.IPv4Network("0.0.0.0/0").address_exclude(ipaddress.IPv4Network(block))
    ]
    assert complement_cidrs == expected

    # The blocked range must not be reachable, a neighbouring range must be.
    nets = [ipaddress.IPv4Network(c) for c in complement_cidrs]
    assert not any(ipaddress.IPv4Network(block).subnet_of(n) for n in nets)
    assert any(ipaddress.ip_address("11.0.0.1") in n for n in nets)
    assert not any(ipaddress.ip_address("10.1.2.3") in n for n in nets)


def test_zones_are_independent():
    cm = _manager({
        "default": {},
        "green": {"egress": {"blockCIDRs": ["10.0.0.0/8"]}},
    })
    default_rules = cm._build_egress_rules("default")
    green_rules = cm._build_egress_rules("green")
    assert len(default_rules) == 1          # DNS only
    assert len(green_rules) == 2            # DNS + complement


# --- DNS narrowing ----------------------------------------------------------- #

def test_dns_cluster_only_pins_port_53_to_the_cluster_resolver():
    rules = _zone_manager({"dns": {"clusterOnly": True}})._build_egress_rules("z")
    dns = rules[0]
    assert dns["to"] == [{
        "namespaceSelector": {"matchLabels": {
            "kubernetes.io/metadata.name": "kube-system"}},
        "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
    }]
    assert {(p["port"], p["protocol"]) for p in dns["ports"]} == {(53, "UDP"), (53, "TCP")}


def test_dns_servers_pin_port_53_to_those_ips():
    rules = _zone_manager({
        "dns": {"servers": ["10.0.0.53", "10.0.0.54"]},
    })._build_egress_rules("z")
    dns = rules[0]
    assert dns["to"] == [
        {"ipBlock": {"cidr": "10.0.0.53/32"}},
        {"ipBlock": {"cidr": "10.0.0.54/32"}},
    ]


# --- zone NetworkPolicy shape ------------------------------------------------ #

def test_zone_policy_selects_by_zone_label_and_is_egress_only():
    policy = _zone_manager({})._build_zone_network_policy("z")
    assert policy["metadata"]["name"] == "whistler-zone-z"
    assert policy["metadata"]["labels"] == {
        "whistler.martinmalmsten.net/zone-policy": "true",
        "whistler.martinmalmsten.net/zone": "z",
    }
    assert policy["spec"]["podSelector"] == {
        "matchLabels": {"whistler.martinmalmsten.net/zone": "z"}}
    # Egress-only: a zone must never widen what can reach a pod (ingress is
    # owned by the baseline isolate-user-pods policy).
    assert policy["spec"]["policyTypes"] == ["Egress"]
