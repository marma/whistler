"""Egress NetworkPolicy rule construction (KubeConfigManager._build_egress_rules).

The blockCIDRs path computes the explicit complement of the blocked ranges
(rather than ipBlock.except, which several CNIs ignore) — a security boundary
worth pinning down.
"""
import ipaddress

from whistler.config import KubeConfigManager


def _manager(egress):
    # Bypass __init__ (which loads kube config and files); we only need the
    # egress dict for this pure method.
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.network_policy_egress = egress
    return cm


def _dns_rule(rule):
    ports = {(p["port"], p["protocol"]) for p in rule.get("ports", [])}
    return rule.get("to") is None and ports == {(53, "UDP"), (53, "TCP")}


def _gateway_rule(rule):
    # Same-namespace SMB to the user's storage gateway (VM homes).
    return rule == {
        "to": [{"podSelector": {"matchLabels": {"app": "whistler-storage-gateway"}}}],
        "ports": [{"port": 445, "protocol": "TCP"}],
    }


def test_default_allows_only_dns_and_storage_gateway():
    rules = _manager({"allowCIDRs": [], "blockCIDRs": []})._build_egress_rules()
    assert len(rules) == 2
    assert _dns_rule(rules[0])
    assert _gateway_rule(rules[1])


def test_allow_cidr_with_ports():
    rules = _manager({
        "allowCIDRs": [{"cidr": "203.0.113.0/24", "ports": [{"port": 443, "protocol": "TCP"}]}],
        "blockCIDRs": [],
    })._build_egress_rules()

    assert _dns_rule(rules[0])
    assert _gateway_rule(rules[1])
    allow = rules[2]
    assert allow["to"] == [{"ipBlock": {"cidr": "203.0.113.0/24"}}]
    assert allow["ports"] == [{"port": 443, "protocol": "TCP"}]


def test_allow_cidr_without_ports_allows_all_ports():
    rules = _manager({"allowCIDRs": [{"cidr": "0.0.0.0/0"}], "blockCIDRs": []})._build_egress_rules()
    allow = rules[2]
    assert allow["to"] == [{"ipBlock": {"cidr": "0.0.0.0/0"}}]
    assert "ports" not in allow  # no port restriction => all ports


def test_block_cidr_emits_complement_excluding_blocked_range():
    block = "10.0.0.0/8"
    rules = _manager({"allowCIDRs": [], "blockCIDRs": [block]})._build_egress_rules()

    # DNS rule + gateway rule + one complement rule.
    assert _dns_rule(rules[0])
    assert _gateway_rule(rules[1])
    complement_cidrs = [ipb["ipBlock"]["cidr"] for ipb in rules[2]["to"]]

    expected = [
        str(n) for n in ipaddress.IPv4Network("0.0.0.0/0").address_exclude(ipaddress.IPv4Network(block))
    ]
    assert complement_cidrs == expected

    # The blocked range must not be reachable, a neighbouring range must be.
    nets = [ipaddress.IPv4Network(c) for c in complement_cidrs]
    assert not any(ipaddress.IPv4Network(block).subnet_of(n) for n in nets)
    assert any(ipaddress.ip_address("11.0.0.1") in n for n in nets)
    assert not any(ipaddress.ip_address("10.1.2.3") in n for n in nets)
