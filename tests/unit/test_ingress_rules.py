"""User-namespace ingress carve-outs (KubeConfigManager._build_ingress_rules):
only the two trusted brokers may reach session pods — the portal (display
relay, any port) and the SSH gateway (sshd, port 22 only). Everything else is
denied by the round-1 deny-all-ingress policy."""
import os

from whistler.config import KubeConfigManager


def _manager(namespace="whistler"):
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.namespace = namespace
    return cm


def _rule_for(rules, app):
    for rule in rules:
        if any(peer["podSelector"]["matchLabels"]["app"] == app
               for peer in rule["from"]):
            return rule
    raise AssertionError(f"no ingress rule admits {app}")


def test_allows_portal_broker_no_ports():
    rules = _manager("whistler")._build_ingress_rules()
    rule = _rule_for(rules, "whistler-portal")
    for peer in rule["from"]:
        assert peer["namespaceSelector"]["matchLabels"] == \
            {"kubernetes.io/metadata.name": "whistler"}
    # No port restriction: the source is a trusted broker and the display port
    # varies per template.
    assert "ports" not in rule


def test_allows_ssh_gateway_on_22_only():
    """Jump routing needs the gateway to reach session sshd, and nothing more:
    an unpinned rule would make the gateway a route to every port in the user's
    namespace (design/proxyjump.md)."""
    rules = _manager("whistler")._build_ingress_rules()
    rule = _rule_for(rules, "whistler-server")
    assert rule["ports"] == [{"port": 22, "protocol": "TCP"}]
    for peer in rule["from"]:
        assert peer["namespaceSelector"]["matchLabels"] == \
            {"kubernetes.io/metadata.name": "whistler"}


def test_brokers_are_separate_rules():
    """`from` and `ports` are ANDed within one rule, so folding the two peers
    together would either drop the gateway's port pin or impose it on the
    portal."""
    rules = _manager("whistler")._build_ingress_rules()
    assert len(rules) == 2
    for rule in rules:
        apps = {peer["podSelector"]["matchLabels"]["app"] for peer in rule["from"]}
        assert len(apps) == 1


def test_no_other_peers_are_admitted():
    rules = _manager("whistler")._build_ingress_rules()
    apps = {peer["podSelector"]["matchLabels"]["app"]
            for rule in rules for peer in rule["from"]}
    assert apps == {"whistler-portal", "whistler-server"}


def test_broker_namespace_env_override(monkeypatch):
    monkeypatch.setenv("PORTAL_NAMESPACE", "infra")
    rules = _manager("whistler")._build_ingress_rules()
    for rule in rules:
        for peer in rule["from"]:
            assert peer["namespaceSelector"]["matchLabels"] == \
                {"kubernetes.io/metadata.name": "infra"}
