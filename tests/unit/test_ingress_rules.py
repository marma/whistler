"""User-namespace ingress carve-out for the display broker
(KubeConfigManager._build_ingress_rules): only the portal (websockets viewer)
may reach desktop pods; everything else is denied."""
import os

from whistler.config import KubeConfigManager


def _manager(namespace="whistler"):
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.namespace = namespace
    return cm


def _peer_apps(rules):
    return {peer["podSelector"]["matchLabels"]["app"] for peer in rules[0]["from"]}


def test_allows_portal_broker_no_ports():
    rules = _manager("whistler")._build_ingress_rules()
    assert len(rules) == 1
    assert _peer_apps(rules) == {"whistler-portal"}
    for peer in rules[0]["from"]:
        assert peer["namespaceSelector"]["matchLabels"] == \
            {"kubernetes.io/metadata.name": "whistler"}
    # No port restriction: the source is a trusted broker and the display port
    # varies per template.
    assert "ports" not in rules[0]


def test_broker_namespace_env_override(monkeypatch):
    monkeypatch.setenv("PORTAL_NAMESPACE", "infra")
    rules = _manager("whistler")._build_ingress_rules()
    for peer in rules[0]["from"]:
        assert peer["namespaceSelector"]["matchLabels"] == \
            {"kubernetes.io/metadata.name": "infra"}
