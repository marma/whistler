"""User-namespace ingress carve-out for the display brokers
(KubeConfigManager._build_ingress_rules): the shared guacd (guacd viewer) and the
portal (webrtc viewer) may reach desktop pods; everything else is denied."""
import os

from whistler.config import KubeConfigManager


def _manager(namespace="whistler"):
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.namespace = namespace
    return cm


def _peer_apps(rules):
    return {peer["podSelector"]["matchLabels"]["app"] for peer in rules[0]["from"]}


def test_allows_both_brokers_no_ports():
    rules = _manager("whistler")._build_ingress_rules()
    assert len(rules) == 1
    assert _peer_apps(rules) == {"whistler-guacd", "whistler-portal"}
    for peer in rules[0]["from"]:
        assert peer["namespaceSelector"]["matchLabels"] == \
            {"kubernetes.io/metadata.name": "whistler"}
    # No port restriction: sources are trusted brokers and the display/signaling
    # port varies per template.
    assert "ports" not in rules[0]


def test_broker_namespace_env_override(monkeypatch):
    monkeypatch.setenv("GUACD_NAMESPACE", "infra")
    rules = _manager("whistler")._build_ingress_rules()
    for peer in rules[0]["from"]:
        assert peer["namespaceSelector"]["matchLabels"] == \
            {"kubernetes.io/metadata.name": "infra"}
