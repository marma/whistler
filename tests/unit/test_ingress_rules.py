"""User-namespace ingress carve-out for guacd (KubeConfigManager._build_ingress_rules)."""
import os

from whistler.config import KubeConfigManager


def _manager(namespace="whistler"):
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.namespace = namespace
    return cm


def test_allows_only_guacd_pod_no_ports():
    rules = _manager("whistler")._build_ingress_rules()
    assert len(rules) == 1
    peer = rules[0]["from"][0]
    assert peer["namespaceSelector"]["matchLabels"] == {"kubernetes.io/metadata.name": "whistler"}
    assert peer["podSelector"]["matchLabels"] == {"app": "whistler-guacd"}
    # No port restriction: source is the single trusted guacd Deployment and the
    # display port varies per template.
    assert "ports" not in rules[0]


def test_guacd_namespace_env_override(monkeypatch):
    monkeypatch.setenv("GUACD_NAMESPACE", "infra")
    rules = _manager("whistler")._build_ingress_rules()
    assert rules[0]["from"][0]["namespaceSelector"]["matchLabels"] == \
        {"kubernetes.io/metadata.name": "infra"}
