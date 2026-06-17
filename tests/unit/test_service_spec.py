"""Per-session Service manifest construction (KubeConfigManager._build_session_service)."""
from whistler.config import KubeConfigManager


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    return cm


def _build(**overrides):
    args = dict(session_name="alice-desk", username="alice", uid="uid-123", display_port=5901)
    args.update(overrides)
    return _manager()._build_session_service(**args)


def test_clusterip_port_and_selector():
    svc = _build(display_port=5900)
    assert svc["spec"]["type"] == "ClusterIP"
    assert svc["spec"]["selector"] == {"session": "alice-desk"}
    assert svc["spec"]["ports"] == [{"name": "display", "port": 5900, "targetPort": 5900}]


def test_owner_reference_to_session():
    owner = _build()["metadata"]["ownerReferences"][0]
    assert owner["kind"] == "Session"
    assert owner["name"] == "alice-desk"
    assert owner["uid"] == "uid-123"
    assert owner["controller"] is True


def test_name_matches_session():
    svc = _build()
    assert svc["metadata"]["name"] == "alice-desk"
