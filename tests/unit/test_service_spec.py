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
    assert svc["spec"]["ports"] == [
        {"name": "display", "port": 5900, "targetPort": 5900},
        {"name": "ssh", "port": 22, "targetPort": 22},
    ]


def test_exposes_ssh_for_jump_routing():
    """The gateway splices ProxyJump channels to this Service's DNS name, so
    22 must be published even for a desktop session whose display port is
    something else entirely (design/proxyjump.md)."""
    ports = {p["name"]: p["port"] for p in _build(display_port=8082)["spec"]["ports"]}
    assert ports["ssh"] == 22
    assert ports["display"] == 8082


def test_ssh_mode_session_publishes_ssh_alone():
    """An ssh-mode session (images/devbase: `mode: ssh, runtime: vm`) has no
    display, but it still needs this Service: resolve_ssh_target hands the
    gateway the Service's DNS name, so without one the jump resolves to nothing
    and the client hangs after authenticating. display_port is None there, and
    a display port nothing serves must not be advertised."""
    svc = _build(session_name="alice-devbox", display_port=None)
    assert svc["spec"]["ports"] == [{"name": "ssh", "port": 22, "targetPort": 22}]
    assert svc["spec"]["selector"] == {"session": "alice-devbox"}


def test_owner_reference_to_session():
    owner = _build()["metadata"]["ownerReferences"][0]
    assert owner["kind"] == "Session"
    assert owner["name"] == "alice-desk"
    assert owner["uid"] == "uid-123"
    assert owner["controller"] is True


def test_name_matches_session():
    svc = _build()
    assert svc["metadata"]["name"] == "alice-desk"
