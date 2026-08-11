"""Per-session Service reconciliation (KubeConfigManager._ensure_session_service).

The manifest builder is covered in test_service_spec.py; this is about what
happens when the Service already exists. It used to return on the 409 and
leave whatever was there, so adding the ssh port published nothing on sessions
that already existed — and dialling a ClusterIP port the Service does not
declare is not refused, it is dropped, so the jump timed out against a
perfectly healthy VM.
"""
import pytest
from kubernetes.client.rest import ApiException

from whistler.config import KubeConfigManager


class _FakeServices:
    def __init__(self, existing=False):
        self.existing = existing
        self.created = []
        self.patched = []

    def create_namespaced_service(self, namespace, body):
        if self.existing:
            raise ApiException(status=409, reason="AlreadyExists")
        self.created.append((namespace, body))

    def patch_namespaced_service(self, name, namespace, body):
        self.patched.append((name, namespace, body))


@pytest.fixture
def manager(monkeypatch):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    return cm


def _ensure(cm, monkeypatch, api):
    monkeypatch.setattr("whistler.config.client.CoreV1Api", lambda: api)
    return cm._ensure_session_service(
        session_name="alice-box", username="alice", uid="uid-1",
        namespace="whistler-alice", display_port=8082)


def test_creates_when_absent(manager, monkeypatch):
    api = _FakeServices(existing=False)
    assert _ensure(manager, monkeypatch, api) is True
    assert api.patched == []
    ports = api.created[0][1]["spec"]["ports"]
    assert {p["name"] for p in ports} == {"display", "ssh"}


def test_patches_an_existing_service_with_the_current_ports(manager, monkeypatch):
    api = _FakeServices(existing=True)
    assert _ensure(manager, monkeypatch, api) is True
    name, namespace, body = api.patched[0]
    assert (name, namespace) == ("alice-box", "whistler-alice")
    assert {p["name"] for p in body["spec"]["ports"]} == {"display", "ssh"}


def test_patch_leaves_immutable_fields_alone(manager, monkeypatch):
    """clusterIP is assigned by the API server and cannot be changed, so the
    patch must carry only what Whistler owns."""
    api = _FakeServices(existing=True)
    _ensure(manager, monkeypatch, api)
    patch = api.patched[0][2]
    assert set(patch) == {"spec"}
    assert set(patch["spec"]) == {"ports", "selector"}


def test_reports_failure_when_the_patch_is_rejected(manager, monkeypatch):
    api = _FakeServices(existing=True)

    def boom(name, namespace, body):
        raise ApiException(status=422, reason="Invalid")

    api.patch_namespaced_service = boom
    assert _ensure(manager, monkeypatch, api) is False


def test_create_failure_other_than_conflict_is_reported(manager, monkeypatch):
    api = _FakeServices()

    def boom(namespace, body):
        raise ApiException(status=403, reason="Forbidden")

    api.create_namespaced_service = boom
    assert _ensure(manager, monkeypatch, api) is False
