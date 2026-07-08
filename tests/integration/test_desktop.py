"""C1 integration test for the desktop pod runtime.

Exercises the provisioning path with no SSH/Guacamole in the loop:
  create Template (mode: desktop, runtime: container) + Session -> operator
  reconcile_session_fn ensures the pod + per-session Service -> the phase
  timer drives status.phase to Ready.

The VM backend is NOT exercised here — see tests/integration/test_vm.py, which
skips unless the cluster has the KubeVirt CRDs (k3d/CI does not).

Talks to the cluster directly (the harness exports KUBECONFIG). Marked
`integration` so it is excluded from the default unit run.
"""
import os
import time

import pytest

pytestmark = pytest.mark.integration

GROUP = "whistler.martinmalmsten.net"
VERSION = "v1"
SYS_NS = os.environ.get("WHISTLER_TEST_SYS_NS", "whistler")
USER = os.environ.get("WHISTLER_TEST_USER", "tester")
USER_NS = f"whistler-user-{USER}"
TEMPLATE = "desk"
SESSION_SHORT = "desk1"
SESSION = f"{USER}-{SESSION_SHORT}"
# pause runs forever and reports Ready immediately — enough to validate the
# pod + Service wiring and the phase machine without a real desktop image.
IMAGE = os.environ.get("WHISTLER_TEST_DESKTOP_IMAGE", "registry.k8s.io/pause:3.9")
DISPLAY_PORT = 5901
READY_TIMEOUT = int(os.environ.get("WHISTLER_TEST_TIMEOUT", "180"))


def _apis():
    try:
        from kubernetes import client, config
    except ImportError:
        pytest.skip("kubernetes client not installed")
    try:
        config.load_kube_config()
    except Exception:
        try:
            config.load_incluster_config()
        except Exception:
            pytest.skip("no kube config available; integration env not configured")
    return client.CustomObjectsApi(), client.CoreV1Api()


def _wait_phase_ready(custom, deadline):
    last = None
    while time.time() < deadline:
        ds = custom.get_namespaced_custom_object(
            GROUP, VERSION, USER_NS, "sessions", SESSION
        )
        last = (ds.get("status") or {}).get("phase")
        if last == "Ready":
            return ds
        if last == "Failed":
            pytest.fail(f"Session reached Failed: {ds.get('status')}")
        time.sleep(3)
    pytest.fail(f"Session did not reach Ready within {READY_TIMEOUT}s (last phase: {last})")


def test_desktop_pod_session_reaches_ready():
    custom, core = _apis()

    # Ensure the user namespace exists so the DesktopSession can be created in it.
    from kubernetes.client.rest import ApiException
    try:
        core.create_namespace({"metadata": {"name": USER_NS}})
    except ApiException as e:
        if e.status != 409:
            raise

    desktop_template = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "Template",
        "metadata": {"name": TEMPLATE, "namespace": SYS_NS},
        "spec": {
            "user": "system",
            "mode": "desktop",
            "runtime": "container",
            "image": IMAGE,
            "displayPort": DISPLAY_PORT,
            "persistence": "ephemeral",
            "resources": {"cpu": "100m", "memory": "64Mi"},
        },
    }
    desktop_session = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "Session",
        "metadata": {
            "name": SESSION, "namespace": USER_NS,
            "labels": {"whistler.martinmalmsten.net/mode": "desktop"},
        },
        "spec": {"templateRef": TEMPLATE, "user": USER},
    }

    try:
        try:
            custom.create_namespaced_custom_object(
                GROUP, VERSION, SYS_NS, "templates", desktop_template)
        except ApiException as e:
            if e.status != 409:
                raise
        custom.create_namespaced_custom_object(
            GROUP, VERSION, USER_NS, "sessions", desktop_session)

        _wait_phase_ready(custom, time.time() + READY_TIMEOUT)

        # The operator should have created a pod and a per-session ClusterIP Service.
        pod = core.read_namespaced_pod(SESSION, USER_NS)
        assert pod.status.phase == "Running"

        svc = core.read_namespaced_service(SESSION, USER_NS)
        assert svc.spec.type == "ClusterIP"
        assert any(p.port == DISPLAY_PORT for p in svc.spec.ports)

        # Endpoints populate once the pod is Ready and matches the selector.
        eps = core.read_namespaced_endpoints(SESSION, USER_NS)
        assert eps.subsets and any(s.addresses for s in eps.subsets), \
            "Service has no ready endpoints for the desktop pod"
    finally:
        from kubernetes.client.rest import ApiException as _ApiException
        for delete in (
            lambda: custom.delete_namespaced_custom_object(
                GROUP, VERSION, USER_NS, "sessions", SESSION),
            lambda: custom.delete_namespaced_custom_object(
                GROUP, VERSION, SYS_NS, "templates", TEMPLATE),
        ):
            try:
                delete()
            except _ApiException:
                pass
