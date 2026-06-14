"""C1 integration test for the display path: portal -> guacd -> RDP pod.

End-to-end proof of round 2:
  create an RDP DesktopTemplate (backend: pod) + DesktopSession -> operator
  provisions an xrdp pod + per-session Service and drives phase to Ready ->
  open the portal's WebSocket -> the portal performs the guacd handshake (guacd
  dials the desktop pod through the round-2 NetworkPolicy ingress carve-out) ->
  a `ready,<id>` instruction comes back.

Needs the integration harness (scripts/integration.sh) which deploys guacd,
port-forwards it, and runs the portal as a host process, exporting
WHISTLER_TEST_PORTAL. The VM backend is NOT exercised (no KubeVirt). RDP has
more moving parts than VNC (large image pull + xrdp boot + valid creds), so the
WS connect is retried with a generous budget.

Marked `integration` so it is excluded from the default unit run.
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
PORTAL = os.environ.get("WHISTLER_TEST_PORTAL")
TEMPLATE = "rdp-it"
SESSION_SHORT = "rdp1"
SESSION = f"{USER}-{SESSION_SHORT}"
# linuxserver/rdesktop serves RDP on 3389 with default creds abc/abc. Note it is
# amd64-only — on arm64 hosts override with an arm64 xrdp image via
# WHISTLER_TEST_RDP_IMAGE, or run this test on an amd64 cluster.
IMAGE = os.environ.get("WHISTLER_TEST_RDP_IMAGE", "linuxserver/rdesktop:latest")
DISPLAY_PORT = 3389
READY_TIMEOUT = int(os.environ.get("WHISTLER_TEST_TIMEOUT", "300"))


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
            GROUP, VERSION, USER_NS, "desktopsessions", SESSION)
        last = (ds.get("status") or {}).get("phase")
        if last == "Ready":
            return
        if last == "Failed":
            pytest.fail(f"DesktopSession reached Failed: {ds.get('status')}")
        time.sleep(3)
    pytest.fail(f"DesktopSession not Ready within budget (last phase: {last})")


async def _expect_ready_over_ws(deadline):
    """Connect the portal WS and wait for a guacd `ready` instruction, retrying
    while xrdp finishes booting."""
    import aiohttp
    from whistler.portal.protocol import Decoder

    url = f"{PORTAL.replace('http', 'ws', 1)}/ws/{SESSION_SHORT}?user={USER}"
    last_err = None
    async with aiohttp.ClientSession() as http:
        while time.time() < deadline:
            try:
                async with http.ws_connect(url, timeout=30) as ws:
                    decoder = Decoder()
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            break
                        for instr in decoder.feed(msg.data.encode("utf-8")):
                            if instr and instr[0] == "ready":
                                return instr
                            if instr and instr[0] == "error":
                                last_err = instr
                                break
                        else:
                            continue
                        break
            except Exception as e:  # noqa: BLE001 - retry transient connect/boot errors
                last_err = e
            time.sleep(5)
    pytest.fail(f"No `ready` from guacd within budget (last: {last_err})")


async def test_display_path_reaches_ready():
    if not PORTAL:
        pytest.skip("WHISTLER_TEST_PORTAL not set; display integration env not configured")
    custom, core = _apis()
    from kubernetes.client.rest import ApiException

    try:
        core.create_namespace({"metadata": {"name": USER_NS}})
    except ApiException as e:
        if e.status != 409:
            raise

    template = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "DesktopTemplate",
        "metadata": {"name": TEMPLATE, "namespace": SYS_NS},
        "spec": {
            "user": "system",
            "image": IMAGE,
            "backend": "pod",
            "protocol": "rdp",
            "displayPort": DISPLAY_PORT,
            "persistence": "ephemeral",
            "connectionParams": {
                "username": "abc", "password": "abc",
                "security": "any", "ignore-cert": "true",
            },
            "resources": {"cpu": "500m", "memory": "2Gi"},
        },
    }
    session = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "DesktopSession",
        "metadata": {"name": SESSION, "namespace": USER_NS},
        "spec": {"templateRef": TEMPLATE, "user": USER},
    }

    try:
        try:
            custom.create_namespaced_custom_object(
                GROUP, VERSION, SYS_NS, "desktoptemplates", template)
        except ApiException as e:
            if e.status != 409:
                raise
        custom.create_namespaced_custom_object(
            GROUP, VERSION, USER_NS, "desktopsessions", session)

        deadline = time.time() + READY_TIMEOUT
        _wait_phase_ready(custom, deadline)
        ready = await _expect_ready_over_ws(deadline)
        assert ready[0] == "ready"
    finally:
        for delete in (
            lambda: custom.delete_namespaced_custom_object(
                GROUP, VERSION, USER_NS, "desktopsessions", SESSION),
            lambda: custom.delete_namespaced_custom_object(
                GROUP, VERSION, SYS_NS, "desktoptemplates", TEMPLATE),
        ):
            try:
                delete()
            except ApiException:
                pass
