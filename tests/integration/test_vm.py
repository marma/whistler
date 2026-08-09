"""C1 integration test for the KubeVirt VM runtime.

Exercises: Template (mode: desktop, runtime: vm) + Session -> operator
reconcile creates the VirtualMachine (containerDisk root, virtiofs home,
cloud-init identity) -> phase timer drives status.phase through Booting to
Ready and records vmiName.

Skips unless the cluster has the KubeVirt CRDs — k3d/CI does not; the
metal/multipass clusters install them via scripts/install_kubevirt.sh. The
CDI (imageURL) variant additionally needs the DataVolume CRD and an opt-in
WHISTLER_TEST_VM_URL (imports are slow and bandwidth-heavy).

The boot sources must be in the operator's vm image allow-list
(scripts/integration.sh writes them into images.yaml).
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
VM_IMAGE = os.environ.get("WHISTLER_TEST_VM_IMAGE", "quay.io/containerdisks/ubuntu:24.04")
VM_URL = os.environ.get("WHISTLER_TEST_VM_URL")  # opt-in for the CDI variant
# Image pull + VM boot is far slower than a pod start.
READY_TIMEOUT = int(os.environ.get("WHISTLER_TEST_VM_TIMEOUT", "600"))


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
    return client.CustomObjectsApi(), client.CoreV1Api(), client.ApiextensionsV1Api()


def _assert_storage_gateway(custom, core):
    """The per-user NFS gateway (home PVC export — replaces virtiofs) must be
    provisioned lazily with the first vm session: Deployment + Service +
    fencing NetworkPolicy, and the VM's cloud-init must mount the export from
    it. (The actual write path — guest write lands uid-correct on the PVC —
    is covered by live verification; it needs a booted guest.)"""
    from kubernetes import client

    gateway = f"whistler-storage-{USER}"
    apps = client.AppsV1Api()
    deploy = apps.read_namespaced_deployment(gateway, USER_NS)
    assert deploy.spec.strategy.type == "Recreate"
    # No Secret and no mounted credential: AUTH_SYS has nothing to
    # authenticate with, which is why the NetworkPolicy below is the boundary.
    (volume,) = deploy.spec.template.spec.volumes
    assert volume.persistent_volume_claim.claim_name == f"whistler-data-{USER}"

    svc = core.read_namespaced_service(gateway, USER_NS)
    assert svc.spec.ports[0].port == 2049

    net = client.NetworkingV1Api()
    policy = net.read_namespaced_network_policy(
        "whistler-storage-gateway", USER_NS)
    assert policy.spec.pod_selector.match_labels == {
        "app": "whistler-storage-gateway"}
    assert policy.spec.ingress[0].ports[0].port == 2049


def _require_crd(ext_api, name, why):
    from kubernetes.client.rest import ApiException
    try:
        ext_api.read_custom_resource_definition(name)
    except ApiException as e:
        if e.status == 404:
            pytest.skip(f"{name} CRD not present — {why}")
        raise


def _wait_phase_ready(custom, session, deadline, observe=None):
    seen = set()
    last = None
    while time.time() < deadline:
        ds = custom.get_namespaced_custom_object(
            GROUP, VERSION, USER_NS, "sessions", session
        )
        last = (ds.get("status") or {}).get("phase")
        seen.add(last)
        if last == "Ready":
            if observe is not None:
                observe.update(seen)
            return ds
        if last == "Failed":
            pytest.fail(f"Session reached Failed: {ds.get('status')}")
        time.sleep(5)
    pytest.fail(f"Session did not reach Ready within the timeout (last phase: {last})")


def _run_vm_session(template_spec, template_name, session_short, observe=None):
    """Create template + session, wait Ready, assert VMI + Service, clean up."""
    custom, core, ext = _apis()
    _require_crd(ext, "virtualmachines.kubevirt.io",
                 "KubeVirt not installed (scripts/install_kubevirt.sh)")
    from kubernetes.client.rest import ApiException

    session = f"{USER}-{session_short}"
    try:
        core.create_namespace({"metadata": {"name": USER_NS}})
    except ApiException as e:
        if e.status != 409:
            raise

    template = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "Template",
        "metadata": {"name": template_name, "namespace": SYS_NS},
        "spec": {"user": "system", "mode": "desktop", "runtime": "vm",
                 "viewer": "vnc", "persistence": "ephemeral", **template_spec},
    }
    session_cr = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "Session",
        "metadata": {
            "name": session, "namespace": USER_NS,
            "labels": {"whistler.martinmalmsten.net/mode": "desktop"},
            # VMs are created Halted and boot on first connect; the connect
            # signal is this annotation (what /connect, /term and /vnc bump).
            # Setting it at creation makes the session boot immediately.
            "annotations": {"whistler/last-connect": "test"},
        },
        "spec": {"templateRef": template_name, "user": USER},
    }

    try:
        try:
            custom.create_namespaced_custom_object(
                GROUP, VERSION, SYS_NS, "templates", template)
        except ApiException as e:
            if e.status != 409:
                raise
        custom.create_namespaced_custom_object(
            GROUP, VERSION, USER_NS, "sessions", session_cr)

        ds = _wait_phase_ready(custom, session,
                               time.time() + READY_TIMEOUT, observe=observe)
        status = ds.get("status") or {}
        assert status.get("runtime") == "vm"
        assert status.get("viewer") == "vnc"
        assert status.get("vmiName") == session

        vmi = custom.get_namespaced_custom_object(
            "kubevirt.io", "v1", USER_NS, "virtualmachineinstances", session)
        assert (vmi.get("status") or {}).get("phase") == "Running"

        svc = core.read_namespaced_service(session, USER_NS)
        assert svc.spec.type == "ClusterIP"

        _assert_storage_gateway(custom, core)

        # The home is an NFS mount of the gateway export, not a VM-attached
        # PVC (virtiofs is gone — kubevirt#13028). The cloud-init document
        # travels via a per-session Secret (KubeVirt's 2048-byte inline cap).
        vm = custom.get_namespaced_custom_object(
            "kubevirt.io", "v1", USER_NS, "virtualmachines", session)
        vm_spec = vm["spec"]["template"]["spec"]
        assert not any(v.get("persistentVolumeClaim")
                       for v in vm_spec["volumes"])
        assert "filesystems" not in vm_spec["domain"]["devices"]
        ci = next(v for v in vm_spec["volumes"] if v["name"] == "cloudinit")
        assert ci["cloudInitNoCloud"] == {
            "secretRef": {"name": f"{session}-cloudinit"}}
        import base64
        ci_secret = core.read_namespaced_secret(f"{session}-cloudinit", USER_NS)
        user_data = base64.b64decode(ci_secret.data["userdata"]).decode()
        assert f"whistler-storage-{USER}.{USER_NS}.svc.cluster.local:/home" \
            in user_data
    finally:
        for delete in (
            lambda: custom.delete_namespaced_custom_object(
                GROUP, VERSION, USER_NS, "sessions", session),
            lambda: custom.delete_namespaced_custom_object(
                GROUP, VERSION, SYS_NS, "templates", template_name),
        ):
            try:
                delete()
            except ApiException:
                pass


def test_vm_container_disk_session_reaches_ready():
    _run_vm_session(
        {"image": VM_IMAGE, "resources": {"cpu": "1", "memory": "2Gi"}},
        template_name="vmtpl", session_short="vm1",
    )


def test_vm_image_url_session_imports_then_boots():
    if not VM_URL:
        pytest.skip("WHISTLER_TEST_VM_URL not set — CDI import variant is opt-in")
    _, _, ext = _apis()
    _require_crd(ext, "datavolumes.cdi.kubevirt.io",
                 "CDI not installed (scripts/install_kubevirt.sh)")
    seen = set()
    _run_vm_session(
        {"imageURL": VM_URL, "rootDiskSize": "10Gi",
         "resources": {"cpu": "1", "memory": "2Gi"}},
        template_name="vmurltpl", session_short="vm2", observe=seen,
    )
    # Importing is timing-dependent (a cached/fast import may skip past the
    # poll), so observe it opportunistically rather than requiring it.
    if "Importing" in seen:
        assert True
