"""KubeVirt VirtualMachine manifest construction (KubeConfigManager._build_vm_spec).

KubeVirt is not available in any test cluster, so the VM path is verified only
through these pure manifest assertions (see CLAUDE.md / plan: unverified e2e)."""
from whistler.config import KubeConfigManager


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    return cm


def _build(**overrides):
    cm = _manager()
    args = dict(
        session_name="alice-desk",
        hostname="desk",
        username="alice",
        uid="uid-123",
        template_spec={"image": "quay.io/example/desktop:latest"},
        pvc_name="whistler-data-alice",
        display_port=5900,
        instancetype=None,
        preemptible=False,
    )
    args.update(overrides)
    return cm._build_vm_spec(**args)


def test_kind_apiversion_and_running():
    vm = _build()
    assert vm["apiVersion"] == "kubevirt.io/v1"
    assert vm["kind"] == "VirtualMachine"
    assert vm["spec"]["running"] is True


def test_owner_reference_to_session():
    owner = _build()["metadata"]["ownerReferences"][0]
    assert owner["kind"] == "Session"
    assert owner["apiVersion"] == "whistler.martinmalmsten.net/v1"
    assert owner["uid"] == "uid-123"
    assert owner["controller"] is True


def test_session_label_on_template_for_service_selection():
    labels = _build()["spec"]["template"]["metadata"]["labels"]
    assert labels["session"] == "alice-desk"
    assert labels["app"] == "whistler-desktop"


def test_volumes_root_container_disk_and_home_pvc():
    volumes = _build()["spec"]["template"]["spec"]["volumes"]
    root = next(v for v in volumes if v["name"] == "rootdisk")
    home = next(v for v in volumes if v["name"] == "home")
    assert root["containerDisk"]["image"] == "quay.io/example/desktop:latest"
    assert home["persistentVolumeClaim"]["claimName"] == "whistler-data-alice"


def test_pod_network_and_masquerade_interface():
    spec = _build()["spec"]["template"]["spec"]
    assert spec["networks"] == [{"name": "default", "pod": {}}]
    assert spec["domain"]["devices"]["interfaces"] == [{"name": "default", "masquerade": {}}]


def test_instancetype_set_omits_inline_cpu_memory():
    vm = _build(instancetype="u1.medium",
                template_spec={"image": "x", "resources": {"cpu": "4", "memory": "8Gi"}})
    assert vm["spec"]["instancetype"] == {"name": "u1.medium"}
    domain = vm["spec"]["template"]["spec"]["domain"]
    assert "cpu" not in domain
    assert "resources" not in domain


def test_inline_cpu_memory_when_no_instancetype():
    vm = _build(instancetype=None,
                template_spec={"image": "x", "resources": {"cpu": "4", "memory": "8Gi"}})
    domain = vm["spec"]["template"]["spec"]["domain"]
    assert domain["cpu"] == {"cores": 4}
    assert domain["resources"] == {"requests": {"memory": "8Gi"}}
    assert "instancetype" not in vm["spec"]


def test_gpu_devices_present_only_when_requested():
    with_gpu = _build(template_spec={"image": "x", "resources": {"gpu": 1}})
    assert with_gpu["spec"]["template"]["spec"]["domain"]["devices"]["gpus"] == \
        [{"name": "gpu0", "deviceName": "nvidia.com/gpu"}]

    without_gpu = _build(template_spec={"image": "x"})
    assert "gpus" not in without_gpu["spec"]["template"]["spec"]["domain"]["devices"]


def test_node_selector_propagated():
    vm = _build(template_spec={"image": "x", "nodeSelector": {"gpu": "true"}})
    assert vm["spec"]["template"]["spec"]["nodeSelector"] == {"gpu": "true"}
