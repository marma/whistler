"""KubeVirt VirtualMachine manifest construction (KubeConfigManager._build_vm_spec).

Pure manifest assertions (no cluster); the e2e path is exercised by
tests/integration/test_vm.py on clusters that have KubeVirt installed."""
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
        user_details={"name": "alice", "uid": 1001,
                      "publicKeys": ["ssh-ed25519 AAA alice"]},
    )
    args.update(overrides)
    return cm._build_vm_spec(**args)


def test_kind_apiversion_and_run_strategy():
    vm = _build()
    assert vm["apiVersion"] == "kubevirt.io/v1"
    assert vm["kind"] == "VirtualMachine"
    # runStrategy (not `running`): stop/start is a spec patch Halted/Always.
    # Default is Halted — VMs boot on first connect, not on session creation.
    assert vm["spec"]["runStrategy"] == "Halted"
    assert "running" not in vm["spec"]


def test_run_strategy_always_when_start_requested():
    # ensure_session passes Always when the last-connect annotation is already
    # present (create + connect coalesced into one reconcile).
    vm = _build(run_strategy="Always")
    assert vm["spec"]["runStrategy"] == "Always"


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


def test_root_container_disk_and_home_virtiofs():
    spec = _build()["spec"]["template"]["spec"]
    volumes = spec["volumes"]
    root = next(v for v in volumes if v["name"] == "rootdisk")
    home = next(v for v in volumes if v["name"] == "home")
    assert root["containerDisk"]["image"] == "quay.io/example/desktop:latest"
    assert home["persistentVolumeClaim"]["claimName"] == "whistler-data-alice"
    # The home PVC is a virtiofs *filesystem* device, not a disk: a
    # filesystem-mode PVC attached as a disk expects a disk.img and would not
    # share files with pod sessions on the same PVC.
    devices = spec["domain"]["devices"]
    assert devices["filesystems"] == [{"name": "home", "virtiofs": {}}]
    disk_names = [d["name"] for d in devices["disks"]]
    assert "home" not in disk_names


def test_cloud_init_volume_carries_identity():
    spec = _build()["spec"]["template"]["spec"]
    disk_names = [d["name"] for d in spec["domain"]["devices"]["disks"]]
    assert "cloudinit" in disk_names
    ci = next(v for v in spec["volumes"] if v["name"] == "cloudinit")
    user_data = ci["cloudInitNoCloud"]["userData"]
    assert user_data.startswith("#cloud-config")
    assert "alice" in user_data
    assert "'1001'" in user_data or "1001" in user_data
    assert "ssh-ed25519 AAA alice" in user_data


def test_portal_access_key_appended_to_guest_keys():
    vm = _build(portal_public_key="ssh-ed25519 PORTALKEY whistler-portal-alice")
    user_data = _cloud_init(vm)
    # Both the user's own key and the portal's web-terminal key are authorized.
    assert "ssh-ed25519 AAA alice" in user_data
    assert "ssh-ed25519 PORTALKEY whistler-portal-alice" in user_data


def test_uid_falls_back_to_run_as_user_then_1000():
    via_sec_ctx = _build(user_details={"name": "alice",
                                       "securityContext": {"runAsUser": 1234},
                                       "publicKeys": []})
    assert "1234" in _cloud_init(via_sec_ctx)
    bare = _build(user_details={"name": "alice"})
    assert "1000" in _cloud_init(bare)


def _cloud_init(vm):
    volumes = vm["spec"]["template"]["spec"]["volumes"]
    return next(v for v in volumes if v["name"] == "cloudinit")["cloudInitNoCloud"]["userData"]


def test_image_url_uses_data_volume_template():
    vm = _build(template_spec={"imageURL": "https://example.com/noble.img",
                               "rootDiskSize": "30Gi"})
    root = next(v for v in vm["spec"]["template"]["spec"]["volumes"]
                if v["name"] == "rootdisk")
    assert root["dataVolume"]["name"] == "alice-desk-root"
    (dvt,) = vm["spec"]["dataVolumeTemplates"]
    assert dvt["metadata"]["name"] == "alice-desk-root"
    assert dvt["spec"]["source"]["http"]["url"] == "https://example.com/noble.img"
    assert dvt["spec"]["storage"]["resources"]["requests"]["storage"] == "30Gi"


def test_image_url_root_disk_size_defaults_to_20gi():
    vm = _build(template_spec={"imageURL": "https://example.com/noble.img"})
    (dvt,) = vm["spec"]["dataVolumeTemplates"]
    assert dvt["spec"]["storage"]["resources"]["requests"]["storage"] == "20Gi"


def test_container_disk_has_no_data_volume_templates():
    vm = _build()
    assert "dataVolumeTemplates" not in vm["spec"]


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
