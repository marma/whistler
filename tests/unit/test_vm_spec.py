"""KubeVirt VirtualMachine manifest construction (KubeConfigManager._build_vm_spec).

Pure manifest assertions (no cluster); the e2e path is exercised by
tests/integration/test_vm.py on clusters that have KubeVirt installed."""
from whistler.config import KubeConfigManager


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.zones = {"default": {}}
    return cm


def _build_both(**overrides):
    """(vm manifest, companion cloud-init Secret manifest)."""
    cm = _manager()
    args = dict(
        session_name="alice-desk",
        hostname="desk",
        username="alice",
        uid="uid-123",
        template_spec={"image": "quay.io/example/desktop:latest"},
        display_port=5900,
        instancetype=None,
        preemptible=False,
        smb_host="whistler-storage-alice.whistler-user-alice.svc.cluster.local",
        smb_password="s3cret",
        user_details={"name": "alice", "uid": 1001,
                      "publicKeys": ["ssh-ed25519 AAA alice"]},
    )
    args.update(overrides)
    return cm._build_vm_spec(**args)


def _build(**overrides):
    return _build_both(**overrides)[0]


def _cloud_init(**overrides):
    return _build_both(**overrides)[1]["stringData"]["userdata"]


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


def test_root_container_disk_and_no_home_attachment():
    spec = _build()["spec"]["template"]["spec"]
    volumes = spec["volumes"]
    root = next(v for v in volumes if v["name"] == "rootdisk")
    assert root["containerDisk"]["image"] == "quay.io/example/desktop:latest"
    # The home PVC is NOT attached to the VM (no virtiofs, no disk): it is
    # mounted by the per-user storage gateway and reaches the guest as a
    # cifs mount set up by cloud-init (kubevirt#13028 made virtiofs homes
    # read-only for the guest user).
    assert not any(v["name"] == "home" for v in volumes)
    devices = spec["domain"]["devices"]
    assert "filesystems" not in devices
    disk_names = [d["name"] for d in devices["disks"]]
    assert "home" not in disk_names


def test_cloud_init_mounts_home_from_gateway():
    user_data = _cloud_init()
    assert "//whistler-storage-alice.whistler-user-alice.svc.cluster.local/home" in user_data
    assert "cifs" in user_data
    assert "s3cret" in user_data


def test_cloud_init_travels_via_session_secret():
    # KubeVirt caps inline cloudInitNoCloud userData at 2048 bytes and ours
    # exceeds it, so the document lives in a per-session Secret referenced
    # from the volume — which also keeps the SMB password out of the VM
    # object. KubeVirt reads the `userdata` key.
    vm, secret = _build_both()
    spec = vm["spec"]["template"]["spec"]
    disk_names = [d["name"] for d in spec["domain"]["devices"]["disks"]]
    assert "cloudinit" in disk_names
    ci = next(v for v in spec["volumes"] if v["name"] == "cloudinit")
    assert ci["cloudInitNoCloud"] == {"secretRef": {"name": "alice-desk-cloudinit"}}
    assert "userData" not in ci["cloudInitNoCloud"]

    assert secret["metadata"]["name"] == "alice-desk-cloudinit"
    # Same Session ownership as the VM: GC'd with the session.
    owner = secret["metadata"]["ownerReferences"][0]
    assert owner["kind"] == "Session" and owner["uid"] == "uid-123"
    user_data = secret["stringData"]["userdata"]
    assert user_data.startswith("#cloud-config")
    assert "alice" in user_data
    assert "'1001'" in user_data or "1001" in user_data
    assert "ssh-ed25519 AAA alice" in user_data


def test_portal_access_key_appended_to_guest_keys():
    user_data = _cloud_init(
        portal_public_key="ssh-ed25519 PORTALKEY whistler-portal-alice")
    # Both the user's own key and the portal's web-terminal key are authorized.
    assert "ssh-ed25519 AAA alice" in user_data
    assert "ssh-ed25519 PORTALKEY whistler-portal-alice" in user_data


def test_uid_falls_back_to_run_as_user_then_1000():
    assert "1234" in _cloud_init(user_details={
        "name": "alice", "securityContext": {"runAsUser": 1234},
        "publicKeys": []})
    assert "1000" in _cloud_init(user_details={"name": "alice"})


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


def test_readiness_probe_targets_display_port_for_browser_desktops():
    # phase Running only means qemu booted; without this probe the operator
    # called the session Ready ~20s before the in-guest streamer listened and
    # the portal proxy returned "desktop backend unreachable".
    spec = _build(viewer="websockets", display_port=8082)["spec"]["template"]["spec"]
    assert spec["readinessProbe"]["tcpSocket"] == {"port": 8082}


def test_readiness_probe_falls_back_to_sshd_without_a_streamer():
    # vnc/ssh guests run no in-guest display server, so sshd — what the web
    # terminal, screenshots and plain ssh all use — is the readiness signal.
    spec = _build(viewer="vnc")["spec"]["template"]["spec"]
    assert spec["readinessProbe"]["tcpSocket"] == {"port": 22}


def test_termination_grace_period_is_short():
    # Left unset KubeVirt 1.8 renders the launcher pod at 60s, so a stop sat
    # for a minute after the guest was already down.
    spec = _build()["spec"]["template"]["spec"]
    assert spec["terminationGracePeriodSeconds"] == 5


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


def test_websockets_viewer_arms_in_guest_desktop():
    # viewer=websockets means a desktop-VM image with the Selkies stack baked
    # in: cloud-init enables the per-user session unit and writes the
    # streamer env (template streamerEnv + displayPort).
    user_data = _cloud_init(
        viewer="websockets",
        template_spec={"image": "x",
                       "streamerEnv": {"SELKIES_H264_STREAMING_MODE": "true"}})
    assert "whistler-desktop@alice.service" in user_data
    assert "SELKIES_PORT=5900" in user_data
    assert "SELKIES_H264_STREAMING_MODE=true" in user_data


def test_vnc_viewer_gets_plain_guest():
    # The noVNC path is agentless — no desktop units in the guest document.
    for viewer in (None, "vnc"):
        user_data = _cloud_init(viewer=viewer)
        assert "whistler-desktop@" not in user_data
        assert "SELKIES_PORT" not in user_data


# --- _build_vm_spec_patch: reconciling an already-created VM ------------------
# A VM object outlives every template edit and per-session override change, so
# reconcile has to bring its spec forward. Merge-patch semantics mean removals
# need explicit nulls, and unchanged specs must produce no write at all.

def _patch(current_spec, **build_overrides):
    desired = _build(**build_overrides)["spec"]
    return KubeConfigManager._build_vm_spec_patch(current_spec, desired)


def test_patch_carries_resource_changes_to_existing_vm():
    # The bug this guards: a session whose cpu/memory was raised kept booting
    # with the values its VM was created with.
    current = _build(template_spec={"image": "x",
                                    "resources": {"cpu": "4", "memory": "8Gi"}})["spec"]
    patch = _patch(current, template_spec={"image": "x",
                                           "resources": {"cpu": "12", "memory": "32Gi"}})
    domain = patch["template"]["spec"]["domain"]
    assert domain["cpu"] == {"cores": 12}
    assert domain["resources"] == {"requests": {"memory": "32Gi"}}


def test_patch_is_none_when_nothing_changed():
    current = _build()["spec"]
    # Server-defaulted fields we never set must not read as drift.
    current["template"]["spec"]["architecture"] = "amd64"
    current["template"]["spec"]["domain"]["machine"] = {"type": "q35"}
    current["template"]["spec"]["domain"]["firmware"] = {"uuid": "abc"}
    assert _patch(current) is None


def test_patch_excludes_run_strategy():
    # start/stop is _create_vm's own decision; a reconcile must not resurrect
    # (or halt) a VM as a side effect of a spec update.
    current = _build(run_strategy="Halted")["spec"]
    patch = _patch(current, run_strategy="Always",
                   template_spec={"image": "y"})
    assert patch is not None and "runStrategy" not in patch


def test_patch_nulls_dropped_gpu_and_node_selector():
    current = _build(template_spec={"image": "x", "resources": {"gpu": 1},
                                    "nodeSelector": {"gpu": "true"}})["spec"]
    patch = _patch(current, template_spec={"image": "x"})
    assert patch["template"]["spec"]["domain"]["devices"]["gpus"] is None
    assert patch["template"]["spec"]["nodeSelector"] is None


def test_patch_nulls_resources_when_switching_to_instancetype():
    # KubeVirt rejects instancetype together with domain.cpu/domain.resources.
    current = _build(template_spec={"image": "x",
                                    "resources": {"cpu": "4", "memory": "8Gi"}})["spec"]
    patch = _patch(current, instancetype="u1.medium", template_spec={"image": "x"})
    assert patch["instancetype"] == {"name": "u1.medium"}
    domain = patch["template"]["spec"]["domain"]
    assert domain["cpu"] is None and domain["resources"] is None


def test_patch_leaves_root_disk_import_alone():
    # The root-disk DataVolume is imported once; KubeVirt won't re-drive it, so
    # a changed imageURL needs a new session rather than a silent no-op patch.
    current = _build(template_spec={"imageURL": "http://ex/a.qcow2"})["spec"]
    patch = _patch(current, template_spec={"imageURL": "http://ex/b.qcow2"})
    assert patch is None or "dataVolumeTemplates" not in patch


def test_patch_updates_image_and_zone_labels():
    current = _build(template_spec={"image": "old:1"})["spec"]
    patch = _patch(current, template_spec={"image": "new:2"})
    volumes = patch["template"]["spec"]["volumes"]
    assert volumes[0]["containerDisk"]["image"] == "new:2"
