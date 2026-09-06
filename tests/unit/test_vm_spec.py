"""KubeVirt VirtualMachine manifest construction (KubeConfigManager._build_vm_spec).

Pure manifest assertions (no cluster); the e2e path is exercised by
tests/integration/test_vm.py on clusters that have KubeVirt installed."""
import pytest

from whistler.config import (KubeConfigManager, PolicyError,
                             VM_RUN_STRATEGY_RUNNING)


# What get_gpu_catalog would derive from a single-4090 passthrough cluster
# (node gpu.product label + KubeVirt permittedHostDevices).
GPU_CATALOG = [{"name": "NVIDIA-GeForce-RTX-4090", "count": 1,
                "vmResource": "nvidia.com/AD102_GEFORCE_RTX_4090"}]


def _manager(catalog=GPU_CATALOG, huge_page_size=None):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.zones = {"default": {}}
    cm.get_gpu_catalog = lambda: catalog
    # Left None the class default (DEFAULT_HUGE_PAGE_SIZE) stands, which is
    # what a cluster running the chart's whistler.hugePages.pageSize has.
    if huge_page_size is not None:
        cm.huge_page_size = huge_page_size
    return cm


def _build_both(**overrides):
    """(vm manifest, companion cloud-init Secret manifest)."""
    cm = _manager(overrides.pop("_catalog", GPU_CATALOG),
                  overrides.pop("_huge_page_size", None))
    args = dict(
        session_name="alice-desk",
        hostname="desk",
        username="alice",
        uid="uid-123",
        template_spec={"image": "quay.io/example/desktop:latest"},
        display_port=5900,
        instancetype=None,
        preemptible=False,
        home_pvc="whistler-home-alice-desk",
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
    # runStrategy (not `running`): stop/start is a spec patch, Halted or
    # RerunOnFailure. Default is Halted — VMs boot on first connect, not on
    # session creation.
    assert vm["spec"]["runStrategy"] == "Halted"
    assert "running" not in vm["spec"]


def test_run_strategy_rerun_on_failure_when_start_requested():
    # ensure_session passes the running strategy when the last-connect
    # annotation is already present (create + connect coalesced into one
    # reconcile). NOT Always: that respawns the VMI whenever it terminates, so
    # a guest choosing Power Off got a reboot instead of a shutdown.
    vm = _build(run_strategy=VM_RUN_STRATEGY_RUNNING)
    assert vm["spec"]["runStrategy"] == "RerunOnFailure"


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


def test_root_container_disk_and_home_disk_attached():
    spec = _build()["spec"]["template"]["spec"]
    volumes = spec["volumes"]
    root = next(v for v in volumes if v["name"] == "rootdisk")
    assert root["containerDisk"]["image"] == "quay.io/example/desktop:latest"
    # The home is a per-instance PVC attached as a second virtio-blk disk.
    # Never virtiofs: KubeVirt runs virtiofsd unprivileged (kubevirt#13028),
    # which makes a shared home read-only for the guest user.
    home = next(v for v in volumes if v["name"] == "homedisk")
    assert home["persistentVolumeClaim"]["claimName"] == \
        "whistler-home-alice-desk"
    devices = spec["domain"]["devices"]
    assert "filesystems" not in devices
    assert [d["name"] for d in devices["disks"]] == \
        ["rootdisk", "cloudinit", "homedisk"]


def test_home_disk_carries_the_serial_the_guest_looks_up():
    # udev turns this into /dev/disk/by-id/virtio-<serial>, which is how the
    # guest finds the disk. Without it the guest would have to guess a device
    # name and could format or mount the wrong disk as someone's home.
    from whistler.cloudinit import HOME_DISK_SERIAL
    disks = _build()["spec"]["template"]["spec"]["domain"]["devices"]["disks"]
    home = next(d for d in disks if d["name"] == "homedisk")
    assert home["serial"] == HOME_DISK_SERIAL
    assert home["disk"]["bus"] == "virtio"


def test_no_home_pvc_attaches_no_disk():
    # Every VM gets a home disk today (gating it on `persistence` gave the
    # desktop templates no home at all), so this pins the None contract
    # rather than a live configuration.
    spec = _build(home_pvc=None)["spec"]["template"]["spec"]
    assert not any(v["name"] == "homedisk" for v in spec["volumes"])
    assert [d["name"] for d in spec["domain"]["devices"]["disks"]] == \
        ["rootdisk", "cloudinit"]


def test_cloud_init_formats_and_mounts_the_home_disk():
    user_data = _cloud_init()
    assert "/dev/disk/by-id/virtio-" in user_data
    assert "mkfs.ext4" in user_data
    assert "nfs4" not in user_data


def test_cloud_init_travels_via_session_secret():
    # KubeVirt caps inline cloudInitNoCloud userData at 2048 bytes and ours
    # exceeds it, so the document lives in a per-session Secret referenced
    # from the volume. KubeVirt reads the `userdata` key.
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


def test_termination_grace_period_is_the_cluster_default():
    # The window a guest gets to act on the ACPI power button. It is a
    # ceiling, not a wait — but it has to be long enough for systemd to
    # unmount an ext4 $HOME, which the old 5s was not.
    spec = _build()["spec"]["template"]["spec"]
    assert spec["terminationGracePeriodSeconds"] == 30


def test_a_template_may_set_its_own_shutdown_grace():
    spec = _build(template_spec={"image": "x",
                                 "resources": {"shutdownGraceSeconds": 120}}
                  )["spec"]["template"]["spec"]
    assert spec["terminationGracePeriodSeconds"] == 120


def test_a_template_may_ask_for_no_grace_at_all():
    # 0 is a real answer (destroy at once), so presence decides, not truth.
    spec = _build(template_spec={"image": "x",
                                 "resources": {"shutdownGraceSeconds": 0}}
                  )["spec"]["template"]["spec"]
    assert spec["terminationGracePeriodSeconds"] == 0


def test_a_nonsense_shutdown_grace_falls_back_to_the_default():
    # Refusing the session over it would turn a typo into a VM that never
    # starts, reported by KubeVirt far from the template that caused it.
    for bad in ("soon", -5, None):
        spec = _build(template_spec={"image": "x",
                                     "resources": {"shutdownGraceSeconds": bad}}
                      )["spec"]["template"]["spec"]
        assert spec["terminationGracePeriodSeconds"] == 30


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


# --- hugepages ----------------------------------------------------------------
# Guest RAM on hugepages is what keeps VFIO's DMA pinning of a large GPU guest
# inside virt-handler's 20s SyncVMI deadline (see DEFAULT_HUGE_PAGE_SIZE). It
# is on for every VM, so the interesting cases are the ways it must NOT be
# emitted — each one is a VM that would never schedule or never be admitted.

def _domain(**overrides):
    return _build(**overrides)["spec"]["template"]["spec"]["domain"]


def test_guest_memory_is_backed_by_hugepages_by_default():
    domain = _domain(template_spec={"image": "x", "resources": {"memory": "8Gi"}})
    assert domain["memory"] == {"hugepages": {"pageSize": "2Mi"}}


def test_template_may_pick_another_page_size():
    domain = _domain(template_spec={"image": "x", "resources": {
        "memory": "8Gi", "hugePageSize": "1Gi"}})
    assert domain["memory"] == {"hugepages": {"pageSize": "1Gi"}}


def test_template_opts_out_with_an_empty_page_size():
    # Empty is a value, not a missing key: this template runs on 4KiB pages on
    # a node with no reservation, while the rest of the cluster uses hugepages.
    domain = _domain(template_spec={"image": "x", "resources": {
        "memory": "8Gi", "hugePageSize": ""}})
    assert "memory" not in domain


def test_cluster_default_off_leaves_every_guest_on_4k_pages():
    domain = _domain(_huge_page_size="",
                     template_spec={"image": "x", "resources": {"memory": "8Gi"}})
    assert "memory" not in domain


def test_no_memory_request_means_no_hugepages():
    # KubeVirt's webhook rejects hugepages with nothing to size them against.
    domain = _domain(template_spec={"image": "x", "resources": {"cpu": "4"}})
    assert "memory" not in domain


def test_instancetype_owns_memory_including_hugepages():
    # The instancetype applier conflicts on domain.memory exactly as it does
    # on domain.cpu/domain.resources.
    domain = _domain(instancetype="u1.medium",
                     template_spec={"image": "x", "resources": {"memory": "8Gi"}})
    assert "memory" not in domain


def test_memory_is_rounded_up_to_a_whole_number_of_pages():
    # KubeVirt's webhook rejects guest RAM that isn't a whole number of pages,
    # at VM creation and far from the template that caused it. Rounding *up*
    # rather than refusing keeps pageSize a cluster-level decision (raising it
    # to 1Gi would otherwise invalidate every template not sized in whole
    # gigabytes) — and up, never down, so nobody silently gets less RAM than
    # the template asked for.
    domain = _domain(template_spec={"image": "x", "resources": {"memory": "9Mi"}})
    assert domain["resources"]["requests"]["memory"] == "10Mi"
    assert domain["memory"] == {"hugepages": {"pageSize": "2Mi"}}


def test_memory_below_one_page_becomes_one_page():
    domain = _domain(template_spec={"image": "x", "resources": {"memory": "1Mi"}})
    assert domain["resources"]["requests"]["memory"] == "2Mi"


def test_rounding_follows_the_page_size_in_force():
    # Same memory, a page size 512x larger: the rounding is not against a
    # constant.
    domain = _domain(template_spec={"image": "x", "resources": {
        "memory": "1536Mi", "hugePageSize": "1Gi"}})
    assert domain["resources"]["requests"]["memory"] == "2Gi"


def test_memory_that_already_fits_is_left_exactly_as_written():
    # No reformatting of a value that needs no change: the manifest should
    # keep saying what the template says.
    domain = _domain(template_spec={"image": "x", "resources": {"memory": "8192Mi"}})
    assert domain["resources"]["requests"]["memory"] == "8192Mi"


def test_no_hugepages_means_no_rounding():
    # An opted-out template runs on 4KiB pages, where any size is legal.
    domain = _domain(template_spec={"image": "x", "resources": {
        "memory": "9Mi", "hugePageSize": ""}})
    assert domain["resources"]["requests"]["memory"] == "9Mi"
    assert "memory" not in domain


def test_gpu_devices_present_only_when_requested():
    # deviceName is the catalog's per-type passthrough resource, not the
    # pod-mode nvidia.com/gpu (a vfio-bound card advertises 0 of those).
    with_gpu = _build(template_spec={"image": "x", "resources": {"gpu": 1}})
    assert with_gpu["spec"]["template"]["spec"]["domain"]["devices"]["gpus"] == \
        [{"name": "gpu0", "deviceName": "nvidia.com/AD102_GEFORCE_RTX_4090"}]

    without_gpu = _build(template_spec={"image": "x"})
    assert "gpus" not in without_gpu["spec"]["template"]["spec"]["domain"]["devices"]


def test_gpu_count_is_one_devices_entry_per_card():
    # KubeVirt has no count field on devices.gpus: it requests deviceName once
    # per entry. A single gpu0 for a count of 4 (what this used to emit) is a
    # session that quietly boots with one card.
    vm = _build(template_spec={"image": "x", "resources": {"gpu": "4"}})
    gpus = vm["spec"]["template"]["spec"]["domain"]["devices"]["gpus"]
    assert [g["name"] for g in gpus] == ["gpu0", "gpu1", "gpu2", "gpu3"]
    assert {g["deviceName"] for g in gpus} == {"nvidia.com/AD102_GEFORCE_RTX_4090"}


@pytest.mark.parametrize("zero", [0, "0", "", None])
def test_a_zero_gpu_count_attaches_nothing(zero):
    """Presence used to be the test (`'gpu' in resources`), so a template
    someone had turned the GPU off in still took a card — and on a mixed
    cluster _vm_gpu_device_name could not resolve one at all, failing the
    session outright. "No GPU" writes the absence rather than a zero, but a CR
    already carrying one has to mean the same thing."""
    vm = _build(template_spec={"image": "x", "resources": {"gpu": zero}})
    assert "gpus" not in vm["spec"]["template"]["spec"]["domain"]["devices"]


def test_a_zero_count_does_not_need_a_resolvable_gpu_type():
    # The mixed-cluster case: two VM-attachable types and no pin is a hard
    # PolicyError for a real request, and must simply not arise for none.
    mixed = [{"name": "a", "vmResource": "nvidia.com/A"},
             {"name": "b", "vmResource": "nvidia.com/B"}]
    with pytest.raises(PolicyError):
        _build(_catalog=mixed,
               template_spec={"image": "x", "resources": {"gpu": 1}})
    vm = _build(_catalog=mixed,
                template_spec={"image": "x", "resources": {"gpu": 0}})
    assert "gpus" not in vm["spec"]["template"]["spec"]["domain"]["devices"]


def test_gpu_type_pin_selects_its_own_resource_on_mixed_clusters():
    from whistler.config import GPU_NODE_LABEL
    catalog = [
        {"name": "NVIDIA-A100-SXM4-40GB", "count": 2,
         "vmResource": "nvidia.com/GA100_A100_SXM4_40GB"},
        {"name": "NVIDIA-GeForce-RTX-4090", "count": 1,
         "vmResource": "nvidia.com/AD102_GEFORCE_RTX_4090"},
    ]
    cm = _manager(catalog)
    vm, _ = cm._build_vm_spec(
        session_name="alice-desk", hostname="desk", username="alice",
        uid="uid-123", display_port=5900, instancetype=None,
        preemptible=False, home_pvc=None, user_details={},
        template_spec={"image": "x", "resources": {"gpu": 1},
                       "nodeSelector": {GPU_NODE_LABEL: "NVIDIA-A100-SXM4-40GB"}})
    assert vm["spec"]["template"]["spec"]["domain"]["devices"]["gpus"] == \
        [{"name": "gpu0", "deviceName": "nvidia.com/GA100_A100_SXM4_40GB"}]


def test_gpu_without_type_on_mixed_cluster_fails_closed():
    from whistler.config import PolicyError
    import pytest
    catalog = [
        {"name": "A", "count": 1, "vmResource": "nvidia.com/AAA"},
        {"name": "B", "count": 1, "vmResource": "nvidia.com/BBB"},
    ]
    with pytest.raises(PolicyError, match="must pin one"):
        _build(template_spec={"image": "x", "resources": {"gpu": 1}},
               _catalog=catalog)


def test_gpu_type_without_vm_resource_fails_closed():
    # The type exists (pod-mode node) but nothing KubeVirt-permitted backs it:
    # emitting a deviceName would create a VM the scheduler can never place.
    from whistler.config import PolicyError, GPU_NODE_LABEL
    import pytest
    catalog = [{"name": "NVIDIA-GeForce-RTX-4090", "count": 1, "vmResource": None}]
    with pytest.raises(PolicyError, match="not VM-attachable"):
        _build(template_spec={"image": "x", "resources": {"gpu": 1},
                              "nodeSelector": {GPU_NODE_LABEL: "NVIDIA-GeForce-RTX-4090"}},
               _catalog=catalog)


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
    # domain.memory conflicts with an instancetype the same way, and a merge
    # patch never removes what it doesn't mention.
    assert domain["memory"] is None


def test_patch_nulls_hugepages_when_a_template_opts_out():
    current = _build(template_spec={"image": "x",
                                    "resources": {"memory": "8Gi"}})["spec"]
    patch = _patch(current, template_spec={"image": "x", "resources": {
        "memory": "8Gi", "hugePageSize": ""}})
    assert patch["template"]["spec"]["domain"]["memory"] is None


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
