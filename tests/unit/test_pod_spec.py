"""Pod manifest construction (KubeConfigManager._build_pod_spec)."""
from whistler.config import KubeConfigManager


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.zones = {"default": {}}
    return cm


def _build(**overrides):
    cm = _manager()
    args = dict(
        full_name="alice-box",
        hostname="box",
        username="alice",
        uid="uid-123",
        mode="ssh",
        runtime="container",
        template_spec={"image": "ubuntu:22.04"},
        pvc_name="whistler-data-alice",
        available_volumes={},
        user_details=None,
        preemptible=False,
    )
    args.update(overrides)
    return cm._build_pod_spec(**args)


def test_basic_metadata_and_owner_reference():
    pod = _build()
    meta = pod["metadata"]
    assert meta["name"] == "alice-box"
    # ssh pods carry app=whistler-instance, and BOTH instance + session labels
    # (the dual-label scheme keeps the SSH watch and desktop Service selecting).
    assert meta["labels"]["app"] == "whistler-instance"
    assert meta["labels"]["instance"] == "alice-box"
    assert meta["labels"]["session"] == "alice-box"
    assert meta["labels"]["user"] == "alice"

    owner = meta["ownerReferences"][0]
    assert owner["kind"] == "Session"
    assert owner["name"] == "alice-box"
    assert owner["uid"] == "uid-123"
    assert owner["controller"] is True
    assert owner["apiVersion"] == "whistler.martinmalmsten.net/v1"


def test_ssh_pod_has_no_display_port_and_kata_sets_runtime_class():
    pod = _build()
    assert "ports" not in pod["spec"]["containers"][0]
    assert "runtimeClassName" not in pod["spec"]

    cm = _manager()
    cm.kata_runtime_class = "kata"
    kata = cm._build_pod_spec(
        full_name="alice-box", hostname="box", username="alice", uid="u",
        mode="ssh", runtime="kata", template_spec={"image": "ubuntu:22.04"},
        pvc_name="pvc", available_volumes={}, user_details=None, preemptible=False,
    )
    assert kata["spec"]["runtimeClassName"] == "kata"
    # ssh kata may still run privileged (mode-agnostic securityContext).
    assert kata["spec"]["containers"][0]["command"] == ["sleep", "3600"]


def test_container_image_and_default_command():
    pod = _build(template_spec={"image": "ubuntu:22.04"})
    container = pod["spec"]["containers"][0]
    assert container["image"] == "ubuntu:22.04"
    assert container["command"] == ["sleep", "3600"]


def test_default_image_when_unspecified():
    pod = _build(template_spec={})
    assert pod["spec"]["containers"][0]["image"] == "ubuntu:latest"


def test_resources_map_cpu_memory_and_gpu():
    pod = _build(template_spec={"resources": {"cpu": "2", "memory": "4Gi", "gpu": 1}})
    res = pod["spec"]["containers"][0]["resources"]
    assert res["requests"] == {"cpu": "2", "memory": "4Gi"}
    assert res["limits"] == {"cpu": "2", "memory": "4Gi", "nvidia.com/gpu": 1}


def test_home_volume_uses_pvc_and_personal_mount_path():
    pod = _build(
        template_spec={"personalMountPath": "/home/alice"},
        pvc_name="whistler-data-alice",
    )
    spec = pod["spec"]
    data_vol = next(v for v in spec["volumes"] if v["name"] == "data")
    assert data_vol["persistentVolumeClaim"]["claimName"] == "whistler-data-alice"

    data_mount = next(m for m in spec["containers"][0]["volumeMounts"] if m["name"] == "data")
    assert data_mount["mountPath"] == "/home/alice"


def test_default_personal_mount_path():
    pod = _build(template_spec={})
    data_mount = next(m for m in pod["spec"]["containers"][0]["volumeMounts"] if m["name"] == "data")
    assert data_mount["mountPath"] == "/userdata"


def test_requested_volume_is_mounted_with_subpath():
    available = {"dataset": {"name": "dataset", "persistentVolumeClaim": {"claimName": "big-pvc"}, "subPath": "train"}}
    pod = _build(
        template_spec={"volumes": {"dataset": "/data/train"}},
        available_volumes=available,
    )
    spec = pod["spec"]

    # Volume added without the subPath key (subPath belongs on the mount).
    vol = next(v for v in spec["volumes"] if v["name"] == "dataset")
    assert "subPath" not in vol
    assert vol["persistentVolumeClaim"]["claimName"] == "big-pvc"

    mount = next(m for m in spec["containers"][0]["volumeMounts"] if m["name"] == "dataset")
    assert mount["mountPath"] == "/data/train"
    assert mount["subPath"] == "train"

    # The source definition must not be mutated by the build.
    assert available["dataset"]["subPath"] == "train"


def test_requested_volume_not_in_catalog_is_ignored():
    pod = _build(template_spec={"volumes": {"ghost": "/mnt/ghost"}}, available_volumes={})
    names = {v["name"] for v in pod["spec"]["volumes"]}
    assert names == {"data"}


def test_security_context_applied_from_user_details():
    sc = {"runAsUser": 1001, "fsGroup": 1001}
    pod = _build(user_details={"name": "alice", "securityContext": sc})
    assert pod["spec"]["securityContext"] == sc


def test_no_security_context_when_user_details_missing():
    pod = _build(user_details=None)
    assert "securityContext" not in pod["spec"]


def test_preemptible_sets_priority_class():
    assert _build(preemptible=True)["spec"]["priorityClassName"] == "whistler-preemptible"
    assert "priorityClassName" not in _build(preemptible=False)["spec"]


# --- GPU RuntimeClass ------------------------------------------------------ #
# The device plugin bind-mounts /dev/nvidia* regardless of runtime, but only
# nvidia-container-runtime's hook injects the driver userspace (nvidia-smi,
# libcuda.so, ...) — so a GPU request needs its own RuntimeClass, separate
# from (and lower-priority than) kata's.

def test_gpu_request_sets_nvidia_runtime_class_by_default():
    pod = _build(template_spec={"resources": {"gpu": 1}})
    assert pod["spec"]["runtimeClassName"] == "nvidia"


def test_gpu_runtime_class_configurable():
    cm = _manager()
    cm.gpu_runtime_class = "nvidia-experimental"
    pod = cm._build_pod_spec(
        full_name="alice-box", hostname="box", username="alice", uid="u",
        mode="ssh", runtime="container", template_spec={"resources": {"gpu": 1}},
        pvc_name="pvc", available_volumes={}, user_details=None, preemptible=False,
    )
    assert pod["spec"]["runtimeClassName"] == "nvidia-experimental"


def test_gpu_runtime_class_empty_disables_it():
    cm = _manager()
    cm.gpu_runtime_class = ""
    pod = cm._build_pod_spec(
        full_name="alice-box", hostname="box", username="alice", uid="u",
        mode="ssh", runtime="container", template_spec={"resources": {"gpu": 1}},
        pvc_name="pvc", available_volumes={}, user_details=None, preemptible=False,
    )
    assert "runtimeClassName" not in pod["spec"]


def test_kata_runtime_class_wins_over_gpu():
    cm = _manager()
    cm.kata_runtime_class = "kata"
    pod = cm._build_pod_spec(
        full_name="alice-box", hostname="box", username="alice", uid="u",
        mode="ssh", runtime="kata", template_spec={"resources": {"gpu": 1}},
        pvc_name="pvc", available_volumes={}, user_details=None, preemptible=False,
    )
    assert pod["spec"]["runtimeClassName"] == "kata"


def test_no_gpu_requested_leaves_runtime_class_unset():
    assert "runtimeClassName" not in _build(template_spec={"image": "ubuntu:22.04"})["spec"]


# --- moved from the retired desktop-pod suite ----------------------------- #
# These assert container-pod behaviour that was only covered in desktop mode
# before container desktops were retired (design/container_workloads.md).

def test_requested_volume_with_subpath_does_not_mutate_source():
    available = {"shared": {"name": "shared",
                            "persistentVolumeClaim": {"claimName": "shared-pvc"},
                            "subPath": "team"}}
    pod = _build(template_spec={"image": "ubuntu:latest",
                                "volumes": {"shared": "/data/shared"}},
                 available_volumes=available)
    # The source definition still carries subPath: it was copied, not mutated.
    assert available["shared"]["subPath"] == "team"
    mount = next(m for m in pod["spec"]["containers"][0]["volumeMounts"]
                 if m["name"] == "shared")
    assert mount == {"name": "shared", "mountPath": "/data/shared", "subPath": "team"}
    vol = next(v for v in pod["spec"]["volumes"] if v["name"] == "shared")
    assert "subPath" not in vol


def test_fuse_flag_runs_container_privileged():
    pod = _build(template_spec={"image": "ubuntu:latest", "fuse": True})
    assert pod["spec"]["containers"][0]["securityContext"]["privileged"] is True


def test_no_fuse_flag_leaves_container_unprivileged():
    container = _build(template_spec={"image": "ubuntu:latest"})["spec"]["containers"][0]
    assert "privileged" not in container.get("securityContext", {})


def test_data_named_requested_volume_is_skipped():
    available = {"data": {"name": "data",
                          "persistentVolumeClaim": {"claimName": "other"}}}
    pod = _build(template_spec={"image": "ubuntu:latest",
                                "volumes": {"data": "/elsewhere"}},
                 available_volumes=available)
    data_vols = [v for v in pod["spec"]["volumes"] if v["name"] == "data"]
    assert len(data_vols) == 1  # only the home PVC; the requested "data" was skipped
    assert data_vols[0]["persistentVolumeClaim"]["claimName"] == "whistler-data-alice"
