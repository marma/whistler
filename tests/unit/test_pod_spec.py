"""Pod manifest construction (KubeConfigManager._build_pod_spec)."""
from whistler.config import KubeConfigManager


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    return cm


def _build(**overrides):
    cm = _manager()
    args = dict(
        full_instance_name="alice-box",
        hostname="box",
        username="alice",
        uid="uid-123",
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
    assert meta["labels"] == {"app": "whistler-instance", "instance": "alice-box", "user": "alice"}

    owner = meta["ownerReferences"][0]
    assert owner["name"] == "alice-box"
    assert owner["uid"] == "uid-123"
    assert owner["controller"] is True
    assert owner["apiVersion"] == "whistler.martinmalmsten.net/v1"


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
