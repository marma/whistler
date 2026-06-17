"""Desktop pod manifest construction (KubeConfigManager._build_pod_spec, mode=desktop)."""
from whistler.config import KubeConfigManager


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.kata_runtime_class = "kata"
    return cm


def _build(**overrides):
    cm = _manager()
    args = dict(
        full_name="alice-desk",
        hostname="desk",
        username="alice",
        uid="uid-123",
        mode="desktop",
        runtime="container",
        template_spec={"image": "vnc:latest"},
        pvc_name="whistler-data-alice",
        available_volumes={},
        user_details=None,
        display_port=5901,
        preemptible=False,
    )
    args.update(overrides)
    return cm._build_pod_spec(**args)


def test_metadata_labels_and_owner_reference():
    pod = _build()
    meta = pod["metadata"]
    assert meta["name"] == "alice-desk"
    assert meta["labels"]["app"] == "whistler-desktop"
    assert meta["labels"]["session"] == "alice-desk"
    assert meta["labels"]["instance"] == "alice-desk"
    assert meta["labels"]["user"] == "alice"

    owner = meta["ownerReferences"][0]
    assert owner["kind"] == "Session"
    assert owner["name"] == "alice-desk"
    assert owner["uid"] == "uid-123"
    assert owner["controller"] is True
    assert owner["apiVersion"] == "whistler.martinmalmsten.net/v1"


def test_kata_runtime_sets_runtime_class():
    pod = _build(runtime="kata")
    assert pod["spec"]["runtimeClassName"] == "kata"
    pod = _build(runtime="container")
    assert "runtimeClassName" not in pod["spec"]


def test_display_port_wired_as_named_container_port():
    pod = _build(display_port=5900)
    container = pod["spec"]["containers"][0]
    assert container["ports"] == [{"containerPort": 5900, "name": "display"}]


def test_no_command_override():
    """Desktop images self-start their display server; unlike the SSH pod we
    must NOT override the entrypoint with `sleep 3600`."""
    container = _build()["spec"]["containers"][0]
    assert "command" not in container


def test_home_pvc_mounted_at_personal_mount_path():
    pod = _build(template_spec={"image": "vnc:latest", "personalMountPath": "/home/user"})
    container = pod["spec"]["containers"][0]
    assert {"name": "data", "mountPath": "/home/user"} in container["volumeMounts"]
    assert {"name": "data", "persistentVolumeClaim": {"claimName": "whistler-data-alice"}} \
        in pod["spec"]["volumes"]


def test_default_personal_mount_path():
    pod = _build()
    container = pod["spec"]["containers"][0]
    assert {"name": "data", "mountPath": "/userdata"} in container["volumeMounts"]


def test_resources_map_cpu_memory_and_gpu():
    pod = _build(template_spec={"image": "vnc:latest",
                                "resources": {"cpu": "2", "memory": "4Gi", "gpu": 1}})
    res = pod["spec"]["containers"][0]["resources"]
    assert res["requests"] == {"cpu": "2", "memory": "4Gi"}
    assert res["limits"] == {"cpu": "2", "memory": "4Gi", "nvidia.com/gpu": 1}


def test_security_context_applied_when_present():
    sc = {"runAsUser": 1000, "fsGroup": 1000}
    pod = _build(user_details={"name": "alice", "securityContext": sc})
    assert pod["spec"]["securityContext"] == sc


def test_security_context_absent_without_user_details():
    pod = _build(user_details=None)
    assert "securityContext" not in pod["spec"]


def test_preemptible_sets_priority_class():
    pod = _build(preemptible=True)
    assert pod["spec"]["priorityClassName"] == "whistler-preemptible"
    pod = _build(preemptible=False)
    assert "priorityClassName" not in pod["spec"]


def test_requested_volume_with_subpath_does_not_mutate_source():
    available = {"shared": {"name": "shared", "persistentVolumeClaim": {"claimName": "shared-pvc"},
                            "subPath": "team"}}
    pod = _build(template_spec={"image": "vnc:latest", "volumes": {"shared": "/data/shared"}},
                 available_volumes=available)
    # source definition still carries subPath (was copied, not mutated)
    assert available["shared"]["subPath"] == "team"
    mount = next(m for m in pod["spec"]["containers"][0]["volumeMounts"] if m["name"] == "shared")
    assert mount == {"name": "shared", "mountPath": "/data/shared", "subPath": "team"}
    vol = next(v for v in pod["spec"]["volumes"] if v["name"] == "shared")
    assert "subPath" not in vol


def test_fuse_flag_runs_container_privileged():
    pod = _build(template_spec={"image": "gnome-grd:latest", "fuse": True})
    container = pod["spec"]["containers"][0]
    assert container["securityContext"]["privileged"] is True


def test_no_fuse_flag_leaves_container_unprivileged():
    pod = _build(template_spec={"image": "vnc:latest"})
    container = pod["spec"]["containers"][0]
    # No securityContext at all, or at least not privileged.
    assert "privileged" not in container.get("securityContext", {})


def test_data_named_requested_volume_is_skipped():
    available = {"data": {"name": "data", "persistentVolumeClaim": {"claimName": "other"}}}
    pod = _build(template_spec={"image": "vnc:latest", "volumes": {"data": "/elsewhere"}},
                 available_volumes=available)
    data_vols = [v for v in pod["spec"]["volumes"] if v["name"] == "data"]
    assert len(data_vols) == 1  # only the home PVC, the requested "data" was skipped
    assert data_vols[0]["persistentVolumeClaim"]["claimName"] == "whistler-data-alice"
