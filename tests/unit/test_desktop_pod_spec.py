"""Desktop pod manifest construction (KubeConfigManager._build_pod_spec, mode=desktop)."""
from whistler.config import KubeConfigManager


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)  # skip __init__ (no cluster)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.kata_runtime_class = "kata"
    cm.streamer_image = "ghcr.io/example/streamer:test"
    cm.zones = {"default": {}}
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


def test_display_port_wired_as_named_port_on_sidecar():
    pod = _build(display_port=5900)
    sidecar = pod["spec"]["initContainers"][0]
    assert sidecar["ports"] == [{"containerPort": 5900, "name": "display"}]
    assert "ports" not in pod["spec"]["containers"][0]


def test_no_command_override():
    """The workload image's entrypoint starts the DE/app session; unlike the
    SSH pod we must NOT override the entrypoint with `sleep 3600`."""
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


# --------------------------------------------------------------------- #
# The streamer sidecar (every desktop pod): the workload container is    #
# display-unaware; a native sidecar (initContainer, restartPolicy=Always) #
# owns Xvfb+Pulse+Selkies and shares the X/Pulse sockets over emptyDirs.  #
# --------------------------------------------------------------------- #

def _build_sidecar(**overrides):
    template_spec = overrides.pop("template_spec", {"image": "xfce-plain:latest"})
    return _build(template_spec=template_spec, display_port=8082, **overrides)


def test_sidecar_is_native_init_sidecar_with_default_image():
    pod = _build_sidecar()
    sidecars = pod["spec"]["initContainers"]
    assert len(sidecars) == 1
    sc = sidecars[0]
    assert sc["name"] == "streamer"
    assert sc["image"] == "ghcr.io/example/streamer:test"
    # restartPolicy=Always is what makes an initContainer a sidecar — without
    # it the pod would wait forever for the streamer to "complete".
    assert sc["restartPolicy"] == "Always"


def test_sidecar_template_streamer_image_overrides_default():
    pod = _build_sidecar(template_spec={"image": "xfce-plain:latest",
                                        "streamerImage": "custom/streamer:v9"})
    assert pod["spec"]["initContainers"][0]["image"] == "custom/streamer:v9"


def test_sidecar_owns_display_port_and_probe_gates_workload():
    pod = _build_sidecar()
    sc = pod["spec"]["initContainers"][0]
    main = pod["spec"]["containers"][0]
    # The display port lives on the sidecar (Selkies runs there), not "main"…
    assert sc["ports"] == [{"containerPort": 8082, "name": "display"}]
    assert "ports" not in main
    assert {"name": "SELKIES_PORT", "value": "8082"} in sc["env"]
    # …and the startupProbe on it is what guarantees the workload container
    # starts against a live display.
    assert sc["startupProbe"]["tcpSocket"]["port"] == 8082


def test_sidecar_shares_x11_and_pulse_sockets_with_workload():
    pod = _build_sidecar()
    vols = {v["name"]: v for v in pod["spec"]["volumes"]}
    assert vols["x11"] == {"name": "x11", "emptyDir": {}}
    assert vols["pulse"] == {"name": "pulse", "emptyDir": {}}
    for c in (pod["spec"]["initContainers"][0], pod["spec"]["containers"][0]):
        mounts = {m["name"]: m["mountPath"] for m in c["volumeMounts"]}
        assert mounts["x11"] == "/tmp/.X11-unix"
        assert mounts["pulse"] == "/tmp/pulse"


def test_sidecar_streamer_env_appended_after_port():
    """streamerEnv carries workload-dependent knobs (e.g. GNOME Shell needs
    SELKIES_H264_STREAMING_MODE=true or static windows render black)."""
    pod = _build_sidecar(template_spec={
        "image": "gnome-plain:latest",
        "streamerEnv": {"SELKIES_H264_STREAMING_MODE": "true",
                        "SELKIES_RESOLUTION": "1920x1080"},
    })
    env = pod["spec"]["initContainers"][0]["env"]
    assert {"name": "SELKIES_PORT", "value": "8082"} in env
    assert {"name": "SELKIES_H264_STREAMING_MODE", "value": "true"} in env
    assert {"name": "SELKIES_RESOLUTION", "value": "1920x1080"} in env
    # …and it configures the sidecar only, never the workload.
    assert {"name": "SELKIES_H264_STREAMING_MODE", "value": "true"} \
        not in pod["spec"]["containers"][0]["env"]


def test_sidecar_workload_env_is_the_entire_display_contract():
    main = _build_sidecar()["spec"]["containers"][0]
    assert {"name": "DISPLAY", "value": ":0"} in main["env"]
    assert {"name": "PULSE_SERVER", "value": "unix:/tmp/pulse/native"} in main["env"]
    # No command override: the workload image's entrypoint starts the session.
    assert "command" not in main


def test_sidecar_workload_keeps_home_pvc_mount():
    main = _build_sidecar()["spec"]["containers"][0]
    assert {"name": "data", "mountPath": "/userdata"} in main["volumeMounts"]


def test_ssh_mode_gets_no_sidecar():
    pod = _build(mode="ssh", display_port=None,
                 template_spec={"image": "ubuntu:latest"})
    assert "initContainers" not in pod["spec"]
    assert pod["spec"]["containers"][0]["command"] == ["sleep", "3600"]


def test_desktop_without_display_port_falls_back_to_plain_pod():
    """Defensive dead end: the resolver always supplies displayPort for
    desktop templates, but a missing one must not produce a broken sidecar."""
    pod = _build(template_spec={"image": "xfce-plain:latest"}, display_port=None)
    assert "initContainers" not in pod["spec"]


# The websockets viewer (Selkies 2.x) needs no TURN/coturn, so desktop pods get
# no injected TURN env regardless of environment — the workload env stays the
# two-var display contract.
def test_websockets_viewer_injects_no_turn_env(monkeypatch):
    monkeypatch.setenv("TURN_HOST", "turn.example")
    container = _build(
        template_spec={"image": "xfce-plain:latest", "viewer": "websockets"},
        display_port=8082,
    )["spec"]["containers"][0]
    assert sorted(e["name"] for e in container["env"]) == ["DISPLAY", "PULSE_SERVER"]
