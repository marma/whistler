"""Per-user SMB storage gateway manifest construction (pure, no cluster):
KubeConfigManager._build_gateway_manifests / _build_gateway_network_policy.

The gateway replaces virtiofs for KubeVirt VM homes (kubevirt#13028): it
mounts the user's home PVC via CSI and exports SMB3 with server-side
identity; the e2e path is exercised by tests/integration/test_vm.py.
"""
from whistler.config import KubeConfigManager


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)
    return cm


def _build(**overrides):
    args = dict(
        username="alice",
        uid=1001,
        pvc_name="whistler-data-alice",
        image="ghcr.io/example/storage-gateway:dev",
        node_selector={"storage": "fast"},
        resources={"limits": {"memory": "256Mi"}},
    )
    args.update(overrides)
    return _manager()._build_gateway_manifests(**args)


def test_deployment_named_and_labelled_per_user():
    deployment, _ = _build()
    assert deployment["metadata"]["name"] == "whistler-storage-alice"
    labels = {"app": "whistler-storage-gateway", "user": "alice"}
    assert deployment["metadata"]["labels"] == labels
    assert deployment["spec"]["selector"]["matchLabels"] == labels
    assert deployment["spec"]["template"]["metadata"]["labels"] == labels


def test_no_owner_reference_gateway_is_per_user_not_per_session():
    # It lives across sessions and dies with the user namespace.
    deployment, service = _build()
    assert "ownerReferences" not in deployment["metadata"]
    assert "ownerReferences" not in service["metadata"]


def test_recreate_strategy_for_rwo_pvc():
    # A RollingUpdate would deadlock on an RWO home PVC.
    deployment, _ = _build()
    assert deployment["spec"]["strategy"] == {"type": "Recreate"}
    assert deployment["spec"]["replicas"] == 1


def test_pod_mounts_pvc_and_secret_and_carries_identity():
    deployment, _ = _build()
    pod = deployment["spec"]["template"]["spec"]
    (container,) = pod["containers"]

    env = {e["name"]: e["value"] for e in container["env"]}
    assert env == {"SMB_USER": "alice", "SMB_UID": "1001"}
    assert container["image"] == "ghcr.io/example/storage-gateway:dev"

    mounts = {m["name"]: m for m in container["volumeMounts"]}
    assert mounts["home"]["mountPath"] == "/shares/home"
    assert mounts["smb-secret"]["mountPath"] == "/etc/whistler-smb"
    assert mounts["smb-secret"]["readOnly"] is True

    volumes = {v["name"]: v for v in pod["volumes"]}
    assert volumes["home"]["persistentVolumeClaim"]["claimName"] == "whistler-data-alice"
    assert volumes["smb-secret"]["secret"]["secretName"] == "whistler-smb-alice"


def test_placement_and_resources_from_values():
    deployment, _ = _build()
    pod = deployment["spec"]["template"]["spec"]
    assert pod["nodeSelector"] == {"storage": "fast"}
    assert pod["containers"][0]["resources"] == {"limits": {"memory": "256Mi"}}

    default_deploy, _ = _build(node_selector=None, resources=None)
    assert default_deploy["spec"]["template"]["spec"]["nodeSelector"] == {}
    assert default_deploy["spec"]["template"]["spec"]["containers"][0]["resources"] == {}


def test_readiness_gates_on_smb_port():
    deployment, _ = _build()
    probe = deployment["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]
    assert probe["tcpSocket"] == {"port": 445}


def test_service_exposes_smb_on_cluster_ip():
    _, service = _build()
    assert service["metadata"]["name"] == "whistler-storage-alice"
    assert service["spec"]["type"] == "ClusterIP"
    assert service["spec"]["selector"] == {
        "app": "whistler-storage-gateway", "user": "alice"}
    (port,) = service["spec"]["ports"]
    assert port["port"] == 445 and port["targetPort"] == 445


def test_gateway_host_matches_service_dns():
    cm = _manager()
    assert cm._gateway_host("alice", "whistler-user-alice") == \
        "whistler-storage-alice.whistler-user-alice.svc.cluster.local"


def test_network_policy_fences_ingress_to_session_pods_on_445():
    policy = _manager()._build_gateway_network_policy("alice")
    spec = policy["spec"]
    assert spec["podSelector"] == {
        "matchLabels": {"app": "whistler-storage-gateway"}}
    assert spec["policyTypes"] == ["Ingress"]
    (rule,) = spec["ingress"]
    # virt-launcher pods inherit app: whistler-desktop from the VM template.
    assert rule["from"] == [
        {"podSelector": {"matchLabels": {"app": "whistler-desktop"}}}]
    assert rule["ports"] == [{"port": 445, "protocol": "TCP"}]
