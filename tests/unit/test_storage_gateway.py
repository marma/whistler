"""Per-user NFS storage gateway manifest construction (pure, no cluster):
KubeConfigManager._build_gateway_manifests / _build_gateway_network_policy.

The gateway replaces virtiofs for KubeVirt VM homes (kubevirt#13028): it
mounts the user's home PVC via CSI and exports NFSv4.2 with server-side
identity; the e2e path is exercised by tests/integration/test_vm.py.
"""
from kubernetes.client.rest import ApiException

from whistler.config import KubeConfigManager


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)
    return cm


def _build(**overrides):
    args = dict(
        username="alice",
        uid=1001,
        gid=2001,
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


def test_pod_mounts_pvc_and_carries_identity():
    deployment, _ = _build()
    pod = deployment["spec"]["template"]["spec"]
    (container,) = pod["containers"]

    # The export's anon identity: every squashed request acts as these ids,
    # so they are the whole server-side identity model (SMB's `force user`).
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env == {"SHARE_USER": "alice", "SHARE_UID": "1001",
                   "SHARE_GID": "2001"}
    assert container["image"] == "ghcr.io/example/storage-gateway:dev"

    (mount,) = container["volumeMounts"]
    assert mount["name"] == "home" and mount["mountPath"] == "/shares/home"

    # NFS AUTH_SYS has no per-share credential, so unlike the SMB gateway
    # there is no Secret to mount.
    (volume,) = pod["volumes"]
    assert volume["persistentVolumeClaim"]["claimName"] == "whistler-data-alice"


def test_capabilities_instead_of_privileged():
    # ganesha's VFS FSAL opens by handle (DAC_READ_SEARCH) and raises its own
    # fd limit (SYS_RESOURCE). The kernel nfsd would have needed the whole pod
    # privileged — and is a kernel-global singleton, so not one per user.
    (container,) = _build()[0]["spec"]["template"]["spec"]["containers"]
    sc = container["securityContext"]
    assert sc["capabilities"]["add"] == ["DAC_READ_SEARCH", "SYS_RESOURCE"]
    assert "privileged" not in sc


def test_placement_and_resources_from_values():
    deployment, _ = _build()
    pod = deployment["spec"]["template"]["spec"]
    assert pod["nodeSelector"] == {"storage": "fast"}
    assert pod["containers"][0]["resources"] == {"limits": {"memory": "256Mi"}}

    default_deploy, _ = _build(node_selector=None, resources=None)
    assert default_deploy["spec"]["template"]["spec"]["nodeSelector"] == {}
    assert default_deploy["spec"]["template"]["spec"]["containers"][0]["resources"] == {}


def test_readiness_asserts_a_real_export_not_just_the_port():
    # ganesha binds 2049 and logs "NFS SERVER INITIALIZED" even when every
    # export failed to build, so a tcpSocket probe reports a gateway that
    # serves nothing as healthy while guest mounts get ENOENT. That is how the
    # SMB->NFS move shipped broken; the probe must ask ganesha what it exports.
    (container,) = _build()[0]["spec"]["template"]["spec"]["containers"]
    ready = container["readinessProbe"]
    assert ready["exec"] == {"command": ["/usr/local/bin/gateway-ready"]}
    assert "tcpSocket" not in ready

    # A gateway that never exports is broken, not slow: the startup probe
    # restarts it so the failure shows up as a CrashLoop rather than a pod
    # sitting NotReady indefinitely.
    startup = container["startupProbe"]
    assert startup["exec"] == {"command": ["/usr/local/bin/gateway-ready"]}
    assert startup["periodSeconds"] * startup["failureThreshold"] >= 30


def test_service_exposes_nfs_on_cluster_ip():
    _, service = _build()
    assert service["metadata"]["name"] == "whistler-storage-alice"
    assert service["spec"]["type"] == "ClusterIP"
    assert service["spec"]["selector"] == {
        "app": "whistler-storage-gateway", "user": "alice"}
    # One port: the export is NFSv4-only, so no rpcbind/NLM/statd.
    (port,) = service["spec"]["ports"]
    assert port["port"] == 2049 and port["targetPort"] == 2049


def test_gateway_host_matches_service_dns():
    cm = _manager()
    assert cm._gateway_host("alice", "whistler-user-alice") == \
        "whistler-storage-alice.whistler-user-alice.svc.cluster.local"


def test_network_policy_fences_ingress_to_session_pods_on_2049():
    # Load-bearing: AUTH_SYS has no per-share credential, so who can reach
    # the export is the entire access decision.
    policy = _manager()._build_gateway_network_policy("alice")
    spec = policy["spec"]
    assert spec["podSelector"] == {
        "matchLabels": {"app": "whistler-storage-gateway"}}
    assert spec["policyTypes"] == ["Ingress"]
    (rule,) = spec["ingress"]
    # virt-launcher pods inherit app: whistler-desktop from the VM template.
    assert rule["from"] == [
        {"podSelector": {"matchLabels": {"app": "whistler-desktop"}}}]
    assert rule["ports"] == [{"port": 2049, "protocol": "TCP"}]


# --- reconcile semantics (_ensure_object) --------------------------------- #
# The gateway is created once and then lives across sessions, so every
# ensure_storage_gateway call has to make an EXISTING object match the
# manifest. This used to be a strategic-merge patch, which merges list entries
# by key: the SMB->NFS rename therefore added a ganesha container beside the
# surviving samba one, and left the Service on 445 and the NetworkPolicy
# fencing 445 — so guests could not mount their home at all.


class _Recorder:
    """Stands in for the create/read/replace trio of a kubernetes API."""

    def __init__(self, exists=False, live=None, fail_replace=False):
        self.exists, self.live, self.fail_replace = exists, live, fail_replace
        self.created = self.replaced = None

    def create(self, ns, body):
        if self.exists:
            raise ApiException(status=409, reason="AlreadyExists")
        self.created = (ns, body)

    def read(self, name, ns):
        return self.live

    def replace(self, name, ns, body):
        if self.fail_replace:
            raise ApiException(status=422, reason="Invalid")
        self.replaced = (name, ns, body)


def _ensure(rec, body, **kw):
    return _manager()._ensure_object(
        "whistler-storage-alice", "whistler-user-alice", body,
        create=rec.create, read=rec.read, replace=rec.replace, **kw)


NEW_DEPLOY = {"kind": "Deployment", "metadata": {"name": "whistler-storage-alice"}}


def test_creates_when_absent():
    rec = _Recorder(exists=False)
    assert _ensure(rec, NEW_DEPLOY) is True
    assert rec.created[1] is NEW_DEPLOY
    assert rec.replaced is None


def test_replaces_rather_than_patches_when_present():
    # The regression: replace makes the manifest authoritative, so a removed
    # or renamed container/port/volume actually disappears.
    rec = _Recorder(exists=True)
    assert _ensure(rec, NEW_DEPLOY) is True
    assert rec.replaced[0] == "whistler-storage-alice"
    assert rec.replaced[2] is NEW_DEPLOY


def test_replace_failure_is_reported_not_swallowed():
    # It used to be logged as a warning and ignored, which is how a gateway
    # stayed stale indefinitely. False makes the operator retry.
    rec = _Recorder(exists=True, fail_replace=True)
    assert _ensure(rec, NEW_DEPLOY) is False


def test_create_failure_other_than_conflict_is_fatal():
    rec = _Recorder()

    def boom(ns, body):
        raise ApiException(status=403, reason="Forbidden")

    rec.create = boom
    assert _ensure(rec, NEW_DEPLOY) is False


def test_immutable_cluster_ip_carried_onto_a_replacement_service():
    # A Service replace that omits the API-server-assigned clusterIP is
    # rejected, so the gateway Service would never pick up its new port.
    class _Spec:
        cluster_ip = "10.43.0.7"

    class _Live:
        spec = _Spec()

    body = {"kind": "Service", "metadata": {"name": "whistler-storage-alice"},
            "spec": {"ports": [{"name": "nfs", "port": 2049}]}}
    rec = _Recorder(exists=True, live=_Live())
    assert _ensure(rec, body, preserve=KubeConfigManager._preserve_cluster_ip)
    assert rec.replaced[2]["spec"]["clusterIP"] == "10.43.0.7"


def test_preserve_tolerates_a_service_without_a_cluster_ip():
    class _Live:
        spec = None

    body = {"kind": "Service", "spec": {}}
    KubeConfigManager._preserve_cluster_ip(_Live(), body)
    assert "clusterIP" not in body["spec"]
