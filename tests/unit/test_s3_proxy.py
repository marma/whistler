"""Shared-dataset S3 proxies (pure manifest construction, no cluster):
KubeConfigManager._build_s3_proxy_manifests / _build_s3_proxy_network_policy.

Sessions never talk to the real S3 server — Whistler runs a proxy per
(volume, mode) so the zone egress rule names an address Whistler assigned,
and so the bucket credential never enters a guest whose user has root. See
design/storage.md; the guest side is in tests/unit/test_cloud_init.py.
"""
from whistler.config import KubeConfigManager, USER_NS_LABEL


DEFINITION = {
    "name": "refdata",
    "type": "s3",
    "bucket": "reference-data",
    "endpoint": "https://s3.example.org",
    "credentialsSecret": "s3-refdata-creds",
}


def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.namespace = "whistler"
    return cm


def _build(mode="ro", definition=None, **kw):
    args = dict(volume="refdata", mode=mode,
                definition=definition or DEFINITION,
                image="rclone/rclone:1.2.3",
                auth_secret_name="whistler-s3-refdata-ro-auth")
    args.update(kw)
    return _manager()._build_s3_proxy_manifests(**args)


def _args(mode="ro", **kw):
    dep, _svc = _build(mode, **kw)
    return dep["spec"]["template"]["spec"]["containers"][0]["args"]


def _env(mode="ro", **kw):
    dep, _svc = _build(mode, **kw)
    return {e["name"]: e for e in
            dep["spec"]["template"]["spec"]["containers"][0]["env"]}


def test_one_proxy_per_volume_and_mode():
    # rclone's --read-only is server-wide, not per key, so ro and rw cannot
    # share a process. That separation is what makes `mode: ro` a boundary on
    # a VM, where the guest is root and a client-side mount flag is not.
    cm = _manager()
    assert cm._s3_proxy_name("refdata", "ro") == "whistler-s3-refdata-ro"
    assert cm._s3_proxy_name("refdata", "rw") == "whistler-s3-refdata-rw"
    assert "--read-only" in _args("ro")
    assert "--read-only" not in _args("rw")


def test_bucket_and_optional_prefix_become_the_remote():
    assert _args("ro")[-1] == ":s3:reference-data"
    with_prefix = dict(DEFINITION, prefix="/subset/")
    assert _args("ro", definition=with_prefix)[-1] == \
        ":s3:reference-data/subset"


def test_bucket_credential_comes_from_a_secret_and_only_lives_here():
    # The whole point of the proxy: the real credential is mounted into it and
    # nowhere else, so a root guest cannot exfiltrate what it never received.
    env = _env("ro")
    assert env["RCLONE_S3_ACCESS_KEY_ID"]["valueFrom"]["secretKeyRef"] == {
        "name": "s3-refdata-creds", "key": "accessKeyId"}
    assert env["RCLONE_S3_SECRET_ACCESS_KEY"]["valueFrom"]["secretKeyRef"] == {
        "name": "s3-refdata-creds", "key": "secretAccessKey"}
    assert env["RCLONE_S3_ENDPOINT"]["value"] == "https://s3.example.org"
    # Never inline in argv or env value — argv is world-readable in /proc.
    flat = str(_build("ro"))
    assert "secretAccessKey" in flat  # the reference
    for arg in _args("ro"):
        assert "AKIA" not in arg


def test_client_key_is_referenced_not_inlined():
    env = _env("ro")
    assert env["WHISTLER_S3_AUTH_KEY"]["valueFrom"]["secretKeyRef"] == {
        "name": "whistler-s3-refdata-ro-auth", "key": "authKey"}
    # argv carries the env reference, which the container runtime expands.
    assert "$(WHISTLER_S3_AUTH_KEY)" in _args("ro")


def test_proxy_runs_unprivileged():
    dep, _svc = _build()
    sc = dep["spec"]["template"]["spec"]["containers"][0]["securityContext"]
    assert sc["runAsNonRoot"] is True
    assert sc["allowPrivilegeEscalation"] is False
    assert sc["readOnlyRootFilesystem"] is True
    assert sc["capabilities"]["drop"] == ["ALL"]


def test_service_is_cluster_internal():
    # A publicly reachable endpoint would have no zone story at all: a leaked
    # key would work from anywhere, so reach must remain the other half.
    _dep, svc = _build()
    assert svc["spec"]["type"] == "ClusterIP"
    assert svc["spec"]["ports"] == [
        {"name": "s3", "port": 8080, "targetPort": 8080}]


def test_host_is_the_proxy_not_the_real_server():
    host = _manager()._s3_proxy_host("refdata", "ro")
    assert host == "whistler-s3-refdata-ro.whistler.svc.cluster.local"
    assert "s3.example.org" not in host


# --- fencing --------------------------------------------------------------- #

def test_only_granted_users_may_reach_a_proxy():
    policy = _manager()._build_s3_proxy_network_policy(
        "refdata", "ro", ["bob", "alice"])
    assert policy["spec"]["policyTypes"] == ["Ingress"]
    (rule,) = policy["spec"]["ingress"]
    (src,) = rule["from"]
    (expr,) = src["namespaceSelector"]["matchExpressions"]
    assert expr["key"] == USER_NS_LABEL
    assert expr["operator"] == "In"
    # Sorted so an unchanged grant set doesn't churn the policy on reconcile.
    assert expr["values"] == ["alice", "bob"]
    assert rule["ports"] == [{"port": 8080, "protocol": "TCP"}]


def test_a_dataset_nobody_is_granted_denies_everyone():
    # Fail closed. An empty ingress list is NetworkPolicy for "deny all", so
    # an ungranted dataset is unreachable rather than open.
    policy = _manager()._build_s3_proxy_network_policy("refdata", "ro", [])
    assert policy["spec"]["ingress"] == []


def test_policy_selects_its_own_proxy_only():
    policy = _manager()._build_s3_proxy_network_policy(
        "refdata", "ro", ["alice"])
    assert policy["spec"]["podSelector"]["matchLabels"] == {
        "app": "whistler-s3-proxy", "volume": "refdata", "mode": "ro"}


# --- catalog --------------------------------------------------------------- #

def test_only_type_s3_entries_are_datasets():
    volumes = [
        {"name": "refdata", "type": "s3", "bucket": "b"},
        {"name": "scratch", "persistentVolumeClaim": {"claimName": "p"}},
        "not-a-dict",
    ]
    assert list(KubeConfigManager.s3_volume_definitions(volumes)) == ["refdata"]
    assert KubeConfigManager.s3_volume_definitions([]) == {}
    assert KubeConfigManager.s3_volume_definitions(None) == {}
