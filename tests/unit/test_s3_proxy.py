"""Shared-dataset S3 proxies (pure manifest construction, no cluster):
KubeConfigManager._build_s3_proxy_manifests / _build_s3_proxy_network_policy.

Sessions never talk to the real S3 server — Whistler runs a proxy per
(volume, mode) so the zone egress rule names an address Whistler assigned,
and so the bucket credential never enters a guest whose user has root. See
design/storage.md; the guest side is in tests/unit/test_cloud_init.py.
"""
from whistler.cloudinit import S3_PROXY_BUCKET
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
    assert _args("ro")[-1] == ':combine,upstreams="data=:s3:reference-data":'
    with_prefix = dict(DEFINITION, prefix="/subset/")
    assert _args("ro", definition=with_prefix)[-1] == \
        ':combine,upstreams="data=:s3:reference-data/subset":'


def test_backend_is_wrapped_so_top_level_files_stay_addressable():
    # Regression, measured 2026-08-17. `rclone serve s3 :s3:<bucket>` promotes
    # the served directory's SUBDIRECTORIES to buckets, so a file at the
    # dataset's top level has no bucket to live in and simply disappears — the
    # dataset mounts empty and nothing errors. The combine wrapper puts
    # exactly one directory at the served root, so the dataset hangs under it
    # verbatim. The guest mounts that same bucket name (cloudinit.py).
    remote = _args("ro")[-1]
    assert remote.startswith(":combine,")
    assert f'upstreams="{S3_PROXY_BUCKET}=' in remote
    # Serving the bare backend is exactly the bug.
    assert remote != ":s3:reference-data"


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


# --- who a proxy admits ----------------------------------------------------- #
#
# s3_proxy_users is what turns grants into the policy's `from` list, so it has
# to answer the same question the mount does. These run the REAL user/group
# resolution (only the API loaders are bypassed, by constructing via __new__
# and setting the catalogs directly, which both loaders tolerate) — a version
# of this calling a method that does not exist shipped once precisely because
# the list was only ever passed in by hand.

def _granted(users, groups=None):
    cm = _manager()
    cm.users = users
    cm.groups = groups or {}
    return cm


def test_proxy_admits_exactly_the_users_granted_that_mode():
    cm = _granted(
        {"alice": {"name": "alice"}, "bob": {"name": "bob"},
         "carol": {"name": "carol"}},
        {"proj": {"members": ["alice", "bob", "carol"],
                  "volumes": [{"name": "refdata", "mode": "ro",
                               "access": {"bob": "rw"}}]}},
    )
    # The group grants ro, except bob who is named rw. Each lands on the
    # proxy that enforces its own mode, and on no other.
    assert cm.s3_proxy_users("refdata", "ro") == ["alice", "carol"]
    assert cm.s3_proxy_users("refdata", "rw") == ["bob"]


def test_a_user_restricted_to_other_volumes_reaches_neither_proxy():
    cm = _granted(
        {"alice": {"name": "alice"}, "mallory": {"name": "mallory"}},
        {"proj": {"members": ["alice"],
                  "volumes": [{"name": "refdata", "mode": "rw"}]},
         "other": {"members": ["mallory"],
                   "volumes": [{"name": "scratch", "mode": "rw"}]}},
    )
    # What excludes mallory is being restricted TO something else: her group
    # gives her an allow-list, and refdata is not on it.
    assert cm.s3_proxy_users("refdata", "rw") == ["alice"]


def test_a_user_with_no_grants_at_all_still_reaches_every_dataset():
    # Consequence of the composition rule, pinned because it is easy to read
    # a dataset's fencing as opt-IN when it is opt-OUT: empty everywhere means
    # UNRESTRICTED, so a user in no group is granted every volume in the
    # catalog, an S3 dataset included. Adding a dataset therefore hands it to
    # everyone who has no allow-list, which is the same posture PVC volumes
    # have always had — consistent, not a new hole, but not "nobody by
    # default" either. Restricting it means giving those users a list.
    cm = _granted({"alice": {"name": "alice"}, "mallory": {"name": "mallory"}})
    assert cm.s3_proxy_users("refdata", "rw") == ["alice", "mallory"]


def test_unrestricted_user_defaults_to_read_write():
    # No allow-list anywhere means no restriction, and a volume named in no
    # grant is rw — the pre-groups default. The ro proxy must not admit them.
    cm = _granted({"alice": {"name": "alice"}})
    assert cm.s3_proxy_users("refdata", "rw") == ["alice"]
    assert cm.s3_proxy_users("refdata", "ro") == []


def test_downgrading_a_user_re_fences_the_mode_they_lost():
    # The proxy they are losing is the one that matters: a guest is root, so
    # the old mode's key is still in their hands from the previous session's
    # rclone.conf. If only the newly-granted mode is reconciled, the downgrade
    # changes nothing. Measured on a live cluster, 2026-08-17.
    cm = _granted(
        {"alice": {"name": "alice"}},
        {"proj": {"members": ["alice"],
                  "volumes": [{"name": "refdata", "mode": "ro"}]}},
    )
    seen = {}
    cm.namespace = "whistler"
    cm._ensure_object = lambda name, ns, body, **kw: seen.update(
        {name: body["spec"]["ingress"]}) or True

    class _Net:
        # Both proxies already exist, which is the situation after a
        # downgrade: the rw one is left over from the earlier grant.
        def read_namespaced_network_policy(self, name, ns):
            return object()
        create_namespaced_network_policy = None
        replace_namespaced_network_policy = None

    import whistler.config as cfg
    real = cfg.client.NetworkingV1Api
    cfg.client.NetworkingV1Api = _Net
    try:
        cm._refresh_s3_proxy_policies("refdata")
    finally:
        cfg.client.NetworkingV1Api = real

    # ro keeps her; rw — the mode she lost — is fenced to nobody.
    assert seen["whistler-s3-refdata-ro"][0]["from"][0][
        "namespaceSelector"]["matchExpressions"][0]["values"] == ["alice"]
    assert seen["whistler-s3-refdata-rw"] == []
