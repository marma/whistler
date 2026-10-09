"""Managed datasets: a claim Whistler creates, served by VersityGW.

design/storage.md, "Datasets are always S3; Whistler creates the storage".
The guest cannot tell a managed dataset from an S3 one — same Service names,
same port, same bucket — so these pin the server side: what is built, in what
order, and what a deletion leaves behind.
"""
from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

import whistler.config as cfg
from whistler import operator
from whistler.cloudinit import S3_PROXY_BUCKET, build_user_data
from whistler.config import (DATASET_SERVER_APP, KubeConfigManager,
                             USER_NS_LABEL)
from whistler.portal.management import _build_dataset_data


def _manager(**attrs):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.namespace = "whistler"
    for k, v in attrs.items():
        setattr(cm, k, v)
    return cm


MANAGED = {"source": "managed", "size": "10Gi"}


# --- what may be saved ------------------------------------------------------ #

@pytest.mark.parametrize("spec, existing, problem", [
    ({"bucket": "b"}, None, None),
    ({}, None, "needs a bucket"),
    (MANAGED, None, None),
    ({"source": "managed"}, None, "needs a size"),
    ({"source": "managed", "size": "lots"}, None, "needs a size"),
    ({"source": "managed", "size": "0"}, None, "needs a size"),
    ({"source": "nfs"}, None, "Unknown dataset source"),
    # The source is fixed: the two are fenced by differently-shaped policies,
    # and switching would leave the old pods selected by none — i.e. open.
    (MANAGED, {"bucket": "b"}, "cannot be changed"),
    ({"bucket": "b"}, MANAGED, "cannot be changed"),
    # Grows, never shrinks: Kubernetes refuses, but only in the operator log.
    ({"source": "managed", "size": "20Gi"}, MANAGED, None),
    ({"source": "managed", "size": "5Gi"}, MANAGED, "cannot shrink"),
])
def test_dataset_spec_problem(spec, existing, problem):
    got = KubeConfigManager.dataset_spec_problem(spec, existing)
    if problem is None:
        assert got is None
    else:
        assert problem in got


def test_portal_drops_s3_fields_from_a_managed_dataset():
    data = _build_dataset_data(
        "corpus", "", "https://s3.example", "bucket", "p/", "eu", "AWS",
        "my-secret", True, source="managed", size=" 50Gi ")
    assert data["source"] == "managed" and data["size"] == "50Gi"
    for key in ("endpoint", "bucket", "prefix", "region", "provider",
                "credentialsSecret"):
        assert data[key] is None


def test_portal_never_saves_a_size_on_an_s3_dataset():
    data = _build_dataset_data("ref", "", "", "bucket", "", "", "", "", True,
                               source="s3", size="50Gi")
    assert data["size"] is None and data["source"] is None


# --- what is built ---------------------------------------------------------- #

def _server(definition=MANAGED):
    return _manager()._build_dataset_server_manifests(
        volume="corpus", definition=definition,
        image="ghcr.io/versity/versitygw:v1.7.0", claim="whistler-dataset-corpus",
        root_secret="corpus-root", keys_secret="corpus-keys")


def _containers(deployment):
    return {c["name"]: c for c in
            deployment["spec"]["template"]["spec"]["containers"]}


def _mounts(container):
    return {m["name"]: m for m in container["volumeMounts"]}


def test_ro_process_is_read_only_in_the_gateway_and_the_kernel():
    # Twice over, so a read-only grant holds even if one of the two fails.
    ro = _containers(_server()[0])["versitygw-ro"]
    assert ro["args"][0] == "--readonly"
    assert _mounts(ro)["data"]["readOnly"] is True
    rw = _containers(_server()[0])["versitygw-rw"]
    assert "--readonly" not in rw["args"]
    assert _mounts(rw)["data"]["readOnly"] is False


def test_both_processes_share_one_account_store_with_no_cache():
    # An account created through the rw admin port must exist in the ro
    # process on the next request, and a deleted one must stop working there
    # just as fast (verified against VersityGW 1.7.0).
    for c in _containers(_server()[0]).values():
        assert "--iam-dir" in c["args"] and "--iam-cache-disable" in c["args"]
        assert _mounts(c)["iam"]["mountPath"] == "/iam"
    volumes = {v["name"]: v for v in
               _server()[0]["spec"]["template"]["spec"]["volumes"]}
    assert "emptyDir" in volumes["iam"]
    assert volumes["seed"]["secret"]["secretName"] == "corpus-keys"


def test_the_account_api_is_never_on_a_guest_port():
    # Without --admin-port VersityGW serves the account API on its S3 port,
    # where guests are.
    for mode, c in _containers(_server()[0]).items():
        args = c["args"]
        admin = args[args.index("--admin-port") + 1]
        s3 = args[args.index("--port") + 1]
        assert admin != s3


def test_guests_never_hold_the_root_key():
    # Both processes run as the dataset's root key, from a Secret no guest
    # descriptor reads (phase 1 handed a mode's root key to every guest).
    for c in _containers(_server()[0]).values():
        assert {e["valueFrom"]["secretKeyRef"]["name"] for e in c["env"]} == {
            "corpus-root"}


def test_a_read_only_dataset_has_no_rw_service_but_keeps_its_writer():
    # The rw process is the only one that can write the bucket policy, so it
    # runs; no guest can reach it (no Service, and its port admits nobody).
    deployment, services = _server({**MANAGED, "readOnly": True})
    assert set(_containers(deployment)) == {"versitygw-ro", "versitygw-rw"}
    assert [s["metadata"]["name"] for s in services] == ["whistler-s3-corpus-ro"]


def test_services_look_exactly_like_an_rclone_proxy_to_the_guest():
    # Same names, same port: cloud-init has no branch for managed datasets.
    _, services = _server()
    by_name = {s["metadata"]["name"]: s["spec"]["ports"][0] for s in services}
    assert by_name["whistler-s3-corpus-ro"] == {
        "name": "s3", "port": 8080, "targetPort": 8080}
    assert by_name["whistler-s3-corpus-rw"] == {
        "name": "s3", "port": 8080, "targetPort": 8081}


def test_server_prepares_the_bucket_and_seeds_the_accounts():
    init = _server()[0]["spec"]["template"]["spec"]["initContainers"][0]
    script = init["command"][-1]
    assert f"mkdir -p /srv/{S3_PROXY_BUCKET}" in script
    assert "cp /seed/users.json /iam/users.json" in script


def test_server_recreates_rather_than_rolls():
    # An RWO claim: a rolling update's new pod on another node never starts.
    assert _server()[0]["spec"]["strategy"] == {"type": "Recreate"}


def test_only_the_operator_reaches_the_writer_and_its_admin_port():
    policy = _manager()._build_dataset_server_operator_access("corpus")
    (rule,) = policy["spec"]["ingress"]
    assert rule["from"] == [{"podSelector": {"matchLabels": {
        "app": "whistler-operator"}}}]
    assert {p["port"] for p in rule["ports"]} == {8081, 9081}


def test_guest_never_asks_to_create_the_bucket():
    # VersityGW answers CreateBucket on a bucket it did not make with a 500,
    # and rclone sends one before its first upload unless told not to.
    ud = build_user_data(
        username="alice", uid=1000, ssh_keys=[], hostname="h",
        shared_datasets=[{
            "name": "corpus", "mode": "rw", "endpoint": "http://x:8080",
            "accessKeyId": "a", "secretAccessKey": "s"}])
    assert "no_check_bucket = true" in str(ud)


# --- fencing ---------------------------------------------------------------- #

def test_managed_policy_selects_the_server_pod_on_its_modes_port():
    cm = _manager()
    policy = cm._build_s3_proxy_network_policy(
        "corpus", "rw", [("alice", "open")], managed=True)
    assert policy["spec"]["podSelector"]["matchLabels"] == {
        "app": DATASET_SERVER_APP, "volume": "corpus"}
    rule = policy["spec"]["ingress"][0]
    assert rule["ports"] == [{"port": 8081, "protocol": "TCP"}]
    assert rule["from"][0]["namespaceSelector"]["matchLabels"] == {
        USER_NS_LABEL: "alice"}
    # Listed by the re-fence pass like any proxy policy.
    assert policy["metadata"]["labels"]["app"] == "whistler-s3-proxy"


def test_isolation_policy_denies_everything():
    policy = _manager()._build_dataset_server_isolation("corpus")
    assert policy["spec"]["ingress"] == []
    assert policy["spec"]["podSelector"]["matchLabels"] == {
        "app": DATASET_SERVER_APP, "volume": "corpus"}


def test_refresh_keeps_a_managed_policy_managed_even_once_undefined(
        monkeypatch):
    # A deleted dataset has no definition to say what shape its policy is;
    # rebuilt in the rclone shape it would select nothing.
    cm = _manager(users={}, groups={}, datasets={})
    seen = {}
    cm._ensure_object = lambda name, ns, body, **kw: seen.update(
        {name: body}) or True
    managed_selector = SimpleNamespace(spec=SimpleNamespace(
        pod_selector=SimpleNamespace(match_labels={
            "app": DATASET_SERVER_APP, "volume": "corpus"})))

    class _Net:
        def read_namespaced_network_policy(self, name, ns):
            return managed_selector
        create_namespaced_network_policy = None
        replace_namespaced_network_policy = None

    monkeypatch.setattr(cfg.client, "NetworkingV1Api", _Net)
    cm._refresh_s3_proxy_policies("corpus")
    for mode in ("ro", "rw"):
        body = seen[f"whistler-s3-corpus-{mode}"]
        assert body["spec"]["podSelector"]["matchLabels"]["app"] == \
            DATASET_SERVER_APP
        assert body["spec"]["ingress"] == []


# --- ensure: order and failure ---------------------------------------------- #

def _ensure_rig(monkeypatch, fail_kinds=()):
    cm = _manager(users={}, groups={}, datasets={"corpus": MANAGED},
                  dataset_server_image="img", dataset_server_resources={})
    order = []

    def ensure_object(name, ns, body, **kw):
        order.append((body["kind"], name))
        return body["kind"] not in fail_kinds
    cm._ensure_object = ensure_object
    cm._ensure_dataset_root_secret = lambda v: f"{v}-root"
    cm.sync_dataset_keys = lambda v: order.append(("Keys", v)) or {}
    cm._refresh_s3_proxy_policies = lambda v: None
    cm._ensure_dataset_claim = lambda v, d: order.append(
        ("Claim", v)) or "whistler-dataset-corpus"

    class _Core:
        create_namespaced_service = read_namespaced_service = None
        replace_namespaced_service = None

        def delete_namespaced_service(self, name, ns):
            order.append(("delete Service", name))

    monkeypatch.setattr(cfg.client, "AppsV1Api", lambda: SimpleNamespace(
        create_namespaced_deployment=None, read_namespaced_deployment=None,
        replace_namespaced_deployment=None))
    monkeypatch.setattr(cfg.client, "CoreV1Api", _Core)
    monkeypatch.setattr(cfg.client, "NetworkingV1Api", lambda: SimpleNamespace(
        create_namespaced_network_policy=None,
        read_namespaced_network_policy=None,
        replace_namespaced_network_policy=None))
    return cm, order


def test_policies_exist_before_the_pod_does(monkeypatch):
    cm, order = _ensure_rig(monkeypatch)
    assert cm.ensure_managed_dataset("corpus", MANAGED)
    kinds = [k for k, _ in order]
    assert kinds.index("Deployment") > max(
        i for i, k in enumerate(kinds) if k == "NetworkPolicy")
    assert ("NetworkPolicy", "whistler-dataset-corpus") in order[:3]
    # The seed exists before the pod that copies it.
    assert kinds.index("Keys") < kinds.index("Deployment")


def test_no_pod_without_its_fence(monkeypatch):
    cm, order = _ensure_rig(monkeypatch, fail_kinds=("NetworkPolicy",))
    assert not cm.ensure_managed_dataset("corpus", MANAGED)
    assert "Deployment" not in [k for k, _ in order]
    assert "Claim" not in [k for k, _ in order]


def test_a_read_only_dataset_loses_its_rw_service(monkeypatch):
    cm, order = _ensure_rig(monkeypatch)
    cm.ensure_managed_dataset("corpus", {**MANAGED, "readOnly": True})
    assert ("delete Service", "whistler-s3-corpus-rw") in order
    assert ("NetworkPolicy", "whistler-s3-corpus-rw") not in order
    # The writer stays reachable by the operator alone.
    assert ("NetworkPolicy", "whistler-dataset-corpus-operator") in order


def test_an_invalid_definition_builds_nothing(monkeypatch):
    cm, order = _ensure_rig(monkeypatch)
    assert not cm.ensure_managed_dataset("corpus", {"source": "managed"})
    assert order == []


def test_ensure_s3_proxy_hands_a_managed_dataset_to_its_server():
    cm = _manager()
    calls = []
    cm.ensure_managed_dataset = lambda v, d: calls.append(v) or True
    # No credentialsSecret, which would refuse an S3 dataset.
    assert cm.ensure_s3_proxy("corpus", "ro", MANAGED)
    assert calls == ["corpus"]


# --- deletion --------------------------------------------------------------- #

def _labelled(name, volume):
    return SimpleNamespace(metadata=SimpleNamespace(
        name=name, labels={"app": DATASET_SERVER_APP, "volume": volume}))


def test_prune_removes_only_servers_of_undefined_datasets(monkeypatch):
    cm = _manager()
    cm._managed_dataset_names = lambda: {"kept"}
    deleted = []
    monkeypatch.setattr(cfg.client, "AppsV1Api", lambda: SimpleNamespace(
        list_namespaced_deployment=lambda ns, label_selector: SimpleNamespace(
            items=[_labelled("whistler-dataset-kept", "kept"),
                   _labelled("whistler-dataset-gone", "gone")]),
        delete_namespaced_deployment=lambda n, ns: deleted.append(n)))
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        list_namespaced_service=lambda ns, label_selector: SimpleNamespace(
            items=[_labelled("whistler-s3-kept-ro", "kept"),
                   _labelled("whistler-s3-gone-ro", "gone")]),
        delete_namespaced_service=lambda n, ns: deleted.append(n)))
    assert cm.prune_dataset_servers() == ["gone"]
    # The claim is never touched, and neither are the policies (fenced to
    # nobody already; deleting one under a terminating pod would open it).
    assert deleted == ["whistler-dataset-gone", "whistler-s3-gone-ro"]


def test_prune_does_nothing_when_it_cannot_read_the_catalog(monkeypatch):
    # The cached catalog survives an API failure by design; deciding what to
    # tear down from it would be the wrong way round.
    cm = _manager()
    cm._managed_dataset_names = lambda: None
    monkeypatch.setattr(cfg.client, "AppsV1Api", lambda: pytest.fail(
        "listed servers without a catalog"))
    assert cm.prune_dataset_servers() == []


# --- archive, resurrect, delete for good ----------------------------------- #

ARCHIVED = {**MANAGED, "archived": True}


def test_an_archived_dataset_admits_nobody_but_keeps_its_grants():
    # The cells stay on the User, so a resurrected dataset comes back as it
    # was; while archived they reach nothing.
    users = {"alice": {"name": "alice",
                       "volumeAccess": {"open": {"corpus": "allowed"}}}}
    cm = _manager(users=users, groups={}, datasets={"corpus": ARCHIVED})
    assert cm.s3_proxy_peers("corpus", "rw") == []
    cm.datasets = {"corpus": MANAGED}
    assert cm.s3_proxy_peers("corpus", "rw") == [("alice", "open")]


def test_an_archived_dataset_is_never_mounted(monkeypatch):
    users = {"alice": {"name": "alice",
                       "volumeAccess": {"open": {"corpus": "allowed"}}}}
    cm = _manager(users=users, groups={}, datasets={"corpus": ARCHIVED})
    cm._refresh_s3_proxy_policies = lambda v: None
    cm.ensure_s3_proxy = lambda *a: pytest.fail("prepared an archived dataset")
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: None)
    assert cm.session_shared_datasets("alice", "open") == []


def test_an_archived_dataset_has_no_server(monkeypatch):
    cm, order = _ensure_rig(monkeypatch)
    assert cm.ensure_managed_dataset("corpus", ARCHIVED)
    assert order == []
    # ...and the prune counts it as not served.
    cm.api = SimpleNamespace(list_namespaced_custom_object=lambda *a: {
        "items": [{"metadata": {"name": "corpus"}, "spec": ARCHIVED},
                  {"metadata": {"name": "live"}, "spec": MANAGED}]})
    cm.group, cm.version = "g", "v1"
    assert cm._managed_dataset_names() == {"live"}


def _crud(existing_spec):
    cm = _manager(group="g", version="v1")
    calls = []

    def get(*a):
        if existing_spec is None:
            raise ApiException(status=404)
        return {"metadata": {"name": "corpus", "resourceVersion": "1"},
                "spec": existing_spec}
    cm.api = SimpleNamespace(
        get_namespaced_custom_object=get,
        create_namespaced_custom_object=lambda *a: calls.append("create"),
        replace_namespaced_custom_object=lambda *a: calls.append("replace"),
        patch_namespaced_custom_object=lambda *a: calls.append(("patch", a[-1])))
    cm._load_datasets = lambda: None
    cm._refresh_s3_proxy_policies = lambda v: calls.append("refence")
    return cm, calls


@pytest.mark.parametrize("existing, message", [
    (ARCHIVED, "archived dataset named 'corpus' exists"),
    (MANAGED, "already exists"),
])
def test_creating_never_takes_an_existing_name(existing, message):
    # An archived dataset holds its name: re-creating it would hand a new
    # definition the old one's grants and, if managed, its data.
    cm, calls = _crud(existing)
    with pytest.raises(cfg.DatasetSpecError, match=message):
        cm.save_dataset({"name": "corpus", **MANAGED}, create=True)
    assert calls == []


def test_an_archived_dataset_cannot_be_edited():
    cm, calls = _crud(ARCHIVED)
    with pytest.raises(cfg.DatasetSpecError, match="resurrect"):
        cm.save_dataset({"name": "corpus", **MANAGED})
    assert calls == []


def test_archive_and_restore_flip_the_flag_and_refence_at_once():
    cm, calls = _crud(MANAGED)
    assert cm.archive_dataset("corpus")
    assert cm.restore_dataset("corpus")
    # None removes the key under a merge patch.
    assert calls == [("patch", {"spec": {"archived": True}}), "refence",
                     ("patch", {"spec": {"archived": None}}), "refence"]


def test_only_an_archived_dataset_can_be_deleted_for_good():
    cm, calls = _crud(MANAGED)
    cm.datasets = {"corpus": MANAGED}
    cm.get_dataset_definitions = lambda: {"corpus": MANAGED}
    with pytest.raises(cfg.DatasetSpecError, match="archive it first"):
        cm.destroy_dataset("corpus")
    cm.get_dataset_definitions = lambda: {"corpus": ARCHIVED}
    assert cm.destroy_dataset("corpus")
    # A mark: the PV write is the operator's.
    assert calls == [("patch", {"metadata": {"annotations": {
        cfg.DELETE_DATA_ANNOTATION: "true"}}})]


def _purge_rig(monkeypatch, spec, annotations, pods=()):
    cm = _manager(group="g", version="v1")
    order = []
    cm.api = SimpleNamespace(
        get_namespaced_custom_object=lambda *a: {
            "metadata": {"name": "corpus", "annotations": annotations},
            "spec": spec},
        delete_namespaced_custom_object=lambda *a: order.append(("cr", a[-1])))
    cm.release_claim_volume = lambda ns, claim: order.append(
        ("release", claim)) or True

    def record(kind):
        return lambda name, ns: order.append((kind, name))
    monkeypatch.setattr(cfg.client, "AppsV1Api", lambda: SimpleNamespace(
        delete_namespaced_deployment=record("deployment")))
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        list_namespaced_pod=lambda ns, label_selector: SimpleNamespace(
            items=list(pods)),
        delete_namespaced_persistent_volume_claim=record("claim"),
        delete_namespaced_service=record("service"),
        delete_namespaced_secret=record("secret")))
    monkeypatch.setattr(cfg.client, "NetworkingV1Api", lambda: SimpleNamespace(
        delete_namespaced_network_policy=record("policy")))
    return cm, order


MARK = {cfg.DELETE_DATA_ANNOTATION: "true"}


def test_purge_takes_the_data_after_the_pods_and_the_record_last(monkeypatch):
    cm, order = _purge_rig(monkeypatch, ARCHIVED, MARK)
    assert cm.purge_dataset("corpus")
    kinds = [k for k, _ in order]
    assert kinds.index("release") > max(
        i for i, k in enumerate(kinds) if k == "deployment")
    assert kinds.index("claim") > kinds.index("release")
    assert kinds.index("policy") > kinds.index("claim")
    assert order[-1] == ("cr", "corpus")
    assert ("secret", "whistler-s3-corpus-rw-auth") in order


def test_purge_keeps_the_fences_while_a_pod_is_terminating(monkeypatch):
    pod = SimpleNamespace(metadata=SimpleNamespace(
        labels={"app": DATASET_SERVER_APP, "volume": "corpus"}))
    cm, order = _purge_rig(monkeypatch, ARCHIVED, MARK, pods=[pod])
    assert not cm.purge_dataset("corpus")
    assert {k for k, _ in order} == {"deployment"}


def test_purge_never_touches_a_bucket(monkeypatch):
    cm, order = _purge_rig(monkeypatch, {"bucket": "b", "archived": True}, MARK)
    assert cm.purge_dataset("corpus")
    assert not {"release", "claim"} & {k for k, _ in order}


@pytest.mark.parametrize("spec, annotations", [
    (MANAGED, MARK),      # marked but live: never
    (ARCHIVED, {}),       # archived but not marked: never
])
def test_purge_acts_only_on_archived_and_marked(monkeypatch, spec,
                                                annotations):
    cm, order = _purge_rig(monkeypatch, spec, annotations)
    assert not cm.purge_dataset("corpus")
    assert order == []


# --- operator dispatch ------------------------------------------------------ #

def test_operator_serves_live_managed_datasets_and_destroys_marked_archives():
    marked = {"annotations": MARK}
    assert operator._served(spec=MANAGED, meta={})
    assert not operator._served(spec=ARCHIVED, meta={})
    assert not operator._served(spec={"bucket": "b"}, meta={})
    assert operator._destroyed(spec=ARCHIVED, meta=marked)
    assert operator._destroyed(spec={"bucket": "b", "archived": True},
                               meta=marked)
    assert not operator._destroyed(spec=MANAGED, meta=marked)
    assert not operator._destroyed(spec=ARCHIVED, meta={})


def test_claim_re_attaches_before_it_provisions(monkeypatch):
    cm = _manager(dataset_storage_class=None)
    calls = []
    cm.reattach_claim = lambda ns, claim, labels: calls.append(
        ("reattach", claim)) or True

    def read(name, ns):
        raise ApiException(status=404)
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        read_namespaced_persistent_volume_claim=read,
        create_namespaced_persistent_volume_claim=lambda *a: pytest.fail(
            "provisioned an empty disk next to a retained one")))
    assert cm._ensure_dataset_claim("corpus", MANAGED) == \
        "whistler-dataset-corpus"
    assert calls == [("reattach", "whistler-dataset-corpus")]


# --- the portal's confirmation ---------------------------------------------- #

@pytest.mark.parametrize("typed", [None, "", "corpu", " corpus2"])
def test_destroy_refuses_without_the_typed_name(typed):
    # Checked server-side, not only by the modal's disabled button: a form
    # posted without the name must not destroy anything.
    import asyncio
    from fastapi import HTTPException
    from whistler.portal.management import admin_dataset_destroy
    cm = SimpleNamespace(destroy_dataset=lambda n: pytest.fail("destroyed"))
    with pytest.raises(HTTPException) as e:
        asyncio.run(admin_dataset_destroy(None, cm, "admin", "corpus", typed))
    assert e.value.status_code == 400


def test_revive_starts_only_served_datasets_whose_server_is_missing(
        monkeypatch):
    # A resurrect returns the spec to what kopf last recorded, so kopf raises
    # no update; the worker's revive pass is what brings the server back.
    items = [
        {"metadata": {"name": "back"}, "spec": MANAGED},
        {"metadata": {"name": "running"}, "spec": MANAGED},
        {"metadata": {"name": "shelved"}, "spec": ARCHIVED},
        {"metadata": {"name": "going", "annotations": MARK}, "spec": MANAGED},
        {"metadata": {"name": "bucket"}, "spec": {"bucket": "b"}},
    ]
    cm = _manager(group="g", version="v1")
    cm.api = SimpleNamespace(
        list_namespaced_custom_object=lambda *a: {"items": items})
    ensured = []
    cm.ensure_managed_dataset = lambda v, d: ensured.append(v) or True

    def read(name, ns):
        if name != "whistler-dataset-running":
            raise ApiException(status=404)
    monkeypatch.setattr(cfg.client, "AppsV1Api", lambda: SimpleNamespace(
        read_namespaced_deployment=read))
    assert cm.revive_dataset_servers() == ["back"]
    assert ensured == ["back"]


def _portal_request(cm):
    async def run(fn, *a):
        return fn(*a)
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(run=run)))


def test_archived_datasets_are_listed_last():
    import asyncio
    from whistler.portal.management import _dataset_rows, _matrix_sections
    defs = {"a-old": ARCHIVED, "b-live": MANAGED, "c-live": {"bucket": "x"},
            "0-old": ARCHIVED}
    cm = SimpleNamespace(get_dataset_definitions=lambda: defs,
                         is_archived_dataset=KubeConfigManager.is_archived_dataset)
    rows = asyncio.run(_dataset_rows(_portal_request(cm), cm))
    assert [r["name"] for r in rows] == ["b-live", "c-live", "0-old", "a-old"]
    sections = asyncio.run(_matrix_sections(_portal_request(cm), cm))
    assert [r["key"] for r in sections[-1]["rows"]] == [
        "b-live", "c-live", "0-old", "a-old"]


# --- phase 2: per-user keys ------------------------------------------------- #

import base64
import json as _json

from whistler import dataset_accounts


def _b64(s):
    return base64.b64encode(s.encode()).decode()


def _keys_rig(monkeypatch, users, stored=None):
    """A manager whose keys Secret holds ``stored``; records writes."""
    cm = _manager(users=users, groups={}, datasets={"corpus": MANAGED})
    written = []
    secret = None if stored is None else SimpleNamespace(
        data={k: _b64(v) for k, v in stored.items()},
        metadata=SimpleNamespace(resource_version="7"))

    def read(name, ns):
        if secret is None:
            raise ApiException(status=404)
        return secret
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        read_namespaced_secret=read,
        create_namespaced_secret=lambda ns, body: written.append(
            ("create", body["stringData"])),
        replace_namespaced_secret=lambda name, ns, body: written.append(
            ("replace", body["stringData"]))))
    return cm, written


ALICE_RW = {"alice": {"name": "alice",
                      "volumeAccess": {"open": {"corpus": "allowed"}}}}


def test_keys_follow_the_matrix(monkeypatch):
    cm, written = _keys_rig(monkeypatch, {**ALICE_RW, "bob": {
        "name": "bob", "volumeAccess": {"lab": {"corpus": "read-only"}}}})
    keys = cm.sync_dataset_keys("corpus")
    assert set(keys) == {"rw.alice", "ro.bob"}
    (kind, data), = written
    assert kind == "create"
    seeded = _json.loads(data["users.json"])["accessAccounts"]
    assert {a: v["secret"] for a, v in seeded.items()} == keys


def test_an_existing_key_survives_and_a_revoked_one_goes(monkeypatch):
    # A guest holding its key keeps working across syncs; a user no longer
    # granted loses theirs from the seed too.
    stored = {"rw.alice": "kept", "ro.mallory": "gone",
              "users.json": "{}"}
    cm, written = _keys_rig(monkeypatch, ALICE_RW, stored)
    assert cm.sync_dataset_keys("corpus") == {"rw.alice": "kept"}
    (kind, data), = written
    assert kind == "replace" and "ro.mallory" not in data


def test_an_unchanged_key_set_is_not_rewritten(monkeypatch):
    keys = {"rw.alice": "kept"}
    stored = {**keys, "users.json": dataset_accounts.render_users_json(keys)}
    cm, written = _keys_rig(monkeypatch, ALICE_RW, stored)
    assert cm.sync_dataset_keys("corpus") == keys
    assert written == []


def test_archived_or_read_only_ceiling_shapes_the_accounts(monkeypatch):
    cm, _ = _keys_rig(monkeypatch, ALICE_RW)
    cm.datasets = {"corpus": {**MANAGED, "readOnly": True}}
    assert set(cm.sync_dataset_keys("corpus")) == {"ro.alice"}
    cm.datasets = {"corpus": {**MANAGED, "archived": True}}
    assert cm.sync_dataset_keys("corpus") == {}


def _accounts_rig(monkeypatch, keys, live):
    cm = _manager()
    calls = []
    cm.sync_dataset_keys = lambda v: keys
    cm._dataset_server_ip = lambda v: "10.0.0.9"
    cm._ensure_dataset_root_secret = lambda v: "root"
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        read_namespaced_secret=lambda n, ns: SimpleNamespace(data={
            "accessKeyId": _b64("whistler-root"),
            "secretAccessKey": _b64("r00t")})))

    def call(method, url, access, secret, body=b"", headers=None):
        assert (access, secret) == ("whistler-root", "r00t")
        path = url.split("10.0.0.9:")[1]
        calls.append((method, path, body))
        if path.endswith("/list-users"):
            accounts = "".join(f"<Accounts><Access>{a}</Access><Secret>{s}"
                               f"</Secret></Accounts>" for a, s in live.items())
            return 200, f"<ListUserAccountsResult>{accounts}" \
                        f"</ListUserAccountsResult>".encode()
        return 200, b""
    cm._vgw_call = staticmethod(call)
    return cm, calls


def test_account_sync_revokes_first_then_grants_then_policy(monkeypatch):
    cm, calls = _accounts_rig(
        monkeypatch, keys={"rw.alice": "k1", "ro.bob": "k2"},
        live={"rw.alice": "k1", "ro.mallory": "old"})
    assert cm.sync_dataset_accounts("corpus")
    paths = [(m, p.split("?")[0], p) for m, p, _ in calls]
    assert paths[0][1] == "9081/list-users"
    assert paths[1][2] == "9081/delete-user?access=ro.mallory"
    assert paths[2][1] == "9081/create-user"
    assert b"<Access>ro.bob</Access>" in calls[2][2]
    assert paths[3][2] == "8081/data?acl"
    assert paths[4][0] == "PUT" and paths[4][2] == "8081/data?policy"
    policy = _json.loads(calls[4][2])
    assert {p for s in policy["Statement"] for p in s["Principal"]["AWS"]} \
        == {"rw.alice", "ro.bob"}
    # rw.alice was already right: not recreated.
    assert not any(b"rw.alice" in body for m, p, body in calls
                   if p.endswith("create-user"))


def test_a_changed_key_is_replaced(monkeypatch):
    cm, calls = _accounts_rig(monkeypatch, keys={"rw.alice": "new"},
                              live={"rw.alice": "old"})
    assert cm.sync_dataset_accounts("corpus")
    ops = [p.split("?")[0] for _, p, _ in calls]
    assert ops[:3] == ["9081/list-users", "9081/delete-user",
                       "9081/create-user"]


def test_nobody_granted_deletes_the_policy(monkeypatch):
    cm, calls = _accounts_rig(monkeypatch, keys={}, live={"ro.x": "s"})
    assert cm.sync_dataset_accounts("corpus")
    assert ("DELETE", "8081/data?policy", b"") in calls


def test_no_ready_server_is_a_retry_not_a_failure_to_record(monkeypatch):
    cm, calls = _accounts_rig(monkeypatch, keys={"rw.alice": "k"}, live={})
    cm._dataset_server_ip = lambda v: None
    assert not cm.sync_dataset_accounts("corpus")
    assert calls == []


def test_a_server_error_stops_the_sync(monkeypatch):
    cm, calls = _accounts_rig(monkeypatch, keys={"rw.alice": "k"}, live={})
    cm._vgw_call = staticmethod(lambda *a, **k: (500, b"boom"))
    assert not cm.sync_dataset_accounts("corpus")


def test_a_guest_gets_its_own_key_not_a_shared_one(monkeypatch):
    cm = _manager(users=ALICE_RW, groups={}, datasets={"corpus": MANAGED})
    cm._refresh_s3_proxy_policies = lambda v: None
    cm.ensure_s3_proxy = lambda *a: True
    cm.sync_dataset_keys = lambda v: {"rw.alice": "alices-key",
                                      "rw.bob": "bobs-key"}
    cm.sync_dataset_accounts = lambda v: True
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: None)
    (d,) = cm.session_shared_datasets("alice", "open", ["corpus"])
    assert (d["accessKeyId"], d["secretAccessKey"]) == ("rw.alice",
                                                       "alices-key")


def test_an_account_that_appeared_since_the_listing_is_replaced(monkeypatch):
    # Two passes woken by one change raced on k3d: create-user -> 409.
    cm, calls = _accounts_rig(monkeypatch, keys={"rw.alice": "k"}, live={})
    real = cm._vgw_call
    seen = []

    def call(method, url, access, secret, body=b"", headers=None):
        if url.endswith("/create-user") and not seen:
            seen.append(1)
            calls.append((method, url.split("10.0.0.9:")[1], body))
            return 409, b"exists"
        return real(method, url, access, secret, body=body, headers=headers)
    cm._vgw_call = staticmethod(call)
    assert cm.sync_dataset_accounts("corpus")
    ops = [p.split("?")[0] for _, p, _ in calls]
    assert ops[1:4] == ["9081/create-user", "9081/delete-user",
                        "9081/create-user"]
