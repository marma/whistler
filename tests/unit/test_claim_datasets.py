"""Datasets from existing claims: `source: claim`.

design/storage.md, "Datasets from existing claims". The same VersityGW
server as a managed dataset — keys, fences, external listener — over a claim
the admin names, which Whistler mounts and otherwise never touches. These pin
both halves: that it is served like a managed dataset, and that creating,
archiving and deleting it leave the claim exactly as it was.
"""
from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

import whistler.config as cfg
from whistler import operator
from whistler.config import KubeConfigManager
from whistler.portal.management import _build_dataset_data


def _manager(**attrs):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.namespace = "whistler"
    for k, v in attrs.items():
        setattr(cm, k, v)
    return cm


CLAIM = {"source": "claim", "claimName": "corpus-data"}
MANAGED = {"source": "managed", "size": "10Gi"}
MARK = {cfg.DELETE_DATA_ANNOTATION: "true"}


# --- what may be saved ------------------------------------------------------ #

@pytest.mark.parametrize("spec, existing, problem", [
    (CLAIM, None, None),
    ({"source": "claim"}, None, "needs the claim's name"),
    ({"source": "claim", "claimName": "Not_A_Name"}, None,
     "needs the claim's name"),
    # Pointing the dataset at another claim is an edit like any other.
    ({"source": "claim", "claimName": "other"}, CLAIM, None),
    # The source is still fixed, in both directions.
    (CLAIM, MANAGED, "cannot be changed"),
    (MANAGED, CLAIM, "cannot be changed"),
    (CLAIM, {"bucket": "b"}, "cannot be changed"),
])
def test_dataset_spec_problem(spec, existing, problem):
    got = KubeConfigManager.dataset_spec_problem(spec, existing)
    if problem is None:
        assert got is None
    else:
        assert problem in got


def test_portal_keeps_the_claim_and_drops_what_does_not_apply():
    data = _build_dataset_data(
        "corpus", "", "https://s3.example", "bucket", "p/", "eu", "AWS",
        "my-secret", True, source="claim", size="50Gi",
        claim_name=" corpus-data ")
    assert data["source"] == "claim" and data["claimName"] == "corpus-data"
    assert data["size"] is None
    for key in ("endpoint", "bucket", "prefix", "region", "provider",
                "credentialsSecret"):
        assert data[key] is None


@pytest.mark.parametrize("source", ["managed", "s3"])
def test_portal_never_saves_a_claim_name_on_another_source(source):
    data = _build_dataset_data("ref", "", "", "bucket", "", "", "", "", True,
                               source=source, size="1Gi",
                               claim_name="corpus-data")
    assert data["claimName"] is None


def test_it_is_served_like_a_managed_dataset_but_not_owned_like_one():
    assert KubeConfigManager.has_dataset_server(CLAIM)
    assert KubeConfigManager.has_dataset_server(MANAGED)
    assert not KubeConfigManager.has_dataset_server({"bucket": "b"})
    assert not KubeConfigManager.is_managed_dataset(CLAIM)
    assert operator._served(spec=CLAIM, meta={})
    assert not operator._served(spec={**CLAIM, "archived": True}, meta={})


# --- what is built ---------------------------------------------------------- #

def _server(definition=CLAIM, **attrs):
    return _manager(**attrs)._build_dataset_server_manifests(
        volume="corpus", definition=definition,
        image="ghcr.io/versity/versitygw:v1.7.0", claim="corpus-data",
        root_secret="corpus-root", keys_secret="corpus-keys")


def _pod(deployment):
    return deployment["spec"]["template"]["spec"]


def _containers(deployment):
    return {c["name"]: c for c in _pod(deployment)["containers"]}


def _mounts(container):
    return {m["name"]: m for m in container["volumeMounts"]}


def test_the_claim_is_the_bucket_directory():
    # Mounted AT /srv/<bucket>, so what is already in it is the bucket's
    # content as it stands; the root is an emptyDir holding the mount point.
    pod = _pod(_server()[0])
    volumes = {v["name"]: v for v in pod["volumes"]}
    assert volumes["data"]["persistentVolumeClaim"]["claimName"] == \
        "corpus-data"
    assert "emptyDir" in volumes["root"]
    for c in pod["containers"]:
        mounts = _mounts(c)
        assert mounts["data"]["mountPath"] == "/srv/corpus"
        assert mounts["root"]["mountPath"] == "/srv"


def test_metadata_goes_to_a_sidecar_not_onto_the_claim():
    # No xattrs on the admin's files (and none needed, so NFS claims work).
    for c in _containers(_server(dataset_external_host="s3.example")[0]
                         ).values():
        assert c["args"][-4:] == ["posix", "--sidecar", "/meta", "/srv"]
        assert _mounts(c)["meta"]["mountPath"] == "/meta"


def test_ro_process_is_read_only_on_the_claim_and_the_sidecar():
    containers = _containers(_server()[0])
    ro, rw = _mounts(containers["versitygw-ro"]), \
        _mounts(containers["versitygw-rw"])
    assert containers["versitygw-ro"]["args"][0] == "--readonly"
    assert ro["data"]["readOnly"] is True and ro["meta"]["readOnly"] is True
    assert rw["data"]["readOnly"] is False and rw["meta"]["readOnly"] is False


def test_external_listener_of_a_read_only_claim_dataset_is_read_only():
    ext = _containers(_server({**CLAIM, "readOnly": True},
                              dataset_external_host="s3.example")[0]
                      )["versitygw-ext"]
    assert ext["args"][0] == "--readonly"
    assert _mounts(ext)["data"]["readOnly"] is True


def test_the_claim_is_never_prepared_or_chowned():
    pod = _pod(_server()[0])
    init = pod["initContainers"][0]
    # Only the account store is seeded: no rename, no mkdir, no mount.
    assert "data" not in _mounts(init)
    assert "mv " not in init["command"][-1] and \
        "mkdir" not in init["command"][-1]
    assert "cp /seed/users.json /iam/users.json" in init["command"][-1]
    # fsGroup would chgrp the whole existing tree.
    assert "fsGroup" not in pod["securityContext"]
    assert "fsGroupChangePolicy" not in pod["securityContext"]


def test_a_managed_dataset_is_built_as_before():
    pod = _pod(_server(MANAGED)[0])
    assert pod["securityContext"]["fsGroup"] == 1000
    for c in pod["containers"]:
        assert "--sidecar" not in c["args"]
        assert _mounts(c)["data"]["mountPath"] == "/srv"
        assert "root" not in _mounts(c) and "meta" not in _mounts(c)


# --- the claim is left alone ------------------------------------------------ #

def _core(monkeypatch, exists=True):
    calls = []

    def read(name, ns):
        calls.append(("read", name, ns))
        if not exists:
            raise ApiException(status=404)
        return SimpleNamespace(spec=None)

    def touched(kind):
        return lambda *a, **kw: pytest.fail(f"{kind} the admin's claim")
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        read_namespaced_persistent_volume_claim=read,
        create_namespaced_persistent_volume_claim=touched("created"),
        patch_namespaced_persistent_volume_claim=touched("patched"),
        delete_namespaced_persistent_volume_claim=touched("deleted")))
    return calls


def test_an_existing_claim_is_only_looked_up(monkeypatch):
    calls = _core(monkeypatch)
    cm = _manager()
    cm.secure_claim = lambda *a: pytest.fail("retained the admin's PV")
    cm.reattach_claim = lambda *a: pytest.fail("re-attached")
    assert cm._ensure_dataset_claim("corpus", CLAIM) == "corpus-data"
    assert calls == [("read", "corpus-data", "whistler")]


def test_a_missing_claim_serves_nothing_and_provisions_nothing(monkeypatch):
    _core(monkeypatch, exists=False)
    cm = _manager()
    cm.reattach_claim = lambda *a: pytest.fail("re-attached")
    assert cm._ensure_dataset_claim("corpus", CLAIM) is None


def test_purge_leaves_the_claim_and_its_volume(monkeypatch):
    cm = _manager(group="g", version="v1")
    order = []
    cm.api = SimpleNamespace(
        get_namespaced_custom_object=lambda *a: {
            "metadata": {"name": "corpus", "annotations": MARK},
            "spec": {**CLAIM, "archived": True}},
        delete_namespaced_custom_object=lambda *a: order.append(("cr", a[-1])))
    cm.release_claim_volume = lambda *a: pytest.fail("released the PV")

    def record(kind):
        return lambda name, ns: order.append((kind, name))
    monkeypatch.setattr(cfg.client, "AppsV1Api", lambda: SimpleNamespace(
        delete_namespaced_deployment=record("deployment")))
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        list_namespaced_pod=lambda ns, label_selector: SimpleNamespace(
            items=[]),
        delete_namespaced_persistent_volume_claim=lambda *a: pytest.fail(
            "deleted the admin's claim"),
        delete_namespaced_service=record("service"),
        delete_namespaced_secret=record("secret")))
    monkeypatch.setattr(cfg.client, "NetworkingV1Api", lambda: SimpleNamespace(
        delete_namespaced_network_policy=record("policy"),
        delete_namespaced_ingress=record("ingress")))
    assert cm.purge_dataset("corpus")
    # Everything Whistler made goes, the record last.
    assert ("secret", "whistler-dataset-corpus-keys") in order
    assert order[-1] == ("cr", "corpus")


def test_the_sweep_never_retains_the_admins_volume():
    cm = _manager(group="g", version="v1")
    cm.api = SimpleNamespace(list_namespaced_custom_object=lambda *a: {
        "items": [{"metadata": {"name": "corpus"}, "spec": CLAIM}]})
    cm.secure_claim = lambda *a: pytest.fail("retained the admin's PV")
    cm.secure_dataset_claims()


def test_external_access_is_offered_for_a_claim_dataset():
    cm = _manager(dataset_external_host="s3.example")
    cm.get_dataset_definitions = lambda: {"corpus": CLAIM}
    cm.get_user_volume_access = lambda u: {
        cfg.EXTERNAL_ZONE: {"corpus": "read-only"}}
    assert cm.external_dataset_mode("alice", "corpus") == "ro"


# --- subPath: several datasets on one claim --------------------------------- #

@pytest.mark.parametrize("spec, problem", [
    ({**CLAIM, "subPath": "projects/imaging"}, None),
    ({**CLAIM, "subPath": "a b/ö"}, None),
    ({**CLAIM, "subPath": "/abs"}, "drop the leading '/'"),
    ({**CLAIM, "subPath": "a/../b"}, "'..'"),
    ({**CLAIM, "subPath": "a//b"}, "empty"),
    ({**CLAIM, "subPath": ""}, "1-1024 characters"),
    ({**CLAIM, "subPath": "a\\b"}, "backslash"),
    ({**MANAGED, "subPath": "a"}, "only be set on a dataset from an existing"),
])
def test_sub_path_problems(spec, problem):
    got = KubeConfigManager.dataset_spec_problem(spec)
    if problem is None:
        assert got is None
    else:
        assert problem in got


def test_every_process_serves_only_the_sub_path():
    pod = _pod(_server({**CLAIM, "subPath": "projects/a"},
                       dataset_external_host="s3.example")[0])
    for c in pod["containers"]:
        data = _mounts(c)["data"]
        assert data["subPath"] == "projects/a"
        assert data["mountPath"] == "/srv/corpus"


def test_a_missing_sub_path_is_refused_before_kubelet_could_create_it():
    # Kubelet creates a missing subPath (root, 0777) when the container that
    # uses it starts; the check runs first, over the whole claim read-only.
    pod = _pod(_server({**CLAIM, "subPath": "projects/a"})[0])
    check, prepare = pod["initContainers"]
    assert check["name"] == "check-sub-path" and prepare["name"] == "prepare"
    mount = _mounts(check)["data"]
    assert mount["readOnly"] is True and "subPath" not in mount
    # The path is data, never part of the script.
    assert {"name": "SUB_PATH", "value": "projects/a"} in check["env"]
    assert "projects/a" not in check["command"][-1]


def test_no_sub_path_no_check():
    pod = _pod(_server()[0])
    assert [c["name"] for c in pod["initContainers"]] == ["prepare"]
    assert all("subPath" not in _mounts(c)["data"] for c in pod["containers"])


def test_portal_trims_a_trailing_slash_and_drops_it_elsewhere():
    data = _build_dataset_data("c", "", "", "", "", "", "", "", True,
                               source="claim", claim_name="x",
                               sub_path=" projects/a/ ")
    assert data["subPath"] == "projects/a"
    data = _build_dataset_data("c", "", "", "", "", "", "", "", True,
                               source="managed", size="1Gi", sub_path="a")
    assert data["subPath"] is None
