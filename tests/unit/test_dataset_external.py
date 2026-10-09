"""External access to managed datasets (design/storage.md, "External access").

A dataset cell in the access matrix's reserved `external` column grants a key
that works from outside the cluster, through a separate listener whose
account store holds only external accounts. These pin who gets such a key,
where it is valid, and when the way in from outside exists at all.
"""
import base64
import json
from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

import whistler.config as cfg
from whistler import dataset_accounts
from whistler.cloudinit import build_user_data
from whistler.config import EXTERNAL_ZONE, KubeConfigManager

MANAGED = {"source": "managed", "size": "1Gi"}


def _manager(external=True, **attrs):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.namespace = "whistler"
    cm.dataset_external_host = "s3.example.org" if external else ""
    cm.dataset_external_scheme = "https"
    cm.dataset_external_ingress_class = None
    cm.dataset_external_tls_secret = None
    cm.dataset_external_annotations = {}
    cm.dataset_external_peers = [{"namespaceSelector": {}}]
    cm.dataset_external_source_filter = "traefik"
    for k, v in attrs.items():
        setattr(cm, k, v)
    return cm


def _users(**cells):
    return {u: {"name": u, "volumeAccess": access}
            for u, access in cells.items()}


# --- who holds what --------------------------------------------------------- #

def test_external_cells_grant_external_accounts_only():
    cm = _manager(users=_users(
        alice={"default": {"corpus": "allowed"},
               EXTERNAL_ZONE: {"corpus": "read-only"}},
        bob={EXTERNAL_ZONE: {"corpus": "allowed"}}),
        groups={}, datasets={"corpus": MANAGED})
    holders = cm.dataset_account_holders("corpus")
    assert holders["rw"] == {"alice"}
    assert holders["xro"] == {"alice"} and holders["xrw"] == {"bob"}
    # The external column is not a place sessions run: it admits nobody
    # to the internal endpoints and offers nothing to mount.
    assert cm.s3_proxy_peers("corpus", "rw") == [("alice", "default")]
    assert [c["name"] for c in cm.get_user_dataset_choices("bob")] == []


def test_no_external_accounts_while_external_access_is_off():
    cm = _manager(external=False, users=_users(
        bob={EXTERNAL_ZONE: {"corpus": "allowed"}}), groups={},
        datasets={"corpus": MANAGED})
    holders = cm.dataset_account_holders("corpus")
    assert holders["xro"] == set() and holders["xrw"] == set()
    assert cm.get_user_external_datasets("bob") == []


@pytest.mark.parametrize("definition, expected", [
    ({**MANAGED, "readOnly": True}, "ro"),       # the ceiling applies outside
    ({**MANAGED, "archived": True}, None),       # archived: nobody
    ({"bucket": "b"}, None),                     # an S3 dataset has no listener
])
def test_external_mode_follows_the_dataset(definition, expected):
    cm = _manager(users=_users(bob={EXTERNAL_ZONE: {"corpus": "allowed"}}),
                  groups={}, datasets={"corpus": definition})
    assert cm.external_dataset_mode("bob", "corpus") == expected


def test_the_external_store_holds_only_external_accounts(monkeypatch):
    cm = _manager(users=_users(alice={"default": {"corpus": "allowed"},
                                      EXTERNAL_ZONE: {"corpus": "allowed"}}),
                  groups={}, datasets={"corpus": MANAGED})
    written = []

    def read(name, ns):
        raise ApiException(status=404)
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        read_namespaced_secret=read,
        create_namespaced_secret=lambda ns, body: written.append(
            body["stringData"])))
    keys = cm.sync_dataset_keys("corpus")
    assert set(keys) == {"rw.alice", "xrw.alice"}
    (data,) = written
    shared = json.loads(data["users.json"])["accessAccounts"]
    external = json.loads(data["users-ext.json"])["accessAccounts"]
    # The policy names external accounts too, so the shared store (its
    # writer's) must know them; the external listener knows nothing else.
    assert set(shared) == {"rw.alice", "xrw.alice"}
    assert set(external) == {"xrw.alice"}


# --- the reserved name ------------------------------------------------------- #

def test_no_zone_may_be_called_external():
    cm = _manager()
    assert cm.save_zone({"name": EXTERNAL_ZONE}) is False


def test_a_zone_cr_called_external_is_ignored():
    cm = _manager()
    cm.api = SimpleNamespace(list_namespaced_custom_object=lambda *a: {
        "items": [{"metadata": {"name": EXTERNAL_ZONE}, "spec": {}},
                  {"metadata": {"name": "lab"}, "spec": {}}]})
    cm.group, cm.version = "g", "v1"
    cm._load_legacy_default_zone = lambda: {}
    cm._load_zones()
    assert EXTERNAL_ZONE not in cm.zones and "lab" in cm.zones


@pytest.mark.parametrize("name, ok", [
    ("corpus", True), ("abc", True), ("ab", False), ("a", False),
    ("x" * 63, True), ("x" * 64, False), ("-ab", False)])
def test_a_managed_datasets_name_is_a_valid_bucket_name(name, ok):
    assert bool(cfg.MANAGED_DATASET_NAME.fullmatch(name)) == ok


# --- the way in from outside ------------------------------------------------- #

def test_the_ingress_routes_the_bucket_path_unrewritten():
    cm = _manager(dataset_external_tls_secret="s3-tls",
                  dataset_external_ingress_class="traefik",
                  dataset_external_annotations={"a": "b"})
    ing = cm._build_dataset_external_ingress("corpus")
    (rule,) = ing["spec"]["rules"]
    assert rule["host"] == "s3.example.org"
    (path,) = rule["http"]["paths"]
    # SigV4 signs the path, so it must reach the listener exactly as the
    # client sent it: /<bucket>/<key>, the bucket being the dataset's name.
    assert path["path"] == "/corpus" and path["pathType"] == "Prefix"
    assert path["backend"]["service"]["name"] == "whistler-dataset-corpus-ext"
    assert ing["spec"]["tls"] == [{"hosts": ["s3.example.org"],
                                   "secretName": "s3-tls"}]
    assert ing["spec"]["ingressClassName"] == "traefik"
    assert ing["metadata"]["annotations"] == {"a": "b"}
    assert "rewrite" not in json.dumps(ing).lower()


def test_only_the_ingress_controller_reaches_the_external_port():
    cm = _manager()
    policy = cm._build_dataset_external_access("corpus")
    (rule,) = policy["spec"]["ingress"]
    assert rule["from"] == [{"namespaceSelector": {}}]
    assert rule["ports"] == [{"port": 8082, "protocol": "TCP"}]


def _exposure_rig(monkeypatch, **attrs):
    cm = _manager(**attrs)
    order = []
    cm.bodies = {}
    cm._ensure_object = lambda name, ns, body, **kw: (
        order.append(("ensure", body["kind"])),
        cm.bodies.__setitem__(body["kind"], body)) and True

    def delete_mw(group, version, ns, plural, name):
        assert (group, version, plural) == cfg.TRAEFIK_MIDDLEWARE_API
        order.append(("delete", "Middleware"))
    monkeypatch.setattr(cfg.client, "CustomObjectsApi", lambda: SimpleNamespace(
        delete_namespaced_custom_object=delete_mw))
    monkeypatch.setattr(cfg.client, "NetworkingV1Api", lambda: SimpleNamespace(
        create_namespaced_network_policy=None, read_namespaced_network_policy=None,
        replace_namespaced_network_policy=None, create_namespaced_ingress=None,
        read_namespaced_ingress=None, replace_namespaced_ingress=None,
        delete_namespaced_ingress=lambda n, ns: order.append(("delete", "Ingress")),
        delete_namespaced_network_policy=lambda n, ns: order.append(
            ("delete", "NetworkPolicy"))))
    return cm, order


def test_opening_fences_first_and_closing_unroutes_first(monkeypatch):
    cm, order = _exposure_rig(monkeypatch)
    assert cm._ensure_external_exposure("corpus", True)
    # Unrestricted: a leftover allow-list goes, after the Ingress stops
    # naming it.
    assert order == [("ensure", "NetworkPolicy"), ("ensure", "Ingress"),
                     ("delete", "Middleware")]
    order.clear()
    assert cm._ensure_external_exposure("corpus", False)
    assert order == [("delete", "Ingress"), ("delete", "Middleware"),
                     ("delete", "NetworkPolicy")]


# --- where it may be reached from -------------------------------------------- #

def test_source_ranges_are_canonical():
    assert cfg.normalize_source_ranges(
        ["2001:db8::/32", " 10.1.2.3/8 ", "192.0.2.7", "", "10.0.0.0/8"]) == \
        ["10.0.0.0/8", "192.0.2.7/32", "2001:db8::/32"]
    with pytest.raises(ValueError, match="'lab'"):
        cfg.normalize_source_ranges(["10.0.0.0/8", "lab"])


@pytest.mark.parametrize("definition, expected", [
    (MANAGED, None),                                    # absent: anywhere
    ({**MANAGED, "externalSources": None}, None),
    ({**MANAGED, "externalSources": []}, []),           # empty: nowhere
    ({**MANAGED, "externalSources": ["192.0.2.1"]}, ["192.0.2.1/32"]),
    # Malformed (written past the portal): closed, never open.
    ({**MANAGED, "externalSources": ["nope"]}, []),
])
def test_external_sources_absent_and_empty_differ(definition, expected):
    assert KubeConfigManager.dataset_external_sources(definition) == expected


def test_a_dataset_with_a_bad_range_is_refused():
    problem = KubeConfigManager.dataset_spec_problem(
        {**MANAGED, "externalSources": ["10.0.0.0/33"]})
    assert problem and "10.0.0.0/33" in problem
    assert KubeConfigManager.dataset_spec_problem(
        {**MANAGED, "externalSources": []}) is None


def test_traefik_filters_through_a_per_dataset_allow_list(monkeypatch):
    cm, order = _exposure_rig(monkeypatch)
    assert cm._ensure_external_exposure("corpus", True, ["192.0.2.0/24"])
    # The allow-list exists before the Ingress that routes through it.
    assert order == [("ensure", "NetworkPolicy"), ("ensure", "Middleware"),
                     ("ensure", "Ingress")]
    mw = cm.bodies["Middleware"]
    assert mw["apiVersion"] == "traefik.io/v1alpha1"
    assert mw["spec"] == {"ipAllowList": {"sourceRange": ["192.0.2.0/24"]}}
    ann = cm.bodies["Ingress"]["metadata"]["annotations"]
    assert ann[cfg.TRAEFIK_MIDDLEWARES_ANNOTATION] == \
        "whistler-whistler-dataset-corpus-ext@kubernetescrd"


def test_the_allow_list_runs_before_the_charts_own_middlewares():
    cm = _manager(dataset_external_annotations={
        cfg.TRAEFIK_MIDDLEWARES_ANNOTATION: "kube-system-ratelimit@kubernetescrd"})
    ann = cm._build_dataset_external_ingress(
        "corpus", ["192.0.2.0/24"])["metadata"]["annotations"]
    assert ann[cfg.TRAEFIK_MIDDLEWARES_ANNOTATION] == (
        "whistler-whistler-dataset-corpus-ext@kubernetescrd,"
        "kube-system-ratelimit@kubernetescrd")


def test_nginx_filters_by_annotation_and_needs_no_middleware(monkeypatch):
    cm, order = _exposure_rig(monkeypatch,
                              dataset_external_source_filter="nginx")
    assert cm._ensure_external_exposure(
        "corpus", True, ["192.0.2.0/24", "2001:db8::/32"])
    assert order == [("ensure", "NetworkPolicy"), ("ensure", "Ingress")]
    ann = cm.bodies["Ingress"]["metadata"]["annotations"]
    assert ann[cfg.NGINX_SOURCE_RANGE_ANNOTATION] == \
        "192.0.2.0/24,2001:db8::/32"


def test_nowhere_fences_the_dataset_off(monkeypatch):
    cm, order = _exposure_rig(monkeypatch)
    assert cm._ensure_external_exposure("corpus", True, [])
    assert order == [("delete", "Ingress"), ("delete", "Middleware"),
                     ("delete", "NetworkPolicy")]


def test_a_restriction_nothing_enforces_keeps_the_dataset_closed(monkeypatch):
    cm, order = _exposure_rig(monkeypatch,
                              dataset_external_source_filter="none")
    assert cm._ensure_external_exposure("corpus", True, ["192.0.2.0/24"])
    assert order == [("delete", "Ingress"), ("delete", "NetworkPolicy")]
    # Unrestricted needs no filter at all.
    order.clear()
    assert cm._ensure_external_exposure("corpus", True, None)
    assert order == [("ensure", "NetworkPolicy"), ("ensure", "Ingress")]


# --- the account sync -------------------------------------------------------- #

def _b64(s):
    return base64.b64encode(s.encode()).decode()


def _sync_rig(monkeypatch, keys, live_shared, live_external, external=True):
    cm = _manager(external=external)
    calls = []
    cm.sync_dataset_keys = lambda v: keys
    cm._dataset_server_ip = lambda v: "10.0.0.9"
    cm._ensure_dataset_root_secret = lambda v: "root"
    cm.exposed = []
    cm.sources = []
    cm._ensure_external_exposure = lambda v, e, s=None: (
        cm.exposed.append(e), cm.sources.append(s)) and True
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        read_namespaced_secret=lambda n, ns: SimpleNamespace(data={
            "accessKeyId": _b64("whistler-root"),
            "secretAccessKey": _b64("r00t")})))
    stores = {"9081": live_shared, "9082": live_external}

    def call(method, url, access, secret, body=b"", headers=None):
        hostport, _, path = url.split("10.0.0.9:")[1].partition("/")
        calls.append((hostport, method, "/" + path, body))
        if path.startswith("list-users"):
            accts = "".join(f"<Accounts><Access>{a}</Access><Secret>{s}</Secret>"
                            f"</Accounts>" for a, s in stores[hostport].items())
            return 200, f"<ListUserAccountsResult>{accts}</ListUserAccountsResult>".encode()
        return 200, b""
    cm._vgw_call = staticmethod(call)
    return cm, calls


def test_an_internal_key_never_enters_the_external_store(monkeypatch):
    cm, calls = _sync_rig(monkeypatch,
                          keys={"rw.alice": "k1", "xro.alice": "k2"},
                          live_shared={}, live_external={})
    assert cm.sync_dataset_accounts("corpus")
    ext_created = [b for port, m, p, b in calls
                   if port == "9082" and p == "/create-user"]
    shared_created = [b for port, m, p, b in calls
                      if port == "9081" and p == "/create-user"]
    assert len(ext_created) == 1 and b"xro.alice" in ext_created[0]
    assert len(shared_created) == 2
    assert cm.exposed == [True]


def test_revoking_the_last_external_grant_closes_the_way_in(monkeypatch):
    cm, calls = _sync_rig(monkeypatch, keys={"rw.alice": "k1"},
                          live_shared={"rw.alice": "k1", "xro.alice": "k2"},
                          live_external={"xro.alice": "k2"})
    assert cm.sync_dataset_accounts("corpus")
    # The external store is reconciled first: a key usable from anywhere
    # dies before anything else happens.
    first_delete = next(c for c in calls if c[2].startswith("/delete-user"))
    assert first_delete[0] == "9082" and "xro.alice" in first_delete[2]
    assert cm.exposed == [False]


def test_external_access_off_never_calls_the_external_listener(monkeypatch):
    cm, calls = _sync_rig(monkeypatch, keys={"rw.alice": "k1"},
                          live_shared={"rw.alice": "k1"}, live_external={},
                          external=False)
    assert cm.sync_dataset_accounts("corpus")
    assert not [c for c in calls if c[0] == "9082"]
    assert cm.exposed == [False]


def test_the_policy_is_on_the_dataset_named_bucket(monkeypatch):
    cm, calls = _sync_rig(monkeypatch, keys={"xrw.bob": "k"},
                          live_shared={}, live_external={})
    assert cm.sync_dataset_accounts("corpus")
    (policy,) = [b for port, m, p, b in calls if p == "/corpus?policy"]
    statement, = json.loads(policy)["Statement"]
    assert statement["Principal"]["AWS"] == ["xrw.bob"]
    assert "s3:PutObject" in statement["Action"]


# --- credentials and regeneration ------------------------------------------- #

def test_a_user_sees_only_their_own_external_key(monkeypatch):
    cm = _manager(users=_users(bob={EXTERNAL_ZONE: {"corpus": "read-only"}}),
                  groups={}, datasets={"corpus": MANAGED})
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        read_namespaced_secret=lambda n, ns: SimpleNamespace(data={
            "xro.bob": _b64("bobs"), "xrw.carol": _b64("carols"),
            "rw.bob": _b64("internal")})))
    c = cm.get_external_credentials("bob", "corpus")
    assert c == {"endpoint": "https://s3.example.org", "bucket": "corpus",
                 "mode": "ro", "accessKeyId": "xro.bob",
                 "secretAccessKey": "bobs"}
    assert cm.get_external_credentials("carol", "corpus") is None


def test_regenerate_removes_the_key_guarded_by_version_and_wakes_the_operator(
        monkeypatch):
    cm = _manager(users=_users(bob={EXTERNAL_ZONE: {"corpus": "allowed"}}),
                  groups={}, datasets={"corpus": MANAGED},
                  group="g", version="v1")
    patches = []
    monkeypatch.setattr(cfg.client, "CoreV1Api", lambda: SimpleNamespace(
        read_namespaced_secret=lambda n, ns: SimpleNamespace(
            data={"xrw.bob": _b64("old")},
            metadata=SimpleNamespace(resource_version="42")),
        patch_namespaced_secret=lambda n, ns, body: patches.append(body)))
    touched = []
    cm.api = SimpleNamespace(patch_namespaced_custom_object=lambda *a:
                             touched.append(a[-1]))
    assert cm.regenerate_external_key("bob", "corpus")
    assert patches == [[{"op": "test", "path": "/metadata/resourceVersion",
                         "value": "42"},
                        {"op": "remove", "path": "/data/xrw.bob"}]]
    assert "whistler/keys-rotated" in touched[0]["metadata"]["annotations"]


def test_regenerate_refuses_a_user_without_external_access():
    cm = _manager(users=_users(bob={"default": {"corpus": "allowed"}}),
                  groups={}, datasets={"corpus": MANAGED})
    assert cm.regenerate_external_key("bob", "corpus") is False


# --- the guest side ------------------------------------------------------------- #

def test_a_guest_mounts_the_bucket_its_descriptor_names():
    ud = build_user_data(
        username="alice", uid=1000, ssh_keys=[], hostname="h",
        shared_datasets=[
            {"name": "corpus", "mode": "rw", "endpoint": "http://x:8080",
             "bucket": "corpus", "accessKeyId": "a", "secretAccessKey": "s"},
            {"name": "old", "mode": "ro", "endpoint": "http://y:8080",
             "accessKeyId": "a", "secretAccessKey": "s"}])
    assert "rclone mount corpus:corpus " in ud
    assert "rclone mount old:data " in ud      # an S3 proxy's fixed bucket


def test_the_sync_hands_the_datasets_sources_to_the_exposure(monkeypatch):
    cm, _calls = _sync_rig(monkeypatch, keys={"xro.alice": "k2"},
                           live_shared={}, live_external={})
    cm.datasets = {"corpus": {**MANAGED, "externalSources": ["192.0.2.9"]}}
    assert cm.sync_dataset_accounts("corpus")
    assert cm.exposed == [True] and cm.sources == [["192.0.2.9/32"]]


# --- the portal form --------------------------------------------------------- #

def test_the_form_answers_anywhere_listed_or_nowhere():
    from fastapi import HTTPException
    from whistler.portal.management import (_build_dataset_data,
                                            _parse_external_sources)
    assert _parse_external_sources("anywhere", "10.0.0.0/8") is None
    assert _parse_external_sources("nowhere", "10.0.0.0/8") == []
    assert _parse_external_sources("listed", "192.0.2.1, 10.0.0.0/8\n") == \
        ["10.0.0.0/8", "192.0.2.1/32"]
    for reach, text in (("listed", ""), ("listed", "lab"), ("open", "")):
        with pytest.raises(HTTPException):
            _parse_external_sources(reach, text)
    # An S3 dataset has no external listener, so nothing to restrict.
    s3 = _build_dataset_data("ref", "", "", "b", "", "", "", "", True,
                             source="s3", external_sources=[])
    assert s3["externalSources"] is None
    managed = _build_dataset_data("ref", "", "", "", "", "", "", "", True,
                                  source="managed", size="1Gi",
                                  external_sources=[])
    assert managed["externalSources"] == []
