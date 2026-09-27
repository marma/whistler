"""An in-memory fake of the Kubernetes APIs the backup code uses, shared by
the backup and backup-service tests."""
import copy
from types import SimpleNamespace

from kubernetes.client.rest import ApiException

G, V = "whistler.martinmalmsten.net", "v1"
KIND_OF = {"zones": "Zone", "groups": "Group", "users": "User",
           "templates": "Template", "datasets": "Dataset",
           "homevolumes": "HomeVolume", "sessions": "Session"}


# --- a fake cluster ----------------------------------------------------------- #

class FakeCluster:
    """CRs keyed (plural, ns, name); Secrets keyed (ns, name)."""

    def __init__(self, crds=tuple(KIND_OF), prune=None):
        self.crs, self.secrets, self.namespaces = {}, {}, set()
        self.crds = set(crds)
        self.prune = prune or {}     # plural -> spec keys the "CRD" drops
        self._rv = 0

    def _stamp(self, obj):
        self._rv += 1
        obj["metadata"]["resourceVersion"] = str(self._rv)
        obj["metadata"].setdefault("uid", f"uid-{self._rv}")
        obj["metadata"].setdefault("managedFields", [{"manager": "x"}])
        return obj

    def put(self, plural, ns, name, spec, **meta):
        obj = {"apiVersion": f"{G}/{V}", "kind": KIND_OF[plural],
               "metadata": {"name": name, "namespace": ns, **meta},
               "spec": spec, "status": {"phase": "whatever"}}
        self.crs[(plural, ns, name)] = self._stamp(obj)

    def put_secret(self, ns, name, data, labels=None):
        self.secrets[(ns, name)] = self._stamp(
            {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
             "metadata": {"name": name, "namespace": ns,
                          "labels": labels or {}}, "data": data})

    # CustomObjectsApi
    def _crd(self, plural):
        if plural not in self.crds:
            raise ApiException(status=404, reason="Not Found")

    def list_namespaced_custom_object(self, g, v, ns, plural, limit=None):
        self._crd(plural)
        return {"items": [copy.deepcopy(o) for (p, n, _), o in
                          sorted(self.crs.items()) if p == plural and n == ns]}

    def list_cluster_custom_object(self, g, v, plural):
        self._crd(plural)
        return {"items": [copy.deepcopy(o) for (p, _, _), o in
                          sorted(self.crs.items()) if p == plural]}

    def get_namespaced_custom_object(self, g, v, ns, plural, name):
        self._crd(plural)
        try:
            return copy.deepcopy(self.crs[(plural, ns, name)])
        except KeyError:
            raise ApiException(status=404, reason="Not Found")

    def _write(self, plural, ns, body):
        body = copy.deepcopy(body)
        for key in self.prune.get(plural, ()):
            body.get("spec", {}).pop(key, None)
        body["metadata"]["namespace"] = ns
        self.crs[(plural, ns, body["metadata"]["name"])] = self._stamp(body)

    def create_namespaced_custom_object(self, g, v, ns, plural, body):
        self._crd(plural)
        if (plural, ns, body["metadata"]["name"]) in self.crs:
            raise ApiException(status=409, reason="Conflict")
        self._write(plural, ns, body)

    def replace_namespaced_custom_object(self, g, v, ns, plural, name, body):
        live = self.crs[(plural, ns, name)]
        if body["metadata"].get("resourceVersion") != \
                live["metadata"]["resourceVersion"]:
            raise ApiException(status=409, reason="Conflict")
        self._write(plural, ns, body)

    # CoreV1Api (returns plain dicts: sanitize_for_serialization passes them)
    def read_namespaced_secret(self, name, ns):
        try:
            return copy.deepcopy(self.secrets[(ns, name)])
        except KeyError:
            raise ApiException(status=404, reason="Not Found")

    def list_namespaced_secret(self, ns, label_selector=None):
        key, _, value = (label_selector or "").partition("=")
        return SimpleNamespace(items=[
            copy.deepcopy(s) for (n, _), s in sorted(self.secrets.items())
            if n == ns and (not key or s["metadata"].get("labels", {}).get(key)
                            == value)])

    def create_namespaced_secret(self, ns, body):
        self.put_secret(ns, body["metadata"]["name"], body.get("data"),
                        body["metadata"].get("labels"))
        self.secrets[(ns, body["metadata"]["name"])]["metadata"][
            "annotations"] = body["metadata"].get("annotations") or {}

    def replace_namespaced_secret(self, name, ns, body):
        self.create_namespaced_secret(ns, body)


class FakeCM:
    def __init__(self, cluster, namespace="whistler", release="whistler"):
        self.api, self.group, self.version = cluster, G, V
        self.namespace = namespace
        self.ssh_ca_secret_name = f"{release}-ssh-ca"
        self.ssh_domain_suffix = ".w"
        self.cluster = cluster

    def dataset_credentials_secret_name(self, name):
        return f"whistler-dataset-{name}-creds"

    def _load_zones(self):
        pass

    def _ensure_user_namespace(self, username):
        self.cluster.namespaces.add(f"whistler-user-{username}")

    def _ensure_archive_namespace(self):
        self.cluster.namespaces.add("whistler-archive")


HELM = {"app.kubernetes.io/managed-by": "Helm"}


def _populated():
    c = FakeCluster()
    c.put("zones", "whistler", "restricted", {"dns": {"clusterOnly": True}})
    c.put("zones", "whistler", "open", {}, labels=HELM)          # from values
    c.put("users", "whistler", "alice",
          {"uid": 1234, "entryPoints": ["portal"], "allowedZones": ["open"],
           "volumeAccess": {"open": {"desk": "allowed"}}})
    c.put("groups", "whistler", "lab", {"members": ["alice"]})
    c.put("templates", "whistler", "devbase", {"image": "x"}, labels=HELM)
    c.put("templates", "whistler", "custom", {"image": "y"},
          annotations={"kopf.zalando.org/last-handled-configuration": "{}"})
    c.put("datasets", "whistler", "corpus", {"bucket": "b"})
    c.put("homevolumes", "whistler-user-alice", "desk",
          {"user": "alice", "pvName": "pv-1", "size": "20Gi"})
    c.put("homevolumes", "whistler-user-alice", "going", {"user": "alice"},
          annotations={"whistler/delete-data": "true"})
    c.put("homevolumes", "whistler-archive", "bob-old-20260101",
          {"pvName": "pv-9", "archived": {"user": "bob", "name": "old"}})
    c.put("sessions", "whistler-user-alice", "alice-box",
          {"templateRef": "custom", "homeVolume": "desk",
           "runOverrides": {"gpuType": "A100"}},
          annotations={"whistler/last-connect": "1790000000.0"})
    c.put("sessions", "kube-system", "stray", {})                 # not ours
    c.put_secret("whistler", "whistler-ssh-ca", {"ca_key": "Q0E="})
    c.put_secret("whistler", "whistler-server-host-key", {"host_key": "SEs="})
    c.put_secret("whistler", "whistler-dataset-corpus-creds",
                 {"accessKeyId": "QQ=="},
                 labels={"app": "whistler-dataset", "dataset": "corpus"})
    c.put_secret("whistler", "unrelated", {"x": "eA=="})
    return c
