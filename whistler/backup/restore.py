"""Putting a backup back (design/backup.md, "Restore").

    plan()     verify the backup against this cluster and say, per object,
               create / replace / unchanged / skip. Writes nothing.
    apply()    carry out a plan, in restore order, ensuring namespaces.
    readback() compare what the cluster kept with what was written: a field
               an older CRD pruned is exactly the silent failure a restore
               would otherwise hide.

**Restore never deletes.** Objects the backup does not know about are left
alone: deleting "what the backup does not know" would make restoring an old
backup destroy everything made since.

**Idempotent.** A replace writes the backup's labels, annotations and spec,
and keeps exactly what ``archive.normalize`` drops from the live object —
kopf's bookkeeping, a running Session's run state — and nothing else. So the
live object normalizes to the backup's afterwards, and the same backup
restored twice is all *unchanged* the second time. That is also why a
restore does not stop a Session that is running.
"""

import logging
from typing import Any, Dict, List, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException

from whistler.backup import BackupError, archive
from whistler.backup.export import (PLURALS, USER_NS_PREFIX,
                                    host_key_secret_name)
from whistler.config import ARCHIVE_NAMESPACE, crd_missing_hint

logger = logging.getLogger("whistler.backup")

CREATE, REPLACE, UNCHANGED, SKIP = "create", "replace", "unchanged", "skip"


def _as_dict(obj) -> Dict[str, Any]:
    return client.ApiClient().sanitize_for_serialization(obj)


class Entry:
    def __init__(self, action, obj, namespace, live=None, reason=None):
        self.action = action
        self.obj = obj                 # normalized, from the backup
        self.namespace = namespace     # where it goes in THIS cluster
        self.live = live               # the object as it is now, or None
        self.reason = reason

    @property
    def kind(self):
        return self.obj["kind"]

    @property
    def name(self):
        return self.obj["metadata"]["name"]

    def as_dict(self):
        return {"action": self.action, "kind": self.kind,
                "namespace": self.namespace, "name": self.name,
                "reason": self.reason}


class Plan:
    def __init__(self, entries, warnings):
        self.entries: List[Entry] = entries
        self.warnings: List[str] = warnings

    def counts(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for e in self.entries:
            out[e.action] = out.get(e.action, 0) + 1
        return out


# --- where things go ---------------------------------------------------------- #

def target_namespace(obj, cm, manifest) -> str:
    ns = (obj.get("metadata") or {}).get("namespace")
    if not ns:
        return cm.namespace
    if ns == manifest.get("archiveNamespace"):
        return ARCHIVE_NAMESPACE
    return ns


def secret_target_name(secret, cm) -> Optional[str]:
    """This install's name for a backed-up Secret's role."""
    meta = secret.get("metadata") or {}
    role = (meta.get("annotations") or {}).get(archive.ROLE_ANNOTATION)
    if role == archive.ROLE_SSH_CA:
        return cm.ssh_ca_secret_name
    if role == archive.ROLE_SERVER_HOST_KEY:
        return host_key_secret_name() or None
    if role == archive.ROLE_DATASET_CREDENTIALS:
        dataset = (meta.get("labels") or {}).get("dataset")
        return cm.dataset_credentials_secret_name(dataset) if dataset \
            else meta.get("name")
    return None


def _strip_role(obj):
    out = {**obj, "metadata": dict(obj["metadata"])}
    annotations = {k: v for k, v in (out["metadata"].get("annotations") or {})
                   .items() if k != archive.ROLE_ANNOTATION}
    if annotations:
        out["metadata"]["annotations"] = annotations
    else:
        out["metadata"].pop("annotations", None)
    return out


# --- reading this cluster --------------------------------------------------------- #

def _fetch(cm, kind, namespace, name) -> Optional[Dict[str, Any]]:
    try:
        if kind == "Secret":
            return _as_dict(client.CoreV1Api().read_namespaced_secret(
                name, namespace))
        obj = cm.api.get_namespaced_custom_object(
            cm.group, cm.version, namespace, PLURALS[kind], name)
        obj.setdefault("kind", kind)
        return obj
    except ApiException as e:
        if e.status == 404:
            return None
        raise


def check_crds(cm, kinds) -> None:
    """Refuse a restore the cluster cannot hold at all. A partial restore is
    harder to reason about than none."""
    for kind in sorted(kinds):
        plural = PLURALS.get(kind)
        if not plural:
            raise BackupError(f"The backup holds {kind} objects, which this "
                              f"Whistler does not know.")
        try:
            cm.api.list_namespaced_custom_object(
                cm.group, cm.version, cm.namespace, plural, limit=1)
        except ApiException as e:
            raise BackupError(f"Cannot restore {kind}: "
                              f"{crd_missing_hint(plural, e)}")


def _decide(obj, namespace, live, release_ns) -> Entry:
    if live is None:
        return Entry(CREATE, obj, namespace)
    if archive.is_helm_managed(live):
        return Entry(SKIP, obj, namespace, live,
                     "managed by Helm here; its values decide it")
    if archive.content(archive.normalize(live, release_ns)) == \
            archive.content(obj):
        return Entry(UNCHANGED, obj, namespace, live)
    return Entry(REPLACE, obj, namespace, live)


def plan(cm, backup: archive.Backup, include_secrets: bool = True) -> Plan:
    manifest = backup.manifest
    warnings: List[str] = []
    check_crds(cm, {o["kind"] for o in backup.objects})

    entries: List[Entry] = []
    for obj in sorted(backup.objects, key=archive._sort_key):
        ns = target_namespace(obj, cm, manifest)
        live = _fetch(cm, obj["kind"], ns, obj["metadata"]["name"])
        entries.append(_decide(obj, ns, live, cm.namespace))

    if include_secrets and backup.secrets is None:
        warnings.append("The backup's secrets are encrypted and no passphrase "
                        "was given: SSH CA, gateway host key and dataset "
                        "credentials are NOT restored.")
    for secret in (backup.secrets or []) if include_secrets else []:
        name = secret_target_name(secret, cm)
        if not name:
            warnings.append(
                f"Not restoring Secret {secret['metadata']['name']}: this "
                f"install has no name for its role (for the gateway host key, "
                f"WHISTLER_HOST_KEY_SECRET_NAME is not set here).")
            continue
        body = _strip_role(secret)
        body["metadata"]["name"] = name
        live = _fetch(cm, "Secret", cm.namespace, name)
        entry = _decide(body, cm.namespace, live, cm.namespace)
        entries.append(entry)

    # Datasets whose credential an admin keeps elsewhere: not ours to back
    # up, so say when it is missing rather than let the proxy fail later.
    for obj in backup.objects:
        ref = (obj.get("spec") or {}).get("credentialsSecret") \
            if obj["kind"] == "Dataset" else None
        if ref and _fetch(cm, "Secret", cm.namespace, ref) is None:
            warnings.append(f"Dataset {obj['metadata']['name']} names Secret "
                            f"{ref}, which is not in this cluster and not in "
                            f"the backup; create it before using the dataset.")
    suffix = manifest.get("sshDomainSuffix")
    here = getattr(cm, "ssh_domain_suffix", None)
    if suffix and here and suffix != here:
        warnings.append(f"The backup was made with SSH suffix {suffix!r} and "
                        f"this install uses {here!r}; host certificates are "
                        f"reissued for the new one.")
    return Plan(entries, warnings)


# --- writing ------------------------------------------------------------------------ #

def _write_order(entry: Entry):
    # Secrets after Datasets and before anything in a user namespace.
    order = list(archive.KIND_ORDER)
    order.insert(order.index("HomeVolume"), "Secret")
    return order.index(entry.kind) if entry.kind in order else len(order)


def _body(entry: Entry) -> Dict[str, Any]:
    """What to write: the backup's object, plus what the live one has that a
    backup never carries (see the module docstring)."""
    obj = entry.obj
    meta = {"name": entry.name, "namespace": entry.namespace,
            "labels": dict(obj["metadata"].get("labels") or {}),
            "annotations": dict(obj["metadata"].get("annotations") or {})}
    body = {"apiVersion": obj.get("apiVersion"), "kind": obj["kind"],
            "metadata": meta}
    if obj["kind"] == "Secret":
        body["type"] = obj.get("type") or "Opaque"
        body["data"] = dict(obj.get("data") or {})
    else:
        body["spec"] = dict(obj.get("spec") or {})
    live = entry.live
    if live:
        lmeta = live.get("metadata") or {}
        meta["resourceVersion"] = lmeta.get("resourceVersion")
        kept = archive.normalize(live)["metadata"].get("annotations") or {}
        for k, v in (lmeta.get("annotations") or {}).items():
            if k not in kept:        # dropped by normalize: the live object's
                meta["annotations"].setdefault(k, v)
        if obj["kind"] == "Session":
            for key in archive.SESSION_RUN_SPEC:
                if key in (live.get("spec") or {}):
                    body["spec"][key] = live["spec"][key]
    if not meta["labels"]:
        meta.pop("labels")
    if not meta["annotations"]:
        meta.pop("annotations")
    return body


def _ensure_namespace(cm, namespace, done):
    if namespace in done or namespace == cm.namespace:
        return
    if namespace == ARCHIVE_NAMESPACE:
        cm._ensure_archive_namespace()
    elif namespace.startswith(USER_NS_PREFIX):
        # The zones just restored decide the namespace's policies.
        if "zones-loaded" not in done:
            cm._load_zones()
            done.add("zones-loaded")
        cm._ensure_user_namespace(namespace[len(USER_NS_PREFIX):])
    done.add(namespace)


def apply(cm, the_plan: Plan) -> List[Entry]:
    """Carry out the plan's creates and replaces. Returns what was written.
    Stops at the first failure, which is reported with what it was doing."""
    written = []
    namespaces: set = set()
    core = client.CoreV1Api()
    for entry in sorted(the_plan.entries, key=_write_order):
        if entry.action not in (CREATE, REPLACE):
            continue
        _ensure_namespace(cm, entry.namespace, namespaces)
        body = _body(entry)
        try:
            if entry.kind == "Secret":
                if entry.action == CREATE:
                    core.create_namespaced_secret(entry.namespace, body)
                else:
                    core.replace_namespaced_secret(entry.name, entry.namespace,
                                                   body)
            else:
                plural = PLURALS[entry.kind]
                if entry.action == CREATE:
                    cm.api.create_namespaced_custom_object(
                        cm.group, cm.version, entry.namespace, plural, body)
                else:
                    cm.api.replace_namespaced_custom_object(
                        cm.group, cm.version, entry.namespace, plural,
                        entry.name, body)
        except ApiException as e:
            raise BackupError(
                f"Restore stopped at {entry.kind} {entry.namespace}/"
                f"{entry.name} ({entry.action}): {e.status} {e.reason}. "
                f"{len(written)} object(s) were written before it.")
        logger.info(f"Restored {entry.kind} {entry.namespace}/{entry.name} "
                    f"({entry.action})")
        written.append(entry)
    return written


def _missing_paths(want, have, path=""):
    """Leaves of ``want`` that ``have`` lacks or holds differently."""
    if isinstance(want, dict):
        if not isinstance(have, dict):
            return [path or "."]
        out = []
        for k, v in want.items():
            out += _missing_paths(v, have.get(k), f"{path}.{k}" if path else k)
        return out
    return [] if want == have else [path or "."]


def readback(cm, written: List[Entry]) -> List[str]:
    """Fields the cluster did not keep, as ``Kind ns/name: path`` lines. On a
    CRD older than the backup these are silently pruned on write."""
    problems = []
    for entry in written:
        if entry.kind == "Secret":
            continue
        live = _fetch(cm, entry.kind, entry.namespace, entry.name)
        if live is None:
            problems.append(f"{entry.kind} {entry.namespace}/{entry.name}: "
                            f"not there after writing it")
            continue
        for path in _missing_paths(entry.obj.get("spec") or {},
                                   live.get("spec") or {}):
            problems.append(f"{entry.kind} {entry.namespace}/{entry.name}: "
                            f"spec.{path}")
    return problems


PRUNED_HINT = ("These fields were dropped by the cluster, which usually means "
               "its CRDs are older than the backup: run `kubectl apply -f "
               "charts/whistler/crds/crds.yaml` and restore again.")
