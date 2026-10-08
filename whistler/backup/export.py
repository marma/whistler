"""What goes into a backup, read from the cluster (design/backup.md, "What
is in it"). The decisions about an object's shape are in ``archive``.

In it:
  release namespace   Users, and the Groups, Zones, Templates and Datasets
                      that Helm did not render (those are in values, in git)
  user namespaces     HomeVolumes (with the PV each one's data is on) and
                      Sessions, without their run state
  archive namespace   the archived HomeVolume records
  Secrets (opt-out)   the SSH CA, the gateway host key, Whistler-managed
                      dataset credentials

Not in it: anything the operator derives (pods, VMs, Services,
NetworkPolicies, cloud-init and host-cert Secrets, VM access keys, S3 proxy
auth), Secrets an admin created and named with `credentialsSecret`, and user
data itself.
"""

import datetime
import logging
import os
from typing import Any, Dict, List, Tuple

from kubernetes import client
from kubernetes.client.rest import ApiException

from whistler.backup import BackupError, archive
from whistler.config import (ARCHIVE_NAMESPACE, DATASET_PLURAL,
                             DELETE_DATA_ANNOTATION, GROUP_PLURAL,
                             HOME_VOLUME_PLURAL, SESSION_PLURAL,
                             TEMPLATE_PLURAL, USER_PLURAL, ZONE_PLURAL,
                             crd_missing_hint)

logger = logging.getLogger("whistler.backup")

USER_NS_PREFIX = "whistler-user-"

# Release-namespace kinds, in restore order.
RELEASE_KINDS = (("Zone", ZONE_PLURAL), ("Group", GROUP_PLURAL),
                 ("User", USER_PLURAL), ("Template", TEMPLATE_PLURAL),
                 ("Dataset", DATASET_PLURAL))
# Kinds that live in user namespaces (and, for HomeVolume, the archive).
SPREAD_KINDS = (("HomeVolume", HOME_VOLUME_PLURAL),
                ("Session", SESSION_PLURAL))
PLURALS = dict(RELEASE_KINDS + SPREAD_KINDS)


def whistler_version() -> str:
    try:
        from importlib.metadata import version
        return version("whistler")
    except Exception:
        return "unknown"


def host_key_secret_name() -> str:
    """The gateway's host key Secret, as the gateway itself names it
    (WHISTLER_HOST_KEY_SECRET_NAME, `<release>-server-host-key`)."""
    return os.environ.get("WHISTLER_HOST_KEY_SECRET_NAME", "")


def _as_dict(obj) -> Dict[str, Any]:
    return client.ApiClient().sanitize_for_serialization(obj)


def _wanted_namespace(ns: str) -> bool:
    return bool(ns) and (ns.startswith(USER_NS_PREFIX) or ns == ARCHIVE_NAMESPACE)


def collect(cm, include_secrets: bool = True
            ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    """``(objects, secrets, warnings)``, normalized."""
    api, group, version = cm.api, cm.group, cm.version
    release_ns = cm.namespace
    objects: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for kind, plural in RELEASE_KINDS:
        try:
            items = api.list_namespaced_custom_object(
                group, version, release_ns, plural).get("items", [])
        except ApiException as e:
            raise BackupError(f"Could not read {plural}: "
                              f"{crd_missing_hint(plural, e)}")
        for item in items:
            item.setdefault("kind", kind)
            if archive.is_helm_managed(item):
                continue
            # A Dataset being deleted for good, as for homes: restoring it
            # would bring back a record for data that is about to be gone.
            if DELETE_DATA_ANNOTATION in (
                    (item.get("metadata") or {}).get("annotations") or {}):
                continue
            objects.append(archive.normalize(item, release_ns))

    for kind, plural in SPREAD_KINDS:
        try:
            items = api.list_cluster_custom_object(
                group, version, plural).get("items", [])
        except ApiException as e:
            raise BackupError(f"Could not read {plural}: "
                              f"{crd_missing_hint(plural, e)}")
        for item in items:
            item.setdefault("kind", kind)
            meta = item.get("metadata") or {}
            if not _wanted_namespace(meta.get("namespace")):
                continue
            # Being deleted with its data: restoring it would bring back a
            # record for a disk that is about to be gone.
            if DELETE_DATA_ANNOTATION in (meta.get("annotations") or {}):
                continue
            objects.append(archive.normalize(item, release_ns))

    secrets: List[Dict[str, Any]] = []
    if include_secrets:
        secrets, secret_warnings = _collect_secrets(cm)
        warnings += secret_warnings
    return objects, secrets, warnings


def _collect_secrets(cm) -> Tuple[List[Dict[str, Any]], List[str]]:
    core = client.CoreV1Api()
    release_ns = cm.namespace
    out, warnings = [], []

    def one(name, role, why_missing):
        if not name:
            warnings.append(why_missing)
            return
        try:
            secret = _as_dict(core.read_namespaced_secret(name, release_ns))
        except ApiException as e:
            if e.status == 404:
                warnings.append(f"{why_missing} (Secret {name} not found)")
                return
            raise
        out.append(_with_role(secret, role, release_ns))

    one(cm.ssh_ca_secret_name, archive.ROLE_SSH_CA,
        "No SSH CA to back up; restored sessions will present a new CA")
    one(host_key_secret_name(), archive.ROLE_SERVER_HOST_KEY,
        "The gateway host key is not backed up (WHISTLER_HOST_KEY_SECRET_NAME "
        "is not set here); a restore will give the gateway a new host key")
    try:
        items = core.list_namespaced_secret(
            release_ns, label_selector="app=whistler-dataset").items
    except ApiException as e:
        raise BackupError(f"Could not read dataset credentials: {e.reason}")
    for secret in items:
        out.append(_with_role(_as_dict(secret),
                              archive.ROLE_DATASET_CREDENTIALS, release_ns))
    return out, warnings


def _with_role(secret: Dict[str, Any], role: str,
               release_ns: str) -> Dict[str, Any]:
    secret.setdefault("apiVersion", "v1")
    secret.setdefault("kind", "Secret")
    out = archive.normalize(secret, release_ns)
    out["metadata"].setdefault("annotations", {})[archive.ROLE_ANNOTATION] = role
    return out


def export(cm, *, include_secrets: bool = True, passphrase: str = None,
           trigger: str = "manual", created: datetime.datetime = None,
           install_id: str = None
           ) -> Tuple[bytes, Dict[str, Any], List[str]]:
    """A complete backup of ``cm``'s cluster: ``(file, manifest, warnings)``."""
    objects, secrets, warnings = collect(cm, include_secrets)
    data, manifest = archive.build(
        objects, secrets, passphrase=passphrase, trigger=trigger,
        created=created, extra={
            "whistlerVersion": whistler_version(),
            "releaseNamespace": cm.namespace,
            "archiveNamespace": ARCHIVE_NAMESPACE,
            "sshDomainSuffix": getattr(cm, "ssh_domain_suffix", None),
            # Which install made it: retention and the first-login offer key
            # on it (backup/schedule.py). None from the CLI.
            "installId": install_id,
        })
    if secrets and not passphrase:
        warnings.append("Secrets are stored unencrypted (no passphrase set): "
                        "this file holds the SSH CA's private key")
    return data, manifest, warnings
