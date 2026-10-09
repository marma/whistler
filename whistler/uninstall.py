"""What an uninstall does before Helm removes the chart (design/backup.md,
"Uninstall"). Run by the chart's ``pre-delete`` hook Job::

    python -m whistler.uninstall

Uninstall means uninstall: afterwards no Whistler namespace, CR or Secret is
left — only the users' disks and the backups, both on retained PVs the next
install re-attaches. In order:

1. **Retain** every PV behind a home, the archive and the backups, whatever
   the storage class says. If this fails the uninstall fails: it is the step
   that turns deleting a namespace from data loss into tidying up.
2. **A final backup** (trigger ``uninstall``). If it fails the uninstall fails
   too, unless ``WHISTLER_UNINSTALL_REQUIRE_FINAL_BACKUP=false``: without it,
   whatever changed since the last scheduled backup is lost unannounced.
3. **Delete the user and archive namespaces, and wait for them to go.** The
   waiting is not optional. Sessions carry a finalizer only the running
   operator removes, and the operator is deleted right after this hook; a
   hook that returned early would leave the namespaces Terminating forever.
4. **Delete what the operator made in the release namespace** and Helm does
   not own: the CRs it did not render, the SSH CA and gateway host key, the
   dataset credentials, the S3 proxies, the backup service's state. Helm's
   own objects are left to Helm.
5. **Delete the backup claim**, so its PV is Released for the next install.

Every step is idempotent: an uninstall that failed half-way is finished by
running it again. ``helm uninstall --no-hooks`` skips all of this; the next
install copes with whatever that left (design/backup.md, the idempotency
rules).
"""

import asyncio
import logging
import os
import sys
import time
from typing import List

from kubernetes import client
from kubernetes.client.rest import ApiException

from whistler import reattach

logger = logging.getLogger("whistler.uninstall")

HELM_MANAGED = ("app.kubernetes.io/managed-by", "Helm")


class UninstallError(Exception):
    pass


def _env_bool(name, default=True):
    return os.environ.get(name, str(default)).strip().lower() not in (
        "false", "0", "no", "off")


def _helm_managed(meta) -> bool:
    return (getattr(meta, "labels", None) or {}).get(HELM_MANAGED[0]) == \
        HELM_MANAGED[1]


def _whistler_namespaces(core) -> List[str]:
    from whistler.config import ARCHIVE_NS_LABEL, USER_NS_LABEL
    out = set()
    for selector in (USER_NS_LABEL, ARCHIVE_NS_LABEL):
        for ns in core.list_namespace(label_selector=selector).items:
            out.add(ns.metadata.name)
    return sorted(out)


def _claim_kind(namespace: str, name: str, archive_ns: str):
    from whistler.config import POD_HOME_PVC_PREFIX
    if namespace == archive_ns:
        return reattach.KIND_ARCHIVED
    if name.startswith(POD_HOME_PVC_PREFIX):
        return reattach.KIND_POD_HOME
    if name.startswith("whistler-home-"):
        return reattach.KIND_HOME
    return None


# --- 1. retain ---------------------------------------------------------------- #

def retain_user_data(cm) -> List[str]:
    """Retain the PV behind every home, archived home and the backup claim.
    Returns the PVs it saw. Anything else in a user namespace (a VM's root
    disk, say) is not user data this release re-attaches, so it is left to
    its class — retaining it would only leak an orphan PV per reinstall."""
    from whistler.config import ARCHIVE_NAMESPACE
    core = client.CoreV1Api()
    seen = []
    for ns in _whistler_namespaces(core):
        username = "" if ns == ARCHIVE_NAMESPACE else ns[len("whistler-user-"):]
        for pvc in core.list_namespaced_persistent_volume_claim(ns).items:
            kind = _claim_kind(ns, pvc.metadata.name, ARCHIVE_NAMESPACE)
            if not kind:
                logger.info(f"Not retaining {ns}/{pvc.metadata.name}: not a "
                            f"home, so nothing would re-attach it")
                continue
            pv = cm.secure_claim(ns, pvc.metadata.name, kind, username)
            if pv:
                seen.append(pv)
    claim = os.environ.get("WHISTLER_BACKUP_CLAIM")
    if claim:
        pv = cm.secure_claim(cm.namespace, claim, reattach.KIND_BACKUPS, "")
        if pv:
            seen.append(pv)
    for pvc in _dataset_claims(cm, core):
        pv = cm.secure_claim(cm.namespace, pvc.metadata.name,
                             reattach.KIND_DATASET, "")
        if pv:
            seen.append(pv)
    return seen


def _dataset_claims(cm, core):
    """Managed datasets' claims (config.ensure_managed_dataset), in the
    release namespace."""
    from whistler.config import DATASET_SERVER_APP
    return core.list_namespaced_persistent_volume_claim(
        cm.namespace, label_selector=f"app={DATASET_SERVER_APP}").items


# --- 2. the final backup -------------------------------------------------------- #

def final_backup(backups) -> str:
    result = asyncio.run(backups.create(by="uninstall hook",
                                        trigger="uninstall"))
    return result["backup"]["file"] if not result.get("skipped") \
        else result.get("unchangedSince")


# --- 3. namespaces ----------------------------------------------------------------- #

def delete_namespaces(timeout: float, poll: float = 3.0,
                      clock=time.monotonic, sleep=time.sleep) -> None:
    core = client.CoreV1Api()
    for ns in _whistler_namespaces(core):
        try:
            core.delete_namespace(ns)
            logger.info(f"Deleting namespace {ns}")
        except ApiException as e:
            if e.status != 404:
                raise
    deadline = clock() + timeout
    while True:
        remaining = _whistler_namespaces(core)
        if not remaining:
            return
        if clock() >= deadline:
            raise UninstallError(
                f"Namespaces still terminating after {int(timeout)}s: "
                f"{', '.join(remaining)}. The uninstall stops here so the "
                f"operator stays up to release their Sessions; run it again, "
                f"or raise whistler.uninstall.timeoutSeconds (and Helm's "
                f"--timeout with it). `kubectl get all -n {remaining[0]}` "
                f"shows what is holding them.")
        sleep(poll)


# --- 4. what the operator made in the release namespace ----------------------------- #

def delete_release_leftovers(cm) -> List[str]:
    from whistler.backup.export import RELEASE_KINDS
    ns = cm.namespace
    core, apps, net = (client.CoreV1Api(), client.AppsV1Api(),
                       client.NetworkingV1Api())
    deleted = []

    def gone(kind, name, fn):
        try:
            fn()
            deleted.append(f"{kind}/{name}")
        except ApiException as e:
            if e.status != 404:
                raise

    for kind, plural in RELEASE_KINDS:
        try:
            items = cm.api.list_namespaced_custom_object(
                cm.group, cm.version, ns, plural).get("items", [])
        except ApiException as e:
            if e.status == 404:
                continue
            raise
        for item in items:
            labels = (item.get("metadata") or {}).get("labels") or {}
            if labels.get(HELM_MANAGED[0]) == HELM_MANAGED[1]:
                continue
            name = item["metadata"]["name"]
            gone(kind, name, lambda: cm.api.delete_namespaced_custom_object(
                cm.group, cm.version, ns, plural, name))

    # The S3 proxies (config.ensure_s3_proxy): Deployment, Service, policy.
    selector = "app=whistler-s3-proxy"
    for d in apps.list_namespaced_deployment(ns, label_selector=selector).items:
        gone("Deployment", d.metadata.name,
             lambda: apps.delete_namespaced_deployment(d.metadata.name, ns))
    for s in core.list_namespaced_service(ns, label_selector=selector).items:
        gone("Service", s.metadata.name,
             lambda: core.delete_namespaced_service(s.metadata.name, ns))
    for p in net.list_namespaced_network_policy(ns,
                                                label_selector=selector).items:
        gone("NetworkPolicy", p.metadata.name,
             lambda: net.delete_namespaced_network_policy(p.metadata.name, ns))

    # Managed dataset servers: Deployment and Services. Their policies go with
    # the proxies' above (the per-mode ones carry the proxy label) and here
    # (the deny-all one), after the Deployment, so no pod outlives its fence.
    from whistler.config import DATASET_SERVER_APP
    selector = f"app={DATASET_SERVER_APP}"
    for d in apps.list_namespaced_deployment(ns, label_selector=selector).items:
        gone("Deployment", d.metadata.name,
             lambda: apps.delete_namespaced_deployment(d.metadata.name, ns))
    for s in core.list_namespaced_service(ns, label_selector=selector).items:
        gone("Service", s.metadata.name,
             lambda: core.delete_namespaced_service(s.metadata.name, ns))
    for p in net.list_namespaced_network_policy(ns,
                                                label_selector=selector).items:
        gone("NetworkPolicy", p.metadata.name,
             lambda: net.delete_namespaced_network_policy(p.metadata.name, ns))
    # Their external paths (config._ensure_external_exposure).
    for i in net.list_namespaced_ingress(ns, label_selector=selector).items:
        gone("Ingress", i.metadata.name,
             lambda: net.delete_namespaced_ingress(i.metadata.name, ns))
    # ...and their Traefik IP allow-lists, where Traefik is what filters.
    from whistler.config import TRAEFIK_MIDDLEWARE_API
    group, version, plural = TRAEFIK_MIDDLEWARE_API
    custom = cm.api
    try:
        middlewares = custom.list_namespaced_custom_object(
            group, version, ns, plural, label_selector=selector)["items"]
    except ApiException as e:
        if e.status != 404:   # 404: no Traefik CRDs, nothing to remove
            raise
        middlewares = []
    for m in middlewares:
        name = m["metadata"]["name"]
        gone("Middleware", name,
             lambda: custom.delete_namespaced_custom_object(
                 group, version, ns, plural, name))

    # Secrets: by label where Whistler labels them, by name where it names
    # them (the CA and host key names come from the chart).
    for label in ("app=whistler-dataset", "app=whistler-s3-proxy",
                  f"app={DATASET_SERVER_APP}", "app=whistler-backup"):
        for s in core.list_namespaced_secret(ns, label_selector=label).items:
            if not _helm_managed(s.metadata):
                gone("Secret", s.metadata.name,
                     lambda: core.delete_namespaced_secret(s.metadata.name, ns))
    for name in (cm.ssh_ca_secret_name,
                 os.environ.get("WHISTLER_HOST_KEY_SECRET_NAME")):
        if name:
            gone("Secret", name,
                 lambda: core.delete_namespaced_secret(name, ns))
    for c in core.list_namespaced_config_map(
            ns, label_selector="app=whistler-backup").items:
        if not _helm_managed(c.metadata):
            gone("ConfigMap", c.metadata.name,
                 lambda: core.delete_namespaced_config_map(c.metadata.name, ns))
    return deleted


# --- 5. the backup claim ---------------------------------------------------------- #

def release_backup_claim(cm) -> None:
    """Delete it: its PV (Retain) goes Released once the backup pod, which
    Helm deletes next, lets go of it, and the next install re-binds it. The
    managed datasets' claims go the same way, re-bound by the next install
    when their Datasets come back (config._ensure_dataset_claim)."""
    core = client.CoreV1Api()
    claims = [pvc.metadata.name for pvc in _dataset_claims(cm, core)]
    if os.environ.get("WHISTLER_BACKUP_CLAIM"):
        claims.append(os.environ["WHISTLER_BACKUP_CLAIM"])
    for claim in claims:
        try:
            core.delete_namespaced_persistent_volume_claim(claim, cm.namespace)
        except ApiException as e:
            if e.status != 404:
                raise


def run(cm, backups, *, require_final_backup=True, timeout=240.0) -> None:
    pvs = retain_user_data(cm)
    logger.info(f"1/5 Retained {len(pvs)} PV(s)")

    if backups is not None and backups.enabled:
        try:
            name = final_backup(backups)
            logger.info(f"2/5 Final backup: {name}")
        except Exception as e:
            if require_final_backup:
                raise UninstallError(
                    f"The final backup failed ({e}). Nothing has been deleted. "
                    f"Fix the backup service, or set whistler.uninstall."
                    f"requireFinalBackup=false to uninstall without it — "
                    f"state changed since the last backup is then lost.")
            logger.warning(f"2/5 Final backup failed, continuing as "
                           f"configured: {e}")
    else:
        logger.info("2/5 Backups are off; no final backup")

    delete_namespaces(timeout)
    logger.info("3/5 User and archive namespaces deleted")
    deleted = delete_release_leftovers(cm)
    logger.info(f"4/5 Deleted {len(deleted)} object(s) from {cm.namespace}: "
                f"{', '.join(deleted) or 'none'}")
    release_backup_claim(cm)
    logger.info("5/5 Backup and dataset claims released; their PVs wait "
                "for the next install")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s",
                        stream=sys.stderr)
    from whistler.logsetup import quiet_chatty_libraries
    quiet_chatty_libraries("INFO")
    from whistler.config import KubeConfigManager
    from whistler.portal.backups import BackupClient
    try:
        run(KubeConfigManager(), BackupClient(),
            require_final_backup=_env_bool(
                "WHISTLER_UNINSTALL_REQUIRE_FINAL_BACKUP", True),
            timeout=float(os.environ.get("WHISTLER_UNINSTALL_TIMEOUT", "240")))
    except (UninstallError, ApiException) as e:
        logger.error(f"Uninstall stopped: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
