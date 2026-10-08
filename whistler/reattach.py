"""Keeping user data across an uninstall, and giving it back afterwards.

Pure (no Kubernetes calls): decisions over PersistentVolumes as plain dicts,
the camelCase shape the API serves, so they are unit-testable like
``hostca`` and ``KubeConfigManager._build_pod_spec``. The calls live in
``KubeConfigManager`` (secure_claim, reattach_claim).

An uninstall deletes namespaces, and deleting a namespace deletes its
claims. Whether the data survives then depends only on the PV's reclaim
policy, and dynamically provisioned PVs usually default to ``Delete``. So
the operator sets ``Retain`` on every PV behind a user's home as soon as it
is bound, whatever the storage class says (design/backup.md, Phase 1). The
PV is cluster-scoped and outlives its claim, sitting ``Released`` with a
``claimRef`` that still names the claim it belonged to. That stale
reference is what finds it again: a reinstall creates a claim of the same
name in the same namespace and binds it to that PV instead of provisioning
an empty one.

**No empty home next to a real one** is the rule everything here serves. A
home whose PV was recorded is never provisioned from scratch. If the PV is
gone, that is an error with a name, not a silent new disk that looks exactly
like data loss to the person it happens to.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

# On the PV: what it holds, and whose it is. The kind is what the uninstall
# hook selects on; the user is for people reading `kubectl get pv`.
USER_DATA_LABEL = "whistler.martinmalmsten.net/user-data"
USER_LABEL = "whistler.martinmalmsten.net/user"

KIND_HOME = "home"          # a HomeVolume's claim (VM home disk image)
KIND_POD_HOME = "pod-home"  # the per-user claim container sessions mount
KIND_ARCHIVED = "archived"  # a home taken from its user (archive_patch)
KIND_BACKUPS = "backups"    # the backup volume (design/backup.md, Phase 3)
KIND_DATASET = "dataset"    # a managed dataset's claim (design/storage.md)

# On an archived PV: where it came from and what record holds it, as JSON.
# The PV is the one object that survives an uninstall, so this is what lets a
# reinstall rebuild the archive namespace (KubeConfigManager.recover_archive).
ARCHIVED_ANNOTATION = "whistler.martinmalmsten.net/archived"

RETAIN = "Retain"
DELETE = "Delete"


def _claim_ref(pv: Dict[str, Any]) -> Dict[str, Any]:
    return (pv.get("spec") or {}).get("claimRef") or {}


def _phase(pv: Dict[str, Any]) -> str:
    return (pv.get("status") or {}).get("phase") or ""


def _name(pv: Dict[str, Any]) -> str:
    return (pv.get("metadata") or {}).get("name") or ""


def secure_patch(pv: Dict[str, Any], kind: str,
                 username: str) -> Optional[Dict[str, Any]]:
    """The patch that makes ``pv`` survive its claim, or None when it
    already does.

    Retain is set here rather than required of the storage class: a bound
    PV's reclaim policy can be changed, and "the admin picked a Retain class"
    is exactly the kind of quiet precondition that turns into data loss.
    """
    spec = pv.get("spec") or {}
    labels = (pv.get("metadata") or {}).get("labels") or {}
    patch: Dict[str, Any] = {}
    if spec.get("persistentVolumeReclaimPolicy") != RETAIN:
        patch["spec"] = {"persistentVolumeReclaimPolicy": RETAIN}
    want = {USER_DATA_LABEL: kind}
    if username:
        want[USER_LABEL] = username
    missing = {k: v for k, v in want.items() if labels.get(k) != v}
    if missing:
        patch["metadata"] = {"labels": missing}
    return patch or None


def release_patch() -> Dict[str, Any]:
    """The patch that lets a retained PV be reclaimed after all: deleting a
    home *with its data* must hand the disk back to the provisioner, which
    under Retain it never does. Set before the claim is deleted, so the
    provisioner deletes the volume when the claim goes."""
    return {"spec": {"persistentVolumeReclaimPolicy": DELETE}}


def is_retained(pv: Dict[str, Any]) -> bool:
    """A Released PV that is not Retain is on its way to being deleted by
    its provisioner (a home deleted with its data): never bind it back."""
    return (pv.get("spec") or {}).get("persistentVolumeReclaimPolicy") == RETAIN


def is_archived(pv: Dict[str, Any]) -> bool:
    labels = (pv.get("metadata") or {}).get("labels") or {}
    return labels.get(USER_DATA_LABEL) == KIND_ARCHIVED


def archive_record(pv: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The provenance an archived PV carries, or None."""
    raw = ((pv.get("metadata") or {}).get("annotations") or {}).get(
        ARCHIVED_ANNOTATION)
    try:
        record = json.loads(raw) if raw else None
    except ValueError:
        return None
    return record if isinstance(record, dict) and record.get("record") else None


def archive_patch(pv: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """Disconnect a PV from its user: kind `archived`, no user label, the
    provenance in an annotation, and Retain (it is about to lose its claim,
    so this is the moment the policy matters most)."""
    return {"metadata": {
                "labels": {USER_DATA_LABEL: KIND_ARCHIVED, USER_LABEL: None},
                "annotations": {ARCHIVED_ANNOTATION: json.dumps(
                    record, sort_keys=True)}},
            "spec": {"persistentVolumeReclaimPolicy": RETAIN}}


def unarchive_patch(username: str) -> Dict[str, Any]:
    """Give an archived PV to ``username`` as an ordinary home."""
    return {"metadata": {
                "labels": {USER_DATA_LABEL: KIND_HOME, USER_LABEL: username},
                "annotations": {ARCHIVED_ANNOTATION: None}},
            "spec": {"persistentVolumeReclaimPolicy": RETAIN}}


def _was_bound_to(pv: Dict[str, Any], namespace: str, claim: str) -> bool:
    ref = _claim_ref(pv)
    return ref.get("namespace") == namespace and ref.get("name") == claim


def find_reattachable(pvs: List[Dict[str, Any]], namespace: str, claim: str,
                      recorded: str = None, allow_archived: bool = False
                      ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """The PV a missing claim should be bound back to.

    Returns ``(pv, None)`` to rebind, ``(None, None)`` when there is nothing
    to give back (a genuinely new claim, so provisioning is right), or
    ``(None, reason)`` when provisioning would be wrong and nothing can be
    rebound either.

    ``recorded`` is the PV name Whistler wrote down once the claim was bound
    (HomeVolume ``spec.pvName``). With it the answer is never "provision":
    a recorded disk that cannot be found is an error. Without it, a
    ``Released`` PV whose stale ``claimRef`` names exactly this claim is the
    one, which covers a home secured before it was recorded, and the pod
    home, which has no CR to record on.

    An archived PV is never given back to a user's claim: its stale claimRef
    still names the claim it was archived from, so without this a new volume
    of the same name would silently pick up the archived disk. Only the
    archive's own moves pass ``allow_archived``.
    """
    if recorded:
        pv = next((p for p in pvs if _name(p) == recorded), None)
        if pv is None:
            return None, (
                f"the disk for {namespace}/{claim} (PersistentVolume "
                f"{recorded}) no longer exists. Whistler will not create an "
                f"empty one in its place; restore the volume, or remove "
                f"spec.pvName from the HomeVolume to start over with an "
                f"empty disk")
        if is_archived(pv) and not allow_archived:
            return None, (
                f"PersistentVolume {recorded} has been archived; restore it "
                f"from the archive before using it")
        return _rebindable(pv, namespace, claim)

    candidates = [p for p in pvs
                  if _was_bound_to(p, namespace, claim)
                  and _phase(p) in ("Released", "Available")
                  and is_retained(p)
                  and (allow_archived or not is_archived(p))]
    if not candidates:
        return None, None
    if len(candidates) > 1:
        names = ", ".join(sorted(_name(p) for p in candidates))
        return None, (
            f"several retained disks were bound to {namespace}/{claim} "
            f"({names}). Whistler will not guess which is the home; delete "
            f"the ones that are not, or record the right one as the "
            f"HomeVolume's spec.pvName")
    return candidates[0], None


def _rebindable(pv: Dict[str, Any], namespace: str, claim: str
                ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    phase = _phase(pv)
    if phase in ("Released", "Available") and not is_retained(pv):
        return None, (f"PersistentVolume {_name(pv)} is being deleted "
                      f"(reclaim policy is not Retain)")
    if phase == "Released" or (phase == "Available" and (
            not _claim_ref(pv) or _was_bound_to(pv, namespace, claim))):
        return pv, None
    if phase == "Bound" and not _was_bound_to(pv, namespace, claim):
        ref = _claim_ref(pv)
        return None, (
            f"PersistentVolume {_name(pv)} is bound to "
            f"{ref.get('namespace')}/{ref.get('name')}, not "
            f"{namespace}/{claim}")
    return None, (f"PersistentVolume {_name(pv)} is {phase or 'in an unknown '
                  'phase'} and cannot be bound to {namespace}/{claim}")


def rebind_manifests(pv: Dict[str, Any], namespace: str, claim: str,
                     labels: Dict[str, str]
                     ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """``(pv_patch, pvc_body)`` binding ``claim`` to ``pv``.

    The patch points the PV's claimRef at the new claim by namespace and
    name, **without** uid or resourceVersion (nulls, so the merge deletes the
    stale ones): a claimRef carrying the old claim's uid never matches the
    new claim, and the PV stays Released forever.

    The claim copies what the binder compares. ``storageClassName`` is
    written even when empty, because an absent one is defaulted by admission
    to the cluster's default class, which then does not match a PV with none.
    """
    spec = pv.get("spec") or {}
    pv_patch = {"spec": {"claimRef": {
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "namespace": namespace, "name": claim,
        "uid": None, "resourceVersion": None}}}
    pvc = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": claim, "namespace": namespace,
                     "labels": dict(labels)},
        "spec": {
            "accessModes": list(spec.get("accessModes") or ["ReadWriteOnce"]),
            "volumeMode": spec.get("volumeMode") or "Filesystem",
            "storageClassName": spec.get("storageClassName") or "",
            "volumeName": _name(pv),
            "resources": {"requests": {
                "storage": (spec.get("capacity") or {}).get("storage")}},
        },
    }
    return pv_patch, pvc


def find_backup_volume(pvs: List[Dict[str, Any]], namespace: str, claim: str,
                       existing: str = None
                       ) -> Tuple[Optional[Dict[str, Any]], List[str],
                                  Optional[str]]:
    """The PV a missing backup claim should be bound to:
    ``(pv, others, problem)``.

    ``existing`` is a static PV the admin named (``existingVolumeName``). It
    must be there and free, and is never substituted. Otherwise the backup
    volume is whichever PV the operator labelled as one, Released and
    retained — so an install never provisions a second backup volume while
    the previous install's is still around (design/backup.md, rule 1). With
    several, the newest is bound and the rest are named in ``others``.
    ``(None, [], None)`` means provision.
    """
    if existing:
        pv, problem = find_reattachable(pvs, namespace, claim,
                                        recorded=existing)
        if problem and not any(_name(p) == existing for p in pvs):
            problem = (f"the backup volume {existing} (whistler.backup.volume."
                       f"existingVolumeName) does not exist")
        return pv, [], problem
    candidates = [p for p in pvs
                  if ((p.get("metadata") or {}).get("labels") or {}).get(
                      USER_DATA_LABEL) == KIND_BACKUPS
                  and _phase(p) in ("Released", "Available")
                  and is_retained(p)]
    if not candidates:
        return None, [], None
    candidates.sort(key=lambda p: (p.get("metadata") or {}).get(
        "creationTimestamp") or "", reverse=True)
    return candidates[0], [_name(p) for p in candidates[1:]], None
