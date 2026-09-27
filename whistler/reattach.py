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

from typing import Any, Dict, List, Optional, Tuple

# On the PV: what it holds, and whose it is. The kind is what the uninstall
# hook selects on; the user is for people reading `kubectl get pv`.
USER_DATA_LABEL = "whistler.martinmalmsten.net/user-data"
USER_LABEL = "whistler.martinmalmsten.net/user"

KIND_HOME = "home"          # a HomeVolume's claim (VM home disk image)
KIND_POD_HOME = "pod-home"  # the per-user claim container sessions mount

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


def _was_bound_to(pv: Dict[str, Any], namespace: str, claim: str) -> bool:
    ref = _claim_ref(pv)
    return ref.get("namespace") == namespace and ref.get("name") == claim


def find_reattachable(pvs: List[Dict[str, Any]], namespace: str, claim: str,
                      recorded: str = None
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
        return _rebindable(pv, namespace, claim)

    candidates = [p for p in pvs
                  if _was_bound_to(p, namespace, claim)
                  and _phase(p) in ("Released", "Available")]
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
