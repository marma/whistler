
import logging
from decimal import Decimal
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple
from abc import ABC, abstractmethod
from kubernetes import client, config as k8s_config
from kubernetes.client import CoreV1Api, NetworkingV1Api
from kubernetes.client.rest import ApiException
from kubernetes.utils import parse_quantity
import base64
import copy
import datetime
import hashlib
import ipaddress
import json
import os
import secrets
import time
import yaml

from whistler.cloudinit import (HOME_DISK_SERIAL, S3_PROXY_BUCKET,
                                build_user_data, resolve_uid, resolve_gid)
from whistler import hostca

logger = logging.getLogger(__name__)

# The port a session's own sshd listens on. Jump routing splices to exactly
# this and nothing else, so the gateway can never become a generic TCP relay
# (design/proxyjump.md).
SESSION_SSH_PORT = 22

# Cluster DNS suffix for the per-session Service. Whistler's own components
# dial a session by that name, so the host certificate has to carry it as a
# principal — see session_service_host / session_ssh_principals.
CLUSTER_DNS_SUFFIX = "svc.cluster.local"

# How a zone treats interactive SSH. An inbound session is an egress channel
# the zone's (egress-only) NetworkPolicies never see, so the posture is the
# only place that can constrain it — see design/proxyjump.md, "SSH is an
# egress channel the zone fence cannot see". Only `none` is a boundary;
# `relay` is friction, and must never be documented as more than that.
SSH_POSTURE_DIRECT = "direct"   # ProxyJump splice: full end-to-end SSH
SSH_POSTURE_RELAY = "relay"     # gateway-mediated PTY only
SSH_POSTURE_NONE = "none"       # no SSH at all
SSH_POSTURES = (SSH_POSTURE_DIRECT, SSH_POSTURE_RELAY, SSH_POSTURE_NONE)

# Marks a Session the gateway created on demand (`ssh <template>.w`) and may
# therefore reap once nothing is connected to it. On the CR rather than in the
# gateway's memory so a gateway restart doesn't orphan it — though reaping
# itself is still gateway-driven; see design/proxyjump.md.
EPHEMERAL_ANNOTATION = "whistler/ephemeral"

# Run intent, as two timestamps on the Session CR. Whoever wants a session
# running or stopped writes one of these and lets the operator's reconcile do
# the work; nothing outside the operator touches a pod or a VirtualMachine.
#
# Why two timestamps instead of one `desired-state: running|stopped`: an
# annotation patch has to *differ* to produce an update event, and a one-shot
# request the operator clears is worse still — clearing it is itself an update,
# whose reconcile would see the surviving start annotation and boot the guest
# straight back up. Two monotonic marks with "latest wins" are declarative:
# re-reading them any number of times gives the same answer, so an unrelated
# reconcile (an admin editing overrides) cannot flip a stopped session on.
#
# It also closes a real hole. START_ANNOTATION alone, once set, said "wants to
# run" forever, so a session stopped from the portal would come back on the
# next reconcile that happened to touch it.
START_ANNOTATION = "whistler/last-connect"
STOP_ANNOTATION = "whistler/last-stop"


def _annotation_time(value: Any) -> Optional[float]:
    """One of these annotations as an epoch float, or None if it isn't one.

    Two formats are in the wild: `str(time.time())` from the gateway and an
    ISO string from `trigger_instance_start`. Both parse here rather than
    forcing a migration of live CRs — and an unrecognised value (the
    integration fixture writes the literal "test") returns None, which the
    caller reads as "present but undatable"."""
    if value is None:
        return None
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        # trigger_instance_start writes utcnow().isoformat() — naive, UTC.
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.timestamp()


def run_intent(annotations: Optional[Dict[str, Any]]) -> bool:
    """Whether a Session's annotations say its workload should be running.

    The rule in one line: a start mark means run, unless a stop mark is at
    least as new. Ties go to stop — two writes in the same clock tick are a
    stop landing on a start, and stopped is the state that runs nothing and
    holds no home volume, so it is the safe way to resolve a coin flip.
    Undatable values (see `_annotation_time`) also lose to a stop for the same
    reason; a bare start mark with no stop still means run, which is what
    keeps every existing CR behaving exactly as it did."""
    annotations = annotations or {}
    if START_ANNOTATION not in annotations:
        return False
    if STOP_ANNOTATION not in annotations:
        return True
    started = _annotation_time(annotations.get(START_ANNOTATION))
    stopped = _annotation_time(annotations.get(STOP_ANNOTATION))
    if started is None or stopped is None:
        return False
    return started > stopped

def effective_session_overrides(spec: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The overrides a Session is actually built from.

    Two slices, and the newer one wins outright rather than merging:
      - ``spec.overrides`` are the instance's *defaults*, edited on the portal's
        edit form and meant to persist.
      - ``spec.runOverrides`` is the answer the portal's start dialog gave for
        **this run only**. Every start writes it — the dialog's answer, or
        nothing at all — so it never outlives the run it was chosen for, and
        changing a value there does not change what the instance starts with
        next time.

    Absent and empty are different, which is why this tests for None rather
    than truthiness: an absent key means "nobody chose for this run, use the
    defaults", while ``{}`` means "chosen: no overrides at all", which is what a
    dialog submitted with every field cleared has to be able to say. Same
    distinction `channels` makes, for the same reason.

    Replacing rather than merging is what makes clearing a field work. The
    dialog is prefilled from the defaults and submits the whole picture, so a
    field left blank is a deliberate "not this run" — merging would quietly put
    the default back."""
    spec = spec or {}
    run = spec.get("runOverrides")
    return run if run is not None else spec.get("overrides")


# Config file locations. Defaults match the in-cluster mount paths used by the
# Helm chart; override via env so the server/operator can run as host processes
# (e.g. local k3d integration testing) without writing to /etc.
CONFIG_DIR = os.environ.get("WHISTLER_CONFIG_DIR", "/etc/whistler-config")
SELECTORS_FILE = os.path.join(CONFIG_DIR, "selectors.yaml")
VOLUMES_FILE = os.path.join(CONFIG_DIR, "volumes.yaml")
NETWORKPOLICY_FILE = os.path.join(CONFIG_DIR, "networkpolicy.yaml")
ZONES_FILE = os.path.join(CONFIG_DIR, "zones.yaml")
IMAGES_FILE = os.path.join(CONFIG_DIR, "images.yaml")
# Seeds the first admin User CR at operator startup (KubeConfigManager.
# ensure_bootstrap_admin); create-if-absent only, see values.yaml bootstrapAdmin.
BOOTSTRAP_ADMIN_FILE = os.path.join(CONFIG_DIR, "bootstrapAdmin.yaml")

# KubeVirt API coordinates for the VM desktop backend. KubeVirt may be absent
# from a given cluster; every call against these is guarded so the operator
# runs cleanly without the CRDs installed.
KUBEVIRT_GROUP = "kubevirt.io"
KUBEVIRT_VERSION = "v1"
KUBEVIRT_VM_PLURAL = "virtualmachines"
KUBEVIRT_VMI_PLURAL = "virtualmachineinstances"

# Page size backing a VM's guest RAM (domain.memory.hugepages.pageSize).
#
# This is a startup-latency setting, not a tuning knob. VFIO DMA-pins the
# whole guest address space when the GPU is attached, and with 4 KiB pages a
# 32 GiB guest is 8.4M pages to pin — measured at 19.75 s on wkstn, against
# virt-handler's *compile-time* 20 s SyncVMI deadline. Blowing that deadline
# does not fail cleanly: virt-handler signals deletion and unmounts the
# containerDisk while qemu keeps running, leaving a virt-launcher pod stuck at
# 2/3 with a healthy guest inside and no Service endpoints (so ssh and the
# portal proxy die while the VNC console still works). 2 MiB pages make that
# same guest 16384 pages, and 1 GiB pages make it 32.
#
# Only 2Mi and 1Gi exist on x86_64 (/sys/kernel/mm/hugepages); libvirt takes
# the value verbatim, so anything else is a VM that never schedules.
#
# The default is 2Mi rather than 1Gi because of how the *node* reserves them
# (below): a 1 GiB page needs a gigabyte of physically contiguous memory, so
# in practice it can only be reserved at boot, while 2 MiB pages can be added
# to a running host. Both sizes are three orders of magnitude clear of the
# deadline; the difference between them is pinning work, not admission.
#
# Hugepages are NOT overcommittable and NOT allocated on demand: the node must
# reserve them up front (kernel hugepagesz=/hugepages=, or nr_hugepages plus a
# kubelet restart), and a VM whose node has none pends forever on Insufficient
# hugepages-<size>. Setting the chart's pageSize to "" turns the whole thing
# off.
DEFAULT_HUGE_PAGE_SIZE = "2Mi"


def _format_quantity(num_bytes: int) -> str:
    """A byte count as the largest binary Kubernetes quantity that divides it
    exactly (``2147483648`` -> ``"2Gi"``). Pure.

    Round-trips through parse_quantity, and keeps a rounded-up guest memory
    readable in the manifest — `3Gi` rather than `3221225472`.
    """
    for suffix, unit in (("Gi", 1 << 30), ("Mi", 1 << 20), ("Ki", 1 << 10)):
        if num_bytes >= unit and num_bytes % unit == 0:
            return f"{num_bytes // unit}{suffix}"
    return str(num_bytes)

# Optional fields of a VirtualMachine spec that _build_vm_spec owns and may
# stop emitting between builds (drop a GPU, move to an instancetype, leave a
# DNS-forcing zone). A JSON merge patch never removes what it doesn't mention,
# so KubeConfigManager._build_vm_spec_patch nulls these explicitly when the
# freshly built spec no longer sets them.
_VM_MANAGED_FIELDS = (
    ("instancetype",),
    ("template", "spec", "nodeSelector"),
    ("template", "spec", "dnsPolicy"),
    ("template", "spec", "dnsConfig"),
    ("template", "spec", "domain", "cpu"),
    ("template", "spec", "domain", "resources"),
    ("template", "spec", "domain", "memory"),
    ("template", "spec", "domain", "devices", "gpus"),
)

# CDI (Containerized Data Importer) coordinates — used for the imageURL boot
# source (HTTP qcow2/raw imported into a per-session root PVC). Like KubeVirt,
# CDI may be absent; all reads are 404-guarded.
CDI_GROUP = "cdi.kubevirt.io"
CDI_VERSION = "v1beta1"
CDI_DV_PLURAL = "datavolumes"

# Custom-resource plurals for the unified Template/Session model (group
# whistler.martinmalmsten.net/v1). One Template kind covers ssh + desktop; one
# Session kind covers what used to be WhistlerInstance + DesktopSession.
TEMPLATE_PLURAL = "templates"
SESSION_PLURAL = "sessions"
USER_PLURAL = "users"
ZONE_PLURAL = "zones"
GROUP_PLURAL = "groups"
DATASET_PLURAL = "datasets"
HOME_VOLUME_PLURAL = "homevolumes"

# Node label a template/override's gpuType is matched against, both as the
# nodeSelector key that schedules onto a GPU of that type and as the node
# label the Dashboard reads to type a node's GPUs. This is the NVIDIA GPU
# Operator's node-feature-discovery label (auto-applied to GPU nodes, e.g.
# "NVIDIA-A100-SXM4-40GB") — not a whistler-specific label an admin has to
# set by hand, unlike the "accelerator" shorthand this used to be.
GPU_NODE_LABEL = "nvidia.com/gpu.product"

# The pod-mode GPU resource (NVIDIA device plugin). A node in VM-passthrough
# mode advertises 0 of these and instead exposes a product-specific vfio
# resource (e.g. "nvidia.com/AD102_GEFORCE_RTX_4090") via the sandbox device
# plugin. Which of those product-specific names are actually GPUs — and not,
# say, the card's audio function, which is advertised right next to it — is
# not guessable from the name; the KubeVirt CR's permittedHostDevices is the
# authority, and _vm_gpu_resource_names reads it. See build_gpu_catalog.
GPU_POD_RESOURCE = "nvidia.com/gpu"


def build_gpu_catalog(nodes: List[Dict[str, Any]],
                      vm_resource_names: Set[str]) -> List[Dict[str, Any]]:
    """Derive the GPU-type catalog from live node data — no static config.

    ``nodes``: ``[{"name": ..., "labels": {...}, "allocatable": {...}}]``.
    ``vm_resource_names``: resource names KubeVirt permits as host devices
    (the set _vm_gpu_resource_names reads from the KubeVirt CR).

    Returns one entry per GPU *type* (the GFD ``nvidia.com/gpu.product``
    label, which is also what templates schedule by via GPU_NODE_LABEL):
    ``{"name", "count", "vmResource"}`` where ``count`` sums pod-mode and
    passthrough devices across nodes and ``vmResource`` is the KubeVirt
    device-plugin resource name for passthrough (None when the type is only
    available to pods). Nodes without the product label are skipped — a
    type nothing can schedule by name isn't selectable; the dashboard still
    counts such capacity separately as "unknown".

    Pure so it's unit-testable; KubeConfigManager.get_gpu_catalog feeds it
    live cluster data."""
    by_name: Dict[str, Dict[str, Any]] = {}
    for node in nodes:
        labels = node.get("labels") or {}
        allocatable = node.get("allocatable") or {}
        gpu_type = labels.get(GPU_NODE_LABEL)
        if not gpu_type:
            continue
        count = int(parse_quantity(allocatable.get(GPU_POD_RESOURCE, 0)))
        vm_resource = None
        for rname in sorted(vm_resource_names):
            vm_count = int(parse_quantity(allocatable.get(rname, 0)))
            if vm_count:
                if vm_resource is not None:
                    logger.warning(
                        "Node %s advertises multiple permitted VM GPU resources "
                        "(%s, %s); using %s", node.get("name"), vm_resource,
                        rname, vm_resource)
                    continue
                vm_resource = rname
                count += vm_count
        entry = by_name.setdefault(
            gpu_type, {"name": gpu_type, "count": 0, "vmResource": None})
        entry["count"] += count
        if vm_resource:
            if entry["vmResource"] and entry["vmResource"] != vm_resource:
                logger.warning(
                    "GPU type %s maps to multiple VM resource names (%s, %s); "
                    "keeping %s", gpu_type, entry["vmResource"], vm_resource,
                    entry["vmResource"])
            else:
                entry["vmResource"] = vm_resource
    return sorted(by_name.values(), key=lambda e: e["name"])

# Zones: admin-defined network postures (whistler.zones) a session runs under.
# Each zone renders to one NetworkPolicy per user namespace selecting pods by
# this label; the label is stamped at pod/VM build time, so a session changes
# zone on reboot, never live. ZONE_HASH_ANNOTATION records a digest of the zone
# config the pod was built under — "what rules was this actually running with"
# stays answerable after the zone definition changes.
ZONE_LABEL = "whistler.martinmalmsten.net/zone"
# Stamped on every per-user namespace at creation (_ensure_user_namespace).
# The S3 proxy's fencing policy selects namespaces by it, so the two must
# agree — hence a constant rather than the literal in two places.
USER_NS_LABEL = "whistler.martinmalmsten.net/user"
ZONE_POLICY_LABEL = "whistler.martinmalmsten.net/zone-policy"
ZONE_HASH_ANNOTATION = "whistler.martinmalmsten.net/zone-config-hash"
DEFAULT_ZONE = "default"

# The cluster resolver's canonical labels (CoreDNS/kube-dns on k3s, kubeadm,
# and managed clusters alike) — target of the dns.clusterOnly zone knob.
CLUSTER_DNS_NAMESPACE = "kube-system"
CLUSTER_DNS_POD_LABELS = {"k8s-app": "kube-dns"}

# Groups of template values a user may be granted permission to override per-
# session (User CR `overrides`, session spec.overrides). Booleans only —
# granting a group does not bound the requested value; allowedGpuTypes /
# allowedVolumes remain the value-level allow-lists for the two groups that
# have one. See KubeConfigManager._apply_overrides.
OVERRIDE_GROUPS = (
    "resources",        # resources.cpu / resources.memory
    "gpuType",          # nodeSelector[GPU_NODE_LABEL] (still gated by allowedGpuTypes)
    "gpuCount",         # resources.gpu
    "uidGid",           # user_details.uid / .gid (VM guest identity)
    "securityContext",  # user_details.securityContext.{fsGroup,runAsUser,runAsGroup}
    "volumes",          # template volumes (still gated by allowedVolumes)
    "zone",             # network zone (still gated by allowedZones)
)

# The ways a person can move bytes into and out of a session — the second of
# the four axes in design/security.md. A Zone carries a *ceiling* (the most any
# session there may use) and a User/Group `channels` grant narrows it; nothing
# widens it. The desktop stream itself is not in the set: it is always on, it
# is the point of a desktop, and pretending it were optional would make the
# vocabulary lie.
CHANNEL_SSH = "ssh"                  # end-to-end jump: scp/sftp/rsync/-L/-R
CHANNEL_RELAY = "relay"              # gateway-mediated PTY (TUI connect)
CHANNEL_TERMINAL = "terminal"        # portal web terminal
CHANNEL_CLIPBOARD = "clipboard"      # desktop clipboard, bidirectional
CHANNEL_SCREENSHOTS = "screenshots"  # portal thumbnails of a desktop
CHANNELS = (CHANNEL_SSH, CHANNEL_RELAY, CHANNEL_TERMINAL,
            CHANNEL_CLIPBOARD, CHANNEL_SCREENSHOTS)

# Which channels Whistler can actually close server-side today. `clipboard`
# is deliberately absent: the toggle would have to be in the streamer, and
# Selkies 2.x exposes none that has been verified (design/security.md,
# "Access channels"). A grant that excludes it is recorded and displayed, but
# it is NOT enforced, and no code here may treat it as if it were.
ENFORCED_CHANNELS = (CHANNEL_SSH, CHANNEL_RELAY, CHANNEL_TERMINAL,
                     CHANNEL_SCREENSHOTS)

# The doors into Whistler itself — the *fourth* axis of design/security.md
# ("Closing the fourth axis: the kiosk situation"), and a different question
# from CHANNELS above. Channels ask what a person may do once they are in a
# session; entry points ask which surface will talk to them at all.
#
# `kiosk` is the /kiosk grid and its full-screen desktop; `portal` is the
# management UI plus the viewer app's ordinary pages (the launch form, the web
# terminal, the machine console); `gateway` is the SSH server — the launcher
# TUI, the relay and the jump alike, which is one grant because they are one
# door on one port.
#
# The grant composes like allowedZones: the union of the user's own list and
# every group's is the set (merge_allow_lists), and **an empty set is no door
# at all** (2026-08-25, "Every allow is explicit"). So "kiosk only" is
# `entryPoints: [kiosk]` on the User — and a group that grants `portal` widens
# its members back out, which is what a grant means everywhere else here and is
# the one thing to keep in mind when binding somebody.
ENTRY_KIOSK = "kiosk"
ENTRY_PORTAL = "portal"
ENTRY_GATEWAY = "gateway"
ENTRY_POINTS = (ENTRY_KIOSK, ENTRY_PORTAL, ENTRY_GATEWAY)

# What a brand-new account is created holding, now that an empty list grants
# nothing. Deliberately not "everything": these are the two grants that decide
# whether an account can be used *at all* — a door to come in through, and the
# zone every unzoned template lands in. allowedVolumes and allowedGpuTypes stay
# empty, because those are the grants an admin means to make one at a time.
NEW_USER_ENTRY_POINTS = list(ENTRY_POINTS)
NEW_USER_ZONES = [DEFAULT_ZONE]

# How a zone's legacy `ssh` posture reads as a channel set, so zones written
# before Zone.spec.channels keep their exact meaning.
_POSTURE_CHANNELS = {
    SSH_POSTURE_DIRECT: (CHANNEL_SSH, CHANNEL_RELAY),
    SSH_POSTURE_RELAY: (CHANNEL_RELAY,),
    SSH_POSTURE_NONE: (),
}


#: Ordered most permissive first. Absent (None) is no access at all, which is
#: why it is not in the list — it is the absence of an entry, not a value.
ACCESS_MODES = ("allowed", "read-only")
#: Which volume kinds the access matrix actually governs today. Everything
#: else is recorded and displayed but still decided by allowedVolumes and the
#: group volume grants, exactly the way `clipboard` sits in CHANNELS but not
#: ENFORCED_CHANNELS. Widening this is a deliberate migration, not a tweak:
#: the matrix has no defaults, so anything moved into it stops working the
#: moment a grant is missing.
ENFORCED_ACCESS_KINDS = ("home",)


def merge_volume_access(*sources) -> Dict[str, Dict[str, str]]:
    """Merge access matrices, most permissive per cell.

    ``{zone: {volume: mode}}``. An absent cell is no access, full stop —
    the same rule merge_allow_lists now follows (design/security.md, "Every
    allow is explicit"). The matrix got there first; the allow-lists were
    brought into line in 2026-08-25.

    Most-permissive-wins because joining a group is a deliberate act whose
    purpose is to confer access; a user who already held something does not
    lose it by joining a project. The hazard that needs watching is not this
    one — it is one member's access reaching another member, which requires a
    shared instance and is why a project instance must carry its own subject
    entry rather than the union of its members'.
    """
    merged: Dict[str, Dict[str, str]] = {}
    for source in sources:
        for zone, volumes in (source or {}).items():
            row = merged.setdefault(zone, {})
            for volume, mode in (volumes or {}).items():
                if mode not in ACCESS_MODES:
                    continue
                # "allowed" beats "read-only"; anything beats absent.
                if row.get(volume) != "allowed":
                    row[volume] = mode
    return merged


def merge_allow_lists(*sources) -> List[str]:
    """The effective allow-list from a user's own field and their groups'.

    One rule for volumes, zones, gpuTypes and entryPoints: **the union is
    what you have, and nothing else.** A user with no list of their own holds
    exactly what their groups grant; a user with a list of their own keeps it
    *and* gains the group's — grants add up, which is what a grant means
    (design/security.md, "The border has four axes", axis 3).

    Empty is **no access**, not "no opinion" (2026-08-25). It used to mean
    unrestricted, which made the safest-looking User CR — no lists at all —
    the most permissive one there is, and made every enforcement point carry
    an `if allowed:` guard that read like a null check and acted like a
    policy. The matrix in merge_volume_access always worked this way; the
    allow-lists now agree with it.

    Order is the caller's: the user's own entries first, then each group's, so
    the portal can show where a name came from without a second lookup.
    """
    merged = []
    for source in sources:
        for item in source or []:
            if item not in merged:
                merged.append(item)
    return merged


def merge_override_grants(*sources) -> Dict[str, bool]:
    """Per-session override grants (User/Group `overrides`), OR'd across
    sources. Only granted keys are returned — an explicit ``false`` in one
    source does not veto a ``true`` in another, since these are grants, not
    denials."""
    merged = {}
    for source in sources:
        for key, granted in (source or {}).items():
            if granted:
                merged[key] = True
    return merged


def merge_channel_grants(*sources) -> Optional[Set[str]]:
    """Union of the channel grants that are *present*, or None when no source
    states one.

    Unlike the allow-lists, absent and empty differ here: no `channels` field
    at all means "this source does not narrow the zone's ceiling", while an
    explicit empty list is a real grant of nothing (the desktop stream alone).
    Collapsing the two would make an empty list unwritable."""
    stated = [s for s in sources if s is not None]
    if not stated:
        return None
    merged = set()
    for source in stated:
        merged.update(source)
    return merged


def target_channels(target: Dict[str, Any]) -> Set[str]:
    """The channels a ``resolve_ssh_target`` result grants.

    Falls back to the zone's ssh posture when `channels` is absent, so the
    gateway's checks can never be skipped by a resolver that doesn't set the
    field — a missing grant must read as the posture, not as "everything"."""
    channels = target.get("channels")
    if channels is not None:
        return set(channels)
    posture = target.get("sshPosture", SSH_POSTURE_DIRECT)
    return set(_POSTURE_CHANNELS.get(posture, ()))


def group_volume_grants(group_spec: Dict[str, Any], username: str) -> Dict[str, str]:
    """What one group grants ``username`` on each volume: ``{name: "rw"|"ro"}``.

    A member's mode is the per-member entry in ``access`` when present, else
    the volume's ``mode`` (default ``rw``, and ``none`` for a volume only the
    named exceptions reach). Someone named in ``access`` but not in
    ``members`` still gets that volume — the CRD says so, and it is the
    natural way to hand one outsider a read-only look at a project — while
    someone in neither gets nothing from this entry."""
    members = group_spec.get("members") or []
    grants = {}
    for entry in group_spec.get("volumes") or []:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not name:
            continue
        access = entry.get("access") or {}
        if username in access:
            mode = access[username]
        elif username in members:
            mode = entry.get("mode") or "rw"
        else:
            continue
        mode = str(mode).strip().lower()
        if mode == "none":
            continue
        grants[name] = "ro" if mode == "ro" else "rw"
    return grants


def _dig(obj, path):
    """Value at a nested dict path, or None if any level is missing."""
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def _set_at(patch, path, value):
    """Set a nested dict path in a patch body, creating levels as needed."""
    for key in path[:-1]:
        patch = patch.setdefault(key, {})
    patch[path[-1]] = value


def _merge_patch_is_noop(current, patch) -> bool:
    """True when applying JSON merge patch `patch` to `current` would change
    nothing. Lets callers skip a write (and the resource churn it causes) on
    the common reconcile where the built spec already matches the cluster.
    A missing key and an empty dict are equivalent here — sending `{}` for an
    absent field is not a meaningful change."""
    for key, value in (patch or {}).items():
        cur = (current or {}).get(key)
        if value is None:
            if cur is not None:
                return False
        elif isinstance(value, dict):
            if not _merge_patch_is_noop(cur if isinstance(cur, dict) else {}, value):
                return False
        elif cur != value:
            return False
    return True


class ConfigWriteError(Exception):
    """A cluster write failed, carrying the API server's own reason.

    Raised instead of returning False where the caller has a user in front of
    it: "Failed to create group." sends someone to the pod logs to find a line
    the server already knew. The commonest cause is worth naming outright —
    **`helm upgrade` never updates CRDs** (Helm only installs a chart's
    `crds/` on first install), so a new kind 404s until someone runs
    `kubectl apply -f charts/whistler/crds/crds.yaml`."""


def crd_missing_hint(plural: str, error: ApiException) -> str:
    """A message for an ApiException from a custom-resource call, naming the
    missing-CRD case when the status says that is what happened."""
    if error.status == 404:
        return (f"the {plural}.whistler.martinmalmsten.net CRD is not "
                f"installed in this cluster. `helm upgrade` does not update "
                f"CRDs — run `kubectl apply -f charts/whistler/crds/crds.yaml`")
    return f"{error.status} {error.reason}"


class PolicyError(Exception):
    """A template violates an operator-enforced policy (image allow-list,
    privileged/runtime rules). Raised by _apply_policy; ensure_session turns it
    into a failed provision."""


class ConfigManager(ABC):
    @abstractmethod
    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_user_templates(self, username: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def user_exists(self, username: str) -> bool:
        pass

    @abstractmethod
    def get_user_instances(self, username: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def add_instance(self, username: str, template_name: str, instance_name: str,
                     preemptible: bool = False,
                     overrides: Optional[Dict[str, Any]] = None,
                     home_volume: Optional[str] = None) -> bool:
        pass

    # --- home volumes (design/security.md) --------------------------------- #

    @staticmethod
    def home_volume_pvc_name(volume: Dict[str, Any]) -> str:
        """The claim backing a home volume. Normally derived from the name;
        adopted volumes carry an explicit `pvcName` because their claim is
        named after the session that created it and a bound PVC cannot be
        renamed. Concrete on the ABC: both sides of the wire must agree."""
        return (volume.get("pvcName")
                or f"whistler-home-{volume.get('name')}")

    @abstractmethod
    def get_user_volume_access(self, username: str) -> Dict[str, Dict[str, str]]:
        """The access matrix ``{zone: {volume: mode}}``, own merged with every
        group's. An absent cell is no access — there is no default."""
        pass

    @abstractmethod
    def set_user_volume_access(self, username: str,
                               matrix: Dict[str, Dict[str, str]]) -> bool:
        """Replace the user's OWN matrix (never the merged view)."""
        pass


    @abstractmethod
    def grant_own_volume_access(self, username: str, zone: str, volume: str,
                                mode: str = "allowed") -> bool:
        """Add one cell to the user's own matrix. Self-service creation of a
        home volume in a zone the user already holds uses this."""
        pass

    def volume_access(self, username: str, zone: str,
                      volume: str) -> Optional[str]:
        """One cell of the merged matrix. Concrete: every implementation must
        answer this the same way, since it is the enforcement point."""
        return (self.get_user_volume_access(username).get(zone) or {}).get(volume)

    @abstractmethod
    def get_home_volumes(self, username: str) -> List[Dict[str, Any]]:
        """The user's named home volumes. Empty is normal — an instance with
        no named volume gets one dedicated to itself."""
        pass

    @abstractmethod
    def save_home_volume(self, username: str, volume: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def delete_home_volume(self, username: str, name: str,
                           delete_data: bool = False) -> bool:
        """Remove the volume. The data is KEPT unless ``delete_data`` — this
        is the user's home directory, and a dropdown must not be able to
        destroy it."""
        pass

    @abstractmethod
    def home_volume_holder(self, username: str, volume: Dict[str, Any],
                           ignore_instance: str = None) -> Optional[str]:
        """The RUNNING instance holding this volume, or None. One live attach:
        a home carries an ext4 filesystem, which cannot be attached to two
        running guests at once."""
        pass

    @abstractmethod
    def get_instance_config(self, username: str,
                            instance_name: str) -> Optional[Dict[str, Any]]:
        """Return the editable slice of a Session CR spec (templateRef,
        preemptible, overrides) for pre-filling the edit form, or None if the
        instance doesn't exist."""
        pass

    @abstractmethod
    def update_instance(self, username: str, instance_name: str,
                        preemptible: bool = False,
                        overrides: Optional[Dict[str, Any]] = None,
                        home_volume: Optional[str] = None) -> bool:
        """Replace a Session CR's editable spec fields (preemptible, overrides,
        homeVolume).
        Changes take effect on the next start/reboot, so this is only meant for
        non-running instances."""
        pass

    @abstractmethod
    def save_template(self, username: str, template_data: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def delete_instance(self, username: str, instance_name: str) -> bool:
        pass

    @abstractmethod
    def delete_template(self, username: str, template_name: str) -> bool:
        pass

    @abstractmethod
    def get_user_desktop_templates(self, username: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_user_desktop_sessions(self, username: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def add_desktop_session(self, username: str, template_name: str, session_name: str,
                            overrides: Optional[Dict[str, Any]] = None) -> bool:
        pass

    @abstractmethod
    def delete_desktop_session(self, username: str, session_name: str) -> bool:
        pass

    @abstractmethod
    def get_selectors(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_volumes(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_gpu_types(self) -> List[str]:
        pass

    @abstractmethod
    def get_zones(self) -> List[str]:
        """Names of the defined network zones ("default" always among them)."""
        pass

    @abstractmethod
    def get_available_images(self, category: Optional[str] = None) -> List[str]:
        pass

    @abstractmethod
    def get_server_host_key(self, secret_name: str) -> Optional[bytes]:
        pass

    @abstractmethod
    def save_server_host_key(self, secret_name: str, key_data: bytes) -> bool:
        pass

    @abstractmethod
    def resolve_ssh_target(self, username: str,
                           name: str) -> Optional[Dict[str, Any]]:
        """Where an SSH jump for ``<name>`` should land, or None when this user
        has no session by that name (design/proxyjump.md)."""
        pass

    @abstractmethod
    def get_ssh_known_hosts_line(self) -> Optional[str]:
        """The ``@cert-authority`` line users add once to trust every
        instance."""
        pass

    @abstractmethod
    def list_ssh_targets(self, username: str) -> List[Dict[str, Any]]:
        """Every session this user has (ssh-mode and desktop alike) with
        whether SSH can reach it — the launcher's data source."""
        pass


    @abstractmethod
    def get_ssh_ca_public_key(self) -> Optional[str]:
        """The host CA's public half, for verifying a session's host
        certificate (whistler/relay.py)."""
        pass

    @abstractmethod
    def get_vm_access_private_key(self, username: str) -> Optional[str]:
        """The per-user access key Whistler authenticates as when it relays
        into a session on that user's behalf."""
        pass

    # ------------------------------------------------------------------ #
    # Admin / management operations                                        #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def list_all_users(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def save_user(self, user_data: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def delete_user(self, username: str) -> bool:
        pass

    @abstractmethod
    def is_user_admin(self, username: str) -> bool:
        pass

    @abstractmethod
    def get_all_templates(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_all_instances(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_cluster_resources(self) -> Dict[str, Any]:
        """Cluster-wide CPU/memory/GPU capacity vs requests, for the Dashboard tab."""
        pass

    @abstractmethod
    def save_system_template(self, template_data: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def save_volume(self, volume_data: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def delete_volume(self, volume_name: str) -> bool:
        pass

    @abstractmethod
    def get_zone_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Full zone catalog (name -> config) for the admin zones editor."""
        pass

    @abstractmethod
    def save_zone(self, zone_data: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def delete_zone(self, zone_name: str) -> bool:
        pass

    @abstractmethod
    def get_user_groups(self, username: str) -> List[Dict[str, Any]]:
        """The Groups this user belongs to (design/security.md, "Group").
        Every get_user_* grant below already folds these in — this is for
        showing *why*, not for a caller to resolve grants itself."""
        pass

    @abstractmethod
    def get_group_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Full group catalog (name -> spec) for the admin Groups editor."""
        pass

    @abstractmethod
    def save_group(self, group_data: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def delete_group(self, group_name: str) -> bool:
        pass

    @abstractmethod
    def get_user_allowed_volumes(self, username: str) -> List[str]:
        """Volumes this user may mount: their own list unioned with every
        group's. Empty means **none** — every allow is explicit."""
        pass

    @abstractmethod
    def get_user_volume_modes(self, username: str) -> Dict[str, str]:
        """``{volume: "rw"|"ro"}`` for group-granted volumes; anything absent
        is read-write."""
        pass

    @abstractmethod
    def set_user_allowed_volumes(self, username: str, volume_names: List[str]) -> bool:
        pass

    @abstractmethod
    def get_user_allowed_gpu_types(self, username: str) -> List[str]:
        pass

    @abstractmethod
    def set_user_allowed_gpu_types(self, username: str, gpu_types: List[str]) -> bool:
        pass

    @abstractmethod
    def get_user_allowed_zones(self, username: str) -> List[str]:
        pass

    @abstractmethod
    def set_user_allowed_zones(self, username: str, zones: List[str]) -> bool:
        pass

    @abstractmethod
    def get_user_entry_points(self, username: str) -> List[str]:
        """Entry points this user may use (ENTRY_POINTS): their own list
        unioned with every group's. Empty means **no door at all** — ask
        ``may_enter`` rather than reading this, so every caller decides it the
        same way."""
        pass

    @abstractmethod
    def set_user_entry_points(self, username: str, entry_points: List[str]) -> bool:
        """Set this user's own entry-point grant. An empty list is a real
        grant of nothing: the account can then only come in through a door one
        of its groups grants. That is why this field needs no absent-vs-empty
        distinction the way `channels` does — absent and empty both grant
        nothing, and only the union is ever asked."""
        pass

    def may_enter(self, username: str, entry_point: str) -> bool:
        """Whether this user may come in through ``entry_point``.

        Concrete on the ABC on purpose: this is the identity half of the kiosk
        situation (design/security.md), it has to be asked at *every* door —
        the SSH gateway, the management portal, the viewer app — and a rule
        that each door restates is a rule one of them will restate differently.
        A missed door is the named failure mode, not a hypothetical one.

        No grant is no entry (2026-08-25). An account nobody has granted a
        door cannot come in through any of them — including one that has just
        been created outside the portal, which is the case worth stating
        because it used to be the most permissive account in the cluster."""
        return entry_point in self.get_user_entry_points(username)

    @abstractmethod
    def get_user_overrides(self, username: str) -> Dict[str, bool]:
        pass

    @abstractmethod
    def set_user_overrides(self, username: str, overrides: Dict[str, bool]) -> bool:
        pass

    @abstractmethod
    def set_user_channels(self, username: str, channels: Optional[List[str]]) -> bool:
        """Set this user's own access-channel grant (CHANNELS)."""
        pass

    @abstractmethod
    def get_user_channels(self, username: str) -> Optional[Set[str]]:
        """This user's channel grant — their own unioned with their groups' —
        or None when nobody states one. Zone-independent; the ceiling is
        applied by effective_channels."""
        pass

    @abstractmethod
    def effective_channels(self, username: str, zone: str) -> Set[str]:
        """Channels this user may use in this zone: the zone's ceiling
        narrowed by their own and their groups' grants."""
        pass

    @abstractmethod
    def session_channels(self, username: str, name: str) -> Set[str]:
        """effective_channels for one of this user's sessions, resolved
        through that session's zone."""
        pass

    @abstractmethod
    def stop_instance(self, username: str, instance_name: str) -> bool:
        """Delete the pod but keep the Session CR (stops compute, preserves state)."""
        pass

    @abstractmethod
    def trigger_instance_start(self, username: str, instance_name: str,
                               run_overrides: Optional[Dict[str, Any]] = None) -> bool:
        """Bump an annotation on the Session CR to fire the operator's reconcile.

        ``run_overrides`` is the overrides *this run* uses, written in the same
        act as the start and gone by the next one: a dict (``{}`` included —
        "no overrides this run") sets spec.runOverrides, and None removes it so
        the instance falls back to its own spec.overrides. Only the portal's
        start dialog passes one; the launcher, the jump and the plain play
        button start an instance the way it is configured."""
        pass

class KubeConfigManager(ConfigManager):
    # Class-level so an instance built with __new__ (the unit tests do this to
    # exercise the pure builders without a cluster — see _load_users) still has
    # a catalog to read. Never mutated in place: _load_groups rebinds it.
    groups: Dict[str, Dict[str, Any]] = {}
    # One warning per process for a missing Group CRD, not one per policy
    # evaluation — the catalog is re-read on every grant lookup.
    _warned_no_group_crd: bool = False
    # Shared datasets (Dataset CRs). None rather than {} so _load_datasets can
    # tell "never loaded" from "loaded and genuinely empty" — the difference
    # decides whether a failed load may fall back to the legacy catalog.
    datasets: Optional[Dict[str, Dict[str, Any]]] = None
    _warned_no_dataset_crd: bool = False
    _warned_no_home_volume_crd: bool = False
    # Default hugepage size for VM guest RAM; __init__ overrides it from the
    # environment. Class-level for the same reason `groups` is — the pure
    # builders are unit-tested on an instance made with __new__.
    huge_page_size: str = DEFAULT_HUGE_PAGE_SIZE

    def __init__(self, kubeconfig: str = None):
        try:
            if kubeconfig:
                k8s_config.load_kube_config(config_file=kubeconfig)
            else:
                k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            try:
                k8s_config.load_kube_config()
            except k8s_config.ConfigException:
                logger.warning("Could not load KubeConfig, standard K8s calls will fail")
        
        self.api = client.CustomObjectsApi()
        self.group = "whistler.martinmalmsten.net"
        self.version = "v1"
        self.namespace = os.environ.get("POD_NAMESPACE")
        
        if not self.namespace:
            try:
                with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
                    self.namespace = f.read().strip()
            except FileNotFoundError:
                self.namespace = "whistler" # Default fallback

        self.users = {}
        self._load_users()

        # Projects: named sets of users sharing grants (design/security.md,
        # "Group"). Loaded like users — refreshed on read, kept stale on API
        # failure rather than wiped, since an empty catalog would silently
        # widen every member back to their own (usually empty) allow-lists.
        self.groups = {}
        self._load_groups()

        # Initialize containers
        self.selectors = {} 
        self._load_selectors()
        
        self.volumes = []
        self.volume_definitions = {}
        self._load_volumes()

        # Shared S3 datasets (design/storage.md). After volumes: the legacy
        # fallback reads `type: s3` entries out of the volume catalog.
        self._load_datasets()

        # Named network zones (whistler.zones); "default" always exists. The
        # legacy networkpolicy.yaml egress config seeds the default zone when
        # zones.yaml doesn't define one.
        self.zones = {DEFAULT_ZONE: {}}
        self._load_zones()

        # Image allow-lists (by template category) + security policy. Enforced
        # operator-side at pod/VM build time (see _apply_policy).
        self.images = {"ssh": [], "desktop": [], "vm": []}
        self._load_images()

        self.force_kata_for_privileged = os.environ.get(
            "WHISTLER_FORCE_KATA_FOR_PRIVILEGED", "false"
        ).strip().lower() in ("1", "true", "yes")
        self.kata_runtime_class = os.environ.get("WHISTLER_KATA_RUNTIME_CLASS", "kata")
        # RuntimeClass applied to pods requesting a GPU (resources.gpu), so
        # they actually run under nvidia-container-runtime rather than plain
        # runc — without it, the device plugin still bind-mounts the device
        # nodes (kubelet does that independently), but the driver userspace
        # (nvidia-smi, libcuda.so, ...) is never injected, since only the
        # NVIDIA runtime's hook does that. Empty disables this (e.g. a
        # cluster using containerd-native CDI with no RuntimeClass at all).
        self.gpu_runtime_class = os.environ.get("WHISTLER_GPU_RUNTIME_CLASS", "nvidia")
        # GPU-type catalog cache (get_gpu_catalog): (catalog, monotonic ts).
        # Derived live from node labels/allocatable + the KubeVirt CR's
        # permittedHostDevices — there is no static gpuTypes config anymore.
        self._gpu_catalog_cache: Optional[tuple] = None
        # SSH host CA: the Secret the operator signs session host keys with
        # and whose public half the gateway hands users for their
        # `@cert-authority` line (see hostca.py). Lives in the system
        # namespace, like the gateway's own host key.
        self.ssh_ca_secret_name = os.environ.get(
            "WHISTLER_SSH_CA_SECRET_NAME", "whistler-ssh-ca")
        # Client-side domain suffix for jump addressing (`Host *.w` in
        # ~/.ssh/config). Nothing resolves it — it exists so one ssh_config
        # stanza can match every instance — but the gateway strips it and the
        # host certificates carry it as a principal, so the operator and the
        # gateway must agree on the string.
        self.ssh_domain_suffix = os.environ.get(
            "WHISTLER_SSH_DOMAIN_SUFFIX", ".w")
        # Default image for the streamer sidecar every desktop pod gets;
        # a template's streamerImage overrides it.
        self.streamer_image = os.environ.get(
            "WHISTLER_STREAMER_IMAGE",
            "ghcr.io/marma/whistler-streamer-selkies2:latest",
        )
        # Per-user NFS storage gateway (VM homes): image plus values-level
        # placement/limits, passed by the chart as JSON envs (nodeSelector
        # and resources are maps, which a plain env var can't carry).
        self.storage_gateway_image = os.environ.get(
            "WHISTLER_STORAGE_GATEWAY_IMAGE",
            "ghcr.io/marma/whistler-storage-gateway:latest",
        )
        self.storage_gateway_node_selector = self._env_json(
            "WHISTLER_STORAGE_GATEWAY_NODE_SELECTOR", {})
        self.storage_gateway_resources = self._env_json(
            "WHISTLER_STORAGE_GATEWAY_RESOURCES", {})
        # Default size of a VM's per-instance home disk. A template may raise
        # it per-session with `homeDiskSize`; this is the floor every VM gets.
        self.home_disk_size = os.environ.get(
            "WHISTLER_HOME_DISK_SIZE", "20Gi")
        # Hugepage size backing VM guest RAM (see DEFAULT_HUGE_PAGE_SIZE). A
        # template may pick another size — or opt out — with
        # `resources.hugePageSize`; empty here disables it everywhere.
        self.huge_page_size = os.environ.get(
            "WHISTLER_VM_HUGE_PAGE_SIZE", DEFAULT_HUGE_PAGE_SIZE).strip()
        # A size no node provides is not an error we can raise here (nothing
        # is scheduled yet) and not one KubeVirt catches either — the VM is
        # admitted and then pends on Insufficient hugepages-<size> forever. So
        # say it once, at startup, where it is still cheap to fix.
        if self.huge_page_size and self.huge_page_size not in ("2Mi", "1Gi"):
            logger.warning(
                f"VM hugepage size {self.huge_page_size!r} is not one x86_64 "
                f"provides (2Mi, 1Gi); VMs will pend unless these nodes are "
                f"arm64 with that size available")
        # S3 dataset proxies (design/storage.md).
        self.s3_proxy_image = os.environ.get(
            "WHISTLER_S3_PROXY_IMAGE", "rclone/rclone:latest")
        self.s3_proxy_resources = self._env_json(
            "WHISTLER_S3_PROXY_RESOURCES", {})

    @staticmethod
    def _env_json(name, default):
        raw = os.environ.get(name, "").strip()
        if not raw:
            return default
        try:
            return yaml.safe_load(raw) or default
        except yaml.YAMLError as e:
            logger.error(f"Invalid JSON in ${name}: {e}")
            return default

    def _get_user_namespace(self, username: str) -> str:
        return f"whistler-user-{username}"

    def _ensure_user_namespace(self, username: str) -> str:
        ns_name = self._get_user_namespace(username)
        core_api = CoreV1Api()
        net_api = NetworkingV1Api()

        # Ensure Namespace
        try:
            core_api.read_namespace(ns_name)
        except ApiException as e:
            if e.status == 404:
                logger.info(f"Creating namespace {ns_name}")
                ns_body = {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": ns_name,
                        "labels": {
                            USER_NS_LABEL: username,
                            "whistler.martinmalmsten.net/managed": "true"
                        }
                    }
                }
                core_api.create_namespace(ns_body)
            else:
                raise

        # Baseline NetworkPolicy: selects every pod (deny-by-default posture),
        # carrying only the ingress carve-outs and the egress every zone needs.
        # Zone-specific egress lives in the per-zone policies below — allows
        # are union'd across policies, so anything granted here is
        # irrevocable by a zone.
        policy_body = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "name": "isolate-user-pods",
                "namespace": ns_name
            },
            "spec": {
                "podSelector": {},
                "policyTypes": ["Ingress", "Egress"],
                "ingress": self._build_ingress_rules(),
                "egress": self._build_baseline_egress_rules()
            }
        }
        self._apply_network_policy(net_api, ns_name, policy_body)

        # Eager per-zone policies: every defined zone gets its policy in every
        # user namespace (an idle policy selecting no pods costs nothing, and
        # `kubectl get netpol` stays self-documenting).
        self._load_zones()
        self._apply_zone_policies(net_api, ns_name)

        return ns_name

    def _apply_zone_policies(self, net_api, ns_name: str):
        """Apply the current zone catalog to one user namespace: one policy
        per zone, pruning policies for zones that no longer exist so a
        deleted zone's allows don't linger. Shared by _ensure_user_namespace
        (connect/reconcile path) and _propagate_zone_policies (admin edits)."""
        for zone in self.zones:
            zone_policy = self._build_zone_network_policy(zone)
            zone_policy["metadata"]["namespace"] = ns_name
            self._apply_network_policy(net_api, ns_name, zone_policy)
        try:
            existing = net_api.list_namespaced_network_policy(
                ns_name, label_selector=f"{ZONE_POLICY_LABEL}=true")
            for pol in existing.items:
                zone = (pol.metadata.labels or {}).get(ZONE_LABEL)
                if zone not in self.zones:
                    logger.info(f"Pruning NetworkPolicy {pol.metadata.name} in "
                                f"{ns_name} (zone {zone!r} no longer defined)")
                    net_api.delete_namespaced_network_policy(
                        pol.metadata.name, ns_name)
        except ApiException as e:
            logger.error(f"Failed to prune stale zone policies in {ns_name}: {e}")

    @staticmethod
    def _apply_network_policy(net_api, ns_name: str, policy_body: Dict[str, Any]):
        name = policy_body["metadata"]["name"]
        logger.debug(f"Applying NetworkPolicy {name} in {ns_name}:\n"
                     f"{yaml.dump(policy_body, default_flow_style=False)}")
        try:
            net_api.read_namespaced_network_policy(name, ns_name)
            logger.info(f"Updating NetworkPolicy {name} in {ns_name}")
            net_api.replace_namespaced_network_policy(name, ns_name, policy_body)
        except ApiException as e:
            if e.status == 404:
                logger.info(f"Creating NetworkPolicy {name} in {ns_name}")
                net_api.create_namespaced_network_policy(ns_name, policy_body)
            else:
                raise

    def _load_users(self):
        # On failure, leave self.users as-is (stale-but-valid) rather than
        # wiping it to empty — a transient API error shouldn't lock out every
        # user. Also lets tests construct a KubeConfigManager via __new__ and
        # set cm.users directly, bypassing __init__ (no self.api) entirely.
        try:
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, self.namespace, USER_PLURAL
            )
        except (ApiException, AttributeError) as e:
            logger.error(f"Failed to load users: {e}")
            return
        self.users = {
            item["metadata"]["name"]: {**(item.get("spec") or {}), "name": item["metadata"]["name"]}
            for item in resp.get("items", [])
        }

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        self._load_users()
        # Falls back to a dummy object so callers that don't gate on
        # user_exists() first still get a usable default identity.
        return self.users.get(username, {"name": username})

    def user_exists(self, username: str) -> bool:
        self._load_users()
        return username in self.users

    def get_user_public_keys(self, username: str) -> List[str]:
        self._load_users()
        user = self.users.get(username)
        if user:
            return user.get("publicKeys", [])
        return []

        templates.sort(key=lambda x: x.get("source", ""))
        return templates

    def get_user_templates(self, username: str) -> List[Dict[str, Any]]:
        templates = []
        user_ns = self._get_user_namespace(username)
        namespaces_to_search = [self.namespace] # System namespace
        
        # If user namespace is different (likely always true), search it too
        if user_ns != self.namespace:
             namespaces_to_search.append(user_ns)

        for ns in namespaces_to_search:
            try:
                # List Templates (ssh access mode only — desktop templates are
                # surfaced separately via get_user_desktop_templates).
                resp = self.api.list_namespaced_custom_object(
                    self.group, self.version, ns, TEMPLATE_PLURAL
                )
                for item in resp.get("items", []):
                    t = item.get("spec", {})
                    if t.get("mode", "ssh") != "ssh":
                        continue
                    full_name = item["metadata"]["name"]

                    # Determine source and display name
                    owner = t.get("user", "system")
                    
                    # If fetching from system namespace, include only system templates
                    if ns == self.namespace:
                         if owner != "system" and owner != username: continue # Should not happen usually
                         # We include "system" templates. 
                         # What if a user puts their template in system NS? We might allow it or filter.
                         # Existing logic filtered by owner.
                         pass

                    if owner == "system":
                        t["name"] = full_name
                        t["fullName"] = full_name
                        t["displayName"] = t.get("displayName") or full_name
                        t["source"] = "system"
                        templates.append(t)
                    elif owner == username:
                        # Strip prefix if present
                        display_name = full_name
                        if full_name.startswith(f"{username}-"):
                            display_name = full_name[len(username)+1:]
                        t["name"] = display_name
                        t["fullName"] = full_name
                        t["displayName"] = t.get("displayName") or display_name
                        t["source"] = "user"
                        templates.append(t)
                    # Else: ignore other users' templates
            except ApiException as e:
                if e.status != 404:
                     logger.error(f"Failed to list templates in {ns}: {e}")
            
        # Deduplicate? If same name exists in both? 
        # For now, append all. Client might handle it or we assume distinct names.
        
        # Sort: system first
        templates.sort(key=lambda x: x.get("source", ""))
        return templates

    def get_user_instances(self, username: str) -> List[Dict[str, Any]]:
        instances = []
        user_ns = self._get_user_namespace(username)
        
        try:
            # List ssh Sessions in user namespace (desktop sessions are listed
            # by get_user_desktop_sessions).
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL,
                label_selector="whistler.martinmalmsten.net/mode=ssh",
            )

            # List Pods for this user
            core_api = client.CoreV1Api()
            try:
                pods = core_api.list_namespaced_pod(
                    user_ns, label_selector=f"user={username}"
                )
                pod_map = {p.metadata.labels.get("instance"): p for p in pods.items}
            except ApiException:
                pod_map = {}
            # ...and the VMIs, because an ssh-mode session is not necessarily a
            # pod: `mode: ssh, runtime: vm` (images/devbase) is a KubeVirt VM
            # reached over the jump host. Its launcher pod is virt-launcher-*
            # with KubeVirt's own labels, so it never matches the user=/instance=
            # selectors above — which used to leave `pod = None` and report a
            # perfectly healthy VM as "Stopped" forever. Same split as
            # get_user_desktop_sessions.
            vmi_map = {}
            if any((item.get("status") or {}).get("runtime") == "vm"
                   for item in resp.get("items", [])):
                try:
                    vmis = self.api.list_namespaced_custom_object(
                        KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns,
                        KUBEVIRT_VMI_PLURAL,
                    )
                    vmi_map = {v["metadata"]["name"]: v for v in vmis.get("items", [])}
                except ApiException:
                    vmi_map = {}

            for item in resp.get("items", []):
                spec = item.get("spec", {})
                status_obj = item.get("status", {}) or {}
                runtime = status_obj.get("runtime") or "container"
                full_name = item["metadata"]["name"]
                # Strip username prefix for display
                display_name = full_name
                if full_name.startswith(f"{username}-"):
                    display_name = full_name[len(username)+1:]

                pod = pod_map.get(full_name)

                pod_status = "Stopped" # Default if no pod
                pod_name = None
                pod_ip = None
                pod_ready = False
                vmi_name = None

                if runtime == "vm":
                    # The operator's phase vocabulary (Ready/Booting/Stopped/...),
                    # exactly as the desktop lister reports it — the CR status is
                    # the only live source for a VM here.
                    pod_status = status_obj.get("phase", "Unknown")
                    vmi_name = status_obj.get("vmiName")
                    pod_name = status_obj.get("podName")
                    pod_ip = status_obj.get("address")
                    vmi = vmi_map.get(full_name)
                    if vmi and (vmi.get("metadata") or {}).get("deletionTimestamp"):
                        pod_status = "Terminating"
                    pod_ready = pod_status == "Ready"
                elif pod:
                    pod_name = pod.metadata.name
                    pod_status = pod.status.phase
                    if pod.metadata.deletion_timestamp:
                        pod_status = "Terminating"
                    pod_ip = pod.status.pod_ip
                    statuses = pod.status.container_statuses or []
                    pod_ready = bool(statuses) and all(cs.ready for cs in statuses)

                # A deleting CR beats any derived phase (see the desktop lister):
                # the operator's timer skips deleting sessions, so status.phase
                # would otherwise sit at its last value for the whole teardown.
                if item["metadata"].get("deletionTimestamp"):
                    pod_status = "Terminating"

                mounts = []
                if pod and pod.spec and pod.spec.containers:
                        # Assume first container is the main one
                        # Python k8s client uses snake_case for attributes
                        for m in pod.spec.containers[0].volume_mounts or []:
                            # Skip service account tokens (usuall mounted at /var/run/secrets/...)
                            if not m.mount_path.startswith("/var/run/secrets"):
                                mounts.append({"name": m.name, "mountPath": m.mount_path})

                inst = {
                    "name": display_name,
                    "template": spec.get("templateRef"),
                    "status": pod_status,
                    "ready": pod_ready,
                    "podName": pod_name,
                    "vmiName": vmi_name,
                    "runtime": runtime,
                    "namespace": user_ns,
                    "ip": pod_ip,
                    "sshHost": None, 
                    "sshPort": None,
                    "mounts": mounts,
                    "preemptible": spec.get("preemptible", False)
                }
                instances.append(inst)
        except ApiException as e:
            if e.status != 404: # Namespace might not exist yet
                logger.error(f"Failed to list instances: {e}")
        return instances

    def add_instance(self, username: str, template_name: str, instance_name: str,
                     preemptible: bool = False,
                     overrides: Optional[Dict[str, Any]] = None,
                     ephemeral: bool = False,
                     home_volume: Optional[str] = None) -> bool:
        user_ns = self._ensure_user_namespace(username)

        spec = {
            "templateRef": template_name,
            "user": username,
            "preemptible": preemptible,
        }
        # Absent means "a home named after this instance", which is exactly
        # the pre-named-volumes behaviour — so the field stays unset rather
        # than being filled in with the default, and the default keeps working
        # for instances created before it existed.
        if home_volume:
            spec["homeVolume"] = home_volume
        if overrides:
            spec["overrides"] = overrides

        metadata = {
            "name": f"{username}-{instance_name}",
            "namespace": user_ns,
            # Denormalize access mode onto the CR so listing can filter
            # ssh vs desktop sessions cheaply (without resolving templates).
            "labels": {"whistler.martinmalmsten.net/mode": "ssh"},
        }
        if ephemeral:
            metadata["annotations"] = {EPHEMERAL_ANNOTATION: "true"}
        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "Session",
            "metadata": metadata,
            "spec": spec,
        }
        try:
            self.api.create_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, body
            )
            return True
        except ApiException as e:
            logger.error(f"Failed to create instance: {e}")
            return False

    def get_instance_config(self, username: str,
                            instance_name: str) -> Optional[Dict[str, Any]]:
        user_ns = self._get_user_namespace(username)
        full_name = f"{username}-{instance_name}"
        try:
            cr = self.api.get_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, full_name
            )
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read instance {full_name}: {e}")
            return None
        spec = cr.get("spec", {})
        return {
            "templateRef": spec.get("templateRef"),
            "preemptible": spec.get("preemptible", False),
            "homeVolume": spec.get("homeVolume"),
            "overrides": spec.get("overrides") or {},
        }

    def update_instance(self, username: str, instance_name: str,
                        preemptible: bool = False,
                        overrides: Optional[Dict[str, Any]] = None,
                        home_volume: Optional[str] = None) -> bool:
        user_ns = self._get_user_namespace(username)
        full_name = f"{username}-{instance_name}"
        try:
            cr = self.api.get_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, full_name
            )
        except ApiException as e:
            logger.error(f"Failed to read instance {full_name} for update: {e}")
            return False

        spec = cr.setdefault("spec", {})
        spec["preemptible"] = preemptible
        # Changing this swaps which disk becomes $HOME on the next start —
        # it does not move data. Cleared means the default (a home named after
        # the instance), same as at creation.
        if home_volume:
            spec["homeVolume"] = home_volume
        else:
            spec.pop("homeVolume", None)
        # Replace the overrides wholesale (a merge patch would leave stale nested
        # keys behind when a group is cleared), so drop the key entirely when the
        # form supplied no overrides.
        if overrides:
            spec["overrides"] = overrides
        else:
            spec.pop("overrides", None)

        try:
            self.api.replace_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, full_name, cr
            )
            logger.info(f"Updated instance {full_name}")
            return True
        except ApiException as e:
            logger.error(f"Failed to update instance {full_name}: {e}")
            return False

    def save_template(self, username: str, template_data: Dict[str, Any]) -> bool:
        # Creating templates via TUI in K8s mode might be restricted to admins
        # For now, we'll implement it as creating a WhistlerTemplate CR in user namespace
        name = template_data.get("name")
        if not name:
            return False

        # Prepend username for user templates to ensure uniqueness
        full_name = f"{username}-{name}"
        
        user_ns = self._ensure_user_namespace(username)
        
        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "Template",
            "metadata": {
                "name": full_name,
                "namespace": user_ns
            },
            "spec": {
                "user": username,
                "mode": template_data.get("mode", "ssh"),
                "runtime": template_data.get("runtime", "container"),
                "image": template_data.get("image"),
                "description": template_data.get("description"),
                "resources": template_data.get("resources"),
                "nodeSelector": template_data.get("nodeSelector"),
                "personalMountPath": template_data.get("personalMountPath"),
                "volumes": template_data.get("volumes")
            }
        }
        logger.debug(f"Saving template body: {body}")
        try:
            # Check if exists to update, or create
            try:
                # Preserve resourceVersion to update (replace).
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, user_ns, TEMPLATE_PLURAL, full_name
                )
                body["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]

                self.api.replace_namespaced_custom_object(
                    self.group, self.version, user_ns, TEMPLATE_PLURAL, full_name, body
                )
            except ApiException as e:
                if e.status == 404:
                    # Create
                    self.api.create_namespaced_custom_object(
                        self.group, self.version, user_ns, TEMPLATE_PLURAL, body
                    )
                else:
                    raise e
            return True
        except ApiException as e:
            logger.error(f"Failed to save template: {e}")
            return False

    def delete_instance(self, username: str, instance_name: str) -> bool:
        logger.info(f"Attempting to delete instance {username}-{instance_name}")
        user_ns = self._get_user_namespace(username)
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, f"{username}-{instance_name}"
            )
            return True
        except ApiException as e:
            logger.error(f"Failed to delete instance: {e}")
            return False

    def delete_template(self, username: str, template_name: str) -> bool:
        logger.info(f"Attempting to delete template {username}-{template_name}")
        user_ns = self._get_user_namespace(username)
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, user_ns, TEMPLATE_PLURAL, f"{username}-{template_name}"
            )
            return True
        except ApiException as e:
            logger.error(f"Failed to delete template: {e}")
            return False

    def _load_selectors(self):
        try:
            with open(SELECTORS_FILE, "r") as f:
                import yaml
                data = yaml.safe_load(f)
                if data:
                    self.selectors = data
        except FileNotFoundError:
            logger.warning(f"No selectors.yaml found at {SELECTORS_FILE}")
        except Exception as e:
            logger.error(f"Failed to load selectors: {e}")

    def get_selectors(self) -> Dict[str, Any]:
        return self.selectors

    # ------------------------------------------------------------------ #
    # GPU-type catalog — read from the cluster, no static config           #
    # ------------------------------------------------------------------ #

    _GPU_CATALOG_TTL = 30.0  # seconds; portal pages poll, nodes don't churn

    def _vm_gpu_resource_names(self) -> Set[str]:
        """Resource names KubeVirt permits as VM host devices — the authority
        on which device-plugin resources are attachable GPUs (a passthrough
        node also advertises the card's *audio* function as an allocatable
        resource, which must not be counted as a GPU). Empty when KubeVirt is
        not installed or permits nothing."""
        names: Set[str] = set()
        try:
            resp = self.api.list_cluster_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, "kubevirts")
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read KubeVirt permittedHostDevices: {e}")
            return names
        for kv in resp.get("items", []):
            permitted = ((kv.get("spec", {}) or {}).get("configuration", {})
                         or {}).get("permittedHostDevices", {}) or {}
            for kind in ("pciHostDevices", "mediatedDevices"):
                for dev in permitted.get(kind, []) or []:
                    rname = dev.get("resourceName")
                    if rname:
                        names.add(rname)
        return names

    def get_gpu_catalog(self) -> List[Dict[str, Any]]:
        """The live GPU-type catalog (see build_gpu_catalog), TTL-cached so
        every portal page render doesn't cost a node list."""
        cached = getattr(self, "_gpu_catalog_cache", None)
        if cached and time.monotonic() - cached[1] < self._GPU_CATALOG_TTL:
            return cached[0]
        try:
            node_items = client.CoreV1Api().list_node().items
        except ApiException as e:
            logger.error(f"Failed to list nodes for GPU catalog: {e}")
            # Serve stale over empty: a blip shouldn't blank the catalog.
            return cached[0] if cached else []
        nodes = [{
            "name": n.metadata.name,
            "labels": n.metadata.labels or {},
            "allocatable": n.status.allocatable or {},
        } for n in node_items]
        catalog = build_gpu_catalog(nodes, self._vm_gpu_resource_names())
        self._gpu_catalog_cache = (catalog, time.monotonic())
        return catalog

    def get_gpu_types(self) -> List[str]:
        return [entry["name"] for entry in self.get_gpu_catalog()]

    def _vm_gpu_device_name(self, gpu_type: Optional[str]) -> str:
        """Resolve the KubeVirt deviceName for a VM template's GPU request.

        ``gpu_type`` is the template's nodeSelector[GPU_NODE_LABEL] (absent
        on single-GPU-type clusters, where templates never needed to pin
        one). Fails closed with an actionable message rather than emitting a
        deviceName the scheduler can never satisfy."""
        catalog = self.get_gpu_catalog()
        if gpu_type:
            entry = next((e for e in catalog if e["name"] == gpu_type), None)
            if entry is None:
                raise PolicyError(
                    f"GPU type {gpu_type!r} is not present on any node "
                    f"(known types: {[e['name'] for e in catalog]})")
            if not entry.get("vmResource"):
                raise PolicyError(
                    f"GPU type {gpu_type!r} is not VM-attachable: no node "
                    f"advertises a KubeVirt-permitted resource for it. Is the "
                    f"device bound to vfio-pci and listed in the KubeVirt "
                    f"CR's permittedHostDevices?")
            return entry["vmResource"]
        vm_capable = [e for e in catalog if e.get("vmResource")]
        if len(vm_capable) == 1:
            return vm_capable[0]["vmResource"]
        if not vm_capable:
            raise PolicyError(
                "template requests a GPU but no VM-attachable GPU was "
                "discovered (no node advertises a KubeVirt-permitted "
                "host-device resource)")
        raise PolicyError(
            f"multiple VM GPU types available "
            f"({[e['name'] for e in vm_capable]}); the template must pin one "
            f"via its gpuType")

    def _vm_guest_memory(
            self, resources: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        """``(memory, hugepage size)`` for this VM's guest RAM — the two
        fields that have to agree. Pure (reads only self.huge_page_size).

        The template's ``resources.hugePageSize`` wins over the cluster
        default, and an explicit empty string is how a template opts out —
        which is why `in` decides here rather than truthiness.

        A guest with no memory request has nothing to back, so it gets no
        hugepages: KubeVirt's admission webhook rejects that pairing, and it
        would be rejected at VM *creation*, far from the template that caused
        it.

        Guest RAM must be a whole number of pages (that same webhook's rule)
        so the memory is **rounded up** to one here, never down: down would
        silently hand the user less RAM than the template asked for, and the
        overshoot is at most one page — 2 MiB at the default size. A request
        below one page becomes one page for the same reason. Rounding rather
        than refusing is what keeps `hugePages.pageSize` a cluster-level
        decision: raising it to 1Gi would otherwise invalidate every template
        whose memory is not a whole number of gigabytes."""
        size = (resources['hugePageSize'] if 'hugePageSize' in resources
                else self.huge_page_size)
        size = (size or "").strip()
        memory = resources.get('memory')
        if not memory:
            return None, None
        if not size:
            return memory, None
        try:
            page_bytes = int(parse_quantity(size))
            memory_bytes = int(parse_quantity(memory))
        except (ValueError, TypeError) as e:
            raise PolicyError(
                f"cannot size hugepages for this template: {e} "
                f"(memory={memory!r}, hugePageSize={size!r})")
        if page_bytes <= 0:
            raise PolicyError(f"invalid hugePageSize {size!r}")
        if memory_bytes <= 0:
            raise PolicyError(f"invalid memory {memory!r}")
        pages = -(-memory_bytes // page_bytes)  # ceil, >= 1
        rounded_bytes = pages * page_bytes
        if rounded_bytes == memory_bytes:
            return memory, size
        rounded = _format_quantity(rounded_bytes)
        logger.info(
            f"Rounding guest memory {memory} up to {rounded} "
            f"({pages} x {size} hugepages)")
        return rounded, size

    def _load_volumes(self):
        try:
            with open(VOLUMES_FILE, "r") as f:
                import yaml
                data = yaml.safe_load(f)
                if data and isinstance(data, list):
                    self.volumes = data
                    self.volume_definitions = {v['name']: v for v in data}
                else:
                    self.volumes = []
                    self.volume_definitions = {}
        except FileNotFoundError:
            logger.warning(f"No volumes.yaml found at {VOLUMES_FILE}")
        except Exception as e:
            logger.error(f"Failed to load volumes: {e}")
            self.volumes = []
            self.volume_definitions = {}

    def get_volumes(self):
        self._load_volumes()
        return self.volumes

    def _load_images(self):
        """Load the image allow-lists from images.yaml into self.images.

        New shape is a map keyed by template category ({ssh, desktop, vm}). A
        legacy flat list is accepted and treated as ssh suggestions. A missing
        file leaves the (empty) defaults — fine for host-process runs that don't
        mount it, and for ssh templates (unrestricted)."""
        self.images = {"ssh": [], "desktop": [], "vm": []}
        try:
            with open(IMAGES_FILE, "r") as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            return
        except Exception as e:
            logger.error(f"Failed to load images.yaml: {e}")
            return
        if isinstance(data, dict):
            for cat in ("ssh", "desktop", "vm"):
                self.images[cat] = [str(i) for i in (data.get(cat) or []) if i]
        elif isinstance(data, list):
            self.images["ssh"] = [str(i) for i in data if i]

    def get_available_images(self, category: Optional[str] = None) -> List[str]:
        """Image suggestions for UX. With a category, returns that list;
        otherwise the de-duplicated union across all categories."""
        self._load_images()
        if category:
            return list(self.images.get(category, []))
        union: List[str] = []
        for cat in ("ssh", "desktop", "vm"):
            for img in self.images.get(cat, []):
                if img not in union:
                    union.append(img)
        return union

    def _apply_policy(self, template_spec: Dict[str, Any], mode: str, runtime: str,
                      username: Optional[str] = None) -> str:
        """Authoritative, operator-side policy applied at build time.

        Returns the effective runtime (possibly coerced) and raises PolicyError
        when the template is not allowed:
          - privileged (or fuse) + runtime=container is coerced to kata when
            whistler.security.forceKataForPrivileged is on, so the privileged
            workload runs inside a lightweight VM rather than on the host kernel.
          - image allow-list is enforced when mode=desktop OR runtime=vm. SSH
            container/kata templates may use any image (the check is skipped).
          - GPU type (template_spec.nodeSelector[GPU_NODE_LABEL], set from the
            gpuTypes catalog by the template editor) must be named in the
            owning user's allowedGpuTypes, when username is given. An empty
            allow-list allows **nothing** (2026-08-25) — it used to mean "no
            restriction", which made an unconfigured user the least restricted
            one in the cluster.
          - Requested volumes (template_spec.volumes keys) must be named in
            the owning user's allowedVolumes the same way. Applies regardless
            of whether the volume came from the template or a session
            override (see _apply_overrides) — the merge happens before this
            runs.
          - A zone must exist in the zone catalog — an unknown zone fails
            closed rather than falling back to default, whose posture may be
            laxer — and must be named in the owning user's allowedZones. An
            *absent* zone is not un-zoned: the session lands in DEFAULT_ZONE,
            so that is the grant it is checked against. Under explicit access
            there is no such thing as a session outside the zone model, and a
            user granted no zone launches nothing."""
        effective_runtime = runtime
        # Desktops are a VM feature. A container session is a throwaway
        # workspace reached through the portal's web terminal; the streamed
        # desktop-in-a-pod worked and is deliberately retired rather than
        # maintained alongside the VM one (design/container_workloads.md).
        if mode == "desktop" and runtime != "vm":
            raise PolicyError(
                f"desktop mode needs runtime 'vm' (template asks for "
                f"{runtime!r}). Container sessions are web-terminal only — "
                f"see design/container_workloads.md"
            )

        wants_privileged = bool(template_spec.get("privileged") or template_spec.get("fuse"))
        if self.force_kata_for_privileged and wants_privileged and effective_runtime == "container":
            logger.warning(
                "Coercing runtime container -> kata for privileged template "
                "(image=%s); forceKataForPrivileged is enabled",
                template_spec.get("image"),
            )
            effective_runtime = "kata"

        if mode == "desktop" or effective_runtime == "vm":
            category = "vm" if effective_runtime == "vm" else "desktop"
            allowed = self.images.get(category, []) or []
            image = template_spec.get("image")
            image_url = template_spec.get("imageURL")
            if effective_runtime == "vm" and image and image_url:
                raise PolicyError(
                    "template sets both image (containerDisk) and imageURL "
                    "(CDI import); they are mutually exclusive"
                )
            # For vm templates the allow-list holds containerDisk refs AND
            # imageURL strings; whichever boot source is set must be listed.
            source = image_url or image
            if source not in allowed:
                raise PolicyError(
                    f"image {source!r} is not in the allowed {category} image list "
                    f"(whistler.images.{category}); allowed: {allowed}"
                )

        gpu_type = (template_spec.get("nodeSelector") or {}).get(GPU_NODE_LABEL)
        if gpu_type and username:
            allowed_gpu_types = self.get_user_allowed_gpu_types(username)
            if gpu_type not in allowed_gpu_types:
                raise PolicyError(
                    f"GPU type {gpu_type!r} is not allowed for user {username!r} "
                    f"(allowedGpuTypes: {allowed_gpu_types or 'none granted'})"
                )

        requested_volumes = template_spec.get("volumes") or {}
        if requested_volumes and username:
            allowed_volumes = self.get_user_allowed_volumes(username)
            disallowed = [v for v in requested_volumes if v not in allowed_volumes]
            if disallowed:
                raise PolicyError(
                    f"volumes {disallowed} are not allowed for user {username!r} "
                    f"(allowedVolumes: {allowed_volumes or 'none granted'})"
                )

        # An absent zone is DEFAULT_ZONE, not "no zone": that is where the
        # session actually lands, so that is what has to be granted.
        zone = template_spec.get("zone") or DEFAULT_ZONE
        if zone not in self.zones:
            # The catalog is admin-editable at runtime (Zone CRs); a miss
            # may just mean it changed since the last load. (A deleted
            # zone lingering in the cache is fail-safe the other way: its
            # policies are already pruned, so the label selects nothing
            # and the pod gets baseline-only egress.)
            self._load_zones()
        if zone not in self.zones:
            raise PolicyError(
                f"zone {zone!r} is not defined (whistler.zones); "
                f"defined zones: {sorted(self.zones)}"
            )
        if username:
            allowed_zones = self.get_user_allowed_zones(username)
            if zone not in allowed_zones:
                raise PolicyError(
                    f"zone {zone!r} is not allowed for user {username!r} "
                    f"(allowedZones: {allowed_zones or 'none granted'})"
                )
        return effective_runtime

    def _apply_overrides(self, template_spec: Dict[str, Any],
                         user_details: Optional[Dict[str, Any]],
                         overrides: Optional[Dict[str, Any]],
                         username: Optional[str]) -> "tuple[Dict[str, Any], Dict[str, Any]]":
        """Merge a session's requested spec.overrides into the template/user
        details used to build its pod or VM.

        Each key present in ``overrides`` requires the matching group in the
        owning user's User CR `overrides` (see get_user_overrides); an
        ungranted key raises PolicyError rather than being silently dropped —
        a live CR with an override the user isn't (or is no longer) granted
        is worth surfacing loudly. gpuType/volumes/zone values are further
        gated by allowedGpuTypes/allowedVolumes/allowedZones, but that happens
        afterwards in _apply_policy against the merged spec this method
        returns, so it applies uniformly whether the value came from the
        template or here.

        Returns (effective_template_spec, effective_user_details); neither
        input dict is mutated."""
        template_spec = template_spec or {}
        user_details = user_details or {}
        if not overrides:
            return template_spec, user_details

        granted = self.get_user_overrides(username) if username else {}

        def _require(group: str):
            if not granted.get(group):
                raise PolicyError(
                    f"user {username!r} is not granted the {group!r} override "
                    f"(User CR overrides.{group})"
                )

        effective_spec = dict(template_spec)
        effective_user = dict(user_details)

        if "resources" in overrides:
            _require("resources")
            requested = overrides["resources"] or {}
            merged_resources = dict(template_spec.get("resources") or {})
            for key in ("cpu", "memory"):
                if key in requested:
                    merged_resources[key] = requested[key]
            effective_spec["resources"] = merged_resources

        if "gpuType" in overrides:
            _require("gpuType")
            effective_spec["nodeSelector"] = {
                **(template_spec.get("nodeSelector") or {}),
                GPU_NODE_LABEL: overrides["gpuType"],
            }

        if "gpuCount" in overrides:
            _require("gpuCount")
            merged_resources = dict(effective_spec.get("resources")
                                    or template_spec.get("resources") or {})
            merged_resources["gpu"] = overrides["gpuCount"]
            effective_spec["resources"] = merged_resources

        if "uid" in overrides or "gid" in overrides:
            _require("uidGid")
            if "uid" in overrides:
                effective_user["uid"] = overrides["uid"]
            if "gid" in overrides:
                effective_user["gid"] = overrides["gid"]

        if "securityContext" in overrides:
            _require("securityContext")
            effective_user["securityContext"] = {
                **(user_details.get("securityContext") or {}),
                **(overrides["securityContext"] or {}),
            }

        if "volumes" in overrides:
            _require("volumes")
            effective_spec["volumes"] = dict(overrides["volumes"] or {})

        if "zone" in overrides:
            _require("zone")
            effective_spec["zone"] = overrides["zone"]

        return effective_spec, effective_user

    def _load_zones(self):
        """Load the zone catalog from Zone CRs (live, admin-editable via the
        portal; the chart renders whistler.zones values as Zone CRs).

        A zone is {egress: {allowCIDRs, blockCIDRs}, dns: {clusterOnly,
        servers}}; each becomes a per-user-namespace NetworkPolicy selecting
        pods labeled with the zone (see _build_zone_network_policy). The
        "default" zone always exists — synthesized when no default CR is
        defined, seeded from the legacy networkpolicy.yaml `egress` config
        so pre-zones values keep their meaning.

        On API failure, fall back to the mounted zones.yaml file (host-process
        dev without the CRD), and past that keep the previous catalog
        (stale-but-valid, like _load_users) rather than wiping it — a
        transient API error must not flip every session to a wrong zone."""
        try:
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, self.namespace, ZONE_PLURAL
            )
            zones = {
                item["metadata"]["name"]: (item.get("spec") or {})
                for item in resp.get("items", [])
            }
        except (ApiException, AttributeError) as e:
            logger.debug(f"Zone CRs unavailable ({e}); falling back to zones.yaml")
            zones = self._load_zones_file()
            if zones is None:  # file also unreadable: keep the stale catalog
                return

        if DEFAULT_ZONE not in zones:
            zones[DEFAULT_ZONE] = self._load_legacy_default_zone()

        self.zones = zones

    @staticmethod
    def _load_zones_file() -> Optional[Dict[str, Any]]:
        try:
            with open(ZONES_FILE, "r") as f:
                data = yaml.safe_load(f)
                if isinstance(data, dict):
                    return {str(name): (cfg or {}) for name, cfg in data.items()}
            return {}
        except FileNotFoundError:
            return {}
        except Exception as e:
            logger.error(f"Failed to load zones.yaml: {e}")
            return None

    @staticmethod
    def _load_legacy_default_zone() -> Dict[str, Any]:
        try:
            with open(NETWORKPOLICY_FILE, "r") as f:
                data = yaml.safe_load(f)
                if data and "egress" in data:
                    return {"egress": data["egress"]}
        except FileNotFoundError:
            pass  # default zone stays deny-all-egress-except-DNS
        except Exception as e:
            logger.error(f"Failed to load networkpolicy.yaml: {e}")
        return {}

    def get_zones(self) -> List[str]:
        self._load_zones()
        return sorted(self.zones.keys())

    def get_zone_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Full zone catalog (name -> config), freshly loaded — drives the
        admin zones editor."""
        self._load_zones()
        return {name: dict(cfg or {}) for name, cfg in self.zones.items()}

    def save_zone(self, zone_data: Dict[str, Any]) -> bool:
        """Create or update a Zone CR from the admin editor, then push the
        resulting NetworkPolicy to every managed user namespace so the change
        applies now, not at each user's next reconcile. (Running sessions keep
        their label; the rules behind that label change in place.)"""
        data = dict(zone_data)
        name = (data.pop("name", "") or "").strip()
        if not name:
            return False
        spec = {k: v for k, v in data.items() if v not in (None, "")}
        try:
            try:
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, self.namespace, ZONE_PLURAL, name
                )
                body = {
                    "apiVersion": f"{self.group}/{self.version}",
                    "kind": "Zone",
                    "metadata": {"name": name, "namespace": self.namespace,
                                 "resourceVersion": existing["metadata"]["resourceVersion"]},
                    # Replace, don't merge: the form always carries the full
                    # zone config, and a cleared field must actually clear.
                    "spec": spec,
                }
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, self.namespace, ZONE_PLURAL, name, body
                )
            except ApiException as e:
                if e.status != 404:
                    raise
                body = {
                    "apiVersion": f"{self.group}/{self.version}",
                    "kind": "Zone",
                    "metadata": {"name": name, "namespace": self.namespace},
                    "spec": spec,
                }
                self.api.create_namespaced_custom_object(
                    self.group, self.version, self.namespace, ZONE_PLURAL, body
                )
        except ApiException as e:
            logger.error(f"Failed to save zone {name!r}: {e}")
            return False
        self._propagate_zone_policies()
        return True

    def delete_zone(self, zone_name: str) -> bool:
        """Delete a Zone CR and prune its NetworkPolicy from every managed
        user namespace. The default zone is undeletable — it is the fallback
        every unzoned template lands in. Sessions still referencing a deleted
        zone fail closed at their next build (_apply_policy)."""
        if zone_name == DEFAULT_ZONE:
            logger.warning("Refusing to delete the default zone")
            return False
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, self.namespace, ZONE_PLURAL, zone_name
            )
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to delete zone {zone_name!r}: {e}")
                return False
        self._propagate_zone_policies()
        return True

    def _propagate_zone_policies(self):
        """Re-apply the (freshly loaded) zone catalog to every managed user
        namespace: update/create each zone's policy, prune deleted ones.
        Best-effort — a namespace that fails here is corrected by its next
        _ensure_user_namespace, and new namespaces always get the current
        catalog."""
        self._load_zones()
        core_api = CoreV1Api()
        net_api = NetworkingV1Api()
        try:
            namespaces = core_api.list_namespace(
                label_selector="whistler.martinmalmsten.net/managed=true")
        except ApiException as e:
            logger.error(f"Failed to list user namespaces for zone propagation: {e}")
            return
        for ns in namespaces.items:
            try:
                self._apply_zone_policies(net_api, ns.metadata.name)
            except ApiException as e:
                logger.error(f"Failed to propagate zone policies to "
                             f"{ns.metadata.name}: {e}")

    def _build_ingress_rules(self) -> list:
        """Ingress for a user namespace: deny everything except the two trusted
        brokers. Without these carve-outs the round-1 deny-all-ingress policy
        would block them and the desktop would never render.

        The portal (websockets viewer) reverse-proxies the browser to the in-pod
        Selkies HTTP/WebSocket server on the pod's display port. It is pinned by
        namespace + pod label; no port is pinned because the display port varies
        per template and the portal only ever dials that one port anyway.

        The SSH gateway reaches session sshd for jump routing and the relay
        (design/proxyjump.md), and unlike the portal it *is* port-pinned: 22 is
        the only thing it ever dials, and a rule that permitted more would make
        the gateway a route to every port in the namespace.

        Both are separate rules, not one rule with two peers, because a
        NetworkPolicy rule's `from` and `ports` are ANDed — folding them
        together would either drop the gateway's port pin or impose it on the
        portal.

        Note this is the baseline policy, which zones can never narrow
        (NetworkPolicy allows are union'd). A zone's `none` SSH posture is
        therefore enforced in the gateway, which is legitimate — the gateway is
        the only route in — but it is enforcement by a trusted component rather
        than by the network. See design/proxyjump.md."""
        broker_ns = os.environ.get("PORTAL_NAMESPACE", self.namespace)
        ns_selector = {"matchLabels": {"kubernetes.io/metadata.name": broker_ns}}
        return [
            {
                "from": [
                    {"namespaceSelector": ns_selector,
                     "podSelector": {"matchLabels": {"app": "whistler-portal"}}},
                ]
            },
            {
                "from": [
                    {"namespaceSelector": ns_selector,
                     "podSelector": {"matchLabels": {"app": "whistler-server"}}},
                ],
                "ports": [{"port": SESSION_SSH_PORT, "protocol": "TCP"}],
            },
        ]

    def _build_baseline_egress_rules(self) -> list:
        """Egress every pod in a user namespace gets, regardless of zone.
        Deliberately minimal: NetworkPolicy allows are union'd across all
        policies selecting a pod, so anything granted here can never be
        revoked by a zone. DNS is per-zone (a zone may pin resolvers)."""
        return [
            # Session pods (including VM virt-launcher pods, whose guest
            # traffic is masqueraded through them) may reach the user's own
            # storage gateway on NFS. Same-namespace podSelector; the
            # gateway's dedicated policy (_build_gateway_network_policy)
            # narrows the ingress side. One port is the whole rule because
            # the export is NFSv4-only — no rpcbind, no NLM, no statd.
            {
                "to": [{"podSelector": {
                    "matchLabels": {"app": "whistler-storage-gateway"}}}],
                "ports": [{"port": 2049, "protocol": "TCP"}],
            },
            # Session pods may reach the shared-dataset S3 proxies, which live
            # in Whistler's own namespace rather than the user's (a dataset is
            # shared, so its proxy cannot belong to one user).
            #
            # This is deliberately a BASELINE allow, so a zone cannot revoke
            # it — NetworkPolicy allows are union'd, so anything here is
            # irrevocable by a zone. That is safe only because reaching a
            # proxy is not the same as reaching a dataset: each proxy's own
            # policy (_build_s3_proxy_network_policy) admits only the users
            # actually granted it, and fails closed when that list is empty.
            {
                "to": [{
                    "namespaceSelector": {"matchLabels": {
                        "kubernetes.io/metadata.name": self.namespace}},
                    "podSelector": {"matchLabels": {
                        "app": "whistler-s3-proxy"}},
                }],
                "ports": [{"port": 8080, "protocol": "TCP"}],
            },
        ]

    def _build_egress_rules(self, zone: str) -> list:
        zone_cfg = self.zones.get(zone) or {}
        egress = zone_cfg.get("egress") or {}
        rules = []

        # DNS. Default: port 53 anywhere (pods must resolve hostnames). A zone
        # may narrow it — clusterOnly pins it to the cluster resolver (closing
        # direct-to-Internet DNS tunnels; upstream names still resolve through
        # CoreDNS forwarding), servers pins it to specific resolver IPs (which
        # _apply_zone_dns also steers the pod's resolv.conf at).
        dns = zone_cfg.get("dns") or {}
        dns_ports = [{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}]
        dns_to = []
        if dns.get("clusterOnly"):
            dns_to.append({
                "namespaceSelector": {"matchLabels": {
                    "kubernetes.io/metadata.name": CLUSTER_DNS_NAMESPACE}},
                "podSelector": {"matchLabels": dict(CLUSTER_DNS_POD_LABELS)},
            })
        for server in dns.get("servers") or []:
            dns_to.append({"ipBlock": {"cidr": f"{server}/32"}})
        dns_rule = {"ports": dns_ports}
        if dns_to:
            dns_rule["to"] = dns_to
        rules.append(dns_rule)

        # Whitelist: explicit allow rules per destination CIDR
        for entry in egress.get("allowCIDRs", []) or []:
            rule = {"to": [{"ipBlock": {"cidr": entry["cidr"]}}]}
            if "ports" in entry:
                rule["ports"] = entry["ports"]
            rules.append(rule)

        # Blacklist: compute the complement CIDRs explicitly rather than using
        # ipBlock.except, which is silently ignored by several CNI plugins.
        block_cidrs = egress.get("blockCIDRs", []) or []
        if block_cidrs:
            remaining = [ipaddress.IPv4Network("0.0.0.0/0")]
            for block in block_cidrs:
                block_net = ipaddress.IPv4Network(block, strict=False)
                new_remaining = []
                for net in remaining:
                    if net.overlaps(block_net):
                        new_remaining.extend(net.address_exclude(block_net))
                    else:
                        new_remaining.append(net)
                remaining = new_remaining
            allowed = [str(net) for net in remaining]
            logger.debug(f"blockCIDRs {block_cidrs} → complement allowCIDRs: {allowed}")
            rules.append({"to": [{"ipBlock": {"cidr": cidr}} for cidr in allowed]})

        return rules

    def _zone_config_hash(self, zone: str) -> str:
        """Short digest of a zone's config, stamped on workloads at build time
        (ZONE_HASH_ANNOTATION) so the rules a session was built under stay
        auditable after the zone definition changes."""
        canonical = json.dumps(self.zones.get(zone) or {}, sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()[:12]

    def _build_zone_network_policy(self, zone: str) -> Dict[str, Any]:
        """One NetworkPolicy per zone per user namespace, selecting pods by
        the zone label. Egress-only: ingress is owned entirely by the baseline
        isolate-user-pods policy (portal carve-out) and the gateway's own
        policy — a zone never widens what can reach a pod."""
        return {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "name": f"whistler-zone-{zone}",
                "labels": {ZONE_POLICY_LABEL: "true", ZONE_LABEL: zone},
            },
            "spec": {
                "podSelector": {"matchLabels": {ZONE_LABEL: zone}},
                "policyTypes": ["Egress"],
                "egress": self._build_egress_rules(zone),
            },
        }

    def _apply_zone_dns(self, pod_spec: Dict[str, Any], zone: str) -> None:
        """Steer a pod (or VMI template) spec at a zone's forced resolvers.
        Enforcement is the zone's port-53 egress rule; this makes resolution
        actually work against those servers. VMs inherit it too: KubeVirt's
        masquerade DHCP advertises the launcher pod's resolv.conf to the
        guest. Note cluster-internal names (e.g. the storage gateway Service)
        only resolve if the forced server handles them — a zone combining
        dns.servers with runtime=vm needs a resolver that forwards cluster
        zones, or clusterOnly instead."""
        servers = ((self.zones.get(zone) or {}).get("dns") or {}).get("servers")
        if servers:
            pod_spec["dnsPolicy"] = "None"
            pod_spec["dnsConfig"] = {"nameservers": [str(s) for s in servers]}

    def _ensure_pvc(self, user, namespace, logger=None):
        pvc_name = f"whistler-data-{user}"
        api = client.CoreV1Api()
        
        try:
            api.read_namespaced_persistent_volume_claim(pvc_name, namespace)
            return pvc_name
        except ApiException as e:
            if e.status != 404:
                raise
        
        # Create PVC
        if logger: logger.info(f"Creating PVC {pvc_name} for user {user}")
        
        access_mode = os.environ.get("USER_VOLUME_ACCESS_MODE", "ReadWriteMany")
        volume_size = os.environ.get("USER_VOLUME_SIZE", "10Gi")
        pvc_body = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": pvc_name,
                "labels": {
                    "app": "whistler",
                    "user": user
                }
            },
            "spec": {
                "accessModes": [access_mode],
                "resources": {
                    "requests": {
                        "storage": volume_size
                    }
                }
            }
        }
        
        try:
            api.create_namespaced_persistent_volume_claim(namespace, pvc_body)
            if logger: logger.info(f"PVC {pvc_name} created")
            return pvc_name
        except ApiException as e:
            if logger: logger.error(f"Failed to create PVC: {e}")
            raise

    # ------------------------------------------------------------------ #
    # Home volumes (design/security.md, "Core model: the access matrix")  #
    #                                                                     #
    # A home is a `disk.img` on a PVC attached as a virtio-blk disk. It   #
    # used to be created per instance and owner-referenced to the Session #
    # so GC reaped the two together; it is now a NAMED object a user owns #
    # and an instance selects, because per-instance homes closed the      #
    # cross-zone hole by removing the choice — which also forbade the     #
    # cases a lab actually needs (one home per zone, the open one         #
    # readable from the restricted instance).                             #
    #                                                                     #
    # The default preserves the old behaviour exactly: an instance with   #
    # no `homeVolume` gets one named after itself.                        #
    # ------------------------------------------------------------------ #

    def get_home_volumes(self, username: str) -> List[Dict[str, Any]]:
        """This user's home volumes, newest API state, sorted by name."""
        user_ns = self._get_user_namespace(username)
        try:
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, user_ns, HOME_VOLUME_PLURAL
            )
        except ApiException as e:
            if e.status == 404 and not self._warned_no_home_volume_crd:
                type(self)._warned_no_home_volume_crd = True
                logger.warning(
                    "No HomeVolume CRD in this cluster, so instances fall "
                    "back to a home named after themselves. `helm upgrade` "
                    "does not update CRDs — run "
                    "`kubectl apply -f charts/whistler/crds/crds.yaml`")
            else:
                logger.error(f"Failed to list home volumes for {username}: {e}")
            return []
        except AttributeError:
            return []
        type(self)._warned_no_home_volume_crd = False
        out = [{**(item.get("spec") or {}),
                "name": item["metadata"]["name"]}
               for item in resp.get("items", [])]
        return sorted(out, key=lambda v: v.get("name") or "")

    def get_home_volume(self, username: str, name: str) -> Optional[Dict[str, Any]]:
        for vol in self.get_home_volumes(username):
            if vol.get("name") == name:
                return vol
        return None

    def save_home_volume(self, username: str, volume: Dict[str, Any]) -> bool:
        """Create or update a HomeVolume CR. The backing PVC is NOT created
        here — it is created on first attach, so a volume that is never used
        costs nothing and a storage class that cannot bind fails where the
        user can see it."""
        data = dict(volume)
        name = (data.pop("name", "") or "").strip()
        if not name:
            return False
        user_ns = self._ensure_user_namespace(username)
        spec = {k: v for k, v in data.items() if v not in (None, "")}
        spec["user"] = username
        body = {"apiVersion": f"{self.group}/{self.version}",
                "kind": "HomeVolume",
                "metadata": {"name": name, "namespace": user_ns},
                "spec": spec}
        try:
            try:
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, user_ns, HOME_VOLUME_PLURAL, name)
                body["metadata"]["resourceVersion"] = \
                    existing["metadata"]["resourceVersion"]
                # Carry the claim forward: it is the identity of the data, and
                # losing it would silently hand the user an empty home.
                if existing.get("spec", {}).get("pvcName") and \
                        "pvcName" not in spec:
                    spec["pvcName"] = existing["spec"]["pvcName"]
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, user_ns, HOME_VOLUME_PLURAL,
                    name, body)
            except ApiException as e:
                if e.status != 404:
                    raise
                self.api.create_namespaced_custom_object(
                    self.group, self.version, user_ns, HOME_VOLUME_PLURAL, body)
        except ApiException as e:
            logger.error(f"Failed to save home volume {name!r}: "
                         f"{crd_missing_hint(HOME_VOLUME_PLURAL, e)}")
            return False
        return True

    def delete_home_volume(self, username: str, name: str,
                           delete_data: bool = False) -> bool:
        """Delete a HomeVolume. The PVC is kept unless ``delete_data``.

        Keeping it is the default because this is the user's home directory
        and a dropdown should not be able to destroy it; the claim is left
        behind, still adoptable by recreating a volume with the same
        `pvcName`. Refuses while the volume is attached to a running
        instance."""
        user_ns = self._get_user_namespace(username)
        volume = self.get_home_volume(username, name)
        if not volume:
            return False
        holder = self.home_volume_holder(username, volume)
        if holder:
            logger.warning(
                f"Refusing to delete home volume {name!r}: in use by {holder}")
            return False
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, user_ns, HOME_VOLUME_PLURAL, name)
        except ApiException as e:
            logger.error(f"Failed to delete home volume {name!r}: {e}")
            return False
        # Drop its cells so the grid does not accumulate rows for volumes
        # that no longer exist. Best-effort: a stale row grants access to a
        # name nothing resolves, which is inert.
        try:
            self.revoke_own_volume_access(username, name)
        except Exception as e:
            logger.warning(f"Could not clear access rows for {name!r}: {e}")
        if delete_data:
            pvc = self.home_volume_pvc_name(volume)
            try:
                client.CoreV1Api().delete_namespaced_persistent_volume_claim(
                    pvc, user_ns)
                logger.info(f"Deleted home volume claim {pvc}")
            except ApiException as e:
                if e.status != 404:
                    logger.error(f"Failed to delete claim {pvc}: {e}")
                    return False
        return True

    def home_volume_holder(self, username: str,
                           volume: Dict[str, Any],
                           ignore_instance: str = None) -> Optional[str]:
        """The RUNNING instance currently holding this volume, or None.

        Derived from the cluster rather than stored on the CR, for the same
        reason session phase is: a stored attachment goes stale the moment
        anything happens out of band, and a stale one either blocks a start
        that should succeed or permits one that should not.

        "Running" means a VMI exists. A stopped VM still *references* the
        claim in its spec and that is fine — the rule is one live attach, not
        one reference.
        """
        user_ns = self._get_user_namespace(username)
        pvc_name = self.home_volume_pvc_name(volume)
        try:
            vmis = self.api.list_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns, "virtualmachineinstances")
            running = {i["metadata"]["name"] for i in vmis.get("items", [])}
            if not running:
                return None
            vms = self.api.list_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns, KUBEVIRT_VM_PLURAL)
        except (ApiException, AttributeError) as e:
            # Fail OPEN here, deliberately: this check exists to prevent
            # filesystem incoherence, not to enforce a security boundary, and
            # an API blip that blocked every start would be worse than the
            # rare double attach it would prevent. The security rule that must
            # fail closed is the access matrix, not this.
            logger.warning(f"Could not determine home volume holders: {e}")
            return None
        for vm in vms.get("items", []):
            name = vm["metadata"]["name"]
            if name not in running or name == ignore_instance:
                continue
            for vol in ((vm.get("spec", {}).get("template", {})
                         .get("spec", {}).get("volumes")) or []):
                claim = (vol.get("persistentVolumeClaim") or {}).get("claimName")
                if claim == pvc_name:
                    return name
        return None

    def resolve_session_home_volume(self, username: str, full_name: str,
                                    requested: str = None,
                                    default_size: str = None,
                                    zone: str = None,
                                    logger=None) -> Dict[str, Any]:
        """The home volume an instance should attach, creating the default one
        if needed. Raises PolicyError when the request cannot be honoured.

        Two cases:
        - A named request must already exist. Silently creating one would turn
          a typo into a brand-new empty home, which looks exactly like data
          loss to the person it happens to.
        - No request means the pre-named-volumes behaviour: a volume named
          after the instance, created on demand.
        """
        if requested:
            volume = self.get_home_volume(username, requested)
            if not volume:
                raise PolicyError(
                    f"Home volume '{requested}' does not exist for user "
                    f"{username}. Create it first, or leave the field empty "
                    f"for a home dedicated to this instance.")
            return volume
        volume = self.get_home_volume(username, full_name)
        if volume:
            return volume
        # The default home for this instance. Named after the instance and
        # carrying its legacy claim name, so an instance that predates named
        # volumes keeps the disk it already has.
        spec = {"name": full_name,
                "description": f"Home for instance {full_name}",
                "pvcName": f"whistler-home-{full_name}"}
        if default_size:
            spec["size"] = default_size
        if not self.save_home_volume(username, spec):
            raise PolicyError(
                f"Could not create the default home volume for {full_name}.")
        # Grant it in the zone this instance runs in. Not a widening: the
        # instance is already permitted there, and before named volumes it
        # would simply have been handed this disk with no grant at all.
        # Without this the matrix refuses every default home — which is every
        # instance that has not chosen one.
        if zone:
            self.grant_own_volume_access(username, zone, full_name)
        if logger:
            logger.info(f"Created default home volume {full_name} "
                        f"(granted in zone {zone or 'none'})")
        return {**spec, "user": username}

    def ensure_home_volume_pvc(self, username: str, volume: Dict[str, Any],
                               fallback_size: str = None,
                               logger=None) -> str:
        """Ensure a home volume's claim exists, returning its name.

        A VM cannot mount a PVC, so the home is a `disk.img` on this claim
        attached as a virtio-blk disk and formatted ext4 by the guest (see
        design/storage.md, and cloudinit.build_user_data for the guest side).
        `volumeMode: Filesystem` is deliberate and is what puts a `disk.img`
        on the share — it is also the only mode `csi-driver-nfs` can serve.

        **No ownerReference.** The claim outlives any one Session now; that is
        the whole point of naming it. Deleting an instance no longer deletes
        the user's home.
        """
        user_ns = self._get_user_namespace(username)
        pvc_name = self.home_volume_pvc_name(volume)
        api = client.CoreV1Api()
        try:
            api.read_namespaced_persistent_volume_claim(pvc_name, user_ns)
            return pvc_name
        except ApiException as e:
            if e.status != 404:
                raise

        if logger:
            logger.info(f"Creating home volume claim {pvc_name}")
        spec = {
            # RWO: exactly one VM attaches this disk. Sharing a block device
            # between writers corrupts it, and nothing here would notice —
            # see design/storage.md on why shared homes need a file-level
            # share instead. home_volume_holder is what actually prevents it;
            # RWO is per-node and would not.
            "accessModes": ["ReadWriteOnce"],
            "volumeMode": "Filesystem",
            "resources": {"requests": {
                "storage": (volume.get("size") or fallback_size
                            or self.home_disk_size)}},
        }
        if volume.get("storageClassName"):
            spec["storageClassName"] = volume["storageClassName"]
        pvc_body = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": pvc_name,
                "labels": {"app": "whistler",
                           "whistler-home-volume": volume.get("name", "")[:63]},
            },
            "spec": spec,
        }
        try:
            api.create_namespaced_persistent_volume_claim(user_ns, pvc_body)
            return pvc_name
        except ApiException as e:
            if e.status == 409:
                return pvc_name
            if logger:
                logger.error(f"Failed to create home volume claim: {e}")
            raise

    def _load_volume_definitions_from_file(self):
        try:
            with open(VOLUMES_FILE, "r") as f:
                data = yaml.safe_load(f)
                return {v['name']: v for v in data} if data else {}
        except Exception:
            return {}

    def _build_volume_wiring(self, *, pvc_name, personal_mount_path,
                             requested_volumes, available_volumes,
                             volume_modes=None):
        """Build (pod_volumes, volume_mounts) for the home PVC plus any requested
        named volumes. Pure; the single source shared by every pod backend
        (ssh / desktop, container / kata).

        ``volume_modes`` ({name: "rw"|"ro"}) carries the per-member access a
        Group granted (KubeConfigManager.get_user_volume_modes); a volume not
        named there is read-write, which is what every volume was before
        groups existed. A read-only grant becomes ``readOnly`` on the mount —
        a real control for a container that cannot remount it, and one the
        NFS export set will have to take over for VMs, where the guest has
        root (design/security.md)."""
        pod_volumes = [{
            "name": "data",
            "persistentVolumeClaim": {"claimName": pvc_name},
        }]
        volume_mounts = [{
            "name": "data",
            "mountPath": personal_mount_path,
        }]

        for vol_name, mount_path in (requested_volumes or {}).items():
            if vol_name in available_volumes:
                # TODO: Remove this hack, we should not have a hardcoded volume named "data"
                if vol_name == "data":
                    continue

                vol_def = available_volumes[vol_name]
                sub_path = vol_def.get("subPath")

                # Copy so the source definition in available_volumes is not mutated;
                # subPath belongs on the mount, not the volume.
                clean_vol_def = vol_def.copy()
                clean_vol_def.pop("subPath", None)
                pod_volumes.append(clean_vol_def)

                mount_def = {"name": vol_name, "mountPath": mount_path}
                if sub_path:
                    mount_def["subPath"] = sub_path
                if (volume_modes or {}).get(vol_name) == "ro":
                    mount_def["readOnly"] = True
                volume_mounts.append(mount_def)

        return pod_volumes, volume_mounts

    def _session_owner_reference(self, full_name: str, uid: str) -> Dict[str, Any]:
        """ownerReference making a Session the controller of its child pod / VM /
        Service, so Kubernetes GC reaps them when the CR is deleted."""
        return {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "Session",
            "name": full_name,
            "uid": uid,
            "controller": True,
            "blockOwnerDeletion": True,
        }

    def _build_pod_spec(self, *, full_name, hostname, username, uid, mode, runtime,
                        template_spec, pvc_name, available_volumes, user_details,
                        preemptible, display_port=None, volume_modes=None):
        """Build the Pod manifest for a session from already-resolved inputs.

        Pure function of its arguments (no Kubernetes API calls) so it is
        unit-tested without a cluster; ``ensure_session`` does the API work.
        Builds the container and kata runtimes (runtime=vm is built by
        ``_build_vm_spec`` instead):
          - the entrypoint is overridden with ``sleep``: a session pod's job
            is to sit there until someone execs into it.
          - both ``instance`` and ``session`` labels are emitted so pod
            watches and Service selectors both resolve.
          - runtime=kata pins the configured Kata RuntimeClass.

        Container sessions are **web terminal only**. The streamed
        desktop-in-a-pod (a Selkies sidecar owning Xvfb/PulseAudio, sharing
        sockets with the workload over emptyDirs) was built, worked, and is
        deliberately retired — the mechanism is written up in full in
        design/container_workloads.md, which is also where the case for
        bringing it back for *single-app* containers lives. ``_apply_policy``
        refuses desktop mode for non-VM runtimes, so display_port never
        reaches here.
        """
        image = template_spec.get('image', 'ubuntu:latest')
        resources = template_spec.get('resources', {})
        node_selector = template_spec.get('nodeSelector', {})
        personal_mount_path = template_spec.get('personalMountPath', '/userdata')
        requested_volumes = template_spec.get('volumes', {}) or {}

        resource_reqs = self._build_resource_reqs(resources)
        pod_volumes, volume_mounts = self._build_volume_wiring(
            pvc_name=pvc_name,
            personal_mount_path=personal_mount_path,
            requested_volumes=requested_volumes,
            available_volumes=available_volumes,
            volume_modes=volume_modes,
        )

        container = {
            "name": "main",
            "image": image,
            "resources": resource_reqs,
            "volumeMounts": volume_mounts,
        }
        # Keep the pod alive: a session image's job is to sit there until
        # someone execs into it, and most have an entrypoint that would exit
        # immediately.
        container["command"] = ["sleep", "3600"]

        app_label = "whistler-instance" if mode == "ssh" else "whistler-desktop"
        zone = template_spec.get('zone') or DEFAULT_ZONE
        pod_body = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": full_name,
                "labels": {
                    "app": app_label,
                    # Both label keys so the SSH watch (instance=) and the desktop
                    # Service selector (session=) resolve regardless of mode.
                    "instance": full_name,
                    "session": full_name,
                    "user": username,
                    # Selects the zone's NetworkPolicy; stamped at build time,
                    # so a session changes zone on reboot, never live.
                    ZONE_LABEL: zone,
                },
                "annotations": {
                    ZONE_HASH_ANNOTATION: self._zone_config_hash(zone),
                },
                "ownerReferences": [self._session_owner_reference(full_name, uid)],
            },
            "spec": {
                "containers": [container],
                "volumes": pod_volumes,
                "nodeSelector": node_selector,
                "hostname": hostname,
                "subdomain": "whistler",
                "automountServiceAccountToken": False,
                # ssh pods run `sleep` as PID 1, which never exits on SIGTERM,
                # so the default 30s grace is always burned in full; nothing in
                # a session needs a long drain either way.
                "terminationGracePeriodSeconds": 5,
            },
        }
        self._apply_zone_dns(pod_body["spec"], zone)

        if user_details and "securityContext" in user_details:
            pod_body["spec"]["securityContext"] = user_details["securityContext"]

        # Some images (e.g. gnome-grd) mount a FUSE filesystem at runtime or run
        # systemd as PID 1; grant privileged when the template asks. In production
        # _apply_policy will have coerced runtime to kata so this is host-isolated.
        if template_spec.get('fuse') or template_spec.get('privileged'):
            sec_ctx = container.setdefault("securityContext", {})
            sec_ctx["privileged"] = True

        if runtime == "kata":
            pod_body["spec"]["runtimeClassName"] = getattr(self, "kata_runtime_class", "kata")
        elif resources.get('gpu'):
            # Kata handles its own device passthrough story, so this only
            # applies to the plain-container path — see gpu_runtime_class.
            gpu_runtime_class = getattr(self, "gpu_runtime_class", "nvidia")
            if gpu_runtime_class:
                pod_body["spec"]["runtimeClassName"] = gpu_runtime_class

        if preemptible:
            pod_body["spec"]["priorityClassName"] = "whistler-preemptible"

        return pod_body


    # ------------------------------------------------------------------ #
    # Session backends: plain pod, Kata pod, or a KubeVirt VM — shared by  #
    # ssh and desktop access modes. Provisioning + lifecycle; the desktop  #
    # display relay (websockets viewer) lives in the portal. These reuse    #
    # common spine (_ensure_user_namespace, _ensure_pvc, _build_egress).   #
    # ------------------------------------------------------------------ #

    def _build_resource_reqs(self, resources: Dict[str, Any]) -> Dict[str, Any]:
        """Map a template's resources{cpu,memory,gpu} to a Kubernetes
        ResourceRequirements dict. Shared by the SSH and desktop pod specs."""
        resource_reqs = {}
        if resources:
            requests = {}
            limits = {}
            if 'cpu' in resources:
                requests['cpu'] = resources['cpu']
                limits['cpu'] = resources['cpu']
            if 'memory' in resources:
                requests['memory'] = resources['memory']
                limits['memory'] = resources['memory']
            if 'gpu' in resources:
                limits['nvidia.com/gpu'] = resources['gpu']
            if requests:
                resource_reqs['requests'] = requests
            if limits:
                resource_reqs['limits'] = limits
        return resource_reqs

    def _build_vm_spec(self, *, session_name, hostname, username, uid,
                       template_spec, display_port, instancetype,
                       preemptible, home_pvc=None, shared_datasets=None,
                       user_details=None, run_strategy="Halted",
                       portal_public_key=None, viewer=None,
                       host_key=None, host_cert=None):
        """Build a KubeVirt VirtualMachine manifest from resolved inputs.

        Pure (no API calls). The VMI launcher pod inherits the template labels
        so the per-session Service can select it.

        Boot source: `image` is a containerDisk (OCI-wrapped qcow2, ephemeral
        root), `imageURL` an HTTP qcow2/raw imported by CDI into a per-session
        root PVC via dataVolumeTemplates. ``home_pvc`` is the per-INSTANCE
        home-disk claim (_ensure_home_disk_pvc), attached as a second
        virtio-blk disk and formatted ext4 by the guest; None for ephemeral
        sessions, whose home stays on the root disk. The user's per-user PVC
        is NOT attached — it serves pod sessions only.

        This replaced an NFS mount from the per-user storage gateway. Ganesha
        cannot re-export an NFS-backed PVC (FSAL_VFS refuses; FSAL_PROXY_V4
        overflows its read buffer in every released version), and virtiofs is
        no help either because KubeVirt runs virtiofsd unprivileged
        (kubevirt#13028). See design/storage.md. cloud-init creates the real user
        (username/uid/keys); serial console + VNC graphics rely on KubeVirt's
        autoattach defaults (both true), which the portal's terminal and noVNC
        viewer depend on.
        """
        image = template_spec.get('image')
        image_url = template_spec.get('imageURL')
        resources = template_spec.get('resources', {}) or {}
        node_selector = template_spec.get('nodeSelector', {})
        user_details = user_details or {}
        zone = template_spec.get('zone') or DEFAULT_ZONE

        labels = {
            "app": "whistler-desktop",
            "session": session_name,
            "user": username,
            # Inherited by the virt-launcher pod (like the Service's session
            # label), where the zone's NetworkPolicy selects it — guest
            # traffic is masqueraded through that pod, so the VM is zoned
            # with no guest-side wiring.
            ZONE_LABEL: zone,
        }

        devices = {
            "disks": [
                {"name": "rootdisk", "disk": {"bus": "virtio"}},
                {"name": "cloudinit", "disk": {"bus": "virtio"}},
            ],
            "interfaces": [{"name": "default", "masquerade": {}}],
        }
        if home_pvc:
            # `serial` is load-bearing, not cosmetic: it is what udev turns
            # into /dev/disk/by-id/virtio-<serial>, which is how the guest
            # finds this disk. Addressing it as /dev/vdb instead would be a
            # bet on probe order, and losing that bet means formatting or
            # mounting the wrong disk as someone's home, silently.
            devices["disks"].append({
                "name": "homedisk",
                "serial": HOME_DISK_SERIAL,
                "disk": {"bus": "virtio"},
            })
        if 'gpu' in resources:
            # deviceName comes from the live GPU catalog: the template's
            # gpuType (nodeSelector) picks the entry, whose vmResource is the
            # KubeVirt-permitted device-plugin name for that card. Per-type,
            # so mixed clusters work — no global resource-name setting.
            gpu_type = (node_selector or {}).get(GPU_NODE_LABEL)
            devices["gpus"] = [{"name": "gpu0",
                                "deviceName": self._vm_gpu_device_name(gpu_type)}]

        # The guest's authorized_keys: the user's own keys plus the portal's
        # per-user access key, which backs the web terminal (an SSH session
        # into the guest — real login semantics, MOTD, exit that sticks —
        # unlike the shared serial console).
        ssh_keys = list(user_details.get("publicKeys", []) or [])
        if portal_public_key:
            ssh_keys.append(portal_public_key)
        # viewer=websockets means a desktop-VM image with the Selkies stack
        # baked in (e.g. desktops/vm-xfce-selkies): cloud-init additionally
        # starts the per-user DE session unit and writes the streamer env
        # (template streamerEnv + displayPort). The vnc viewer needs no agent,
        # so its guests get the plain (ssh-style) document.
        desktop_stream = viewer == 'websockets'
        # What "ready" means for this guest: the streamer port for a browser
        # desktop, otherwise sshd — which is what the web terminal, the
        # screenshot grabber and a plain `ssh` all land on. Either way it is a
        # port cloud-init has to finish before it answers.
        readiness_port = display_port if desktop_stream else 22
        user_data = build_user_data(
            username=username,
            uid=resolve_uid(user_details),
            gid=resolve_gid(user_details),
            ssh_keys=ssh_keys,
            hostname=hostname,
            home_disk=bool(home_pvc),
            shared_datasets=shared_datasets,
            desktop=desktop_stream,
            streamer_env=template_spec.get('streamerEnv') if desktop_stream else None,
            display_port=display_port if desktop_stream else None,
            host_key=host_key,
            host_cert=host_cert,
        )

        if image_url:
            root_volume = {"name": "rootdisk",
                           "dataVolume": {"name": f"{session_name}-root"}}
        else:
            # No imagePullPolicy on purpose: let KubeVirt's tag-based
            # defaulting decide (`:latest` → Always, anything else →
            # IfNotPresent), which is why the dev images are built as
            # `<name>[-cuda]:latest` rather than `:dev`/`:dev-cuda` (see
            # desktops/vm-*-selkies/build.sh). Mutable dev tags then re-pull on
            # every VMI start instead of silently booting a stale cached qcow2,
            # while production's immutable versioned tags keep IfNotPresent and
            # don't make the registry a hard dependency of every VM boot.
            # Pinning Always here would take that choice away from both.
            root_volume = {"name": "rootdisk",
                           "containerDisk": {"image": image or 'ubuntu:latest'}}

        # The userData travels via a per-session Secret (userDataSecretRef),
        # not inline: KubeVirt's admission webhook caps inline userData at
        # 2048 bytes and ours exceeds it. KubeVirt expects the document under
        # the key `userdata`. Owner-referenced to the Session like the VM
        # itself.
        cloudinit_secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": f"{session_name}-cloudinit",
                "labels": labels,
                "ownerReferences": [self._session_owner_reference(session_name, uid)],
            },
            "stringData": {"userdata": user_data},
        }

        domain = {"devices": devices}
        vm_spec = {
            # runStrategy (not `running`) so stop/start is a spec patch:
            # Halted = stopped-but-existing, Always = running/restarted.
            # VMs default to Halted at creation — unlike pods they are
            # expensive, so they boot on first connect (the connect/term/vnc
            # pages bump whistler/last-connect), not on session creation.
            "runStrategy": run_strategy,
            "template": {
                "metadata": {
                    "labels": labels,
                    "annotations": {
                        ZONE_HASH_ANNOTATION: self._zone_config_hash(zone),
                    },
                },
                "spec": {
                    # The ACPI shutdown window. Left unset this is NOT KubeVirt's
                    # documented 30s: v1.8.4 renders the virt-launcher pod at 60s,
                    # so a stop sat for a full minute after the guest was already
                    # down. Systemd shuts these desktop guests down in ~2s (no
                    # databases, no long drains — $HOME is a network mount the
                    # gateway owns), so spend a few seconds on a clean ACPI
                    # shutdown and force off after that rather than waiting on a
                    # guest that has already gone.
                    "terminationGracePeriodSeconds": 5,
                    # Readiness = "the guest is actually serving", not "qemu
                    # started". A VMI reaches phase Running the moment the domain
                    # boots, ~20s before cloud-init has brought up the streamer
                    # (or sshd), and _probe_vmi used to map Running -> Ready. The
                    # connect page believed it, redirected to /desktop/<id>/, and
                    # the proxy dialed a port nothing was listening on yet: a
                    # guaranteed "desktop backend unreachable" on every cold
                    # start. Probing from the launcher works because the interface
                    # is masquerade (all ports forwarded to the guest).
                    # failureThreshold is generous: this gates the *first* Ready,
                    # and a guest that stops answering should read as broken
                    # rather than flap the session out from under a live viewer.
                    "readinessProbe": {
                        "tcpSocket": {"port": readiness_port},
                        "initialDelaySeconds": 5,
                        "periodSeconds": 5,
                        "failureThreshold": 30,
                    },
                    "nodeSelector": node_selector,
                    "domain": domain,
                    "networks": [{"name": "default", "pod": {}}],
                    "volumes": [
                        root_volume,
                        {"name": "cloudinit", "cloudInitNoCloud": {
                            "secretRef": {
                                "name": cloudinit_secret["metadata"]["name"]}}},
                    ] + ([
                        {"name": "homedisk", "persistentVolumeClaim": {
                            "claimName": home_pvc}},
                    ] if home_pvc else []),
                },
            },
        }
        # Forced resolvers reach the guest through KubeVirt's masquerade DHCP,
        # which advertises the launcher pod's resolv.conf.
        self._apply_zone_dns(vm_spec["template"]["spec"], zone)
        if image_url:
            # `storage:` (not `pvc:`) lets CDI's StorageProfile pick the
            # access/volume modes for the root-disk PVC.
            vm_spec["dataVolumeTemplates"] = [{
                "metadata": {"name": f"{session_name}-root"},
                "spec": {
                    "source": {"http": {"url": image_url}},
                    "storage": {"resources": {"requests": {
                        "storage": template_spec.get('rootDiskSize', '20Gi'),
                    }}},
                },
            }]

        # instancetype supplies cpu/memory; KubeVirt rejects setting both it and
        # domain.cpu/domain.resources, so they are mutually exclusive here.
        # domain.memory goes the same way — the instancetype applier conflicts
        # on it, and an instancetype carries its own memory.hugepages.
        if instancetype:
            vm_spec["instancetype"] = {"name": instancetype}
        else:
            if 'cpu' in resources:
                domain["cpu"] = {"cores": int(resources['cpu'])}
            # Guest RAM on hugepages (see DEFAULT_HUGE_PAGE_SIZE): this is
            # what keeps a GPU VM's DMA pinning inside virt-handler's 20s
            # SyncVMI deadline. The node must have them reserved. The memory
            # comes back from the same call because hugepages round it up to
            # a whole number of pages.
            memory, huge_page_size = self._vm_guest_memory(resources)
            if memory:
                domain["resources"] = {"requests": {"memory": memory}}
            if huge_page_size:
                domain["memory"] = {"hugepages": {"pageSize": huge_page_size}}

        vm_body = {
            "apiVersion": f"{KUBEVIRT_GROUP}/{KUBEVIRT_VERSION}",
            "kind": "VirtualMachine",
            "metadata": {
                "name": session_name,
                "labels": labels,
                "ownerReferences": [self._session_owner_reference(session_name, uid)],
            },
            "spec": vm_spec,
        }
        return vm_body, cloudinit_secret

    @staticmethod
    def _build_vm_spec_patch(current_spec, desired_spec):
        """Merge patch bringing an existing VirtualMachine's spec in line with
        a freshly built one — or None when nothing we own differs. Pure.

        Excluded on purpose: ``runStrategy`` (start/stop is its own decision,
        see _create_vm) and ``dataVolumeTemplates`` (the root-disk DataVolume
        is imported once and KubeVirt won't re-drive it, so changing an
        imageURL needs a new session, not a patch).

        A JSON merge patch only ever merges, so fields the new spec no longer
        sets have to be nulled explicitly (_VM_MANAGED_FIELDS) or they linger
        — e.g. domain.cpu after a template moved to an instancetype, which
        KubeVirt then rejects as mutually exclusive.
        """
        patch = {k: copy.deepcopy(v) for k, v in desired_spec.items()
                 if k not in ("runStrategy", "dataVolumeTemplates")}
        for path in _VM_MANAGED_FIELDS:
            if not _dig(desired_spec, path) and _dig(current_spec, path):
                _set_at(patch, path, None)
        return None if _merge_patch_is_noop(current_spec, patch) else patch

    def _build_session_service(self, *, session_name, username, uid, display_port):
        """Build the per-session ClusterIP Service manifest (pure). It selects
        the desktop pod / VMI launcher pod by the ``session`` label and exposes
        the display port — the portal's websockets viewer reaches a desktop here.

        It also exposes 22, which is what makes jump routing addressable: the
        gateway splices to this Service's cluster DNS name rather than to a
        pod/VMI IP, so an instance keeps one address across reboots. Harmless
        where nothing listens (a desktop pod with no sshd yet): the connection
        is simply refused.

        ``display_port`` is optional: an ssh-mode session has no display, and
        this Service exists for it purely to carry port 22."""
        ports = [
            {
                "name": "ssh",
                "port": SESSION_SSH_PORT,
                "targetPort": SESSION_SSH_PORT,
            },
        ]
        if display_port:
            ports.insert(0, {
                "name": "display",
                "port": display_port,
                "targetPort": display_port,
            })
        return {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": session_name,
                "labels": {
                    "app": "whistler-desktop",
                    "session": session_name,
                    "user": username,
                },
                "ownerReferences": [self._session_owner_reference(session_name, uid)],
            },
            "spec": {
                "type": "ClusterIP",
                "selector": {"session": session_name},
                "ports": ports,
            },
        }

    def _ensure_session_service(self, *, session_name, username, uid, namespace,
                                display_port):
        """Create *or reconcile* the per-session ClusterIP Service.

        Reconciled, not create-once. It used to return on the 409 and leave
        whatever was there, which meant a Service outlived the shape Whistler
        wanted from it: adding the ssh port published nothing on sessions that
        already existed, and dialling that port on the ClusterIP hung — no
        matching Service port means no DNAT, so the packet is dropped rather
        than refused, and the caller sees a timeout with a healthy session on
        the other side.

        A merge patch of `spec.ports` (and the selector) replaces the list
        wholesale while leaving everything the API server owns — clusterIP
        above all, which is immutable — untouched.
        """
        body = self._build_session_service(
            session_name=session_name, username=username, uid=uid,
            display_port=display_port,
        )
        core_api = client.CoreV1Api()
        try:
            core_api.create_namespaced_service(namespace, body)
            logger.info(f"Service {session_name} created")
            return True
        except ApiException as e:
            if e.status != 409:
                logger.error(f"Failed to create service {session_name}: {e}")
                return False

        try:
            core_api.patch_namespaced_service(session_name, namespace, {
                "spec": {
                    "ports": body["spec"]["ports"],
                    "selector": body["spec"]["selector"],
                },
            })
            return True
        except ApiException as e:
            logger.error(f"Failed to reconcile service {session_name}: {e}")
            return False

    def _resolve_template(self, user_ns: str, template_ref: str) -> Optional[Dict[str, Any]]:
        """Resolve a Template from the user namespace, falling back to the system
        namespace. Shared by every session backend (ssh + desktop)."""
        custom_api = client.CustomObjectsApi()
        try:
            return custom_api.get_namespaced_custom_object(
                self.group, self.version, user_ns, TEMPLATE_PLURAL, template_ref
            )
        except ApiException as e:
            if e.status != 404:
                raise
        system_ns = os.environ.get("POD_NAMESPACE", "whistler")
        if system_ns != user_ns:
            try:
                return custom_api.get_namespaced_custom_object(
                    self.group, self.version, system_ns, TEMPLATE_PLURAL, template_ref
                )
            except ApiException:
                pass
        return None

    def ensure_session(self, username: str, session_name: str) -> Dict[str, Any]:
        """Ensure the pod-or-VM (and, for desktop, the per-session Service) for a
        Session exists. Resolves the referenced Template, applies operator policy
        (image allow-list + privileged->kata coercion), then dispatches on the
        effective runtime.

        Returns a dict ``{ok, mode, runtime, displayPort}`` the operator writes
        into Session status. Raises ``PolicyError`` for a hard, non-retryable
        policy violation; returns ``ok=False`` for transient failures (template
        not yet present, pod terminating) so the operator retries."""
        user_ns = self._ensure_user_namespace(username)
        full_name = f"{username}-{session_name}"
        result = {"ok": False, "mode": None, "runtime": None, "displayPort": None,
                  "viewer": None, "phase": None}

        try:
            cr = self.api.get_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, full_name
            )
        except ApiException as e:
            logger.error(f"Session {full_name} not found: {e}")
            return result

        spec = cr.get('spec', {})
        uid = cr['metadata']['uid']
        template_ref = spec.get('templateRef')

        template = self._resolve_template(user_ns, template_ref)
        if not template:
            logger.error(f"Template {template_ref} not found")
            return result

        template_spec = template.get('spec', {})
        mode = template_spec.get('mode', 'ssh')
        runtime = template_spec.get('runtime', 'container')
        # The websockets viewer (Selkies 2.x) serves H.264 over HTTP/WebSockets;
        # displayPort is the port the streamer sidecar's Selkies server listens
        # on and the portal reverse-proxies. The per-session Service exposes it
        # and status advertises it.
        display_port = template_spec.get('displayPort', 8082)
        persistence = template_spec.get('persistence', 'ephemeral')
        # SSH ephemeral sessions carry preemptible on the Session spec; desktop
        # templates express it via persistence. Honor either.
        preemptible = bool(spec.get('preemptible')) or persistence == 'preemptible'

        # Merge this run's overrides into the template/user details used to
        # build the workload, gated by the owning user's granted override
        # groups (raises PolicyError if ungranted). Which slice that is —
        # the instance's defaults or the start dialog's one-shot answer — is
        # effective_session_overrides' rule, shared with resolve_ssh_target so
        # the gateway cannot disagree with the workload about, say, its zone.
        user_details = self.get_user(username)
        template_spec, user_details = self._apply_overrides(
            template_spec, user_details, effective_session_overrides(spec),
            username)

        # Authoritative policy (may raise PolicyError, may coerce runtime->kata).
        effective_runtime = self._apply_policy(template_spec, mode, runtime, username)
        result["mode"] = mode
        result["runtime"] = effective_runtime
        if mode == 'desktop':
            # Viewer default depends on the effective runtime (which the CRD
            # schema can't express): VMs get the agentless noVNC path over the
            # KubeVirt VNC subresource, pods the Selkies websockets relay.
            viewer = template_spec.get('viewer') or (
                'vnc' if effective_runtime == 'vm' else 'websockets')
            result["displayPort"] = display_port
            result["viewer"] = viewer

        # The per-user PVC is the POD home. VMs get a per-instance disk
        # instead (below), so provisioning this for a VM-only user would
        # reserve storage nobody ever mounts.
        pvc_name = None
        if effective_runtime != 'vm':
            try:
                pvc_name = self._ensure_pvc(username, user_ns, logger)
            except Exception:
                return result

        # A connect (portal connect/term/vnc, or SSH) bumps the start
        # annotation before/while reconcile runs, so a create and a trigger
        # that coalesce into one event still boot immediately; a stop writes
        # the stop annotation and the newer of the two wins (see run_intent).
        # Absent any connect, the workload starts life Stopped — pods
        # included, so a freshly-created session doesn't start running before
        # anyone has asked to use it.
        #
        # This is the ONLY place the run decision is made, and it is made from
        # the CR. That is what lets stop be a CR patch from an unprivileged
        # caller: the operator, reconciling, is what touches the workload.
        wants_start = run_intent(cr['metadata'].get('annotations'))

        if effective_runtime == 'vm':
            # The home is a named volume the instance selects, not a share
            # (design/storage.md, design/security.md).
            #
            # EVERY VM gets one, deliberately not gated on `persistence`.
            # That field describes whether the *instance* is reaped, not
            # whether the user's data is disposable: the desktop templates
            # are `persistence: ephemeral` and their sessions live for weeks.
            # Gating on it silently gave those guests no home at all, which is
            # how this was found.
            #
            # Ensured BEFORE the VM: a guest that boots with no disk to mount
            # comes up with a root-owned empty home, so a failure here is
            # transient (the operator retries), not a degraded boot.
            # PolicyError propagates: the operator turns it into
            # status.policyFailed + statusMessage, which is the only path that
            # reaches the user. Returning ok=False instead would be retried
            # silently forever, and the operator overwrites policyFailed on
            # every successful call, so setting it in the result would be
            # discarded.
            home_volume = self.resolve_session_home_volume(
                username, full_name,
                requested=(cr.get('spec') or {}).get('homeVolume'),
                default_size=template_spec.get('homeDiskSize'),
                zone=template_spec.get('zone') or DEFAULT_ZONE,
                logger=logger)

            # The access matrix decides whether this home may be mounted in
            # THIS zone. Absent is a refusal with nothing below it — the whole
            # point of the table (design/security.md). Checked at start, like
            # the attach rule: a stopped instance mounts nothing, and refusing
            # at creation would stop an admin from setting the grant after the
            # fact.
            if wants_start:
                zone = template_spec.get('zone') or DEFAULT_ZONE
                access = self.volume_access(
                    username, zone, home_volume.get("name"))
                if access is None:
                    raise PolicyError(
                        f"Home volume '{home_volume.get('name')}' is not "
                        f"granted in zone '{zone}'. An administrator grants "
                        f"this in the user's access matrix; creating a home "
                        f"volume grants it only in the zone it was made for.")

            # One live attach. Checked only when this reconcile would START
            # the guest: a created-but-stopped instance holds nothing, and
            # refusing to *create* it would make the volume picker unusable.
            if wants_start:
                holder = self.home_volume_holder(
                    username, home_volume, ignore_instance=full_name)
                if holder:
                    raise PolicyError(
                        f"Home volume '{home_volume.get('name')}' is in use "
                        f"by the running instance '{holder}'. Stop that "
                        f"instance first — a home disk carries an ext4 "
                        f"filesystem, which cannot be attached to two running "
                        f"guests at once.")

            try:
                home_pvc = self.ensure_home_volume_pvc(
                    username, home_volume,
                    fallback_size=template_spec.get('homeDiskSize'),
                    logger=logger)
            except Exception:
                return result
            ok = self._create_vm(
                user_ns, full_name, session_name, username, uid,
                template_spec, display_port,
                template_spec.get('instancetype'), preemptible,
                home_pvc=home_pvc,
                shared_datasets=self.session_shared_datasets(username),
                start=wants_start,
                viewer=result.get("viewer"),
                user_details=user_details,
            )
        elif wants_start:
            ok = self._create_pod(
                user_ns, full_name, session_name, username, uid, mode,
                effective_runtime, template_spec, pvc_name, display_port,
                preemptible, user_details=user_details,
            )
        else:
            # Reconcile *toward* stopped rather than merely declining to
            # start: a pod session that is running when the intent says
            # stopped has to go. Deleting the pod is the stop (state lives on
            # the PVC), and it is idempotent — a 404 is the desired state.
            ok = self._delete_session_pod(user_ns, full_name)

        # Honest initial phase: without a connect the workload isn't
        # started, and reporting Provisioning would show a phantom
        # "Starting" badge until the phase timer corrects it. But a re-connect
        # to an already-Ready session (e.g. opening a second view onto a
        # running desktop/VM) hits this same path — don't regress an already
        # up workload back to Provisioning, or the phase timer's next probe
        # (up to 10s away) briefly makes the session look down, and anything
        # gating on phase=="Ready" right after the connect (the web terminal's
        # readiness check) can spuriously fail.
        # A stop is the one case where a Ready session must NOT keep its
        # phase: we just halted it, and holding Ready for up to a probe
        # interval would leave the launcher offering a connect into a guest on
        # its way down. Stopping vs Stopped tells "was running, going down"
        # from "was never up" — a guest takes seconds to shut down, and the
        # phase timer replaces either with what it actually finds.
        current_phase = (cr.get('status') or {}).get('phase')
        if wants_start:
            result["phase"] = ("Ready" if current_phase == "Ready"
                               else "Provisioning")
        elif current_phase in (None, "", "Stopped", "Failed"):
            result["phase"] = "Stopped"
        else:
            result["phase"] = "Stopping"

        if not ok:
            return result

        # EVERY session gets the Service, not just desktops. The old rule
        # ("SSH sessions are bridged via kubectl exec and need no Service")
        # died with the exec bridge: jump routing splices to this Service's
        # cluster DNS name (resolve_ssh_target returns exactly that), so an
        # ssh-mode session without one resolves to nothing and the client hangs
        # after authenticating to the gateway — which is what a `mode: ssh,
        # runtime: vm` devbase session hit, the first of its kind.
        # Container sessions get it too: they carry the same `session` label,
        # the Service is free, and it is the address a pod image with sshd will
        # need (design/proxyjump.md). display_port is None for ssh mode, so the
        # Service then publishes port 22 alone.
        if not self._ensure_session_service(
            session_name=full_name, username=username, uid=uid,
            namespace=user_ns,
            # `displayPort` defaults to 8082 for every template, so it must be
            # gated on the mode here (as at the other call sites) or an ssh
            # Service would advertise a display nothing serves.
            display_port=display_port if mode == 'desktop' else None,
        ):
            return result

        result["ok"] = True
        return result

    def _create_pod(self, user_ns, full_name, session_name, username, uid, mode,
                    runtime, template_spec, pvc_name, display_port,
                    preemptible, user_details=None) -> bool:
        pod_body = self._build_pod_spec(
            full_name=full_name,
            hostname=session_name,
            username=username,
            uid=uid,
            mode=mode,
            runtime=runtime,
            template_spec=template_spec,
            pvc_name=pvc_name,
            available_volumes=self._load_volume_definitions_from_file(),
            user_details=user_details if user_details is not None else self.get_user(username),
            preemptible=preemptible,
            display_port=display_port if mode == 'desktop' else None,
            # Read-only where a Group granted this member only `ro`; resolved
            # here (operator-side, at build time) rather than trusted from the
            # template or the guest.
            volume_modes=self.get_user_volume_modes(username),
        )
        logger.debug(f"Creating Pod:\n{yaml.safe_dump(pod_body)}")
        core_api = client.CoreV1Api()
        try:
            core_api.create_namespaced_pod(user_ns, pod_body)
            logger.info(f"Pod {full_name} created")
            return True
        except ApiException as e:
            if e.status == 409:
                try:
                    existing = core_api.read_namespaced_pod(full_name, user_ns)
                    if existing.metadata.deletion_timestamp:
                        logger.info(f"Pod {full_name} is terminating.")
                        return False
                except ApiException:
                    pass
                return True  # Already exists
            logger.error(f"Failed to create pod: {e}")
            return False

    def _vm_access_secret_name(self, username: str) -> str:
        return f"whistler-vm-access-{username}"

    def _ensure_vm_access_key(self, username: str, user_ns: str) -> Optional[str]:
        """Per-user SSH keypair for the portal's VM web terminal: the public
        half is injected into every VM via cloud-init, the private half stays
        in a Secret in the user namespace that only the portal (and operator)
        can read. Returns the public key, or None if the Secret could not be
        ensured (the VM still boots; only the web terminal is degraded)."""
        secret_name = self._vm_access_secret_name(username)
        core_api = client.CoreV1Api()
        try:
            sec = core_api.read_namespaced_secret(secret_name, user_ns)
            import base64
            return base64.b64decode(sec.data["id_ed25519.pub"]).decode().strip()
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read VM access key for {username}: {e}")
                return None

        import asyncssh  # deferred: only the VM path needs key generation
        key = asyncssh.generate_private_key(
            "ssh-ed25519", comment=f"whistler-portal-{username}")
        public = key.export_public_key().decode().strip()
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": secret_name,
                "labels": {"app": "whistler", "user": username},
            },
            "stringData": {
                "id_ed25519": key.export_private_key().decode(),
                "id_ed25519.pub": public,
            },
        }
        try:
            core_api.create_namespaced_secret(user_ns, body)
            logger.info(f"Created VM access keypair for {username}")
            return public
        except ApiException as e:
            if e.status == 409:  # lost a race; use the winner's key
                return self._ensure_vm_access_key(username, user_ns)
            logger.error(f"Failed to create VM access key for {username}: {e}")
            return None

    def get_vm_access_private_key(self, username: str) -> Optional[str]:
        """Private half of the per-user VM access keypair (portal-side)."""
        user_ns = self._get_user_namespace(username)
        core_api = client.CoreV1Api()
        try:
            sec = core_api.read_namespaced_secret(
                self._vm_access_secret_name(username), user_ns)
            import base64
            return base64.b64decode(sec.data["id_ed25519"]).decode()
        except ApiException as e:
            logger.error(f"Failed to read VM access key for {username}: {e}")
            return None

    # ------------------------------------------------------------------ #
    # Per-user NFS storage gateway (VM homes). KubeVirt's unprivileged     #
    # virtiofsd (kubevirt#13028) made a directly-shared home read-only for #
    # the guest user, so the home PVC is instead mounted by a per-user     #
    # NFS-Ganesha pod (images/storage-gateway/) and exported as NFSv4.2;   #
    # the guest mounts it from cloud-init. Server-side identity: client    #
    # uids are never trusted, the export's Squash = All_Squash lands every #
    # write on the PVC as the user's real uid — consistent with pod        #
    # sessions sharing the PVC. NFS AUTH_SYS has no per-share credential,  #
    # so reaching the export IS the permission: the fencing NetworkPolicy  #
    # below plus the per-user namespace are the whole boundary (see        #
    # design/security.md).                                                 #
    # ------------------------------------------------------------------ #

    def _gateway_name(self, username: str) -> str:
        return f"whistler-storage-{username}"

    def _gateway_host(self, username: str, user_ns: str) -> str:
        return f"{self._gateway_name(username)}.{user_ns}.svc.cluster.local"

    def _build_gateway_manifests(self, *, username, uid, gid, pvc_name, image,
                                 node_selector=None, resources=None):
        """Deployment + Service manifests for the per-user storage gateway
        (pure, unit-testable like _build_pod_spec). No ownerReferences: the
        gateway is per-user, not per-session — it lives across sessions and
        dies with the user namespace."""
        name = self._gateway_name(username)
        labels = {"app": "whistler-storage-gateway", "user": username}
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "labels": labels},
            "spec": {
                "replicas": 1,
                # Recreate, not RollingUpdate: the home PVC may be RWO, and
                # a rolling replacement would deadlock on the volume.
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        # Values-level pinning to fast/storage nodes — the
                        # gateway is deliberately NOT co-scheduled with VMs.
                        "nodeSelector": node_selector or {},
                        # ganesha.nfsd exits promptly on SIGTERM, and with the
                        # Recreate strategy on an RWO PVC the new pod cannot
                        # start until the old one is fully gone.
                        "terminationGracePeriodSeconds": 5,
                        "containers": [{
                            "name": "ganesha",
                            "image": image,
                            "env": [
                                {"name": "SHARE_USER", "value": username},
                                {"name": "SHARE_UID", "value": str(uid)},
                                {"name": "SHARE_GID", "value": str(gid)},
                            ],
                            "ports": [{"containerPort": 2049, "name": "nfs"}],
                            # Two capabilities, not privileged: ganesha's VFS
                            # FSAL addresses files by handle
                            # (open_by_handle_at, hence DAC_READ_SEARCH) and
                            # raises its own fd limit (SYS_RESOURCE). The
                            # kernel nfsd would have needed the whole pod
                            # privileged and is a kernel-global singleton
                            # besides — unusable one-per-user.
                            "securityContext": {
                                "capabilities": {
                                    "add": ["DAC_READ_SEARCH", "SYS_RESOURCE"],
                                },
                            },
                            # NOT a tcpSocket probe. ganesha binds 2049 and
                            # logs "NFS SERVER INITIALIZED" even when every
                            # export failed to build, so a port check calls a
                            # gateway that serves nothing healthy while guest
                            # mounts get ENOENT — which is exactly how the
                            # SMB->NFS move shipped broken. gateway-ready
                            # asks ganesha over DBus which exports it has.
                            "readinessProbe": {
                                "exec": {"command": ["/usr/local/bin/gateway-ready"]},
                                "periodSeconds": 10,
                                "failureThreshold": 3,
                            },
                            # A gateway that never manages to export is broken,
                            # not slow: restart it so the failure is a visible
                            # CrashLoop rather than a pod sitting NotReady
                            # forever. 30 x 2s is far longer than ganesha's
                            # ~1s startup.
                            "startupProbe": {
                                "exec": {"command": ["/usr/local/bin/gateway-ready"]},
                                "periodSeconds": 2,
                                "failureThreshold": 30,
                            },
                            "volumeMounts": [
                                {"name": "home", "mountPath": "/shares/home"},
                            ],
                            "resources": resources or {},
                        }],
                        "volumes": [
                            {"name": "home",
                             "persistentVolumeClaim": {"claimName": pvc_name}},
                        ],
                    },
                },
            },
        }
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": name, "labels": labels},
            "spec": {
                "type": "ClusterIP",
                "selector": labels,
                "ports": [{"name": "nfs", "port": 2049, "targetPort": 2049}],
            },
        }
        return deployment, service

    def _build_gateway_network_policy(self, username: str) -> Dict[str, Any]:
        """Fencing (pure): only this user's session pods may reach the
        gateway, and only on NFS. virt-launcher pods inherit the
        app: whistler-desktop label from the VM template metadata. Additive
        with isolate-user-pods, whose portal carve-out also selects the
        gateway pod (harmless — the portal never speaks NFS).

        Under SMB this narrowed an already-authenticated share; under NFS
        AUTH_SYS it IS the authentication, since the export trusts whoever
        can open a connection to it."""
        labels = {"app": "whistler-storage-gateway", "user": username}
        return {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": "whistler-storage-gateway", "labels": labels},
            "spec": {
                "podSelector": {
                    "matchLabels": {"app": "whistler-storage-gateway"}},
                "policyTypes": ["Ingress"],
                "ingress": [{
                    "from": [{"podSelector": {
                        "matchLabels": {"app": "whistler-desktop"}}}],
                    "ports": [{"port": 2049, "protocol": "TCP"}],
                }],
            },
        }

    # ------------------------------------------------------------------ #
    # Shared datasets over S3 (design/storage.md).                        #
    #                                                                     #
    # Sessions never talk to the real S3 server. Whistler starts a proxy  #
    # per (volume, mode) and the zone egress rule names THAT — a Service  #
    # whose address Whistler assigned and therefore knows. Pointing a     #
    # zone rule straight at an external endpoint binds it to an address   #
    # nobody here owns: move the server, or let a CIDR be reused, and the #
    # rule silently means something else, with restricted data reachable  #
    # from a more permissive zone. Silent and fail-open, which is the     #
    # combination refused everywhere else in this codebase.               #
    #                                                                     #
    # The proxy also holds the bucket credential, so it never enters a    #
    # guest whose user has root. A guest cannot exfiltrate what it never  #
    # received, and revocation is real rather than theoretical.           #
    #                                                                     #
    # One proxy per (volume, mode) because rclone's --read-only is a      #
    # server-wide flag, not a per-key one. That is what finally makes     #
    # `mode: ro` a boundary on a VM, where a mount flag is not.           #
    # ------------------------------------------------------------------ #

    @staticmethod
    def s3_volume_definitions(volumes) -> Dict[str, Dict[str, Any]]:
        """``{name: definition}`` for LEGACY S3 entries in a volume catalog
        (``type: s3`` in whistler.volumes), which is where datasets lived
        before they became their own kind.

        Kept as a fallback only. Datasets do not belong in the volume catalog:
        every other entry there is a Kubernetes volume source that
        _build_volume_wiring copies straight into a pod spec, and an S3
        definition copied there is not a valid one. Use Dataset CRs.
        """
        return {v["name"]: v for v in (volumes or [])
                if isinstance(v, dict) and v.get("type") == "s3"}

    def _load_datasets(self):
        """Load the dataset catalog from Dataset CRs — live and
        admin-editable in the portal, exactly like zones (the chart renders
        whistler.datasets values as Dataset CRs).

        Falls back to legacy ``type: s3`` entries in the volume catalog so
        values written before the Dataset kind keep working, and past that
        keeps the previous catalog rather than wiping it: an empty catalog
        would silently unmount every dataset on the next session build.
        """
        try:
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, self.namespace, DATASET_PLURAL
            )
            datasets = {
                item["metadata"]["name"]: {**(item.get("spec") or {}),
                                           "name": item["metadata"]["name"]}
                for item in resp.get("items", [])
            }
        except (ApiException, AttributeError) as e:
            if getattr(e, "status", None) == 404 and not self._warned_no_dataset_crd:
                type(self)._warned_no_dataset_crd = True
                logger.warning(
                    "No Dataset CRD in this cluster, so only legacy `type: s3` "
                    "volume entries are datasets. `helm upgrade` does not "
                    "update CRDs — run "
                    "`kubectl apply -f charts/whistler/crds/crds.yaml`")
            else:
                logger.debug(
                    f"Dataset CRs unavailable ({e}); keeping previous catalog")
            try:
                legacy = self.s3_volume_definitions(self.get_volumes())
            except Exception:  # no volume catalog either; nothing to fall to
                legacy = {}
            # Keep a catalog we already have when the fallback is empty: an
            # empty one would silently unmount every dataset.
            if legacy or self.datasets is None:
                self.datasets = legacy
            return
        type(self)._warned_no_dataset_crd = False
        # Legacy entries stay visible, but a Dataset CR of the same name wins:
        # the CR is the editable one, so it must be what an admin sees change.
        merged = self.s3_volume_definitions(self.get_volumes())
        merged.update(datasets)
        self.datasets = merged

    def get_dataset_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Full dataset catalog (name -> spec), freshly loaded — drives the
        admin datasets editor and every grant picker."""
        self._load_datasets()
        return {name: dict(spec or {})
                for name, spec in (self.datasets or {}).items()}

    def get_dataset_names(self) -> List[str]:
        """Dataset names, for the grant pickers. Separate from get_volumes()
        on purpose: a dataset is granted, never chosen as an instance mount,
        and it is not a Kubernetes volume source."""
        return sorted(self.get_dataset_definitions())

    @staticmethod
    def dataset_mode(definition: Dict[str, Any], granted: str) -> str:
        """The mode a dataset is actually served at.

        ``readOnly`` on the Dataset is a CEILING: it wins over any rw grant,
        and it is the whole reason the field exists. Without it a dataset is
        writable by everyone granted it. Shared data is read-mostly and S3
        resolves concurrent writers as a silent last-writer-wins, so the
        ceiling is the posture to prefer (design/storage.md).
        """
        if (definition or {}).get("readOnly"):
            return "ro"
        return "ro" if granted == "ro" else "rw"

    def dataset_credentials_secret_name(self, name: str) -> str:
        """The Secret this dataset's credential lives in when Whistler manages
        it. A dataset may instead point `credentialsSecret` at a Secret the
        admin created, which is the way to keep the credential out of
        Whistler's hands entirely."""
        return f"whistler-dataset-{name}-creds"

    def save_dataset_credentials(self, name: str, access_key: str,
                                 secret_key: str) -> bool:
        """Create or update the Secret holding a dataset's REAL bucket
        credential. Only the dataset's proxies mount it.

        Called from the admin editor when the credential fields are filled in;
        blank fields there mean "leave the existing credential alone", which
        is why this is separate from save_dataset. The value is never read
        back out for display — an admin can replace a credential but not
        retrieve one."""
        secret_name = self.dataset_credentials_secret_name(name)
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": secret_name,
                         "labels": {"app": "whistler-dataset",
                                    "dataset": name}},
            "stringData": {"accessKeyId": access_key,
                           "secretAccessKey": secret_key},
        }
        api = client.CoreV1Api()
        try:
            api.create_namespaced_secret(self.namespace, body)
            return True
        except ApiException as e:
            if e.status != 409:
                logger.error(f"Failed to create dataset secret {secret_name}: {e}")
                return False
        try:
            api.replace_namespaced_secret(secret_name, self.namespace, body)
            return True
        except ApiException as e:
            logger.error(f"Failed to update dataset secret {secret_name}: {e}")
            return False

    def has_dataset_credentials(self, name: str) -> bool:
        """Whether this dataset has a usable credential — the editor shows
        this instead of the credential itself.

        Answers the same question ensure_s3_proxy asks, deliberately: the
        dataset must REFERENCE a Secret, and that Secret must exist. A
        managed Secret left over from an earlier credential does not count
        while nothing points at it, or the list would show a tick beside a
        dataset whose proxy refuses to start.
        """
        definition = self.get_dataset_definitions().get(name) or {}
        secret_name = definition.get("credentialsSecret")
        if not secret_name:
            return False
        try:
            client.CoreV1Api().read_namespaced_secret(secret_name, self.namespace)
            return True
        except ApiException:
            return False

    def save_dataset(self, dataset_data: Dict[str, Any]) -> bool:
        """Create or update a Dataset CR from the admin editor.

        Unlike save_zone this does NOT push anything to running sessions: a
        dataset is mounted by cloud-init at boot, so an endpoint or bucket
        change reaches a guest on its next start. What it does do is re-fence
        the existing proxies, because `readOnly` can revoke write access and
        that must not wait for someone else's session to reconcile.
        """
        data = dict(dataset_data)
        name = (data.pop("name", "") or "").strip()
        if not name:
            return False
        spec = {k: v for k, v in data.items() if v not in (None, "")}
        body_meta = {"name": name, "namespace": self.namespace}
        try:
            try:
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, self.namespace,
                    DATASET_PLURAL, name
                )
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, self.namespace,
                    DATASET_PLURAL, name,
                    {"apiVersion": f"{self.group}/{self.version}",
                     "kind": "Dataset",
                     # Replace, not merge: the form carries the whole spec and
                     # a cleared field must actually clear.
                     "metadata": {**body_meta,
                                  "resourceVersion":
                                      existing["metadata"]["resourceVersion"]},
                     "spec": spec})
            except ApiException as e:
                if e.status != 404:
                    raise
                self.api.create_namespaced_custom_object(
                    self.group, self.version, self.namespace, DATASET_PLURAL,
                    {"apiVersion": f"{self.group}/{self.version}",
                     "kind": "Dataset",
                     "metadata": body_meta,
                     "spec": spec})
        except ApiException as e:
            logger.error(f"Failed to save dataset {name!r}: "
                         f"{crd_missing_hint(DATASET_PLURAL, e)}")
            return False
        self._load_datasets()
        self._refresh_s3_proxy_policies(name)
        return True

    def delete_dataset(self, name: str) -> bool:
        """Delete a Dataset CR and fence its proxies to nobody.

        The proxy Deployments are left running rather than deleted: fencing
        them is what makes them unreachable, and a delete that raced a session
        build would just see the proxy recreated. Sessions holding a mount
        keep it until they stop — the CR is the catalog, not the data path.
        """
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, self.namespace, DATASET_PLURAL, name
            )
        except ApiException as e:
            logger.error(f"Failed to delete dataset {name!r}: {e}")
            return False
        self._load_datasets()
        # After the reload, so s3_proxy_users no longer resolves anyone to it.
        self._refresh_s3_proxy_policies(name)
        return True

    @staticmethod
    def _s3_proxy_name(volume: str, mode: str) -> str:
        return f"whistler-s3-{volume}-{mode}"

    def _s3_proxy_host(self, volume: str, mode: str) -> str:
        return (f"{self._s3_proxy_name(volume, mode)}"
                f".{self.namespace}.svc.cluster.local")

    def _build_s3_proxy_manifests(self, *, volume, mode, definition, image,
                                  auth_secret_name, resources=None):
        """Deployment + Service for one (volume, mode) S3 proxy. Pure.

        ``definition`` is the volume's catalog entry: ``bucket``, optional
        ``prefix`` and ``endpoint``, and ``credentialsSecret`` naming a Secret
        that holds the REAL bucket credential. That secret is mounted here and
        nowhere else.

        No ownerReferences: a proxy is per shared volume, not per session or
        per user, and outlives all of them.
        """
        name = self._s3_proxy_name(volume, mode)
        labels = {"app": "whistler-s3-proxy", "volume": volume, "mode": mode}
        backend = f":s3:{definition['bucket']}"
        prefix = (definition.get("prefix") or "").strip("/")
        if prefix:
            backend = f"{backend}/{prefix}"
        # Serve the backend wrapped in a `combine` remote rather than directly.
        # `rclone serve s3 :s3:<bucket>` promotes the served directory's
        # SUBDIRECTORIES to buckets, so every file at the dataset's top level
        # becomes unaddressable and the dataset mounts EMPTY with no error
        # (measured against versitygw, 2026-08-17). Combine puts exactly one
        # directory — S3_PROXY_BUCKET — at the served root, so the dataset
        # appears under it verbatim, loose files included.
        remote = f':combine,upstreams="{S3_PROXY_BUCKET}={backend}":'

        args = [
            "serve", "s3", "--addr", ":8080",
            # Kubernetes expands $(VAR) in args from the container's own env,
            # so the key stays out of the Deployment spec. It does land in
            # this container's argv, which only rclone itself can read.
            "--auth-key", "$(WHISTLER_S3_AUTH_KEY)",
            # A dataset changed by another writer stays stale in the proxy's
            # VFS listing cache for this long. Default is 5m; datasets are
            # read-mostly but "my colleague's file isn't there" is a bad
            # first impression.
            "--dir-cache-time", "1m",
            remote,
        ]
        if mode == "ro":
            args.insert(2, "--read-only")

        env = [
            # The client-facing key pair Whistler generated for this proxy.
            {"name": "WHISTLER_S3_AUTH_KEY", "valueFrom": {"secretKeyRef": {
                "name": auth_secret_name, "key": "authKey"}}},
            # The REAL bucket credential. rclone reads backend config from
            # RCLONE_<BACKEND>_<OPTION>, so these configure the `:s3:` remote
            # above without a config file on disk.
            {"name": "RCLONE_S3_PROVIDER", "value":
                definition.get("provider", "Other")},
            {"name": "RCLONE_S3_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {
                "name": definition["credentialsSecret"],
                "key": "accessKeyId"}}},
            {"name": "RCLONE_S3_SECRET_ACCESS_KEY", "valueFrom": {
                "secretKeyRef": {"name": definition["credentialsSecret"],
                                 "key": "secretAccessKey"}}},
        ]
        if definition.get("endpoint"):
            env.append({"name": "RCLONE_S3_ENDPOINT",
                        "value": definition["endpoint"]})
        if definition.get("region"):
            env.append({"name": "RCLONE_S3_REGION",
                        "value": definition["region"]})

        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "labels": labels},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "containers": [{
                            "name": "rclone",
                            "image": image,
                            "args": args,
                            "env": env,
                            "ports": [{"containerPort": 8080, "name": "s3"}],
                            # Nothing here needs to be root, write to its own
                            # filesystem, or gain privileges: it is a protocol
                            # translator holding one credential.
                            "securityContext": {
                                "runAsNonRoot": True,
                                "runAsUser": 1000,
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "readinessProbe": {
                                "tcpSocket": {"port": 8080},
                                "periodSeconds": 10,
                            },
                            "resources": resources or {},
                        }],
                    },
                },
            },
        }
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": name, "labels": labels},
            "spec": {
                "type": "ClusterIP",
                "selector": labels,
                "ports": [{"name": "s3", "port": 8080, "targetPort": 8080}],
            },
        }
        return deployment, service

    def _build_s3_proxy_network_policy(self, volume: str, mode: str,
                                       permitted_users) -> Dict[str, Any]:
        """Fencing (pure): only the session pods of users granted this volume
        at this mode may reach this proxy, and only on its port.

        This is the half of the boundary that Whistler can actually enforce.
        The generated key is the other half, and the two compose as AND —
        which is the whole reason the endpoint is cluster-internal: a leaked
        key is inert without the reach, and reach is what zones control.

        An EMPTY permitted-users list yields a policy with no `from`, which
        NetworkPolicy reads as "deny all ingress". Fail closed: a volume
        nobody is granted is a volume nobody can reach.
        """
        name = self._s3_proxy_name(volume, mode)
        labels = {"app": "whistler-s3-proxy", "volume": volume, "mode": mode}
        ingress: List[Dict[str, Any]] = []
        users = sorted(permitted_users or [])
        if users:
            ingress.append({
                "from": [{
                    # Session pods live in per-user namespaces; select those
                    # by the user label the namespace carries.
                    "namespaceSelector": {"matchExpressions": [{
                        "key": USER_NS_LABEL,
                        "operator": "In",
                        "values": users,
                    }]},
                }],
                "ports": [{"port": 8080, "protocol": "TCP"}],
            })
        return {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": name, "labels": labels},
            "spec": {
                "podSelector": {"matchLabels": labels},
                "policyTypes": ["Ingress"],
                "ingress": ingress,
            },
        }

    def _ensure_s3_auth_secret(self, volume: str, mode: str) -> Optional[str]:
        """Ensure the client-facing key pair for one (volume, mode) proxy,
        returning the Secret's name. Generated once and then stable — the
        guests that already hold it must keep working.

        This key is NOT the bucket credential. It only opens the proxy, which
        is cluster-internal and fenced by NetworkPolicy, so it is the second
        half of an AND rather than a standalone permission.
        """
        name = f"{self._s3_proxy_name(volume, mode)}-auth"
        api = client.CoreV1Api()
        try:
            api.read_namespaced_secret(name, self.namespace)
            return name
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read S3 auth secret {name}: {e}")
                return None
        access = f"whistler-{volume}-{mode}"
        secret = secrets.token_urlsafe(32)
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name,
                         "labels": {"app": "whistler-s3-proxy",
                                    "volume": volume, "mode": mode}},
            # rclone's --auth-key takes "access,secret" as one token.
            "stringData": {"authKey": f"{access},{secret}",
                           "accessKeyId": access,
                           "secretAccessKey": secret},
        }
        try:
            api.create_namespaced_secret(self.namespace, body)
            return name
        except ApiException as e:
            if e.status == 409:
                return name
            logger.error(f"Failed to create S3 auth secret {name}: {e}")
            return None

    def s3_proxy_users(self, volume: str, mode: str) -> List[str]:
        """Users whose grant on ``volume`` resolves to exactly ``mode``.

        Drives the proxy's fencing policy, so it has to answer the same
        question the mount does: get_user_volume_modes carries the group's
        per-member rw/ro, a volume named nowhere in those grants is
        read-write (the pre-groups default), and the dataset's own
        ``readOnly`` is a ceiling over both — so a read-only dataset resolves
        every user to ro and leaves its rw proxy admitting nobody."""
        definition = self.get_dataset_definitions().get(volume) or {}
        out = []
        for user in self.list_all_users() or []:
            username = user.get("name") if isinstance(user, dict) else user
            if not username:
                continue
            # Every allow is explicit: a user granted nothing gets nothing,
            # so an unconfigured account no longer lands in every rw proxy.
            if volume not in self.get_user_allowed_volumes(username):
                continue
            granted = (self.get_user_volume_modes(username) or {}).get(
                volume, "rw")
            if self.dataset_mode(definition, granted) == mode:
                out.append(username)
        return out

    def session_shared_datasets(self, username: str) -> List[Dict[str, Any]]:
        """Resolve this user's S3 datasets into cloud-init descriptors,
        ensuring each one's proxy on the way. Returns [] when no datasets
        are defined, which is the common case.

        A failed proxy is skipped rather than failing the boot: a guest that
        comes up without one dataset is far better than a guest that does not
        come up. The unit retries, so the mount lands when the proxy does.
        """
        definitions = self.get_dataset_definitions()
        if not definitions:
            return []
        allowed = self.get_user_allowed_volumes(username)
        modes = self.get_user_volume_modes(username) or {}
        core = client.CoreV1Api()
        out = []
        for name in sorted(definitions):
            # Re-fence EVERY dataset's proxies, including ones this user is
            # not granted: a grant change only reaches a proxy through some
            # session's reconcile, and it is the proxies the user is losing
            # that matter most. Never fatal, for the same reason as below.
            try:
                self._refresh_s3_proxy_policies(name)
            except Exception as e:
                logger.error(f"Could not re-fence dataset {name!r}: {e}")
        for name, definition in sorted(definitions.items()):
            # Every allow is explicit, as everywhere else.
            if name not in allowed:
                continue
            # The dataset's readOnly ceiling wins over the grant.
            mode = self.dataset_mode(definition, modes.get(name, "rw"))
            try:
                ready = self.ensure_s3_proxy(name, mode, definition)
            except Exception as e:
                # Broad on purpose. Datasets are admin-editable, so a
                # malformed one is an ordinary occurrence, and the cost of
                # letting it escape is that NO session anywhere can start.
                logger.error(f"Dataset {name!r} could not be prepared: {e}")
                continue
            if not ready:
                logger.error(
                    f"S3 proxy for {name}/{mode} not ready; skipping mount")
                continue
            secret_name = f"{self._s3_proxy_name(name, mode)}-auth"
            try:
                sec = core.read_namespaced_secret(secret_name, self.namespace)
                access = base64.b64decode(sec.data["accessKeyId"]).decode()
                secret = base64.b64decode(
                    sec.data["secretAccessKey"]).decode()
            except (ApiException, KeyError, TypeError) as e:
                logger.error(f"Could not read {secret_name}: {e}")
                continue
            out.append({
                "name": name,
                "mode": mode,
                # The proxy, never the real S3 server. This is the address
                # Whistler assigned and the zone rules name.
                "endpoint": f"http://{self._s3_proxy_host(name, mode)}:8080",
                "accessKeyId": access,
                "secretAccessKey": secret,
            })
        return out

    def _refresh_s3_proxy_policies(self, volume: str) -> None:
        """Re-fence every EXISTING proxy for ``volume``, in both modes.

        ensure_s3_proxy only runs for the mode a session actually mounts, so
        downgrading a user from rw to ro would otherwise leave the rw proxy's
        policy still naming them — and they are root in their own guest, so
        they keep that proxy's key from the previous session's rclone.conf.
        The downgrade would then change nothing at all. Measured, 2026-08-17.

        Only touches proxies that already exist: fencing the other mode must
        never be the thing that conjures it into being.
        """
        net = client.NetworkingV1Api()
        for mode in ("ro", "rw"):
            name = self._s3_proxy_name(volume, mode)
            try:
                net.read_namespaced_network_policy(name, self.namespace)
            except ApiException as e:
                if e.status != 404:
                    logger.error(f"Could not read policy for {name}: {e}")
                continue
            self._ensure_object(
                name, self.namespace,
                self._build_s3_proxy_network_policy(
                    volume, mode, self.s3_proxy_users(volume, mode)),
                create=net.create_namespaced_network_policy,
                read=net.read_namespaced_network_policy,
                replace=net.replace_namespaced_network_policy)

    def ensure_s3_proxy(self, volume: str, mode: str, definition) -> bool:
        """Ensure the (volume, mode) proxy matches its manifests — Deployment,
        Service and fencing NetworkPolicy. Self-healing like
        ensure_storage_gateway: every call reconciles all three, so a grant
        change reaches a running proxy's policy. False on failure; callers
        treat that as transient and retry."""
        if not (definition or {}).get("credentialsSecret"):
            # No credential means no working proxy. Refuse here, where the
            # caller already knows to skip this dataset, rather than building
            # a Deployment that cannot authenticate — and NEVER raise: one
            # malformed dataset must not stop every VM in the cluster from
            # starting (measured 2026-08-18, a KeyError here did exactly
            # that, retrying forever).
            logger.error(
                f"Dataset {volume!r} has no credentialsSecret; not starting a "
                f"proxy for it. Set one in the portal's Datasets editor.")
            return False
        auth_secret = self._ensure_s3_auth_secret(volume, mode)
        if not auth_secret:
            return False
        deployment, service = self._build_s3_proxy_manifests(
            volume=volume, mode=mode, definition=definition,
            image=self.s3_proxy_image,
            auth_secret_name=auth_secret,
            resources=self.s3_proxy_resources,
        )
        policy = self._build_s3_proxy_network_policy(
            volume, mode, self.s3_proxy_users(volume, mode))
        apps = client.AppsV1Api()
        core = client.CoreV1Api()
        net = client.NetworkingV1Api()
        name = self._s3_proxy_name(volume, mode)
        ok = self._ensure_object(
            name, self.namespace, deployment,
            create=apps.create_namespaced_deployment,
            read=apps.read_namespaced_deployment,
            replace=apps.replace_namespaced_deployment)
        ok = self._ensure_object(
            name, self.namespace, service,
            create=core.create_namespaced_service,
            read=core.read_namespaced_service,
            replace=core.replace_namespaced_service,
            preserve=self._preserve_cluster_ip) and ok
        ok = self._ensure_object(
            name, self.namespace, policy,
            create=net.create_namespaced_network_policy,
            read=net.read_namespaced_network_policy,
            replace=net.replace_namespaced_network_policy) and ok
        return ok

    @staticmethod
    def _preserve_cluster_ip(live, body: Dict[str, Any]) -> None:
        """Carry the API-server-assigned clusterIP onto a replacement Service
        body. It is immutable, and a replace that omits it is rejected."""
        cluster_ip = getattr(live.spec, "cluster_ip", None) if live.spec else None
        if cluster_ip:
            body.setdefault("spec", {})["clusterIP"] = cluster_ip

    def _ensure_object(self, name: str, user_ns: str, body: Dict[str, Any], *,
                       create, read, replace, preserve=None) -> bool:
        """Create ``body``, or make an existing object match it. True on
        success; the caller treats False as transient and retries.

        **Replace, not patch.** A strategic-merge patch merges list entries by
        their key, so renaming a container adds the new one *beside* the old
        rather than superseding it, and a removed volume or a renamed port
        simply stays. That is not hypothetical: the SMB->NFS move left live
        gateways running a ganesha container and a samba container at once,
        with a Service still publishing 445 and a NetworkPolicy still fencing
        445, so the guest could not mount its home at all. Replace makes the
        manifest authoritative, which is what "ensure" is supposed to mean.
        """
        kind = body.get("kind", "object")
        try:
            create(user_ns, body)
            logger.info(f"Created {kind} {name} in {user_ns}")
            return True
        except ApiException as e:
            if e.status != 409:
                logger.error(f"Failed to create {kind} {name}: {e}")
                return False

        try:
            if preserve:
                preserve(read(name, user_ns), body)
            replace(name, user_ns, body)
            return True
        except ApiException as e:
            logger.error(f"Failed to reconcile {kind} {name}: {e}")
            return False

    def ensure_storage_gateway(self, username: str, user_ns: str,
                               pvc_name: str) -> bool:
        """Ensure the per-user storage gateway matches its manifests
        (Deployment + Service + fencing NetworkPolicy), lazily with the first
        runtime:vm session. Self-healing: every call reconciles all three, so
        a values change, an image bump in the dev loop, or a change to the
        manifests themselves reaches an already-running gateway. False on
        failure — callers must treat that as transient and retry."""
        user_details = self.get_user(username)
        deployment, service = self._build_gateway_manifests(
            username=username,
            uid=resolve_uid(user_details),
            gid=resolve_gid(user_details),
            pvc_name=pvc_name,
            image=self.storage_gateway_image,
            node_selector=self.storage_gateway_node_selector,
            resources=self.storage_gateway_resources,
        )
        policy = self._build_gateway_network_policy(username)
        name = deployment["metadata"]["name"]

        apps_api = client.AppsV1Api()
        core_api = client.CoreV1Api()
        net_api = NetworkingV1Api()

        # A tuple, not a generator: all three must be attempted even if an
        # earlier one fails, so one transient error doesn't leave the other
        # two stale. The False return makes the operator retry regardless.
        return all((
            self._ensure_object(
                name, user_ns, deployment,
                create=apps_api.create_namespaced_deployment,
                read=apps_api.read_namespaced_deployment,
                replace=apps_api.replace_namespaced_deployment),
            self._ensure_object(
                name, user_ns, service,
                create=core_api.create_namespaced_service,
                read=core_api.read_namespaced_service,
                replace=core_api.replace_namespaced_service,
                preserve=self._preserve_cluster_ip),
            self._ensure_object(
                policy["metadata"]["name"], user_ns, policy,
                create=net_api.create_namespaced_network_policy,
                read=net_api.read_namespaced_network_policy,
                replace=net_api.replace_namespaced_network_policy),
        ))

    def get_vmi_address(self, username: str, session_name: str) -> Optional[str]:
        """The running VMI's pod-network IP (masquerade forwards all ports,
        including the guest's sshd). None while the VMI is absent/booting."""
        user_ns = self._get_user_namespace(username)
        full_name = f"{username}-{session_name}"
        try:
            vmi = self.api.get_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns,
                KUBEVIRT_VMI_PLURAL, full_name)
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read VMI {full_name}: {e}")
            return None
        for iface in (vmi.get("status") or {}).get("interfaces") or []:
            if iface.get("ipAddress"):
                return iface["ipAddress"]
        return None

    def _create_vm(self, user_ns, full_name, session_name, username, uid,
                   template_spec, display_port, instancetype,
                   preemptible, home_pvc=None, shared_datasets=None,
                   start=False,
                   viewer=None, user_details=None) -> bool:
        # Issued (or renewed) before the cloud-init Secret is written, since
        # that document carries it into the guest. Best-effort: a cluster with
        # no CA yet just boots an uncertified guest.
        host_key, host_cert = self.ensure_session_host_cert(
            user_ns, full_name, uid,
            self.session_ssh_principals(username, session_name))
        vm_body, cloudinit_secret = self._build_vm_spec(
            session_name=full_name,
            hostname=session_name,
            username=username,
            uid=uid,
            template_spec=template_spec,
            display_port=display_port,
            home_pvc=home_pvc,
            shared_datasets=shared_datasets,
            instancetype=instancetype,
            preemptible=preemptible,
            user_details=user_details if user_details is not None else self.get_user(username),
            run_strategy="Always" if start else "Halted",
            portal_public_key=self._ensure_vm_access_key(username, user_ns),
            viewer=viewer,
            host_key=host_key,
            host_cert=host_cert,
        )
        # The userData Secret must exist before the VM starts; replace on
        # conflict so key/template changes reach the guest on its next boot.
        core_api = client.CoreV1Api()
        secret_name = cloudinit_secret["metadata"]["name"]
        try:
            core_api.create_namespaced_secret(user_ns, cloudinit_secret)
        except ApiException as e:
            if e.status != 409:
                logger.error(
                    f"Failed to create cloud-init secret {secret_name}: {e}")
                return False
            try:
                core_api.replace_namespaced_secret(
                    secret_name, user_ns, cloudinit_secret)
            except ApiException as pe:
                logger.warning(
                    f"Could not update cloud-init secret {secret_name}: {pe}")

        logger.debug(f"Creating KubeVirt VM:\n{yaml.safe_dump(vm_body)}")
        try:
            self.api.create_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns, KUBEVIRT_VM_PLURAL, vm_body
            )
            logger.info(f"VirtualMachine {full_name} created "
                        f"({'running' if start else 'halted until first connect'})")
            return True
        except ApiException as e:
            if e.status == 409:
                # Already exists — created Halted, or halted by stop_instance.
                # Reconcile its spec against the freshly built one so template
                # edits and per-session overrides (cpu/memory/gpu, image, zone,
                # nodeSelector) actually reach it; without this a VM keeps
                # whatever it was created with forever. KubeVirt applies
                # spec.template changes to the *next* VMI, so a running guest
                # picks them up on reboot — the same change-on-reboot contract
                # zones already have.
                self._patch_vm_spec(user_ns, full_name, vm_body["spec"])
                # Drive runStrategy to the run intent, in BOTH directions.
                # It used to only ever flip a VM on, which left stopping to
                # whoever asked for it — meaning the gateway and the portal
                # each needed `patch` on virtualmachines. Halting here is what
                # makes a stop a plain annotation write for them and keeps
                # KubeVirt writes inside the operator, where lifecycle already
                # lives. Cheap and idempotent: the patch is a no-op when the
                # VM is already in the wanted state.
                desired = "Always" if start else "Halted"
                try:
                    self.api.patch_namespaced_custom_object(
                        KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns,
                        KUBEVIRT_VM_PLURAL, full_name,
                        {"spec": {"runStrategy": desired}},
                    )
                except ApiException as pe:
                    logger.warning(f"Could not set runStrategy={desired} on "
                                   f"VirtualMachine {full_name}: {pe}")
                return True
            # 404 here means the KubeVirt CRDs are not installed in this cluster.
            logger.error(f"Failed to create VirtualMachine {full_name} "
                         f"(is KubeVirt installed?): {e}")
            return False

    def _patch_vm_spec(self, user_ns, full_name, desired_spec) -> None:
        """Reconcile an existing VirtualMachine toward the freshly built spec.
        Best-effort: a failure here leaves the VM on its old shape and the next
        reconcile retries. KubeVirt hands spec.template to the *next* VMI, so
        the guest sees the change on its next boot (the VM carries a
        RestartRequired condition meanwhile) — CPU/memory edits to a running VM
        therefore need a stop/start, matching how zones already behave."""
        try:
            current = self.api.get_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns,
                KUBEVIRT_VM_PLURAL, full_name)
        except ApiException as e:
            logger.warning(f"Could not read VirtualMachine {full_name}: {e}")
            return

        patch = self._build_vm_spec_patch(current.get("spec") or {}, desired_spec)
        if patch is None:
            return
        try:
            self.api.patch_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns,
                KUBEVIRT_VM_PLURAL, full_name, {"spec": patch})
            logger.info(f"Updated VirtualMachine {full_name} spec "
                        f"(applies on next boot): {json.dumps(patch)}")
        except ApiException as e:
            logger.warning(f"Could not update VirtualMachine {full_name}: {e}")

    def get_user_desktop_templates(self, username: str) -> List[Dict[str, Any]]:
        templates = []
        user_ns = self._get_user_namespace(username)
        namespaces_to_search = [self.namespace]
        if user_ns != self.namespace:
            namespaces_to_search.append(user_ns)

        for ns in namespaces_to_search:
            try:
                resp = self.api.list_namespaced_custom_object(
                    self.group, self.version, ns, TEMPLATE_PLURAL
                )
                for item in resp.get("items", []):
                    t = item.get("spec", {})
                    if t.get("mode") != "desktop":
                        continue
                    full_name = item["metadata"]["name"]
                    owner = t.get("user", "system")
                    if owner == "system":
                        t["name"] = full_name
                        t["fullName"] = full_name
                        t["source"] = "system"
                        templates.append(t)
                    elif owner == username:
                        display_name = full_name
                        if full_name.startswith(f"{username}-"):
                            display_name = full_name[len(username) + 1:]
                        t["name"] = display_name
                        t["fullName"] = full_name
                        t["source"] = "user"
                        templates.append(t)
            except ApiException as e:
                if e.status != 404:
                    logger.error(f"Failed to list desktop templates in {ns}: {e}")

        templates.sort(key=lambda x: x.get("source", ""))
        return templates

    @staticmethod
    def _pod_session_phase(pod) -> str:
        """Map a live pod to a desktop session phase (mirrors the operator's
        probe). An absent pod means the session was stopped."""
        if pod is None:
            return "Stopped"
        if pod.metadata.deletion_timestamp:
            return "Terminating"
        phase = getattr(pod.status, "phase", None)
        statuses = pod.status.container_statuses or []
        all_ready = bool(statuses) and all(cs.ready for cs in statuses)
        if phase == "Running" and all_ready:
            return "Ready"
        if phase == "Failed":
            return "Failed"
        if phase == "Pending":
            return "Pending"
        return "Booting"

    def get_user_desktop_sessions(self, username: str) -> List[Dict[str, Any]]:
        sessions = []
        user_ns = self._get_user_namespace(username)
        try:
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL,
                label_selector="whistler.martinmalmsten.net/mode=desktop",
            )

            # The CR's status.phase is only refreshed by the operator's ~10s timer,
            # so it lags reality (e.g. stays "Ready" for seconds after a stop). For
            # container/kata sessions derive the phase from the live pod instead, so
            # the dashboard reacts immediately. VM-runtime sessions have no pod, so
            # they otherwise keep the operator-reported phase -- except for
            # termination, which is checked live below too (a stop halts the VMI
            # without deleting the Session CR, so the CR-level deletionTimestamp
            # check further down never catches it).
            core_api = client.CoreV1Api()
            try:
                pods = core_api.list_namespaced_pod(user_ns)
                pod_map = {p.metadata.name: p for p in pods.items}
            except ApiException:
                pod_map = {}
            try:
                vmis = self.api.list_namespaced_custom_object(
                    KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns, KUBEVIRT_VMI_PLURAL,
                )
                vmi_map = {v["metadata"]["name"]: v for v in vmis.get("items", [])}
            except ApiException:
                vmi_map = {}

            for item in resp.get("items", []):
                spec = item.get("spec", {})
                status = item.get("status", {}) or {}
                full_name = item["metadata"]["name"]
                display_name = full_name
                if full_name.startswith(f"{username}-"):
                    display_name = full_name[len(username) + 1:]

                phase = status.get("phase", "Unknown")
                if status.get("runtime") != "vm":
                    phase = self._pod_session_phase(pod_map.get(full_name))
                else:
                    vmi = vmi_map.get(full_name)
                    if vmi and (vmi.get("metadata") or {}).get("deletionTimestamp"):
                        phase = "Terminating"
                # A deleting CR beats any derived phase: the operator's timer
                # skips deleting sessions, so status.phase would stay at its
                # last value (e.g. Ready) for the whole teardown.
                if item["metadata"].get("deletionTimestamp"):
                    phase = "Terminating"

                sessions.append({
                    "name": display_name,
                    "template": spec.get("templateRef"),
                    "namespace": user_ns,
                    "phase": phase,
                    # The unified runtime (container/kata/vm) replaces the old
                    # backend; keep the "backend" key as an alias for templates.
                    "runtime": status.get("runtime"),
                    "backend": status.get("runtime"),
                    "podName": status.get("podName"),
                    "vmiName": status.get("vmiName"),
                    "address": status.get("address"),
                    "displayPort": status.get("displayPort"),
                    "viewer": status.get("viewer"),
                })
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to list desktop sessions: {e}")
        return sessions

    def list_all_desktop_sessions(self) -> List[Dict[str, Any]]:
        """Every desktop session in the cluster, in one API call, for the
        portal's screenshot loop.

        Deliberately *not* the per-user view: this reports ``status.phase`` as
        the operator wrote it instead of deriving liveness from the live pod,
        because a background pass over the whole cluster shouldn't cost a pod
        list per namespace to shave ~10s off a phase that only gates a
        screenshot. Not on the ConfigManager ABC — the portal always holds a
        KubeConfigManager, and the ABC exists for the auth/routing surface the
        unit tests fake."""
        sessions: List[Dict[str, Any]] = []
        try:
            resp = self.api.list_cluster_custom_object(
                self.group, self.version, SESSION_PLURAL,
                label_selector="whistler.martinmalmsten.net/mode=desktop",
            )
        except ApiException as e:
            logger.error(f"Failed to list desktop sessions cluster-wide: {e}")
            return sessions

        for item in resp.get("items", []):
            meta = item.get("metadata", {})
            status = item.get("status", {}) or {}
            spec = item.get("spec", {}) or {}
            username = spec.get("user")
            full_name = meta.get("name", "")
            if not username or not full_name:
                continue
            display_name = full_name
            if full_name.startswith(f"{username}-"):
                display_name = full_name[len(username) + 1:]
            sessions.append({
                "user": username,
                "name": display_name,
                "namespace": meta.get("namespace"),
                # A deleting session is never Ready, whatever the CR still says.
                "phase": "Terminating" if meta.get("deletionTimestamp")
                         else status.get("phase", "Unknown"),
                "runtime": status.get("runtime"),
                "podName": status.get("podName"),
                "vmiName": status.get("vmiName"),
                "template": spec.get("templateRef"),
                "preemptible": spec.get("preemptible", False),
            })
        return sessions

    def add_desktop_session(self, username: str, template_name: str, session_name: str,
                            overrides: Optional[Dict[str, Any]] = None,
                            ephemeral: bool = False) -> bool:
        user_ns = self._ensure_user_namespace(username)
        spec = {
            "templateRef": template_name,
            "user": username,
        }
        if overrides:
            spec["overrides"] = overrides
        metadata = {
            "name": f"{username}-{session_name}",
            "namespace": user_ns,
            "labels": {"whistler.martinmalmsten.net/mode": "desktop"},
        }
        if ephemeral:
            metadata["annotations"] = {EPHEMERAL_ANNOTATION: "true"}
        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "Session",
            "metadata": metadata,
            "spec": spec,
        }
        try:
            self.api.create_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, body
            )
            return True
        except ApiException as e:
            logger.error(f"Failed to create desktop session: {e}")
            return False

    def delete_desktop_session(self, username: str, session_name: str) -> bool:
        logger.info(f"Attempting to delete desktop session {username}-{session_name}")
        user_ns = self._get_user_namespace(username)
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, f"{username}-{session_name}"
            )
            return True
        except ApiException as e:
            logger.error(f"Failed to delete desktop session: {e}")
            return False

    def get_server_host_key(self, secret_name: str) -> Optional[bytes]:
        try:
            api = CoreV1Api()
            secret = api.read_namespaced_secret(secret_name, self.namespace)
            
            # secret.data is a dictionary where values are base64-decoded bytes (if using client python models)
            # Wait, kubernetes python client automatically decodes base64 data in .data?
            # No, usually .data contains base64 encoded strings, but read_namespaced_secret returns a V1Secret object.
            # let's check V1Secret.data type. It's dict(str, str).
            # Actually, the python client MIGHT decode it if we access it a certain way, or we need to decode it.
            # Let's verify. standard client returns base64 strings in .data.
            # WAIT. CoreV1Api.read_namespaced_secret returns V1Secret.
            # V1Secret.data -> 'The value is base64 encoded strings.'
            
            if secret.data and 'host_key' in secret.data:
                import base64
                return base64.b64decode(secret.data['host_key'])
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to get host key secret: {e}")
        except Exception as e:
            logger.error(f"Error reading host key: {e}")
        return None

    def save_server_host_key(self, secret_name: str, key_data: bytes) -> bool:
        import base64
        encoded = base64.b64encode(key_data).decode('utf-8')

        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": secret_name,
                "namespace": self.namespace
            },
            "type": "Opaque",
            "data": {
                "host_key": encoded
            }
        }

        api = CoreV1Api()
        try:
            try:
                api.read_namespaced_secret(secret_name, self.namespace)
                # Update
                api.patch_namespaced_secret(secret_name, self.namespace, body)
            except ApiException as e:
                if e.status == 404:
                    # Create
                    api.create_namespaced_secret(self.namespace, body)
                else:
                    raise e
            return True
        except ApiException as e:
             logger.error(f"Failed to save host key secret: {e}")
             return False

    # ------------------------------------------------------------------ #
    # SSH host CA (design/proxyjump.md, hostca.py)                          #
    # ------------------------------------------------------------------ #

    def ensure_ssh_ca(self) -> Optional[bytes]:
        """The CA private key, generating and persisting one on first use.

        Create-if-absent and 409-tolerant: the operator and the gateway race
        on startup, and losing that race must mean "read theirs", never
        "overwrite with mine" — a replaced CA silently invalidates every
        certificate already delivered to a guest.

        Cached in memory once read: this is on the VM reconcile path, and the
        CA never changes during a process's life (a rotation is a restart).
        """
        cached = getattr(self, "_ssh_ca_key", None)
        if cached:
            return cached

        api = CoreV1Api()
        existing = self._read_ssh_ca_secret(api)
        if existing:
            self._ssh_ca_key = existing
            return existing

        key_data = hostca.generate_ca_key()
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": self.ssh_ca_secret_name,
                         "namespace": self.namespace},
            "type": "Opaque",
            "data": {
                "ca_key": base64.b64encode(key_data).decode(),
                "ca_pub": base64.b64encode(
                    hostca.ca_public_key(key_data).encode()).decode(),
            },
        }
        try:
            api.create_namespaced_secret(self.namespace, body)
            logger.info(f"Generated SSH host CA in secret {self.ssh_ca_secret_name}")
            self._ssh_ca_key = key_data
            return key_data
        except ApiException as e:
            if e.status == 409:
                # Someone else created it between our read and our write.
                self._ssh_ca_key = self._read_ssh_ca_secret(api)
                return self._ssh_ca_key
            logger.error(f"Failed to create SSH CA secret: {e}")
            return None

    def _read_ssh_ca_secret(self, api=None) -> Optional[bytes]:
        api = api or CoreV1Api()
        try:
            secret = api.read_namespaced_secret(
                self.ssh_ca_secret_name, self.namespace)
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read SSH CA secret: {e}")
            return None
        data = secret.data or {}
        if "ca_key" in data:
            return base64.b64decode(data["ca_key"])
        return None

    def get_ssh_ca_public_key(self) -> Optional[str]:
        """The CA's public half, for the user's ``@cert-authority`` line.
        Read-only: a component that only verifies never creates the CA."""
        try:
            secret = CoreV1Api().read_namespaced_secret(
                self.ssh_ca_secret_name, self.namespace)
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read SSH CA secret: {e}")
            return None
        data = secret.data or {}
        if "ca_pub" in data:
            return base64.b64decode(data["ca_pub"]).decode().strip()
        if "ca_key" in data:
            return hostca.ca_public_key(base64.b64decode(data["ca_key"]))
        return None

    def get_ssh_known_hosts_line(self) -> Optional[str]:
        """The one line a user adds to ~/.ssh/known_hosts to trust every
        instance, present and future."""
        ca_pub = self.get_ssh_ca_public_key()
        if not ca_pub:
            return None
        return hostca.known_hosts_line(ca_pub, f"*{self.ssh_domain_suffix}")

    def session_service_host(self, username: str, session_name: str) -> str:
        """The address Whistler's own components dial a session by: the
        per-session Service's cluster DNS name.

        One function so the relay's target and the certificate's principals
        cannot drift. They did: the relay dialled the FQDN while the
        certificate only carried the bare Service name, and asyncssh checks a
        host certificate's principals against *the name it connected to*
        (`_validate_openssh_host_certificate` → `cert.validate(CERT_TYPE_HOST,
        host)`), so every relay failed verification against a perfectly valid
        certificate — reported to the user as "could not open a session",
        which reads like a missing sshd.
        """
        return (f"{username}-{session_name}."
                f"{self._get_user_namespace(username)}.{CLUSTER_DNS_SUFFIX}")

    def session_ssh_principals(self, username: str, session_name: str) -> List[str]:
        """Host principals a session's certificate must carry: the names a
        client can dial it by. The suffixed form is the one end users actually
        verify (`ssh box.w`); the rest are the internal names Whistler's own
        components (the relay, the screenshot grabber, the portal) reach it as
        — the bare ``<user>-<session>`` Service name and the three
        search-path forms of its cluster DNS name, since a resolver's search
        list decides which of them a caller ends up dialling.
        """
        full_name = f"{username}-{session_name}"
        user_ns = self._get_user_namespace(username)
        return hostca.session_principals(
            session_name, self.ssh_domain_suffix,
            extra=[full_name,
                   f"{full_name}.{user_ns}",
                   f"{full_name}.{user_ns}.svc",
                   f"{full_name}.{user_ns}.{CLUSTER_DNS_SUFFIX}"])

    def _host_cert_secret_name(self, full_name: str) -> str:
        return f"{full_name}-hostcert"

    def ensure_session_host_cert(self, user_ns: str, full_name: str, uid: str,
                                 principals: List[str]):
        """Return ``(host_key_pem, cert_line)`` for a session, issuing on first
        call and re-issuing when the names or the expiry demand it.

        Persisted in a per-session Secret rather than regenerated per build:
        the cloud-init Secret is *replaced* on every reconcile, so a freshly
        generated key each time would change the guest's identity on every
        reboot — precisely the churn the CA exists to end. The key survives
        re-issue; only the certificate over it is replaced.

        Owner-referenced to the Session, so it is garbage-collected with it.
        Returns ``(None, None)`` when no CA is available, which callers treat
        as "no certificate this time" rather than a failure — an uncertified
        guest is the pre-CA status quo, not a broken one.
        """
        ca_key = self.ensure_ssh_ca()
        if not ca_key:
            return (None, None)

        api = CoreV1Api()
        secret_name = self._host_cert_secret_name(full_name)
        host_key = cert_line = None
        valid_before = 0
        try:
            secret = api.read_namespaced_secret(secret_name, user_ns)
            data = secret.data or {}
            if "host_key" in data:
                host_key = base64.b64decode(data["host_key"])
            if "host_cert" in data:
                cert_line = base64.b64decode(data["host_cert"]).decode()
            valid_before = int(
                (secret.metadata.annotations or {}).get(
                    "whistler/cert-valid-before", 0) or 0)
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read host cert secret {secret_name}: {e}")
                return (None, None)

        if host_key and not hostca.needs_reissue(cert_line, principals, valid_before):
            return (host_key, cert_line)

        try:
            host_key, cert_line, valid_before = hostca.issue_host_cert(
                ca_private_key=ca_key,
                principals=principals,
                key_id=full_name,
                host_private_key=host_key,
            )
        except Exception as e:
            logger.error(f"Failed to issue host certificate for {full_name}: {e}")
            return (None, None)

        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": secret_name,
                "annotations": {"whistler/cert-valid-before": str(valid_before)},
                "ownerReferences": [self._session_owner_reference(full_name, uid)],
            },
            "type": "Opaque",
            "data": {
                "host_key": base64.b64encode(host_key).decode(),
                "host_cert": base64.b64encode(cert_line.encode()).decode(),
            },
        }
        try:
            api.create_namespaced_secret(user_ns, body)
        except ApiException as e:
            if e.status != 409:
                logger.error(f"Failed to store host cert {secret_name}: {e}")
                return (None, None)
            try:
                api.replace_namespaced_secret(secret_name, user_ns, body)
            except ApiException as pe:
                logger.error(f"Failed to update host cert {secret_name}: {pe}")
                return (None, None)
        logger.info(f"Issued SSH host certificate for {full_name} "
                    f"(principals: {', '.join(principals)})")
        return (host_key, cert_line)

    def zone_ssh_posture(self, zone: str) -> str:
        """How a zone treats interactive SSH (SSH_POSTURES). Unknown values
        fail closed to `none`, matching how an unknown zone already does —
        a typo in a restricted zone's posture must not silently open it."""
        posture = (self.zones.get(zone) or {}).get("ssh")
        if posture is None:
            return SSH_POSTURE_DIRECT
        posture = str(posture).strip().lower()
        if posture not in SSH_POSTURES:
            logger.warning(
                f"Zone {zone} has unknown ssh posture {posture!r}; denying SSH")
            return SSH_POSTURE_NONE
        return posture

    # ------------------------------------------------------------------ #
    # Access channels (design/security.md, "Access channels")             #
    # ------------------------------------------------------------------ #

    def zone_channel_ceiling(self, zone: str) -> Set[str]:
        """The most any session in this zone may use.

        `Zone.spec.channels` when set — an unknown name is dropped with a
        warning, the same fail-closed reading as an unknown ssh posture, and
        an explicitly empty list is a ceiling of nothing. When the field is
        absent the ceiling is derived from the legacy `ssh` posture so a zone
        written before channels existed keeps its exact meaning: `direct`
        ceilings nothing, `relay` closes the jump, `none` closes both, and
        none of the three ever governed the other channels."""
        cfg = self.zones.get(zone) or {}
        stated = cfg.get("channels")
        if stated is not None:
            ceiling = set()
            for channel in stated:
                if channel in CHANNELS:
                    ceiling.add(channel)
                else:
                    logger.warning(
                        f"Zone {zone} lists unknown channel {channel!r}; ignoring")
            return ceiling
        posture = self.zone_ssh_posture(zone)
        return set(_POSTURE_CHANNELS[posture]) | {
            CHANNEL_TERMINAL, CHANNEL_CLIPBOARD, CHANNEL_SCREENSHOTS}

    def get_user_channels(self, username: str) -> Optional[Set[str]]:
        """This user's channel grant — their own unioned with every group's —
        or None when nobody states one (no narrowing of the zone ceiling).

        This is the third axis: the same zone, the same instance, different
        doors for the internal helper and the external researcher."""
        self._load_users()
        own = self.users.get(username, {}).get("channels")
        return merge_channel_grants(own, *(g.get("channels")
                                           for g in self.get_user_groups(username)))

    def effective_channels(self, username: str, zone: str) -> Set[str]:
        """What ``username`` may actually use in ``zone``: the ceiling narrowed
        by the grant. The grant never widens — a channel absent from the
        ceiling stays absent however it was granted.

        Reads the zone catalog as it stands rather than reloading it: the one
        path that matters for freshness — session_channels, and the gateway's
        resolve_ssh_target — already reloads before it gets here, and a second
        list call per connection buys nothing."""
        ceiling = self.zone_channel_ceiling(zone)
        grant = self.get_user_channels(username)
        return ceiling if grant is None else (ceiling & grant)

    def session_channels(self, username: str, name: str) -> Set[str]:
        """The channel set for one of this user's sessions, resolved through
        the session's own zone. The portal's entry point — it holds a user and
        a session name, not a zone. An unresolvable session gets the empty
        set: refusing a channel to a session that does not exist is free, and
        the caller is about to 404 anyway."""
        target = self.resolve_ssh_target(username, name)
        if not target:
            return set()
        return self.effective_channels(username, target.get("zone") or DEFAULT_ZONE)

    def resolve_ssh_target(self, username: str, name: str) -> Optional[Dict[str, Any]]:
        """Where an SSH jump for ``<name>`` should land, or None when this user
        has no session by that name.

        The single lookup point for jump routing, so the membership rule lives
        in one place when project instances arrive (design/proxyjump.md,
        "Membership, not ownership"). Today "sessions the user may reach" is
        exactly "sessions in their own namespace" — ssh-mode and desktop alike,
        which is what makes `ssh mydesktop.w` reach a VM the portal created.

        The address is the per-session Service's cluster DNS name rather than a
        live pod/VMI IP: it is stable across reboots, so nothing here has to be
        re-resolved when a guest restarts, and dialling it while the workload is
        down simply fails — which the caller is retrying through anyway.
        """
        user_ns = self._get_user_namespace(username)
        full_name = f"{username}-{name}"
        try:
            cr = self.api.get_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, full_name)
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read session {full_name}: {e}")
            return None

        spec = cr.get("spec") or {}
        status = cr.get("status") or {}
        # Freshly loaded, not the catalog cached at startup: an admin who sets
        # a zone's ssh posture to `none` in the portal must close it now, not
        # at the gateway's next restart. Cheap — a jump connect is rare, and
        # this call is already reading two other objects.
        self._load_zones()
        template = self._resolve_template(user_ns, spec.get("templateRef")) or {}
        template_spec = template.get("spec", {})
        # The *running* session's zone, so a one-shot zone chosen at start is
        # the one whose ssh posture and channels the gateway enforces.
        zone = ((effective_session_overrides(spec) or {}).get("zone")
                or template_spec.get("zone") or DEFAULT_ZONE)
        return {
            "name": name,
            "fullName": full_name,
            "namespace": user_ns,
            "runtime": status.get("runtime") or template_spec.get("runtime", "container"),
            "mode": status.get("mode") or template_spec.get("mode", "ssh"),
            "host": self.session_service_host(username, name),
            "port": SESSION_SSH_PORT,
            "zone": zone,
            "sshPosture": self.zone_ssh_posture(zone),
            # The zone's ceiling narrowed by this user's (and their groups')
            # channel grant — the set the gateway actually decides on. Kept
            # alongside sshPosture rather than replacing it because the two
            # answer different questions: the posture is the zone's stance,
            # this is what *this person* gets there.
            "channels": sorted(self.effective_channels(username, zone)),
            "phase": status.get("phase"),
            # Created on demand by the gateway and reapable once nothing is
            # connected (design/proxyjump.md).
            "ephemeral": (cr["metadata"].get("annotations") or {}).get(
                EPHEMERAL_ANNOTATION) == "true",
            # The operator records a hard policy refusal here; without it a
            # refused start is indistinguishable from a slow boot and the user
            # gets a mystery timeout instead of the reason.
            "policyFailed": bool(status.get("policyFailed")),
            "statusMessage": status.get("statusMessage"),
        }

    def list_ssh_targets(self, username: str) -> List[Dict[str, Any]]:
        """Every session this user has, in one list, with whether SSH can
        actually reach it.

        The launcher's data source. It spans ssh-mode instances *and* desktop
        sessions because jump routing does — `resolve_ssh_target` looks a name
        up regardless of mode — and listing only one kind meant the launcher
        showed precisely the sessions it could *not* connect to (pods, which
        have had no sshd since the exec bridge was removed) while omitting the
        ones it reaches fine (VMs).

        ``sshReachable`` keys on the runtime because that is the honest signal
        today: VMs run sshd from cloud-init; a container runs whatever its
        image runs, which is not sshd. When a Whistler-compatible pod image
        lands (design/proxyjump.md) this is the single place to widen.
        """
        targets = []
        for inst in self.get_user_instances(username):
            # The runtime comes from the session, not from the mode: an
            # ssh-mode session is a container *or* a VM (images/devbase is
            # `mode: ssh, runtime: vm`). Hardcoding "container" here listed a
            # devbase VM as "web terminal only" and made connect refuse it up
            # front — the launcher showing exactly the sessions it cannot reach,
            # which is the failure this method was written to end.
            runtime = inst.get("runtime") or "container"
            targets.append({
                "name": inst.get("name"),
                "template": inst.get("template"),
                "status": inst.get("status"),
                "runtime": runtime,
                "mode": "ssh",
                "sshReachable": runtime == "vm",
            })
        for sess in self.get_user_desktop_sessions(username):
            runtime = sess.get("runtime")
            targets.append({
                "name": sess.get("name"),
                "template": sess.get("template"),
                "status": sess.get("phase"),
                "runtime": runtime,
                "mode": "desktop",
                "sshReachable": runtime == "vm",
            })
        targets.sort(key=lambda t: t.get("name") or "")
        return targets

    # ------------------------------------------------------------------ #
    # Admin / management operations                                        #
    # ------------------------------------------------------------------ #

    def _save_user_spec(self, username: str, spec_updates: Dict[str, Any]) -> bool:
        """Get-merge-replace-or-create a single User CR, mirroring
        save_system_template: only the keys in spec_updates are touched, so
        concurrent partial updates (e.g. set_user_overrides) don't clobber the
        rest of the spec.

        A value of ``None`` **removes** its key rather than writing a null.
        That is the only way to say "this user states nothing here", which for
        `channels` is a different thing from an empty list (which grants
        nothing) — see merge_channel_grants."""
        writes = {k: v for k, v in spec_updates.items() if v is not None}
        removals = [k for k, v in spec_updates.items() if v is None]
        try:
            try:
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, self.namespace, USER_PLURAL, username
                )
                merged = {**(existing.get("spec") or {}), **writes}
                for key in removals:
                    merged.pop(key, None)
                body = {
                    "apiVersion": f"{self.group}/{self.version}",
                    "kind": "User",
                    "metadata": {"name": username, "namespace": self.namespace,
                                 "resourceVersion": existing["metadata"]["resourceVersion"]},
                    "spec": merged,
                }
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, self.namespace, USER_PLURAL, username, body
                )
            except ApiException as e:
                if e.status == 404:
                    body = {
                        "apiVersion": f"{self.group}/{self.version}",
                        "kind": "User",
                        "metadata": {"name": username, "namespace": self.namespace},
                        "spec": writes,
                    }
                    self.api.create_namespaced_custom_object(
                        self.group, self.version, self.namespace, USER_PLURAL, body
                    )
                else:
                    raise
            return True
        except ApiException as e:
            logger.error(f"Failed to save user {username}: {e}")
            return False

    def list_all_users(self) -> List[Dict[str, Any]]:
        self._load_users()
        return list(self.users.values())

    def save_user(self, user_data: Dict[str, Any]) -> bool:
        username = user_data.get("name")
        if not username:
            return False
        spec = {k: v for k, v in user_data.items() if k != "name"}
        return self._save_user_spec(username, spec)

    def delete_user(self, username: str) -> bool:
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, self.namespace, USER_PLURAL, username
            )
            return True
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to delete user {username}: {e}")
            return False

    def is_user_admin(self, username: str) -> bool:
        self._load_users()
        return bool(self.users.get(username, {}).get("admin", False))

    def _has_any_access_cell(self, username: str, volume: str) -> bool:
        """Whether the user's OWN matrix mentions this volume in any zone.
        Own, not merged: a group grant is somebody else's decision and must
        not suppress the backfill of the user's own home."""
        self._load_users()
        own = self.users.get(username, {}).get("volumeAccess") or {}
        return any(volume in (cells or {}) for cells in own.values())

    def _instance_zone(self, user_ns: str, instance: str) -> str:
        """The zone an existing VM was built in, read from the label the
        operator stamps at build time. Falls back to the default zone, which
        is where an unzoned template lands anyway."""
        try:
            vm = self.api.get_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns,
                KUBEVIRT_VM_PLURAL, instance)
        except (ApiException, AttributeError):
            return DEFAULT_ZONE
        labels = (((vm.get("spec") or {}).get("template") or {})
                  .get("metadata") or {}).get("labels") or {}
        return labels.get(ZONE_LABEL) or DEFAULT_ZONE

    def adopt_legacy_home_disks(self) -> int:
        """One-shot migration: turn per-instance home PVCs into named
        HomeVolumes, and cut them loose from their Session.

        Before named volumes a home was `whistler-home-<instance>`, owner-
        referenced to the Session so Kubernetes GC reaped the two together.
        That reference is now wrong in the most damaging possible way: it
        would delete a user's home the moment the instance that happened to
        create it was removed. So this both records the volume and REMOVES the
        ownerReference.

        Idempotent, and safe to run on every operator start: a claim with no
        ownerReference and a matching HomeVolume is already adopted and is
        skipped. Returns the number of claims adopted, for the log.
        """
        adopted = 0
        core = client.CoreV1Api()
        try:
            namespaces = core.list_namespace(
                label_selector=f"{USER_NS_LABEL}").items
        except ApiException as e:
            logger.error(f"Could not list user namespaces to adopt homes: {e}")
            return 0
        for ns in namespaces:
            username = (ns.metadata.labels or {}).get(USER_NS_LABEL)
            ns_name = ns.metadata.name
            if not username:
                continue
            try:
                claims = core.list_namespaced_persistent_volume_claim(
                    ns_name, label_selector="app=whistler").items
            except ApiException as e:
                logger.error(f"Could not list claims in {ns_name}: {e}")
                continue
            existing = {v.get("name") for v in self.get_home_volumes(username)}
            for claim in claims:
                name = claim.metadata.name
                if not name.startswith("whistler-home-"):
                    continue
                instance = name[len("whistler-home-"):]
                owners = claim.metadata.owner_references or []
                is_owned = any(o.kind == "Session" for o in owners)
                # Fully done means all three: recorded, released, and granted
                # somewhere. Anything less falls through and is completed —
                # which is what lets a volume adopted before the access matrix
                # existed pick up its cell on a later run.
                if (instance in existing and not is_owned
                        and self._has_any_access_cell(username, instance)):
                    continue
                if instance not in existing:
                    if not self.save_home_volume(username, {
                            "name": instance,
                            "description": f"Home for instance {instance} "
                                           f"(adopted)",
                            "pvcName": name,
                            "size": (claim.spec.resources.requests or {}).get(
                                "storage") if claim.spec.resources else None}):
                        continue
                # Backfill a missing access cell, whether the volume was
                # adopted just now or on an earlier run. Not inside the branch
                # above: adoption is idempotent, so a volume adopted BEFORE
                # the matrix existed would otherwise never get a cell — and
                # the matrix has no defaults, so its owner's instances would
                # all be refused at their next start. Granted where the
                # instance already runs, which changes nothing about what that
                # instance could already do.
                if not self._has_any_access_cell(username, instance):
                    zone = self._instance_zone(ns_name, instance)
                    self.grant_own_volume_access(username, zone, instance)
                    logger.info(f"Granted adopted home {instance!r} in zone "
                                f"{zone} for {username}")
                if is_owned:
                    keep = [o for o in owners if o.kind != "Session"]
                    try:
                        if keep:
                            # Read-modify-write: ownerReferences merges by uid,
                            # so a merge patch cannot REMOVE one entry from a
                            # list while keeping others.
                            live = core.read_namespaced_persistent_volume_claim(
                                name, ns_name)
                            live.metadata.owner_references = keep
                            core.replace_namespaced_persistent_volume_claim(
                                name, ns_name, live)
                        else:
                            # null deletes the key. An empty LIST would be a
                            # silent no-op — ownerReferences has a merge patch
                            # strategy, so merging [] into it changes nothing,
                            # and the home would still be reaped with its
                            # Session while looking adopted. Measured
                            # 2026-08-18; same trap as _ensure_object's
                            # "Replace, not patch".
                            core.patch_namespaced_persistent_volume_claim(
                                name, ns_name,
                                {"metadata": {"ownerReferences": None}})
                    except ApiException as e:
                        logger.error(
                            f"Could not release {name} from its Session: {e}")
                        continue
                adopted += 1
                logger.info(f"Adopted home {name} in {ns_name} as home volume "
                            f"{instance!r} for {username}")
        return adopted

    def ensure_bootstrap_admin(self):
        """Create-if-absent seed of the first admin User CR from
        whistler.bootstrapAdmin (values.yaml). Called once by the operator at
        startup (kopf.on.startup); never overwrites an existing User CR of the
        same name, so later edits (via the portal or kubectl) stick across
        Helm upgrades.

        It seeds NEW_USER_ENTRY_POINTS and NEW_USER_ZONES because an empty
        list now grants nothing: without them the account this exists to
        create could not open the portal it exists to be used from. Only on
        creation — an admin who has since narrowed their own entry points does
        not get them handed back on the next operator restart."""
        try:
            with open(BOOTSTRAP_ADMIN_FILE, "r") as f:
                data = yaml.safe_load(f) or {}
        except FileNotFoundError:
            return
        name = (data.get("name") or "").strip()
        if not name:
            return
        try:
            self.api.get_namespaced_custom_object(
                self.group, self.version, self.namespace, USER_PLURAL, name
            )
            logger.info(f"Bootstrap admin '{name}' already exists, skipping.")
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to check bootstrap admin '{name}': {e}")
                return
            body = {
                "apiVersion": f"{self.group}/{self.version}",
                "kind": "User",
                "metadata": {"name": name, "namespace": self.namespace},
                "spec": {"publicKeys": data.get("publicKeys") or [],
                         "admin": True,
                         "entryPoints": list(NEW_USER_ENTRY_POINTS),
                         "allowedZones": list(NEW_USER_ZONES)},
            }
            try:
                self.api.create_namespaced_custom_object(
                    self.group, self.version, self.namespace, USER_PLURAL, body
                )
                logger.info(f"Created bootstrap admin user '{name}'.")
            except ApiException as ce:
                logger.error(f"Failed to create bootstrap admin '{name}': {ce}")

    # ------------------------------------------------------------------ #
    # Groups: a project's shared grants (design/security.md, "Group")      #
    #                                                                      #
    # Every get_user_* accessor below answers with the *effective* grant —  #
    # the user's own field unioned with each group they belong to — so      #
    # _apply_policy, _apply_overrides and the portal all see one resolved   #
    # answer and no caller has to remember that groups exist.               #
    # ------------------------------------------------------------------ #

    def _load_groups(self):
        """Load the Group catalog. Like _load_users: on API failure keep the
        previous catalog rather than wiping it, since an empty catalog reads
        as "nobody is in a project" and would cut every member back to their
        own (often empty) allow-lists.

        Since grants became explicit that direction is *fail-closed* — a lost
        catalog locks people out rather than letting them in, which is the
        right way round but is also why the previous catalog is kept rather
        than dropped: a flapping API call must not bounce a project's members
        out of their own sessions. A cluster without the CRD simply has no
        groups, and warns once, because "no groups" and "the CRD was never
        applied" look identical from here. This is the state a `helm upgrade`
        leaves behind, because Helm does not update CRDs."""
        try:
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, self.namespace, GROUP_PLURAL
            )
        except (ApiException, AttributeError) as e:
            if getattr(e, "status", None) == 404 and not self._warned_no_group_crd:
                type(self)._warned_no_group_crd = True
                logger.warning(
                    "No Group CRD in this cluster, so no group grants apply "
                    "and every user falls back to their own allow-lists. "
                    "`helm upgrade` does not update CRDs — run "
                    "`kubectl apply -f charts/whistler/crds/crds.yaml`")
            else:
                logger.debug(f"Failed to load groups ({e}); keeping previous catalog")
            return
        type(self)._warned_no_group_crd = False
        self.groups = {
            item["metadata"]["name"]: {**(item.get("spec") or {}),
                                       "name": item["metadata"]["name"]}
            for item in resp.get("items", [])
        }

    def get_user_groups(self, username: str) -> List[Dict[str, Any]]:
        """The groups this user belongs to, by name. Membership is either
        plain ``members`` or a per-member entry in a volume's ``access`` map —
        both are ways of being granted something by the project."""
        self._load_groups()
        member_of = []
        for name in sorted(self.groups):
            # `name` re-stamped rather than trusted from the spec: callers
            # (the portal's provenance table) key on it, and a catalog set
            # directly — as the unit tests do — hasn't been through the loader.
            spec = {**self.groups[name], "name": name}
            if username in (spec.get("members") or []):
                member_of.append(spec)
            elif group_volume_grants(spec, username):
                member_of.append(spec)
        return member_of

    def get_user_volume_access(self, username: str) -> Dict[str, Dict[str, str]]:
        """The user's effective access matrix: ``{zone: {volume: mode}}``,
        their own table merged with every group's. Absent means no access."""
        self._load_users()
        own = self.users.get(username, {}).get("volumeAccess") or {}
        return merge_volume_access(own, *(
            (g.get("volumeAccess") or {})
            for g in self.get_user_groups(username)))

    def volume_access(self, username: str, zone: str,
                      volume: str) -> Optional[str]:
        """``"allowed"`` / ``"read-only"`` / ``None`` for one cell. None is a
        refusal, not a default — there is nothing below it to fall back to."""
        return (self.get_user_volume_access(username).get(zone) or {}).get(volume)

    def set_user_volume_access(self, username: str,
                               matrix: Dict[str, Dict[str, str]]) -> bool:
        """Replace the user's OWN matrix. Group-derived cells are not written
        here — saving the merged view would copy a project's grants onto a
        member permanently, the same trap the volume allow-list editor
        already avoids."""
        pruned = {zone: {v: m for v, m in (cells or {}).items()
                         if m in ACCESS_MODES}
                  for zone, cells in (matrix or {}).items()}
        pruned = {z: cells for z, cells in pruned.items() if cells}
        # None removes the key: "this user states nothing" rather than an
        # empty object, so the CR stays clean when every cell is cleared.
        return self._save_user_spec(username, {"volumeAccess": pruned or None})

    def grant_own_volume_access(self, username: str, zone: str, volume: str,
                                mode: str = "allowed") -> bool:
        """Add a single cell to the user's own matrix, leaving the rest alone.

        This is what makes home volumes self-service: a user creating a volume
        in a zone they may already enter is not gaining anything — they could
        already start an instance there with a fresh home — so the grant is
        written without an admin. Anything else (another zone, another user)
        stays an admin's decision.
        """
        if mode not in ACCESS_MODES:
            return False
        self._load_users()
        own = dict(self.users.get(username, {}).get("volumeAccess") or {})
        row = dict(own.get(zone) or {})
        row[volume] = mode
        own[zone] = row
        return self._save_user_spec(username, {"volumeAccess": own})

    def revoke_own_volume_access(self, username: str, volume: str) -> bool:
        """Drop a volume from every zone of the user's own matrix — used when
        the volume itself is deleted, so the grid does not accumulate rows for
        things that no longer exist."""
        self._load_users()
        own = dict(self.users.get(username, {}).get("volumeAccess") or {})
        changed = False
        for zone in list(own):
            if volume in (own[zone] or {}):
                own[zone] = {v: m for v, m in own[zone].items() if v != volume}
                changed = True
            if not own[zone]:
                own.pop(zone)
        if not changed:
            return True
        return self._save_user_spec(username, {"volumeAccess": own or None})

    def get_user_allowed_volumes(self, username: str) -> List[str]:
        self._load_users()
        own = self.users.get(username, {}).get("allowedVolumes", [])
        return merge_allow_lists(own, *(
            list(group_volume_grants(g, username))
            for g in self.get_user_groups(username)))

    def get_user_volume_modes(self, username: str) -> Dict[str, str]:
        """``{volume: "rw"|"ro"}`` for every volume a *group* grants this user.

        Only group-granted volumes appear: a volume the user reaches through
        their own allowedVolumes is read-write, which is what it has always
        been. Across groups the most
        permissive grant wins — a user granted ``rw`` in one project does not
        lose it by also being a read-only guest in another."""
        modes: Dict[str, str] = {}
        for group in self.get_user_groups(username):
            for name, mode in group_volume_grants(group, username).items():
                if modes.get(name) != "rw":
                    modes[name] = mode
        # A volume the user holds outright is theirs read-write, whatever a
        # group says: the group grant adds access, it cannot take it away.
        self._load_users()
        for name in self.users.get(username, {}).get("allowedVolumes", []) or []:
            modes[name] = "rw"
        return modes

    def set_user_allowed_volumes(self, username: str, volume_names: List[str]) -> bool:
        return self._save_user_spec(username, {"allowedVolumes": volume_names})

    def get_user_allowed_gpu_types(self, username: str) -> List[str]:
        self._load_users()
        own = self.users.get(username, {}).get("allowedGpuTypes", [])
        return merge_allow_lists(own, *(g.get("allowedGpuTypes")
                                        for g in self.get_user_groups(username)))

    def set_user_allowed_gpu_types(self, username: str, gpu_types: List[str]) -> bool:
        return self._save_user_spec(username, {"allowedGpuTypes": gpu_types})

    def get_user_allowed_zones(self, username: str) -> List[str]:
        self._load_users()
        own = self.users.get(username, {}).get("allowedZones", [])
        return merge_allow_lists(own, *(g.get("allowedZones")
                                        for g in self.get_user_groups(username)))

    def set_user_allowed_zones(self, username: str, zones: List[str]) -> bool:
        return self._save_user_spec(username, {"allowedZones": zones})

    def get_user_entry_points(self, username: str) -> List[str]:
        self._load_users()
        own = self.users.get(username, {}).get("entryPoints", [])
        return merge_allow_lists(own, *(g.get("entryPoints")
                                        for g in self.get_user_groups(username)))

    def set_user_entry_points(self, username: str, entry_points: List[str]) -> bool:
        # Order the write by ENTRY_POINTS rather than by the form, so the CR
        # reads the same whichever way the boxes were ticked.
        return self._save_user_spec(
            username, {"entryPoints": [e for e in ENTRY_POINTS
                                       if e in set(entry_points or [])]})

    def get_user_overrides(self, username: str) -> Dict[str, bool]:
        self._load_users()
        own = self.users.get(username, {}).get("overrides", {}) or {}
        merged = merge_override_grants(own, *(g.get("overrides")
                                              for g in self.get_user_groups(username)))
        return {g: bool(merged.get(g, False)) for g in OVERRIDE_GROUPS}

    def set_user_overrides(self, username: str, overrides: Dict[str, bool]) -> bool:
        normalized = {g: bool(overrides.get(g, False)) for g in OVERRIDE_GROUPS}
        return self._save_user_spec(username, {"overrides": normalized})

    def set_user_channels(self, username: str, channels: Optional[List[str]]) -> bool:
        """Set this user's own channel grant, or clear it with ``None``.

        Clearing *removes* the field (this user narrows nothing); passing an
        empty list writes one (this user is granted nothing but the desktop
        stream). The two are different answers and both have to be
        expressible — see merge_channel_grants."""
        return self._save_user_spec(
            username, {"channels": None if channels is None else list(channels)})

    # -- Groups: admin CRUD ------------------------------------------------ #

    def get_group_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Full group catalog (name -> spec), freshly loaded — drives the
        admin Groups editor."""
        self._load_groups()
        return {name: dict(spec) for name, spec in self.groups.items()}

    def save_group(self, group_data: Dict[str, Any]) -> bool:
        """Create or update a Group CR from the admin editor. Unlike
        save_zone this needs no propagation step: a group grants nothing that
        is materialized in the cluster — every field is read at policy time,
        so an edit applies to the next session start (and, for channels, to
        the next connection attempt).

        Returns False for a request that is simply malformed (no name), and
        raises ConfigWriteError when the cluster refuses the write — the
        caller has an admin in front of it who deserves the reason."""
        data = dict(group_data)
        name = (data.pop("name", "") or "").strip()
        if not name:
            return False
        spec = {k: v for k, v in data.items() if v not in (None, "")}
        try:
            try:
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, self.namespace, GROUP_PLURAL, name
                )
                body = {
                    "apiVersion": f"{self.group}/{self.version}",
                    "kind": "Group",
                    "metadata": {"name": name, "namespace": self.namespace,
                                 "resourceVersion": existing["metadata"]["resourceVersion"]},
                    "spec": spec,
                }
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, self.namespace, GROUP_PLURAL, name, body
                )
            except ApiException as e:
                if e.status != 404:
                    raise
                body = {
                    "apiVersion": f"{self.group}/{self.version}",
                    "kind": "Group",
                    "metadata": {"name": name, "namespace": self.namespace},
                    "spec": spec,
                }
                self.api.create_namespaced_custom_object(
                    self.group, self.version, self.namespace, GROUP_PLURAL, body
                )
        except ApiException as e:
            logger.error(f"Failed to save group {name}: {e}")
            raise ConfigWriteError(
                f"could not save group {name!r}: "
                f"{crd_missing_hint(GROUP_PLURAL, e)}") from e
        self._load_groups()
        return True

    def delete_group(self, group_name: str) -> bool:
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, self.namespace, GROUP_PLURAL, group_name
            )
        except ApiException as e:
            logger.error(f"Failed to delete group {group_name}: {e}")
            raise ConfigWriteError(
                f"could not delete group {group_name!r}: "
                f"{crd_missing_hint(GROUP_PLURAL, e)}") from e
        self._load_groups()
        return True

    def get_all_templates(self) -> List[Dict[str, Any]]:
        """List all Templates (ssh + desktop) across the system namespace."""
        templates = []
        try:
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, self.namespace, TEMPLATE_PLURAL
            )
            for item in resp.get("items", []):
                t = dict(item.get("spec", {}))
                slug = item["metadata"]["name"]
                t["fullName"] = slug
                t["name"] = slug
                t["displayName"] = t.get("displayName") or slug
                t["namespace"] = item["metadata"]["namespace"]
                templates.append(t)
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to list all templates: {e}")
        return templates

    def get_all_instances(self) -> List[Dict[str, Any]]:
        """List all ssh Sessions across all user namespaces."""
        instances = []
        core_api = client.CoreV1Api()
        try:
            nss = core_api.list_namespace(label_selector="whistler.martinmalmsten.net/managed=true")
            user_namespaces = [ns.metadata.name for ns in nss.items]
        except ApiException:
            user_namespaces = []

        for ns in user_namespaces:
            username = ns.removeprefix("whistler-user-")
            try:
                resp = self.api.list_namespaced_custom_object(
                    self.group, self.version, ns, SESSION_PLURAL,
                    label_selector="whistler.martinmalmsten.net/mode=ssh",
                )
                try:
                    pods = core_api.list_namespaced_pod(ns, label_selector=f"user={username}")
                    pod_map = {p.metadata.labels.get("instance"): p for p in pods.items}
                except ApiException:
                    pod_map = {}

                for item in resp.get("items", []):
                    spec = item.get("spec", {})
                    status_obj = item.get("status", {}) or {}
                    full_name = item["metadata"]["name"]
                    display_name = full_name.removeprefix(f"{username}-")
                    pod = pod_map.get(full_name)
                    pod_status = "Stopped"
                    pod_name = None
                    if status_obj.get("runtime") == "vm":
                        # ssh-mode VMs (images/devbase) have no whistler-labelled
                        # pod; report status.phase as the operator wrote it.
                        # Deliberately no per-namespace VMI list here — this is
                        # the cluster-wide admin view, the same tradeoff
                        # list_all_desktop_sessions documents.
                        pod_status = status_obj.get("phase", "Unknown")
                        pod_name = status_obj.get("vmiName") or status_obj.get("podName")
                    elif pod:
                        pod_name = pod.metadata.name
                        pod_status = pod.status.phase or "Unknown"
                        if pod.metadata.deletion_timestamp:
                            pod_status = "Terminating"
                    instances.append({
                        "username": username,
                        "name": display_name,
                        "fullName": full_name,
                        "template": spec.get("templateRef"),
                        "status": pod_status,
                        "podName": pod_name,
                        "namespace": ns,
                        "preemptible": spec.get("preemptible", False),
                    })
            except ApiException as e:
                if e.status != 404:
                    logger.error(f"Failed to list instances in {ns}: {e}")
        return instances

    def get_cluster_resources(self) -> Dict[str, Any]:
        """Cluster-wide capacity snapshot for the Dashboard tab: total CPU/RAM
        and per-GPU-type counts, each split into free / used by whistler /
        used by whistler (preemptible) / used by other workloads.

        Accounting is by resource *requests* (the scheduler's own ledger),
        not live utilization — the only option for GPUs, which have no
        fractional usage metric, so CPU/RAM use the same method for one
        consistent story.

        Gathers raw node/pod data from the API, then hands the arithmetic to
        the pure ``_summarize_cluster_resources`` (see there for the payload
        shapes).
        """
        core_api = client.CoreV1Api()

        try:
            node_items = core_api.list_node().items
        except ApiException as e:
            logger.error(f"Failed to list nodes: {e}")
            node_items = []

        # GPU_NODE_LABEL is the node label the gpuType nodeSelector templates
        # and overrides request GPUs by (see save_system_template / gpuType
        # overrides) — used below both for node GPU totals and, as a
        # fallback, to type a scheduled pod's GPU request.
        #
        # A node advertises a GPU either pod-mode (GPU_POD_RESOURCE) or,
        # under VM passthrough, as a KubeVirt-permitted vfio resource (e.g.
        # "nvidia.com/AD102_GEFORCE_RTX_4090") — never both at once for the
        # same device, so summing the two is safe. vm_resource_type maps a
        # vfio resource back to the product of the node advertising it, to
        # type virt-launcher pods that carry no GPU nodeSelector.
        vm_resources = self._vm_gpu_resource_names()
        node_gpu_type: Dict[str, Optional[str]] = {}
        vm_resource_type: Dict[str, Optional[str]] = {}
        nodes = []
        for node in node_items:
            allocatable = node.status.allocatable or {}
            gpu_type = (node.metadata.labels or {}).get(GPU_NODE_LABEL)
            node_gpu_type[node.metadata.name] = gpu_type
            gpu_count = int(parse_quantity(allocatable.get(GPU_POD_RESOURCE, 0)))
            for rname in vm_resources:
                vm_count = int(parse_quantity(allocatable.get(rname, 0)))
                if vm_count:
                    gpu_count += vm_count
                    vm_resource_type.setdefault(rname, gpu_type)
            nodes.append({
                "cpu": allocatable.get("cpu", "0"),
                "memory": allocatable.get("memory", "0"),
                "gpuType": gpu_type,
                "gpuCount": gpu_count,
            })

        try:
            managed_namespaces = {
                ns.metadata.name for ns in core_api.list_namespace(
                    label_selector="whistler.martinmalmsten.net/managed=true").items
            }
        except ApiException as e:
            logger.error(f"Failed to list managed namespaces: {e}")
            managed_namespaces = set()

        # Whether a Session is preemptible only lives on the Session CR: the
        # VM backend (unlike the plain-pod one) never carries it onto the
        # actual pod/VMI (see _build_vm_spec), so the running workload can't
        # be asked directly.
        preemptible_sessions = set()
        try:
            sessions = self.api.list_cluster_custom_object(self.group, self.version, SESSION_PLURAL)
            for item in sessions.get("items", []):
                if item.get("spec", {}).get("preemptible"):
                    preemptible_sessions.add(item["metadata"]["name"])
        except ApiException as e:
            logger.error(f"Failed to list sessions: {e}")

        try:
            pod_items = core_api.list_pod_for_all_namespaces().items
        except ApiException as e:
            logger.error(f"Failed to list pods: {e}")
            pod_items = []

        pod_requests = []
        for pod in pod_items:
            if pod.status.phase in ("Succeeded", "Failed") or pod.metadata.deletion_timestamp:
                continue
            labels = pod.metadata.labels or {}
            if pod.metadata.namespace in managed_namespaces:
                session_name = labels.get("session") or labels.get("instance")
                bucket = "whistlerPreemptible" if session_name in preemptible_sessions else "whistler"
            else:
                bucket = "other"

            cpu = Decimal(0)
            memory = Decimal(0)
            gpu_count = 0
            requested_vm_type = None
            for container in (pod.spec.containers or []):
                requests = (container.resources and container.resources.requests) or {}
                if "cpu" in requests:
                    cpu += parse_quantity(requests["cpu"])
                if "memory" in requests:
                    memory += parse_quantity(requests["memory"])
                if GPU_POD_RESOURCE in requests:
                    gpu_count += int(parse_quantity(requests[GPU_POD_RESOURCE]))
                for rname in vm_resources:
                    if rname in requests:
                        gpu_count += int(parse_quantity(requests[rname]))
                        requested_vm_type = requested_vm_type \
                            or vm_resource_type.get(rname)

            gpu_type = (pod.spec.node_selector or {}).get(GPU_NODE_LABEL) \
                or node_gpu_type.get(pod.spec.node_name) \
                or requested_vm_type

            pod_requests.append({
                "bucket": bucket, "cpu": cpu, "memory": memory,
                "gpuType": gpu_type, "gpuCount": gpu_count,
            })

        return self._summarize_cluster_resources(nodes, pod_requests)

    def _summarize_cluster_resources(self, nodes: List[Dict[str, Any]],
                                     pod_requests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Pure aggregation behind get_cluster_resources — no API calls, so
        it's testable with plain dicts.

        ``nodes``: one entry per node with raw allocatable quantities, e.g.
        ``{"cpu": "4", "memory": "16Gi", "gpuType": "A100" | None, "gpuCount": "2"}``.
        ``pod_requests``: one entry per non-terminal pod, already summed
        across its own containers, with the ownership bucket
        (``"whistler" | "whistlerPreemptible" | "other"``) and GPU type
        already resolved, e.g.
        ``{"bucket": "whistler", "cpu": Decimal("0.5"), "memory": Decimal(2**30), "gpuType": "A100", "gpuCount": 1}``.

        Returns ``{"cpu": summary, "memory": summary, "gpus": [{"type": ..., **summary}, ...]}``
        where a ``summary`` is ``{"total", "free", "whistler", "whistlerPreemptible", "other"}``.
        """
        buckets = ("whistler", "whistlerPreemptible", "other")

        cpu_total = sum((parse_quantity(n.get("cpu", 0)) for n in nodes), Decimal(0))
        mem_total = sum((parse_quantity(n.get("memory", 0)) for n in nodes), Decimal(0))
        gpu_total: Dict[str, int] = {}
        for n in nodes:
            count = int(parse_quantity(n.get("gpuCount", 0)))
            if count:
                gpu_type = n.get("gpuType") or "unknown"
                gpu_total[gpu_type] = gpu_total.get(gpu_type, 0) + count

        cpu_used = {b: Decimal(0) for b in buckets}
        mem_used = {b: Decimal(0) for b in buckets}
        gpu_used: Dict[str, Dict[str, int]] = {}
        for req in pod_requests:
            bucket = req["bucket"]
            cpu_used[bucket] += parse_quantity(req.get("cpu", 0))
            mem_used[bucket] += parse_quantity(req.get("memory", 0))
            gpu_count = int(req.get("gpuCount") or 0)
            if gpu_count:
                gpu_type = req.get("gpuType") or "unknown"
                gpu_used.setdefault(gpu_type, {b: 0 for b in buckets})[bucket] += gpu_count

        def _summary(total, used):
            free = total - sum(used.values())
            return {"total": total, "free": free if free > 0 else type(total)(0), **used}

        gpus = [
            {"type": gpu_type, **_summary(gpu_total.get(gpu_type, 0),
                                          gpu_used.get(gpu_type, {b: 0 for b in buckets}))}
            for gpu_type in sorted(set(gpu_total) | set(gpu_used))
        ]

        return {
            "cpu": _summary(cpu_total, cpu_used),
            "memory": _summary(mem_total, mem_used),
            "gpus": gpus,
        }

    def save_system_template(self, template_data: Dict[str, Any]) -> bool:
        name = template_data.get("name")
        if not name:
            return False
        # Build the spec only from the fields the caller actually provided, so an
        # update merges over the existing spec rather than clobbering fields the
        # admin form doesn't carry (e.g. a desktop template's displayPort /
        # nodeSelector / volumes).
        spec = {"user": "system"}
        for key in ("mode", "runtime", "displayName", "image", "imageURL",
                    "rootDiskSize", "description",
                    "resources", "nodeSelector", "personalMountPath", "volumes",
                    "displayPort", "viewer", "streamerImage", "streamerEnv",
                    "privileged", "fuse", "instancetype", "persistence", "zone"):
            if template_data.get(key) is not None:
                spec[key] = template_data[key]
        try:
            try:
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, self.namespace, TEMPLATE_PLURAL, name
                )
                existing_spec = existing.get("spec") or {}
                # The merge is top-level, so an incoming `resources` replaces
                # the whole map. The admin form has no hugePageSize field
                # (it is a YAML-level opt-out — see whistler.hugePages), and
                # losing it on an unrelated edit would silently move a VM back
                # onto 4KiB pages, which is a boot that may miss KubeVirt's
                # SyncVMI deadline rather than a visible change.
                if "resources" in spec:
                    old_size = (existing_spec.get("resources") or {}).get("hugePageSize")
                    if old_size is not None and "hugePageSize" not in spec["resources"]:
                        spec["resources"] = {**spec["resources"],
                                             "hugePageSize": old_size}
                merged = {**existing_spec, **spec}
                body = {
                    "apiVersion": f"{self.group}/{self.version}",
                    "kind": "Template",
                    "metadata": {"name": name, "namespace": self.namespace,
                                 "resourceVersion": existing["metadata"]["resourceVersion"]},
                    "spec": merged,
                }
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, self.namespace, TEMPLATE_PLURAL, name, body
                )
            except ApiException as e:
                if e.status == 404:
                    # Create: apply sensible defaults for fields the form omitted.
                    spec.setdefault("mode", "ssh")
                    spec.setdefault("runtime", "container")
                    spec.setdefault("displayName", name)
                    spec.setdefault("personalMountPath", "/userdata")
                    body = {
                        "apiVersion": f"{self.group}/{self.version}",
                        "kind": "Template",
                        "metadata": {"name": name, "namespace": self.namespace},
                        "spec": spec,
                    }
                    self.api.create_namespaced_custom_object(
                        self.group, self.version, self.namespace, TEMPLATE_PLURAL, body
                    )
                else:
                    raise
            return True
        except ApiException as e:
            logger.error(f"Failed to save system template: {e}")
            return False

    def delete_system_template(self, template_name: str) -> bool:
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, self.namespace, TEMPLATE_PLURAL, template_name
            )
            return True
        except ApiException as e:
            logger.error(f"Failed to delete system template {template_name}: {e}")
            return False

    def save_volume(self, volume_data: Dict[str, Any]) -> bool:
        try:
            self._load_volumes()
            name = volume_data.get("name")
            if not name:
                return False
            self.volume_definitions[name] = volume_data
            self.volumes = list(self.volume_definitions.values())
            with open(VOLUMES_FILE, "w") as f:
                yaml.safe_dump(self.volumes, f, default_flow_style=False, allow_unicode=True)
            return True
        except Exception as e:
            logger.error(f"Failed to save volume: {e}")
            return False

    def delete_volume(self, volume_name: str) -> bool:
        try:
            self._load_volumes()
            if volume_name not in self.volume_definitions:
                return False
            del self.volume_definitions[volume_name]
            self.volumes = list(self.volume_definitions.values())
            with open(VOLUMES_FILE, "w") as f:
                yaml.safe_dump(self.volumes, f, default_flow_style=False, allow_unicode=True)
            return True
        except Exception as e:
            logger.error(f"Failed to delete volume: {e}")
            return False

    def _delete_session_pod(self, user_ns: str, full_name: str) -> bool:
        """Delete a session's pod, treating "already gone" as success.

        How a *pod* session stops: its state lives on the PVC, so the pod is
        disposable. Operator-side (called from ensure_session when the run
        intent says stopped), which is why nothing else needs pod-delete
        rights to stop something."""
        core_api = client.CoreV1Api()
        try:
            core_api.delete_namespaced_pod(full_name, user_ns)
            logger.info(f"Stopped pod {full_name} in {user_ns}")
            return True
        except ApiException as e:
            if e.status == 404:
                return True  # already the desired state
            logger.error(f"Failed to stop pod {full_name}: {e}")
            return False

    def stop_instance(self, username: str, instance_name: str) -> bool:
        """Ask for the workload to stop, leaving the Session CR in place.

        A declaration, not an action: it writes STOP_ANNOTATION on the Session
        CR and the operator's reconcile does the work — halting the
        VirtualMachine (runStrategy, so the VM object and its CDI root disk
        survive) or deleting the pod. Exactly mirrors how starting has always
        worked (`trigger_instance_start`), and returning True means "the
        request is recorded", not "the guest is down"; the phase timer reports
        the rest.

        It used to halt the VM and delete the pod itself, from whichever
        process called it. That made *stopping* a privilege: the SSH gateway
        needed `patch` on virtualmachines for the launcher's stop key — a
        KubeVirt write in the one process that terminates untrusted SSH. The
        operator already owns pod and VM lifecycle; this puts stopping where
        starting already was and leaves both UI surfaces with CR writes only
        (design/proxyjump.md, "Stopping through the operator").
        """
        user_ns = self._get_user_namespace(username)
        full_name = f"{username}-{instance_name}"
        patch = {"metadata": {"annotations": {
            STOP_ANNOTATION: str(time.time())}}}
        try:
            self.api.patch_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL,
                full_name, patch)
            logger.info(f"Requested stop of {full_name} in {user_ns}")
            return True
        except ApiException as e:
            if e.status == 404:
                # No CR, no workload: the caller's goal is already met.
                logger.info(f"Stop requested for absent session {full_name}")
                return True
            logger.error(f"Failed to request stop of {full_name}: {e}")
            return False

    def trigger_instance_start(self, username: str, instance_name: str,
                               run_overrides: Optional[Dict[str, Any]] = None) -> bool:
        """Bump the start annotation to fire the operator's reconcile, and set
        (or clear) the overrides this run is to use.

        The annotation is an epoch float, like the stop annotation and like the
        gateway's own bump: the two marks are compared against each other
        (run_intent), so writing them in the same units keeps that comparison
        obvious. Older CRs hold an ISO string here and still parse.

        The two writes are one act on purpose — a run's overrides must never
        outlive the start that chose them, and must never be visible without
        one. A plain start (the launcher, the jump, the play button with no
        dialog) sends an explicit null, which is how a merge patch removes a
        key, so it always runs the instance as configured no matter what the
        previous run chose. A start that *carries* overrides has to
        read-modify-replace instead: merge-patching a map cannot delete
        entries, so a volume mount from the previous run would survive into a
        run that did not ask for it."""
        user_ns = self._get_user_namespace(username)
        full_name = f"{username}-{instance_name}"
        started = str(time.time())
        try:
            if run_overrides is None:
                self.api.patch_namespaced_custom_object(
                    self.group, self.version, user_ns, SESSION_PLURAL, full_name,
                    {"metadata": {"annotations": {START_ANNOTATION: started}},
                     "spec": {"runOverrides": None}},
                )
            else:
                cr = self.api.get_namespaced_custom_object(
                    self.group, self.version, user_ns, SESSION_PLURAL, full_name
                )
                cr.setdefault("spec", {})["runOverrides"] = run_overrides
                cr.setdefault("metadata", {}).setdefault(
                    "annotations", {})[START_ANNOTATION] = started
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, user_ns, SESSION_PLURAL, full_name, cr
                )
            logger.info(f"Triggered reconcile for {full_name}")
            return True
        except ApiException as e:
            logger.error(f"Failed to trigger start for {full_name}: {e}")
            return False
