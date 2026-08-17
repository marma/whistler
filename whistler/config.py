
import logging
from decimal import Decimal
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from abc import ABC, abstractmethod
from kubernetes import client, config as k8s_config
from kubernetes.client import CoreV1Api, NetworkingV1Api
from kubernetes.client.rest import ApiException
from kubernetes.utils import parse_quantity
import base64
import copy
import hashlib
import ipaddress
import json
import os
import yaml

from whistler.cloudinit import (HOME_DISK_SERIAL, build_user_data,
                                resolve_uid, resolve_gid)
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

# Config file locations. Defaults match the in-cluster mount paths used by the
# Helm chart; override via env so the server/operator can run as host processes
# (e.g. local k3d integration testing) without writing to /etc.
CONFIG_DIR = os.environ.get("WHISTLER_CONFIG_DIR", "/etc/whistler-config")
SELECTORS_FILE = os.path.join(CONFIG_DIR, "selectors.yaml")
VOLUMES_FILE = os.path.join(CONFIG_DIR, "volumes.yaml")
NETWORKPOLICY_FILE = os.path.join(CONFIG_DIR, "networkpolicy.yaml")
ZONES_FILE = os.path.join(CONFIG_DIR, "zones.yaml")
IMAGES_FILE = os.path.join(CONFIG_DIR, "images.yaml")
GPU_TYPES_FILE = os.path.join(CONFIG_DIR, "gpuTypes.yaml")
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

# Node label a template/override's gpuType is matched against, both as the
# nodeSelector key that schedules onto a GPU of that type and as the node
# label the Dashboard reads to type a node's GPUs. This is the NVIDIA GPU
# Operator's node-feature-discovery label (auto-applied to GPU nodes, e.g.
# "NVIDIA-A100-SXM4-40GB") — not a whistler-specific label an admin has to
# set by hand, unlike the "accelerator" shorthand this used to be.
GPU_NODE_LABEL = "nvidia.com/gpu.product"

# Zones: admin-defined network postures (whistler.zones) a session runs under.
# Each zone renders to one NetworkPolicy per user namespace selecting pods by
# this label; the label is stamped at pod/VM build time, so a session changes
# zone on reboot, never live. ZONE_HASH_ANNOTATION records a digest of the zone
# config the pod was built under — "what rules was this actually running with"
# stays answerable after the zone definition changes.
ZONE_LABEL = "whistler.martinmalmsten.net/zone"
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

# How a zone's legacy `ssh` posture reads as a channel set, so zones written
# before Zone.spec.channels keep their exact meaning.
_POSTURE_CHANNELS = {
    SSH_POSTURE_DIRECT: (CHANNEL_SSH, CHANNEL_RELAY),
    SSH_POSTURE_RELAY: (CHANNEL_RELAY,),
    SSH_POSTURE_NONE: (),
}


def merge_allow_lists(*sources) -> List[str]:
    """The effective allow-list from a user's own field and their groups'.

    One rule for volumes, zones and gpuTypes: **empty everywhere means no
    restriction; otherwise the union bounds you.** So an ungrouped user is
    unaffected, a user with no list of their own is bounded by their group's,
    and a user with a list of their own keeps it *and* gains the group's —
    grants add up, which is what a grant means (design/security.md, "The
    border has four axes", axis 3).

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
                     overrides: Optional[Dict[str, Any]] = None) -> bool:
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
                        overrides: Optional[Dict[str, Any]] = None) -> bool:
        """Replace a Session CR's editable spec fields (preemptible, overrides).
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
        group's. Empty means unrestricted."""
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
    def trigger_instance_start(self, username: str, instance_name: str) -> bool:
        """Bump an annotation on the Session CR to fire the operator's reconcile."""
        pass

class KubeConfigManager(ConfigManager):
    # Class-level so an instance built with __new__ (the unit tests do this to
    # exercise the pure builders without a cluster — see _load_users) still has
    # a catalog to read. Never mutated in place: _load_groups rebinds it.
    groups: Dict[str, Dict[str, Any]] = {}
    # One warning per process for a missing Group CRD, not one per policy
    # evaluation — the catalog is re-read on every grant lookup.
    _warned_no_group_crd: bool = False

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

        # Named network zones (whistler.zones); "default" always exists. The
        # legacy networkpolicy.yaml egress config seeds the default zone when
        # zones.yaml doesn't define one.
        self.zones = {DEFAULT_ZONE: {}}
        self._load_zones()

        # Image allow-lists (by template category) + security policy. Enforced
        # operator-side at pod/VM build time (see _apply_policy).
        self.images = {"ssh": [], "desktop": [], "vm": []}
        self._load_images()

        # Catalog of selectable GPU types (see gpuTypes.yaml / whistler.gpuTypes).
        self.gpu_types = []
        self._load_gpu_types()
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
        # KubeVirt device-plugin resource name a VM's domain.devices.gpus[]
        # requests for passthrough. Product-specific — a kubevirt-flavored
        # sandbox device plugin (e.g. the NVIDIA GPU Operator's) derives it
        # from the GPU's PCI codename (e.g. "nvidia.com/AD102_GEFORCE_RTX_4090"),
        # not the generic pod-mode "nvidia.com/gpu" resource. Must match both
        # the cluster's device plugin and the KubeVirt CR's
        # permittedHostDevices.resourceName for the same PCI device.
        self.gpu_vm_resource_name = os.environ.get(
            "WHISTLER_GPU_VM_RESOURCE_NAME", "nvidia.com/gpu")
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
                            "whistler.martinmalmsten.net/user": username,
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
                     ephemeral: bool = False) -> bool:
        user_ns = self._ensure_user_namespace(username)

        spec = {
            "templateRef": template_name,
            "user": username,
            "preemptible": preemptible,
        }
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
            "overrides": spec.get("overrides") or {},
        }

    def update_instance(self, username: str, instance_name: str,
                        preemptible: bool = False,
                        overrides: Optional[Dict[str, Any]] = None) -> bool:
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

    def _load_gpu_types(self):
        try:
            with open(GPU_TYPES_FILE, "r") as f:
                data = yaml.safe_load(f)
                if data:
                    self.gpu_types = data
        except FileNotFoundError:
            logger.warning(f"No gpuTypes.yaml found at {GPU_TYPES_FILE}")
        except Exception as e:
            logger.error(f"Failed to load GPU types: {e}")

    def get_gpu_types(self) -> List[str]:
        return self.gpu_types

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
            gpuTypes catalog by the template editor) is checked against the
            owning user's allowedGpuTypes, when username is given and the user
            has a non-empty allow-list configured. An empty/absent allow-list
            means "no restriction".
          - Requested volumes (template_spec.volumes keys) are checked against
            the owning user's allowedVolumes the same way. Applies regardless
            of whether the volume came from the template or a session
            override (see _apply_overrides) — the merge happens before this
            runs.
          - An explicit zone (template-baked or overridden) must exist in the
            zone catalog — an unknown zone fails closed rather than falling
            back to default, whose posture may be laxer — and is checked
            against the owning user's allowedZones like gpuType/volumes. An
            absent zone (implicit default) skips both checks so a
            misconfigured allow-list can't brick plain templates."""
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
            if allowed_gpu_types and gpu_type not in allowed_gpu_types:
                raise PolicyError(
                    f"GPU type {gpu_type!r} is not allowed for user {username!r} "
                    f"(allowedGpuTypes: {allowed_gpu_types})"
                )

        requested_volumes = template_spec.get("volumes") or {}
        if requested_volumes and username:
            allowed_volumes = self.get_user_allowed_volumes(username)
            if allowed_volumes:
                disallowed = [v for v in requested_volumes if v not in allowed_volumes]
                if disallowed:
                    raise PolicyError(
                        f"volumes {disallowed} are not allowed for user {username!r} "
                        f"(allowedVolumes: {allowed_volumes})"
                    )

        zone = template_spec.get("zone")
        if zone:
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
                if allowed_zones and zone not in allowed_zones:
                    raise PolicyError(
                        f"zone {zone!r} is not allowed for user {username!r} "
                        f"(allowedZones: {allowed_zones})"
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

    def _ensure_home_disk_pvc(self, session_name, namespace, uid, size=None,
                              logger=None):
        """Ensure the per-INSTANCE home-disk PVC exists, returning its name.

        A VM cannot mount a PVC, so its home is a `disk.img` on this claim
        attached as a virtio-blk disk and formatted ext4 by the guest
        (see design/storage.md, and cloudinit.build_user_data for the guest
        side). `volumeMode: Filesystem` is deliberate and is what puts a
        `disk.img` on the share — it is also the only mode `csi-driver-nfs`
        can serve.

        Per instance, not per user: a home that followed a *user* would carry
        data between zones, because zone membership changes on reboot and the
        disk would follow. Owner-referenced to the Session so Kubernetes GC
        reaps it with the instance — which is also why ephemeral sessions
        simply don't call this (their home stays on the root disk).
        """
        pvc_name = f"whistler-home-{session_name}"
        api = client.CoreV1Api()
        try:
            api.read_namespaced_persistent_volume_claim(pvc_name, namespace)
            return pvc_name
        except ApiException as e:
            if e.status != 404:
                raise

        if logger:
            logger.info(f"Creating home disk PVC {pvc_name}")
        pvc_body = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": pvc_name,
                "labels": {"app": "whistler", "session": session_name},
                "ownerReferences": [
                    self._session_owner_reference(session_name, uid)],
            },
            "spec": {
                # RWO: exactly one VM attaches this disk. Sharing a block
                # device between writers corrupts it, and nothing here would
                # notice — see design/storage.md on why shared homes need a
                # file-level share instead.
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem",
                "resources": {"requests": {
                    "storage": size or self.home_disk_size}},
            },
        }
        try:
            api.create_namespaced_persistent_volume_claim(namespace, pvc_body)
            return pvc_name
        except ApiException as e:
            if e.status == 409:
                return pvc_name
            if logger:
                logger.error(f"Failed to create home disk PVC: {e}")
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
                       preemptible, home_pvc=None,
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
            gpu_resource_name = getattr(self, "gpu_vm_resource_name", "nvidia.com/gpu")
            devices["gpus"] = [{"name": "gpu0", "deviceName": gpu_resource_name}]

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
        if instancetype:
            vm_spec["instancetype"] = {"name": instancetype}
        else:
            if 'cpu' in resources:
                domain["cpu"] = {"cores": int(resources['cpu'])}
            if 'memory' in resources:
                domain["resources"] = {"requests": {"memory": resources['memory']}}

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

        # Merge any session-level spec.overrides into the template/user
        # details used to build the workload, gated by the owning user's
        # granted override groups (raises PolicyError if ungranted).
        user_details = self.get_user(username)
        template_spec, user_details = self._apply_overrides(
            template_spec, user_details, spec.get('overrides'), username)

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

        # A connect (portal connect/term/vnc, or SSH) bumps this annotation
        # before/while reconcile runs; its presence means "the user wants in
        # now", so boot immediately even if the create and the trigger
        # coalesced into one event. Absent a connect, the workload starts
        # life Stopped — pods included, so a freshly-created session doesn't
        # start running before anyone has asked to use it.
        wants_start = 'whistler/last-connect' in (
            cr['metadata'].get('annotations') or {})

        if effective_runtime == 'vm':
            # The home is a per-INSTANCE disk, not a share (design/storage.md).
            # Ephemeral sessions get none: their data is discarded anyway, so
            # /home stays on the root disk and no PVC is provisioned or
            # reaped for a throwaway session.
            #
            # Ensured BEFORE the VM: a guest that boots with no disk to mount
            # comes up with a root-owned empty home, so a failure here is
            # transient (the operator retries), not a degraded boot.
            home_pvc = None
            if persistence != 'ephemeral':
                try:
                    home_pvc = self._ensure_home_disk_pvc(
                        full_name, user_ns, uid,
                        size=template_spec.get('homeDiskSize'),
                        logger=logger)
                except Exception:
                    return result
            ok = self._create_vm(
                user_ns, full_name, session_name, username, uid,
                template_spec, display_port,
                template_spec.get('instancetype'), preemptible,
                home_pvc=home_pvc,
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
            ok = True

        # Honest initial phase: without a connect the workload isn't
        # started, and reporting Provisioning would show a phantom
        # "Starting" badge until the phase timer corrects it. But a re-connect
        # to an already-Ready session (e.g. opening a second view onto a
        # running desktop/VM) hits this same path — don't regress an already
        # up workload back to Provisioning, or the phase timer's next probe
        # (up to 10s away) briefly makes the session look down, and anything
        # gating on phase=="Ready" right after the connect (the web terminal's
        # readiness check) can spuriously fail.
        current_phase = (cr.get('status') or {}).get('phase')
        if current_phase == "Ready":
            result["phase"] = current_phase
        else:
            result["phase"] = "Provisioning" if wants_start else "Stopped"

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
                   preemptible, home_pvc=None, start=False,
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
                # Only a connect (start=True, i.e. the last-connect annotation
                # is present) flips it to running; other reconciles (admin
                # edits etc.) must leave a stopped VM stopped.
                if start:
                    try:
                        self.api.patch_namespaced_custom_object(
                            KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns,
                            KUBEVIRT_VM_PLURAL, full_name,
                            {"spec": {"runStrategy": "Always"}},
                        )
                    except ApiException as pe:
                        logger.warning(f"Could not restart VirtualMachine {full_name}: {pe}")
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
            username = (item.get("spec", {}) or {}).get("user")
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
        zone = ((spec.get("overrides") or {}).get("zone")
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

    def ensure_bootstrap_admin(self):
        """Create-if-absent seed of the first admin User CR from
        whistler.bootstrapAdmin (values.yaml). Called once by the operator at
        startup (kopf.on.startup); never overwrites an existing User CR of the
        same name, so later edits (via the portal or kubectl) stick across
        Helm upgrades."""
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
                "spec": {"publicKeys": data.get("publicKeys") or [], "admin": True},
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
        as "nobody is in a project" and would widen every member back to
        their own (usually empty, i.e. unrestricted) allow-lists.

        A cluster without the CRD simply has no groups — the pre-Group
        behaviour, and therefore silent — but it is silent in the *permissive*
        direction, so it warns once rather than never. This is the state a
        `helm upgrade` leaves behind, because Helm does not update CRDs."""
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

    def get_user_allowed_volumes(self, username: str) -> List[str]:
        self._load_users()
        own = self.users.get(username, {}).get("allowedVolumes", [])
        return merge_allow_lists(own, *(
            list(group_volume_grants(g, username))
            for g in self.get_user_groups(username)))

    def get_user_volume_modes(self, username: str) -> Dict[str, str]:
        """``{volume: "rw"|"ro"}`` for every volume a *group* grants this user.

        Only group-granted volumes appear: a volume the user reaches through
        their own allowedVolumes (or through an unrestricted empty list) is
        read-write, which is what it has always been. Across groups the most
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
        node_gpu_type: Dict[str, Optional[str]] = {}
        nodes = []
        for node in node_items:
            allocatable = node.status.allocatable or {}
            gpu_type = (node.metadata.labels or {}).get(GPU_NODE_LABEL)
            node_gpu_type[node.metadata.name] = gpu_type
            nodes.append({
                "cpu": allocatable.get("cpu", "0"),
                "memory": allocatable.get("memory", "0"),
                "gpuType": gpu_type,
                "gpuCount": allocatable.get("nvidia.com/gpu", "0"),
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

            gpu_type = (pod.spec.node_selector or {}).get(GPU_NODE_LABEL) \
                or node_gpu_type.get(pod.spec.node_name)

            cpu = Decimal(0)
            memory = Decimal(0)
            gpu_count = 0
            for container in (pod.spec.containers or []):
                requests = (container.resources and container.resources.requests) or {}
                if "cpu" in requests:
                    cpu += parse_quantity(requests["cpu"])
                if "memory" in requests:
                    memory += parse_quantity(requests["memory"])
                if "nvidia.com/gpu" in requests:
                    gpu_count += int(parse_quantity(requests["nvidia.com/gpu"]))

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
                merged = {**(existing.get("spec") or {}), **spec}
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

    def stop_instance(self, username: str, instance_name: str) -> bool:
        """Stop the running workload but leave the Session CR in place.
        Pods are deleted (state lives on the PVC); VMs are halted via
        runStrategy so the VirtualMachine object (and a CDI root disk)
        survive for restart on the next connect."""
        user_ns = self._get_user_namespace(username)
        full_name = f"{username}-{instance_name}"

        runtime = None
        try:
            cr = self.api.get_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, full_name
            )
            runtime = (cr.get("status") or {}).get("runtime")
        except ApiException:
            pass  # fall through to the pod path

        if runtime == "vm":
            try:
                self.api.patch_namespaced_custom_object(
                    KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns,
                    KUBEVIRT_VM_PLURAL, full_name,
                    {"spec": {"runStrategy": "Halted"}},
                )
                logger.info(f"Halted VirtualMachine {full_name} in {user_ns}")
                return True
            except ApiException as e:
                if e.status == 404:
                    return True  # Already gone
                logger.error(f"Failed to halt VirtualMachine {full_name}: {e}")
                return False

        core_api = client.CoreV1Api()
        try:
            core_api.delete_namespaced_pod(full_name, user_ns)
            logger.info(f"Stopped pod {full_name} in {user_ns}")
            return True
        except ApiException as e:
            if e.status == 404:
                return True  # Already stopped
            logger.error(f"Failed to stop pod {full_name}: {e}")
            return False

    def trigger_instance_start(self, username: str, instance_name: str) -> bool:
        """Bump the whistler/last-connect annotation to fire the operator's reconcile."""
        import datetime
        user_ns = self._get_user_namespace(username)
        full_name = f"{username}-{instance_name}"
        patch = {
            "metadata": {
                "annotations": {
                    "whistler/last-connect": datetime.datetime.utcnow().isoformat()
                }
            }
        }
        try:
            self.api.patch_namespaced_custom_object(
                self.group, self.version, user_ns, SESSION_PLURAL, full_name, patch
            )
            logger.info(f"Triggered reconcile for {full_name}")
            return True
        except ApiException as e:
            logger.error(f"Failed to trigger start for {full_name}: {e}")
            return False
