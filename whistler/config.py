
import logging
from decimal import Decimal
from pathlib import Path
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
from kubernetes import client, config as k8s_config
from kubernetes.client import CoreV1Api, NetworkingV1Api
from kubernetes.client.rest import ApiException
from kubernetes.utils import parse_quantity
import ipaddress
import os
import yaml

from whistler.cloudinit import build_user_data, resolve_uid, resolve_gid

logger = logging.getLogger(__name__)

# Config file locations. Defaults match the in-cluster mount paths used by the
# Helm chart; override via env so the server/operator can run as host processes
# (e.g. local k3d integration testing) without writing to /etc.
CONFIG_DIR = os.environ.get("WHISTLER_CONFIG_DIR", "/etc/whistler-config")
SELECTORS_FILE = os.path.join(CONFIG_DIR, "selectors.yaml")
VOLUMES_FILE = os.path.join(CONFIG_DIR, "volumes.yaml")
NETWORKPOLICY_FILE = os.path.join(CONFIG_DIR, "networkpolicy.yaml")
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

# Node label a template/override's gpuType is matched against, both as the
# nodeSelector key that schedules onto a GPU of that type and as the node
# label the Dashboard reads to type a node's GPUs. This is the NVIDIA GPU
# Operator's node-feature-discovery label (auto-applied to GPU nodes, e.g.
# "NVIDIA-A100-SXM4-40GB") — not a whistler-specific label an admin has to
# set by hand, unlike the "accelerator" shorthand this used to be.
GPU_NODE_LABEL = "nvidia.com/gpu.product"

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
)


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
    def get_available_images(self, category: Optional[str] = None) -> List[str]:
        pass

    @abstractmethod
    def get_server_host_key(self, secret_name: str) -> Optional[bytes]:
        pass

    @abstractmethod
    def save_server_host_key(self, secret_name: str, key_data: bytes) -> bool:
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
    def get_user_allowed_volumes(self, username: str) -> List[str]:
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
    def get_user_overrides(self, username: str) -> Dict[str, bool]:
        pass

    @abstractmethod
    def set_user_overrides(self, username: str, overrides: Dict[str, bool]) -> bool:
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

        # Initialize containers
        self.selectors = {} 
        self._load_selectors()
        
        self.volumes = []
        self.volume_definitions = {}
        self._load_volumes()

        self.network_policy_egress = {"allowCIDRs": [], "blockCIDRs": []}
        self._load_network_policy()

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
        # Default image for the streamer sidecar every desktop pod gets;
        # a template's streamerImage overrides it.
        self.streamer_image = os.environ.get(
            "WHISTLER_STREAMER_IMAGE",
            "ghcr.io/marma/whistler-streamer-selkies2:latest",
        )
        # Per-user SMB storage gateway (VM homes): image plus values-level
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

        # Ensure NetworkPolicy
        policy_name = "isolate-user-pods"
        policy_body = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "name": policy_name,
                "namespace": ns_name
            },
            "spec": {
                "podSelector": {},
                "policyTypes": ["Ingress", "Egress"],
                "ingress": self._build_ingress_rules(),
                "egress": self._build_egress_rules()
            }
        }
        logger.debug(f"Applying NetworkPolicy {policy_name} in {ns_name}:\n{yaml.dump(policy_body, default_flow_style=False)}")
        try:
            net_api.read_namespaced_network_policy(policy_name, ns_name)
            logger.info(f"Updating NetworkPolicy {policy_name} in {ns_name}")
            net_api.replace_namespaced_network_policy(policy_name, ns_name, policy_body)
        except ApiException as e:
            if e.status == 404:
                logger.info(f"Creating NetworkPolicy {policy_name} in {ns_name}")
                net_api.create_namespaced_network_policy(ns_name, policy_body)
            else:
                raise
        
        return ns_name

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

            for item in resp.get("items", []):
                spec = item.get("spec", {})
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

                if pod:
                    pod_name = pod.metadata.name
                    pod_status = pod.status.phase
                    if pod.metadata.deletion_timestamp:
                        pod_status = "Terminating"
                    pod_ip = pod.status.pod_ip
                    statuses = pod.status.container_statuses or []
                    pod_ready = bool(statuses) and all(cs.ready for cs in statuses)

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
                     overrides: Optional[Dict[str, Any]] = None) -> bool:
        user_ns = self._ensure_user_namespace(username)

        spec = {
            "templateRef": template_name,
            "user": username,
            "preemptible": preemptible,
        }
        if overrides:
            spec["overrides"] = overrides

        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "Session",
            "metadata": {
                "name": f"{username}-{instance_name}",
                "namespace": user_ns,
                # Denormalize access mode onto the CR so listing can filter
                # ssh vs desktop sessions cheaply (without resolving templates).
                "labels": {"whistler.martinmalmsten.net/mode": "ssh"},
            },
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
            runs."""
        effective_runtime = runtime
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
        is worth surfacing loudly. gpuType/volumes values are further gated
        by allowedGpuTypes/allowedVolumes, but that happens afterwards in
        _apply_policy against the merged spec this method returns, so it
        applies uniformly whether the value came from the template or here.

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

        return effective_spec, effective_user

    def _load_network_policy(self):
        try:
            with open(NETWORKPOLICY_FILE, "r") as f:
                data = yaml.safe_load(f)
                if data and "egress" in data:
                    self.network_policy_egress = data["egress"]
        except FileNotFoundError:
            pass  # Use defaults (deny-all egress except DNS)
        except Exception as e:
            logger.error(f"Failed to load networkpolicy.yaml: {e}")

    def _build_ingress_rules(self) -> list:
        """Ingress for a user namespace: deny everything except the trusted portal
        reaching desktop pods. Without this carve-out the round-1 deny-all-ingress
        policy would block it and the desktop would never render.

        The portal (websockets viewer) reverse-proxies the browser to the in-pod
        Selkies HTTP/WebSocket server on the pod's display port. It is pinned by
        namespace + pod label; no port is pinned because the display port varies
        per template and the portal only ever dials that one port anyway."""
        broker_ns = os.environ.get("PORTAL_NAMESPACE", self.namespace)
        ns_selector = {"matchLabels": {"kubernetes.io/metadata.name": broker_ns}}
        return [{
            "from": [
                {"namespaceSelector": ns_selector,
                 "podSelector": {"matchLabels": {"app": "whistler-portal"}}},
            ]
        }]

    def _build_egress_rules(self) -> list:
        rules = []

        # DNS is always allowed so pods can resolve hostnames
        rules.append({
            "ports": [
                {"port": 53, "protocol": "UDP"},
                {"port": 53, "protocol": "TCP"}
            ]
        })

        # Session pods (including VM virt-launcher pods, whose guest traffic
        # is masqueraded through them) may reach the user's own storage
        # gateway on SMB. Same-namespace podSelector; the gateway's dedicated
        # policy (_build_gateway_network_policy) narrows the ingress side.
        rules.append({
            "to": [{"podSelector": {
                "matchLabels": {"app": "whistler-storage-gateway"}}}],
            "ports": [{"port": 445, "protocol": "TCP"}],
        })

        # Whitelist: explicit allow rules per destination CIDR
        for entry in self.network_policy_egress.get("allowCIDRs", []) or []:
            rule = {"to": [{"ipBlock": {"cidr": entry["cidr"]}}]}
            if "ports" in entry:
                rule["ports"] = entry["ports"]
            rules.append(rule)

        # Blacklist: compute the complement CIDRs explicitly rather than using
        # ipBlock.except, which is silently ignored by several CNI plugins.
        block_cidrs = self.network_policy_egress.get("blockCIDRs", []) or []
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

    def _load_volume_definitions_from_file(self):
        try:
            with open(VOLUMES_FILE, "r") as f:
                data = yaml.safe_load(f)
                return {v['name']: v for v in data} if data else {}
        except Exception:
            return {}

    def _build_volume_wiring(self, *, pvc_name, personal_mount_path,
                             requested_volumes, available_volumes):
        """Build (pod_volumes, volume_mounts) for the home PVC plus any requested
        named volumes. Pure; the single source shared by every pod backend
        (ssh / desktop, container / kata)."""
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
                        preemptible, display_port=None):
        """Build the Pod manifest for a session from already-resolved inputs.

        Pure function of its arguments (no Kubernetes API calls) so it is
        unit-tested without a cluster; ``ensure_session`` does the API work.
        Handles both access modes (ssh / desktop) and the container/kata runtimes
        (runtime=vm is built by ``_build_vm_spec`` instead):
          - ssh overrides the entrypoint with ``sleep`` (the image must stay
            alive for the exec bridge); desktop does not (the workload image's
            entrypoint starts the DE/app session).
          - desktop pods get a native streamer sidecar (Xvfb + PulseAudio +
            Selkies, see desktops/streamer-selkies2) sharing the X/Pulse
            sockets with the workload container over emptyDirs; the workload
            image needs no display server at all and its entire display
            contract is the injected DISPLAY/PULSE_SERVER env.
          - both ``instance`` and ``session`` labels are emitted so the SSH
            server's pod-watch and the desktop Service selector both resolve.
          - runtime=kata pins the configured Kata RuntimeClass.
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
        )

        container = {
            "name": "main",
            "image": image,
            "resources": resource_reqs,
            "volumeMounts": volume_mounts,
        }
        streamer_sidecar = None
        if mode == "ssh":
            # SSH images are bridged via `kubectl exec`; keep the pod alive.
            container["command"] = ["sleep", "3600"]
        elif display_port is not None:
            # Every desktop pod gets the streamer sidecar: the workload image
            # knows nothing about displays — a native sidecar (initContainer
            # with restartPolicy=Always) owns Xvfb + PulseAudio + Selkies and
            # shares the X/Pulse sockets over emptyDirs. The startupProbe on
            # the Selkies port gates the workload container: its entrypoint (a
            # DE session or app) can assume DISPLAY is live without a wait
            # loop. The display port therefore lives on the sidecar, not
            # "main".
            # Pod containers share the IPC namespace by default, which is what
            # lets the workload's X clients use MIT-SHM against the sidecar's
            # Xvfb (compose needs explicit ipc: wiring for the same effect).
            # streamerEnv carries workload-dependent streaming knobs the sidecar
            # cannot infer (e.g. SELKIES_H264_STREAMING_MODE for GL compositors).
            sidecar_env = [{"name": "SELKIES_PORT", "value": str(display_port)}]
            for k, v in (template_spec.get('streamerEnv') or {}).items():
                sidecar_env.append({"name": str(k), "value": str(v)})
            streamer_sidecar = {
                "name": "streamer",
                "image": template_spec.get('streamerImage') or self.streamer_image,
                "restartPolicy": "Always",
                "env": sidecar_env,
                "ports": [{"containerPort": display_port, "name": "display"}],
                "volumeMounts": [
                    {"name": "x11", "mountPath": "/tmp/.X11-unix"},
                    {"name": "pulse", "mountPath": "/tmp/pulse"},
                ],
                "startupProbe": {
                    "tcpSocket": {"port": display_port},
                    "periodSeconds": 2,
                    "failureThreshold": 60,
                },
                # Requests only (scheduling hint); encode bursts get whatever
                # CPU the node has free.
                "resources": {"requests": {"cpu": "250m", "memory": "256Mi"}},
            }
            container["env"] = [
                {"name": "DISPLAY", "value": ":0"},
                {"name": "PULSE_SERVER", "value": "unix:/tmp/pulse/native"},
            ]
            container["volumeMounts"] = volume_mounts + [
                {"name": "x11", "mountPath": "/tmp/.X11-unix"},
                {"name": "pulse", "mountPath": "/tmp/pulse"},
            ]
        # (mode == desktop with display_port None builds a plain pod — it can't
        # be reached by the portal, but the resolver always supplies the
        # template's displayPort default so this is a defensive dead end, not a
        # supported shape.)

        app_label = "whistler-instance" if mode == "ssh" else "whistler-desktop"
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
            },
        }

        if streamer_sidecar is not None:
            pod_body["spec"]["initContainers"] = [streamer_sidecar]
            pod_body["spec"]["volumes"] = pod_volumes + [
                {"name": "x11", "emptyDir": {}},
                {"name": "pulse", "emptyDir": {}},
            ]

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
                       preemptible, smb_host, smb_password,
                       user_details=None, run_strategy="Halted",
                       portal_public_key=None, viewer=None):
        """Build a KubeVirt VirtualMachine manifest from resolved inputs.

        Pure (no API calls). The VMI launcher pod inherits the template labels
        so the per-session Service can select it.

        Boot source: `image` is a containerDisk (OCI-wrapped qcow2, ephemeral
        root), `imageURL` an HTTP qcow2/raw imported by CDI into a per-session
        root PVC via dataVolumeTemplates. The user's home PVC is NOT attached
        to the VM: it is mounted by the per-user storage gateway
        (ensure_storage_gateway) and reaches the guest as a cifs mount of
        ``//smb_host/home`` set up by cloud-init — KubeVirt's unprivileged
        virtiofsd made a directly-shared home read-only for the guest user
        (kubevirt#13028), and keeping the PVC off the VM also sidesteps RWO
        contention with the gateway. cloud-init creates the real user
        (username/uid/keys); serial console + VNC graphics rely on KubeVirt's
        autoattach defaults (both true), which the portal's terminal and noVNC
        viewer depend on.
        """
        image = template_spec.get('image')
        image_url = template_spec.get('imageURL')
        resources = template_spec.get('resources', {}) or {}
        node_selector = template_spec.get('nodeSelector', {})
        user_details = user_details or {}

        labels = {
            "app": "whistler-desktop",
            "session": session_name,
            "user": username,
        }

        devices = {
            "disks": [
                {"name": "rootdisk", "disk": {"bus": "virtio"}},
                {"name": "cloudinit", "disk": {"bus": "virtio"}},
            ],
            "interfaces": [{"name": "default", "masquerade": {}}],
        }
        if 'gpu' in resources:
            devices["gpus"] = [{"name": "gpu0", "deviceName": "nvidia.com/gpu"}]

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
        user_data = build_user_data(
            username=username,
            uid=resolve_uid(user_details),
            gid=resolve_gid(user_details),
            ssh_keys=ssh_keys,
            hostname=hostname,
            smb_host=smb_host,
            smb_password=smb_password,
            desktop=desktop_stream,
            streamer_env=template_spec.get('streamerEnv') if desktop_stream else None,
            display_port=display_port if desktop_stream else None,
        )

        if image_url:
            root_volume = {"name": "rootdisk",
                           "dataVolume": {"name": f"{session_name}-root"}}
        else:
            root_volume = {"name": "rootdisk",
                           "containerDisk": {"image": image or 'ubuntu:latest'}}

        # The userData travels via a per-session Secret (userDataSecretRef),
        # not inline: KubeVirt's admission webhook caps inline userData at
        # 2048 bytes (ours exceeds it), and this keeps the SMB password out
        # of the VM object. KubeVirt expects the document under the key
        # `userdata`. Owner-referenced to the Session like the VM itself.
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
                "metadata": {"labels": labels},
                "spec": {
                    "nodeSelector": node_selector,
                    "domain": domain,
                    "networks": [{"name": "default", "pod": {}}],
                    "volumes": [
                        root_volume,
                        {"name": "cloudinit", "cloudInitNoCloud": {
                            "secretRef": {
                                "name": cloudinit_secret["metadata"]["name"]}}},
                    ],
                },
            },
        }
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

    def _build_session_service(self, *, session_name, username, uid, display_port):
        """Build the per-session ClusterIP Service manifest (pure). It selects
        the desktop pod / VMI launcher pod by the ``session`` label and exposes
        the display port — the portal's websockets viewer reaches a desktop here."""
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
                "ports": [{
                    "name": "display",
                    "port": display_port,
                    "targetPort": display_port,
                }],
            },
        }

    def _ensure_session_service(self, *, session_name, username, uid, namespace,
                                display_port):
        """Create the per-session ClusterIP Service (idempotent)."""
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
            if e.status == 409:
                return True  # Already exists
            logger.error(f"Failed to create service {session_name}: {e}")
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
            # The home reaches the guest through the per-user SMB storage
            # gateway, not virtiofs (see _build_vm_spec). It must be ensured
            # BEFORE the VM: the SMB password is baked into the VM's
            # cloud-init userData at creation, so a missing gateway is a
            # transient failure (operator retries), not a degraded boot.
            smb_password = self.ensure_storage_gateway(
                username, user_ns, pvc_name)
            if not smb_password:
                return result
            ok = self._create_vm(
                user_ns, full_name, session_name, username, uid,
                template_spec, display_port,
                template_spec.get('instancetype'), preemptible,
                smb_host=self._gateway_host(username, user_ns),
                smb_password=smb_password,
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
        # "Starting" badge until the phase timer corrects it.
        result["phase"] = "Provisioning" if wants_start else "Stopped"

        if not ok:
            return result

        # Desktop sessions are reached through a per-session Service (the portal dials
        # it); SSH sessions are bridged via `kubectl exec` and need no Service.
        if mode == 'desktop':
            if not self._ensure_session_service(
                session_name=full_name, username=username, uid=uid,
                namespace=user_ns, display_port=display_port,
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
    # Per-user SMB storage gateway (VM homes). KubeVirt's unprivileged     #
    # virtiofsd (kubevirt#13028) made a directly-shared home read-only for #
    # the guest user, so the home PVC is instead mounted by a per-user     #
    # Samba pod (images/storage-gateway/) and exported as SMB3; the guest  #
    # cifs-mounts it from cloud-init. Server-side identity: client uids    #
    # are never trusted, `force user` lands every write on the PVC as the  #
    # user's real uid — consistent with pod sessions sharing the PVC.      #
    # ------------------------------------------------------------------ #

    def _gateway_name(self, username: str) -> str:
        return f"whistler-storage-{username}"

    def _gateway_host(self, username: str, user_ns: str) -> str:
        return f"{self._gateway_name(username)}.{user_ns}.svc.cluster.local"

    def _smb_secret_name(self, username: str) -> str:
        return f"whistler-smb-{username}"

    def _ensure_smb_secret(self, username: str, user_ns: str) -> Optional[str]:
        """Random per-user SMB password, generated once into Secret
        whistler-smb-<user> (same pattern as _ensure_vm_access_key). The
        gateway pod feeds it to smbpasswd at startup; every VM's cloud-init
        writes it into the guest's root-only credentials file. Returns the
        password, or None if the Secret could not be ensured."""
        secret_name = self._smb_secret_name(username)
        core_api = client.CoreV1Api()
        try:
            sec = core_api.read_namespaced_secret(secret_name, user_ns)
            import base64
            return base64.b64decode(sec.data["password"]).decode()
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to read SMB secret for {username}: {e}")
                return None

        import secrets as _secrets
        password = _secrets.token_urlsafe(24)
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": secret_name,
                "labels": {"app": "whistler", "user": username},
            },
            "stringData": {"password": password},
        }
        try:
            core_api.create_namespaced_secret(user_ns, body)
            logger.info(f"Created SMB secret for {username}")
            return password
        except ApiException as e:
            if e.status == 409:  # lost a race; use the winner's password
                return self._ensure_smb_secret(username, user_ns)
            logger.error(f"Failed to create SMB secret for {username}: {e}")
            return None

    def _build_gateway_manifests(self, *, username, uid, pvc_name, image,
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
                        "containers": [{
                            "name": "samba",
                            "image": image,
                            "env": [
                                {"name": "SMB_USER", "value": username},
                                {"name": "SMB_UID", "value": str(uid)},
                            ],
                            "ports": [{"containerPort": 445, "name": "smb"}],
                            "readinessProbe": {
                                "tcpSocket": {"port": 445},
                                "initialDelaySeconds": 2,
                                "periodSeconds": 5,
                            },
                            "volumeMounts": [
                                {"name": "home", "mountPath": "/shares/home"},
                                {"name": "smb-secret",
                                 "mountPath": "/etc/whistler-smb",
                                 "readOnly": True},
                            ],
                            "resources": resources or {},
                        }],
                        "volumes": [
                            {"name": "home",
                             "persistentVolumeClaim": {"claimName": pvc_name}},
                            {"name": "smb-secret",
                             "secret": {"secretName": self._smb_secret_name(username)}},
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
                "ports": [{"name": "smb", "port": 445, "targetPort": 445}],
            },
        }
        return deployment, service

    def _build_gateway_network_policy(self, username: str) -> Dict[str, Any]:
        """Fencing (pure): only this user's session pods may reach the
        gateway, and only on SMB. virt-launcher pods inherit the
        app: whistler-desktop label from the VM template metadata. Additive
        with isolate-user-pods, whose portal carve-out also selects the
        gateway pod (harmless — the portal never speaks SMB)."""
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
                    "ports": [{"port": 445, "protocol": "TCP"}],
                }],
            },
        }

    def ensure_storage_gateway(self, username: str, user_ns: str,
                               pvc_name: str) -> Optional[str]:
        """Ensure the per-user storage gateway exists (Secret + Deployment +
        Service + fencing NetworkPolicy), lazily with the first runtime:vm
        session. Idempotent; an existing Deployment is patched so values
        changes (image bumps in the dev loop) roll through. Returns the SMB
        password on success (the caller bakes it into cloud-init), None on
        failure — callers must treat that as transient and retry."""
        password = self._ensure_smb_secret(username, user_ns)
        if not password:
            return None

        deployment, service = self._build_gateway_manifests(
            username=username,
            uid=resolve_uid(self.get_user(username)),
            pvc_name=pvc_name,
            image=self.storage_gateway_image,
            node_selector=self.storage_gateway_node_selector,
            resources=self.storage_gateway_resources,
        )
        name = deployment["metadata"]["name"]

        apps_api = client.AppsV1Api()
        try:
            apps_api.create_namespaced_deployment(user_ns, deployment)
            logger.info(f"Storage gateway {name} created for {username}")
        except ApiException as e:
            if e.status != 409:
                logger.error(f"Failed to create storage gateway {name}: {e}")
                return None
            try:
                apps_api.patch_namespaced_deployment(name, user_ns, deployment)
            except ApiException as pe:
                logger.warning(f"Could not update storage gateway {name}: {pe}")

        core_api = client.CoreV1Api()
        try:
            core_api.create_namespaced_service(user_ns, service)
        except ApiException as e:
            if e.status != 409:
                logger.error(
                    f"Failed to create storage gateway service {name}: {e}")
                return None

        net_api = NetworkingV1Api()
        try:
            net_api.create_namespaced_network_policy(
                user_ns, self._build_gateway_network_policy(username))
        except ApiException as e:
            if e.status != 409:
                logger.error(
                    f"Failed to create storage gateway NetworkPolicy: {e}")
                return None

        return password

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
                   preemptible, smb_host, smb_password, start=False,
                   viewer=None, user_details=None) -> bool:
        vm_body, cloudinit_secret = self._build_vm_spec(
            session_name=full_name,
            hostname=session_name,
            username=username,
            uid=uid,
            template_spec=template_spec,
            display_port=display_port,
            smb_host=smb_host,
            smb_password=smb_password,
            instancetype=instancetype,
            preemptible=preemptible,
            user_details=user_details if user_details is not None else self.get_user(username),
            run_strategy="Always" if start else "Halted",
            portal_public_key=self._ensure_vm_access_key(username, user_ns),
            viewer=viewer,
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

    def add_desktop_session(self, username: str, template_name: str, session_name: str,
                            overrides: Optional[Dict[str, Any]] = None) -> bool:
        user_ns = self._ensure_user_namespace(username)
        spec = {
            "templateRef": template_name,
            "user": username,
        }
        if overrides:
            spec["overrides"] = overrides
        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "Session",
            "metadata": {
                "name": f"{username}-{session_name}",
                "namespace": user_ns,
                "labels": {"whistler.martinmalmsten.net/mode": "desktop"},
            },
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
    # Admin / management operations                                        #
    # ------------------------------------------------------------------ #

    def _save_user_spec(self, username: str, spec_updates: Dict[str, Any]) -> bool:
        """Get-merge-replace-or-create a single User CR, mirroring
        save_system_template: only the keys in spec_updates are touched, so
        concurrent partial updates (e.g. set_user_overrides) don't clobber the
        rest of the spec."""
        try:
            try:
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, self.namespace, USER_PLURAL, username
                )
                merged = {**(existing.get("spec") or {}), **spec_updates}
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
                        "spec": spec_updates,
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

    def get_user_allowed_volumes(self, username: str) -> List[str]:
        self._load_users()
        user = self.users.get(username, {})
        return user.get("allowedVolumes", [])

    def set_user_allowed_volumes(self, username: str, volume_names: List[str]) -> bool:
        return self._save_user_spec(username, {"allowedVolumes": volume_names})

    def get_user_allowed_gpu_types(self, username: str) -> List[str]:
        self._load_users()
        user = self.users.get(username, {})
        return user.get("allowedGpuTypes", [])

    def set_user_allowed_gpu_types(self, username: str, gpu_types: List[str]) -> bool:
        return self._save_user_spec(username, {"allowedGpuTypes": gpu_types})

    def get_user_overrides(self, username: str) -> Dict[str, bool]:
        self._load_users()
        user = self.users.get(username, {})
        overrides = user.get("overrides", {}) or {}
        return {g: bool(overrides.get(g, False)) for g in OVERRIDE_GROUPS}

    def set_user_overrides(self, username: str, overrides: Dict[str, bool]) -> bool:
        normalized = {g: bool(overrides.get(g, False)) for g in OVERRIDE_GROUPS}
        return self._save_user_spec(username, {"overrides": normalized})

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
                    full_name = item["metadata"]["name"]
                    display_name = full_name.removeprefix(f"{username}-")
                    pod = pod_map.get(full_name)
                    pod_status = "Stopped"
                    pod_name = None
                    if pod:
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
                    "privileged", "fuse", "instancetype", "persistence"):
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
