
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
from kubernetes import client, config as k8s_config
from kubernetes.client import CoreV1Api, NetworkingV1Api
from kubernetes.client.rest import ApiException
import ipaddress
import os
import yaml

logger = logging.getLogger(__name__)

# Config file locations. Defaults match the in-cluster mount paths used by the
# Helm chart; override via env so the server/operator can run as host processes
# (e.g. local k3d integration testing) without writing to /etc.
USERS_FILE = os.environ.get("WHISTLER_USERS_FILE", "/etc/whistler/users.yaml")
CONFIG_DIR = os.environ.get("WHISTLER_CONFIG_DIR", "/etc/whistler-config")
SELECTORS_FILE = os.path.join(CONFIG_DIR, "selectors.yaml")
VOLUMES_FILE = os.path.join(CONFIG_DIR, "volumes.yaml")
NETWORKPOLICY_FILE = os.path.join(CONFIG_DIR, "networkpolicy.yaml")
IMAGES_FILE = os.path.join(CONFIG_DIR, "images.yaml")

# KubeVirt API coordinates for the VM desktop backend. KubeVirt may be absent
# from a given cluster; every call against these is guarded so the operator
# runs cleanly without the CRDs installed.
KUBEVIRT_GROUP = "kubevirt.io"
KUBEVIRT_VERSION = "v1"
KUBEVIRT_VM_PLURAL = "virtualmachines"
KUBEVIRT_VMI_PLURAL = "virtualmachineinstances"

# Custom-resource plurals for the unified Template/Session model (group
# whistler.martinmalmsten.net/v1). One Template kind covers ssh + desktop; one
# Session kind covers what used to be WhistlerInstance + DesktopSession.
TEMPLATE_PLURAL = "templates"
SESSION_PLURAL = "sessions"


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
    def add_instance(self, username: str, template_name: str, instance_name: str, preemptible: bool = False) -> bool:
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
    def add_desktop_session(self, username: str, template_name: str, session_name: str) -> bool:
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
    def get_all_templates(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_all_instances(self) -> List[Dict[str, Any]]:
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

        self.users = {} # Not really used in Kube mode but kept for compat if needed
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
        self.force_kata_for_privileged = os.environ.get(
            "WHISTLER_FORCE_KATA_FOR_PRIVILEGED", "false"
        ).strip().lower() in ("1", "true", "yes")
        self.kata_runtime_class = os.environ.get("WHISTLER_KATA_RUNTIME_CLASS", "kata")

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
        try:
            with open(USERS_FILE, "r") as f:
                import yaml
                data = yaml.safe_load(f)
                if data:
                    for u in data:
                        self.users[u["name"]] = u
        except FileNotFoundError:
            logger.warning(f"No users.yaml found at {USERS_FILE}")
        except Exception as e:
            logger.error(f"Failed to load users: {e}")

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        # In K8s mode, we assume users exist or are managed externally.
        # For now, we return a dummy user object to satisfy the interface.
        return self.users.get(username, {"name": username})

    def user_exists(self, username: str) -> bool:
        return username in self.users

    def get_user_public_keys(self, username: str) -> List[str]:
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
                
                if pod:
                    pod_name = pod.metadata.name
                    pod_status = pod.status.phase
                    if pod.metadata.deletion_timestamp:
                        pod_status = "Terminating"
                    pod_ip = pod.status.pod_ip
                    
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

    def add_instance(self, username: str, template_name: str, instance_name: str, preemptible: bool = False) -> bool:
        user_ns = self._ensure_user_namespace(username)
        
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
            "spec": {
                "templateRef": template_name,
                "user": username,
                "preemptible": preemptible
            }
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

    def _apply_policy(self, template_spec: Dict[str, Any], mode: str, runtime: str) -> str:
        """Authoritative, operator-side policy applied at build time.

        Returns the effective runtime (possibly coerced) and raises PolicyError
        when the template is not allowed:
          - privileged (or fuse) + runtime=container is coerced to kata when
            whistler.security.forceKataForPrivileged is on, so the privileged
            workload runs inside a lightweight VM rather than on the host kernel.
          - image allow-list is enforced when mode=desktop OR runtime=vm. SSH
            container/kata templates may use any image (the check is skipped)."""
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
            if image not in allowed:
                raise PolicyError(
                    f"image {image!r} is not in the allowed {category} image list "
                    f"(whistler.images.{category}); allowed: {allowed}"
                )
        return effective_runtime

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
        """Ingress for a user namespace: deny everything except the shared guacd
        pod reaching desktop pods (it dials the per-session Service to bridge the
        display). Without this carve-out the round-1 deny-all-ingress policy would
        block guacd and the desktop would never render.

        The source is restricted to the single trusted guacd Deployment by
        namespace + pod label; no port is pinned because the display port varies
        per template and guacd only ever dials that port anyway."""
        guacd_ns = os.environ.get("GUACD_NAMESPACE", self.namespace)
        return [{
            "from": [{
                "namespaceSelector": {
                    "matchLabels": {"kubernetes.io/metadata.name": guacd_ns}
                },
                "podSelector": {
                    "matchLabels": {"app": "whistler-guacd"}
                }
            }]
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
            alive for the exec bridge); desktop does not (the display server
            self-starts) and exposes the display port.
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
        if mode == "ssh":
            # SSH images are bridged via `kubectl exec`; keep the pod alive.
            container["command"] = ["sleep", "3600"]
        else:
            # Desktop images self-start their display server; expose its port.
            if display_port is not None:
                container["ports"] = [{"containerPort": display_port, "name": "display"}]

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

        if preemptible:
            pod_body["spec"]["priorityClassName"] = "whistler-preemptible"

        return pod_body


    # ------------------------------------------------------------------ #
    # Session backends: plain pod, Kata pod, or a KubeVirt VM — shared by  #
    # ssh and desktop access modes. Provisioning + lifecycle; the desktop  #
    # display tunnel (Guacamole) lives in the portal. These reuse the      #
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
                       template_spec, pvc_name, display_port, instancetype,
                       preemptible):
        """Build a KubeVirt VirtualMachine manifest from resolved inputs.

        Pure (no API calls). KubeVirt is not available in any cluster we test
        against, so this is unit-tested in isolation but treated as unverified
        end-to-end. The VMI launcher pod inherits the template labels so the
        per-session Service can select it.
        """
        image = template_spec.get('image', 'ubuntu:latest')
        resources = template_spec.get('resources', {}) or {}
        node_selector = template_spec.get('nodeSelector', {})

        labels = {
            "app": "whistler-desktop",
            "session": session_name,
            "user": username,
        }

        devices = {
            "disks": [
                {"name": "rootdisk", "disk": {"bus": "virtio"}},
                {"name": "home", "disk": {"bus": "virtio"}},
            ],
            "interfaces": [{"name": "default", "masquerade": {}}],
        }
        if 'gpu' in resources:
            devices["gpus"] = [{"name": "gpu0", "deviceName": "nvidia.com/gpu"}]

        domain = {"devices": devices}
        vm_spec = {
            "running": True,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "nodeSelector": node_selector,
                    "domain": domain,
                    "networks": [{"name": "default", "pod": {}}],
                    "volumes": [
                        {"name": "rootdisk", "containerDisk": {"image": image}},
                        {"name": "home", "persistentVolumeClaim": {"claimName": pvc_name}},
                    ],
                },
            },
        }

        # instancetype supplies cpu/memory; KubeVirt rejects setting both it and
        # domain.cpu/domain.resources, so they are mutually exclusive here.
        if instancetype:
            vm_spec["instancetype"] = {"name": instancetype}
        else:
            if 'cpu' in resources:
                domain["cpu"] = {"cores": int(resources['cpu'])}
            if 'memory' in resources:
                domain["resources"] = {"requests": {"memory": resources['memory']}}

        return {
            "apiVersion": f"{KUBEVIRT_GROUP}/{KUBEVIRT_VERSION}",
            "kind": "VirtualMachine",
            "metadata": {
                "name": session_name,
                "labels": labels,
                "ownerReferences": [self._session_owner_reference(session_name, uid)],
            },
            "spec": vm_spec,
        }

    def _build_session_service(self, *, session_name, username, uid, display_port):
        """Build the per-session ClusterIP Service manifest (pure). It selects
        the desktop pod / VMI launcher pod by the ``session`` label and exposes
        the display port — the future Guacamole tunnel reaches a desktop here."""
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
        result = {"ok": False, "mode": None, "runtime": None, "displayPort": None}

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
        display_port = template_spec.get('displayPort', 3389)
        persistence = template_spec.get('persistence', 'ephemeral')
        # SSH ephemeral sessions carry preemptible on the Session spec; desktop
        # templates express it via persistence. Honor either.
        preemptible = bool(spec.get('preemptible')) or persistence == 'preemptible'

        # Authoritative policy (may raise PolicyError, may coerce runtime->kata).
        effective_runtime = self._apply_policy(template_spec, mode, runtime)
        result["mode"] = mode
        result["runtime"] = effective_runtime
        if mode == 'desktop':
            result["displayPort"] = display_port

        try:
            pvc_name = self._ensure_pvc(username, user_ns, logger)
        except Exception:
            return result

        if effective_runtime == 'vm':
            ok = self._create_vm(
                user_ns, full_name, session_name, username, uid,
                template_spec, pvc_name, display_port,
                template_spec.get('instancetype'), preemptible,
            )
        else:
            ok = self._create_pod(
                user_ns, full_name, session_name, username, uid, mode,
                effective_runtime, template_spec, pvc_name, display_port,
                preemptible,
            )

        if not ok:
            return result

        # Desktop sessions are reached through a per-session Service (guacd dials
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
                    preemptible) -> bool:
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
            user_details=self.get_user(username),
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

    def _create_vm(self, user_ns, full_name, session_name, username, uid,
                   template_spec, pvc_name, display_port, instancetype,
                   preemptible) -> bool:
        vm_body = self._build_vm_spec(
            session_name=full_name,
            hostname=session_name,
            username=username,
            uid=uid,
            template_spec=template_spec,
            pvc_name=pvc_name,
            display_port=display_port,
            instancetype=instancetype,
            preemptible=preemptible,
        )
        logger.debug(f"Creating KubeVirt VM:\n{yaml.safe_dump(vm_body)}")
        try:
            self.api.create_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, user_ns, KUBEVIRT_VM_PLURAL, vm_body
            )
            logger.info(f"VirtualMachine {full_name} created")
            return True
        except ApiException as e:
            if e.status == 409:
                return True  # Already exists
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
            # they keep the operator-reported phase.
            core_api = client.CoreV1Api()
            try:
                pods = core_api.list_namespaced_pod(user_ns)
                pod_map = {p.metadata.name: p for p in pods.items}
            except ApiException:
                pod_map = {}

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
                })
        except ApiException as e:
            if e.status != 404:
                logger.error(f"Failed to list desktop sessions: {e}")
        return sessions

    def add_desktop_session(self, username: str, template_name: str, session_name: str) -> bool:
        user_ns = self._ensure_user_namespace(username)
        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "Session",
            "metadata": {
                "name": f"{username}-{session_name}",
                "namespace": user_ns,
                "labels": {"whistler.martinmalmsten.net/mode": "desktop"},
            },
            "spec": {
                "templateRef": template_name,
                "user": username,
            }
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

    def _write_users_file(self, users_dict: Dict[str, Any]):
        users_list = list(users_dict.values())
        with open(USERS_FILE, "w") as f:
            yaml.safe_dump(users_list, f, default_flow_style=False, allow_unicode=True)

    def list_all_users(self) -> List[Dict[str, Any]]:
        self._load_users()
        return list(self.users.values())

    def save_user(self, user_data: Dict[str, Any]) -> bool:
        try:
            self._load_users()
            username = user_data.get("name")
            if not username:
                return False
            self.users[username] = user_data
            self._write_users_file(self.users)
            return True
        except Exception as e:
            logger.error(f"Failed to save user: {e}")
            return False

    def delete_user(self, username: str) -> bool:
        try:
            self._load_users()
            if username not in self.users:
                return False
            del self.users[username]
            self._write_users_file(self.users)
            return True
        except Exception as e:
            logger.error(f"Failed to delete user: {e}")
            return False

    def get_user_allowed_volumes(self, username: str) -> List[str]:
        self._load_users()
        user = self.users.get(username, {})
        return user.get("allowedVolumes", [])

    def set_user_allowed_volumes(self, username: str, volume_names: List[str]) -> bool:
        try:
            self._load_users()
            if username not in self.users:
                self.users[username] = {"name": username}
            self.users[username]["allowedVolumes"] = volume_names
            self._write_users_file(self.users)
            return True
        except Exception as e:
            logger.error(f"Failed to set user allowed volumes: {e}")
            return False

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

    def save_system_template(self, template_data: Dict[str, Any]) -> bool:
        name = template_data.get("name")
        if not name:
            return False
        # Build the spec only from the fields the caller actually provided, so an
        # update merges over the existing spec rather than clobbering fields the
        # admin form doesn't carry (e.g. a desktop template's connectionParams /
        # rdpSecurity / nodeSelector / volumes).
        spec = {"user": "system"}
        for key in ("mode", "runtime", "displayName", "image", "description",
                    "resources", "nodeSelector", "personalMountPath", "volumes",
                    "displayPort", "protocol", "connectionParams", "rdpSecurity",
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
        """Delete the running pod but leave the Session CR in place."""
        user_ns = self._get_user_namespace(username)
        full_name = f"{username}-{instance_name}"
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
