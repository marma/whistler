
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
from kubernetes import client, config as k8s_config
from kubernetes.client import CoreV1Api, NetworkingV1Api
from kubernetes.client.rest import ApiException
from sys import stderr

logger = logging.getLogger(__name__)

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
    def get_selectors(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_volumes(self) -> List[Dict[str, Any]]:
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
                logger.warning("Could not load kubernetes config")

        self.api = client.CustomObjectsApi()
        self.group = "whistler.io"
        self.version = "v1"
        self.api = client.CustomObjectsApi()
        self.group = "whistler.io"
        self.version = "v1"
        
        # Determine namespace
        import os
        self.namespace = os.environ.get("POD_NAMESPACE")
        if not self.namespace:
            try:
                with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
                    self.namespace = f.read().strip()
            except FileNotFoundError:
                self.namespace = "whistler" # Default fallback

        self.users = {}
        self._load_users()

        self.selectors = []
        self._load_selectors()

        self.volumes = []
        self._load_volumes()

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
                            "whistler.io/user": username,
                            "whistler.io/managed": "true"
                        }
                    }
                }
                core_api.create_namespace(ns_body)
            else:
                raise

        # Ensure NetworkPolicy
        policy_name = "isolate-user-pods"
        try:
            net_api.read_namespaced_network_policy(policy_name, ns_name)
        except ApiException as e:
            if e.status == 404:
                logger.info(f"Creating NetworkPolicy {policy_name} in {ns_name}")
                policy_body = {
                    "apiVersion": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "metadata": {
                        "name": policy_name,
                        "namespace": ns_name
                    },
                    "spec": {
                        "podSelector": {},
                        "policyTypes": ["Ingress"],
                        "ingress": [] # Deny all ingress
                    }
                }
                net_api.create_namespaced_network_policy(ns_name, policy_body)
            else:
                pass # Ignore other errors or assume it exists
        
        return ns_name

    def _load_users(self):
        try:
            with open("/etc/whistler/users.yaml", "r") as f:
                import yaml
                data = yaml.safe_load(f)
                if data:
                    for u in data:
                        self.users[u["name"]] = u
        except FileNotFoundError:
            logger.warning("No users.yaml found at /etc/whistler/users.yaml")
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
                # List WhistlerTemplates
                resp = self.api.list_namespaced_custom_object(
                    self.group, self.version, ns, "whistlertemplates"
                )
                for item in resp.get("items", []):
                    t = item.get("spec", {})
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
                        t["source"] = "system"
                        templates.append(t)
                    elif owner == username:
                        # Strip prefix if present
                        display_name = full_name
                        if full_name.startswith(f"{username}-"):
                            display_name = full_name[len(username)+1:]
                        t["name"] = display_name
                        t["fullName"] = full_name
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
            # List WhistlerInstances in user namespace
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, user_ns, "whistlerinstances"
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
            "kind": "WhistlerInstance",
            "metadata": {
                "name": f"{username}-{instance_name}",
                "namespace": user_ns
            },
            "spec": {
                "templateRef": template_name,
                "user": username,
                "preemptible": preemptible
            }
        }
        try:
            self.api.create_namespaced_custom_object(
                self.group, self.version, user_ns, "whistlerinstances", body
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
            "kind": "WhistlerTemplate",
            "metadata": {
                "name": full_name,
                "namespace": user_ns
            },
            "spec": {
                "user": username,
                "image": template_data.get("image"),
                "description": template_data.get("description"),
                "resources": template_data.get("resources"),
                "nodeSelector": template_data.get("nodeSelector"),
                "personalMountPath": template_data.get("personalMountPath"),
                "volumes": template_data.get("volumes")
            }
        }
        import sys
        print(f"DEBUG: Saving template body: {body}", file=sys.stderr)
        try:
            # Check if exists to update, or create
            try:
                self.api.get_namespaced_custom_object(
                    self.group, self.version, user_ns, "whistlertemplates", full_name
                )
                # Update (replace)
                # We need to preserve resourceVersion to update
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, user_ns, "whistlertemplates", full_name
                )
                body["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]
                
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, user_ns, "whistlertemplates", full_name, body
                )
            except ApiException as e:
                if e.status == 404:
                    # Create
                    self.api.create_namespaced_custom_object(
                        self.group, self.version, user_ns, "whistlertemplates", body
                    )
                else:
                    raise e
            return True
        except ApiException as e:
            logger.error(f"Failed to save template: {e}")
            return False

    def delete_instance(self, username: str, instance_name: str) -> bool:
        print(f"Deleting instance {username}-{instance_name}", file=stderr, flush=True)
        user_ns = self._get_user_namespace(username)
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, user_ns, "whistlerinstances", f"{username}-{instance_name}"
            )
            return True
        except ApiException as e:
            logger.error(f"Failed to delete instance: {e}")
            return False

    def _load_selectors(self):
        try:
            with open("/etc/whistler-config/selectors.yaml", "r") as f:
                import yaml
                data = yaml.safe_load(f)
                if data:
                    self.selectors = data
        except FileNotFoundError:
            logger.warning("No selectors.yaml found at /etc/whistler-config/selectors.yaml")
        except Exception as e:
            logger.error(f"Failed to load selectors: {e}")

    def get_selectors(self) -> Dict[str, Any]:
        return self.selectors

    def _load_volumes(self):
        try:
            with open("/etc/whistler-config/volumes.yaml", "r") as f:
                import yaml
                data = yaml.safe_load(f)
                if data:
                    self.volumes = data
        except FileNotFoundError:
            logger.warning("No volumes.yaml found at /etc/whistler-config/volumes.yaml")
        except Exception as e:
            logger.error(f"Failed to load volumes: {e}")

    def get_volumes(self) -> List[Dict[str, Any]]:
        return self.volumes
