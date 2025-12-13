
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
from kubernetes import client, config as k8s_config
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

    def get_user_templates(self, username: str) -> List[Dict[str, Any]]:
        templates = []
        try:
            # List WhistlerTemplates
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, self.namespace, "whistlertemplates"
            )
            for item in resp.get("items", []):
                t = item.get("spec", {})
                full_name = item["metadata"]["name"]
                
                # Determine source and display name
                owner = t.get("user", "system")
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
            logger.error(f"Failed to list templates: {e}")
            
        # Sort templates: system first, then user
        templates.sort(key=lambda x: x.get("source", ""))
        return templates

    def get_user_instances(self, username: str) -> List[Dict[str, Any]]:
        instances = []
        try:
            # List WhistlerInstances, filter by user label/field
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, self.namespace, "whistlerinstances"
            )
            
            # List Pods for this user to get actual status
            core_api = client.CoreV1Api()
            # We assume pods have label user=<username> and instance=<instance_name>
            # This was set in operator.py
            pods = core_api.list_namespaced_pod(
                self.namespace, label_selector=f"user={username}"
            )
            pod_map = {p.metadata.labels.get("instance"): p for p in pods.items}

            for item in resp.get("items", []):
                spec = item.get("spec", {})
                # status = item.get("status", {}) # Don't rely on CR status
                
                if spec.get("user") == username:
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
                        
                    inst = {
                        "name": display_name,
                        "template": spec.get("templateRef"),
                        "status": pod_status,
                        "podName": pod_name,
                        "ip": pod_ip,
                        "sshHost": None, 
                        "sshPort": None
                    }
                    instances.append(inst)
        except ApiException as e:
            logger.error(f"Failed to list instances: {e}")
        return instances

    def add_instance(self, username: str, template_name: str, instance_name: str, preemptible: bool = False) -> bool:
        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "WhistlerInstance",
            "metadata": {
                "name": f"{username}-{instance_name}",
                "namespace": self.namespace
            },
            "spec": {
                "templateRef": template_name,
                "user": username,
                "preemptible": preemptible
            }
        }
        try:
            self.api.create_namespaced_custom_object(
                self.group, self.version, self.namespace, "whistlerinstances", body
            )
            return True
        except ApiException as e:
            logger.error(f"Failed to create instance: {e}")
            return False

    def save_template(self, username: str, template_data: Dict[str, Any]) -> bool:
        # Creating templates via TUI in K8s mode might be restricted to admins
        # For now, we'll implement it as creating a WhistlerTemplate CR
        name = template_data.get("name")
        if not name:
            return False

        # Prepend username for user templates to ensure uniqueness
        full_name = f"{username}-{name}"
        
        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "WhistlerTemplate",
            "metadata": {
                "name": full_name,
                "namespace": self.namespace
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
        try:
            # Check if exists to update, or create
            try:
                self.api.get_namespaced_custom_object(
                    self.group, self.version, self.namespace, "whistlertemplates", full_name
                )
                # Update (replace)
                # We need to preserve resourceVersion to update
                existing = self.api.get_namespaced_custom_object(
                    self.group, self.version, self.namespace, "whistlertemplates", full_name
                )
                body["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]
                
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, self.namespace, "whistlertemplates", full_name, body
                )
            except ApiException as e:
                if e.status == 404:
                    # Create
                    self.api.create_namespaced_custom_object(
                        self.group, self.version, self.namespace, "whistlertemplates", body
                    )
                else:
                    raise e
            return True
        except ApiException as e:
            logger.error(f"Failed to save template: {e}")
            return False

    def delete_instance(self, username: str, instance_name: str) -> bool:
        print(f"Deleting instance {username}-{instance_name}", file=stderr, flush=True)
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, self.namespace, "whistlerinstances", f"{username}-{instance_name}"
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
