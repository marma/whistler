import yaml
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException

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
    def add_instance(self, username: str, template_name: str, instance_name: str) -> bool:
        pass

    @abstractmethod
    def save_template(self, username: str, template_data: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    def delete_instance(self, username: str, instance_name: str) -> bool:
        pass

class YamlConfigManager(ConfigManager):
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = Path(config_path)
        self.config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self) -> None:
        if not self.config_path.exists():
            logger.warning(f"Config file {self.config_path} not found. Using empty config.")
            self.config = {"users": {}}
            return

        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            self.config = {"users": {}}

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        return self.config.get("users", {}).get(username)

    def get_user_templates(self, username: str) -> List[Dict[str, Any]]:
        templates = []
        
        # System templates
        system_templates = self.config.get("templates", [])
        for t in system_templates:
            t_copy = t.copy()
            t_copy["source"] = "system"
            templates.append(t_copy)

        # User templates
        user = self.get_user(username)
        if user:
            user_templates = user.get("templates", [])
            for t in user_templates:
                t_copy = t.copy()
                t_copy["source"] = "user"
                templates.append(t_copy)
                
        return templates

    def user_exists(self, username: str) -> bool:
        return username in self.config.get("users", {})

    def save_config(self) -> None:
        try:
            with open(self.config_path, 'w') as f:
                yaml.safe_dump(self.config, f)
        except Exception as e:
            logger.error(f"Failed to save config file: {e}")

    def get_user_instances(self, username: str) -> List[Dict[str, Any]]:
        user = self.get_user(username)
        if user:
            return user.get("instances", [])
        return []

    def add_instance(self, username: str, template_name: str, instance_name: str) -> bool:
        user = self.get_user(username)
        if not user:
            return False
        
        templates = self.get_user_templates(username)
        template = next((t for t in templates if t["name"] == template_name), None)
        if not template:
            return False

        if "instances" not in user:
            user["instances"] = []
        
        # Check if instance name already exists
        if any(i["name"] == instance_name for i in user["instances"]):
            return False

        new_instance = {
            "name": instance_name,
            "template": template_name,
            "image": template["image"],
            "resources": template.get("resources", {}),
            "status": "stopped" # Default status
        }
        user["instances"].append(new_instance)
        self.save_config()
        return True

    def save_template(self, username: str, template_data: Dict[str, Any]) -> bool:
        # Prevent saving system templates (though TUI should also block this)
        if template_data.get("source") == "system":
            return False

        user = self.get_user(username)
        if not user:
            return False
        
        if "templates" not in user:
            user["templates"] = []
        
        templates = user["templates"]
        existing_index = next((i for i, t in enumerate(templates) if t["name"] == template_data["name"]), -1)
        
        if existing_index >= 0:
            templates[existing_index] = template_data
        else:
            templates.append(template_data)
            
        self.save_config()
        return True

    def delete_instance(self, username: str, instance_name: str) -> bool:
        user = self.get_user(username)
        if not user or "instances" not in user:
            return False
            
        instances = user["instances"]
        initial_len = len(instances)
        user["instances"] = [i for i in instances if i["name"] != instance_name]
        
        if len(user["instances"]) < initial_len:
            self.save_config()
            return True
            
        return False

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
        self.namespace = "default" # TODO: Make configurable

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        # In K8s mode, we assume users exist or are managed externally.
        # For now, we return a dummy user object to satisfy the interface.
        return {"name": username}

    def user_exists(self, username: str) -> bool:
        # Always return True for now, or implement a User CRD/ConfigMap check
        return True

    def get_user_templates(self, username: str) -> List[Dict[str, Any]]:
        templates = []
        try:
            # List WhistlerTemplates
            resp = self.api.list_namespaced_custom_object(
                self.group, self.version, self.namespace, "whistlertemplates"
            )
            for item in resp.get("items", []):
                t = item.get("spec", {})
                t["name"] = item["metadata"]["name"]
                t["source"] = "system" # All CRD templates are effectively system/shared for now
                templates.append(t)
        except ApiException as e:
            logger.error(f"Failed to list templates: {e}")
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
                    name = item["metadata"]["name"]
                    pod = pod_map.get(name)
                    
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
                        "name": name,
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

    def add_instance(self, username: str, template_name: str, instance_name: str) -> bool:
        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "WhistlerInstance",
            "metadata": {
                "name": instance_name,
                "namespace": self.namespace
            },
            "spec": {
                "templateRef": template_name,
                "user": username
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

        body = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "WhistlerTemplate",
            "metadata": {
                "name": name,
                "namespace": self.namespace
            },
            "spec": {
                "image": template_data.get("image"),
                "description": template_data.get("description"),
                "resources": template_data.get("resources"),
                "nodeSelector": template_data.get("nodeSelector")
            }
        }
        try:
            # Check if exists to update, or create
            try:
                self.api.get_namespaced_custom_object(
                    self.group, self.version, self.namespace, "whistlertemplates", name
                )
                # Update (replace)
                self.api.replace_namespaced_custom_object(
                    self.group, self.version, self.namespace, "whistlertemplates", name, body
                )
            except ApiException:
                # Create
                self.api.create_namespaced_custom_object(
                    self.group, self.version, self.namespace, "whistlertemplates", body
                )
            return True
        except ApiException as e:
            logger.error(f"Failed to save template: {e}")
            return False

    def delete_instance(self, username: str, instance_name: str) -> bool:
        try:
            self.api.delete_namespaced_custom_object(
                self.group, self.version, self.namespace, "whistlerinstances", instance_name
            )
            return True
        except ApiException as e:
            logger.error(f"Failed to delete instance: {e}")
            return False
