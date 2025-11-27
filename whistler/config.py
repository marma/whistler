import yaml
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class ConfigManager:
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
