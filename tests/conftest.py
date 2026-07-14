"""Shared test fixtures.

`FactConfigManager` lives here (rather than a sibling module) so pytest's
auto-loaded conftest makes it and its fixtures available to every test without
import-path juggling.
"""
from typing import Any, Dict, List, Optional

import pytest

from whistler.config import ConfigManager, OVERRIDE_GROUPS


class FakeConfigManager(ConfigManager):
    """In-memory ConfigManager for unit tests — no Kubernetes API.

    users: {username: {"name", "publicKeys": [...], "securityContext": {...}}}
    templates: {username: [{"name", "fullName", ...}]}
    instances: {username: [{"name", "status", "podName", ...}]}
    """

    def __init__(self, users=None, templates=None, instances=None,
                 desktop_templates=None, desktop_sessions=None, gpu_types=None,
                 zones=None):
        self.users: Dict[str, Dict[str, Any]] = users or {}
        self._templates: Dict[str, List[Dict[str, Any]]] = templates or {}
        self._instances: Dict[str, List[Dict[str, Any]]] = instances or {}
        self._desktop_templates: Dict[str, List[Dict[str, Any]]] = desktop_templates or {}
        self._desktop_sessions: Dict[str, List[Dict[str, Any]]] = desktop_sessions or {}
        self._gpu_types: List[str] = gpu_types or []
        self._zones: Dict[str, Dict[str, Any]] = zones or {"default": {}}

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        return self.users.get(username)

    def user_exists(self, username: str) -> bool:
        return username in self.users

    def get_user_public_keys(self, username: str) -> List[str]:
        return (self.users.get(username) or {}).get("publicKeys", [])

    def get_user_templates(self, username: str) -> List[Dict[str, Any]]:
        return list(self._templates.get(username, []))

    def get_user_instances(self, username: str) -> List[Dict[str, Any]]:
        return list(self._instances.get(username, []))

    def add_instance(self, username, template_name, instance_name, preemptible=False,
                     overrides=None):
        self._instances.setdefault(username, []).append({
            "name": instance_name,
            "template": template_name,
            "status": "Stopped",
            "podName": None,
            "preemptible": preemptible,
            "overrides": overrides,
        })
        return True

    def save_template(self, username, template_data):
        return True

    def delete_instance(self, username, instance_name):
        return True

    def delete_template(self, username, template_name):
        return True

    def get_user_desktop_templates(self, username):
        return list(self._desktop_templates.get(username, []))

    def get_user_desktop_sessions(self, username):
        return list(self._desktop_sessions.get(username, []))

    def add_desktop_session(self, username, template_name, session_name, overrides=None):
        self._desktop_sessions.setdefault(username, []).append({
            "name": session_name,
            "template": template_name,
            "phase": "Provisioning",
            "runtime": None,
            "backend": None,
            "podName": None,
            "overrides": overrides,
        })
        return True

    def delete_desktop_session(self, username, session_name):
        return True

    def get_selectors(self):
        return {}

    def get_volumes(self):
        return []

    def get_gpu_types(self):
        return list(self._gpu_types)

    def get_zones(self):
        return sorted(self._zones.keys())

    def get_zone_definitions(self):
        return {name: dict(cfg or {}) for name, cfg in self._zones.items()}

    def save_zone(self, zone_data):
        data = dict(zone_data)
        name = (data.pop("name", "") or "").strip()
        if not name:
            return False
        self._zones[name] = data
        return True

    def delete_zone(self, zone_name):
        if zone_name == "default":
            return False
        self._zones.pop(zone_name, None)
        return True

    def get_available_images(self, category=None):
        return []

    def get_server_host_key(self, secret_name):
        return None

    def save_server_host_key(self, secret_name, key_data):
        return True

    def list_all_users(self):
        return list(self.users.values())

    def save_user(self, user_data):
        username = user_data.get("name")
        if username:
            self.users[username] = user_data
        return bool(username)

    def delete_user(self, username):
        self.users.pop(username, None)
        return True

    def is_user_admin(self, username):
        return bool((self.users.get(username) or {}).get("admin", False))

    def get_all_templates(self):
        result = []
        for templates in self._templates.values():
            result.extend(templates)
        return result

    def get_all_instances(self):
        result = []
        for username, instances in self._instances.items():
            for inst in instances:
                result.append({"username": username, **inst})
        return result

    def get_cluster_resources(self):
        zero = {"total": 0, "free": 0, "whistler": 0, "whistlerPreemptible": 0, "other": 0}
        return {"cpu": dict(zero), "memory": dict(zero), "gpus": []}

    def save_system_template(self, template_data):
        return True

    def save_volume(self, volume_data):
        return True

    def delete_volume(self, volume_name):
        return True

    def get_user_allowed_volumes(self, username):
        return (self.users.get(username) or {}).get("allowedVolumes", [])

    def set_user_allowed_volumes(self, username, volume_names):
        if username in self.users:
            self.users[username]["allowedVolumes"] = volume_names
        return True

    def get_user_allowed_gpu_types(self, username):
        return (self.users.get(username) or {}).get("allowedGpuTypes", [])

    def set_user_allowed_gpu_types(self, username, gpu_types):
        if username in self.users:
            self.users[username]["allowedGpuTypes"] = gpu_types
        return True

    def get_user_allowed_zones(self, username):
        return (self.users.get(username) or {}).get("allowedZones", [])

    def set_user_allowed_zones(self, username, zones):
        if username in self.users:
            self.users[username]["allowedZones"] = zones
        return True

    def get_user_overrides(self, username):
        overrides = (self.users.get(username) or {}).get("overrides", {}) or {}
        return {g: bool(overrides.get(g, False)) for g in OVERRIDE_GROUPS}

    def set_user_overrides(self, username, overrides):
        if username in self.users:
            self.users[username]["overrides"] = {
                g: bool(overrides.get(g, False)) for g in OVERRIDE_GROUPS
            }
        return True

    def stop_instance(self, username, instance_name):
        return True

    def trigger_instance_start(self, username, instance_name):
        return True


@pytest.fixture
def make_config():
    """Factory so each test can build a FakeConfigManager with its own state."""
    def _make(**kwargs):
        return FakeConfigManager(**kwargs)
    return _make
