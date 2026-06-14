"""Shared test fixtures.

`FactConfigManager` lives here (rather than a sibling module) so pytest's
auto-loaded conftest makes it and its fixtures available to every test without
import-path juggling.
"""
from typing import Any, Dict, List, Optional

import pytest

from whistler.config import ConfigManager


class FakeConfigManager(ConfigManager):
    """In-memory ConfigManager for unit tests — no Kubernetes API.

    users: {username: {"name", "publicKeys": [...], "securityContext": {...}}}
    templates: {username: [{"name", "fullName", ...}]}
    instances: {username: [{"name", "status", "podName", ...}]}
    """

    def __init__(self, users=None, templates=None, instances=None,
                 desktop_templates=None, desktop_sessions=None):
        self.users: Dict[str, Dict[str, Any]] = users or {}
        self._templates: Dict[str, List[Dict[str, Any]]] = templates or {}
        self._instances: Dict[str, List[Dict[str, Any]]] = instances or {}
        self._desktop_templates: Dict[str, List[Dict[str, Any]]] = desktop_templates or {}
        self._desktop_sessions: Dict[str, List[Dict[str, Any]]] = desktop_sessions or {}

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

    def add_instance(self, username, template_name, instance_name, preemptible=False):
        self._instances.setdefault(username, []).append({
            "name": instance_name,
            "template": template_name,
            "status": "Stopped",
            "podName": None,
            "preemptible": preemptible,
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

    def add_desktop_session(self, username, template_name, session_name):
        self._desktop_sessions.setdefault(username, []).append({
            "name": session_name,
            "template": template_name,
            "phase": "Provisioning",
            "backend": None,
            "podName": None,
        })
        return True

    def delete_desktop_session(self, username, session_name):
        return True

    def get_selectors(self):
        return {}

    def get_volumes(self):
        return []

    def get_server_host_key(self, secret_name):
        return None

    def save_server_host_key(self, secret_name, key_data):
        return True


@pytest.fixture
def make_config():
    """Factory so each test can build a FakeConfigManager with its own state."""
    def _make(**kwargs):
        return FakeConfigManager(**kwargs)
    return _make
