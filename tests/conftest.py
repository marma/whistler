"""Shared test fixtures.

`FactConfigManager` lives here (rather than a sibling module) so pytest's
auto-loaded conftest makes it and its fixtures available to every test without
import-path juggling.
"""
from typing import Any, Dict, List, Optional

import pytest

from whistler.config import (CHANNEL_CLIPBOARD, CHANNEL_SCREENSHOTS,
                             CHANNEL_TERMINAL, ConfigManager, DEFAULT_ZONE,
                             OVERRIDE_GROUPS, SSH_POSTURE_DIRECT,
                             _POSTURE_CHANNELS, group_volume_grants,
                             merge_allow_lists, merge_channel_grants,
                             merge_override_grants)


class FakeConfigManager(ConfigManager):
    """In-memory ConfigManager for unit tests — no Kubernetes API.

    users: {username: {"name", "publicKeys": [...], "securityContext": {...}}}
    templates: {username: [{"name", "fullName", ...}]}
    instances: {username: [{"name", "status", "podName", ...}]}
    """

    def __init__(self, users=None, templates=None, instances=None,
                 desktop_templates=None, desktop_sessions=None, gpu_types=None,
                 zones=None, ssh_targets=None, ssh_domain_suffix=".w",
                 ssh_ca_public_key=None, vm_access_keys=None, groups=None,
                 home_volumes=None, home_volume_holders=None):
        self.users: Dict[str, Dict[str, Any]] = users or {}
        # {group name: spec} — resolved through the same pure helpers the real
        # manager uses, so a test of the fake is a test of the rule.
        self.groups: Dict[str, Dict[str, Any]] = groups or {}
        # {username: {name: {...resolve_ssh_target dict...}}} for jump routing.
        self._ssh_targets: Dict[str, Dict[str, Any]] = ssh_targets or {}
        self.ssh_domain_suffix = ssh_domain_suffix
        self.started: List[tuple] = []
        self.created: List[tuple] = []
        self.deleted: List[tuple] = []
        self.ssh_ca_public_key = ssh_ca_public_key
        self.vm_access_keys: Dict[str, str] = vm_access_keys or {}
        self._templates: Dict[str, List[Dict[str, Any]]] = templates or {}
        self._instances: Dict[str, List[Dict[str, Any]]] = instances or {}
        # {username: [volume dicts]} and {volume name: holding instance}.
        self._home_volumes: Dict[str, List[Dict[str, Any]]] = home_volumes or {}
        self._home_volume_holders: Dict[str, str] = home_volume_holders or {}
        self.deleted_home_data: List[tuple] = []
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
                     overrides=None, ephemeral=False, home_volume=None):
        self._instances.setdefault(username, []).append({
            "name": instance_name,
            "template": template_name,
            "status": "Stopped",
            "podName": None,
            "preemptible": preemptible,
            "overrides": overrides,
            "ephemeral": ephemeral,
            "homeVolume": home_volume,
        })
        self.created.append((username, template_name, instance_name, ephemeral))
        return True

    def get_user_volume_access(self, username):
        from whistler.config import merge_volume_access
        own = self.users.get(username, {}).get("volumeAccess") or {}
        return merge_volume_access(own, *(
            (g.get("volumeAccess") or {})
            for g in self.get_user_groups(username)))

    def set_user_volume_access(self, username, matrix):
        self.users.setdefault(username, {})["volumeAccess"] = matrix
        return True

    def grant_own_volume_access(self, username, zone, volume, mode="allowed"):
        own = self.users.setdefault(username, {}).setdefault("volumeAccess", {})
        own.setdefault(zone, {})[volume] = mode
        return True

    def get_home_volumes(self, username):
        return sorted(self._home_volumes.get(username, []),
                      key=lambda v: v.get("name") or "")

    def save_home_volume(self, username, volume):
        vols = self._home_volumes.setdefault(username, [])
        for i, existing in enumerate(vols):
            if existing.get("name") == volume.get("name"):
                vols[i] = {**existing, **volume}
                return True
        vols.append(dict(volume))
        return True

    def delete_home_volume(self, username, name, delete_data=False):
        vols = self._home_volumes.get(username, [])
        for i, v in enumerate(vols):
            if v.get("name") == name:
                if self.home_volume_holder(username, v):
                    return False
                vols.pop(i)
                self.deleted_home_data.append((username, name, delete_data))
                return True
        return False

    def home_volume_holder(self, username, volume, ignore_instance=None):
        holder = self._home_volume_holders.get(volume.get("name"))
        return None if holder == ignore_instance else holder

    def get_instance_config(self, username, instance_name):
        for inst in self._instances.get(username, []):
            if inst["name"] == instance_name:
                return {
                    "templateRef": inst.get("template"),
                    "preemptible": inst.get("preemptible", False),
                    "homeVolume": inst.get("homeVolume"),
                    "overrides": inst.get("overrides") or {},
                }
        return None

    def update_instance(self, username, instance_name, preemptible=False,
                        overrides=None, home_volume=None):
        for inst in self._instances.get(username, []):
            if inst["name"] == instance_name:
                inst["preemptible"] = preemptible
                inst["overrides"] = overrides
                inst["homeVolume"] = home_volume
                return True
        return False

    def save_template(self, username, template_data):
        return True

    def delete_instance(self, username, instance_name):
        self.deleted.append((username, instance_name))
        self._instances[username] = [
            i for i in self._instances.get(username, [])
            if i["name"] != instance_name]
        return True

    def delete_template(self, username, template_name):
        return True

    def get_user_desktop_templates(self, username):
        return list(self._desktop_templates.get(username, []))

    def get_user_desktop_sessions(self, username):
        return list(self._desktop_sessions.get(username, []))

    def add_desktop_session(self, username, template_name, session_name, overrides=None,
                            ephemeral=False):
        self._desktop_sessions.setdefault(username, []).append({
            "name": session_name,
            "template": template_name,
            "phase": "Provisioning",
            "runtime": None,
            "backend": None,
            "podName": None,
            "overrides": overrides,
            "ephemeral": ephemeral,
        })
        self.created.append((username, template_name, session_name, ephemeral))
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

    def resolve_ssh_target(self, username, name):
        target = (self._ssh_targets.get(username) or {}).get(name)
        return dict(target) if target else None

    def get_ssh_known_hosts_line(self):
        return "@cert-authority *.w ssh-ed25519 AAAAFAKE"

    def list_ssh_targets(self, username):
        # Mirrors KubeConfigManager: the runtime comes from the session, so an
        # ssh-mode VM (images/devbase) is SSH-reachable like a desktop VM.
        targets = [
            {"name": i.get("name"), "template": i.get("template"),
             "status": i.get("status"),
             "runtime": i.get("runtime") or "container",
             "mode": "ssh",
             "sshReachable": (i.get("runtime") or "container") == "vm"}
            for i in self._instances.get(username, [])
        ]
        targets += [
            {"name": s.get("name"), "template": s.get("template"),
             "status": s.get("phase"), "runtime": s.get("runtime"),
             "mode": "desktop", "sshReachable": s.get("runtime") == "vm"}
            for s in self._desktop_sessions.get(username, [])
        ]
        targets.sort(key=lambda t: t.get("name") or "")
        return targets

    def get_ssh_ca_public_key(self):
        return self.ssh_ca_public_key

    def get_vm_access_private_key(self, username):
        return self.vm_access_keys.get(username)


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

    def list_all_desktop_sessions(self):
        return []

    def save_system_template(self, template_data):
        return True

    def save_volume(self, volume_data):
        return True

    def delete_volume(self, volume_name):
        return True

    def get_user_groups(self, username):
        member_of = []
        for name in sorted(self.groups):
            spec = {**self.groups[name], "name": name}
            if username in (spec.get("members") or []) or group_volume_grants(spec, username):
                member_of.append(spec)
        return member_of

    def get_group_definitions(self):
        return {name: dict(spec) for name, spec in self.groups.items()}

    def save_group(self, group_data):
        data = dict(group_data)
        name = (data.pop("name", "") or "").strip()
        if not name:
            return False
        self.groups[name] = data
        return True

    def delete_group(self, group_name):
        self.groups.pop(group_name, None)
        return True

    def get_user_allowed_volumes(self, username):
        own = (self.users.get(username) or {}).get("allowedVolumes", [])
        return merge_allow_lists(own, *(list(group_volume_grants(g, username))
                                        for g in self.get_user_groups(username)))

    def get_user_volume_modes(self, username):
        modes = {}
        for group in self.get_user_groups(username):
            for name, mode in group_volume_grants(group, username).items():
                if modes.get(name) != "rw":
                    modes[name] = mode
        for name in (self.users.get(username) or {}).get("allowedVolumes", []) or []:
            modes[name] = "rw"
        return modes

    def set_user_allowed_volumes(self, username, volume_names):
        if username in self.users:
            self.users[username]["allowedVolumes"] = volume_names
        return True

    def get_user_allowed_gpu_types(self, username):
        own = (self.users.get(username) or {}).get("allowedGpuTypes", [])
        return merge_allow_lists(own, *(g.get("allowedGpuTypes")
                                        for g in self.get_user_groups(username)))

    def set_user_allowed_gpu_types(self, username, gpu_types):
        if username in self.users:
            self.users[username]["allowedGpuTypes"] = gpu_types
        return True

    def get_user_allowed_zones(self, username):
        own = (self.users.get(username) or {}).get("allowedZones", [])
        return merge_allow_lists(own, *(g.get("allowedZones")
                                        for g in self.get_user_groups(username)))

    def set_user_allowed_zones(self, username, zones):
        if username in self.users:
            self.users[username]["allowedZones"] = zones
        return True

    def set_user_channels(self, username, channels):
        # None removes the field (narrows nothing); [] is a real empty grant,
        # exactly as KubeConfigManager._save_user_spec treats it.
        if username in self.users:
            if channels is None:
                self.users[username].pop("channels", None)
            else:
                self.users[username]["channels"] = list(channels)
        return True

    def get_user_channels(self, username):
        own = (self.users.get(username) or {}).get("channels")
        return merge_channel_grants(own, *(g.get("channels")
                                           for g in self.get_user_groups(username)))

    def zone_channel_ceiling(self, zone):
        cfg = self._zones.get(zone) or {}
        stated = cfg.get("channels")
        if stated is not None:
            return set(stated)
        posture = cfg.get("ssh") or SSH_POSTURE_DIRECT
        return set(_POSTURE_CHANNELS.get(posture, ())) | {
            CHANNEL_TERMINAL, CHANNEL_CLIPBOARD, CHANNEL_SCREENSHOTS}

    def effective_channels(self, username, zone):
        ceiling = self.zone_channel_ceiling(zone)
        grant = self.get_user_channels(username)
        return ceiling if grant is None else (ceiling & grant)

    def session_channels(self, username, name):
        target = self.resolve_ssh_target(username, name)
        if not target:
            return set()
        return self.effective_channels(username, target.get("zone") or DEFAULT_ZONE)

    def get_user_overrides(self, username):
        own = (self.users.get(username) or {}).get("overrides", {}) or {}
        merged = merge_override_grants(own, *(g.get("overrides")
                                              for g in self.get_user_groups(username)))
        return {g: bool(merged.get(g, False)) for g in OVERRIDE_GROUPS}

    def set_user_overrides(self, username, overrides):
        if username in self.users:
            self.users[username]["overrides"] = {
                g: bool(overrides.get(g, False)) for g in OVERRIDE_GROUPS
            }
        return True

    def stop_instance(self, username, instance_name):
        return True

    def trigger_instance_start(self, username, instance_name):
        self.started.append((username, instance_name))
        return True


@pytest.fixture
def make_config():
    """Factory so each test can build a FakeConfigManager with its own state."""
    def _make(**kwargs):
        return FakeConfigManager(**kwargs)
    return _make
