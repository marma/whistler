"""The backup service's state in the cluster (design/backup.md, "The
first-login offer").

- **The install id** is the uid of the Helm-rendered ConfigMap
  ``<release>-install``. Helm deletes it on uninstall and creates a new one
  on install, while ``helm upgrade`` keeps it: exactly "a reinstall changes
  it, an upgrade does not", without guessing from what is in the cluster.
- **The restore decision** lives in ``<release>-install-state``, written by
  this service. A decision recorded for another install id counts as
  pending, which is what makes the offer come back after every reinstall.
- **Settings** (schedule, retention) in ``<release>-backup-settings``.
- **The passphrase** in the Secret ``<release>-backup-passphrase``. It is
  never put in a backup: someone holding a copy of the volume or a
  downloaded file does not thereby hold the CA.
"""

import base64
import datetime
import json
import os
from typing import Any, Dict, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException

from whistler.backup import schedule


def _env(name, default):
    return os.environ.get(name) or default


class ClusterState:
    def __init__(self, namespace: str, release: str = "whistler"):
        self.ns = namespace
        self.install_cm = _env("WHISTLER_BACKUP_INSTALL_CONFIGMAP",
                               f"{release}-install")
        self.state_cm = _env("WHISTLER_BACKUP_STATE_CONFIGMAP",
                             f"{release}-install-state")
        self.settings_cm = _env("WHISTLER_BACKUP_SETTINGS_CONFIGMAP",
                                f"{release}-backup-settings")
        self.passphrase_secret = _env("WHISTLER_BACKUP_PASSPHRASE_SECRET",
                                      f"{release}-backup-passphrase")

    @property
    def core(self):
        return client.CoreV1Api()

    def _read_cm(self, name) -> Optional[Any]:
        try:
            return self.core.read_namespaced_config_map(name, self.ns)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def _write_cm(self, name, data: Dict[str, str]) -> None:
        body = {"apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": name, "namespace": self.ns,
                             "labels": {"app": "whistler-backup"}},
                "data": data}
        try:
            self.core.replace_namespaced_config_map(name, self.ns, body)
        except ApiException as e:
            if e.status != 404:
                raise
            self.core.create_namespaced_config_map(self.ns, body)

    # --- install ---------------------------------------------------------- #

    def install_id(self) -> Optional[str]:
        cm = self._read_cm(self.install_cm)
        return cm.metadata.uid if cm is not None else None

    def decision(self, install_id: Optional[str]) -> Dict[str, Any]:
        cm = self._read_cm(self.state_cm)
        state = {}
        if cm is not None and cm.data and cm.data.get("state"):
            try:
                state = json.loads(cm.data["state"])
            except ValueError:
                state = {}
        if not install_id or state.get("installId") != install_id:
            return {"installId": install_id,
                    "decision": schedule.DECISION_PENDING}
        return state

    def record_decision(self, install_id: str, decision: str, by: str,
                        backup: str = None) -> Dict[str, Any]:
        state = {"installId": install_id, "decision": decision,
                 "decidedBy": by, "backup": backup,
                 "decidedAt": datetime.datetime.now(datetime.timezone.utc)
                 .strftime("%Y-%m-%dT%H:%M:%SZ")}
        self._write_cm(self.state_cm, {"state": json.dumps(state,
                                                           sort_keys=True)})
        return state

    # --- settings ---------------------------------------------------------- #

    @staticmethod
    def default_settings() -> Dict[str, Any]:
        return schedule.parse_settings({
            "mode": _env("WHISTLER_BACKUP_SCHEDULE_MODE", "daily"),
            "hours": _env("WHISTLER_BACKUP_SCHEDULE_HOURS", "24"),
            "at": _env("WHISTLER_BACKUP_SCHEDULE_AT", "03:00"),
            "retain": _env("WHISTLER_BACKUP_RETAIN", "14")})

    def settings(self) -> Dict[str, Any]:
        """The portal's saved settings over the chart's defaults. A saved
        value that no longer validates falls back to the defaults rather than
        to "off": the failure that stops backups quietly is the one to avoid."""
        defaults = self.default_settings()
        cm = self._read_cm(self.settings_cm)
        if cm is None or not cm.data or not cm.data.get("settings"):
            return defaults
        try:
            return schedule.parse_settings(json.loads(cm.data["settings"]),
                                           defaults)
        except ValueError:
            return defaults

    def save_settings(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        settings = schedule.parse_settings(raw, self.default_settings())
        self._write_cm(self.settings_cm,
                       {"settings": json.dumps(settings, sort_keys=True)})
        return settings

    # --- passphrase ---------------------------------------------------------- #

    def passphrase(self) -> Optional[str]:
        try:
            secret = self.core.read_namespaced_secret(self.passphrase_secret,
                                                      self.ns)
        except ApiException as e:
            if e.status == 404:
                return None
            raise
        raw = (secret.data or {}).get("passphrase")
        return base64.b64decode(raw).decode() if raw else None

    def set_passphrase(self, passphrase: Optional[str]) -> None:
        if not passphrase:
            try:
                self.core.delete_namespaced_secret(self.passphrase_secret,
                                                   self.ns)
            except ApiException as e:
                if e.status != 404:
                    raise
            return
        body = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
                "metadata": {"name": self.passphrase_secret,
                             "namespace": self.ns,
                             "labels": {"app": "whistler-backup"}},
                "data": {"passphrase": base64.b64encode(
                    passphrase.encode()).decode()}}
        try:
            self.core.replace_namespaced_secret(self.passphrase_secret,
                                                self.ns, body)
        except ApiException as e:
            if e.status != 404:
                raise
            self.core.create_namespaced_secret(self.ns, body)
