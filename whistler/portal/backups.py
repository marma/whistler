"""The portal's client for the backup service (design/backup.md, Phase 4).

The portal never mounts the backup volume; it asks ``<release>-backup`` over
HTTP, presenting this pod's ServiceAccount token, which the service checks
with a TokenReview. The token is re-read on every call: projected tokens
rotate, and a portal that cached one would start failing an hour later.

Two kinds of failure, kept apart because the portal treats them differently:

- ``BackupUnavailable``: the service cannot be reached or will not talk to
  us. The portal keeps working and says so. A broken backup volume must not
  take the admin UI down, which is the reason the service is separate.
- ``BackupRefused``: the service understood and said no (a bad passphrase, a
  damaged file, invalid settings). The message is for the admin.
"""

import os
from typing import Any, Dict, Optional, Tuple

import aiohttp

SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

QUICK = aiohttp.ClientTimeout(total=5)
SLOW = aiohttp.ClientTimeout(total=300)     # backing up, restoring, uploads


# Storage drivers whose data lives on the cluster's own nodes, so backups on
# them survive a reinstall but not losing the cluster. Everything else (NFS,
# SMB, cloud disks) is treated as outside it. A heuristic for a warning, not
# a guarantee either way: said as such on the page.
_IN_CLUSTER_DRIVERS = ("local", "hostpath", "longhorn", "rook", "ceph",
                       "openebs", "topolvm", "portworx", "linstor")


def in_cluster_storage(driver: Optional[str]) -> bool:
    return bool(driver) and any(k in driver.lower() for k in _IN_CLUSTER_DRIVERS)


class BackupUnavailable(Exception):
    pass


class BackupRefused(Exception):
    pass


class BackupClient:
    def __init__(self, url: str = None, token_path: str = SA_TOKEN_PATH,
                 token: str = None):
        self.url = (url if url is not None
                    else os.environ.get("WHISTLER_BACKUP_URL", "")).rstrip("/")
        self.token_path = token_path
        self._token = token

    @property
    def enabled(self) -> bool:
        """False when the chart has backups off (no WHISTLER_BACKUP_URL): no
        Backups page, no offer."""
        return bool(self.url)

    def _headers(self) -> Dict[str, str]:
        token = self._token
        if token is None:
            try:
                with open(self.token_path) as f:
                    token = f.read().strip()
            except OSError as e:
                raise BackupUnavailable(
                    f"No ServiceAccount token to present to the backup "
                    f"service: {e}")
        return {"Authorization": f"Bearer {token}"}

    async def _call(self, method: str, path: str, *, json=None, data=None,
                    timeout=QUICK, raw=False):
        if not self.enabled:
            raise BackupUnavailable("Backups are not enabled in this install.")
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(method, self.url + path,
                                           headers=self._headers(),
                                           json=json, data=data) as resp:
                    if resp.status in (401, 403):
                        raise BackupUnavailable(
                            f"The backup service refused this portal "
                            f"({resp.status}); its ServiceAccount must be in "
                            f"WHISTLER_BACKUP_ALLOWED_SERVICEACCOUNTS.")
                    if resp.status == 400:
                        try:
                            message = (await resp.json()).get("error")
                        except (aiohttp.ContentTypeError, ValueError):
                            message = await resp.text()
                        raise BackupRefused(message or "Refused.")
                    if resp.status >= 400:
                        raise BackupUnavailable(
                            f"The backup service answered {resp.status}.")
                    if raw:
                        return await resp.read(), resp.headers
                    return await resp.json()
        except (aiohttp.ClientError, TimeoutError) as e:
            raise BackupUnavailable(
                f"The backup service is not reachable ({type(e).__name__}"
                f"{': ' + str(e) if str(e) else ''}).")

    # reading
    async def status(self) -> Dict[str, Any]:
        return await self._call("GET", "/v1/status")

    async def list(self):
        return await self._call("GET", "/v1/backups")

    async def download(self, name: str) -> Tuple[bytes, str]:
        data, headers = await self._call("GET", f"/v1/backups/{name}",
                                         timeout=SLOW, raw=True)
        return data, name

    # writing
    async def create(self, by: str):
        return await self._call("POST", "/v1/backups", json={"by": by},
                                timeout=SLOW)

    async def upload(self, data: bytes):
        return await self._call("PUT", "/v1/backups", data=data, timeout=SLOW)

    async def delete(self, name: str):
        return await self._call("DELETE", f"/v1/backups/{name}")

    async def preview(self, name: str, passphrase: Optional[str] = None,
                      include_secrets: bool = True):
        return await self._call("POST", f"/v1/backups/{name}/preview",
                                json={"passphrase": passphrase,
                                      "includeSecrets": include_secrets},
                                timeout=SLOW)

    async def restore(self, name: str, by: str,
                      passphrase: Optional[str] = None,
                      include_secrets: bool = True):
        return await self._call("POST", f"/v1/backups/{name}/restore",
                                json={"passphrase": passphrase,
                                      "includeSecrets": include_secrets,
                                      "by": by},
                                timeout=SLOW)

    async def save_settings(self, settings: Dict[str, Any]):
        return await self._call("PUT", "/v1/settings", json=settings)

    async def set_passphrase(self, passphrase: Optional[str]):
        return await self._call("PUT", "/v1/passphrase",
                                json={"passphrase": passphrase})

    async def decline(self, by: str):
        return await self._call("POST", "/v1/install/decision",
                                json={"decision": "declined", "by": by})
