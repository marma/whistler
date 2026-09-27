"""The backup service (design/backup.md, "Who mounts it: a small backup
service"): the one process that mounts the backup volume.

    python -m whistler.backup serve

An HTTP API for the portal (Phase 4) and the uninstall hook (Phase 5), plus
the scheduler. It is never exposed outside the cluster: callers present
their ServiceAccount token, which is checked with a TokenReview against an
allow-list of ServiceAccounts, and a NetworkPolicy limits who can connect at
all. Downloads and uploads are streamed through the portal.

Why a separate process: a backup volume that cannot bind leaves this pod
Pending, and that must not take the admin UI down with it; one mounter works
on any access mode; and the files hold the SSH CA's private key, which the
internet-facing portal should not.

    GET    /healthz                         liveness, no auth
    GET    /v1/status                       volume, install, schedule, health
    GET    /v1/backups                      list, newest first
    POST   /v1/backups                      back up now {trigger?, by?}
    PUT    /v1/backups                      upload a backup file (body)
    GET    /v1/backups/{file}               download
    DELETE /v1/backups/{file}               delete
    POST   /v1/backups/{file}/preview       plan a restore {passphrase?, includeSecrets?}
    POST   /v1/backups/{file}/restore       restore     {passphrase?, includeSecrets?, by?}
    GET    /v1/settings                     schedule and retention
    PUT    /v1/settings                     change them
    PUT    /v1/passphrase                   set or clear {passphrase}
    POST   /v1/install/decision             {decision: declined, by}
"""

import asyncio
import datetime
import hashlib
import logging
import os
import time
from typing import Any, Callable, Dict, Optional

from aiohttp import web
from kubernetes import client
from kubernetes.client.rest import ApiException

from whistler.backup import BackupError, archive, restore, schedule
from whistler.backup.export import export

logger = logging.getLogger("whistler.backup")

# Typed storage keys where aiohttp has them (3.9 AppKey, 3.10+ RequestKey).
CALLER = getattr(web, "RequestKey", lambda n, t=None: n)("caller", str)
SCHEDULER = getattr(web, "AppKey", lambda n, t=None: n)("scheduler",
                                                        asyncio.Task)

PORT = 8090
TICK_SECONDS = 60
AUTH_CACHE_SECONDS = 60


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


def _iso(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ") if value else None


def _public(entry: Dict[str, Any]) -> Dict[str, Any]:
    m = entry.get("manifest") or {}
    return {"file": entry["file"], "size": entry["size"],
            "createdAt": _iso(entry["createdAt"]), "trigger": entry["trigger"],
            "installId": entry.get("installId"),
            "contentHash": entry.get("contentHash"),
            "counts": m.get("counts") or {}, "secrets": m.get("secrets"),
            "whistlerVersion": m.get("whistlerVersion"),
            "problem": entry.get("problem")}


# --- who may call ----------------------------------------------------------------- #

class TokenReviewAuth:
    """A bearer token is a ServiceAccount's if the API server says so. Only
    the ServiceAccounts in ``allowed`` (``system:serviceaccount:<ns>:<name>``)
    get in. Positive answers are cached briefly: the portal polls status."""

    def __init__(self, allowed, review: Callable[[str], Optional[str]] = None):
        self.allowed = set(allowed)
        self._review = review or self._token_review
        self._cache: Dict[str, tuple] = {}

    @staticmethod
    def _token_review(token: str) -> Optional[str]:
        resp = client.AuthenticationV1Api().create_token_review(
            {"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
             "spec": {"token": token}})
        status = resp.status
        if status and status.authenticated and status.user:
            return status.user.username
        return None

    async def __call__(self, token: str) -> Optional[str]:
        key = hashlib.sha256(token.encode()).hexdigest()
        hit = self._cache.get(key)
        if hit and hit[1] > time.monotonic():
            return hit[0]
        loop = asyncio.get_running_loop()
        try:
            user = await loop.run_in_executor(None, self._review, token)
        except ApiException as e:
            logger.error(f"TokenReview failed: {e.status} {e.reason}")
            return None
        if user in self.allowed:
            self._cache[key] = (user, time.monotonic() + AUTH_CACHE_SECONDS)
            return user
        if user:
            logger.warning(f"Refused {user}: not allowed to use backups")
        return None


# --- the service -------------------------------------------------------------------- #

class BackupService:
    def __init__(self, cm, store, state, clock=utcnow):
        self.cm, self.store, self.state, self.clock = cm, store, state, clock
        self.lock = asyncio.Lock()
        self.last_attempt = self.last_success = None
        self.last_error: Optional[str] = None
        self.last_scheduled_attempt = None
        self.last_warnings = []

    async def _run(self, fn, *args):
        return await asyncio.get_running_loop().run_in_executor(None, fn, *args)

    # backing up
    def _backup(self, trigger, skip_unchanged=False) -> Dict[str, Any]:
        now = self.clock()
        self.last_attempt = now
        try:
            install_id = self.state.install_id()
            data, manifest, warnings = export(
                self.cm, passphrase=self.state.passphrase(), trigger=trigger,
                created=now, install_id=install_id)
            entries = self.store.list()
            if skip_unchanged and entries and \
                    entries[0].get("contentHash") == manifest["contentHash"]:
                self.last_success, self.last_error = now, None
                return {"skipped": True, "unchangedSince": entries[0]["file"]}
            entry = self.store.save(data, now, trigger)
        except Exception as e:
            self.last_error = f"{_iso(now)}: {e}"
            logger.error(f"Backup ({trigger}) failed: {e}")
            raise
        self.last_success, self.last_error = now, None
        self.last_warnings = warnings
        logger.info(f"Backed up ({trigger}) to {entry['file']}")
        return {"skipped": False, "backup": _public(entry),
                "warnings": warnings}

    async def backup(self, trigger="manual", skip_unchanged=False):
        if trigger not in ("manual", "uninstall", "scheduled"):
            raise BackupError(f"Cannot start a {trigger!r} backup.")
        async with self.lock:
            return await self._run(self._backup, trigger, skip_unchanged)

    # the scheduler
    def _tick(self) -> str:
        entries = self.store.list()
        install_id = self.state.install_id()
        if not install_id:
            # No install record (a chart without it): nothing can be offered
            # or safely pruned, so back up but never delete.
            decision = schedule.DECISION_FRESH
        else:
            decision = self.state.decision(install_id)["decision"]
        offer = schedule.offer_state(decision, entries, install_id)
        if offer == "fresh" and install_id:
            self.state.record_decision(install_id, schedule.DECISION_FRESH,
                                       "system")
        if offer == "offer":
            return "paused"
        settings = self.state.settings()
        now = self.clock()
        last = max([t for t in (self.last_scheduled_attempt,
                                schedule.last_scheduled(entries, install_id))
                    if t] or [None], default=None)
        if not schedule.is_due(settings, last, now):
            return "idle"
        self.last_scheduled_attempt = now
        result = self._backup("scheduled", skip_unchanged=True)
        for name in schedule.prunable(self.store.list(), install_id,
                                      settings["retain"]):
            self.store.delete(name)
            logger.info(f"Retention removed {name}")
        return "unchanged" if result["skipped"] else "backed-up"

    async def tick(self) -> str:
        async with self.lock:
            return await self._run(self._tick)

    async def run_scheduler(self, interval=TICK_SECONDS):
        while True:
            try:
                await self.tick()
            except Exception as e:
                logger.error(f"Scheduler tick failed: {e}")
            await asyncio.sleep(interval)

    # reading
    def _status(self) -> Dict[str, Any]:
        entries = self.store.list()
        install_id = self.state.install_id()
        decision = self.state.decision(install_id)
        settings = self.state.settings()
        offer = schedule.offer_state(decision["decision"], entries, install_id)
        last = max([t for t in (self.last_scheduled_attempt,
                                schedule.last_scheduled(entries, install_id))
                    if t] or [None], default=None)
        return {
            "volume": {**self.store.usage(), **self._claim_info()},
            "install": {**decision, "installId": install_id,
                        "known": bool(install_id)},
            "offer": offer == "offer",
            "offers": [_public(e) for e in
                       schedule.from_other_installs(entries, install_id)]
            if offer == "offer" else [],
            "paused": offer == "offer",
            "settings": settings,
            "nextRun": None if offer == "offer" else
            _iso(schedule.next_run(settings, last, self.clock())),
            "lastAttempt": _iso(self.last_attempt),
            "lastSuccess": _iso(self.last_success or (
                entries[0]["createdAt"] if entries else None)),
            "lastError": self.last_error,
            "warnings": self.last_warnings,
            "stale": schedule.stale(settings, self.last_success or (
                entries[0]["createdAt"] if entries else None),
                bool(self.last_error), self.clock()),
            "passphraseSet": bool(self.state.passphrase()),
            "backups": len(entries),
        }

    def _claim_info(self) -> Dict[str, Any]:
        """Which PV the backups are on, so the portal can say whether they
        would survive losing the cluster."""
        claim = os.environ.get("WHISTLER_BACKUP_CLAIM")
        if not claim:
            return {}
        core = client.CoreV1Api()
        try:
            pvc = core.read_namespaced_persistent_volume_claim(
                claim, self.cm.namespace)
            info = {"claim": claim, "phase": pvc.status.phase,
                    "storageClassName": pvc.spec.storage_class_name,
                    "persistentVolume": pvc.spec.volume_name}
            if pvc.spec.volume_name:
                pv = core.read_persistent_volume(pvc.spec.volume_name)
                spec = pv.spec
                info["reclaimPolicy"] = spec.persistent_volume_reclaim_policy
                info["driver"] = (spec.csi.driver if spec.csi else
                                  "nfs" if spec.nfs else
                                  "local" if (spec.local or spec.host_path)
                                  else None)
            return info
        except ApiException as e:
            return {"claim": claim, "problem": f"{e.status} {e.reason}"}

    async def status(self):
        return await self._run(self._status)

    async def list(self):
        return [_public(e) for e in await self._run(self.store.list)]

    # uploads, downloads, deletes
    def _upload(self, data: bytes) -> Dict[str, Any]:
        manifest = archive.read_manifest(data)
        archive.read(data)      # checksums, format, objects: all of it
        try:
            created = datetime.datetime.strptime(
                manifest["createdAt"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=datetime.timezone.utc)
        except (KeyError, TypeError, ValueError):
            created = self.clock()
        return _public(self.store.save(data, created, "uploaded"))

    async def upload(self, data: bytes):
        async with self.lock:
            return await self._run(self._upload, data)

    async def download(self, name: str) -> bytes:
        return await self._run(self.store.read, name)

    async def delete(self, name: str):
        async with self.lock:
            await self._run(self.store.delete, name)

    # restoring
    def _load(self, name, passphrase):
        return archive.read(self.store.read(name),
                            passphrase=passphrase or self.state.passphrase())

    def _preview(self, name, passphrase, include_secrets):
        backup = self._load(name, passphrase)
        the_plan = restore.plan(self.cm, backup, include_secrets)
        return {"entries": [e.as_dict() for e in the_plan.entries],
                "counts": the_plan.counts(), "warnings": the_plan.warnings,
                "secrets": backup.secrets_mode,
                "secretsReadable": backup.secrets is not None}

    async def preview(self, name, passphrase=None, include_secrets=True):
        return await self._run(self._preview, name, passphrase, include_secrets)

    def _restore(self, name, passphrase, include_secrets, by):
        backup = self._load(name, passphrase)
        the_plan = restore.plan(self.cm, backup, include_secrets)
        pre = None
        if any(e.action in (restore.CREATE, restore.REPLACE)
               for e in the_plan.entries):
            # Every restore can be undone with another restore.
            pre = self._backup("pre-restore")["backup"]["file"]
        written = restore.apply(self.cm, the_plan)
        pruned = restore.readback(self.cm, written)
        install_id = self.state.install_id()
        if install_id and self.state.decision(install_id)["decision"] == \
                schedule.DECISION_PENDING:
            self.state.record_decision(install_id, schedule.DECISION_RESTORED,
                                       by or "unknown", backup=name)
        logger.info(f"{by} restored {name}: {len(written)} object(s) written")
        return {"written": len(written), "counts": the_plan.counts(),
                "warnings": the_plan.warnings, "pruned": pruned,
                "prunedHint": restore.PRUNED_HINT if pruned else None,
                "preRestoreBackup": pre}

    async def restore(self, name, passphrase=None, include_secrets=True,
                      by=None):
        async with self.lock:
            return await self._run(self._restore, name, passphrase,
                                   include_secrets, by)

    # settings and decisions
    async def settings(self):
        return await self._run(self.state.settings)

    async def save_settings(self, raw):
        return await self._run(self.state.save_settings, raw)

    async def set_passphrase(self, passphrase):
        await self._run(self.state.set_passphrase, passphrase)

    def _decide(self, decision, by):
        if decision != schedule.DECISION_DECLINED:
            raise BackupError("Only 'declined' can be recorded directly; a "
                              "restore records 'restored' itself.")
        install_id = self.state.install_id()
        if not install_id:
            raise BackupError("This install has no install record.")
        return self.state.record_decision(install_id, decision, by or "unknown")

    async def decide(self, decision, by=None):
        return await self._run(self._decide, decision, by)


# --- HTTP ----------------------------------------------------------------------------- #

def make_app(service: BackupService, auth) -> web.Application:
    @web.middleware
    async def guard(request, handler):
        if request.path.startswith("/v1/"):
            header = request.headers.get("Authorization", "")
            if not header.startswith("Bearer "):
                return web.json_response({"error": "authentication required"},
                                         status=401)
            user = await auth(header[len("Bearer "):].strip())
            if not user:
                return web.json_response({"error": "not allowed"}, status=403)
            request[CALLER] = user
        try:
            return await handler(request)
        except BackupError as e:
            return web.json_response({"error": str(e)}, status=400)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)

    async def body(request) -> Dict[str, Any]:
        if not request.can_read_body:
            return {}
        try:
            data = await request.json()
        except ValueError:
            raise BackupError("The request body is not JSON.")
        if not isinstance(data, dict):
            raise BackupError("The request body must be a JSON object.")
        return data

    def by(request, data):
        # Who clicked, as the portal says, and which process said it.
        return f"{data.get('by') or '?'} (via {request[CALLER]})"

    async def healthz(request):
        return web.json_response({"ok": True})

    async def status(request):
        return web.json_response(await service.status())

    async def list_backups(request):
        return web.json_response(await service.list())

    async def create(request):
        data = await body(request)
        result = await service.backup(data.get("trigger") or "manual")
        return web.json_response(result, status=201)

    async def upload(request):
        return web.json_response(await service.upload(await request.read()),
                                 status=201)

    async def download(request):
        name = request.match_info["file"]
        data = await service.download(name)
        return web.Response(body=data, content_type="application/gzip",
                            headers={"Content-Disposition":
                                     f'attachment; filename="{name}"'})

    async def delete(request):
        await service.delete(request.match_info["file"])
        return web.json_response({"deleted": request.match_info["file"]})

    async def preview(request):
        data = await body(request)
        return web.json_response(await service.preview(
            request.match_info["file"], data.get("passphrase"),
            data.get("includeSecrets", True)))

    async def do_restore(request):
        data = await body(request)
        return web.json_response(await service.restore(
            request.match_info["file"], data.get("passphrase"),
            data.get("includeSecrets", True), by(request, data)))

    async def get_settings(request):
        return web.json_response(await service.settings())

    async def put_settings(request):
        return web.json_response(await service.save_settings(
            await body(request)))

    async def put_passphrase(request):
        data = await body(request)
        await service.set_passphrase(data.get("passphrase") or None)
        return web.json_response({"passphraseSet": bool(data.get("passphrase"))})

    async def decision(request):
        data = await body(request)
        return web.json_response(await service.decide(
            data.get("decision"), by(request, data)))

    app = web.Application(middlewares=[guard],
                          client_max_size=archive.MAX_ARCHIVE_BYTES)
    app.add_routes([
        web.get("/healthz", healthz),
        web.get("/v1/status", status),
        web.get("/v1/backups", list_backups),
        web.post("/v1/backups", create),
        web.put("/v1/backups", upload),
        web.get("/v1/backups/{file}", download),
        web.delete("/v1/backups/{file}", delete),
        web.post("/v1/backups/{file}/preview", preview),
        web.post("/v1/backups/{file}/restore", do_restore),
        web.get("/v1/settings", get_settings),
        web.put("/v1/settings", put_settings),
        web.put("/v1/passphrase", put_passphrase),
        web.post("/v1/install/decision", decision),
    ])
    return app


def serve() -> None:
    from whistler.backup.state import ClusterState
    from whistler.backup.store import Store
    from whistler.config import KubeConfigManager

    cm = KubeConfigManager()
    store = Store(os.environ.get("WHISTLER_BACKUP_DIR", "/backups"))
    store.sweep_incoming()
    state = ClusterState(cm.namespace,
                         os.environ.get("WHISTLER_RELEASE_NAME", "whistler"))
    allowed = [a.strip() for a in os.environ.get(
        "WHISTLER_BACKUP_ALLOWED_SERVICEACCOUNTS", "").split(",") if a.strip()]
    if not allowed:
        logger.warning("WHISTLER_BACKUP_ALLOWED_SERVICEACCOUNTS is empty: "
                       "every API call will be refused")
    service = BackupService(cm, store, state)
    app = make_app(service, TokenReviewAuth(allowed))

    async def start(app):
        app[SCHEDULER] = asyncio.create_task(service.run_scheduler(
            int(os.environ.get("WHISTLER_BACKUP_TICK_SECONDS", TICK_SECONDS))))

    async def stop(app):
        app[SCHEDULER].cancel()

    app.on_startup.append(start)
    app.on_cleanup.append(stop)
    # Not WHISTLER_BACKUP_PORT: Kubernetes injects exactly that name
    # (`tcp://<ip>:8090`) for a Service called whistler-backup.
    web.run_app(app, port=int(os.environ.get("WHISTLER_BACKUP_LISTEN_PORT",
                                             PORT)),
                access_log=None, print=None)
