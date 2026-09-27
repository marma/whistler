"""The backup file: one ``.tar.gz`` holding

    manifest.json   format, versions, counts, checksums, what secrets it has
    state.yaml      the CRs, plain Kubernetes objects, kubectl-applyable
    secrets.yaml    the durable Secrets, or
    secrets.enc     the same, encrypted with the backup passphrase

Pure: no cluster, no clock unless passed one. Everything that decides what
an object looks like in a backup is here (``normalize``), so export and the
restore's comparison can never disagree about it.

Deterministic on purpose. The same state gives a byte-identical
``state.yaml`` (objects sorted, keys sorted), which is what makes two
backups diffable and lets a scheduler skip a backup identical to the last
one (``content_hash``).
"""

import base64
import datetime
import gzip
import hashlib
import io
import json
import os
import tarfile
from typing import Any, Dict, List, Optional, Tuple

import yaml

from whistler.backup import BackupError

FORMAT = 1

MANIFEST = "manifest.json"
STATE = "state.yaml"
SECRETS_PLAIN = "secrets.yaml"
SECRETS_ENCRYPTED = "secrets.enc"
MEMBERS = (MANIFEST, STATE, SECRETS_PLAIN, SECRETS_ENCRYPTED)

# What a reader accepts, uncompressed. State is kilobytes; anything near this
# is not a Whistler backup, and a gzip bomb is exactly what an upload is.
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024

# Restore order, which is also file order. References are by name and
# resolved live, so this is not needed for correctness, but a Session created
# before its User logs a failed reconcile nobody needs to read.
KIND_ORDER = ("Zone", "Group", "User", "Template", "Dataset",
              "HomeVolume", "Session")

# Kept on the Secret in the file: what it is for. Names contain the release
# name (`<release>-ssh-ca`), so a restore maps by role to the names the
# target install is configured with.
ROLE_ANNOTATION = "whistler/backup-role"
ROLE_SSH_CA = "ssh-ca"
ROLE_SERVER_HOST_KEY = "server-host-key"
ROLE_DATASET_CREDENTIALS = "dataset-credentials"
ROLES = (ROLE_SSH_CA, ROLE_SERVER_HOST_KEY, ROLE_DATASET_CREDENTIALS)

# Metadata that belongs to the live object, not to its meaning.
_DROPPED_ANNOTATION_PREFIXES = ("kopf.zalando.org/", "kubectl.kubernetes.io/",
                                "meta.helm.sh/", "deployment.kubernetes.io/")
# A Session's run state. Dropped so a restore boots nothing: with neither
# mark, run_intent answers stopped (whistler.config.START_ANNOTATION /
# STOP_ANNOTATION; literal here to keep this module free of the config one).
SESSION_RUN_ANNOTATIONS = ("whistler/last-connect", "whistler/last-stop")
SESSION_RUN_SPEC = ("runOverrides",)

HELM_MANAGED_LABEL = ("app.kubernetes.io/managed-by", "Helm")


def is_helm_managed(obj: Dict[str, Any]) -> bool:
    labels = (obj.get("metadata") or {}).get("labels") or {}
    return labels.get(HELM_MANAGED_LABEL[0]) == HELM_MANAGED_LABEL[1]


def normalize(obj: Dict[str, Any], release_namespace: str = None
              ) -> Dict[str, Any]:
    """An object as it appears in a backup: identity, labels, annotations
    and content, nothing the API server or a controller owns.

    Objects in ``release_namespace`` are written without a namespace, so
    they restore into whatever the target's release namespace is called.
    """
    meta = obj.get("metadata") or {}
    kind = obj.get("kind")
    out_meta: Dict[str, Any] = {"name": meta.get("name")}
    ns = meta.get("namespace")
    if ns and ns != release_namespace:
        out_meta["namespace"] = ns
    labels = dict(meta.get("labels") or {})
    if labels:
        out_meta["labels"] = labels
    annotations = {k: v for k, v in (meta.get("annotations") or {}).items()
                   if not k.startswith(_DROPPED_ANNOTATION_PREFIXES)
                   and not (kind == "Session"
                            and k in SESSION_RUN_ANNOTATIONS)}
    if annotations:
        out_meta["annotations"] = annotations
    out = {"apiVersion": obj.get("apiVersion"), "kind": kind,
           "metadata": out_meta}
    if kind == "Secret":
        out["type"] = obj.get("type") or "Opaque"
        out["data"] = dict(obj.get("data") or {})
    else:
        spec = dict(obj.get("spec") or {})
        if kind == "Session":
            for key in SESSION_RUN_SPEC:
                spec.pop(key, None)
        out["spec"] = spec
    return out


def content(obj: Dict[str, Any]) -> Dict[str, Any]:
    """What a restore compares: everything but identity."""
    meta = obj.get("metadata") or {}
    return {"labels": meta.get("labels") or {},
            "annotations": meta.get("annotations") or {},
            "spec": obj.get("spec"), "data": obj.get("data"),
            "type": obj.get("type")}


def _sort_key(obj: Dict[str, Any]):
    kind = obj.get("kind")
    meta = obj.get("metadata") or {}
    order = KIND_ORDER.index(kind) if kind in KIND_ORDER else len(KIND_ORDER)
    return (order, kind or "", meta.get("namespace") or "",
            meta.get("name") or "")


def dump(objects: List[Dict[str, Any]]) -> bytes:
    """Deterministic multi-document YAML."""
    ordered = sorted(objects, key=_sort_key)
    if not ordered:
        return b""
    return yaml.safe_dump_all(ordered, sort_keys=True,
                              default_flow_style=False,
                              explicit_start=True).encode()


def load(data: bytes) -> List[Dict[str, Any]]:
    try:
        docs = [d for d in yaml.safe_load_all(data.decode()) if d]
    except (yaml.YAMLError, UnicodeDecodeError) as e:
        raise BackupError(f"The backup's objects are not valid YAML: {e}")
    for d in docs:
        if not isinstance(d, dict) or not d.get("kind") or not (
                d.get("metadata") or {}).get("name"):
            raise BackupError("The backup holds an object with no kind or "
                              "name; it is not a Whistler backup.")
    return docs


def content_hash(state: bytes, secrets: bytes) -> str:
    """Identity of the backed-up state, independent of when it was taken or
    how the secrets are wrapped."""
    return hashlib.sha256(
        hashlib.sha256(state).digest() + hashlib.sha256(secrets).digest()
    ).hexdigest()


# --- secrets encryption ------------------------------------------------------ #
#
# scrypt for the key (a passphrase is typed, so the KDF is the defence against
# guessing), AES-256-GCM for the data. JSON envelope so a reader can see what
# it is looking at. The passphrase is never stored with a backup.

_AAD = b"whistler-backup/secrets/v1"
_SCRYPT = {"n": 2 ** 15, "r": 8, "p": 1}


def _key(passphrase: str, salt: bytes, n: int, r: int, p: int) -> bytes:
    from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
    return Scrypt(salt=salt, length=32, n=n, r=r, p=p).derive(
        passphrase.encode())


def encrypt(plaintext: bytes, passphrase: str) -> bytes:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    salt, nonce = os.urandom(16), os.urandom(12)
    key = _key(passphrase, salt, **_SCRYPT)
    envelope = {"kdf": "scrypt", **_SCRYPT, "cipher": "AES-256-GCM",
                "salt": base64.b64encode(salt).decode(),
                "nonce": base64.b64encode(nonce).decode(),
                "ciphertext": base64.b64encode(
                    AESGCM(key).encrypt(nonce, plaintext, _AAD)).decode()}
    return json.dumps(envelope, sort_keys=True).encode()


def decrypt(blob: bytes, passphrase: str) -> bytes:
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    try:
        env = json.loads(blob)
        if env.get("kdf") != "scrypt" or env.get("cipher") != "AES-256-GCM":
            raise ValueError("unknown kdf or cipher")
        key = _key(passphrase, base64.b64decode(env["salt"]),
                   int(env["n"]), int(env["r"]), int(env["p"]))
        return AESGCM(key).decrypt(base64.b64decode(env["nonce"]),
                                   base64.b64decode(env["ciphertext"]), _AAD)
    except InvalidTag:
        raise BackupError("Wrong passphrase for this backup's secrets.")
    except (ValueError, KeyError, TypeError) as e:
        raise BackupError(f"The backup's encrypted secrets are unreadable: {e}")


# --- the file ------------------------------------------------------------------ #

def build(objects: List[Dict[str, Any]], secrets: List[Dict[str, Any]], *,
          passphrase: str = None, trigger: str = "manual",
          created: datetime.datetime = None,
          extra: Dict[str, Any] = None) -> Tuple[bytes, Dict[str, Any]]:
    """The backup file and its manifest. ``objects`` and ``secrets`` are
    already normalized. No secrets → no secrets member at all."""
    created = created or datetime.datetime.now(datetime.timezone.utc)
    state = dump(objects)
    secrets_plain = dump(secrets) if secrets else b""
    members: Dict[str, bytes] = {STATE: state}
    if secrets:
        if passphrase:
            members[SECRETS_ENCRYPTED] = encrypt(secrets_plain, passphrase)
            secrets_mode = "encrypted"
        else:
            members[SECRETS_PLAIN] = secrets_plain
            secrets_mode = "plain"
    else:
        secrets_mode = "none"
    counts: Dict[str, int] = {}
    for obj in objects:
        counts[obj["kind"]] = counts.get(obj["kind"], 0) + 1
    manifest = {
        **(extra or {}),
        "format": FORMAT,
        "createdAt": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "trigger": trigger,
        "counts": dict(sorted(counts.items())),
        "secrets": secrets_mode,
        "secretCount": len(secrets),
        "contentHash": content_hash(state, secrets_plain),
        "sha256": {name: hashlib.sha256(data).hexdigest()
                   for name, data in sorted(members.items())},
    }
    members[MANIFEST] = json.dumps(manifest, indent=2, sort_keys=True).encode()

    buf = io.BytesIO()
    mtime = int(created.timestamp())
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tar:
        # Manifest first, so a reader that only wants it stops early.
        for name in sorted(members, key=lambda n: (n != MANIFEST, n)):
            data = members[name]
            info = tarfile.TarInfo(name)
            info.size, info.mtime, info.mode = len(data), mtime, 0o600
            tar.addfile(info, io.BytesIO(data))
    return gzip.compress(buf.getvalue(), mtime=0), manifest


class Backup:
    """A read backup. ``secrets`` is None when they are encrypted and no
    passphrase was given (``secrets_mode`` says which)."""

    def __init__(self, manifest, objects, secrets, secrets_mode):
        self.manifest = manifest
        self.objects = objects
        self.secrets = secrets
        self.secrets_mode = secrets_mode


def _members(data: bytes) -> Dict[str, bytes]:
    """The archive's members, read as untrusted input: only the known
    names, only regular files, bounded size.

    Nothing is ever extracted to disk — members are read into memory by
    name — so a path like ``../../etc/passwd`` has nowhere to land. Links,
    directories and devices are refused outright even under a valid name:
    a symlink or hard link has no content of its own, only a pointer, and
    the only safe reading of a pointer in an uploaded file is none.
    """
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
            raw = gz.read(MAX_ARCHIVE_BYTES + 1)
    except (OSError, EOFError) as e:
        raise BackupError(f"Not a gzip file: {e}")
    if len(raw) > MAX_ARCHIVE_BYTES:
        raise BackupError("The backup is larger than any Whistler backup "
                          "should be; refusing to read it.")
    out: Dict[str, bytes] = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as tar:
            for info in tar:
                if info.name not in MEMBERS:
                    raise BackupError(
                        f"Unexpected member {info.name!r}; not a Whistler "
                        f"backup.")
                if not info.isreg():
                    what = ("a symbolic link" if info.issym() else
                            "a hard link" if info.islnk() else
                            "a directory" if info.isdir() else
                            "a device or other special file")
                    raise BackupError(
                        f"Member {info.name!r} is {what}, not a regular file; "
                        f"not a Whistler backup.")
                if info.name in out:
                    raise BackupError(f"Duplicate member {info.name!r}.")
                out[info.name] = tar.extractfile(info).read()
    except tarfile.TarError as e:
        raise BackupError(f"Not a tar archive: {e}")
    return out


def read(data: bytes, passphrase: str = None) -> Backup:
    members = _members(data)
    if MANIFEST not in members or STATE not in members:
        raise BackupError("The backup has no manifest or no state.")
    try:
        manifest = json.loads(members[MANIFEST])
    except ValueError as e:
        raise BackupError(f"The backup's manifest is unreadable: {e}")
    fmt = manifest.get("format")
    if not isinstance(fmt, int) or fmt > FORMAT:
        raise BackupError(
            f"This backup is format {fmt}, and this Whistler reads up to "
            f"format {FORMAT}. Restore it with the Whistler version that made "
            f"it ({manifest.get('whistlerVersion', 'unknown')}) or newer.")
    for name, want in (manifest.get("sha256") or {}).items():
        if name not in members:
            raise BackupError(f"The backup is missing {name}.")
        if hashlib.sha256(members[name]).hexdigest() != want:
            raise BackupError(f"{name} does not match its checksum; the "
                              f"backup is damaged.")
    for name in members:
        if name != MANIFEST and name not in (manifest.get("sha256") or {}):
            raise BackupError(f"{name} is not listed in the manifest.")
    objects = load(members[STATE])
    mode = manifest.get("secrets", "none")
    secrets: Optional[List[Dict[str, Any]]] = []
    if SECRETS_PLAIN in members:
        secrets = load(members[SECRETS_PLAIN])
    elif SECRETS_ENCRYPTED in members:
        secrets = (load(decrypt(members[SECRETS_ENCRYPTED], passphrase))
                   if passphrase else None)
    return Backup(manifest, objects, secrets, mode)


def read_manifest(data: bytes) -> Dict[str, Any]:
    """Only the manifest, with the same untrusted-input checks as ``read``
    but without parsing the objects or touching the secrets."""
    members = _members(data)
    if MANIFEST not in members:
        raise BackupError("The backup has no manifest.")
    try:
        manifest = json.loads(members[MANIFEST])
    except ValueError as e:
        raise BackupError(f"The backup's manifest is unreadable: {e}")
    if not isinstance(manifest, dict):
        raise BackupError("The backup's manifest is not an object.")
    return manifest


def filename(manifest: Dict[str, Any]) -> str:
    """``whistler-backup-<UTC timestamp>-<trigger>.tar.gz``"""
    stamp = (manifest.get("createdAt") or "").replace("-", "").replace(
        ":", "").replace("T", "-").rstrip("Z")
    return f"whistler-backup-{stamp}-{manifest.get('trigger', 'manual')}.tar.gz"
