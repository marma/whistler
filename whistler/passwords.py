"""Portal and kiosk passwords: hashing, temporary passwords, and their records.

Pure functions here; where a record is kept is KubeConfigManager's business
(``get_password_record`` / ``set_password``). A record lives in a Secret per
user in the release namespace, ``whistler-cred-<user>``, not on the User CR:
User CRs are read by every component, printed by ``kubectl get usr -o yaml``
and exported to backups in the clear, while Secrets are the kind Kubernetes
(and the backup's passphrase) treats as sensitive.

The flow: an account is given a **temporary** password (portal New user or
Reset password, the operator for the bootstrap admin, or the CLI below). It is
good for ``TEMPORARY_LIFETIME`` and only for one thing: choosing a real
password, which the sign-in asks for before any session is issued. While the
dev gate (``WHISTLER_AUTH_ALLOW_ANY``) is open none of this is consulted.

Hashing is scrypt from the standard library, the same no-dependency choice
as totp.py. SSH stays key-only; these passwords open the portal and kiosk.

Break-glass, from inside the operator pod::

    python -m whistler.passwords reset <user>
"""
import base64
import hashlib
import hmac
import secrets
import time
from typing import Any, Dict, Optional

TEMPORARY_LIFETIME = 7 * 24 * 3600
MIN_LENGTH = 10

SECRET_PREFIX = "whistler-cred-"
SECRET_LABEL = "whistler.martinmalmsten.net/credential"
SECRET_USER_LABEL = "whistler.martinmalmsten.net/user"

# scrypt cost: ~16 MiB and well under 100 ms per check on a server core.
_N, _R, _P = 2 ** 14, 8, 1

# Temporary passwords are read off a screen and typed, so no 0/O/1/l/I.
_ALPHABET = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

OK = "ok"
CHANGE = "change"          # right temporary password: choose a real one first
REFUSED = "refused"


def secret_name(username: str) -> str:
    return f"{SECRET_PREFIX}{username}"


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=_N, r=_R, p=_P,
                            dklen=32)
    return "$".join(["scrypt", str(_N), str(_R), str(_P),
                     base64.b64encode(salt).decode(),
                     base64.b64encode(digest).decode()])


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, n, r, p, salt, digest = encoded.split("$")
        if scheme != "scrypt":
            return False
        expected = base64.b64decode(digest)
        actual = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt),
                                n=int(n), r=int(r), p=int(p),
                                dklen=len(expected))
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(actual, expected)


def generate_temporary() -> str:
    """Four groups of four, e.g. ``xK7m-9pQr-T2wz-hN4c`` (~94 bits)."""
    return "-".join("".join(secrets.choice(_ALPHABET) for _ in range(4))
                    for _ in range(4))


def new_record(password: str, temporary: bool,
               now: Optional[float] = None) -> Dict[str, Any]:
    now = time.time() if now is None else now
    return {"hash": hash_password(password), "temporary": temporary,
            "setAt": int(now),
            "expiresAt": int(now + TEMPORARY_LIFETIME) if temporary else None}


def check(record: Optional[Dict[str, Any]], password: str,
          now: Optional[float] = None) -> str:
    """OK, CHANGE or REFUSED. An expired temporary password is REFUSED like
    a wrong one: the remedy is a reset, which only an admin can do."""
    if not record or not password or not record.get("hash"):
        return REFUSED
    now = time.time() if now is None else now
    if record.get("temporary") and (record.get("expiresAt") or 0) <= now:
        return REFUSED
    if not verify_password(password, record["hash"]):
        return REFUSED
    return CHANGE if record.get("temporary") else OK


def status(record: Optional[Dict[str, Any]],
           now: Optional[float] = None) -> str:
    """What the admin pages show: none, temporary, expired or set."""
    if not record or not record.get("hash"):
        return "none"
    if record.get("temporary"):
        now = time.time() if now is None else now
        return "expired" if (record.get("expiresAt") or 0) <= now else "temporary"
    return "set"


def new_password_problem(username: str, password: str, confirm: str,
                         current: Optional[str] = None) -> Optional[str]:
    """Why a chosen password is refused, or None."""
    if password != confirm:
        return "The two passwords do not match."
    if len(password) < MIN_LENGTH:
        return f"Use at least {MIN_LENGTH} characters."
    if password.strip().lower() == username.lower():
        return "The password cannot be the user name."
    if current is not None and password == current:
        return "Choose a password different from the current one."
    return None


def to_secret_data(record: Dict[str, Any]) -> Dict[str, str]:
    """Secret ``data`` (base64), one key per field."""
    fields = {"hash": record["hash"],
              "temporary": "true" if record.get("temporary") else "false",
              "setAt": str(record.get("setAt") or 0)}
    if record.get("expiresAt"):
        fields["expiresAt"] = str(record["expiresAt"])
    return {k: base64.b64encode(v.encode()).decode() for k, v in fields.items()}


def from_secret_data(data: Optional[Dict[str, str]]) -> Optional[Dict[str, Any]]:
    if not data or "hash" not in data:
        return None
    try:
        raw = {k: base64.b64decode(v).decode() for k, v in data.items()}
        return {"hash": raw["hash"],
                "temporary": raw.get("temporary") == "true",
                "setAt": int(raw.get("setAt") or 0),
                "expiresAt": int(raw["expiresAt"]) if raw.get("expiresAt") else None}
    except (ValueError, TypeError):
        return None


class Throttle:
    """Failed attempts per user name, in process memory: after FREE failures
    each further attempt waits twice as long as the last (capped at
    MAX_WAIT). Per portal process, which is why the chart runs one; a
    success clears it. Keyed on the name tried, so guessing at one account
    does not slow down another."""
    FREE = 5
    MAX_WAIT = 300

    def __init__(self):
        self._state: Dict[str, Any] = {}

    def wait(self, username: str, now: Optional[float] = None) -> float:
        """Seconds until this name may be tried again (0: now)."""
        now = time.time() if now is None else now
        _, until = self._state.get(username, (0, 0.0))
        return max(0.0, until - now)

    def failed(self, username: str, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        count, _ = self._state.get(username, (0, 0.0))
        count += 1
        until = 0.0
        if count > self.FREE:
            until = now + min(2 ** (count - self.FREE), self.MAX_WAIT)
        self._state[username] = (count, until)

    def succeeded(self, username: str) -> None:
        self._state.pop(username, None)


def main(argv=None) -> int:
    import argparse
    from whistler.config import KubeConfigManager

    parser = argparse.ArgumentParser(prog="python -m whistler.passwords")
    sub = parser.add_subparsers(dest="command", required=True)
    reset = sub.add_parser("reset", help="give a user a new temporary password")
    reset.add_argument("username")
    reset.add_argument("--kubeconfig")
    args = parser.parse_args(argv)

    cm = KubeConfigManager(kubeconfig=args.kubeconfig)
    if not cm.user_exists(args.username):
        print(f"No such user: {args.username}")
        return 1
    password, expires = cm.issue_temporary_password(args.username)
    print(f"Temporary password for {args.username}: {password}")
    print(f"Valid until {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(expires))}; "
          "it must be changed at first sign-in.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
