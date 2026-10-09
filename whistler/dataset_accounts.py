"""Per-user dataset keys: the pure half (design/storage.md, phase 2).

A managed dataset's VersityGW holds one account per (user, mode) the access
matrix grants — ``ro.alice``, ``rw.alice`` — and nothing else a guest can
use. The root account never leaves the pod. Everything here is a pure
function of the matrix and the stored keys; config.py does the API calls.

What shaped it, all measured against VersityGW v1.7.0:

- A ``user``-role account is refused a bucket it does not own, so each one
  is granted by a **bucket policy**, and only its mode's actions. ``admin``
  role would need no policy, but it ignores even an explicit Deny and can
  write a policy with Principal ``"*"`` — which VersityGW reads as
  *anonymous*. That is not a role to hand a guest.
- A policy's principals must exist in the account store of the process that
  writes it, and only the rw process can write one (the ro process runs
  ``--readonly``). So both processes share one account store, and the policy
  — an xattr on the shared bucket directory — is enforced by both.
- The bucket is named after the dataset, not a fixed ``data``: a policy may
  name only the bucket it is set on (no wildcard, no second name), and the
  external listener's clients sign the path ``/<dataset>/…`` that an Ingress
  routes on, so one bucket name has to serve every listener.
- The account store is a plain ``users.json``. The operator renders it into
  a Secret that seeds the pod at start, so a restart loses no account; live
  changes go through the admin API (``PATCH /create-user`` etc.).
"""
import datetime
import hashlib
import hmac
import json
import urllib.parse
from typing import Dict, Iterable, Optional, Tuple
from xml.etree import ElementTree
from xml.sax.saxutils import escape

MODES = ("ro", "rw")
# Accounts of the external listener (design/storage.md, "External access"):
# granted by the matrix's reserved `external` zone, held in the external
# process's own account store so an internal key opens nothing outside.
EXTERNAL_MODES = ("xro", "xrw")
ALL_MODES = MODES + EXTERNAL_MODES

# What each mode's accounts may do to the bucket and its objects. Never
# s3:*: that would include PutBucketPolicy/PutBucketAcl, and with them a way
# to make the bucket public. Rename is CopyObject (GetObject + PutObject) +
# DeleteObject; GetBucketVersioning only silences a 403 rclone logs on purge.
READ_ACTIONS = ("s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation",
                "s3:GetBucketVersioning")
WRITE_ACTIONS = READ_ACTIONS + (
    "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload",
    "s3:ListMultipartUploadParts", "s3:ListBucketMultipartUploads")


def account_name(username: str, mode: str) -> str:
    """``<mode>.<user>``. A username is a DNS label, so it has no dot, and
    the name cannot collide across users or modes."""
    return f"{mode}.{username}"


def parse_account_name(access: str) -> Optional[Tuple[str, str]]:
    """``(username, mode)`` for one of ours, else None."""
    mode, _, username = access.partition(".")
    return (username, mode) if mode in ALL_MODES and username else None


def is_external(access: str) -> bool:
    return (parse_account_name(access) or ("", ""))[1] in EXTERNAL_MODES


def external_mode(mode: str) -> str:
    """The external account mode for an internal one: ro -> xro."""
    return f"x{mode}"


def desired_accounts(holders: Dict[str, Iterable[str]]) -> Dict[str, Tuple[str, str]]:
    """``{access: (username, mode)}`` from ``{mode: usernames}``."""
    return {account_name(u, mode): (u, mode)
            for mode in ALL_MODES for u in (holders.get(mode) or ())}


def render_users_json(keys: Dict[str, str]) -> str:
    """VersityGW's internal-IAM file (``<iam-dir>/users.json``) holding
    exactly these accounts, all role ``user``. Sorted, so an unchanged key
    set renders byte-identical and the Secret is not rewritten."""
    return json.dumps({"accessAccounts": {
        access: {"access": access, "secret": secret, "role": "user",
                 "userID": 0, "groupID": 0, "projectID": 0}
        for access, secret in sorted(keys.items())}}, sort_keys=True)


def build_policy(bucket: str, accounts: Iterable[str]) -> Optional[dict]:
    """The bucket policy granting each account its mode's actions, or None
    when there is no account (a policy with no principal is invalid; the
    caller deletes the policy instead)."""
    by_mode = {mode: sorted(a for a in accounts
                            if (parse_account_name(a) or ("", ""))[1] == mode)
               for mode in ALL_MODES}
    resources = [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]
    statements = [
        {"Effect": "Allow", "Principal": {"AWS": by_mode[mode]},
         "Action": list(READ_ACTIONS if mode in ("ro", "xro")
                        else WRITE_ACTIONS),
         "Resource": resources}
        for mode in ALL_MODES if by_mode[mode]]
    return {"Version": "2012-10-17", "Statement": statements} \
        if statements else None


def account_xml(access: str, secret: str) -> bytes:
    return (f"<Account><Access>{escape(access)}</Access>"
            f"<Secret>{escape(secret)}</Secret><Role>user</Role>"
            f"<UserID>0</UserID><GroupID>0</GroupID><ProjectID>0</ProjectID>"
            f"</Account>").encode()


def parse_list_users(body: bytes) -> Dict[str, str]:
    """``{access: secret}`` from ``PATCH /list-users``."""
    root = ElementTree.fromstring(body)
    out = {}
    for el in root.iter():
        if el.tag.rsplit("}", 1)[-1] != "Accounts":
            continue
        fields = {c.tag.rsplit("}", 1)[-1]: (c.text or "") for c in el}
        if fields.get("Access"):
            out[fields["Access"]] = fields.get("Secret", "")
    return out


# --- AWS Signature Version 4 ------------------------------------------------- #
#
# Both the S3 API and VersityGW's admin API take SigV4. ~40 lines of stdlib
# rather than botocore, for the same reason totp.py is: it is a published
# algorithm, and the tests check it against AWS's own worked example.

def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _quote(s: str, safe: str = "-_.~") -> str:
    return urllib.parse.quote(s, safe=safe)


def sign_v4(method: str, url: str, access: str, secret: str, *,
            body: bytes = b"", headers: Dict[str, str] = None,
            region: str = "us-east-1", service: str = "s3",
            now: datetime.datetime = None) -> Dict[str, str]:
    """The headers to send with this request: the caller's, plus Host,
    X-Amz-Date, X-Amz-Content-Sha256 and Authorization. Every header passed
    in is signed."""
    parts = urllib.parse.urlsplit(url)
    now = now or datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = amz_date[:8]
    payload = hashlib.sha256(body).hexdigest()
    out = {"Host": parts.netloc, "X-Amz-Date": amz_date,
           "X-Amz-Content-Sha256": payload, **(headers or {})}
    canonical_headers = {k.lower().strip(): " ".join(str(v).split())
                         for k, v in out.items()}
    signed = ";".join(sorted(canonical_headers))
    query = sorted(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    canonical = "\n".join([
        method.upper(),
        _quote(urllib.parse.unquote(parts.path) or "/", safe="/-_.~"),
        "&".join(f"{_quote(k)}={_quote(v)}" for k, v in query),
        "".join(f"{k}:{canonical_headers[k]}\n" for k in sorted(canonical_headers)),
        signed,
        payload,
    ])
    scope = f"{day}/{region}/{service}/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope,
                         hashlib.sha256(canonical.encode()).hexdigest()])
    key = _hmac(_hmac(_hmac(_hmac(f"AWS4{secret}".encode(), day), region),
                      service), "aws4_request")
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    out["Authorization"] = (f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
                            f"SignedHeaders={signed}, Signature={signature}")
    return out
