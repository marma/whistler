"""SSH host certificate authority for Whistler sessions.

Pure (no Kubernetes imports, no file I/O) so it is unit-testable without a
cluster, like ``cloudinit.build_user_data`` and
``KubeConfigManager._build_pod_spec``.

Why a CA at all. Every session gets its own sshd and therefore its own host
key, so without one a user meets a fresh TOFU prompt per instance — and an
ephemeral instance reusing a name a previous one had produces the
"REMOTE HOST IDENTIFICATION HAS CHANGED" wall of hashes rather than a prompt.
Neither is a security control: users click through both. Worse, the portal
connects to guests with ``known_hosts=None`` (screenshots, web terminal),
i.e. no host verification at all.

A CA collapses all of that into one line the user adds once::

    @cert-authority *.w ssh-ed25519 AAAA...

after which every present and future instance verifies, and the portal and
the relay can validate against the same CA instead of trusting whatever
answers.

Key type is ed25519 throughout: small, fast, no parameter choices to get
wrong, and supported by every OpenSSH since 6.5 (certificates since 5.4).

Validity is bounded but long (a year). A certificate is delivered by
cloud-init and therefore only changes when the guest reboots, so a short
lifetime would expire live sessions with no way to refresh them; the
expiry's job here is to bound the damage from a leaked host key over
Whistler's lifetime, not to be a revocation mechanism. Re-issue happens
during the renewal window on any reconcile, and reaches the guest on its
next boot -- the same change-on-reboot contract zones already have.
"""

import time

import asyncssh

CA_KEY_ALGORITHM = "ssh-ed25519"
HOST_KEY_ALGORITHM = "ssh-ed25519"

DEFAULT_VALIDITY_SECONDS = 365 * 24 * 3600
# Re-issue this long before expiry. Generous because delivery is
# boot-triggered: a guest that stays up for weeks still gets a valid
# certificate written into its next boot's cloud-init.
RENEW_BEFORE_SECONDS = 30 * 24 * 3600

# Where the guest keeps them. A Whistler-specific name rather than replacing
# ssh_host_ed25519_key: the image's own key stays valid, so a client that has
# not adopted the CA still connects (with a TOFU prompt) instead of failing.
GUEST_HOST_KEY_PATH = "/etc/ssh/ssh_host_whistler_ed25519_key"
GUEST_HOST_CERT_PATH = f"{GUEST_HOST_KEY_PATH}-cert.pub"


def generate_ca_key() -> bytes:
    """A new CA private key, in OpenSSH format."""
    return asyncssh.generate_private_key(CA_KEY_ALGORITHM).export_private_key()


def ca_public_key(ca_private_key: bytes) -> str:
    """The CA's public half as an authorized_keys-style one-liner."""
    key = asyncssh.import_private_key(ca_private_key)
    return key.export_public_key().decode().strip()


def known_hosts_line(ca_public: str, pattern: str = "*") -> str:
    """The single line a user adds to ``~/.ssh/known_hosts`` to trust every
    instance forever. ``pattern`` is a host pattern, normally the gateway's
    domain suffix as a wildcard (``*.w``)."""
    return f"@cert-authority {pattern} {ca_public.strip()}"


def session_principals(name: str, suffix: str = "", extra=()) -> list:
    """Host principals a session's certificate should carry.

    The client verifies the name *it dialled*, so the suffixed form is the
    one that matters (``ssh test.w`` checks ``test.w``). The bare name is
    included for anyone addressing the instance without the suffix, and
    ``extra`` carries the internal names (the full ``<user>-<session>`` CR
    name, cluster DNS) that Whistler's own components dial.
    """
    principals = []
    for candidate in [f"{name}{suffix}" if suffix else None, name, *extra]:
        if candidate and candidate not in principals:
            principals.append(candidate)
    return principals


def issue_host_cert(*, ca_private_key: bytes, principals, key_id: str,
                    host_private_key: bytes = None,
                    validity_seconds: int = DEFAULT_VALIDITY_SECONDS,
                    now: float = None):
    """Sign a host key with the CA.

    Returns ``(host_private_key_pem, cert_line, valid_before_epoch)``. A new
    host key is generated when ``host_private_key`` is None; passing the
    existing one re-signs in place, which is what renewal wants — the guest's
    key does not change, only the certificate over it.
    """
    now = time.time() if now is None else now
    ca = asyncssh.import_private_key(ca_private_key)
    if host_private_key:
        host_key = asyncssh.import_private_key(host_private_key)
    else:
        host_key = asyncssh.generate_private_key(HOST_KEY_ALGORITHM)

    # A minute of backdate absorbs clock skew between the operator and the
    # guest: a certificate that is not yet valid fails closed, and a guest
    # whose clock lags by seconds at first boot is entirely ordinary.
    valid_after = int(now) - 60
    valid_before = int(now) + int(validity_seconds)
    cert = ca.generate_host_certificate(
        host_key, key_id,
        principals=list(principals),
        valid_after=valid_after,
        valid_before=valid_before,
    )
    return (host_key.export_private_key(),
            cert.export_certificate().decode().strip(),
            valid_before)


def cert_principals(cert_line) -> list:
    """Principals carried by an issued certificate. Raises on garbage, which
    callers treat as "re-issue"."""
    return list(asyncssh.import_certificate(cert_line).principals)


def needs_reissue(cert_line, principals, valid_before, now: float = None,
                  renew_before: int = RENEW_BEFORE_SECONDS) -> bool:
    """Whether a stored certificate should be replaced — because it is
    unreadable, no longer covers the names the session answers to, or is
    inside its renewal window.

    ``valid_before`` is passed in rather than read back off the certificate:
    asyncssh exposes it only as a private attribute, and depending on that
    would make a library upgrade a silent expiry bug.
    """
    if not cert_line:
        return True
    try:
        current = set(cert_principals(cert_line))
    except (asyncssh.KeyImportError, ValueError):
        return True
    if current != set(principals):
        return True
    now = time.time() if now is None else now
    try:
        return now + renew_before >= float(valid_before)
    except (TypeError, ValueError):
        return True
