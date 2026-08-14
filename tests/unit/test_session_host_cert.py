"""Session host certificates cover every name Whistler dials (config.py).

The regression: `resolve_ssh_target` handed the relay the per-session
Service's FQDN while `session_ssh_principals` only issued the bare Service
name, and asyncssh validates a host certificate against *the name it
connected to*. So the TUI's connect failed host verification against a
perfectly valid, unexpired, correctly-signed certificate — and said "could
not open a session", which reads like a missing sshd.

These assert against `cert.validate(CERT_TYPE_HOST, host)`, the exact call
asyncssh's `_validate_openssh_host_certificate` makes, rather than a
list-membership check that would pass on a lookalike.
"""
import asyncssh
import pytest
from asyncssh.public_key import CERT_TYPE_HOST

from whistler import hostca
from whistler.config import KubeConfigManager


def _manager(suffix=".w"):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.ssh_domain_suffix = suffix
    return cm


def _cert_for(cm, username, session):
    _key, cert_line, _valid_before = hostca.issue_host_cert(
        ca_private_key=hostca.generate_ca_key(),
        principals=cm.session_ssh_principals(username, session),
        key_id=f"{username}-{session}",
    )
    return asyncssh.import_certificate(cert_line)


def test_certificate_covers_the_address_the_relay_dials():
    cm = _manager()
    host = cm.session_service_host("alice", "box")
    assert host == "alice-box.whistler-user-alice.svc.cluster.local"
    # No exception == asyncssh would accept it.
    _cert_for(cm, "alice", "box").validate(CERT_TYPE_HOST, host)


@pytest.mark.parametrize("dialled", [
    "box.w",                                            # what a user types
    "box",                                              # suffix-less
    "alice-box",                                        # bare Service name
    "alice-box.whistler-user-alice",                    # search-path forms
    "alice-box.whistler-user-alice.svc",
    "alice-box.whistler-user-alice.svc.cluster.local",
])
def test_certificate_covers_every_reachable_name(dialled):
    cm = _manager()
    _cert_for(cm, "alice", "box").validate(CERT_TYPE_HOST, dialled)


def test_certificate_still_refuses_a_name_it_was_not_issued_for():
    """Widening the principals must not have widened them to everything: the
    certificate is what stops one session answering for another."""
    cm = _manager()
    cert = _cert_for(cm, "alice", "box")
    for foreign in ("alice-other.whistler-user-alice.svc.cluster.local",
                    "alice-box.whistler-user-bob.svc.cluster.local",
                    "evil.example.com"):
        with pytest.raises(ValueError):
            cert.validate(CERT_TYPE_HOST, foreign)
