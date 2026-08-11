"""SSH host certificate authority (whistler/hostca.py).

The load-bearing test here is test_client_validates_certificate_against_ca: it
runs a real asyncssh handshake with a real client doing real `@cert-authority`
validation, because "we produced a certificate-shaped blob" and "OpenSSH
accepts this host without a TOFU prompt" are different claims and only the
second one is the feature.
"""
import asyncio
import socket

import asyncssh
import pytest

from whistler import hostca


def _ca():
    return hostca.generate_ca_key()


def test_ca_public_key_is_an_authorized_keys_line():
    pub = hostca.ca_public_key(_ca())
    assert pub.startswith("ssh-ed25519 ")
    assert "\n" not in pub
    # Importable as a key, which is what a client's known_hosts parser does.
    assert asyncssh.import_public_key(pub) is not None


def test_known_hosts_line_shape():
    line = hostca.known_hosts_line(hostca.ca_public_key(_ca()), "*.w")
    marker, pattern, algo, _blob = line.split(" ", 3)
    assert marker == "@cert-authority"
    assert pattern == "*.w"
    assert algo == "ssh-ed25519"


def test_session_principals_covers_dialled_and_internal_names():
    principals = hostca.session_principals("box", ".w", extra=["alice-box"])
    # The suffixed form first: it is the name a client actually verifies.
    assert principals == ["box.w", "box", "alice-box"]


def test_session_principals_deduplicates():
    # A suffix-less deployment must not emit the same principal twice.
    assert hostca.session_principals("box", "", extra=["box"]) == ["box"]


def test_issue_host_cert_carries_principals_and_key_id():
    ca = _ca()
    key, cert, valid_before = hostca.issue_host_cert(
        ca_private_key=ca, principals=["box.w", "box"], key_id="alice-box")
    assert hostca.cert_principals(cert) == ["box.w", "box"]
    assert cert.startswith("ssh-ed25519-cert-v01@openssh.com ")
    assert asyncssh.import_private_key(key) is not None
    assert valid_before > 0


def test_reissue_keeps_the_host_key():
    """Renewal must not change the guest's identity — only the certificate
    over it. A new key on every reconcile is exactly the churn the CA exists
    to end."""
    ca = _ca()
    key1, cert1, _ = hostca.issue_host_cert(
        ca_private_key=ca, principals=["box.w"], key_id="alice-box")
    key2, cert2, _ = hostca.issue_host_cert(
        ca_private_key=ca, principals=["box.w", "extra"], key_id="alice-box",
        host_private_key=key1)
    # Compare public halves: OpenSSH private-key encoding carries a random
    # check value, so identical keys do not round-trip to identical bytes.
    assert (asyncssh.import_private_key(key2).export_public_key()
            == asyncssh.import_private_key(key1).export_public_key())
    assert cert2 != cert1
    assert set(hostca.cert_principals(cert2)) == {"box.w", "extra"}


def test_needs_reissue_on_missing_or_unreadable():
    assert hostca.needs_reissue(None, ["box.w"], 2 ** 40) is True
    assert hostca.needs_reissue("gibberish", ["box.w"], 2 ** 40) is True


def test_needs_reissue_when_principals_change():
    _key, cert, valid_before = hostca.issue_host_cert(
        ca_private_key=_ca(), principals=["box.w"], key_id="alice-box")
    assert hostca.needs_reissue(cert, ["box.w"], valid_before) is False
    # Renaming the suffix (or adding a project alias) must re-issue, or the
    # client verifies a name the certificate does not carry.
    assert hostca.needs_reissue(cert, ["box.internal"], valid_before) is True


def test_needs_reissue_inside_the_renewal_window():
    _key, cert, valid_before = hostca.issue_host_cert(
        ca_private_key=_ca(), principals=["box.w"], key_id="alice-box",
        validity_seconds=hostca.RENEW_BEFORE_SECONDS // 2)
    assert hostca.needs_reissue(cert, ["box.w"], valid_before) is True


def test_needs_reissue_on_garbage_expiry():
    """A Secret whose annotation was lost or mangled must fail toward
    re-issuing rather than serving an unbounded certificate."""
    _key, cert, _ = hostca.issue_host_cert(
        ca_private_key=_ca(), principals=["box.w"], key_id="alice-box")
    assert hostca.needs_reissue(cert, ["box.w"], "not-a-number") is True
    assert hostca.needs_reissue(cert, ["box.w"], None) is True


class _EchoServer(asyncssh.SSHServer):
    def begin_auth(self, username):
        return False  # no auth: this test is about *host* verification


async def _serve(host_key, host_cert):
    return await asyncssh.create_server(
        _EchoServer, "127.0.0.1", 0,
        server_host_keys=[(asyncssh.import_private_key(host_key),
                           asyncssh.import_certificate(host_cert))])


async def _try_connect(port, dialled_name, ca_pub):
    """Handshake against 127.0.0.1:port while telling the client it dialled
    ``dialled_name`` — an explicit socket, because the whole question is
    whether the certificate covers the *name*, not the address. Returns True
    if the host verified."""
    sock = socket.create_connection(("127.0.0.1", port))
    try:
        # known_hosts as (host_keys, ca_keys, revoked_keys): no host keys at
        # all, so acceptance can only come from the CA signature.
        conn = await asyncssh.connect(
            host=dialled_name, sock=sock, username="whoever",
            known_hosts=([], [ca_pub], []))
        conn.close()
        await conn.wait_closed()
        return True
    except asyncssh.HostKeyNotVerifiable:
        return False


@pytest.mark.parametrize("dialled,expect_ok", [
    ("box.w", True),      # the name the certificate was issued for
    ("other.w", False),   # a different instance's name
])
def test_client_validates_certificate_against_ca(dialled, expect_ok):
    """The whole point of the CA: a client that trusts only the CA key
    connects to an instance it has never seen, with no TOFU prompt and no
    per-host entry — and refuses a certificate whose principals don't cover
    the name it dialled."""
    async def run():
        ca = _ca()
        host_key, host_cert, _ = hostca.issue_host_cert(
            ca_private_key=ca, principals=["box.w"], key_id="alice-box")
        server = await _serve(host_key, host_cert)
        try:
            return await _try_connect(
                server.sockets[0].getsockname()[1], dialled,
                asyncssh.import_public_key(hostca.ca_public_key(ca)))
        finally:
            server.close()

    assert asyncio.run(run()) is expect_ok


def test_unrelated_ca_is_rejected():
    """A certificate from another CA must not validate — otherwise the
    `@cert-authority` line would be decoration."""
    async def run():
        host_key, host_cert, _ = hostca.issue_host_cert(
            ca_private_key=_ca(), principals=["box.w"], key_id="alice-box")
        server = await _serve(host_key, host_cert)
        try:
            return await _try_connect(
                server.sockets[0].getsockname()[1], "box.w",
                asyncssh.import_public_key(hostca.ca_public_key(_ca())))
        finally:
            server.close()

    assert asyncio.run(run()) is False
