"""Gateway-mediated SSH relay (whistler/relay.py).

Driven against a real asyncssh server rather than mocks: the relay's whole job
is protocol plumbing — PTY requests, window-change, exit status, host
verification — and a mock would assert that we called methods, not that the
two ends actually talk.
"""
import asyncio

import asyncssh
import pytest

from whistler import hostca, relay


class _Chan:
    """Stands in for the user's SSH channel: collects what the relay pumps
    toward the client."""

    def __init__(self):
        self.out = bytearray()
        self.err = bytearray()

    def write(self, data):
        self.out.extend(data)

    def write_stderr(self, data):
        self.err.extend(data)


class _Server(asyncssh.SSHServer):
    def begin_auth(self, username):
        return False   # authorisation is not what these tests are about


def _handle(process):
    """A tiny remote 'shell': echoes what it is asked, reports the terminal
    size it was given, and exits with a requested status."""
    command = process.command or "shell"
    if command == "size":
        width, height, _, _ = process.get_terminal_size()
        process.stdout.write(f"{width}x{height}\n")
    elif command == "term":
        process.stdout.write(f"{process.get_terminal_type()}\n")
    elif command == "stderr":
        process.stderr.write("to-stderr\n")
    elif command.startswith("exit"):
        process.exit(int(command.split()[1]))
        return
    else:
        process.stdout.write("hello\n")
    process.exit(0)


async def _serve(host_key=None, host_cert=None):
    keys = ([(asyncssh.import_private_key(host_key),
              asyncssh.import_certificate(host_cert))] if host_cert
            else [asyncssh.generate_private_key("ssh-ed25519")])
    return await asyncssh.create_server(
        _Server, "127.0.0.1", 0, server_host_keys=keys, process_factory=_handle)


def _client_key():
    return asyncssh.generate_private_key("ssh-ed25519").export_private_key()


def _run(coro):
    return asyncio.run(coro)


# --------------------------------------------------------------------------- #
# Bridging                                                                     #
# --------------------------------------------------------------------------- #

def test_relays_output_and_exit_status():
    async def go():
        server = await _serve()
        chan = _Chan()
        try:
            r = await relay.open_relay(
                chan=chan, host="127.0.0.1",
                port=server.sockets[0].getsockname()[1],
                username="alice", private_key=_client_key(), command="echo")
            status = await r.wait_closed()
            return bytes(chan.out), status
        finally:
            server.close()

    out, status = _run(go())
    assert out == b"hello\n"
    assert status == 0


def test_exit_status_is_propagated():
    async def go():
        server = await _serve()
        try:
            r = await relay.open_relay(
                chan=_Chan(), host="127.0.0.1",
                port=server.sockets[0].getsockname()[1],
                username="alice", private_key=_client_key(), command="exit 42")
            return await r.wait_closed()
        finally:
            server.close()

    # A relayed command that fails must fail for the user too, or scripting
    # over the relay silently succeeds.
    assert _run(go()) == 42


def test_stderr_stays_separate_without_a_pty():
    async def go():
        server = await _serve()
        chan = _Chan()
        try:
            r = await relay.open_relay(
                chan=chan, host="127.0.0.1",
                port=server.sockets[0].getsockname()[1],
                username="alice", private_key=_client_key(), command="stderr")
            await r.wait_closed()
            return bytes(chan.out), bytes(chan.err)
        finally:
            server.close()

    out, err = _run(go())
    assert out == b""
    assert err == b"to-stderr\n"


def test_pty_is_requested_with_the_users_terminal():
    async def go():
        server = await _serve()
        chan = _Chan()
        try:
            r = await relay.open_relay(
                chan=chan, host="127.0.0.1",
                port=server.sockets[0].getsockname()[1],
                username="alice", private_key=_client_key(),
                term_type="xterm-256color", term_size=(120, 40), command="term")
            await r.wait_closed()
            return bytes(chan.out)
        finally:
            server.close()

    assert _run(go()) == b"xterm-256color\r\n"   # CRLF: it is a PTY


def test_terminal_size_reaches_the_remote_session():
    async def go():
        server = await _serve()
        chan = _Chan()
        try:
            r = await relay.open_relay(
                chan=chan, host="127.0.0.1",
                port=server.sockets[0].getsockname()[1],
                username="alice", private_key=_client_key(),
                term_type="xterm", term_size=(100, 30), command="size")
            await r.wait_closed()
            return bytes(chan.out)
        finally:
            server.close()

    assert _run(go()) == b"100x30\r\n"


# --------------------------------------------------------------------------- #
# Host verification against the CA                                             #
# --------------------------------------------------------------------------- #

def test_known_hosts_for_accepts_only_the_ca():
    ca_pub = hostca.ca_public_key(hostca.generate_ca_key())
    host_keys, ca_keys, revoked = relay.known_hosts_for(ca_pub)
    # No host keys at all: acceptance can only come from a CA signature.
    assert host_keys == []
    assert len(ca_keys) == 1
    assert revoked == []


def test_no_ca_means_no_verification():
    """The pre-CA status quo, and it must stay reachable — but it is a
    downgrade, so the caller gets asyncssh's 'accept anything' explicitly
    rather than by accident."""
    assert relay.known_hosts_for(None) is None


def test_relay_verifies_the_host_certificate():
    async def go():
        ca = hostca.generate_ca_key()
        host_key, host_cert, _ = hostca.issue_host_cert(
            ca_private_key=ca, principals=["127.0.0.1"], key_id="alice-box")
        server = await _serve(host_key, host_cert)
        try:
            r = await relay.open_relay(
                chan=_Chan(), host="127.0.0.1",
                port=server.sockets[0].getsockname()[1],
                username="alice", private_key=_client_key(),
                ca_public_key=hostca.ca_public_key(ca), command="echo")
            await r.wait_closed()
            return True
        finally:
            server.close()

    assert _run(go()) is True


def test_relay_refuses_a_host_without_a_whistler_certificate():
    """After a CA rollout this is a real signal — someone else answering on
    that address — not a transient failure, so it gets its own message."""
    async def go():
        ca = hostca.generate_ca_key()
        server = await _serve()   # plain host key, no certificate
        try:
            await relay.open_relay(
                chan=_Chan(), host="127.0.0.1",
                port=server.sockets[0].getsockname()[1],
                username="alice", private_key=_client_key(),
                ca_public_key=hostca.ca_public_key(ca), command="echo")
        finally:
            server.close()

    with pytest.raises(relay.RelayError) as e:
        _run(go())
    assert "not signed by the Whistler CA" in str(e.value)


# --------------------------------------------------------------------------- #
# Failure modes reach the user as messages                                     #
# --------------------------------------------------------------------------- #

def test_missing_access_key_is_a_relay_error():
    with pytest.raises(relay.RelayError) as e:
        _run(relay.open_relay(chan=_Chan(), host="127.0.0.1", port=1,
                              username="alice", private_key=None))
    assert "No access key" in str(e.value)


def test_unusable_access_key_is_a_relay_error():
    with pytest.raises(relay.RelayError) as e:
        _run(relay.open_relay(chan=_Chan(), host="127.0.0.1", port=1,
                              username="alice", private_key=b"not-a-key"))
    assert "Unusable access key" in str(e.value)


def test_unreachable_host_is_a_relay_error():
    async def go():
        # Bind and immediately close to get a port nothing is listening on.
        server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        server.close()
        await server.wait_closed()
        await relay.open_relay(chan=_Chan(), host="127.0.0.1", port=port,
                               username="alice", private_key=_client_key())

    with pytest.raises(relay.RelayError) as e:
        _run(go())
    assert "Could not connect" in str(e.value)
