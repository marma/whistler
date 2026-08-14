"""Gateway-mediated SSH relay: bridging a user's session channel to a
session's own sshd (design/proxyjump.md, "Session handover").

Two ways into an instance, and they are complementary rather than
alternatives:

- **ProxyJump** (server.py, `_jump_to_instance`) splices encrypted bytes. The
  crypto terminates in the guest and the gateway sees nothing. That is the
  path for tooling — scp, rsync, VS Code Remote-SSH — and for purists.
- **The relay**, here. The gateway is an SSH *client* toward the instance and
  pumps bytes between two connections. The gateway sees plaintext, because it
  is a participant rather than a pipe.

The relay exists because the jump is negotiated by the *client* at connect
time: a user already sitting in the TUI cannot be turned into a jump, so
"connect" in the TUI has to be relayed. It replaces the `kubectl exec` bridge
it grew out of, and the swap is a straight win — the exec bridge needed a
pty, a subprocess, fd readers, an injected static socat for port forwards and
a hand-rolled agent bridge; this needs an SSH client. It is also uniform:
pods and VMs are the same code once both run sshd.

On plaintext: the exec bridge had exactly the same property (the gateway ran
`kubectl exec` and saw every byte), so this is not a regression. It is worth
stating plainly rather than leaving implicit, because the jump path's
end-to-end guarantee makes it easy to assume this one shares it.

Host verification is against the Whistler CA (hostca.py), not
``known_hosts=None``. The portal still connects without verification for
screenshots and the web terminal; this module is the pattern that should
replace it.
"""

import asyncio
import logging

import asyncssh

logger = logging.getLogger("whistler.relay")

CONNECT_TIMEOUT = 15


class RelayError(Exception):
    """The relay could not be established. Carries a message meant for the
    user's terminal, not a stack trace."""


class _RelaySession(asyncssh.SSHClientSession):
    """The remote half. Everything it receives goes to the user's channel."""

    def __init__(self, chan, on_exit):
        self._chan = chan
        self._on_exit = on_exit
        self.exit_status = None

    def data_received(self, data, datatype):
        # stderr on a PTY session arrives as datatype None anyway; when there
        # is no PTY, keep the streams separate so a caller redirecting stdout
        # still sees stderr.
        if datatype == asyncssh.EXTENDED_DATA_STDERR:
            self._chan.write_stderr(data)
        else:
            self._chan.write(data)

    def exit_status_received(self, status):
        self.exit_status = status

    def connection_lost(self, exc):
        if exc:
            logger.debug(f"Relay remote session lost: {exc}")
        if self._on_exit and not self._on_exit.done():
            self._on_exit.set_result(self.exit_status)


class Relay:
    """A live relay. The caller feeds it the user's input and lifecycle events
    and awaits :meth:`wait_closed` for the remote exit status."""

    def __init__(self, conn, chan, session, closed):
        self._conn = conn
        self._chan = chan
        self._session = session
        self._closed = closed

    def write(self, data):
        if not self._chan.is_closing():
            self._chan.write(data)

    def write_eof(self):
        try:
            self._chan.write_eof()
        except OSError:
            pass

    def change_terminal_size(self, width, height):
        try:
            self._chan.change_terminal_size(width, height)
        except OSError:
            pass

    def send_signal(self, signal):
        try:
            self._chan.send_signal(signal)
        except (OSError, ValueError):
            pass

    def send_break(self, msec):
        try:
            self._chan.send_break(msec)
        except OSError:
            pass

    async def wait_closed(self):
        """The remote command's exit status, or None if it never sent one."""
        try:
            return await self._closed
        finally:
            self.close()

    def close(self):
        if not self._chan.is_closing():
            self._chan.close()
        self._conn.close()


def known_hosts_for(ca_public_key):
    """asyncssh ``known_hosts`` accepting exactly the Whistler CA.

    The tuple form is (host_keys, ca_keys, revoked_keys): no host keys at all,
    so acceptance can only come from a certificate this CA signed. Returns
    None — asyncssh's "accept anything" — when no CA is configured, which is
    the pre-CA status quo and is logged as the downgrade it is.
    """
    if not ca_public_key:
        logger.warning("No SSH host CA available: relaying without host "
                       "verification (see design/proxyjump.md)")
        return None
    return ([], [asyncssh.import_public_key(ca_public_key)], [])


async def open_relay(*, chan, host, port, username, private_key,
                     ca_public_key=None, term_type=None, term_size=None,
                     command=None, agent_path=None,
                     connect_timeout=CONNECT_TIMEOUT):
    """Connect to a session's sshd and bridge it to ``chan``.

    ``private_key`` is the per-user access key. It has to be: the gateway
    never holds the user's own private key, so a relayed session is
    authenticated as Whistler, acting for that user — which is why the guest
    must carry that user's access key in *their* authorized_keys, and why on
    a shared instance one drop-in per member is load-bearing
    (design/security.md).

    ``agent_path`` forwards the user's own agent onward, so a relayed shell
    can still `git push`. That is the whole of what the old `_bridge_agent`
    plus an injected socat did.
    """
    if not private_key:
        raise RelayError("No access key for this user; cannot relay.")

    try:
        key = asyncssh.import_private_key(private_key)
    except (asyncssh.KeyImportError, ValueError) as e:
        raise RelayError(f"Unusable access key: {e}") from e

    try:
        conn = await asyncio.wait_for(
            asyncssh.connect(
                host, port=port, username=username, client_keys=[key],
                known_hosts=known_hosts_for(ca_public_key),
                # asyncssh reads ~/.ssh/config by default. Nothing should be
                # able to redirect the gateway's own connections by dropping a
                # file in its home — and a `Host *` stanza there would apply
                # to every session Whistler relays into.
                config=None,
                agent_path=agent_path,
                agent_forwarding=bool(agent_path),
            ),
            connect_timeout)
    except asyncio.TimeoutError:
        raise RelayError(f"Timed out connecting to {host}:{port}") from None
    except (asyncssh.HostKeyNotVerifiable, asyncssh.KeyExchangeFailed) as e:
        # Worth its own message: the guest presented no valid Whistler
        # certificate, which after a CA rollout is a real signal rather than a
        # transient failure. Two exception types because asyncssh reports it
        # either way depending on how far it got — pinning known_hosts to CA
        # keys alone also narrows the offered host-key algorithms, so a guest
        # with only a plain host key fails during key exchange rather than at
        # verification.
        if ca_public_key:
            raise RelayError(
                f"Host key for {host} is not signed by the Whistler CA: {e}") from e
        raise RelayError(f"Could not connect to {host}:{port}: {e}") from e
    except (OSError, asyncssh.Error) as e:
        raise RelayError(f"Could not connect to {host}:{port}: {e}") from e

    closed = asyncio.get_running_loop().create_future()
    try:
        remote_chan, session = await conn.create_session(
            lambda: _RelaySession(chan, closed),
            command,
            term_type=term_type or (),
            term_size=term_size or (),
            # Raw bytes, not asyncssh's default utf-8. A relay is a pipe: it
            # must not transcode, and decoding would corrupt binary output and
            # raise on any byte sequence that isn't valid UTF-8.
            encoding=None,
        )
    except (OSError, asyncssh.Error) as e:
        conn.close()
        raise RelayError(f"Could not start a session on {host}: {e}") from e

    logger.info(f"Relay established to {username}@{host}:{port} "
                f"({'command' if command else 'shell'})")
    return Relay(conn, remote_chan, session, closed)
