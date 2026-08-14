"""Non-interactive commands the gateway answers (server.py).

`ssh <gateway> known-hosts` and `ssh <gateway> ssh-config` exist so the two
strings a user must place in their own ~/.ssh can be *redirected* into a file.
The launcher's `?` screen shows them, but a full-screen TUI is precisely the
thing you cannot select text out of, and the `@cert-authority` line is 90-odd
characters that wrap in the box. So these have to emit clean, appendable text
— no framing, no CRLF when there is no PTY, and a real exit status.
"""
import asyncio

import asyncssh
import pytest

from whistler.server import WhistlerSession


class _Chan:
    """A binary channel, as strictly as the real one.

    `connection_made` calls `set_encoding(None)`, so asyncssh does `bytes(data)`
    on everything written and a `str` raises TypeError — out of
    `session_started`, where the client sees only "closed by remote host".
    A fake that accepted both let exactly that ship, so this one refuses str.
    """

    def __init__(self):
        self.out, self.err, self.status = "", "", None

    @staticmethod
    def _decode(data):
        assert isinstance(data, bytes), f"channel is binary, got {type(data)}"
        return data.decode()

    def write(self, data):
        self.out += self._decode(data)

    def write_stderr(self, data):
        self.err += self._decode(data)

    def exit(self, status):
        self.status = status


def _session(make_config, pty=False, **cfg):
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": []}}, **cfg)
    sess = WhistlerSession(config_manager=cm, username="alice")
    sess._chan = _Chan()
    if pty:
        sess.term_type = "xterm-256color"
    return sess


@pytest.mark.asyncio
async def test_known_hosts_emits_one_appendable_line(make_config):
    sess = _session(make_config)
    await sess._run_gateway_command("known-hosts")

    assert sess._chan.out.startswith("@cert-authority *.w ")
    # Exactly one line, LF-terminated: `>> ~/.ssh/known_hosts` must not
    # produce a stray CR that OpenSSH reads as part of the key blob.
    assert sess._chan.out.endswith("\n") and "\r" not in sess._chan.out
    assert sess._chan.out.count("\n") == 1
    assert sess._chan.status == 0


@pytest.mark.asyncio
async def test_ssh_config_emits_both_host_blocks(make_config):
    sess = _session(make_config)
    await sess._run_gateway_command("ssh-config")

    out = sess._chan.out
    assert "Host whistler-gateway" in out
    assert "Host *.w" in out
    assert "ProxyJump whistler-gateway" in out
    assert "    User alice" in out
    # Unindented Host lines: this is appended to a config file, not framed in
    # a box like the `?` screen's copy.
    assert "\nHost *.w" in out
    assert "\r" not in out
    assert sess._chan.status == 0


@pytest.mark.asyncio
async def test_a_pty_client_gets_its_terminals_line_endings(make_config):
    sess = _session(make_config, pty=True)
    await sess._run_gateway_command("known-hosts")

    assert sess._chan.out.endswith("\r\n")


@pytest.mark.asyncio
async def test_unknown_command_fails_with_the_list_of_real_ones(make_config):
    sess = _session(make_config)
    await sess._run_gateway_command("rm -rf /")

    # A closed list of nouns, not a shell: nothing ran, and the error names
    # the alternatives rather than leaving the user guessing.
    assert sess._chan.out == ""
    assert "known-hosts" in sess._chan.err and "ssh-config" in sess._chan.err
    assert sess._chan.status == 1


@pytest.mark.asyncio
async def test_known_hosts_without_a_ca_says_so_instead_of_emitting_nothing(
        make_config, monkeypatch):
    sess = _session(make_config)
    monkeypatch.setattr(sess.config_manager, "get_ssh_known_hosts_line",
                        lambda: None)
    await sess._run_gateway_command("known-hosts")

    assert sess._chan.out == ""
    assert "host CA" in sess._chan.err
    assert sess._chan.status == 1


# --------------------------------------------------------------------------- #
# Against a real asyncssh channel.                                             #
#                                                                              #
# The fake above is strict about bytes because the first cut of this feature   #
# shipped a `str` write: the fake accepted it, the real channel — which        #
# connection_made puts in binary mode — raised TypeError out of                #
# session_started, and the client saw only "closed by remote host". A test     #
# that drives the actual WhistlerSession over a real connection is the one     #
# that could not have missed it.                                               #
# --------------------------------------------------------------------------- #


class _Server(asyncssh.SSHServer):
    def __init__(self, config_manager):
        self.config_manager = config_manager

    def begin_auth(self, username):
        return False   # authentication is not what this test is about

    def session_requested(self):
        return WhistlerSession(config_manager=self.config_manager,
                               username="alice")


@pytest.mark.asyncio
async def test_over_a_real_channel_the_output_is_appendable(make_config):
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": []}})
    server = await asyncssh.create_server(
        lambda: _Server(cm), "127.0.0.1", 0,
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")])
    port = next(iter(server.sockets)).getsockname()[1]

    async def _run_all():
        async with asyncssh.connect("127.0.0.1", port, username="alice",
                                    known_hosts=None) as conn:
            return (await conn.run("known-hosts", check=True),
                    await conn.run("ssh-config", check=True),
                    await conn.run("nope"))

    try:
        # Bounded: a command that raises inside session_started leaves the
        # client waiting on a channel nobody will ever close, so the failure
        # mode of this test without the timeout is a hung suite rather than a
        # red assertion. Verified by breaking it on purpose.
        known_hosts, config, unknown = await asyncio.wait_for(_run_all(), 15)
    finally:
        server.close()
        await server.wait_closed()

    assert known_hosts.stdout == "@cert-authority *.w ssh-ed25519 AAAAFAKE\n"
    assert "Host *.w" in config.stdout and "ProxyJump" in config.stdout
    assert unknown.exit_status == 1 and "known-hosts" in unknown.stderr
