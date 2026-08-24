"""ProxyJump routing (SSHServer.connection_requested → _jump_to_instance).

This is the access-control decision for the whole SSH plane — the set of
instances the gateway will splice a channel to — so most of these tests are
about what it *refuses* (design/proxyjump.md).
"""
import asyncio

import asyncssh
import pytest

from whistler import server as server_module
from whistler.config import KubeConfigManager, SSH_POSTURE_NONE
from whistler.server import SSHServer, strip_ssh_suffix


@pytest.fixture(autouse=True)
def _clear_splice_registry():
    """The splice registry is process-global, so a test that leaves an entry
    behind would change the next test's reaping decision."""
    server_module._JUMP_SPLICES.clear()
    yield
    server_module._JUMP_SPLICES.clear()


class _FakeSplicer:
    """Stands in for the TCP half of a splice. Records what the gateway tried
    to reach, and can be told to fail the way a not-yet-listening sshd does.
    Substituted for `SSHServer._splice`, so everything above it — resolution,
    the port pin, postures, waiting, on-demand create — is the real code."""

    def __init__(self, fail=False):
        self.fail = fail
        self.forwarded = []

    async def __call__(self, target):
        self.forwarded.append((target["host"], target["port"]))
        if self.fail:
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED, "Connection refused")
        return f"forwarder:{target['host']}:{target['port']}"


def _target(name="box", **overrides):
    target = {
        "name": name,
        "fullName": f"alice-{name}",
        "namespace": "whistler-alice",
        "runtime": "vm",
        "mode": "desktop",
        "host": f"alice-{name}.whistler-alice.svc.cluster.local",
        "port": 22,
        "zone": "default",
        "sshPosture": "direct",
        "phase": "Ready",
        "policyFailed": False,
        "statusMessage": None,
    }
    target.update(overrides)
    return target


def _server(make_config, targets=None, fail=False, **cfg):
    cm = make_config(
        users={"alice": {"name": "alice", "publicKeys": []}},
        ssh_targets={"alice": targets or {}}, **cfg)
    srv = SSHServer(config_manager=cm)
    srv.username = "alice"
    srv.splicer = _FakeSplicer(fail=fail)
    srv._splice = srv.splicer
    return srv


def _jump(srv, host="box.w", port=22):
    return asyncio.run(srv.connection_requested(host, port, "127.0.0.1", 5555))


# --------------------------------------------------------------------------- #
# Name parsing                                                                 #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("dialled,expected", [
    ("box.w", "box"),
    ("box", "box"),            # the suffix is optional, not required
    ("box.w.", "box"),         # fully-qualified form the resolver may hand us
    ("BOX.W", "BOX"),          # DNS names are case-insensitive; the name isn't
    ("my-box.w", "my-box"),    # dashes are ordinary, unlike the old username hack
])
def test_strip_ssh_suffix(dialled, expected):
    assert strip_ssh_suffix(dialled, ".w") == expected


def test_strip_ssh_suffix_leaves_other_domains_alone():
    # A name that merely ends in something similar must not be truncated.
    assert strip_ssh_suffix("box.web", ".w") == "box.web"


# --------------------------------------------------------------------------- #
# What it refuses                                                              #
# --------------------------------------------------------------------------- #

def test_unknown_name_is_refused(make_config):
    srv = _server(make_config, targets={})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        _jump(srv)
    assert "no instance named 'box'" in str(e.value)
    assert srv.splicer.forwarded == []


def test_another_users_instance_is_not_reachable(make_config):
    """Resolution is scoped to the authenticated user, so a name only bob has
    simply does not exist for alice — no cross-user probing."""
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": []}},
                     ssh_targets={"bob": {"box": _target()}})
    srv = SSHServer(config_manager=cm)
    srv.username = "alice"
    srv.splicer = _FakeSplicer()
    srv._splice = srv.splicer
    with pytest.raises(asyncssh.ChannelOpenError):
        _jump(srv)
    assert srv.splicer.forwarded == []


@pytest.mark.parametrize("port", [80, 443, 5900, 8082, 2049])
def test_only_sshd_is_reachable(make_config, port):
    """The gateway must never become a generic TCP relay: not the display
    port, not the storage gateway's NFS port, nothing but sshd."""
    srv = _server(make_config, targets={"box": _target()})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        _jump(srv, port=port)
    assert e.value.code == asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED
    assert srv.splicer.forwarded == []


def test_port_is_checked_before_the_name_is_resolved(make_config):
    """A refused port must not even reveal whether the instance exists."""
    srv = _server(make_config, targets={})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        _jump(srv, host="nonexistent.w", port=443)
    assert e.value.code == asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED


@pytest.mark.parametrize("dialled", [
    "", ".w", "a/b.w",
    "../../etc/passwd",     # nothing here is ever a path
    "box; rm -rf /",        # nothing here is ever a shell word
    "box name.w",           # whitespace: not a DNS label, not a CR name
    "-box.w",               # leading dash: invalid as a Kubernetes name
    "a" * 80,               # over the DNS label limit
])
def test_unparseable_names_are_refused(make_config, dialled):
    srv = _server(make_config, targets={"box": _target()})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        _jump(srv, host=dialled)
    assert e.value.code == asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED


@pytest.mark.parametrize("posture", ["relay", "none"])
def test_zone_posture_can_forbid_direct_ssh(make_config, posture):
    """An inbound session is an egress channel the zone's egress-only policies
    never see, so the posture is the only thing that can constrain it."""
    srv = _server(make_config,
                  targets={"box": _target(zone="restricted", sshPosture=posture)})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        _jump(srv)
    assert e.value.code == asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED
    assert "restricted" in str(e.value)
    assert srv.splicer.forwarded == []


def test_a_channel_grant_closes_the_jump_the_zone_would_allow(make_config):
    """The third axis: the zone permits direct SSH, this user is not granted
    it. Same zone, same instance, different doors from the colleague who is
    (design/security.md, "The border has four axes")."""
    srv = _server(make_config, targets={"box": _target(
        zone="restricted", sshPosture="direct", channels=["relay", "terminal"])})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        _jump(srv)
    assert e.value.code == asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED
    assert srv.splicer.forwarded == []


def test_an_ssh_grant_still_jumps(make_config):
    srv = _server(make_config, targets={"box": _target(channels=["ssh"])})
    assert _jump(srv)
    assert srv.splicer.forwarded == [
        ("alice-box.whistler-alice.svc.cluster.local", 22)]


# --------------------------------------------------------------------------- #
# What it permits                                                              #
# --------------------------------------------------------------------------- #

def test_splices_to_the_session_service(make_config):
    srv = _server(make_config, targets={"box": _target()})
    result = _jump(srv)
    assert srv.splicer.forwarded == [
        ("alice-box.whistler-alice.svc.cluster.local", 22)]
    assert result == "forwarder:alice-box.whistler-alice.svc.cluster.local:22"


def test_connecting_starts_a_stopped_instance(make_config):
    """`ssh box.w` on a halted VM means "start it and let me in": the gateway
    declares intent on the CR and the operator does the work. Unlike the
    launcher — where starting is its own key — a jump has no second key to
    press, and the client is showing the wait either way."""
    srv = _server(make_config, targets={"box": _target(phase="Stopped")})
    _jump(srv)
    assert srv.config_manager.started == [("alice", "box")]


def test_suffixless_dial_works(make_config):
    srv = _server(make_config, targets={"box": _target()})
    _jump(srv, host="box")
    assert srv.splicer.forwarded == [
        ("alice-box.whistler-alice.svc.cluster.local", 22)]


def test_localhost_is_no_longer_special(make_config):
    """`localhost` used to mean "forward into the instance this gateway is
    exec-bridging". With the bridge gone it is just another name to resolve —
    and a user's own `-L`/`-R` forwards now ride the end-to-end connection and
    never reach the gateway at all."""
    srv = _server(make_config, targets={})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        asyncio.run(srv.connection_requested("localhost", 8080, "127.0.0.1", 1))
    # Port-pinned first, so it never even gets as far as the name.
    assert e.value.code == asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED


# --------------------------------------------------------------------------- #
# Waiting, and telling a slow boot from a refusal                              #
# --------------------------------------------------------------------------- #

def test_policy_refusal_is_reported_not_timed_out(make_config, monkeypatch):
    """A start the operator refused must reach the user as its reason.
    Without this the three outcomes collapse to two and a policy violation
    reads as an unexplained timeout."""
    monkeypatch.setattr("whistler.server.JUMP_RETRY_INTERVAL", 0)
    srv = _server(make_config, fail=True, targets={
        "box": _target(policyFailed=True,
                       statusMessage="zone 'restricted' forbids volume 'scratch'")})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        _jump(srv)
    assert e.value.code == asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED
    assert "forbids volume 'scratch'" in str(e.value)


def test_timeout_names_the_instance_and_phase(make_config, monkeypatch):
    monkeypatch.setattr("whistler.server.JUMP_RETRY_INTERVAL", 0)
    monkeypatch.setattr("whistler.server.JUMP_CONNECT_TIMEOUT", 0)
    srv = _server(make_config, fail=True,
                  targets={"box": _target(phase="Provisioning")})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        _jump(srv)
    assert e.value.code == asyncssh.OPEN_CONNECT_FAILED
    assert "box" in str(e.value) and "Provisioning" in str(e.value)


def test_retries_until_sshd_answers(make_config, monkeypatch):
    """A cold boot refuses the connection for a while; the client should see a
    slow connect rather than a failure."""
    monkeypatch.setattr("whistler.server.JUMP_RETRY_INTERVAL", 0)
    srv = _server(make_config, fail=True, targets={"box": _target()})

    attempts = {"n": 0}
    original = srv.splicer

    async def flaky(target):
        attempts["n"] += 1
        if attempts["n"] >= 3:
            srv.splicer.fail = False       # sshd finally came up
        return await original(target)

    srv._splice = flaky
    assert _jump(srv).startswith("forwarder:")
    assert attempts["n"] == 3


# --------------------------------------------------------------------------- #
# No creation from a connection                                                #
# --------------------------------------------------------------------------- #

def test_a_template_name_is_not_auto_created(make_config):
    """`ssh <template>.w` deliberately does NOT make one.

    A channel open is the wrong place to wait on a cold boot: the client has
    nothing to show, no way to report why it is taking minutes, and no way to
    say it will never finish. Creating from a template belongs in the
    launcher, which can track the wait and explain it."""
    srv = _server(make_config, targets={},
                  templates={"alice": [{"name": "ubuntu"}]})
    with pytest.raises(asyncssh.ChannelOpenError) as e:
        _jump(srv, host="ubuntu.w")
    assert "no instance named 'ubuntu'" in str(e.value)
    assert srv.config_manager.created == []
    assert srv.splicer.forwarded == []


# --------------------------------------------------------------------------- #
# Splice tracking and reaping on-demand instances                              #
# --------------------------------------------------------------------------- #

def test_ephemeral_is_reaped_when_the_last_splice_closes(make_config, monkeypatch):
    monkeypatch.setattr("whistler.server.JUMP_EPHEMERAL_GRACE", 0)
    srv = _server(make_config, targets={"box": _target(ephemeral=True)})
    server_module._JUMP_SPLICES[("alice", "box")] = 1

    async def run():
        srv._splice_closed(_target("box", ephemeral=True))
        await asyncio.sleep(0)   # let the reap task run
        await asyncio.sleep(0)

    asyncio.run(run())
    assert srv.config_manager.deleted == [("alice", "box")]


def test_named_instance_is_never_reaped(make_config, monkeypatch):
    """Only what the gateway created on demand is the gateway's to remove."""
    monkeypatch.setattr("whistler.server.JUMP_EPHEMERAL_GRACE", 0)
    srv = _server(make_config, targets={"box": _target()})
    server_module._JUMP_SPLICES[("alice", "box")] = 1

    async def run():
        srv._splice_closed(_target("box", ephemeral=False))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    asyncio.run(run())
    assert srv.config_manager.deleted == []


def test_reap_waits_for_the_last_of_several_splices(make_config, monkeypatch):
    """scp and ssh at once are two channels on one instance; the first to
    close must not pull the box out from under the second."""
    monkeypatch.setattr("whistler.server.JUMP_EPHEMERAL_GRACE", 0)
    srv = _server(make_config, targets={"box": _target(ephemeral=True)})
    target = _target("box", ephemeral=True)
    server_module._JUMP_SPLICES[("alice", "box")] = 2

    async def run():
        srv._splice_closed(target)
        await asyncio.sleep(0)
        assert srv.config_manager.deleted == []   # one still open
        srv._splice_closed(target)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    asyncio.run(run())
    assert srv.config_manager.deleted == [("alice", "box")]


def test_reconnect_inside_the_grace_window_cancels_the_reap(make_config, monkeypatch):
    """`scp` then `ssh` seconds apart is the common shape; reaping between
    them would make the second pay for a cold boot."""
    monkeypatch.setattr("whistler.server.JUMP_EPHEMERAL_GRACE", 0.05)
    srv = _server(make_config, targets={"box": _target(ephemeral=True)})
    target = _target("box", ephemeral=True)
    server_module._JUMP_SPLICES[("alice", "box")] = 1

    async def run():
        srv._splice_closed(target)                       # reap scheduled
        server_module._JUMP_SPLICES[("alice", "box")] = 1  # someone reconnects
        await asyncio.sleep(0.15)

    asyncio.run(run())
    assert srv.config_manager.deleted == []


def test_splice_counts_a_real_connection(make_config):
    """The counting path itself, against a real loopback listener — the fake
    splicer used above deliberately doesn't exercise it."""
    srv = _server(make_config, targets={"box": _target(ephemeral=True)})
    del srv._splice   # use the real one

    async def run():
        listener = await asyncio.start_server(
            lambda r, w: None, "127.0.0.1", 0)
        port = listener.sockets[0].getsockname()[1]
        target = _target("box", ephemeral=True, host="127.0.0.1", port=port)
        forwarder = await srv._splice(target)
        assert server_module._JUMP_SPLICES[("alice", "box")] == 1
        forwarder.connection_lost(None)
        assert ("alice", "box") not in server_module._JUMP_SPLICES
        listener.close()

    asyncio.run(run())


# --------------------------------------------------------------------------- #
# Zone posture resolution                                                      #
# --------------------------------------------------------------------------- #

def _manager(zones):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.zones = zones
    return cm


def test_zone_posture_defaults_to_direct():
    assert _manager({"default": {}}).zone_ssh_posture("default") == "direct"


@pytest.mark.parametrize("value", ["direct", "relay", "none", "NONE", " relay "])
def test_zone_posture_is_normalised(value):
    posture = _manager({"z": {"ssh": value}}).zone_ssh_posture("z")
    assert posture == value.strip().lower()


def test_unknown_zone_posture_fails_closed():
    """A typo in a restricted zone's posture must not silently open it —
    the same fail-closed rule unknown zones already follow."""
    assert _manager({"z": {"ssh": "yes-please"}}).zone_ssh_posture("z") \
        == SSH_POSTURE_NONE
