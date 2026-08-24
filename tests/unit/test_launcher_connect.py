"""The launcher's hand-over (WhistlerSession._connect_to_instance).

The launcher connects; it does not boot. Starting a session is its own key in
the TUI, so `start=False` is the whole point of these tests: connect must not
smuggle a cold boot in behind a progress-dot wait the user cannot escape, and
it must say so immediately rather than spending the connect budget on dots.

The direct paths keep the old behaviour — `ssh box.w` has no launcher to press
a key in — which is what the last test pins.
"""
import asyncio

import pytest

from whistler.server import WhistlerSession


class _FakeChan:
    """The SSH channel, reduced to what a failed or successful connect uses."""

    def __init__(self):
        self.written = b""
        self.exit_status = None

    def write(self, data):
        self.written += data

    def exit(self, status):
        self.exit_status = status

    @property
    def text(self):
        return self.written.decode()


def _target(name="box", phase="Ready", **overrides):
    target = {
        "name": name,
        "fullName": f"alice-{name}",
        "namespace": "whistler-alice",
        "runtime": "vm",
        "mode": "ssh",
        "host": f"alice-{name}.whistler-alice.svc.cluster.local",
        "port": 22,
        "zone": "default",
        "sshPosture": "direct",
        "phase": phase,
        "policyFailed": False,
        "statusMessage": None,
    }
    target.update(overrides)
    return target


def _session(make_config, targets, ready=True):
    """A session wired to a fake channel, with the two IO-bound steps stubbed:
    everything between them — resolution, the channel check, the start
    decision and the wait — is the real code."""
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": []}},
                     ssh_targets={"alice": targets})
    sess = WhistlerSession(config_manager=cm, username="alice",
                           target_name="box")
    sess._chan = _FakeChan()
    sess.term_type = "xterm"
    sess.relayed = []

    async def sshd_ready(target):
        return ready

    async def run_relay_shell(target, command=None):
        sess.relayed.append(target["fullName"])
        return None

    sess._sshd_ready = sshd_ready
    sess._run_relay_shell = run_relay_shell
    return sess


def test_launcher_connect_does_not_start_a_stopped_session(make_config):
    sess = _session(make_config, {"box": _target(phase="Stopped")}, ready=False)
    sess._return_to_tui = True

    asyncio.run(sess._connect_to_instance(start=False))

    assert sess.config_manager.started == []
    assert sess.relayed == []


def test_launcher_connect_says_so_at_once_rather_than_timing_out(make_config):
    """A stopped session with no start on the way is a decided outcome, not a
    slow one: reporting it now is the difference between a sentence the user
    can act on and a minute of dots."""
    sess = _session(make_config, {"box": _target(phase="Stopped")}, ready=False)
    sess._return_to_tui = True

    asyncio.run(sess._connect_to_instance(start=False))

    assert "not running" in sess._chan.text
    assert "Stopped" in sess._chan.text
    # The launcher is coming back, so the channel stays open.
    assert sess._chan.exit_status is None


def test_launcher_connect_relays_to_a_running_session(make_config):
    sess = _session(make_config, {"box": _target()})
    sess._return_to_tui = True

    asyncio.run(sess._connect_to_instance(start=False))

    assert sess.relayed == ["alice-box"]
    assert sess.config_manager.started == []


def test_a_direct_instance_target_still_starts_on_connect(make_config):
    """`ssh user@gateway` straight at an instance (and the jump) has no second
    key to press, so connect there still means "bring it up and let me in"."""
    sess = _session(make_config, {"box": _target(phase="Stopped")})

    asyncio.run(sess._connect_to_instance())

    assert sess.config_manager.started == [("alice", "box")]
    assert sess.relayed == ["alice-box"]


def test_a_relay_channel_the_user_lacks_is_still_refused(make_config):
    """The channel check comes before the start decision, so neither path can
    be used to reach a session the zone or the grant closes."""
    sess = _session(make_config, {"box": _target(channels=["ssh"])})

    asyncio.run(sess._connect_to_instance(start=False))

    assert sess.relayed == []
    assert "not available to you" in sess._chan.text


@pytest.mark.parametrize("start", [True, False])
def test_a_policy_refusal_is_still_reported(make_config, monkeypatch, start):
    """The fast fail must not swallow the other decided outcome: a start the
    operator refused reaches the user as its reason, on both paths."""
    monkeypatch.setattr("whistler.server.JUMP_RETRY_INTERVAL", 0)
    sess = _session(make_config, {"box": _target(
        phase="Provisioning", policyFailed=True,
        statusMessage="zone 'secret' is not allowed")}, ready=False)

    asyncio.run(sess._connect_to_instance(start=start))

    assert "zone 'secret' is not allowed" in sess._chan.text
