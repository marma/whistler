"""The gateway restarts onto a host key that changed in its Secret
(server.watch_host_key) — the mechanism a restore (design/backup.md) relies on
to take effect without RBAC on the gateway's Deployment."""

import asyncio

import asyncssh
import pytest

from whistler import server


def _key():
    return asyncssh.generate_private_key("ssh-ed25519").export_private_key()


class _Secret:
    """Answers get_server_host_key from a script, one entry per poll; the
    last entry repeats. An Exception instance is raised instead of returned."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.polls = 0

    def get_server_host_key(self, secret_name):
        self.polls += 1
        answer = self.answers[min(self.polls, len(self.answers)) - 1]
        if isinstance(answer, Exception):
            raise answer
        return answer


async def _watch(secret, key_in_use, polls=5):
    """Run the watcher for at least ``polls`` polls; True if it fired."""
    fired = asyncio.Event()
    task = asyncio.ensure_future(server.watch_host_key(
        secret, "gw-host-key", key_in_use, fired.set, interval=0))
    for _ in range(200):
        if fired.is_set() or task.done() or secret.polls >= polls:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    return fired.is_set()


async def test_same_key_does_not_restart():
    key = _key()
    # Re-exported bytes differ (random check value) for the same key; that
    # must not read as a change.
    reexported = asyncssh.import_private_key(key).export_private_key()
    assert reexported != key
    assert await _watch(_Secret(reexported), key) is False


async def test_changed_key_restarts():
    key = _key()
    assert await _watch(_Secret(key, key, _key()), key) is True


@pytest.mark.parametrize("answer", [None, b"", b"not a key",
                                    RuntimeError("API down")])
async def test_missing_unreadable_or_failing_secret_is_not_a_change(answer):
    """Restarting on a missing Secret would generate yet another key on the
    way back up — exactly the churn persisting it exists to prevent."""
    key = _key()
    secret = _Secret(answer)
    assert await _watch(secret, key) is False
    assert secret.polls >= 5  # it kept watching


async def test_change_after_an_outage_still_restarts():
    key = _key()
    assert await _watch(_Secret(None, RuntimeError("API down"), _key()),
                        key) is True
