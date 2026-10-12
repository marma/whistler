"""Key auth survives a Textual launcher having run on the gateway's loop.

Textual's `App.run_async` installs `asyncio.eager_task_factory` on the running
loop and leaves it there. asyncssh (2.24.1+) starts a server auth task in the
handler's constructor and only replies `if conn.auth is self` — so with eager
tasks the "key acceptable" reply was dropped before the handler was assigned,
and every login after the first launcher session hung after the key offer.
"""
import asyncio

import asyncssh
import pytest

from whistler.server import _keep_tasks_lazy

pytestmark = pytest.mark.skipif(
    not hasattr(asyncio, "eager_task_factory"),
    reason="no eager task factory before Python 3.12")


class _KeyServer(asyncssh.SSHServer):
    def __init__(self, public_key):
        self._public_key = public_key

    def begin_auth(self, username):
        return True

    def public_key_auth_supported(self):
        return True

    def validate_public_key(self, username, key):
        # Synchronous, like SSHServer.validate_public_key: nothing in the auth
        # task yields, which is what let an eager task run to completion.
        return key.public_data == self._public_key.public_data


async def _login_after_eager_request(guard):
    loop = asyncio.get_running_loop()
    if guard:
        _keep_tasks_lazy(loop)
    # What App.run_async does on the shared loop.
    loop.set_task_factory(asyncio.eager_task_factory)

    host_key = asyncssh.generate_private_key("ssh-ed25519")
    client_key = asyncssh.generate_private_key("ssh-ed25519")
    server = await asyncssh.create_server(
        lambda: _KeyServer(client_key.convert_to_public()), "127.0.0.1", 0,
        server_host_keys=[host_key])
    port = server.sockets[0].getsockname()[1]
    try:
        conn = await asyncio.wait_for(asyncssh.connect(
            "127.0.0.1", port, username="alice", known_hosts=None,
            client_keys=[client_key], agent_path=None), timeout=5)
        conn.close()
        await conn.wait_closed()
    finally:
        server.close()
        await server.wait_closed()
        loop.set_task_factory(None)


def test_key_auth_completes_after_textual_requests_eager_tasks():
    asyncio.run(_login_after_eager_request(guard=True))


def test_the_guard_still_accepts_other_factories():
    async def check():
        loop = asyncio.get_running_loop()
        _keep_tasks_lazy(loop)

        def factory(loop, coro, **kwargs):
            return asyncio.Task(coro, loop=loop, **kwargs)

        loop.set_task_factory(asyncio.eager_task_factory)
        assert loop.get_task_factory() is None
        loop.set_task_factory(factory)
        assert loop.get_task_factory() is factory
        loop.set_task_factory(None)
    asyncio.run(check())


def test_unguarded_loop_reproduces_the_hang():
    """Pins the mechanism: if this starts passing, asyncssh or Textual
    changed and the guard may no longer be needed."""
    if tuple(int(p) for p in asyncssh.__version__.split(".")[:3]) < (2, 24, 1):
        pytest.skip("asyncssh before 2.24.1 sends auth replies unconditionally")
    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
        asyncio.run(_login_after_eager_request(guard=False))
