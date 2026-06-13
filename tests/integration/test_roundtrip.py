"""C1 round-trip integration test.

Exercises the full path the Step 1-3 refactor changed:
  auth -> server creates the WhistlerInstance CR -> operator reconcile creates
  the pod -> server's pod watch sees it reach Running -> session is bridged into
  the pod via `kubectl exec`.

Requires a running cluster, a running whistler server + operator, and a key the
test user is authorised with. The Makefile `integration` target wires this up
against k3d; configuration is passed via env:

  WHISTLER_TEST_SSH_HOST   (default 127.0.0.1)
  WHISTLER_TEST_SSH_PORT   (default 8022)
  WHISTLER_TEST_USER       (default tester)
  WHISTLER_TEST_TEMPLATE   (default small)
  WHISTLER_TEST_KEY        (path to the private key; required)

Marked `integration` so it is excluded from the default unit run.
"""
import os

import asyncssh
import pytest

pytestmark = pytest.mark.integration

HOST = os.environ.get("WHISTLER_TEST_SSH_HOST", "127.0.0.1")
PORT = int(os.environ.get("WHISTLER_TEST_SSH_PORT", "8022"))
USER = os.environ.get("WHISTLER_TEST_USER", "tester")
TEMPLATE = os.environ.get("WHISTLER_TEST_TEMPLATE", "small")
KEY = os.environ.get("WHISTLER_TEST_KEY")

# Pod scheduling + image pull can take a while on a cold cluster.
CONNECT_TIMEOUT = int(os.environ.get("WHISTLER_TEST_TIMEOUT", "180"))


def _require_key():
    if not KEY or not os.path.exists(KEY):
        pytest.skip("WHISTLER_TEST_KEY not set or missing; integration env not configured")
    return KEY


async def test_ephemeral_session_runs_command_in_pod():
    """Connecting as `user-<template>` provisions a pod and runs a command in it."""
    key = _require_key()
    async with asyncssh.connect(
        HOST, PORT,
        username=f"{USER}-{TEMPLATE}",
        client_keys=[key],
        known_hosts=None,
        connect_timeout=CONNECT_TIMEOUT,
    ) as conn:
        result = await conn.run("echo whistler-roundtrip-ok", check=True)
        assert "whistler-roundtrip-ok" in result.stdout


async def test_unauthorized_user_is_refused():
    """A user the server doesn't know must not be able to authenticate."""
    _require_key()
    with pytest.raises((asyncssh.PermissionDenied, asyncssh.Error)):
        async with asyncssh.connect(
            HOST, PORT,
            username="definitely-not-a-user",
            client_keys=[_require_key()],
            known_hosts=None,
            connect_timeout=CONNECT_TIMEOUT,
        ):
            pass
