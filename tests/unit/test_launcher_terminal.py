"""The launcher's terminal environment (WhistlerSession._new_app).

Textual snapshots os.environ when the App object is constructed and Rich picks
the colour system from COLORTERM/TERM right then — so the client's terminal has
to be in the environment at *every* construction. The app the session builds
after a relay handover used to be built outside that window, and the launcher
came back from a connect in a different palette than it left in.
"""
import os

import pytest

from whistler.server import WhistlerSession


class _FakeChan:
    def write(self, data):
        pass


def _session(make_config, term="xterm-256color"):
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": []}})
    sess = WhistlerSession(config_manager=cm, username="alice")
    sess._chan = _FakeChan()
    sess.term_type = term
    return sess


def test_new_app_is_truecolor(make_config):
    """Whatever the gateway process's own environment says."""
    sess = _session(make_config)
    for ambient in ({}, {"TERM": "dumb"}, {"TERM": "linux", "COLORTERM": ""}):
        with _environ(ambient):
            app = sess._new_app()
            assert app.console.color_system == "truecolor"
            assert app.ssh_channel is sess._chan


def test_new_app_restores_the_process_environment(make_config):
    sess = _session(make_config)
    with _environ({"TERM": "linux"}):
        sess._new_app()
        assert os.environ.get("TERM") == "linux"
        assert "COLORTERM" not in os.environ
    with _environ({}):
        sess._new_app()
        assert "TERM" not in os.environ


def test_new_app_without_a_pty_leaves_term_alone(make_config):
    """No pty request means no client terminal to advertise."""
    sess = _session(make_config, term=None)
    with _environ({"TERM": "linux"}):
        sess._new_app()
        assert os.environ.get("TERM") == "linux"


class _environ:
    """Replace the environment for the duration of the block."""

    def __init__(self, env):
        self._env = env

    def __enter__(self):
        self._saved = dict(os.environ)
        os.environ.clear()
        os.environ.update(self._env)

    def __exit__(self, *exc):
        os.environ.clear()
        os.environ.update(self._saved)
