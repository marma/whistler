"""Public-key auth and username -> target resolution (SSHServer)."""
import asyncssh
import pytest

from whistler.server import SSHServer


def _keypair(comment="user@host"):
    """Return (private_key_obj, authorized_keys_line) for a fresh key."""
    key = asyncssh.generate_private_key("ssh-ed25519")
    line = key.export_public_key().decode().strip() + f" {comment}"
    return key, line


def test_authorized_key_authenticates_and_routes_to_tui(make_config):
    key, line = _keypair()
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": [line]}})
    srv = SSHServer(config_manager=cm)

    assert srv.validate_public_key("alice", key) is True
    assert srv.username == "alice"
    assert srv.target_type == "tui"
    assert srv.target_name is None


def test_unknown_user_is_rejected(make_config):
    key, _line = _keypair()
    cm = make_config(users={})
    srv = SSHServer(config_manager=cm)

    assert srv.validate_public_key("nobody", key) is False


def test_wrong_key_is_rejected(make_config):
    _registered, registered_line = _keypair()
    other_key, _ = _keypair()
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": [registered_line]}})
    srv = SSHServer(config_manager=cm)

    # A different key for a known user must not authenticate (regression guard
    # against the old substring match, which compared base64 fragments).
    assert srv.validate_public_key("alice", other_key) is False


def test_malformed_authorized_key_is_skipped_not_fatal(make_config):
    key, good_line = _keypair()
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": ["not-a-key", good_line]}})
    srv = SSHServer(config_manager=cm)

    # The garbage entry is skipped; the valid one still authenticates.
    assert srv.validate_public_key("alice", key) is True


def test_resolve_target_is_tui_or_an_existing_instance(make_config):
    """The legacy suffix now only ever names an existing instance. It used to
    also accept a *template* name, meaning "make me one and connect" — dropped
    with the jump's equivalent, because creating belongs in an interface that
    can show progress."""
    cm = make_config(
        users={"alice": {"name": "alice", "publicKeys": []}},
        templates={"alice": [{"name": "small"}]},
    )
    srv = SSHServer(config_manager=cm)

    srv._resolve_target("alice", ["alice"])
    assert srv.target_type == "tui"

    # A template name is not special any more: it is just a name to look up.
    srv._resolve_target("alice", ["alice", "small"])
    assert srv.target_type == "instance"
    assert srv.target_name == "small"

    srv._resolve_target("alice", ["alice", "123"])
    assert srv.target_type == "instance"
    assert srv.target_name == "123"
    assert srv.active_instance_name == "123"


def test_resolve_target_rejoins_dashed_suffix(make_config):
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": []}})
    srv = SSHServer(config_manager=cm)

    srv._resolve_target("alice", ["alice", "my", "box"])
    assert srv.target_type == "instance"
    assert srv.target_name == "my-box"


# --- username routing: the whole name wins over the legacy split ---------- #

def test_username_with_a_dash_authenticates_as_itself(make_config):
    """The bug the old unconditional split had: `alice-smith` could never log
    in, because it was read as user `alice` wanting instance `smith`."""
    key, line = _keypair()
    cm = make_config(users={"alice-smith": {"name": "alice-smith",
                                            "publicKeys": [line]}})
    srv = SSHServer(config_manager=cm)

    assert srv.validate_public_key("alice-smith", key) is True
    assert srv.username == "alice-smith"
    assert srv.target_type == "tui"


def test_dashed_user_wins_over_a_same_named_instance(make_config):
    """Both readings are possible for `alice-box`; the real user wins."""
    key, line = _keypair()
    cm = make_config(users={
        "alice-box": {"name": "alice-box", "publicKeys": [line]},
        "alice": {"name": "alice", "publicKeys": [line]},
    })
    srv = SSHServer(config_manager=cm)

    assert srv.validate_public_key("alice-box", key) is True
    assert srv.username == "alice-box"
    assert srv.target_type == "tui"


def test_legacy_routing_still_works_for_non_users(make_config):
    key, line = _keypair()
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": [line]}})
    srv = SSHServer(config_manager=cm)

    assert srv.validate_public_key("alice-box", key) is True
    assert srv.username == "alice"
    assert srv.target_type == "instance"
    assert srv.target_name == "box"


def test_legacy_routing_can_be_switched_off(monkeypatch, make_config):
    monkeypatch.setattr("whistler.server.LEGACY_USERNAME_ROUTING", False)
    key, line = _keypair()
    cm = make_config(users={"alice": {"name": "alice", "publicKeys": [line]}})
    srv = SSHServer(config_manager=cm)

    # With the convention gone, `alice-box` is simply an unknown user — the
    # instance is reached by `ssh box.w` through the jump instead.
    assert srv.validate_public_key("alice-box", key) is False
