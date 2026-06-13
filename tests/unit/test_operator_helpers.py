"""Operator helper logic that needs no cluster."""
from whistler.operator import _instance_short_name


def test_strips_user_prefix():
    assert _instance_short_name("alice-box", "alice") == "box"


def test_preserves_dashes_in_instance_name():
    assert _instance_short_name("alice-my-box", "alice") == "my-box"


def test_returns_name_unchanged_without_prefix():
    assert _instance_short_name("box", "alice") == "box"
