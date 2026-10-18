"""The ~/.ssh/config stanza's gateway alias and destination (tui.py).

What is served is covered in test_gateway_commands.py; these pin the naming
rules, which OpenSSH reads in ways that are easy to get subtly wrong.
"""
import fnmatch

import pytest

from whistler.tui import gateway_alias, gateway_destination, ssh_config_stanza


@pytest.mark.parametrize("suffix, alias", [
    (".w", "whistler-gateway-w"),
    (".wp", "whistler-gateway-wp"),
    (".vm.example.com", "whistler-gateway-vm-example-com"),
    ("", "whistler-gateway"),
])
def test_alias_follows_the_suffix(suffix, alias):
    assert gateway_alias(suffix) == alias


@pytest.mark.parametrize("suffix", [".w", ".vm.example.com", "w", "-w", ""])
def test_the_gateway_never_jumps_through_itself(suffix):
    """The instance block must not apply to the gateway's own alias, or the
    jump recurses. A dotted suffix never matches; the others need the
    negation, which OpenSSH honours on a Host line."""
    alias = gateway_alias(suffix)
    host_line = next(line for line in ssh_config_stanza("a", suffix).splitlines()
                     if line.startswith("Host ") and "ProxyJump" not in line
                     and line != f"Host {alias}")
    patterns = host_line.split()[1:]
    positive = [p for p in patterns if not p.startswith("!")]
    negated = [p[1:] for p in patterns if p.startswith("!")]
    matches = (any(fnmatch.fnmatchcase(alias, p) for p in positive)
               and not any(fnmatch.fnmatchcase(alias, p) for p in negated))
    assert not matches


def test_port_22_is_left_implicit():
    assert "Port" not in ssh_config_stanza("a", ".w", "gw", 22)
    assert gateway_destination("a", "gw", 22) == ("a@gw", "a@gw")


def test_a_non_default_port_reaches_both_forms():
    assert gateway_destination("a", "gw", 30022) == (
        "a@gw:30022", "-p 30022 a@gw")


def test_unset_host_is_the_placeholder():
    assert gateway_destination("a") == ("a@<gateway-host>", "a@<gateway-host>")
