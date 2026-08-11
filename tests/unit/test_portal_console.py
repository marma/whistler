"""The machine-console action on dashboard rows
(whistler.portal.management._merge_sessions).

The console is the VM's emulated display from power-on — firmware, bootloader,
kernel messages — not the desktop stream. It is admin-only, and the row builder
is what decides whether the button exists at all. The server-side gate that
makes that a control rather than a hint is tested in test_desktop_proxy.py.
"""
from whistler.portal.management import _merge_sessions


def _vm(name="v1"):
    return {"name": name, "template": "ubuntu-vm", "phase": "Ready",
            "runtime": "vm"}


def _container_desktop(name="d1"):
    # Retired shape (design/container_workloads.md), but the row builder must
    # still do the right thing if one is left over in a cluster.
    return {"name": name, "template": "xfce", "phase": "Ready",
            "runtime": "container"}


def _row(rows, name):
    return next(r for r in rows if r["name"] == name)


def test_admin_gets_a_console_url_for_a_vm():
    rows = _merge_sessions([], [_vm()], "alice", is_admin=True)
    assert _row(rows, "v1")["console_url"] == "/console/v1?user=alice"


def test_non_admin_gets_none():
    rows = _merge_sessions([], [_vm()], "alice", is_admin=False)
    assert _row(rows, "v1")["console_url"] is None


def test_is_admin_defaults_to_no_console():
    """Fail closed: a caller that forgets to resolve admin must not leak the
    action into the page."""
    rows = _merge_sessions([], [_vm()], "alice")
    assert _row(rows, "v1")["console_url"] is None


def test_non_vm_session_has_no_console_even_for_admins():
    """There is no emulated display to look at — the console is a property of
    the machine, not of the session."""
    rows = _merge_sessions([], [_container_desktop()], "alice", is_admin=True)
    assert _row(rows, "d1")["console_url"] is None


def test_ssh_instances_have_no_console():
    rows = _merge_sessions([{"name": "box", "status": "Running"}], [],
                           "alice", is_admin=True)
    assert _row(rows, "box")["console_url"] is None


def test_console_is_separate_from_the_desktop_url():
    """Distinct actions, distinct links. Conflating them is what made the
    console unreachable for Selkies VMs in the first place — and, briefly, what
    made gating "the console" on admin take the *desktop* away from every
    viewer:vnc VM, since /connect redirects those to the same page."""
    row = _row(_merge_sessions([], [_vm()], "alice", is_admin=True), "v1")
    assert row["connect_url"] != row["console_url"]
    assert "/connect/" in row["connect_url"]
    assert "/console/" in row["console_url"]
