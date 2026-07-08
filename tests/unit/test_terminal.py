"""Web-terminal pure helpers: kubectl-exec command assembly, resize control-frame
parsing, and the ssh/desktop session resolution used by the terminal routes. No
cluster, no PTY — the relay's I/O is exercised by integration, not here."""
from whistler.portal import terminal
from whistler.portal.app import _resolve_target


# --- build_exec_command ---------------------------------------------------- #

def test_exec_command_is_interactive_tty_pinned_to_main():
    # -c main: no "Defaulted container" banner, and never the streamer sidecar.
    cmd = terminal.build_exec_command("pod-x", "ns-y")
    assert cmd[:9] == ["kubectl", "exec", "-it", "pod-x", "-n", "ns-y",
                       "-c", "main", "--"]


def test_exec_command_falls_back_to_sh():
    # The remote shell expression must degrade to /bin/sh when bash is absent.
    cmd = terminal.build_exec_command("pod-x", "ns-y", shell="/bin/zsh")
    assert cmd[-1].endswith("exec /bin/zsh || exec /bin/sh")


def test_exec_command_drops_root_to_desktop_user():
    # Root + DESKTOP_USER set (the desktop workload convention) -> su into the
    # created user's login shell, with DISPLAY/PULSE_SERVER re-exported so GUI
    # apps launched from the terminal reach the streamed desktop.
    script = terminal.build_exec_command("pod-x", "ns-y")[-1]
    assert '[ "$(id -u)" = "0" ]' in script
    assert 'exec su - "$DESKTOP_USER"' in script
    assert "DISPLAY='$DISPLAY'" in script and "PULSE_SERVER='$PULSE_SERVER'" in script


# --- parse_resize ---------------------------------------------------------- #

def test_parse_resize_valid():
    assert terminal.parse_resize('{"resize": [120, 40]}') == (120, 40)


def test_parse_resize_clamps_and_floors():
    assert terminal.parse_resize('{"resize": [0, 99999]}') == (1, terminal._MAX_DIM)


def test_parse_resize_ignores_plain_input():
    # Ordinary keystrokes (incl. text that isn't a resize object) are not resizes,
    # so the relay forwards them to the PTY instead of swallowing them.
    assert terminal.parse_resize("ls -la\n") is None
    assert terminal.parse_resize('{"other": 1}') is None
    assert terminal.parse_resize('{not json') is None
    assert terminal.parse_resize("") is None


# --- _resolve_target ------------------------------------------------------- #

def _ssh(name, status, pod="p"):
    return {"name": name, "status": status, "podName": pod, "namespace": "ns"}


def _desk(name, phase, runtime, pod="p"):
    return {"name": name, "phase": phase, "runtime": runtime, "podName": pod, "namespace": "ns"}


def test_resolve_ssh_ready_when_running():
    t = _resolve_target([_ssh("a", "Running")], [], "a")
    assert t["supported"] and t["ready"] and t["podName"] == "p"


def test_resolve_ssh_not_ready_when_pending():
    t = _resolve_target([_ssh("a", "Pending")], [], "a")
    assert t["supported"] and not t["ready"]


def test_resolve_desktop_container_ready():
    t = _resolve_target([], [_desk("d", "Ready", "container")], "d")
    assert t["supported"] and t["ready"]


def test_resolve_desktop_vm_unsupported():
    # VM-runtime desktops have no pod to exec into — terminal is gated off.
    t = _resolve_target([], [_desk("d", "Ready", "vm")], "d")
    assert t["supported"] is False


def test_resolve_unknown_returns_none():
    assert _resolve_target([], [], "missing") is None
