"""The slimmed launcher TUI (whistler/tui.py).

Smoke-level on purpose: these check that the launcher still composes, lists
what the user has, and hands a connect choice back to the session — the three
things the diet had to preserve — rather than pinning a layout that is meant
to keep changing.
"""
import pytest

from whistler.tui import SshHelpScreen, WhistlerApp


def _vm(name="box", template="ubuntu-vm", phase="Ready"):
    """A VM session — the kind SSH can actually reach."""
    return {"name": name, "template": template, "phase": phase, "runtime": "vm"}


def _stopped_vm(name="box", template="ubuntu-vm"):
    """A VM session whose workload is halted — the one the launcher starts."""
    return _vm(name, template, phase="Stopped")


def _pod(name="scratch", template="small", status="Running"):
    """An ssh-mode container session — listed, but with no sshd to reach."""
    return {"name": name, "template": template, "status": status}


def _config(make_config, sessions=(), instances=()):
    return make_config(
        users={"alice": {"name": "alice", "publicKeys": []}},
        desktop_sessions={"alice": list(sessions)},
        instances={"alice": list(instances)})


@pytest.mark.asyncio
async def test_lists_vm_sessions_with_their_ssh_address(make_config):
    """VMs are desktop-mode sessions. Listing only ssh-mode ones showed the
    launcher exactly the sessions it cannot connect to and hid the ones it
    can — regression guard for that."""
    cm = _config(make_config, sessions=[_vm()])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        table = app.query_one("#instances_table")
        assert table.row_count == 1
        row = table.get_row_at(0)
        assert row[0] == "box"
        # The address the user would type from their own shell, shown next to
        # every row so the direct path is discoverable without reading docs.
        assert row[3] == "box.w"


@pytest.mark.asyncio
async def test_lists_container_sessions_as_web_terminal_only(make_config):
    """Still listed — they exist and can be deleted from here — but the ssh
    column says what is true rather than an address that would hang."""
    cm = _config(make_config, instances=[_pod()])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        row = app.query_one("#instances_table").get_row_at(0)
        assert row[0] == "scratch"
        assert "web terminal" in row[3]
        assert ".w" not in row[3]


@pytest.mark.asyncio
async def test_both_kinds_appear_together(make_config):
    cm = _config(make_config, sessions=[_vm("box")], instances=[_pod("scratch")])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        table = app.query_one("#instances_table")
        assert table.row_count == 2
        assert {table.get_row_at(i)[0] for i in range(2)} == {"box", "scratch"}


@pytest.mark.asyncio
async def test_connect_refuses_a_session_with_no_sshd(make_config):
    """Refused in the launcher rather than by letting the relay burn its whole
    connect budget waiting for an sshd that will never answer."""
    cm = _config(make_config, instances=[_pod()])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_connect_instance()
        await pilot.pause()
        assert app.is_running

    assert app.return_value is None


@pytest.mark.asyncio
async def test_connect_exits_with_the_chosen_instance(make_config):
    """The app cannot hand its own channel to a remote shell, so it exits with
    the choice and the session does the relaying."""
    cm = _config(make_config, sessions=[_vm()])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_connect_instance()
        await pilot.pause()

    assert app.return_value == ("connect", "box")


@pytest.mark.asyncio
async def test_connect_with_nothing_selected_does_not_exit(make_config):
    app = WhistlerApp(config_manager=_config(make_config), username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_connect_instance()
        await pilot.pause()
        assert app.is_running

    assert app.return_value is None


def test_the_logo_block_is_square():
    """Every line the same cell width, because `text-align: center` centers
    each line on its own: a row one cell short of its neighbours is centered
    one cell further along, which shows up as the top of the logo sitting
    proud of the rest.

    Only at ODD container widths — 61 and 62 both center to the same offset in
    an 84-column terminal and differ in an 85-column one — which is why this
    survived a long time and why it is worth a test rather than an eyeball."""
    from rich.cells import cell_len
    from whistler.tui import LOGO
    widths = {cell_len(line) for line in LOGO.split("\n")}
    assert len(widths) == 1, f"ragged logo: {sorted(widths)}"


def test_the_logo_survives_a_whitespace_trim():
    """The reason the padding is computed rather than typed: the art's own
    trailing spaces (the ANSI Shadow `R` ends its top row with one) are
    exactly what an editor set to trim trailing whitespace removes."""
    from rich.cells import cell_len
    from whistler.tui import _square_block
    trimmed = "\n".join(l.rstrip() for l in ["ab ", "cd", "e  "])
    squared = _square_block(trimmed)
    assert {cell_len(l) for l in squared.split("\n")} == {2}


@pytest.mark.asyncio
async def test_the_status_column_shows_the_collapsed_state(make_config):
    """`Ready` is the operator's word; the row has to say which of the two
    keys applies, and Running/Starting/Stopped is that."""
    cm = _config(make_config, sessions=[_vm(phase="Ready")])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        assert app.query_one("#instances_table").get_row_at(0)[2] == "Running"


@pytest.mark.asyncio
async def test_connect_refuses_a_stopped_session(make_config):
    """Connect no longer starts anything. It used to, which meant enter on a
    halted VM replaced the launcher with a progress-dot wait as long as a cold
    boot, with no way back and no way to start it and go do something else."""
    cm = _config(make_config, sessions=[_stopped_vm()])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_connect_instance()
        await pilot.pause()
        assert app.is_running

    assert app.return_value is None
    assert cm.started == []      # and it did not quietly start it either


@pytest.mark.asyncio
async def test_start_declares_intent_on_a_stopped_session(make_config):
    """The same call the portal's play button makes: bump last-connect and let
    the operator create the pod / unhalt the VM. The launcher stays up and the
    poll shows the row come Running."""
    cm = _config(make_config, sessions=[_stopped_vm()])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_start_instance()
        await pilot.pause()
        assert app.is_running       # starting is not a handover

    assert cm.started == [("alice", "box")]


@pytest.mark.asyncio
async def test_start_leaves_a_running_session_alone(make_config):
    cm = _config(make_config, sessions=[_vm()])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_start_instance()
        await pilot.pause()

    assert cm.started == []


@pytest.mark.asyncio
async def test_start_with_nothing_selected_does_nothing(make_config):
    cm = _config(make_config)
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_start_instance()
        await pilot.pause()

    assert cm.started == []


@pytest.mark.asyncio
async def test_ssh_help_screen_shows_the_three_commands(make_config):
    """What the screen is for: the one invocation a user types by hand, and
    the two that write the files so they don't have to."""
    cm = _config(make_config, sessions=[_vm("box")])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_ssh_help()
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, SshHelpScreen)
        text = screen.help_text()
        # The explicit form, with the selected instance in it — this is the
        # one you can use before any config exists.
        assert "ssh -J alice@<gateway-host> alice@box.w" in text
        assert "ssh-config  >> ~/.ssh/config" in text
        assert "known-hosts >> ~/.ssh/known_hosts" in text


@pytest.mark.asyncio
async def test_ssh_help_screen_does_not_reprint_the_config_stanza(make_config):
    """It is 16 lines whose only use is being in a file, and the command
    beside it puts them there — while a terminal in mouse mode won't let you
    select them anyway. Its content is covered by
    tests/unit/test_gateway_commands.py, where it is actually served."""
    app = WhistlerApp(config_manager=_config(make_config), username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_ssh_help()
        await pilot.pause()
        text = app.screen.help_text()
        assert "Host whistler-gateway" not in text
        assert "AddKeysToAgent" not in text
        # Small enough to read at a glance on any terminal: it was 44 lines,
        # then 31. A guard against re-growth.
        assert len(text.splitlines()) <= 18


@pytest.mark.asyncio
async def test_stop_halts_a_running_session(make_config):
    """The reversible half of the pair: `stop_instance` deletes the pod or
    halts the VirtualMachine and leaves the Session CR, so `s` brings it
    back."""
    cm = _config(make_config, sessions=[_vm("box")])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_stop_instance()
        await pilot.pause()

    assert cm.stopped == [("alice", "box")]


@pytest.mark.asyncio
async def test_stop_works_for_container_sessions_too(make_config):
    """Both kinds are Session CRs and stop_instance picks the right mechanism
    by runtime — so the launcher needs no second stop path, even though a pod
    has no SSH to connect to."""
    cm = _config(make_config, instances=[_pod("scratch")])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_stop_instance()
        await pilot.pause()

    assert cm.stopped == [("alice", "scratch")]


@pytest.mark.asyncio
async def test_stop_leaves_an_already_stopped_session_alone(make_config):
    cm = _config(make_config, sessions=[_stopped_vm()])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_stop_instance()
        await pilot.pause()

    assert cm.stopped == []


@pytest.mark.asyncio
async def test_stop_with_nothing_selected_does_nothing(make_config):
    cm = _config(make_config)
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_stop_instance()
        await pilot.pause()

    assert cm.stopped == []


def test_delete_is_gone_from_the_launcher(make_config):
    """Deleting a session destroys its identity — a configuration change, so
    the portal owns it (2026-08-23). The launcher runs sessions; it does not
    change them. A re-added key here would put a destructive, unconfirmed
    action one keystroke from `s`."""
    import whistler.tui as tui
    assert not hasattr(tui.WhistlerApp, "action_delete")
    assert not any(b.action == "delete" for b in tui.WhistlerApp.BINDINGS)


def test_config_screens_are_gone():
    """Template and instance editing moved to the portal; a re-added screen
    here would resurrect the two-surfaces-drifting problem the diet solved."""
    import whistler.tui as tui
    for removed in ("InstanceCreateScreen", "TemplateEditScreen",
                    "TemplateViewScreen"):
        assert not hasattr(tui, removed)


@pytest.mark.asyncio
async def test_ssh_help_screen_releases_the_mouse_so_it_can_be_copied(make_config):
    """A terminal in xterm mouse-reporting mode routes drags to the app
    instead of selecting text — so the one screen whose whole purpose is text
    the user must paste into ~/.ssh was the one screen they could not select
    from. The screen turns reporting off while it is up and back on after."""
    app = WhistlerApp(config_manager=_config(make_config), username="alice")
    toggles = []

    async with app.run_test() as pilot:
        await pilot.pause()
        app.driver.set_mouse_tracking = toggles.append

        app.action_ssh_help()
        await pilot.pause()
        assert toggles == [False]

        await app.pop_screen()
        await pilot.pause()
        assert toggles == [False, True]
