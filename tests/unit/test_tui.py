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


@pytest.mark.asyncio
async def test_ssh_help_screen_shows_a_usable_config_stanza(make_config):
    cm = _config(make_config)
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_ssh_help()
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, SshHelpScreen)
        text = screen.help_text()
        assert "Host *.w" in text
        assert "ProxyJump alice@" in text
        assert "@cert-authority" in text   # from the fake's CA line


@pytest.mark.asyncio
async def test_delete_removes_the_instance(make_config):
    cm = _config(make_config, instances=[_pod("scratch")])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_delete()
        await pilot.pause()

    assert cm.deleted == [("alice", "scratch")]


@pytest.mark.asyncio
async def test_delete_works_for_vm_sessions_too(make_config):
    """Both kinds are Session CRs, and delete_instance removes one by name
    regardless of mode — so the launcher needs no second delete path now that
    it lists both."""
    cm = _config(make_config, sessions=[_vm("box")])
    app = WhistlerApp(config_manager=cm, username="alice")

    async with app.run_test() as pilot:
        await pilot.pause()
        app.query_one("#instances_table").focus()
        app.action_delete()
        await pilot.pause()

    assert cm.deleted == [("alice", "box")]


def test_config_screens_are_gone():
    """Template and instance editing moved to the portal; a re-added screen
    here would resurrect the two-surfaces-drifting problem the diet solved."""
    import whistler.tui as tui
    for removed in ("InstanceCreateScreen", "TemplateEditScreen",
                    "TemplateViewScreen"):
        assert not hasattr(tui, removed)
