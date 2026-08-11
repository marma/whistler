"""The slimmed launcher TUI (whistler/tui.py).

Smoke-level on purpose: these check that the launcher still composes, lists
what the user has, and hands a connect choice back to the session — the three
things the diet had to preserve — rather than pinning a layout that is meant
to keep changing.
"""
import pytest

from whistler.tui import SshHelpScreen, WhistlerApp


def _config(make_config, instances=()):
    return make_config(
        users={"alice": {"name": "alice", "publicKeys": []}},
        instances={"alice": list(instances)})


@pytest.mark.asyncio
async def test_lists_instances_with_their_ssh_address(make_config):
    cm = _config(make_config, [
        {"name": "box", "template": "ubuntu", "status": "Running"}])
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
async def test_connect_exits_with_the_chosen_instance(make_config):
    """The app cannot hand its own channel to a remote shell, so it exits with
    the choice and the session does the relaying."""
    cm = _config(make_config, [
        {"name": "box", "template": "ubuntu", "status": "Running"}])
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
    cm = _config(make_config, [
        {"name": "box", "template": "ubuntu", "status": "Running"}])
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
