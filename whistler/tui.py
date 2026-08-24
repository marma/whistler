"""The SSH gateway's terminal UI: an instance launcher.

List, start, stop, connect. Templates, instances, users and zones are
configured in the web portal, so there is one configuration surface rather
than two that drift out of sync — which is what the TUI's create/edit screens
had done. See design/proxyjump.md, "TUI diet".

Nothing here changes a session, only runs one. Start and stop are operations
on a workload — reversible, and the pair a terminal is good at. Delete was
the odd one out and moved to the portal (2026-08-23): it destroys the session
itself, which is a configuration change wearing an operation's clothes.

Starting and connecting are separate keys. Connect used to imply "start it if
it is off and wait", which read well and behaved badly: the screen was gone
for as long as a cold VM takes to boot, with no way back and no way to start
something and then do anything else. Now `s` declares the intent, the row
shows the state, and enter connects when it is Running.

What it keeps is what a terminal does better than a browser: see what you
have, get into it, get out. And it advertises the direct path (`ssh box.w`
through the gateway as a jump host), because for anything beyond a look that
is the better tool — it brings scp, rsync, port forwarding and IDE remotes
with it.
"""

from rich.cells import cell_len
from textual.binding import Binding
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, DataTable, Label
from textual.containers import Container, VerticalScroll
from textual.screen import ModalScreen, Screen
from whistler.status import status_group
import asyncio
import logging

logger = logging.getLogger("whistler.tui")

class LoadingScreen(Screen):
    """Full-screen loading screen with animated spinner."""
    
    CSS = """
    LoadingScreen {
        align: center middle;
        background: $surface;  /* Opaque background */
    }

    .loading-container {
        width: 60;
        height: auto;
        border: thick $accent;
        padding: 2;
        background: $surface;
        align: center middle;
    }

    .title {
        text-align: center;
        text-style: bold;
        color: $accent;
        margin-bottom: 2;
        width: 100%;
        content-align: center middle;
    }

    .spinner {
        text-align: center;
        text-style: bold;
        color: $warning;
        margin-bottom: 1;
        width: 100%;
        content-align: center middle;
    }

    .status {
        text-align: center;
        color: $text;
        margin-top: 1;
        width: 100%;
    }

    .help {
        text-align: center;
        color: $text-muted;
        margin-top: 1;
        width: 100%;
        text-style: italic;
    }
    """
    
    def __init__(self, initial_status: str = "Loading..."):
        super().__init__()
        self.status_message = initial_status
        self.spinner_state = 0
        self.spinner_chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        self.spinner_colors = ["#ff0000", "#ff7f00", "#ffff00", "#00ff00", "#0000ff", "#4b0082", "#9400d3"]
        self.color_index = 0
    
    def compose(self) -> ComposeResult:
        yield Container(
            Label("WHISTLER", classes="title"),
            Label("", id="spinner", classes="spinner"),
            Label(self.status_message, id="status", classes="status"),
            Label("(press ctrl-c to cancel)", classes="help"),
            classes="loading-container"
        )
    
    def action_cancel(self) -> None:
        self.app.exit("cancelled")
        
    def on_key(self, event) -> None:
        # Explicitly handle ctrl+c if binding doesn't catch it for some reason?
        # Actually standard bindings should work.
        pass
    
    BINDINGS = [
        Binding("ctrl+c", "cancel", "Cancel"),
    ]

    
    def on_mount(self) -> None:
        self.update_spinner()
        self.set_interval(0.1, self.update_spinner)
    
    def update_spinner(self) -> None:
        """Update the spinner animation."""
        spinner_label = self.query_one("#spinner", Label)
        char = self.spinner_chars[self.spinner_state]
        color = self.spinner_colors[self.color_index]
        
        # Use rich text for coloring
        spinner_label.update(f"[{color}]{char}[/] Loading...")
        
        self.spinner_state = (self.spinner_state + 1) % len(self.spinner_chars)
        self.color_index = (self.color_index + 1) % len(self.spinner_colors)
    
    def update_status(self, status: str) -> None:
        """Update the status message."""
        self.status_message = status
        try:
            status_label = self.query_one("#status", Label)
            status_label.update(status)
        except Exception:
            pass

GATEWAY_HOST_PLACEHOLDER = "<gateway-host>"


def _state(target: dict) -> str:
    """The user-facing state of one launcher row (whistler/status.py)."""
    return status_group(target.get("status"), target.get("ready", True))


def _square_block(art: str) -> str:
    """Pad every line of a block of text to the width of its widest line.

    Because `text-align: center` centers each line *independently*: a line one
    cell narrower than its neighbours lands one cell off, which is visible as a
    wobble in ASCII art even though nothing is wrong with the layout. The logo
    had exactly that — the ANSI Shadow `R` ends its top row with a trailing
    space, and trailing spaces do not survive contact with an editor that trims
    them. Padding here means they never need to: the source can hold the art
    ragged and it still renders square.

    Measured in cells rather than characters so a future logo with double-width
    glyphs pads correctly."""
    lines = art.strip("\n").split("\n")
    width = max(cell_len(line) for line in lines)
    return "\n".join(line + " " * (width - cell_len(line)) for line in lines)


LOGO = _square_block(r"""
██╗    ██╗██╗  ██╗██╗███████╗████████╗██╗     ███████╗██████╗
██║    ██║██║  ██║██║██╔════╝╚══██╔══╝██║     ██╔════╝██╔══██╗
██║ █╗ ██║███████║██║███████╗   ██║   ██║     █████╗  ██████╔╝
██║███╗██║██╔══██║██║╚════██║   ██║   ██║     ██╔══╝  ██╔══██╗
╚███╔███╔╝██║  ██║██║███████║   ██║   ███████╗███████╗██║  ██║
 ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝╚══════╝   ╚═╝   ╚══════╝╚══════╝╚═╝  ╚═╝""")


def ssh_config_stanza(username: str, suffix: str,
                      gateway_host: str = GATEWAY_HOST_PLACEHOLDER) -> str:
    """The ``~/.ssh/config`` a jump needs, as plain unindented text.

    One consumer now: ``ssh <gateway> ssh-config``, which prints it for
    redirection into the file. The `?` screen used to render it too and
    stopped — a terminal in mouse-reporting mode hands drags to the
    application instead of selecting text, so the on-screen copy was the one
    thing here nobody could actually copy, and it was most of the screen.
    """
    return "\n".join([
        "Host whistler-gateway",
        f"    HostName {gateway_host}",
        f"    User {username}",
        "    AddKeysToAgent yes",
        "    ControlMaster auto",
        "    ControlPath ~/.ssh/cm-%C",
        "    ControlPersist 10m",
        "",
        f"Host *{suffix}",
        "    ProxyJump whistler-gateway",
        f"    User {username}",
        "    AddKeysToAgent yes",
        "    ControlMaster auto",
        "    ControlPath ~/.ssh/cm-%C",
        "    ControlPersist 10m",
    ])


class SshHelpScreen(ModalScreen):
    """How to reach instances directly, without going through this TUI.

    The TUI is a launcher; for anything beyond a quick look the better path is
    the user's own ssh client, which brings scp/rsync/port-forwarding/VS Code
    with it (design/proxyjump.md). What that takes is two files and one flag,
    so this screen is the explicit `-J` invocation plus the two commands that
    write the files. Everything else it used to hold — the config stanza
    itself, agent-forwarding prose, scp examples — was screen-filling text
    that either lives in a file or in `man ssh_config`."""

    BINDINGS = [("escape,q,question_mark", "app.pop_screen", "Close")]

    CSS = """
    SshHelpScreen {
        align: center middle;
    }
    #help-box {
        /* Sized to the longest command line rather than to the old stanza,
           which was 16 lines wide and tall. Long usernames or instance names
           wrap inside it, which is why the width is a little over. */
        width: 72;
        height: auto;
        /* Still scrolls rather than clips: a terminal can be shorter than
           this, and a truncated command is worse than one you scroll to. */
        max-height: 100%;
        overflow-y: auto;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    #help-box Static { text-align: left; }
    """

    def __init__(self, username: str, suffix: str, known_hosts: str | None,
                 example: str):
        super().__init__()
        self.username = username
        self.suffix = suffix
        self.known_hosts = known_hosts
        self.example = example

    def help_text(self) -> str:
        """The instructions, as plain text — built here rather than inline in
        compose so it can be asserted on.

        Three commands and the sentence each one needs. The `~/.ssh/config`
        stanza used to be printed here in full, which made this the longest
        screen in the launcher to say something the user cannot act on by
        reading: it is 16 lines of text whose only use is being in a file, and
        `ssh <gateway> ssh-config` puts it there. What is worth showing is the
        explicit `-J` form, because that one *is* typed by hand — it is how
        you reach an instance before any config exists."""
        gw = f"{self.username}@{GATEWAY_HOST_PLACEHOLDER}"
        body = [
            "[b]Direct ssh access[/b] — access to running instances uses",
            "the SSH ProxyJump capability, which can be used explicitly:",
            "",
            f"    ssh -J {gw} {self.username}@{self.example}{self.suffix}",
            "",
            "Configuration for the proxy can be retrieved from the gateway",
            "and added permanently to [b]~/.ssh/config[/b]:",
            "",
            f"    ssh {gw} ssh-config  >> ~/.ssh/config",
        ]
        # Only when a host CA exists: the command would otherwise print
        # nothing, and telling someone to append nothing is worse than silence.
        if self.known_hosts:
            body += [
                "",
                "To retrieve the host CA's @cert-authority line and add it to",
                "known hosts:",
                "",
                f"    ssh {gw} known-hosts >> ~/.ssh/known_hosts",
            ]
        body += ["", "[dim]esc to close[/dim]"]
        return "\n".join(body)

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="help-box"):
            yield Static(self.help_text(), markup=True)

    # Mouse reporting off while this screen is up, restored on close. This
    # screen's entire purpose is text the user must get into their own files,
    # and a terminal in mouse-reporting mode routes drags to the application
    # instead of selecting — so the one screen that exists to be copied out of
    # was the one screen you could not copy out of. Nothing here needs the
    # mouse; it closes on a keypress. Guarded because a locally-run Textual
    # driver has no such hook (whistler/server.py, WhistlerDriver).

    def on_mount(self) -> None:
        self._set_mouse(False)

    def on_unmount(self) -> None:
        self._set_mouse(True)

    def _set_mouse(self, enabled: bool) -> None:
        setter = getattr(getattr(self.app, "driver", None),
                         "set_mouse_tracking", None)
        if setter:
            setter(enabled)


class WhistlerApp(App):
    """Instance launcher.

    List, start, stop, connect. Creating, editing and deleting sessions,
    templates, users and zones lives in the web portal — one configuration
    surface rather than two that drift apart. What is left is what a terminal
    is genuinely better at: see what you have, get into it, get out.

    Starting is a step of its own (`s`), not something connect does for you.
    Enter connects to a session that is *Running*; a stopped one is started
    and the row shows it coming up. Folding the two together meant enter on a
    stopped VM handed the screen to a progress-dot wait of up to a minute with
    no way back — and no way to start something and go do anything else."""

    CSS = """
    Screen {
        layout: vertical;
        align: center top;
    }

    .logo {
        color: green;
        text-align: center;
        margin: 1;
    }

    .welcome {
        text-align: center;
        color: $text;
    }

    DataTable {
        margin: 1;
        height: auto;
        max-height: 20;
        width: auto;
    }

    .section-header {
        margin-top: 1;
        text-align: center;
        width: 100%;
        text-style: bold;
    }

    .hint {
        text-align: center;
        color: $text-muted;
        margin-bottom: 1;
    }

    Static {
        width: 100%;
        text-align: center;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("enter", "connect_instance", "Connect", priority=True),
        Binding("c", "connect_instance", "Connect"),
        Binding("s", "start_instance", "Start"),
        Binding("S", "stop_instance", "Stop"),
        Binding("r", "refresh", "Refresh"),
        Binding("question_mark", "ssh_help", "ssh help"),
        Binding("d", "toggle_dark", "Toggle dark"),
    ]

    def __init__(self, config_manager=None, username=None, session=None, **kwargs):
        super().__init__(**kwargs)
        self.config_manager = config_manager
        self.username = username
        self.session = session
        self.cached_instances = []
        self._poll_task = None
        self._known_hosts = None

    @property
    def suffix(self) -> str:
        return getattr(self.config_manager, "ssh_domain_suffix", ".w")

    def compose(self) -> ComposeResult:
        logger.debug("WhistlerApp.compose")
        yield Header()
        yield Static(LOGO, classes="logo")
        yield Static("Your friendly terminal operator", classes="welcome")

        yield Label("Instances", classes="section-header")
        yield DataTable(id="instances_table")
        # Two short lines rather than one long one: at 80 columns a single
        # line wraps mid-phrase, and the second says the thing a launcher
        # cannot answer for itself — where sessions come from.
        yield Static(
            "s start  ·  S stop  ·  enter connect when Running  ·  "
            "? ssh setup"
            "\ncreate, edit and delete sessions in the web portal",
            classes="hint")

        yield Footer()

    def _setup_tables(self, size=None) -> None:
        width = (size.width if size else self.size.width) or 80
        col_width = max(10, (width - 4) // 4)
        try:
            table = self.query_one("#instances_table", DataTable)
            table.clear(columns=True)
            table.cursor_type = "row"
            table.add_column("Instance", width=col_width)
            table.add_column("Template", width=col_width)
            table.add_column("Status", width=col_width)
            # The address rather than the IP: what the user would type from
            # their own shell, so the launcher teaches the direct path by
            # showing it next to every row — and says so plainly when a
            # session has no SSH to offer.
            table.add_column("ssh", width=col_width)
        except Exception:
            # Widgets might not be ready yet
            pass

    async def on_mount(self) -> None:
        logger.debug("WhistlerApp.on_mount")
        self._setup_tables()
        await self._update_cache()
        self.refresh_data()
        self._poll_task = asyncio.create_task(self._poll_data_loop())

    async def _poll_data_loop(self):
        while True:
            await asyncio.sleep(5)
            await self._update_cache()
            self.refresh_data()

    async def _update_cache(self):
        if not self.config_manager or not self.username:
            return
        loop = asyncio.get_running_loop()
        try:
            self.cached_instances = await loop.run_in_executor(
                None, self.config_manager.list_ssh_targets, self.username)
            if self._known_hosts is None:
                self._known_hosts = await loop.run_in_executor(
                    None, self.config_manager.get_ssh_known_hosts_line) or ""
        except Exception as e:
            logger.error(f"Failed to update cache: {e}")

    def on_resize(self, event=None) -> None:
        self._setup_tables(event.size if event else None)
        self.refresh_data()

    def refresh_data(self) -> None:
        if not self.config_manager or not self.username:
            return
        try:
            table = self.query_one("#instances_table", DataTable)
        except Exception:
            return  # not on this screen

        selected = None
        if table.row_count > 0:
            try:
                selected = table.coordinate_to_cell_key(
                    table.cursor_coordinate).row_key.value
            except Exception:
                pass

        table.clear()
        for instance in self.cached_instances:
            name = instance.get("name", "Unknown")
            # Say what is true per row rather than showing an address that
            # would hang: a container session has no sshd, and its way in is
            # the portal's web terminal.
            address = (f"{name}{self.suffix}" if instance.get("sshReachable")
                       else "— web terminal only")
            table.add_row(
                name,
                instance.get("template", "Unknown"),
                # The collapsed state, not the raw phase: the two keys the row
                # offers are "start" and "connect", and which one applies is
                # exactly what Stopped/Starting/Running says. Ready, Booting
                # and Importing are the operator's words for the same three.
                _state(instance),
                address,
                key=name)

        if selected:
            try:
                table.move_cursor(row=table.get_row_index(selected))
            except Exception:
                pass

    def _get_selected_instance(self):
        try:
            table = self.query_one("#instances_table", DataTable)
            return table.get_row_at(table.cursor_coordinate.row)[0]
        except Exception:
            return None

    def _selected_target(self):
        name = self._get_selected_instance()
        if not name:
            return None
        return next((t for t in self.cached_instances if t.get("name") == name),
                    None)

    def action_ssh_help(self) -> None:
        example = self._get_selected_instance() or "<instance>"
        self.push_screen(SshHelpScreen(
            self.username, self.suffix, self._known_hosts or None, example))

    def action_connect_instance(self) -> None:
        """Hand this session over to the instance.

        The app exits with the choice and the *session* does the relaying
        (whistler/relay.py) — a Textual app cannot hand its own channel over,
        and the session drops back into a fresh TUI when the remote shell
        ends."""
        target = self._selected_target()
        if not target:
            self.notify("No instance selected.")
            return
        if not target.get("sshReachable"):
            # Refuse here rather than letting the relay spend its whole
            # connect budget failing: a container has no sshd to wait for.
            self.notify(
                f"{target['name']} has no SSH — open it in the portal's web "
                f"terminal instead.", severity="warning")
            return
        state = _state(target)
        if state != "Running":
            # Connect no longer starts anything. Same reason as above, one step
            # earlier: this key hands the screen to a relay, and a session that
            # is off (or still booting) has nothing to hand it to.
            if state in ("Stopped", "Error"):
                self.notify(f"{target['name']} is not running — press s to "
                            f"start it.", severity="warning")
            else:
                self.notify(f"{target['name']} is {state.lower()} — connect "
                            f"when it says Running.")
            return
        self.exit(("connect", target["name"]))

    def action_start_instance(self) -> None:
        """Start a stopped session and stay here while it boots.

        Declaring intent is all this does: `trigger_instance_start` bumps the
        Session CR's last-connect annotation, the operator's reconcile creates
        the pod or unhalts the VM, and the five-second poll shows the row walk
        Stopped -> Starting -> Running. The portal's play button is the same
        call, so the two surfaces cannot disagree about what starting means."""
        target = self._selected_target()
        if not target:
            self.notify("No instance selected.")
            return
        name = target["name"]
        state = _state(target)
        if state not in ("Stopped", "Error"):
            self.notify(f"{name} is already {state.lower()}.")
            return

        self.notify(f"Starting {name}...")

        async def do_start():
            loop = asyncio.get_running_loop()
            ok = await loop.run_in_executor(
                None, self.config_manager.trigger_instance_start,
                self.username, name)
            if not ok:
                self.notify(f"Failed to start {name}.", severity="error")
            await self._refresh_async()

        asyncio.create_task(do_start())

    def action_stop_instance(self) -> None:
        """Stop the workload, keeping the session.

        `stop_instance` deletes the pod or halts the VirtualMachine; the
        Session CR, the home volume and a VM's root disk all survive, so `s`
        brings it back. That is the whole reason this key replaced delete
        (2026-08-23): stopping is the reversible half of the pair the launcher
        already had a start for, and **deleting is a change, not an
        operation** — it destroys a session's identity and belongs on the one
        surface that owns configuration, the portal.

        Deliberately no confirmation prompt: the worst case of a mis-stop is a
        reboot, and the key that undoes it is the one beside it. Delete had no
        prompt either and badly needed one — a third reason it is better off
        in the portal.

        Uppercase `S` so it cannot be a slip of the `s` that starts. A toggle
        on one key would have been tidier and is exactly wrong here — the two
        directions are not equally cheap, and the expensive one should need
        the shift.
        """
        target = self._selected_target()
        if not target:
            self.notify("No instance selected.")
            return
        name = target["name"]
        # The portal's rule, verbatim: anything already stopped or on its way
        # down has nothing to stop.
        if _state(target) in ("Stopped", "Stopping"):
            self.notify(f"{name} is not running.")
            return

        self.notify(f"Stopping {name}...")

        async def do_stop():
            loop = asyncio.get_running_loop()
            ok = await loop.run_in_executor(
                None, self.config_manager.stop_instance, self.username, name)
            if not ok:
                self.notify(f"Failed to stop {name}.", severity="error")
            await self._refresh_async()

        asyncio.create_task(do_stop())

    def action_refresh(self) -> None:
        asyncio.create_task(self._refresh_async())

    async def _refresh_async(self):
        await self._update_cache()
        self.refresh_data()

    def action_toggle_dark(self) -> None:
        self.theme = ("textual-light" if self.theme == "textual-dark"
                      else "textual-dark")

    @property
    def driver(self):
        return getattr(self, "_driver", None)

if __name__ == "__main__":
    # Render the launcher against a live cluster, outside SSH. Handy for
    # working on the layout without a round trip through the gateway.
    import argparse

    from whistler.config import KubeConfigManager

    parser = argparse.ArgumentParser(description="Run the Whistler TUI locally")
    parser.add_argument("--kubeconfig", help="Path to a kubeconfig file")
    parser.add_argument("--user", required=True, help="Username to render as")
    args = parser.parse_args()

    WhistlerApp(config_manager=KubeConfigManager(kubeconfig=args.kubeconfig),
                username=args.user).run()
