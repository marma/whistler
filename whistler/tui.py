"""The SSH gateway's terminal UI: an instance launcher.

Read-and-connect only. Templates, instances, users and zones are configured
in the web portal, so there is one configuration surface rather than two that
drift out of sync — which is what the TUI's create/edit screens had done. See
design/proxyjump.md, "TUI diet".

What it keeps is what a terminal does better than a browser: see what you
have, get into it, get out. And it advertises the direct path (`ssh box.w`
through the gateway as a jump host), because for anything beyond a look that
is the better tool — it brings scp, rsync, port forwarding and IDE remotes
with it.
"""

from textual.binding import Binding
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, DataTable, Label
from textual.containers import Container
from textual.screen import ModalScreen, Screen
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

class SshHelpScreen(ModalScreen):
    """How to reach instances directly, without going through this TUI.

    Deliberately prominent: the TUI is a launcher, and the better path for
    everything except a quick look is the user's own ssh client, which gets
    scp/rsync/port-forwarding/VS Code for free (design/proxyjump.md)."""

    BINDINGS = [("escape,q,question_mark", "app.pop_screen", "Close")]

    CSS = """
    SshHelpScreen {
        align: center middle;
    }
    #help-box {
        width: 78;
        height: auto;
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
        compose so it can be asserted on, and reused wherever else the hint
        needs printing."""
        host = "<gateway-host>"
        body = [
            "[b]Connect straight to an instance[/b]",
            "",
            "Add this once to [b]~/.ssh/config[/b]:",
            "",
            f"    Host *{self.suffix}",
            f"        ProxyJump {self.username}@{host}",
            f"        User {self.username}",
            "",
            "Then, from your own shell:",
            "",
            f"    ssh {self.example}{self.suffix}",
            f"    scp report.pdf {self.example}{self.suffix}:",
            f"    rsync -a data/ {self.example}{self.suffix}:data/",
            "",
            "Naming an instance that doesn't exist yet creates it from the",
            "template of that name and waits for it to boot.",
        ]
        if self.known_hosts:
            body += [
                "",
                "And this once to [b]~/.ssh/known_hosts[/b], so no instance",
                "ever asks you to trust a new host key:",
                "",
                f"    {self.known_hosts}",
            ]
        body += ["", "[dim]esc to close[/dim]"]
        return "\n".join(body)

    def compose(self) -> ComposeResult:
        with Container(id="help-box"):
            yield Static(self.help_text(), markup=True)


class WhistlerApp(App):
    """Instance launcher.

    Read-and-connect only, by design. Creating and editing templates,
    instances, users and zones lives in the web portal — one configuration
    surface rather than two that drift apart. What is left is what a terminal
    is genuinely better at: see what you have, get into it, get out."""

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
        Binding("D", "delete", "Delete"),
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
        logo = r"""
██╗    ██╗██╗  ██╗██╗███████╗████████╗██╗     ███████╗██████╗
██║    ██║██║  ██║██║██╔════╝╚══██╔══╝██║     ██╔════╝██╔══██╗
██║ █╗ ██║███████║██║███████╗   ██║   ██║     █████╗  ██████╔╝
██║███╗██║██╔══██║██║╚════██║   ██║   ██║     ██╔══╝  ██╔══██╗
╚███╔███╔╝██║  ██║██║███████║   ██║   ███████╗███████╗██║  ██║
 ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝╚══════╝   ╚═╝   ╚══════╝╚══════╝╚═╝  ╚═╝"""

        yield Static(logo, classes="logo")
        yield Static("Your friendly terminal operator", classes="welcome")

        yield Label("Instances", classes="section-header")
        yield DataTable(id="instances_table")
        yield Static(
            "enter to connect  ·  ? for direct ssh access  ·  "
            "manage templates and instances in the web portal",
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
            # their own shell, so the TUI teaches the direct path by showing
            # it next to every row.
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
                None, self.config_manager.get_user_instances, self.username)
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
            table.add_row(
                name,
                instance.get("template", "Unknown"),
                instance.get("status", "Unknown"),
                f"{name}{self.suffix}",
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
        instance_name = self._get_selected_instance()
        if not instance_name:
            self.notify("No instance selected.")
            return
        self.exit(("connect", instance_name))

    def action_delete(self) -> None:
        instance_name = self._get_selected_instance()
        if not instance_name:
            self.notify("No instance selected.")
            return

        self.notify(f"Deleting instance {instance_name}...")

        async def do_delete():
            loop = asyncio.get_running_loop()
            success = await loop.run_in_executor(
                None, self.config_manager.delete_instance,
                self.username, instance_name)
            if success:
                self.notify(f"Instance {instance_name} deleted.")
                asyncio.create_task(self._refresh_async())
            else:
                self.notify(f"Failed to delete instance {instance_name}.",
                            severity="error")

        asyncio.create_task(do_delete())

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
