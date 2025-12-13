from textual.binding import Binding
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, DataTable, Input, Button, Label, Select, Checkbox
from textual.containers import Container
from textual.screen import ModalScreen, Screen
import asyncio

class InstanceCreateScreen(ModalScreen):
    BINDINGS = [("escape", "app.pop_screen", "Close")]
    
    CSS = """
    InstanceCreateScreen {
        align: center middle;
    }

    .main-container {
        width: 60;
        height: auto;
        border: solid green;
        padding: 1;
        background: $surface;
    }

    .input-grid {
        layout: grid;
        grid-size: 2;
        grid-columns: 1fr 2fr;
        width: 100%;
        margin-bottom: 1;
    }

    .input-grid Label {
        padding: 1;
        text-align: left;
        align-vertical: middle;
    }

    .header {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    .buttons {
        width: 100%;
        height: auto;
        align: center middle;
        layout: horizontal;
    }

    Button {
        margin: 1;
    }

    .checkbox-container {
        width: 100%;
        height: auto;
        align: center middle;
        margin-bottom: 1;
    }
    """

    def compose(self) -> ComposeResult:
        yield Container(
            Label("Create New Instance", classes="header"),
            Container(
                Label("Instance Name:"),
                Input(placeholder="e.g. my-instance", id="instance_name"),
                classes="input-grid"
            ),
            Container(
                Checkbox("Preemptible (lower priority)", id="preemptible"),
                classes="checkbox-container"
            ),
            Container(
                Button("Create", variant="primary", id="create_btn"),
                Button("Cancel", variant="error", id="cancel_btn"),
                classes="buttons"
            ),
            classes="main-container"
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "create_btn":
            name = self.query_one("#instance_name", Input).value
            preemptible = self.query_one("#preemptible", Checkbox).value
            if name:
                self.dismiss({"name": name, "preemptible": preemptible})
        elif event.button.id == "cancel_btn":
            self.dismiss(None)

class TemplateEditScreen(ModalScreen):
    BINDINGS = [("escape", "app.pop_screen", "Cancel")]
    
    CSS = """
    TemplateEditScreen {
        align: center middle;
    }

    .main-container {
        width: 70;
        height: auto;
        max-height: 90vh;
        overflow-y: auto;
        border: round orange;
        padding: 1;
        margin: 0;
        background: $surface;
    }

    .input-grid {
        layout: grid;
        grid-size: 2;
        grid-columns: 1fr 2fr;
        width: 100%;
        height: auto;
        grid-gutter: 0;
    }

    .input-grid Label {
        padding: 1;
        height: auto;
        text-align: left;
        align-vertical: middle;
    }

    .input-grid Input {
        height: auto;
        min-height: 3;
        text-align: left;
    }

    .input-grid Select {
        height: auto;
        min-height: 3;
    }

    .input-grid Checkbox {
        height: auto;
        min-height: 3;
    }

    .header {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    .buttons {
        width: 100%;
        height: auto;
        align: center middle;
        layout: horizontal;
    }

    .inputs {
        text-align: left;
        align-vertical: middle;
        padding: 0;
        margin: 0;
    }

    .labels {
        text-align: left;
        align-vertical: middle;
        padding: 0;
        margin: 0;
    }

    Button {
        margin: 1;
        text-align: left;
    }

    Input {
        border: round grey;
        
    }

    Select {
        border: none;
        height: auto;
        min-height: 3;
    }

    #advanced_container {
        height: auto;
        margin-top: 1;
    }

    #volumes_container {
        height: auto;
        margin-top: 1;
    }
    """

    def __init__(self, template: dict | None = None):
        super().__init__()
        self.template = template or {}

    def compose(self) -> ComposeResult:
        resources = self.template.get("resources", {})
        node_selector = self.template.get("nodeSelector", {})
        
        # Get selectors from config
        selectors_list = self.app.config_manager.get_selectors() if self.app.config_manager else []
        
        # Prepare dynamic widgets list
        dynamic_widgets = []
        
        if isinstance(selectors_list, list):
            for i, selector in enumerate(selectors_list):
                 name = selector.get("name", "Unknown")
                 key = selector.get("key")
                 values = selector.get("values", [])
                 
                 if not key:
                     continue
                     
                 # Convert values to Select options
                 options = [(v, v) for v in values]
                 
                 # Current value
                 current_val = node_selector.get(key, Select.BLANK)
                 
                 # Widget ID
                 widget_id = f"sel_{i}"
                 
                 dynamic_widgets.append(Label(f"{name}:"))
                 dynamic_widgets.append(Select(options, value=current_val, prompt=f"Select {name}", id=widget_id))

        yield Container(
            Label("Template Details", classes="header"),
            Container(
                Label("Name:"),
                Input(value=self.template.get("name", ""), placeholder="e.g. my-template", id="name"),
                Label("Description:"),
                Input(value=self.template.get("description", ""), placeholder="e.g. My custom template", id="description"),
                Label("Image:"),
                Input(value=self.template.get("image", ""), placeholder="e.g. ubuntu:latest", id="image"),
                Label("CPU:"),
                Input(value=resources.get("cpu", ""), placeholder="e.g. 500m", id="cpu"),
                Label("Memory:"),
                Input(value=resources.get("memory", ""), placeholder="e.g. 512Mi", id="memory"),
                Label("GPU (optional):"),
                Input(value=resources.get("gpu", ""), placeholder="e.g. 1", id="gpu"),
                classes="input-grid"
            ),
            
            Container(
                Label("Node Selectors:", classes="header"),
                Container(
                    *dynamic_widgets,
                    classes="input-grid"
                ),
                id="advanced_container"
            ),

            Container(
                Label("Volumes:", classes="header"),
                Container(
                    Label("User volume:"),
                    Input(value=self.template.get("personalMountPath", "/userdata"), placeholder="/userdata", id="personal_mount_path"),
                    *self._create_volume_widgets(),
                    classes="input-grid"
                ),
                id="volumes_container"
            ),

            Container(
                Button("Save", variant="primary", id="save_btn"),
                Button("Cancel", variant="error", id="cancel_btn"),
                classes="buttons"
            ),
            classes="main-container"
        )

    def _create_volume_widgets(self):
        widgets = []
        volumes_list = self.app.config_manager.get_volumes() if self.app.config_manager else []
        current_volumes = self.template.get("volumes", {})
        
        if isinstance(volumes_list, list):
            for i, vol in enumerate(volumes_list):
                 vol_name = vol.get("name")
                 if not vol_name: continue
                 
                 is_checked = vol_name in current_volumes
                 # Default path is /<name> if not specified, otherwise use saved path
                 path_val = current_volumes.get(vol_name, f"/{vol_name}")
                 
                 widgets.append(Checkbox(vol_name, value=is_checked, id=f"vol_chk_{i}"))
                 widgets.append(Input(value=path_val, placeholder=f"/{vol_name}", id=f"vol_path_{i}"))
        return widgets

    def action_cancel(self) -> None:
        self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save_btn":
            name = self.query_one("#name", Input).value
            image = self.query_one("#image", Input).value
            cpu = self.query_one("#cpu", Input).value
            memory = self.query_one("#memory", Input).value
            gpu = self.query_one("#gpu", Input).value

            # Collect selectors
            node_selector = {}
            
            selectors_list = self.app.config_manager.get_selectors() if self.app.config_manager else []
            if isinstance(selectors_list, list):
                for i, selector in enumerate(selectors_list):
                    key = selector.get("key")
                    widget_id = f"sel_{i}"
                    try:
                        val = self.query_one(f"#{widget_id}", Select).value
                        if val != Select.BLANK:
                            node_selector[key] = val
                    except:
                        pass

            # Collect volumes
            volumes = {}
            volumes_list = self.app.config_manager.get_volumes() if self.app.config_manager else []
            if isinstance(volumes_list, list):
                for i, vol in enumerate(volumes_list):
                    vol_name = vol.get("name")
                    try:
                        checked = self.query_one(f"#vol_chk_{i}", Checkbox).value
                        if checked:
                            path = self.query_one(f"#vol_path_{i}", Input).value
                            if not path:
                                path = f"/{vol_name}"
                            volumes[vol_name] = path
                    except Exception:
                        pass

            if name and image:
                template_data = {
                    "name": name,
                    "description": self.query_one("#description", Input).value,
                    "image": image,
                    "resources": {
                        "cpu": cpu,
                        "memory": memory
                    },
                    "personalMountPath": self.query_one("#personal_mount_path", Input).value or "/userdata",
                    "nodeSelector": node_selector,
                    "volumes": volumes
                }
                if gpu:
                    template_data["resources"]["gpu"] = gpu
                self.dismiss(template_data)
            else:
                self.notify("Name and Image are required.", severity="error")
        elif event.button.id == "cancel_btn":
            self.dismiss(None)

class TemplateViewScreen(ModalScreen):
    BINDINGS = [("escape", "app.pop_screen", "Close"), ("e", "edit", "Edit Template")]
    
    CSS = """
    TemplateViewScreen {
        align: center middle;
    }

    .main-container {
        width: 70;
        height: auto;
        border: solid green;
        padding: 1;
        background: $surface;
    }

    .details-grid {
        layout: grid;
        grid-size: 2;
        grid-columns: 1fr 2fr;
        height: auto;
        width: 100%;
        margin-bottom: 1;
    }

    .details-grid Label {
        padding: 1;
        text-align: left;
        width: 100%;
    }
    
    .label-key {
        text-style: bold;
    }

    .value {
        color: $text-muted;
    }

    .buttons {
        width: 100%;
        height: auto;
        align: center middle;
        layout: horizontal;
    }

    Button {
        margin: 1;
    }
    """

    def __init__(self, template: dict):
        super().__init__()
        self.template = template

    def compose(self) -> ComposeResult:
        resources = self.template.get("resources", {})
        node_selector = self.template.get("nodeSelector", {})
        volumes = self.template.get("volumes", {})
        
        # Format node selector for display
        ns_str = "\n".join([f"{k}: {v}" for k, v in node_selector.items()]) if node_selector else "None"
        # Format volumes for display
        vol_str = "\n".join([f"{k} -> {v}" for k, v in volumes.items()]) if volumes else "None"

        yield Container(
            Label("Template Details", classes="label-key"),
            Container(
                Label("Name:", classes="label-key"),
                Label(self.template.get("name", ""), classes="value"),
                
                Label("Description:", classes="label-key"),
                Label(self.template.get("description", ""), classes="value"),
                
                Label("Image:", classes="label-key"),
                Label(self.template.get("image", ""), classes="value"),
                
                Label("CPU:", classes="label-key"),
                Label(str(resources.get("cpu", "")), classes="value"),
                
                Label("Memory:", classes="label-key"),
                Label(str(resources.get("memory", "")), classes="value"),
                
                Label("GPU:", classes="label-key"),
                Label(str(resources.get("gpu", "None")), classes="value"),
                
                Label("Node Selector:", classes="label-key"),
                Label(ns_str, classes="value"),
                
                Label("Volumes:", classes="label-key"),
                Label(vol_str, classes="value"),

                Label("Personal Mount Path:", classes="label-key"),
                Label(self.template.get("personalMountPath", "/userdata"), classes="value"),


                Label("Source:", classes="label-key"),
                Label(self.template.get("source", "user"), classes="value"),
                
                classes="details-grid"
            ),
            Container(
                Button("Edit", variant="primary", id="edit_btn", disabled=self.template.get("source") == "system"),
                Button("Close", variant="default", id="close_btn"),
                classes="buttons"
            ),
            classes="main-container"
        )

    def action_edit(self) -> None:
        if self.template.get("source") != "system":
            self.dismiss("edit")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "edit_btn":
            self.action_edit()
        elif event.button.id == "close_btn":
            self.dismiss(None)

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
            classes="loading-container"
        )
    
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

class WhistlerApp(App):
    """A Textual app to manage Kubernetes pods via SSH."""

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
        height: 10;
        width: auto;
    }

    .section-header {
        margin-top: 1;
        text-align: center;
        width: 100%;
        text-style: bold;
    }

    Static {
        width: 100%;
        text-align: center;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("d", "toggle_dark", "Toggle dark"),
        Binding("n", "create_template", "New Template"),
        Binding("c", "connect_instance", "Connect"),
        Binding("i", "instantiate", "Create Instance"),
        Binding("D", "delete_instance", "Delete Instance"),
        Binding("r", "refresh", "refresh"),
    ]


    def compose(self) -> ComposeResult:
        import sys
        print("WhistlerApp.compose", file=sys.stderr, flush=True)
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
        
        yield Label("Templates", classes="section-header")
        yield DataTable(id="templates_table")
        
        yield Label("Instances", classes="section-header")
        yield DataTable(id="instances_table")
        
        yield Footer()

    def __init__(self, config_manager=None, username=None, session=None, **kwargs):
        super().__init__(**kwargs)
        self.config_manager = config_manager
        self.username = username
        self.session = session
        self.cached_templates = []
        self.cached_instances = []
        self._poll_task = None

    def _setup_tables(self, size=None) -> None:
        # Calculate column width (screen width - margins) // number of columns
        # We have 5 columns in templates table now
        width = size.width if size else self.size.width
        if width == 0:
            # Fallback if size is not yet available
            width = 80
            
        col_width = max(10, (width - 4) // 5)
        
        # Setup Templates Table
        try:
            templates_table = self.query_one("#templates_table", DataTable)
            templates_table.clear(columns=True)
            templates_table.cursor_type = "row"
            templates_table.add_column("Template Name", width=col_width)
            templates_table.add_column("Image", width=col_width)
            templates_table.add_column("CPU", width=col_width)
            templates_table.add_column("Memory", width=col_width)
            templates_table.add_column("Source", width=col_width)
            
            # Setup Instances Table
            # Instances table has 4 columns
            inst_col_width = max(10, (width - 4) // 4)
            instances_table = self.query_one("#instances_table", DataTable)
            instances_table.clear(columns=True)
            instances_table.cursor_type = "row"
            instances_table.add_column("Instance Name", width=inst_col_width)
            instances_table.add_column("Template", width=inst_col_width)
            instances_table.add_column("Status", width=inst_col_width)
            instances_table.add_column("IP", width=inst_col_width)
        except Exception:
            # Widgets might not be ready yet
            pass

    async def on_mount(self) -> None:
        import sys
        print("WhistlerApp.on_mount", file=sys.stderr, flush=True)
        self._setup_tables()
        # Initial fetch
        await self._update_cache()
        self.refresh_data()
        # Start polling
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
            import sys
            import time
            start_time = time.perf_counter()
            # print("Fetching data from Kubernetes...", file=sys.stderr)
            # Run blocking K8s calls in executor
            self.cached_templates = await loop.run_in_executor(
                None, self.config_manager.get_user_templates, self.username
            )
            self.cached_instances = await loop.run_in_executor(
                None, self.config_manager.get_user_instances, self.username
            )
        except Exception as e:
            import sys
            print(f"Failed to update cache: {e}", file=sys.stderr)

    def on_resize(self, event=None) -> None:
        if event:
             self._setup_tables(event.size)
        else:
             self._setup_tables()
        self.refresh_data()

    def refresh_data(self) -> None:
        if not self.config_manager or not self.username:
            return

        # Refresh Templates
        try:
            templates_table = self.query_one("#templates_table", DataTable)
        except Exception:
            # Widgets not found (likely on a different screen), skip refresh
            return
        
        # Save selection
        selected_template = None
        if templates_table.row_count > 0:
             try:
                 selected_template = templates_table.coordinate_to_cell_key(templates_table.cursor_coordinate).row_key.value
             except:
                 pass

        templates_table.clear()
        # Use cached data
        for template in self.cached_templates:
            resources = template.get("resources", {})
            source = template.get("source", "user")
            templates_table.add_row(
                template.get("name", "Unknown"),
                template.get("image", "Unknown"),
                resources.get("cpu", "-"),
                resources.get("memory", "-"),
                source,
                key=template.get("name") # Use name as row key
            )
            
        # Restore selection
        if selected_template:
            try:
                # Find the row index for the key
                row_index = templates_table.get_row_index(selected_template)
                templates_table.move_cursor(row=row_index)
            except:
                pass

        # Refresh Instances
        instances_table = self.query_one("#instances_table", DataTable)
        
        # Save selection
        selected_instance = None
        if instances_table.row_count > 0:
             try:
                 selected_instance = instances_table.coordinate_to_cell_key(instances_table.cursor_coordinate).row_key.value
             except:
                 pass

        instances_table.clear()
        # Use cached data
        for instance in self.cached_instances:
            resources = instance.get("resources", {})
            instances_table.add_row(
                instance.get("name", "Unknown"),
                instance.get("template", "Unknown"),
                instance.get("status", "Unknown"),
                instance.get("ip") or "-",
                key=instance.get("name")
            )
            
        # Restore selection
        if selected_instance:
            try:
                row_index = instances_table.get_row_index(selected_instance)
                instances_table.move_cursor(row=row_index)
            except:
                pass

    def action_instantiate(self) -> None:
        templates_table = self.query_one("#templates_table", DataTable)
        if not templates_table.has_focus:
            self.notify("Select a template first.")
            return

        try:
            row_key = templates_table.coordinate_to_cell_key(templates_table.cursor_coordinate).row_key
            template_name = row_key.value
        except Exception:
            self.notify("No template selected.")
            return

        def create_instance(result: dict | None) -> None:
            if result:
                instance_name = result["name"]
                preemptible = result["preemptible"]
                self.notify(f"Creating instance {instance_name}...")
                
                async def do_create():
                    loop = asyncio.get_running_loop()
                    success = await loop.run_in_executor(
                        None, 
                        lambda: self.config_manager.add_instance(self.username, template_name, instance_name, preemptible=preemptible)
                    )
                    if success:
                        self.notify(f"Instance {instance_name} created!")
                        asyncio.create_task(self._refresh_async())
                    else:
                        self.notify("Failed to create instance (name might exist).", severity="error")
                
                asyncio.create_task(do_create())

        self.push_screen(InstanceCreateScreen(), create_instance)

    def action_create_template(self) -> None:
        def save_template(template_data: dict | None) -> None:
            if template_data:
                # New templates are always user source
                template_data["source"] = "user"
                self.notify(f"Saving template {template_data['name']}...")
                
                async def do_save():
                    loop = asyncio.get_running_loop()
                    success = await loop.run_in_executor(
                        None,
                        lambda: self.config_manager.save_template(self.username, template_data)
                    )
                    if success:
                        self.notify(f"Template {template_data['name']} saved!")
                        asyncio.create_task(self._refresh_async())
                    else:
                        self.notify("Failed to save template.", severity="error")
                
                asyncio.create_task(do_save())
        
        self.push_screen(TemplateEditScreen(), save_template)

    def _get_selected_template(self):
        templates_table = self.query_one("#templates_table", DataTable)
        if not templates_table.has_focus:
            self.notify("Select a template first.")
            return None

        try:
            row_key = templates_table.coordinate_to_cell_key(templates_table.cursor_coordinate).row_key
            template_name = row_key.value
        except Exception:
            self.notify("No template selected.")
            return None

        templates = self.cached_templates
        return next((t for t in templates if t["name"] == template_name), None)

    def edit_template_internal(self, template: dict) -> None:
        if template.get("source") == "system":
            self.notify("Cannot edit system templates.", severity="error")
            return

        def save_template(template_data: dict | None) -> None:
            if template_data:
                # Preserve source (should be user)
                template_data["source"] = "user"
                self.notify(f"Updating template {template_data['name']}...")
                
                async def do_save():
                    loop = asyncio.get_running_loop()
                    success = await loop.run_in_executor(
                        None,
                        lambda: self.config_manager.save_template(self.username, template_data)
                    )
                    if success:
                        self.notify(f"Template {template_data['name']} updated!")
                        asyncio.create_task(self._refresh_async())
                    else:
                        self.notify("Failed to save template.", severity="error")
                
                asyncio.create_task(do_save())
        
        self.push_screen(TemplateEditScreen(template), save_template)

    def action_edit_template(self) -> None:
        template = self._get_selected_template()
        if template:
            self.edit_template_internal(template)

    def action_view_template(self) -> None:
        template = self._get_selected_template()
        if template:
            def on_close(result: str | None) -> None:
                if result == "edit":
                    self.edit_template_internal(template)
            
            self.push_screen(TemplateViewScreen(template), on_close)

    def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.theme = "textual-light" if self.theme == "textual-dark" else "textual-dark"

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id == "templates_table":
            self.action_view_template()

    def _get_selected_instance(self):
        instances_table = self.query_one("#instances_table", DataTable)
        if not instances_table.has_focus:
            return None

        try:
            row_index = instances_table.cursor_coordinate.row
            # Get the first cell of the row (Instance Name)
            return instances_table.get_row_at(row_index)[0]
        except Exception:
            return None

    def action_delete_instance(self) -> None:
        instances_table = self.query_one("#instances_table", DataTable)
        if not instances_table.has_focus:
            self.notify("Select an instance first.")
            return

        instance_name = self._get_selected_instance()
        if not instance_name:
            self.notify("No instance selected.")
            return

        self.notify(f"Deleting instance {instance_name}...")
        
        async def do_delete():
            loop = asyncio.get_running_loop()
            success = await loop.run_in_executor(
                None,
                lambda: self.config_manager.delete_instance(self.username, instance_name)
            )
            if success:
                self.notify(f"Instance {instance_name} deleted.")
                asyncio.create_task(self._refresh_async())
            else:
                self.notify(f"Failed to delete instance {instance_name}.", severity="error")
        
        asyncio.create_task(do_delete())

    async def _refresh_async(self):
        await self._update_cache()
        self.refresh_data()

    def action_connect_instance(self) -> None:
        instances_table = self.query_one("#instances_table", DataTable)
        if not instances_table.has_focus:
            self.notify("Select an instance first.")
            return

        instance_name = self._get_selected_instance()
        if not instance_name:
            self.notify("No instance selected.")
            return

        self.notify(f"Connecting to {instance_name}... (Not implemented yet)")

    @property
    def driver(self):
        return getattr(self, "_driver", None)

if __name__ == "__main__":
    import argparse
    import sys
    import os
    
    # Add parent directory to path to allow importing whistler modules
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    from whistler.config import ConfigManager

    parser = argparse.ArgumentParser(description="Run Whistler TUI")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--user", help="Username to load from config")
    args = parser.parse_args()

    config_manager = None
    username = None

    if args.config:
        config_manager = ConfigManager(args.config)
        if args.user:
            username = args.user
        elif config_manager.config.get("users"):
            # Default to first user if not specified
            username = next(iter(config_manager.config["users"]))
            print(f"No user specified, defaulting to: {username}")

    app = WhistlerApp(config_manager=config_manager, username=username)
    app.run()
