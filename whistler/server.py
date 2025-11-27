import asyncio
import asyncssh
import sys
import os
import pty
import fcntl
import termios
import struct
import traceback
from textual.driver import Driver
from textual.app import App
from textual.geometry import Size
from textual.events import Resize
from textual._xterm_parser import XTermParser
from whistler.tui import WhistlerApp

import argparse
from functools import partial
from functools import partial
from whistler.config import ConfigManager, YamlConfigManager, KubeConfigManager
from asyncio import Event

class WhistlerDriver(Driver):
    def __init__(self, next_driver: Driver | None = None, *, debug: bool = False, size: tuple[int, int] | None = None, **kwargs):
        super().__init__(next_driver, debug=debug, size=size)
        self._parser = XTermParser(debug=debug)
        self.exit_event = Event()
        print("WhistlerDriver initialized", file=sys.stderr, flush=True)

    def write(self, data: str | bytes) -> None:
        # print(f"WhistlerDriver.write: {len(data)} bytes", file=sys.stderr, flush=True)
        if self._app and self._app.ssh_channel:
            if isinstance(data, str):
                data = data.encode('utf-8')
            self._app.ssh_channel.write(data)

    def flush(self) -> None:
        # No explicit flush needed for asyncssh channel write
        pass

    def start_application_mode(self) -> None:
        print("WhistlerDriver.start_application_mode", file=sys.stderr, flush=True)
        # Send initial size event
        size = (80, 24) # Default fallback
        if self._app and hasattr(self._app, 'session') and self._app.session:
             size = self._app.session.initial_term_size
             print(f"Using initial_term_size from session: {size}", file=sys.stderr, flush=True)
        elif self._app and self._app.ssh_channel:
             term_size = self._app.ssh_channel.get_terminal_size()
             if term_size:
                 size = term_size[:2]
        
        # Enable mouse support
        self.write("\x1b[?1000h")
        self.write("\x1b[?1006h")
        self.write("\x1b[?1015h")
        self.write("\x1b[?1049h") # Alt screen
        self.write("\x1b[?25l")   # Hide cursor
        self.flush()

        event = Resize(Size(*size), Size(*size))
        self.process_message(event)
        
        # Dispatch again after a short delay to ensure app is ready
        loop = asyncio.get_running_loop()
        loop.call_later(0.25, self.process_message, event)

    def disable_input(self) -> None:
        print("WhistlerDriver.disable_input", file=sys.stderr, flush=True)
        self.exit_event.set()

    def stop_application_mode(self) -> None:
        print("WhistlerDriver.stop_application_mode", file=sys.stderr, flush=True)
        self.write("\x1b[?1000l") # Disable mouse support
        self.write("\x1b[?1006l")
        self.write("\x1b[?1015l")
        self.write("\x1b[?1049l") # Disable alt screen
        self.write("\x1b[?25h")   # Show cursor
        self.flush()

    def feed_data(self, data: str | bytes) -> None:
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        if len(data) > 0 and data[0] == '\x1b':
           print(f"WhistlerDriver.feed_data escape: {repr(data)}", file=sys.stderr, flush=True)
        for event in self._parser.feed(data):
            self.process_message(event)

    def process_message(self, event: Event) -> None:
        if self._app:
            self._app.post_message(event)

async def start_server():
    parser = argparse.ArgumentParser(description="Whistler SSH Server")
    parser.add_argument("--config", default="config.yaml", help="Path to configuration file")
    parser.add_argument("--kubeconfig", help="Path to kubeconfig file (enables K8s mode)")
    args = parser.parse_args()

    if args.kubeconfig:
        print(f"Starting in Kubernetes mode (config: {args.kubeconfig})", file=sys.stderr)
        config_manager = KubeConfigManager(kubeconfig=args.kubeconfig)
    else:
        print(f"Starting in YAML mode (config: {args.config})", file=sys.stderr)
        config_manager = YamlConfigManager(args.config)
    
    # Create a partial to pass config_manager to SSHServer
    server_factory = partial(SSHServer, config_manager=config_manager)

    await asyncssh.create_server(server_factory, '', 8022,
                                 server_host_keys=['ssh_host_key'],
                                 line_editor=False)

class SSHServer(asyncssh.SSHServer):
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.username = None
        self.target_type = "tui" # tui, template, instance
        self.target_name = None

    def connection_made(self, conn):
        print('SSH connection received from %s.' % conn.get_extra_info('peername')[0], file=sys.stderr)

    def connection_lost(self, exc):
        if exc:
            print('SSH connection error: ' + str(exc), file=sys.stderr)
        else:
            print('SSH connection closed.', file=sys.stderr)

    def begin_auth(self, username):
        # We require password auth now to identify the user
        return True

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        # Parse username for target
        # Format: user, user-template-templatename, user-instance-instancename
        # For simplicity in this iteration, let's assume:
        # - "user": TUI
        # - "user-template": Create instance from template (requires parsing)
        # - "user-instance": Connect to instance (requires parsing)
        
        # Actually, let's stick to the README examples:
        # ssh someuser@... -> TUI
        # ssh someuser-small@... -> Create/Connect ephemeral from template 'small'
        # ssh someuser-123@... -> Connect to instance '123'
        
        parts = username.split('-')
        real_user = parts[0]
        
        if len(parts) == 1:
            self.username = real_user
            self.target_type = "tui"
        elif len(parts) >= 2:
            self.username = real_user
            # Heuristic: check if suffix matches a template or instance
            suffix = "-".join(parts[1:])
            
            # Check templates first
            templates = self.config_manager.get_user_templates(real_user)
            if any(t['name'] == suffix for t in templates):
                self.target_type = "template"
                self.target_name = suffix
            else:
                # Assume instance
                self.target_type = "instance"
                self.target_name = suffix

        if self.config_manager.user_exists(real_user):
            print(f"User {real_user} authenticated (found in config). Target: {self.target_type} {self.target_name}", file=sys.stderr, flush=True)
        else:
            print(f"User {real_user} authenticated (NOT found in config)", file=sys.stderr, flush=True)
        return True

    def session_requested(self):
        print("SSHServer.session_requested", file=sys.stderr, flush=True)
        return WhistlerSession(
            config_manager=self.config_manager, 
            username=self.username,
            target_type=self.target_type,
            target_name=self.target_name
        )

class WhistlerSession(asyncssh.SSHServerSession):
    def __init__(self, config_manager=None, username=None, target_type="tui", target_name=None, *args, **kwargs):
        # super().__init__(*args, **kwargs) # SSHServerSession is just object
        self._app = None
        self._app_task = None
        self._chan = None
        self._shell_task = None
        self._master_fd = None
        self.config_manager = config_manager
        self.username = username
        self.target_type = target_type
        self.target_name = target_name
        self.initial_term_size = (80, 24)
        print("WhistlerSession initialized", file=sys.stderr, flush=True)

    def connection_made(self, chan):
        print("WhistlerSession.connection_made", file=sys.stderr, flush=True)
        self._chan = chan
        self._chan.set_encoding(None)

    def pty_requested(self, term_type, term_size, term_modes):
        self.initial_term_size = (term_size[0], term_size[1])
        return True

    def shell_requested(self):
        print("WhistlerSession.shell_requested", file=sys.stderr, flush=True)
        return True

    def data_received(self, data, datatype):
        # print(f"WhistlerSession.data_received: {len(data)}", file=sys.stderr, flush=True)
        if self._app and self._app.driver:
            self._app.driver.feed_data(data)

    def exec_requested(self, command):
        print(f"WhistlerSession.exec_requested: {command}", file=sys.stderr, flush=True)
        return True

    def session_started(self):
        print("WhistlerSession.session_started", file=sys.stderr, flush=True)
        
        if self.target_type == "tui":
            self._app = WhistlerApp(driver_class=WhistlerDriver, config_manager=self.config_manager, username=self.username, session=self)
            self._app.ssh_channel = self._chan
            self._app_task = asyncio.create_task(self._run_app())
        elif self.target_type == "instance":
            # Find the instance
            instances = self.config_manager.get_user_instances(self.username)
            instance = next((i for i in instances if i["name"] == self.target_name), None)
            
            if instance and instance.get("podName") and instance.get("status") == "Running":
                 self._shell_task = asyncio.create_task(self._run_pod_shell(instance["podName"]))
            else:
                 self._chan.write(f"Instance {self.target_name} not found or not running.\r\n".encode('utf-8'))
                 self._chan.exit(1)
        elif self.target_type == "template":
             # Create instance logic would go here, then connect
             # For now, just error
             self._chan.write(f"Creating instance from template {self.target_name} not yet implemented via SSH direct connect.\r\n".encode('utf-8'))
             self._chan.exit(1)
        else:
            print(f"Target type {self.target_type} unknown, falling back to TUI", file=sys.stderr)
            self._app = WhistlerApp(driver_class=WhistlerDriver, config_manager=self.config_manager, username=self.username, session=self)
            self._app.ssh_channel = self._chan
            self._app_task = asyncio.create_task(self._run_app())

    async def _run_app(self):
        print("WhistlerSession._run_app starting", file=sys.stderr, flush=True)
        try:
            # Run the app with our custom driver
            await self._app.run_async()
        except Exception as e:
            print(f"App error: {e}", file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
        finally:
            print("WhistlerSession._run_app finished", file=sys.stderr, flush=True)
            self._chan.exit(0)

    async def _run_pod_shell(self, pod_name):
        print(f"Starting shell for pod {pod_name}", file=sys.stderr)
        
        # Create PTY
        master, slave = pty.openpty()
        self._master_fd = master
        
        # Set initial size
        if self.initial_term_size:
            cols, rows = self.initial_term_size
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            fcntl.ioctl(master, termios.TIOCSWINSZ, winsize)

        try:
            process = await asyncio.create_subprocess_exec(
                "kubectl", "exec", "-it", pod_name, "--", "/bin/bash",
                stdin=slave, stdout=slave, stderr=slave,
                preexec_fn=os.setsid
            )
            os.close(slave) # Close slave in parent
            
            # Pump data from master to SSH channel
            loop = asyncio.get_running_loop()
            
            while True:
                try:
                    data = await loop.run_in_executor(None, os.read, master, 1024)
                    if not data:
                        break
                    self._chan.write(data)
                except OSError:
                    break
            
            await process.wait()
        except Exception as e:
            print(f"Shell error: {e}", file=sys.stderr)
        finally:
            print("Shell finished", file=sys.stderr)
            if self._master_fd:
                os.close(self._master_fd)
                self._master_fd = None
            self._chan.exit(0)

    def data_received(self, data, datatype):
        # print(f"WhistlerSession.data_received: {len(data)} bytes", file=sys.stderr, flush=True)
        if self._app and self._app.driver:
            self._app.driver.feed_data(data)
        elif self._master_fd:
            os.write(self._master_fd, data.encode('utf-8') if isinstance(data, str) else data)

    def terminal_size_changed(self, width, height, pixwidth, pixheight):
        if self._app:
            self._app.post_message(Resize(Size(width, height), Size(width, height)))
        elif self._master_fd:
             winsize = struct.pack("HHHH", height, width, 0, 0)
             fcntl.ioctl(self._master_fd, termios.TIOCSWINSZ, winsize)

    def connection_lost(self, exc):
        print("WhistlerSession.connection_lost", file=sys.stderr, flush=True)
        if self._app_task:
            self._app_task.cancel()


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    asyncssh.set_debug_level(2)

    # Generate a host key if it doesn't exist
    try:
        asyncssh.read_private_key('ssh_host_key')
    except FileNotFoundError:
        key = asyncssh.generate_private_key('ssh-rsa')
        key.write_private_key('ssh_host_key')

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start_server())
    except (OSError, asyncssh.Error) as exc:
        sys.exit('Error starting server: ' + str(exc))

    loop.run_forever()


