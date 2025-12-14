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
from whistler.tui import WhistlerApp, LoadingScreen

import argparse
from functools import partial
from functools import partial
from whistler.config import ConfigManager, KubeConfigManager
from asyncio import Event
from textual.worker import Worker, WorkerState





class WhistlerDriver(Driver):
    def __init__(self, next_driver: Driver | None = None, *, debug: bool = False, size: tuple[int, int] | None = None, **kwargs):
        super().__init__(next_driver, debug=debug, size=size)
        self._parser = XTermParser(debug=debug)
        self.exit_event = Event()
        print("WhistlerDriver initialized", file=sys.stderr, flush=True)

    def write(self, data: str | bytes) -> None:
        # print(f"WhistlerDriver.write: {len(data)} bytes: {repr(data)[:50]}", file=sys.stderr, flush=True)
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
        elif self._app and hasattr(self._app, 'initial_term_size'):
             size = self._app.initial_term_size
             print(f"Using initial_term_size from app: {size}", file=sys.stderr, flush=True)
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
        loop.call_later(0.05, self.process_message, event)

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
        # if len(data) > 0 and data[0] == '\x1b':
        #    print(f"WhistlerDriver.feed_data escape: {repr(data)}", file=sys.stderr, flush=True)
        for event in self._parser.feed(data):
            self.process_message(event)

    def process_message(self, event: Event) -> None:
        if self._app:
            self._app.post_message(event)

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
        # if len(data) > 0 and data[0] == '\x1b':
        #    print(f"WhistlerDriver.feed_data escape: {repr(data)}", file=sys.stderr, flush=True)
        for event in self._parser.feed(data):
            self.process_message(event)

    def process_message(self, event: Event) -> None:
        if self._app:
            self._app.post_message(event)

class LoadingApp(App):
    """App to display loading screen during pod operations."""
    
    def __init__(self, ssh_channel, initial_term_size=(80, 24), initial_status="Loading...", **kwargs):
        # Use WhistlerDriver directly
        super().__init__(driver_class=WhistlerDriver, **kwargs)
        self.ssh_channel = ssh_channel
        self.initial_term_size = initial_term_size
        self.initial_status = initial_status
        self.loading_screen = None
        self._should_exit = False
    
    def on_mount(self) -> None:
        print("LoadingApp.on_mount", file=sys.stderr, flush=True)
        self.loading_screen = LoadingScreen(initial_status=self.initial_status)
        self.push_screen(self.loading_screen)
    
    def update_status(self, status: str) -> None:
        """Update the loading screen status."""
        if self.loading_screen:
            self.loading_screen.update_status(status)
    
    def request_exit(self) -> None:
        """Request the app to exit."""
        print("LoadingApp.request_exit", file=sys.stderr, flush=True)
        self._should_exit = True
        self.exit()

async def start_server():
    parser = argparse.ArgumentParser(description="Whistler SSH Server")
    parser.add_argument("--kubeconfig", help="Path to kubeconfig file")
    parser.add_argument("--in-cluster", action="store_true", help="Run in Kubernetes in-cluster mode")
    args = parser.parse_args()

    # Always run in K8s mode
    mode = "in-cluster" if args.in_cluster else f"config: {args.kubeconfig}" if args.kubeconfig else "default"
    print(f"Starting in Kubernetes mode ({mode})", file=sys.stderr)
    config_manager = KubeConfigManager(kubeconfig=args.kubeconfig)
    
    # Create a partial to pass config_manager to SSHServer
    server_factory = partial(SSHServer, config_manager=config_manager)

    await asyncssh.create_server(server_factory, '', 8022,
                                 server_host_keys=['ssh_host_key'],
                                 line_editor=False,
                                 agent_forwarding=True,
                                 keepalive_interval=30,
                                 keepalive_count_max=5)

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
        # We require public key auth now
        return True

    def agent_auth_requested(self):
        return True

    def password_auth_supported(self):
        # Allow password auth (which will accept anything) only in dev mode
        return os.environ.get("WHISTLER_AUTH_ALLOW_ANY") == "true"

    def validate_password(self, username, password):
        # Only allowed in dev mode
        if os.environ.get("WHISTLER_AUTH_ALLOW_ANY") != "true":
            return False
            
        print(f"Dev mode: allowing {username} via password auth", file=sys.stderr)
        
        parts = username.split('-')
        real_user = parts[0]
        self.username = real_user
        
        # Determine target (same logic as before)
        if len(parts) == 1:
            self.target_type = "tui"
        elif len(parts) >= 2:
            suffix = "-".join(parts[1:])
            templates = self.config_manager.get_user_templates(real_user)
            if any(t['name'] == suffix for t in templates):
                self.target_type = "template"
                self.target_name = suffix
            else:
                self.target_type = "instance"
                self.target_name = suffix
        return True

    def public_key_auth_supported(self):
        return True
        

    def validate_public_key(self, username, key):
        parts = username.split('-')
        real_user = parts[0]
        
        # Check for dev mode bypass
        if os.environ.get("WHISTLER_AUTH_ALLOW_ANY") == "true":
             print(f"Dev mode: allowing {real_user} without key check", file=sys.stderr)
             self.username = real_user
             
             # Determine target (same logic as before)
             if len(parts) == 1:
                 self.target_type = "tui"
             elif len(parts) >= 2:
                 suffix = "-".join(parts[1:])
                 templates = self.config_manager.get_user_templates(real_user)
                 if any(t['name'] == suffix for t in templates):
                     self.target_type = "template"
                     self.target_name = suffix
                 else:
                     self.target_type = "instance"
                     self.target_name = suffix
                     self.active_instance_name = suffix
             return True

        # Check if user exists and key matches
        if not self.config_manager.user_exists(real_user):
             print(f"User {real_user} not found", file=sys.stderr)
             return False
             
        allowed_keys = self.config_manager.get_user_public_keys(real_user)
        key_data = key.export_public_key().decode('utf-8').split()[1] # Extract base64 part
        
        # Simple check: is the key in the allowed list?
        # Note: allowed_keys in values.yaml might be full "ssh-rsa AAA..." strings
        for allowed in allowed_keys:
            if key_data in allowed:
                self.username = real_user
                
                # Determine target (same logic as before)
                if len(parts) == 1:
                    self.target_type = "tui"
                elif len(parts) >= 2:
                    suffix = "-".join(parts[1:])
                    templates = self.config_manager.get_user_templates(real_user)
                    if any(t['name'] == suffix for t in templates):
                        self.target_type = "template"
                        self.target_name = suffix
                    else:
                        self.target_type = "instance"
                        self.target_name = suffix
                        self.active_instance_name = suffix
                
                print(f"User {real_user} authenticated via public key. Target: {self.target_type} {self.target_name}", file=sys.stderr)
                return True
                
        print(f"Public key validation failed for {real_user}", file=sys.stderr)
        return False

    def session_requested(self):
        print("SSHServer.session_requested", file=sys.stderr, flush=True)
        return WhistlerSession(
            server=self,
            config_manager=self.config_manager, 
            username=self.username,
            target_type=self.target_type,
            target_name=self.target_name
        )
    
    async def connection_requested(self, dest_host, dest_port, orig_host, orig_port):
        print(f"Connection requested: {dest_host}:{dest_port} from {orig_host}:{orig_port}", file=sys.stderr)
        
        # Only allow forwarding to localhost (which maps to the container)
        if dest_host not in ("localhost", "127.0.0.1"):
            print(f"Forwarding denied: destination {dest_host} not allowed (only localhost)", file=sys.stderr)
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED,
                "Forwarding is only allowed to localhost (the container)"
            )
            
        instance_name = getattr(self, "active_instance_name", None)
        if not instance_name:
             print("Forwarding denied: no active instance", file=sys.stderr)
             raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED,
                "No active container instance found for forwarding"
            )
            
        # Resolve instance
        instances = self.config_manager.get_user_instances(self.username)
        instance = next((i for i in instances if i["name"] == instance_name), None)
        
        if instance and instance.get("podName") and instance.get("status") == "Running":
            print(f"Tunneling {dest_host}:{dest_port} -> Pod {instance['podName']}:127.0.0.1:{dest_port}", file=sys.stderr)
            return await self._create_pod_tunnel(instance['podName'], dest_port)
        else:
            print(f"Forwarding failed: instance {instance_name} not running or not found", file=sys.stderr)
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED,
                f"Container {instance_name} is not reachable"
            )

    async def _create_pod_tunnel(self, pod_name, port):
        # Use kubectl exec + socat to tunnel to localhost inside the pod
        # This handles services bound to 127.0.0.1 strictly
        cmd = [
            "kubectl", "exec", "-i", pod_name, "-n", self.config_manager.namespace,
            "--", "socat", "-", f"TCP4:127.0.0.1:{port}"
        ]
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Helper to log stderr without blocking
            async def log_stderr():
                try:
                    while True:
                        line = await process.stderr.readline()
                        if not line: break
                        print(f"Tunnel {pod_name}:{port} stderr: {line.decode().strip()}", file=sys.stderr)
                except Exception:
                    pass

            asyncio.create_task(log_stderr())
            
            return process.stdout, process.stdin
        except Exception as e:
            print(f"Failed to create tunnel: {e}", file=sys.stderr)
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED,
                f"Tunnel creation failed: {e}"
            )


class WhistlerSession(asyncssh.SSHServerSession):
    def __init__(self, server=None, config_manager=None, username=None, target_type="tui", target_name=None, *args, **kwargs):
        # super().__init__(*args, **kwargs) # SSHServerSession is just object
        self.server = server
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
        self._resize_timer = None
        self._pending_size = None
        self._last_processed_size = None
        self._agent_task = None
        self.local_agent_path = None
        self.pod_socket_path = None
        self.term_type = None
        self._process_stdin = None
        self.is_ephemeral = False
        print("WhistlerSession initialized", file=sys.stderr, flush=True)

    def connection_made(self, chan):
        print("WhistlerSession.connection_made", file=sys.stderr, flush=True)
        self._chan = chan
        self._chan.set_encoding(None)

    def pty_requested(self, term_type, term_size, term_modes):
        self.initial_term_size = (term_size[0], term_size[1])
        self.term_type = term_type
        return True

    def shell_requested(self):
        print("WhistlerSession.shell_requested", file=sys.stderr, flush=True)
        return True

    def data_received(self, data, datatype):
        # print(f"WhistlerSession.data_received: {len(data)}", file=sys.stderr, flush=True)
        if self._app and self._app.driver:
            self._app.driver.feed_data(data)
        elif self._master_fd is not None:
             # Forward to PTY master
             try:
                 os.write(self._master_fd, data.encode('utf-8') if isinstance(data, str) else data)
             except OSError:
                 pass
        elif self._process_stdin is not None:
             # Forward to process stdin (non-PTY)
             try:
                 self._process_stdin.write(data.encode('utf-8') if isinstance(data, str) else data)
                 # self._process_stdin.drain() # Not async here, need to check if we can await or if it's buffered
             except Exception:
                 pass

    def exec_requested(self, command):
        print(f"WhistlerSession.exec_requested: {command}", file=sys.stderr, flush=True)
        return True
    
    def session_started(self):
        print("WhistlerSession.session_started", file=sys.stderr, flush=True)
        
        # Check for agent forwarding
        self.local_agent_path = self._chan.get_agent_path()
        if self.local_agent_path:
            import secrets
            # Generate a unique path for the pod socket
            self.pod_socket_path = f"/tmp/agent-{secrets.token_hex(4)}.sock"
            print(f"Agent forwarding requested. Local: {self.local_agent_path}, Pod: {self.pod_socket_path}", file=sys.stderr)

        # Temporarily set TERM/COLORTERM based on client request so Textual/Rich picks it up
        old_term = os.environ.get('TERM')
        old_colorterm = os.environ.get('COLORTERM')
        old_escdelay = os.environ.get('ESCDELAY')
        
        if hasattr(self, 'term_type') and self.term_type:
            os.environ['TERM'] = self.term_type
            # Assume truecolor support for modern SSH clients if not specified
            os.environ['COLORTERM'] = 'truecolor'

        try:
            if self.target_type == "tui":
                self._app = WhistlerApp(driver_class=WhistlerDriver, config_manager=self.config_manager, username=self.username, session=self)
                self._app.ssh_channel = self._chan
                self._app_task = asyncio.create_task(self._run_app())
            elif self.target_type == "instance":
                # Find the instance
                self._shell_task = asyncio.create_task(self._connect_to_instance())
            elif self.target_type == "template":
                 self._shell_task = asyncio.create_task(self._create_and_connect_ephemeral())
            else:
                print(f"Target type {self.target_type} unknown, falling back to TUI", file=sys.stderr, flush=True)
                self._app = WhistlerApp(driver_class=WhistlerDriver, config_manager=self.config_manager, username=self.username, session=self)
                self._app.ssh_channel = self._chan
                self._app_task = asyncio.create_task(self._run_app())
        finally:
            # Restore environment
            if old_term: os.environ['TERM'] = old_term
            else: os.environ.pop('TERM', None)
            
            if old_colorterm: os.environ['COLORTERM'] = old_colorterm
            else: os.environ.pop('COLORTERM', None)

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

    async def _create_and_connect_ephemeral(self):
         # Create ephemeral instance
         self.is_ephemeral = True
         import secrets
         hex_id = secrets.token_hex(4)
         instance_name = f"{self.target_name}-{hex_id}"
         
         # Resolve full template name
         templates = self.config_manager.get_user_templates(self.username)
         template_obj = next((t for t in templates if t["name"] == self.target_name), None)
         template_ref = template_obj["fullName"] if template_obj else self.target_name
         
         if self.term_type:
             # Use loading screen for PTY mode
             loading_app = LoadingApp(self._chan, self.initial_term_size, f"Creating ephemeral instance {instance_name}...")
             loading_app.ssh_channel = self._chan
             
             async def create_task():
                 # Create instance
                 if self.config_manager.add_instance(self.username, template_ref, instance_name, preemptible=True):
                     loading_app.update_status(f"Waiting for instance {instance_name} to be ready...")
                     self.target_name = instance_name
                     return await self._connect_to_instance_with_app(loading_app)
                 else:
                     loading_app.request_exit()
                     self._chan.write(f"Failed to create ephemeral instance.\r\n".encode('utf-8'))
                     self._chan.exit(1)
                     return None
             
             # Run the loading app with the create task
             task = asyncio.create_task(create_task())
             try:
                 await loading_app.run_async()
                 # Wait for task to complete
                 pod_name = await task
                 
                 if pod_name:
                     await self._run_pod_shell(pod_name)
             except asyncio.CancelledError:
                 print("Task cancelled in _create_and_connect_ephemeral", file=sys.stderr, flush=True)
                 task.cancel()
                 raise
             finally:
                 # Cleanup
                 print(f"Entering finally block for {instance_name}", file=sys.stderr, flush=True)
                 try:
                     self._chan.write(f"\r\nCleaning up ephemeral instance {instance_name}...\r\n".encode('utf-8'))
                 except Exception:
                     pass
                 try:
                     self.config_manager.delete_instance(self.username, instance_name)
                     print(f"delete_instance called for {instance_name}", file=sys.stderr, flush=True)
                 except Exception as e:
                     print(f"Error calling delete_instance: {e}", file=sys.stderr, flush=True)
                 try:
                     self._chan.exit(0)
                 except Exception:
                     pass
         else:
             # Non-PTY mode: use simple text output
             self._chan.write(f"Creating ephemeral instance {instance_name} (full name: {self.username}-{instance_name}) from template {self.target_name}...\r\n".encode('utf-8'))
             
             if self.config_manager.add_instance(self.username, template_ref, instance_name):
                 try:
                     self.target_name = instance_name
                     await self._connect_to_instance()
                 except Exception as e:
                     print(f"Error in _create_and_connect_ephemeral: {e}", file=sys.stderr, flush=True)
                     self._chan.write(f"Error connecting to instance: {e}\r\n".encode('utf-8'))
                 except asyncio.CancelledError:
                     print("Task cancelled in _create_and_connect_ephemeral", file=sys.stderr, flush=True)
                     raise
                 finally:
                     print(f"Entering finally block for {instance_name}", file=sys.stderr, flush=True)
                     try:
                         self._chan.write(f"\r\nCleaning up ephemeral instance {instance_name}...\r\n".encode('utf-8'))
                     except Exception:
                         pass
                     try:
                         self.config_manager.delete_instance(self.username, instance_name)
                         print(f"delete_instance called for {instance_name}", file=sys.stderr, flush=True)
                     except Exception as e:
                         print(f"Error calling delete_instance: {e}", file=sys.stderr, flush=True)
                     try:
                         self._chan.exit(0)
                     except Exception:
                         pass
             else:
                 self._chan.write(f"Failed to create ephemeral instance.\r\n".encode('utf-8'))
                 self._chan.exit(1)

    async def _connect_to_instance_with_app(self, loading_app):
        """Connect to instance using the provided loading app."""
        instances = self.config_manager.get_user_instances(self.username)
        instance = next((i for i in instances if i["name"] == self.target_name), None)
        
        if not instance:
            loading_app.request_exit()
            self._chan.write(f"Instance {self.target_name} not found.\r\n".encode('utf-8'))
            self._chan.exit(1)
            return
            
        pod_name = instance.get("podName")
        
        # If terminating, wait for it to finish first
        if instance.get("status") == "Terminating":
            loading_app.update_status("Waiting for existing pod to terminate...")
            while instance and instance.get("status") == "Terminating":
                await asyncio.sleep(0.5)
                instances = self.config_manager.get_user_instances(self.username)
                instance = next((i for i in instances if i["name"] == self.target_name), None)
            
            if instance:
                pod_name = instance.get("podName")
        
        if not pod_name or (instance and instance.get("status") != "Running"):
            # Trigger operator to ensure pod exists
            import time
            try:
                full_cr_name = f"{self.username}-{self.target_name}"
                self.config_manager.api.patch_namespaced_custom_object(
                    self.config_manager.group, self.config_manager.version, self.config_manager.namespace,
                    "whistlerinstances", full_cr_name,
                    {"metadata": {"annotations": {"whistler.io/last-connect": str(time.time())}}}
                )
            except Exception as e:
                print(f"Failed to patch instance: {e}", file=sys.stderr)
            
            loading_app.update_status(f"Starting instance {self.target_name}...")
            pod_name = await self._wait_for_pod_with_app(self.target_name, loading_app)
        
        # Exit the loading app
        loading_app.request_exit()
        
        if pod_name:
            # Update server context for forwarding
            if self.server:
                self.server.active_instance_name = self.target_name
                print(f"Updated server active_instance_name to {self.target_name}", file=sys.stderr)

            # Start agent bridge if needed
            if self.local_agent_path and self.pod_socket_path:
                self._agent_task = asyncio.create_task(self._bridge_agent(pod_name))
                await asyncio.sleep(0.5)

            return pod_name
        else:
            self._chan.write(f"Failed to start instance {self.target_name}.\r\n".encode('utf-8'))
            self._chan.exit(1)
            return None

    async def _connect_to_instance(self, loading_screen=None):
        """Connect to instance (for non-PTY mode)."""
        instances = self.config_manager.get_user_instances(self.username)
        instance = next((i for i in instances if i["name"] == self.target_name), None)
        
        if self.term_type and not loading_screen:
            # PTY mode: use loading app
            loading_app = LoadingApp(self._chan, self.initial_term_size, f"Connecting to instance {self.target_name}...")
            loading_app.ssh_channel = self._chan
            
            task = asyncio.create_task(self._connect_to_instance_with_app(loading_app))
            try:
                await loading_app.run_async()
                pod_name = await task
                if pod_name:
                    await self._run_pod_shell(pod_name)
            except asyncio.CancelledError:
                task.cancel()
                raise
            return
        
        # Non-PTY mode or already have loading screen
        if not instance:
            self._chan.write(f"Instance {self.target_name} not found.\r\n".encode('utf-8'))
            self._chan.exit(1)
            return
            
        pod_name = instance.get("podName")
        
        # If terminating, wait for it to finish first
        if instance.get("status") == "Terminating":
            self._chan.write(b"Waiting for existing pod to terminate ")
            while instance and instance.get("status") == "Terminating":
                await asyncio.sleep(0.5)
                self._chan.write(b".")
                instances = self.config_manager.get_user_instances(self.username)
                instance = next((i for i in instances if i["name"] == self.target_name), None)
            self._chan.write(b"\r\n")
            
            if instance:
                pod_name = instance.get("podName")
        
        if not pod_name or (instance and instance.get("status") != "Running"):
            # Trigger operator to ensure pod exists
            import time
            try:
                full_cr_name = f"{self.username}-{self.target_name}"
                self.config_manager.api.patch_namespaced_custom_object(
                    self.config_manager.group, self.config_manager.version, self.config_manager.namespace,
                    "whistlerinstances", full_cr_name,
                    {"metadata": {"annotations": {"whistler.io/last-connect": str(time.time())}}}
                )
            except Exception as e:
                print(f"Failed to patch instance: {e}", file=sys.stderr)
            
            pod_name = await self._wait_for_pod(self.target_name)
        
        if pod_name:
            # Update server context for forwarding
            if self.server:
                self.server.active_instance_name = self.target_name
                print(f"Updated server active_instance_name to {self.target_name}", file=sys.stderr)

            # Start agent bridge if needed
            if self.local_agent_path and self.pod_socket_path:
                self._agent_task = asyncio.create_task(self._bridge_agent(pod_name))
                await asyncio.sleep(0.5)

            await self._run_pod_shell(pod_name)
        else:
            self._chan.write(f"Failed to start instance {self.target_name}.\r\n".encode('utf-8'))
            self._chan.exit(1)

    def _generate_motd(self, instance, template, all_volumes):
        message = []
        
        # Welcome message
        banner = """
    ********************************************************************
    *  ██╗    ██╗██╗  ██╗██╗███████╗████████╗██╗     ███████╗██████╗   *
    *  ██║    ██║██║  ██║██║██╔════╝╚══██╔══╝██║     ██╔════╝██╔══██╗  *
    *  ██║ █╗ ██║███████║██║███████╗   ██║   ██║     █████╗  ██████╔╝  *
    *  ██║███╗██║██╔══██║██║╚════██║   ██║   ██║     ██╔══╝  ██╔══██╗  *
    *  ╚███╔███╔╝██║  ██║██║███████║   ██║   ███████╗███████╗██║  ██║  *
    *   ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝╚══════╝   ╚═╝   ╚══════╝╚══════╝╚═╝  ╚═╝  *
    ********************************************************************
        """
        message.append(banner)
        message.append(f"Welcome to Whistler. You are connected to {instance['name']}")
        
        # Personal mount check
        personal_mount = template.get("personalMountPath")
        if not personal_mount:
             personal_mount = "/userdata"

        if personal_mount:
            message.append(f"and your user directory is mounted under {personal_mount}")
            
        # Volumes list
        visible_volumes = []
        
        # Use actual mounts from pod if available (source of truth)
        real_mounts = instance.get("mounts")
        if real_mounts is not None:
             for m in real_mounts:
                 visible_volumes.append(f"* {m['name']} - {m['mountPath']}")
        else:
             # Fallback to template definition if pod info unavailable
             template_volumes = template.get("volumes", [])
             
             # Add personal mount to the list of volumes
             if personal_mount:
                  visible_volumes.append(f"* User Volume - {personal_mount}")
    
             for vol in template_volumes:
                  name = vol.get("name", "Unknown")
                  path = vol.get("mountPath", "Unknown")
                  visible_volumes.append(f"* {name} - {path}")
             
        # Also check global volumes.yaml? The user request implies showing mounted volumes.
        # k8s template spec has volumes.
        
        if visible_volumes:
            message.append("Mounted volumes are")
            message.extend(visible_volumes)
            message.append("")
            
        # Ephemeral warning
        if self.is_ephemeral:
            message.append("This instance is ephemeral and will be terminated once you close the connection.\nMake sure to save any work to mounted persistant volumes before exiting.")
            message.append("")
            
        # Preemptible warning
        if instance.get("preemptible"):
            message.append("This instance is preemptible, it can terminate without warning at any time.\nPlan accordingly.")
            message.append("")
            
        return "\n".join(message) + "\n"

    async def _run_pod_shell(self, pod_name):
        print(f"Starting shell for pod {pod_name}", file=sys.stderr)
        
        # Get instance and template info for MOTD
        instances = self.config_manager.get_user_instances(self.username)
        instance = next((i for i in instances if i["name"] == self.target_name), None)
        
        motd = ""
        if instance:
            templates = self.config_manager.get_user_templates(self.username)
            # TemplateRef in instance might be full name "user-template", but get_user_templates returns list with "name" (short) and "fullName"
            # Instance template ref is likely just the name if created via TUI? 
            # In config.py add_instance: "templateRef": template_name
            # Let's match by fullName or name
            template_ref = instance.get("template")
            template = next((t for t in templates if t["fullName"] == template_ref or t["name"] == template_ref), {})
            
            all_volumes = self.config_manager.get_volumes() # Global volume definitions if needed
            motd = self._generate_motd(instance, template, all_volumes)
            print(f"Generated MOTD for {self.username}: {len(motd)} chars", file=sys.stderr)
        else:
             print(f"MOTD: Instance {self.target_name} not found in {len(instances)} instances", file=sys.stderr)
             motd = f"Connecting to {self.target_name}...\r\n(Instance details not found for MOTD)\r\n"
            
        if motd:
            # We need to write CRLF for raw PTY/SSH output to look right
            formatted_motd = motd.replace("\n", "\r\n")
            
            # Clear screen? Maybe not, just header.
            # self._chan.write(b"\x1b[2J\x1b[H") 
            
            self._chan.write(formatted_motd.encode('utf-8'))
            print("MOTD sent to channel", file=sys.stderr)
            
            # Ensure the MOTD is sent before we hook up the PTY
            # asyncssh doesn't have drain on channel. buffer is managed.
            # We keep the sleep to ensure render.
            await asyncio.sleep(0.5)

        
        process = None
        use_pty = self.term_type is not None
        
        try:
            cmd = ["kubectl", "exec", "-n", self.config_manager.namespace]
            
            if use_pty:
                cmd.append("-it")
            else:
                cmd.append("-i")
                
            cmd.append(pod_name)
            cmd.append("--")
            
            if self.pod_socket_path:
                cmd.extend(["env", f"SSH_AUTH_SOCK={self.pod_socket_path}"])
                
            cmd.append("/bin/bash")
            
            if use_pty:
                # PTY Mode
                master, slave = pty.openpty()
                self._master_fd = master
                
                # Set initial size
                if self.initial_term_size:
                    cols, rows = self.initial_term_size
                    winsize = struct.pack("HHHH", rows, cols, 0, 0)
                    fcntl.ioctl(master, termios.TIOCSWINSZ, winsize)

                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=slave, stdout=slave, stderr=slave,
                    preexec_fn=os.setsid
                )
                os.close(slave) # Close slave in parent
                
                loop = asyncio.get_running_loop()
                pty_closed = loop.create_future()
                
                def read_pty():
                    try:
                        data = os.read(master, 1024)
                        if not data:
                            if not pty_closed.done():
                                pty_closed.set_result(True)
                        else:
                            self._chan.write(data)
                    except (OSError, Exception):
                        if not pty_closed.done():
                            pty_closed.set_result(True)

                loop.add_reader(master, read_pty)
                
                # Wait for either process exit or PTY close
                wait_task = asyncio.create_task(process.wait())
                
                try:
                    done, pending = await asyncio.wait(
                        [wait_task, pty_closed], 
                        return_when=asyncio.FIRST_COMPLETED
                    )
                except asyncio.CancelledError:
                    print("Shell task cancelled, cleaning up...", file=sys.stderr)
                    raise

            else:
                # Non-PTY Mode (Pipes)
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                self._process_stdin = process.stdin
                
                async def forward_output(reader, channel_write_func):
                    try:
                        while True:
                            data = await reader.read(1024)
                            if not data:
                                break
                            channel_write_func(data)
                    except Exception as e:
                        print(f"Output forwarder error: {e}", file=sys.stderr)

                # Forward stdout -> channel stdout
                stdout_task = asyncio.create_task(forward_output(process.stdout, self._chan.write))
                
                # Forward stderr -> channel stderr
                stderr_task = asyncio.create_task(forward_output(process.stderr, partial(self._chan.write, datatype=asyncssh.EXTENDED_DATA_STDERR)))
                
                try:
                    await process.wait()
                    # Wait for output forwarding to finish (drain pipes)
                    await asyncio.gather(stdout_task, stderr_task)
                except asyncio.CancelledError:
                    print("Shell task cancelled, cleaning up...", file=sys.stderr)
                    stdout_task.cancel()
                    stderr_task.cancel()
                    raise
                # finally: tasks are already done or cancelled

        except Exception as e:
            print(f"Shell error: {e}", file=sys.stderr)
        finally:
            print("Shell finished, cleaning up resources...", file=sys.stderr)
            loop = asyncio.get_running_loop()
            if self._master_fd:
                loop.remove_reader(self._master_fd)
                os.close(self._master_fd)
                self._master_fd = None
            
            if process and process.returncode is None:
                print("Terminating kubectl process...", file=sys.stderr)
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
            
            self._chan.exit(0)

    async def _wait_for_pod_with_app(self, instance_name, loading_app, timeout=60):
        """Wait for pod to be ready, updating the loading app."""
        start_time = asyncio.get_running_loop().time()
        last_status = None
        
        while asyncio.get_running_loop().time() - start_time < timeout:
            instances = self.config_manager.get_user_instances(self.username)
            instance = next((i for i in instances if i["name"] == instance_name), None)
            
            if instance:
                status = instance.get("status")
                pod_name = instance.get("podName")
                
                if status == "Running" and pod_name:
                    return pod_name
                
                if status != last_status:
                    loading_app.update_status(f"Instance status: {status}")
                    last_status = status
            
            await asyncio.sleep(0.5)
        return None

    async def _wait_for_pod(self, instance_name, timeout=60):
        """Wait for pod (non-PTY mode)."""
        start_time = asyncio.get_running_loop().time()
        last_status = None
        
        while asyncio.get_running_loop().time() - start_time < timeout:
            instances = self.config_manager.get_user_instances(self.username)
            instance = next((i for i in instances if i["name"] == instance_name), None)
            
            if instance:
                status = instance.get("status")
                pod_name = instance.get("podName")
                
                if status == "Running" and pod_name:
                    return pod_name
                
                if status != last_status:
                    if last_status:
                        self._chan.write(b"\r\n")
                    self._chan.write(f"Instance status: {status} ".encode('utf-8'))
                    last_status = status
                else:
                    self._chan.write(b".")
            
            await asyncio.sleep(0.5)
        return None

    def data_received(self, data, datatype):
        if self._app and self._app.driver:
            self._app.driver.feed_data(data)
        elif self._master_fd:
            os.write(self._master_fd, data.encode('utf-8') if isinstance(data, str) else data)
        elif self._process_stdin:
            self._process_stdin.write(data.encode('utf-8') if isinstance(data, str) else data)

    def eof_received(self):
        print("WhistlerSession.eof_received", file=sys.stderr, flush=True)
        if self._master_fd:
            try:
                # Send EOT (Ctrl-D) to PTY
                os.write(self._master_fd, b'\x04')
            except Exception as e:
                 print(f"Error sending EOT to PTY: {e}", file=sys.stderr)
        elif self._process_stdin:
            try:
                if self._process_stdin.can_write_eof():
                     self._process_stdin.write_eof()
                else:
                     self._process_stdin.close()
            except Exception as e:
                 print(f"Error closing stdin on EOF: {e}", file=sys.stderr)
        return False # Continue to allow output from command processing

    def terminal_size_changed(self, width, height, pixwidth, pixheight):
        if self._app:
            self._pending_size = (width, height)
            
            if not self._resize_timer:
                # Leading edge: process immediately
                self._process_resize()
                # Start cooldown timer
                loop = asyncio.get_running_loop()
                self._resize_timer = loop.call_later(0.1, self._resize_cooldown_expired)
            
        elif self._master_fd:
             winsize = struct.pack("HHHH", height, width, 0, 0)
             fcntl.ioctl(self._master_fd, termios.TIOCSWINSZ, winsize)

    def _process_resize(self):
        if self._app and self._pending_size:
            width, height = self._pending_size
            self._app.post_message(Resize(Size(width, height), Size(width, height)))
            self._last_processed_size = self._pending_size

    def _resize_cooldown_expired(self):
        # Trailing edge: if pending size is different from what we last processed, process it now
        if self._pending_size != self._last_processed_size:
             self._process_resize()
             # Restart timer to maintain rate limit if we just processed
             loop = asyncio.get_running_loop()
             self._resize_timer = loop.call_later(0.1, self._resize_cooldown_expired)
        else:
             self._resize_timer = None

    def connection_lost(self, exc):
        print(f"WhistlerSession.connection_lost: {exc}", file=sys.stderr, flush=True)
        if self._app_task:
            print("Cancelling app task", file=sys.stderr, flush=True)
            self._app_task.cancel()
        if self._shell_task:
            print(f"Cancelling shell task {self._shell_task}", file=sys.stderr, flush=True)
            self._shell_task.cancel()
        if self._agent_task:
            print("Cancelling agent task", file=sys.stderr, flush=True)
            self._agent_task.cancel()

    async def _bridge_agent(self, pod_name):
        print(f"Starting agent bridge: {self.local_agent_path} -> pod {pod_name}:{self.pod_socket_path}", file=sys.stderr)
        try:
            # Ensure socat is available in the pod
            socat_bin = "socat"
            if not await self._is_command_available(pod_name, "socat"):
                print(f"socat not found in pod {pod_name}, attempting to inject static binary...", file=sys.stderr)
                socat_bin = "/tmp/socat-static"
                if not await self._is_file_present(pod_name, socat_bin):
                     await self._inject_static_socat(pod_name, socat_bin)
            
            # Connect to local agent socket
            local_reader, local_writer = await asyncio.open_unix_connection(self.local_agent_path)
            
            # Start socat in pod using the determined binary path
            # Using fork again to allow multiple sequential connections (ssh behavior)
            cmd = [
                "kubectl", "exec", "-i", pod_name, "-n", self.config_manager.namespace, "--",
                socat_bin, f"UNIX-LISTEN:{self.pod_socket_path},fork,mode=600", "STDIO"
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            async def forward(reader, writer, name):
                try:
                    while True:
                        data = await reader.read(4096)
                        if not data:
                            print(f"Bridge {name} closed (EOF)", file=sys.stderr)
                            break
                        writer.write(data)
                        await writer.drain()
                except Exception as e:
                    print(f"Bridge {name} error: {e}", file=sys.stderr)
                finally:
                    try:
                        writer.close()
                    except:
                        pass
            
            # Helper to read stderr
            async def log_stderr(reader):
                while True:
                    line = await reader.readline()
                    if not line: break
                    print(f"Agent bridge stderr: {line.decode().strip()}", file=sys.stderr)

            # local -> remote (process.stdin)
            t1 = asyncio.create_task(forward(local_reader, process.stdin, "local->remote"))
            # remote (process.stdout) -> local
            t2 = asyncio.create_task(forward(process.stdout, local_writer, "remote->local"))
            # stderr logger
            t3 = asyncio.create_task(log_stderr(process.stderr))
            
            await asyncio.gather(t1, t2)
            
        except Exception as e:
             print(f"Agent bridge failed: {e}", file=sys.stderr)
        finally:
             print("Agent bridge finished", file=sys.stderr)

    async def _is_command_available(self, pod_name, cmd):
        check_cmd = ["kubectl", "exec", pod_name, "-n", self.config_manager.namespace, "--", "command", "-v", cmd]
        process = await asyncio.create_subprocess_exec(
            *check_cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        return await process.wait() == 0

    async def _is_file_present(self, pod_name, path):
        check_cmd = ["kubectl", "exec", pod_name, "-n", self.config_manager.namespace, "--", "test", "-f", path]
        process = await asyncio.create_subprocess_exec(
            *check_cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        return await process.wait() == 0

    async def _inject_static_socat(self, pod_name, target_path):
        # Use bundled binary
        local_binary = "/app/bin/socat_x64"
        
        # Fallback for local development (outside container)
        if not os.path.exists(local_binary):
            # Try path relative to current directory
            local_binary = os.path.join(os.getcwd(), "bin", "socat_x64")
            
        if not os.path.exists(local_binary):
             raise Exception(f"Bundled socat binary not found at {local_binary}")
        
        # Inject into pod
        print(f"Injecting static socat from {local_binary} to {pod_name}:{target_path}...", file=sys.stderr)
        # Use cat < local | kubectl exec ... "cat > target && chmod +x target"
        inject_cmd = [
            "kubectl", "exec", "-i", pod_name, "-n", self.config_manager.namespace, "--",
            "sh", "-c", f"cat > {target_path} && chmod +x {target_path}"
        ]
        
        with open(local_binary, "rb") as f:
            process = await asyncio.create_subprocess_exec(
                *inject_cmd,
                stdin=f,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                 raise Exception(f"Failed to inject socat: {stderr.decode()}")


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


