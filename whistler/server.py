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

async def start_server():
    parser = argparse.ArgumentParser(description="Whistler SSH Server")
    parser.add_argument("--config", default="config.yaml", help="Path to configuration file")
    parser.add_argument("--kubeconfig", help="Path to kubeconfig file (enables K8s mode)")
    parser.add_argument("--in-cluster", action="store_true", help="Run in Kubernetes in-cluster mode")
    args = parser.parse_args()

    if args.kubeconfig or args.in_cluster:
        mode = "in-cluster" if args.in_cluster else f"config: {args.kubeconfig}"
        print(f"Starting in Kubernetes mode ({mode})", file=sys.stderr)
        config_manager = KubeConfigManager(kubeconfig=args.kubeconfig)
    else:
        print(f"Starting in YAML mode (config: {args.config})", file=sys.stderr)
        config_manager = YamlConfigManager(args.config)
    
    # Create a partial to pass config_manager to SSHServer
    server_factory = partial(SSHServer, config_manager=config_manager)

    await asyncssh.create_server(server_factory, '', 8022,
                                 server_host_keys=['ssh_host_key'],
                                 line_editor=False,
                                 agent_forwarding=True)

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
        
        if hasattr(self, 'term_type') and self.term_type:
            os.environ['TERM'] = self.term_type
            # Assume truecolor support for modern SSH clients if not specified
            os.environ['COLORTERM'] = 'truecolor'
            print(f"Setting env for App init: TERM={self.term_type}, COLORTERM=truecolor", file=sys.stderr)

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
         import secrets
         hex_id = secrets.token_hex(4)
         instance_name = f"{self.target_name}-{hex_id}"
         
         # Resolve full template name
         templates = self.config_manager.get_user_templates(self.username)
         template_obj = next((t for t in templates if t["name"] == self.target_name), None)
         template_ref = template_obj["fullName"] if template_obj else self.target_name
         
         self._chan.write(f"Creating ephemeral instance {instance_name} (full name: {self.username}-{instance_name}) from template {self.target_name}...\r\n".encode('utf-8'))
         
         # Pass preemptible=True for ephemeral instances? Maybe optional.
         if self.config_manager.add_instance(self.username, template_ref, instance_name):
             try:
                 # Wait for pod and connect
                 self.target_name = instance_name # Switch target to the new instance
                 await self._connect_to_instance()
             except Exception as e:
                 print(f"Error in _create_and_connect_ephemeral: {e}", file=sys.stderr, flush=True)
                 self._chan.write(f"Error connecting to instance: {e}\r\n".encode('utf-8'))
             except asyncio.CancelledError:
                 print("Task cancelled in _create_and_connect_ephemeral", file=sys.stderr, flush=True)
                 raise
             finally:
                 # Cleanup
                 print(f"Entering finally block for {instance_name}", file=sys.stderr, flush=True)
                 
                 # Try to notify user, but ignore errors if channel is closed
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

    async def _connect_to_instance(self):
        instances = self.config_manager.get_user_instances(self.username)
        instance = next((i for i in instances if i["name"] == self.target_name), None)
        
        if instance:
             pod_name = instance.get("podName")
             
             # If terminating, wait for it to finish first
             if instance.get("status") == "Terminating":
                 self._chan.write(b"Waiting for existing pod to terminate ")
                 while instance and instance.get("status") == "Terminating":
                     await asyncio.sleep(1)
                     self._chan.write(b".")
                     instances = self.config_manager.get_user_instances(self.username)
                     instance = next((i for i in instances if i["name"] == self.target_name), None)
                 self._chan.write(b"\r\n")
                 # Refresh pod_name/status after wait
                 if instance:
                     pod_name = instance.get("podName")
             
             if not pod_name or (instance and instance.get("status") != "Running"):
                 self._chan.write(f"Instance {self.target_name} is stopped. Waking up...\r\n".encode('utf-8'))
                 # Trigger operator to ensure pod exists by patching annotation
                 import time
                 try:
                     # We need the full CR name for patching
                     full_cr_name = f"{self.username}-{self.target_name}"
                     self.config_manager.api.patch_namespaced_custom_object(
                         self.config_manager.group, self.config_manager.version, self.config_manager.namespace,
                         "whistlerinstances", full_cr_name,
                         {"metadata": {"annotations": {"whistler.io/last-connect": str(time.time())}}}
                     )
                 except Exception as e:
                     print(f"Failed to patch instance: {e}", file=sys.stderr)
                 
                 # Wait for pod to be ready
                 pod_name = await self._wait_for_pod(self.target_name)
             
             if pod_name:
                self._chan.write(b"Instance is running. Starting shell...\r\n")
                
                # Update server context for forwarding
                if self.server:
                    self.server.active_instance_name = self.target_name
                    print(f"Updated server active_instance_name to {self.target_name}", file=sys.stderr)

                # Start agent bridge if needed
                if self.local_agent_path and self.pod_socket_path:
                    self._agent_task = asyncio.create_task(self._bridge_agent(pod_name))
                    # Give it a moment to start?
                    await asyncio.sleep(0.5)

                await self._run_pod_shell(pod_name)
             else:
                 self._chan.write(f"Failed to start instance {self.target_name}.\r\n".encode('utf-8'))
                 self._chan.exit(1)
        else:
             self._chan.write(f"Instance {self.target_name} not found.\r\n".encode('utf-8'))
             self._chan.exit(1)

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

        process = None
        process = None
        try:
            cmd = ["kubectl", "exec", "-it", pod_name, "-n", self.config_manager.namespace, "--"]
            
            if self.pod_socket_path:
                cmd.extend(["env", f"SSH_AUTH_SOCK={self.pod_socket_path}"])
                
            cmd.append("/bin/bash")
            
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
                    # We could await process.wait() here but we are in finally
                except ProcessLookupError:
                    pass
            
            self._chan.exit(0)

    async def _wait_for_pod(self, instance_name, timeout=60):
        start_time = asyncio.get_running_loop().time()
        last_status = None
        
        while asyncio.get_running_loop().time() - start_time < timeout:
            instances = self.config_manager.get_user_instances(self.username)
            instance = next((i for i in instances if i["name"] == instance_name), None)
            
            if instance:
                status = instance.get("status")
                pod_name = instance.get("podName")
                
                if status == "Running" and pod_name:
                    self._chan.write(b"\r\n")
                    return pod_name
                
                if status != last_status:
                    if last_status:
                        self._chan.write(b"\r\n")
                    self._chan.write(f"Instance status: {status} ".encode('utf-8'))
                    last_status = status
                else:
                    self._chan.write(b".")
            
            await asyncio.sleep(1)
        return None

    def data_received(self, data, datatype):
        # print(f"WhistlerSession.data_received: {len(data)} bytes", file=sys.stderr, flush=True)
        if self._app and self._app.driver:
            self._app.driver.feed_data(data)
        elif self._master_fd:
            os.write(self._master_fd, data.encode('utf-8') if isinstance(data, str) else data)

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
            # Connect to local agent socket
            local_reader, local_writer = await asyncio.open_unix_connection(self.local_agent_path)
            
            # Start socat in pod
            # Removed fork to ensure single stream stability for v1. 
            # We will need a loop if we want to handle multiple connections, but let's debug first.
            cmd = [
                "kubectl", "exec", "-i", pod_name, "-n", self.config_manager.namespace, "--",
                "socat", f"UNIX-LISTEN:{self.pod_socket_path},mode=600", "STDIO"
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


