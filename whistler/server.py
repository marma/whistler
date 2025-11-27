import asyncio
import asyncssh
import sys
import os
from textual.driver import Driver
from textual.app import App
from textual.geometry import Size
from textual.events import Resize
from textual._xterm_parser import XTermParser
from whistler.tui import WhistlerApp

import argparse
from functools import partial
from whistler.config import ConfigManager
from asyncio import Event

class WhistlerDriver(Driver):
    def __init__(self, next_driver: Driver | None = None, *, debug: bool = False, size: tuple[int, int] | None = None, **kwargs):
        super().__init__(next_driver, debug=debug, size=size)
        self._parser = XTermParser(debug=debug)
        self.exit_event = Event()
        print("WhistlerDriver initialized", file=sys.stderr, flush=True)

    def write(self, data: str) -> None:
        # print(f"WhistlerDriver.write: {len(data)} bytes", file=sys.stderr, flush=True)
        if self._app and self._app.ssh_channel:
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
        if len(data) > 0 and data[0] == '\x1b':
           print(f"WhistlerDriver.feed_data escape: {repr(data)}", file=sys.stderr, flush=True)
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        for event in self._parser.feed(data):
            self.process_message(event)

    def process_message(self, event: Event) -> None:
        if self._app:
            self._app.post_message(event)

async def start_server():
    parser = argparse.ArgumentParser(description="Whistler SSH Server")
    parser.add_argument("--config", default="config.yaml", help="Path to configuration file")
    args = parser.parse_args()

    config_manager = ConfigManager(args.config)
    
    # Create a partial to pass config_manager to SSHServer
    server_factory = partial(SSHServer, config_manager=config_manager)

    await asyncssh.create_server(server_factory, '', 8022,
                                 server_host_keys=['ssh_host_key'])

class SSHServer(asyncssh.SSHServer):
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.username = None

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
        self.username = username
        if self.config_manager.user_exists(username):
            print(f"User {username} authenticated (found in config)", file=sys.stderr, flush=True)
        else:
            print(f"User {username} authenticated (NOT found in config)", file=sys.stderr, flush=True)
        return True

    def session_requested(self):
        print("SSHServer.session_requested", file=sys.stderr, flush=True)
        return WhistlerSession(config_manager=self.config_manager, username=self.username)

class WhistlerSession(asyncssh.SSHServerSession):
    def __init__(self, config_manager=None, username=None, *args, **kwargs):
        # super().__init__(*args, **kwargs) # SSHServerSession is just object
        self._app = None
        self._app_task = None
        self._chan = None
        self.config_manager = config_manager
        self.username = username
        self.initial_term_size = (80, 24)
        print("WhistlerSession initialized", file=sys.stderr, flush=True)

    def connection_made(self, chan):
        print("WhistlerSession.connection_made", file=sys.stderr, flush=True)
        self._chan = chan
        self._chan.set_encoding('utf-8')

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
        self._chan.set_line_mode(False)
        self._chan.set_echo(False)
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
        finally:
            print("WhistlerSession._run_app finished", file=sys.stderr, flush=True)
            self._chan.exit(0)

    def data_received(self, data, datatype):
        # print(f"WhistlerSession.data_received: {len(data)} bytes", file=sys.stderr, flush=True)
        if self._app and self._app.driver:
            self._app.driver.feed_data(data)

    def terminal_size_changed(self, width, height, pixwidth, pixheight):
        if self._app:
            self._app.post_message(Resize(Size(width, height), Size(width, height)))

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


