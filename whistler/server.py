import asyncio
import asyncssh
import sys
import os
import logging
import re
import threading
from kubernetes import client
from kubernetes import watch as k8s_watch
import traceback
from textual.driver import Driver
from textual.app import App
from textual.geometry import Size
from textual.events import Resize
from textual._xterm_parser import XTermParser
import time
from whistler.tui import WhistlerApp, LoadingScreen, ssh_config_stanza
from whistler import relay
from whistler.status import status_group
from whistler.logsetup import quiet_chatty_libraries

import argparse
from functools import partial
from whistler.config import (CHANNEL_RELAY, CHANNEL_SSH, ConfigManager,
                             KubeConfigManager, SESSION_SSH_PORT,
                             SSH_POSTURE_DIRECT, target_channels)
from asyncio import Event

logger = logging.getLogger("whistler.server")

# How long a jump connection waits for the target's sshd to answer, and how
# often it retries in the meantime. Generous because the wait covers a cold
# VM boot (cloud-init, the NFS home mount) and the client sees it as nothing
# worse than a slow connect — `ssh box.w` on a stopped instance is supposed
# to start it and wait.
JUMP_CONNECT_TIMEOUT = float(os.environ.get("WHISTLER_JUMP_TIMEOUT", "180"))
JUMP_RETRY_INTERVAL = 3.0

# How long an on-demand instance survives its last closed connection. A grace
# window rather than an immediate reap because the common shapes — `scp` then
# `ssh`, a `git push` followed by a build — are several short connections
# seconds apart, and reaping between them would make every one of them pay for
# a cold boot.
JUMP_EPHEMERAL_GRACE = float(os.environ.get("WHISTLER_JUMP_EPHEMERAL_GRACE", "60"))

# Live jump splices per (user, instance). The reap signal is this reaching
# zero: an SSH connection has no "session end" a gateway can see, since the
# client may open several channels and close them independently, so the
# gateway counts them. Process-local, which is the known limitation — a
# gateway restart forgets the counts and leaves an on-demand instance running
# until someone connects and disconnects again (see design/proxyjump.md).
_JUMP_SPLICES = {}


class _TrackedForwarder(asyncssh.forward.SSHForwarder):
    """A spliced connection that tells the gateway when it closes, so
    on-demand instances can be reaped once nothing is attached."""

    def __init__(self, peer, on_close):
        super().__init__(peer)
        self._on_close = on_close

    def connection_lost(self, exc):
        super().connection_lost(exc)
        on_close, self._on_close = self._on_close, None
        if on_close:
            on_close()

# A session CR is named `<user>-<name>`, so an instance name is a DNS-1123
# label minus the leading-digit allowance we don't need to police here.
INSTANCE_NAME_RE = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$")

# Honour the pre-ProxyJump `user-<instance>` username convention. Deprecated:
# instances are addressed by name through the jump mechanism now, and the
# convention costs a real bug (a username containing a dash). Kept for one
# release so muscle memory and existing scripts don't break; set false to
# turn it off, and it goes away entirely after that.
LEGACY_USERNAME_ROUTING = os.environ.get(
    "WHISTLER_LEGACY_USERNAME_ROUTING", "true").strip().lower() in (
        "1", "true", "yes")


def strip_ssh_suffix(dest_host: str, suffix: str) -> str:
    """The instance name inside a dialled hostname.

    The suffix (``.w``) is a client-side convention that exists so one
    ``Host *.w`` stanza matches every instance; nothing resolves it, and the
    gateway is the only thing that ever parses it. Names are accepted with or
    without it, so `ssh box.w` and a bare `ssh -J gw box` both work.
    """
    host = (dest_host or "").strip().rstrip(".")
    if suffix and host.lower().endswith(suffix.lower()):
        host = host[:-len(suffix)]
    return host

class WhistlerDriver(Driver):
    def __init__(self, next_driver: Driver | None = None, *, debug: bool = False, size: tuple[int, int] | None = None, **kwargs):
        super().__init__(next_driver, debug=debug, size=size)
        self._parser = XTermParser(debug=debug)
        self.exit_event = Event()
        logger.debug("WhistlerDriver initialized")

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
        logger.debug("WhistlerDriver.start_application_mode")
        
        # Send initial size event
        size = (80, 24) # Default fallback
        if self._app and hasattr(self._app, 'session') and self._app.session:
             size = self._app.session.initial_term_size
             logger.info(f"Using initial_term_size from session: {size}")
        elif self._app and hasattr(self._app, 'initial_term_size'):
             size = self._app.initial_term_size
             logger.info(f"Using initial_term_size from app: {size}")
        elif self._app and self._app.ssh_channel:
             term_size = self._app.ssh_channel.get_terminal_size()
             if term_size:
                 size = term_size[:2]
        
        self.set_mouse_tracking(True)
        self.write("\x1b[?1049h") # Alt screen
        self.write("\x1b[?25l")   # Hide cursor
        self.flush()

        event = Resize(Size(*size), Size(*size))
        self.process_message(event)
        
        # Dispatch again after a short delay to ensure app is ready
        loop = asyncio.get_running_loop()
        loop.call_later(0.05, self.process_message, event)

    def set_mouse_tracking(self, enabled: bool) -> None:
        """Turn xterm mouse reporting on or off mid-run.

        A screen whose content the user has to copy out (the launcher's `?`)
        turns it off while it is open: a terminal in reporting mode delivers
        drags to the application, so native selection stops working — see
        SshHelpScreen in whistler/tui.py.
        """
        end = "h" if enabled else "l"
        for mode in ("1000", "1006", "1015"):
            self.write(f"\x1b[?{mode}{end}")
        self.flush()

    def disable_input(self) -> None:
        logger.debug("WhistlerDriver.disable_input")
        self.exit_event.set()

    def stop_application_mode(self) -> None:
        logger.debug("WhistlerDriver.stop_application_mode")
        self.set_mouse_tracking(False)
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
        logger.debug("LoadingApp.on_mount")
        self.loading_screen = LoadingScreen(initial_status=self.initial_status)
        self.push_screen(self.loading_screen)
    
    def update_status(self, status: str) -> None:
        """Update the loading screen status."""
        if self.loading_screen:
            self.loading_screen.update_status(status)
    
    def request_exit(self) -> None:
        """Request the app to exit."""
        logger.debug("LoadingApp.request_exit")
        self._should_exit = True
        self.exit()

async def start_server():
    parser = argparse.ArgumentParser(description="Whistler SSH Server")
    parser.add_argument("--kubeconfig", help="Path to kubeconfig file")
    parser.add_argument("--in-cluster", action="store_true", help="Run in Kubernetes in-cluster mode")
    args = parser.parse_args()

    # Always run in K8s mode
    mode = "in-cluster" if args.in_cluster else f"config: {args.kubeconfig}" if args.kubeconfig else "default"
    
    # Configure logging
    log_level = os.environ.get("WHISTLER_LOG_LEVEL", "DEBUG").upper()
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr,
        force=True
    )
    lib_level = quiet_chatty_libraries(log_level)
    logger.info(f"Starting in Kubernetes mode ({mode}) with log level "
                f"{log_level} (libraries: {lib_level})")

    config_manager = KubeConfigManager(kubeconfig=args.kubeconfig)

    # The SSH host CA (design/proxyjump.md). Ensured here rather than left to
    # the operator's first VM so the `@cert-authority` line Whistler hands
    # users exists from the moment the gateway is up. Idempotent and
    # race-tolerant: whoever loses simply reads the other's key.
    try:
        if config_manager.ensure_ssh_ca():
            logger.info("SSH host CA ready: %s",
                        config_manager.get_ssh_known_hosts_line())
    except Exception as e:
        # Not fatal: without a CA, guests fall back to per-instance TOFU,
        # which is exactly the pre-CA status quo.
        logger.warning(f"Could not ensure the SSH host CA: {e}")

    # Create a partial to pass config_manager to SSHServer
    server_factory = partial(SSHServer, config_manager=config_manager)

    # Handle Host Key Persistence
    host_key_path = 'ssh_host_key'
    secret_name = os.environ.get("WHISTLER_HOST_KEY_SECRET_NAME")
    
    if secret_name:
        logger.info(f"Checking for persisted host key in secret {secret_name}")
        key_data = config_manager.get_server_host_key(secret_name)
        
        if key_data:
            logger.info("Found persisted host key, using it.")
            with open(host_key_path, 'wb') as f:
                f.write(key_data)
            # Ensure permissions
            os.chmod(host_key_path, 0o600)
        else:
            logger.info("No persisted host key found or failed to load. Generating new one.")
            # Generate new key
            # asyncssh.create_server will generate one if file is missing, but we want to save it.
            # So we generate it manually first if not present on disk either
            if not os.path.exists(host_key_path):
                 from asyncssh.public_key import generate_private_key
                 key = generate_private_key('ssh-rsa')
                 key_data = key.export_private_key()
                 with open(host_key_path, 'wb') as f:
                     f.write(key_data)
                 os.chmod(host_key_path, 0o600)
            
            # Read it back to save to secret
            with open(host_key_path, 'rb') as f:
                key_data = f.read()
            
            if config_manager.save_server_host_key(secret_name, key_data):
                logger.info(f"Persisted new host key to secret {secret_name}")
            else:
                logger.error(f"Failed to persist host key to secret {secret_name}")

    await asyncssh.create_server(server_factory, '', 8022,
                                 server_host_keys=[host_key_path],
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
        logger.info('SSH connection received from %s.' % conn.get_extra_info('peername')[0])
        self._conn = conn

    def connection_lost(self, exc):
        if exc:
            logger.error('SSH connection error: ' + str(exc))
        else:
            logger.info('SSH connection closed.')

    def subsystem_requested(self, subsystem):
        # Subsystems are handled per-session (WhistlerSession).
        return False

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
            
        logger.warning(f"Dev mode: allowing {username} via password auth")
        real_user, parts = self._split_username(username)
        self._resolve_target(real_user, parts)
        return True

    def public_key_auth_supported(self):
        return True
        

    def _resolve_target(self, real_user, parts):
        """Set username/target_type/target_name from the SSH username.

        The username is just the username: `ssh alice@gateway` lands in the
        TUI, and instances are addressed by name through the jump mechanism
        (`ssh alice@box.w`), which is what `parts` used to be smuggling.

        The legacy convention — `alice-<template>` / `alice-<instance>` — is
        still honoured when WHISTLER_LEGACY_USERNAME_ROUTING is on (the
        default, for one release), but only as a *fallback*: a username that
        names a real user is taken whole first. That ordering alone fixes the
        long-standing bug where a user whose name contains a dash could never
        log in, since `alice-smith` now resolves to the user before it is
        considered as user `alice` wanting instance `smith`.
        """
        self.username = real_user
        self.target_type = "tui"
        if len(parts) == 1 or not LEGACY_USERNAME_ROUTING:
            return

        # Only an existing instance. The old convention also let a *template*
        # name here mean "make me one and connect" — dropped along with the
        # jump's equivalent: creating from a template belongs in an interface
        # that can show progress, not behind a connection that either blocks
        # or times out (design/proxyjump.md).
        suffix = "-".join(parts[1:])
        logger.warning(
            f"Deprecated username routing: '{real_user}-{suffix}'. Use "
            f"`ssh {suffix}{getattr(self.config_manager, 'ssh_domain_suffix', '.w')}` "
            f"with a ProxyJump stanza instead (design/proxyjump.md).")
        self.target_type = "instance"
        self.target_name = suffix
        self.active_instance_name = suffix

    def _split_username(self, username):
        """(real_user, parts) for an SSH username.

        The whole string is preferred when it names a real user, so a user
        called `alice-smith` authenticates as themselves rather than being
        read as `alice` asking for instance `smith` — the bug the old
        unconditional split had. Only a username that is *not* a user falls
        back to the legacy convention.
        """
        if not LEGACY_USERNAME_ROUTING or self.config_manager.user_exists(username):
            return username, [username]
        parts = username.split('-')
        return parts[0], parts

    def validate_public_key(self, username, key):
        real_user, parts = self._split_username(username)

        # Check for dev mode bypass
        if os.environ.get("WHISTLER_AUTH_ALLOW_ANY") == "true":
            logger.warning(f"Dev mode: allowing {real_user} without key check")
            self._resolve_target(real_user, parts)
            return True

        # Check if user exists and key matches
        if not self.config_manager.user_exists(real_user):
            logger.warning(f"User {real_user} not found")
            return False

        allowed_keys = self.config_manager.get_user_public_keys(real_user)

        # Compare canonical key material for exact equality. The previous
        # substring check (`key_data in allowed`) could be fooled by a shorter
        # key whose base64 happened to be a substring of an authorized one.
        presented = key.public_data
        for allowed in allowed_keys:
            try:
                allowed_key = asyncssh.import_public_key(allowed)
            except (asyncssh.KeyImportError, ValueError) as e:
                logger.warning(f"Skipping malformed authorized key for {real_user}: {e}")
                continue
            if allowed_key.public_data == presented:
                self._resolve_target(real_user, parts)
                logger.info(f"User {real_user} authenticated via public key. Target: {self.target_type} {self.target_name}")
                return True

        logger.warning(f"Public key validation failed for {real_user}")
        return False

    def session_requested(self):
        logger.debug("SSHServer.session_requested")
        target_name = self.target_name
        template_name = None
        is_ephemeral = False
        
        if self.target_type == "template":
            import secrets
            template_name = self.target_name
            target_name = f"{self.target_name}-{secrets.token_hex(4)}"
            is_ephemeral = True
            logger.info(f"SSHServer: Generated ephemeral name {target_name} for template {template_name}")

        return WhistlerSession(
            server=self,
            config_manager=self.config_manager, 
            username=self.username,
            target_type=self.target_type,
            target_name=target_name,
            template_name=template_name,
            is_ephemeral=is_ephemeral
        )
    
    async def connection_requested(self, dest_host, dest_port, orig_host, orig_port):
        """A `direct-tcpip` channel open — the one place SSH carries a
        destination *name* to a server we control, and therefore how Whistler
        addresses instances without encoding them in the username
        (design/proxyjump.md).

        Two callers land here, told apart by the destination:

        - ``localhost``/``127.0.0.1``: a `-L` port forward from a session this
          gateway is already bridging via ``kubectl exec``. Legacy; it goes
          away with the exec bridge.
        - anything else: a ProxyJump (`ssh -J gw box.w`). The name is resolved
          against this user's own sessions and the channel is spliced to that
          instance's sshd — the gateway moves bytes and never sees plaintext.
        """
        logger.info(f"Connection requested: {dest_host}:{dest_port} from {orig_host}:{orig_port}")
        return await self._jump_to_instance(dest_host, dest_port)

    async def _jump_to_instance(self, dest_host, dest_port):
        """Route a ProxyJump channel to the named instance's sshd.

        Fail-closed at every step: an unparseable name, a name this user has no
        session for, a port other than sshd, or a zone whose posture forbids
        direct SSH all refuse the channel rather than falling through to
        something permissive. The gateway must never become a generic TCP
        relay — the set of instances it will splice to is the access-control
        decision for the whole SSH plane.
        """
        loop = asyncio.get_running_loop()
        cm = self.config_manager
        suffix = getattr(cm, "ssh_domain_suffix", ".w")
        name = strip_ssh_suffix(dest_host, suffix)

        # Validated before it reaches an API call or a log line. A session CR
        # is named `<user>-<name>`, so anything that isn't a DNS-1123 label
        # cannot name one — rejecting it here keeps unresolvable junk out of
        # the Kubernetes client and out of the logs.
        if not INSTANCE_NAME_RE.match(name):
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED,
                f"Not a valid Whistler instance name: {dest_host!r}")

        # Port-pinned before anything is resolved: this mechanism reaches sshd
        # and nothing else. A user's own `-L`/`-R` forwards ride the
        # end-to-end connection and are the instance sshd's business, so they
        # never arrive here.
        if dest_port != SESSION_SSH_PORT:
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED,
                f"Only port {SESSION_SSH_PORT} is reachable through this "
                f"gateway (asked for {dest_port})")

        target = await loop.run_in_executor(
            None, cm.resolve_ssh_target, self.username, name)
        if not target:
            # Deliberately not "create it from the template of that name".
            # That reads well in a demo and behaves badly: a channel open is
            # the wrong place to wait on a cold boot, because the client has
            # nothing to show and no way to report why it is taking minutes —
            # or that it will never finish. Creating from a template belongs
            # in the launcher, which can track and explain the wait.
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED,
                f"{self.username} has no instance named '{name}'")

        # The zone's ceiling already narrowed by this user's (and their
        # groups') channel grant — the third axis, so two people in the same
        # zone on the same instance can get different doors. `sshPosture` is
        # kept in the message because it is the zone's half of the answer and
        # the one an admin edits.
        channels = target_channels(target)
        if CHANNEL_SSH not in channels:
            posture = target.get("sshPosture", SSH_POSTURE_DIRECT)
            logger.warning(f"Jump to {name} denied for {self.username}: zone "
                           f"{target.get('zone')} posture {posture}, granted "
                           f"channels {sorted(channels)}")
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED,
                f"Direct SSH to '{name}' is not available to you in zone "
                f"'{target.get('zone')}'")

        # Declare intent and let the operator do the work: bumping
        # last-connect fires reconcile, which starts a halted VM. Harmless
        # when it is already running. Unlike the launcher, a jump has no
        # second key to press — `ssh box.w` on a stopped instance is meant to
        # bring it up (the client is showing the wait either way).
        await loop.run_in_executor(
            None, cm.trigger_instance_start, self.username, name)

        logger.info(f"Jump: {self.username}@{dest_host} -> {target['fullName']} "
                    f"({target['host']}:{target['port']})")
        return await self._wait_and_splice(target)


    def _splice_closed(self, target):
        """Last one out reaps the instance — if the gateway made it."""
        key = (self.username, target["name"])
        remaining = _JUMP_SPLICES.get(key, 1) - 1
        if remaining > 0:
            _JUMP_SPLICES[key] = remaining
            return
        _JUMP_SPLICES.pop(key, None)
        if target.get("ephemeral"):
            asyncio.create_task(self._reap_ephemeral(target))

    async def _reap_ephemeral(self, target):
        name = target["name"]
        await asyncio.sleep(JUMP_EPHEMERAL_GRACE)
        # Someone reconnected inside the grace window: it is in use again, and
        # whoever closes *that* connection gets the decision instead.
        if _JUMP_SPLICES.get((self.username, name)):
            return
        logger.info(f"Jump: reaping on-demand instance '{name}' for {self.username}")
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, self.config_manager.delete_instance, self.username, name)
        except Exception as e:
            logger.warning(f"Could not reap on-demand instance '{name}': {e}")

    async def _splice(self, target):
        """Connect to the target's sshd and hand asyncssh a forwarder for it.

        Deliberately not `conn.forward_connection`: that returns a plain
        SSHForwarder with no close hook, and the reap signal is exactly "this
        splice closed". Same two steps it performs, plus the bookkeeping.
        """
        loop = asyncio.get_running_loop()
        try:
            _, peer = await loop.create_connection(
                asyncssh.forward.SSHForwarder, target["host"], target["port"])
        except OSError as exc:
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED, str(exc)) from None

        key = (self.username, target["name"])
        _JUMP_SPLICES[key] = _JUMP_SPLICES.get(key, 0) + 1
        return _TrackedForwarder(peer, partial(self._splice_closed, target))

    async def _wait_and_splice(self, target):
        """Retry the splice until the target's sshd answers.

        The client sees a slow connect, which is the right shape for "start my
        instance and let me in". Three outcomes, not two: a hard policy
        refusal has to be told apart from a slow boot, or a session the
        operator refused to start reads to the user as an unexplained timeout.
        """
        loop = asyncio.get_running_loop()
        cm = self.config_manager
        deadline = time.monotonic() + JUMP_CONNECT_TIMEOUT
        last_error = None

        while True:
            try:
                return await self._splice(target)
            except asyncssh.ChannelOpenError as e:
                last_error = e

            fresh = await loop.run_in_executor(
                None, cm.resolve_ssh_target, self.username, target["name"])
            if fresh and fresh.get("policyFailed"):
                raise asyncssh.ChannelOpenError(
                    asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED,
                    fresh.get("statusMessage")
                    or f"Policy refused to start '{target['name']}'")
            if fresh is None:
                raise asyncssh.ChannelOpenError(
                    asyncssh.OPEN_CONNECT_FAILED,
                    f"Instance '{target['name']}' disappeared while starting")

            if time.monotonic() >= deadline:
                raise asyncssh.ChannelOpenError(
                    asyncssh.OPEN_CONNECT_FAILED,
                    f"Timed out waiting for sshd on '{target['name']}' "
                    f"(phase: {fresh.get('phase')}): {last_error}")
            await asyncio.sleep(JUMP_RETRY_INTERVAL)







class WhistlerSession(asyncssh.SSHServerSession):
    def __init__(self, server=None, config_manager=None, username=None, target_type="tui", target_name=None, 
                 template_name=None, is_ephemeral=False, *args, **kwargs):
        # super().__init__(*args, **kwargs) # SSHServerSession is just object
        self.server = server
        self._app = None
        self._app_task = None
        self._chan = None
        self._shell_task = None
        self.config_manager = config_manager
        self.username = username
        self.target_type = target_type
        self.target_name = target_name
        self.initial_term_size = (80, 24)
        self._resize_timer = None
        self._pending_size = None
        self._last_processed_size = None
        self.term_type = None
        self.is_ephemeral = is_ephemeral
        self.template_name = template_name
        self.exec_command = None
        self._cleanup_done = False
        self._cleanup_lock = asyncio.Lock()
        # Set while a gateway-mediated SSH relay is bridging this channel to a
        # session's own sshd (whistler/relay.py). Mutually exclusive with the
        # TUI app and with the legacy exec bridge's PTY.
        self._relay = None
        # True while a handover launched from the launcher is running: the
        # channel outlives the remote shell because the TUI comes back.
        self._return_to_tui = False
        logger.debug(f"WhistlerSession initialized: user={username}, type={target_type}, target={target_name}, ephemeral={is_ephemeral}")

    async def _cleanup_ephemeral(self):
        """Cleanup ephemeral instance if not already done."""
        logger.debug(f"WhistlerSession._cleanup_ephemeral called. user={self.username}, target={self.target_name}, ephemeral={self.is_ephemeral}, done={self._cleanup_done}")
        async with self._cleanup_lock:
            if not self.is_ephemeral or self._cleanup_done:
                logger.warning("WhistlerSession._cleanup_ephemeral: Skipping cleanup (already done or not ephemeral)")
                return

            instance_name = self.target_name
            if not instance_name:
                logger.debug("WhistlerSession._cleanup_ephemeral: No target name to delete")
                return

            try:
                logger.debug(f"WhistlerSession._cleanup_ephemeral: Deleting instance {instance_name}")
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.config_manager.delete_instance, self.username, instance_name)
                logger.debug(f"WhistlerSession._cleanup_ephemeral: Successfully deleted {instance_name}")
            except Exception as e:
                logger.error(f"Error calling delete_instance: {e}")
                traceback.print_exc(file=sys.stderr)
            
            self._cleanup_done = True

    def connection_made(self, chan):
        logger.debug("WhistlerSession.connection_made")
        self._chan = chan
        self._chan.set_encoding(None)

    def connection_lost(self, exc):
        if self._app:
             self._app.exit()
        
        # Ensure cleanup happens if session ends (e.g. sftp disconnect)
        if self.is_ephemeral and not self._cleanup_done:
             asyncio.create_task(self._cleanup_ephemeral())

    def subsystem_requested(self, subsystem):
        """No subsystems on the gateway session.

        SFTP used to be served here by shelling out to `kubectl exec`. It is
        now native and end-to-end: `sftp box.w` through the ProxyJump stanza
        talks to the instance's own sshd, so the gateway has nothing to
        reimplement (design/proxyjump.md)."""
        if subsystem == "sftp":
            logger.info("SFTP against the gateway is no longer served; use "
                        "the jump (sftp <instance>.w) to reach the instance "
                        "directly.")
        return False

    def pty_requested(self, term_type, term_size, term_modes):
        logger.debug(f"WhistlerSession.pty_requested: {term_type} {term_size}")
        self.initial_term_size = (term_size[0], term_size[1])
        self.term_type = term_type
        return True

    def shell_requested(self):
        logger.debug("WhistlerSession.shell_requested")
        return True

    def data_received(self, data, datatype):
        """User input goes to whichever of the two things owns the screen: the
        launcher, or a relayed remote session."""
        if self._app:
             # Check for Ctrl-C explicitly to handle race conditions where driver is not ready or fails to route
             is_ctrl_c = False
             if isinstance(data, bytes) and b'\x03' in data:
                 is_ctrl_c = True
             elif isinstance(data, str) and '\x03' in data:
                 is_ctrl_c = True

             if is_ctrl_c:
                 if hasattr(self._app, 'exit'):
                     self._app.exit("cancelled")
                     return

             if hasattr(self._app, 'driver') and self._app.driver:
                self._app.driver.feed_data(data)
        elif self._relay:
             self._relay.write(data.encode('utf-8') if isinstance(data, str) else data)

    def signal_received(self, signal):
        # print(f"DEBUG: WhistlerSession.signal_received: {signal}", file=sys.stderr, flush=True)
        if self._relay:
            # Pass it on rather than interpreting it: the remote shell's job.
            self._relay.send_signal(signal)
            return
        if signal == 'INT' or signal == 'TERM':
             if self._app and hasattr(self._app, 'action_cancel'):
                 asyncio.create_task(self._app.action_cancel())
             elif self._app and hasattr(self._app, 'exit'):
                 self._app.exit("cancelled")

    def break_received(self, msec):
        logger.info(f"WhistlerSession.break_received: {msec}")
        if self._relay:
            self._relay.send_break(msec)
            return
        # Treat break as Ctrl-C
        if self._app and hasattr(self._app, 'action_cancel'):
             asyncio.create_task(self._app.action_cancel())
        elif self._app and hasattr(self._app, 'exit'):
             self._app.exit("cancelled")

    def exec_requested(self, command):
        logger.info(f"WhistlerSession.exec_requested: {command}")
        self.exec_command = command
        return True

    async def _run_gateway_command(self, command):
        """Answer the handful of commands the *gateway* itself understands.

        `ssh <gateway> <command>` used to be a flat error. It is now the way
        to obtain the two strings a user has to get into their own ~/.ssh —
        by redirection rather than by selecting text out of a full-screen TUI,
        which a terminal in mouse-reporting mode will not let you do:

            ssh gw ssh-config  >> ~/.ssh/config
            ssh gw known-hosts >> ~/.ssh/known_hosts

        Deliberately a closed list of nouns, not a shell: this channel is the
        gateway process, and the place to run commands is an instance.
        """
        # LF, not CRLF: the point is redirection into a file. A client that
        # asked for a PTY wants its terminal's convention instead.
        nl = "\r\n" if self.term_type else "\n"
        name = (command or "").strip()
        cm = self.config_manager

        # connection_made puts the channel in binary mode (set_encoding(None)),
        # so everything written to it has to be bytes — a str reaches
        # asyncssh's `bytes(data)` and raises TypeError out of session_started,
        # which the client sees as "closed by remote host".
        def out(text):
            self._chan.write(text.encode('utf-8'))

        def err(text):
            self._chan.write_stderr(text.encode('utf-8'))

        if name == "known-hosts":
            # In an executor: the CA lives in a Secret and the kubernetes
            # client is synchronous, so reading it on the loop thread would
            # stall every other session for the round trip.
            line = await asyncio.get_running_loop().run_in_executor(
                None, cm.get_ssh_known_hosts_line)
            if not line:
                err(f"No SSH host CA is configured on this gateway.{nl}")
                self._chan.exit(1)
                return
            out(line + nl)
        elif name == "ssh-config":
            out(ssh_config_stanza(
                self.username,
                getattr(cm, "ssh_domain_suffix", "")).replace("\n", nl) + nl)
        else:
            err(f"Unknown gateway command '{name}'.{nl}"
                f"Try one of: known-hosts, ssh-config — or connect to an "
                f"instance: ssh <instance>"
                f"{getattr(cm, 'ssh_domain_suffix', '')}{nl}")
            self._chan.exit(1)
            return
        self._chan.exit(0)

    def _new_app(self):
        """Build a launcher app with the client's terminal advertised in the
        environment.

        Textual snapshots os.environ when the App is *constructed*, and Rich
        derives the colour system from COLORTERM/TERM at that same moment — so
        this has to wrap every construction, not just the first one. The app
        built after a relay handover missed it and came back in 8 colours
        (see _run_app).
        """
        old_term = os.environ.get('TERM')
        old_colorterm = os.environ.get('COLORTERM')

        if self.term_type:
            os.environ['TERM'] = self.term_type
            # Assume truecolor support for modern SSH clients if not specified
            os.environ['COLORTERM'] = 'truecolor'
        try:
            app = WhistlerApp(driver_class=WhistlerDriver,
                              config_manager=self.config_manager,
                              username=self.username, session=self)
        finally:
            # Restore environment
            if old_term is None: os.environ.pop('TERM', None)
            else: os.environ['TERM'] = old_term

            if old_colorterm is None: os.environ.pop('COLORTERM', None)
            else: os.environ['COLORTERM'] = old_colorterm

        app.ssh_channel = self._chan
        return app

    def session_started(self):
        logger.debug("WhistlerSession.session_started")

        if self.target_type == "tui":
            if self.exec_command:
                 self._shell_task = asyncio.create_task(
                     self._run_gateway_command(self.exec_command))
            else:
                self._app = self._new_app()
                self._app_task = asyncio.create_task(self._run_app())
        elif self.target_type == "instance":
            # Find the instance
            self._shell_task = asyncio.create_task(self._connect_to_instance(command=self.exec_command))
        else:
            logger.warning(f"Target type {self.target_type} unknown, falling back to TUI")
            self._app = self._new_app()
            self._app_task = asyncio.create_task(self._run_app())

    async def _run_app(self):
        """Run the launcher, and keep running it around any handovers.

        Choosing "connect" exits the app with the instance name rather than
        connecting in place: a Textual app cannot hand its own channel to a
        remote shell. The session does the relay, and when the remote shell
        ends we come back with a fresh app — so the TUI behaves like a place
        you return to rather than a thing you leave.
        """
        logger.debug("WhistlerSession._run_app starting")
        try:
            while True:
                await self._app.run_async()
                choice = getattr(self._app, "return_value", None)
                self._app = None
                if not (isinstance(choice, tuple) and choice[:1] == ("connect",)):
                    break

                self.target_name = choice[1]
                self.active_instance_name = choice[1]
                if self.server:
                    self.server.active_instance_name = choice[1]
                self._return_to_tui = True
                try:
                    # The launcher only offers connect on a Running session,
                    # and starting one is its own key there — so this path
                    # connects, it does not boot.
                    await self._connect_to_instance(start=False)
                finally:
                    self._return_to_tui = False

                self._app = self._new_app()
        except Exception as e:
            logger.error(f"App error: {e}")
            traceback.print_exc(file=sys.stderr)
        finally:
            logger.debug("WhistlerSession._run_app finished")
            self._chan.exit(0)



    def _fail(self, message):
        """Report why a connection didn't happen, and end the session unless
        the launcher is waiting to take the screen back."""
        try:
            self._chan.write(f"\r\n{message}\r\n".encode('utf-8'))
            if not self._return_to_tui:
                self._chan.exit(1)
        except Exception:
            pass

    async def _sshd_ready(self, target):
        """Whether the instance is answering on its SSH port yet."""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(target["host"], target["port"]), 5)
        except (OSError, asyncio.TimeoutError):
            return False
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
        return True

    async def _connect_to_instance(self, loading_screen=None, command=None,
                                   start=True):
        """Connect this channel to an instance over the SSH relay.

        Same three-outcome wait as the jump path — ready, still booting, or
        refused by policy — because a session the operator declined to start
        must say so rather than time out silently. The difference is that
        here there is a terminal to explain the wait in.

        ``start=False`` connects to what is already running and nothing else.
        That is how the launcher calls it: starting a session is its own key
        there now, so connect must not smuggle a cold boot in behind a
        progress-dot wait the user cannot escape. The direct paths (a named
        instance target, and the jump) still start on connect — `ssh box.w`
        has no launcher to press a key in.
        """
        loop = asyncio.get_running_loop()
        cm = self.config_manager
        name = self.target_name

        target = await loop.run_in_executor(
            None, cm.resolve_ssh_target, self.username, name)
        if not target:
            self._fail(f"No instance named '{name}'.")
            return

        # The relay is its own channel: a zone posture of `relay` permits
        # exactly this path and forbids the end-to-end jump, and a channel
        # grant can close it for this user while leaving it open for the
        # colleague on the same instance.
        if CHANNEL_RELAY not in target_channels(target):
            self._fail(f"Connecting to '{name}' is not available to you in "
                       f"zone '{target.get('zone')}'.")
            return

        if start:
            # Declare intent; the operator starts a stopped instance.
            await loop.run_in_executor(
                None, cm.trigger_instance_start, self.username, name)

        interactive = bool(self.term_type) and not command
        deadline = time.monotonic() + JUMP_CONNECT_TIMEOUT
        announced = False
        while not await self._sshd_ready(target):
            fresh = await loop.run_in_executor(
                None, cm.resolve_ssh_target, self.username, name)
            if fresh is None:
                self._fail(f"Instance '{name}' disappeared while starting.")
                return
            if fresh.get("policyFailed"):
                self._fail(fresh.get("statusMessage")
                           or f"Policy refused to start '{name}'.")
                return
            phase = fresh.get("phase")
            at_rest = bool(phase) and status_group(phase) in ("Stopped",
                                                              "Error")
            if not start and at_rest:
                # Nothing is coming: we did not ask for a start, and the
                # session is not on its way up. Say so now rather than
                # spending the whole connect budget on dots — the launcher is
                # still behind this channel, and `s` is the answer.
                self._fail(f"'{name}' is not running (phase: {phase}). Start "
                           f"it from the launcher first.")
                return
            if time.monotonic() >= deadline:
                self._fail(f"Timed out waiting for '{name}' to accept SSH "
                           f"(phase: {fresh.get('phase')}).")
                return
            if interactive:
                if not announced:
                    self._chan.write(
                        f"Waiting for {name} ".encode('utf-8'))
                    announced = True
                self._chan.write(b".")
            await asyncio.sleep(JUMP_RETRY_INTERVAL)

        if interactive and announced:
            self._chan.write(b"\r\n")

        error = await self._run_relay_shell(target, command=command)
        if error:
            # The relay's own words, not a guess. The generic "if this is a
            # pod it may not run an sshd yet" that used to stand here was
            # wrong for every other cause — a certificate the gateway won't
            # accept reads identically to a missing sshd — and the real reason
            # only existed in a log line that had usually rotated away by the
            # time anyone looked.
            self._fail(f"Could not open a session on '{name}': {error}")


    async def _run_relay_shell(self, target, command=None):
        """Bridge this channel to the instance's own sshd (whistler/relay.py).

        The successor to `_run_pod_shell`: one SSH client connection instead of
        a PTY, a `kubectl exec` subprocess, fd readers and a hand-rolled agent
        bridge — and identical for pods and VMs, since both are just sshd.

        Returns None once the relay has run to completion, or the reason it
        could not be established — a message meant for the user's terminal,
        since they are the only one who can act on "your instance's host
        certificate isn't one I trust".
        """
        loop = asyncio.get_running_loop()
        cm = self.config_manager
        private_key, ca_pub = await asyncio.gather(
            loop.run_in_executor(None, cm.get_vm_access_private_key, self.username),
            loop.run_in_executor(None, cm.get_ssh_ca_public_key),
        )

        term_size = None
        if self.term_type:
            cols, rows = self._pending_size or self.initial_term_size or (80, 24)
            term_size = (cols, rows)

        try:
            self._relay = await relay.open_relay(
                chan=self._chan,
                host=target["host"], port=target["port"],
                username=self.username,
                private_key=private_key,
                ca_public_key=ca_pub,
                term_type=self.term_type,
                term_size=term_size,
                command=command,
                # The user's own forwarded agent, passed straight through, so
                # a relayed shell can still reach their keys.
                agent_path=self._chan.get_connection().get_agent_path(),
            )
        except relay.RelayError as e:
            logger.warning(f"Relay to {target['name']} failed: {e}")
            self._relay = None
            return str(e)

        try:
            status = await self._relay.wait_closed()
        finally:
            self._relay = None

        # A handover launched from the TUI returns to it, so the channel has to
        # stay open; a direct connection is the whole session and ends with it.
        if not self._return_to_tui:
            try:
                self._chan.exit(status if status is not None else 0)
            except OSError:
                pass
        return None


    def _trigger_reconcile(self, instance_full_name, namespace):
        """Bump an annotation on the Session so the operator's update handler
        fires and (re)creates the pod. Pod creation is owned by the operator; the
        server only declares intent and waits. Runs in an executor (the
        kubernetes client is synchronous)."""
        try:
            self.config_manager.api.patch_namespaced_custom_object(
                self.config_manager.group, self.config_manager.version, namespace,
                "sessions", instance_full_name,
                {"metadata": {"annotations": {"whistler/last-connect": str(time.time())}}}
            )
        except Exception as e:
            logger.error(f"Failed to trigger reconcile for {instance_full_name}: {e}")

    async def _watch_pod_ready(self, namespace, instance_full_name, status_cb=None, timeout=None):
        """Wait until the instance's pod reaches the Running phase, driven by a
        Kubernetes watch instead of polling.

        The blocking watch runs in an executor thread and only ever marshals
        plain event data back to the event loop via a queue; all UI work
        (status_cb) happens here on the loop thread, so the SSH channel is never
        touched from another thread. Returns the pod name, or None on timeout.
        """
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        stop = threading.Event()
        label_selector = f"instance={instance_full_name}"

        def run_watch():
            core_api = client.CoreV1Api()
            w = k8s_watch.Watch()
            end = None if timeout is None else time.monotonic() + timeout
            try:
                while not stop.is_set():
                    if end is not None and time.monotonic() >= end:
                        break
                    # Bound each watch chunk so we re-check the stop flag and
                    # tolerate the pod not existing yet (operator creates it).
                    remaining = None if end is None else max(1, int(end - time.monotonic()))
                    chunk = 5 if remaining is None else min(5, remaining)
                    try:
                        for event in w.stream(
                            core_api.list_namespaced_pod,
                            namespace=namespace,
                            label_selector=label_selector,
                            timeout_seconds=chunk,
                            _request_timeout=chunk + 5,
                        ):
                            if stop.is_set():
                                break
                            pod = event["object"]
                            phase = getattr(pod.status, "phase", None)
                            name = pod.metadata.name
                            terminating = pod.metadata.deletion_timestamp is not None
                            loop.call_soon_threadsafe(queue.put_nowait, ("status", phase, name, terminating))
                            if phase == "Running" and not terminating:
                                loop.call_soon_threadsafe(queue.put_nowait, ("ready", name))
                                return
                    except Exception as e:
                        # Transient API/watch errors: report and re-establish.
                        loop.call_soon_threadsafe(queue.put_nowait, ("error", str(e)))
            finally:
                w.stop()
                loop.call_soon_threadsafe(queue.put_nowait, ("done", None))

        watch_future = loop.run_in_executor(None, run_watch)
        last_status = None
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    if watch_future.done():
                        return None
                    continue

                kind = msg[0]
                if kind == "ready":
                    return msg[1]
                elif kind == "status":
                    _, phase, _name, _terminating = msg
                    if phase and phase != last_status:
                        last_status = phase
                        if status_cb:
                            status_cb(phase)
                elif kind == "error":
                    logger.debug(f"Pod watch error for {instance_full_name}: {msg[1]}")
                elif kind == "done":
                    return None
        except asyncio.CancelledError:
            stop.set()
            raise
        finally:
            stop.set()


    async def _wait_for_pod(self, instance_name, timeout=None):
        """Wait for pod to be ready (non-PTY mode), writing status to the channel."""
        ns = self.config_manager._get_user_namespace(self.username)
        full_name = f"{self.username}-{instance_name}"

        def status_cb(phase):
            try:
                if self.term_type and not self.exec_command:
                    self._chan.write(f"\r\nInstance status: {phase} ".encode('utf-8'))
            except Exception:
                pass

        return await self._watch_pod_ready(ns, full_name, status_cb=status_cb, timeout=timeout)


    def eof_received(self):
        logger.debug("WhistlerSession.eof_received")
        if self._relay:
            self._relay.write_eof()
        return True # Return True to keep channel open/manual EOF handling

    def terminal_size_changed(self, width, height, pixwidth, pixheight):
        # Always update pending size so whoever picks up next knows it
        self._pending_size = (width, height)

        if self._app:
            if not self._resize_timer:
                # Leading edge: process immediately
                self._process_resize()
                # Start cooldown timer
                loop = asyncio.get_running_loop()
                self._resize_timer = loop.call_later(0.1, self._resize_cooldown_expired)
            
        if self._relay:
             # A window-change request on the remote session: the guest's own
             # sshd delivers SIGWINCH, so this is one message instead of the
             # exec bridge's ioctl on a local PTY master.
             self._relay.change_terminal_size(width, height)

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







if __name__ == '__main__':

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


