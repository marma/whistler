"""KubeVirt subresource websockets for VM sessions: serial console + VNC.

VM-runtime sessions have no pod to ``kubectl exec`` into and no in-guest
streamer to reverse-proxy. Instead KubeVirt exposes two subresources on the
VMI — ``/console`` (the serial byte stream) and ``/vnc`` (raw RFB) — as
websockets on the *Kubernetes API server*, which relays them via virt-handler
to the virt-launcher's domain sockets. That path bypasses the pod network
entirely, so it works regardless of the user-namespace NetworkPolicy, and the
guest needs no agent (the noVNC viewer works from BIOS onward).

Both subresources speak the ``plain.kubevirt.io`` websocket subprotocol:
binary frames carrying the raw byte stream, no additional framing. The
browser-side bridges are byte-transparent; only the API-server dial needs
auth, which is why this module (and not ``proxy``) owns it.

Auth: the portal runs either in-cluster (ServiceAccount bearer token) or as a
host process against a kubeconfig (client certs — the integration harness).
Both shapes are read from the kubernetes client's default Configuration,
which ``KubeConfigManager.__init__`` populates at startup.
"""
import asyncio
import logging
import ssl

import aiohttp
from aiohttp import web

from kubernetes import client as k8s_client

from whistler.portal.terminal import parse_resize

logger = logging.getLogger("whistler.portal")

SUBPROTOCOL = "plain.kubevirt.io"

_SSL_CONTEXT_CACHE = None


def subresource_ws_url(base: str, namespace: str, name: str, kind: str) -> str:
    """Websocket URL for a VMI subresource (``kind`` = console | vnc)."""
    if base.startswith("https://"):
        base = "wss://" + base[len("https://"):]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://"):]
    return (f"{base}/apis/subresources.kubevirt.io/v1/namespaces/{namespace}"
            f"/virtualmachineinstances/{name}/{kind}")


def api_config():
    """(base_url, headers, ssl_context) for dialing the API server, derived
    from the kubernetes client's default Configuration.

    Auth headers come from ``auth_settings()`` — NOT ``api_key`` directly:
    the dict key for the in-cluster token has changed across client versions
    ("authorization" historically, "BearerToken" in current ones), while
    auth_settings() always yields the ready-to-send header name and value.
    They are re-derived on every call (after running the client's
    ``refresh_api_key_hook``) because bound SA tokens rotate (~1h); only the
    TLS context — CA bundle and optional kubeconfig client certs, both
    static — is cached."""
    global _SSL_CONTEXT_CACHE

    conf = k8s_client.Configuration.get_default_copy()
    if conf.refresh_api_key_hook:
        conf.refresh_api_key_hook(conf)
    headers = {}
    for setting in (conf.auth_settings() or {}).values():
        if setting.get("in") == "header" and setting.get("value"):
            headers[setting["key"]] = setting["value"]

    if _SSL_CONTEXT_CACHE is None:
        if conf.verify_ssl:
            ssl_context = ssl.create_default_context(cafile=conf.ssl_ca_cert)
        else:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        if conf.cert_file and conf.key_file:
            ssl_context.load_cert_chain(conf.cert_file, conf.key_file)
        _SSL_CONTEXT_CACHE = ssl_context

    return conf.host, headers, _SSL_CONTEXT_CACHE


async def kube_ws_client_ctx(app):
    """cleanup_ctx: a dedicated ClientSession for API-server websockets. The
    desktop proxy's session is deliberately auth/TLS-free (it dials in-cluster
    Services), so the API-server credentials live on their own session."""
    app["kube_ws_client"] = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=10),
    )
    yield
    await app["kube_ws_client"].close()


async def open_subresource_ws(session: aiohttp.ClientSession, namespace: str,
                              name: str, kind: str):
    """Dial a VMI subresource websocket (authorized, TLS). Raises
    aiohttp.ClientError on failure — callers map that to a clean HTTP error
    before the browser socket is opened."""
    base, headers, ssl_context = api_config()
    url = subresource_ws_url(base, namespace, name, kind)
    return await session.ws_connect(
        url, protocols=(SUBPROTOCOL,), headers=headers, ssl=ssl_context,
        max_msg_size=0, heartbeat=30,
    )


async def relay_ssh(browser_ws: web.WebSocketResponse, host: str, username: str,
                    private_key_pem: str, *, connect_budget: float = 90.0) -> None:
    """Bridge the xterm.js websocket to an SSH session inside the guest.

    This is the default VM web terminal: unlike the serial console it gives
    real login semantics — a fresh session per connection, PAM/MOTD on entry,
    exit actually ends it, and resize works. The guest trusts the portal's
    per-user access key (injected via cloud-init); the connection goes to the
    VMI's pod-network IP, which masquerade forwards to the guest's sshd.

    sshd lags VMI-Ready on first boot (cloud-init must create the user and
    install keys), so connection/auth failures are retried until
    ``connect_budget`` runs out — with a one-time notice to the terminal so
    the user isn't staring at a blank screen."""
    import asyncssh  # deferred: only VM terminals need an SSH client

    key = asyncssh.import_private_key(private_key_pem)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + connect_budget
    notified = False
    while True:
        try:
            conn = await asyncssh.connect(
                host, username=username, client_keys=[key],
                known_hosts=None, connect_timeout=10)
            break
        except (OSError, asyncssh.Error) as e:
            if loop.time() >= deadline:
                logger.warning(f"SSH to VM {host} never came up: {e}")
                if not browser_ws.closed:
                    await browser_ws.close(
                        message=f"could not reach SSH in the VM: {e}".encode())
                return
            if not notified:
                await browser_ws.send_bytes(
                    b"Waiting for SSH inside the VM (first boot takes a moment)...\r\n")
                notified = True
            await asyncio.sleep(2)

    try:
        # encoding=None: raw bytes both ways, the terminal owns the encoding.
        proc = await conn.create_process(
            term_type="xterm-256color", term_size=(80, 24), encoding=None)

        async def browser_to_ssh():
            async for msg in browser_ws:
                if msg.type == web.WSMsgType.TEXT:
                    resize = parse_resize(msg.data)
                    if resize is not None:
                        proc.change_terminal_size(*resize)
                        continue
                    proc.stdin.write(msg.data.encode("utf-8"))
                elif msg.type == web.WSMsgType.BINARY:
                    proc.stdin.write(msg.data)
                else:
                    break

        async def ssh_to_browser():
            while True:
                data = await proc.stdout.read(65536)
                if not data:
                    break
                await browser_ws.send_bytes(data)

        done, pending = await asyncio.wait(
            [asyncio.ensure_future(browser_to_ssh()),
             asyncio.ensure_future(ssh_to_browser())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.warning(f"VM SSH relay error: {exc}")
    finally:
        conn.close()
        if not browser_ws.closed:
            await browser_ws.close()


async def relay_console(browser_ws: web.WebSocketResponse, upstream_ws) -> None:
    """Bridge the xterm.js websocket to the VMI serial console until either
    side closes. A serial line has no resize ioctl, so the client's JSON
    resize control frames are dropped rather than forwarded into the guest as
    keystrokes; everything else is raw bytes both ways."""

    async def browser_to_console():
        async for msg in browser_ws:
            if msg.type == web.WSMsgType.TEXT:
                if parse_resize(msg.data) is not None:
                    continue
                await upstream_ws.send_bytes(msg.data.encode("utf-8"))
            elif msg.type == web.WSMsgType.BINARY:
                await upstream_ws.send_bytes(msg.data)
            else:
                break

    async def console_to_browser():
        async for msg in upstream_ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                await browser_ws.send_bytes(msg.data)
            elif msg.type == aiohttp.WSMsgType.TEXT:
                await browser_ws.send_bytes(msg.data.encode("utf-8"))
            else:
                break

    try:
        done, pending = await asyncio.wait(
            [asyncio.ensure_future(browser_to_console()),
             asyncio.ensure_future(console_to_browser())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.warning(f"VM console relay error: {exc}")
    finally:
        await upstream_ws.close()
        if not browser_ws.closed:
            await browser_ws.close()
