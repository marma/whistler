"""The portal aiohttp app: launch + connect to desktop sessions in a browser.

Routes (all under a dev-only auth gate — see ``auth_middleware``):
  GET  /                 list templates + sessions, launch form
  POST /launch           create a DesktopSession, redirect to /connect/<id>
  GET  /connect/<id>     page that waits for Ready then opens the WS
  GET  /status/<id>      JSON readiness poll
  GET  /ws/<id>          WebSocket <-> guacd relay
  GET  /healthz          readiness probe

State lives in CRs; the portal holds none. Blocking KubeConfigManager calls run
in an executor so the event loop stays responsive.
"""
import asyncio
import codecs
import html
import logging
import os
import secrets

from aiohttp import web

from whistler.portal import guacd
from whistler.portal.guacd import handshake, resolve_session, _build_guacd_params

logger = logging.getLogger("whistler.portal")


# --------------------------------------------------------------------------- #
# Auth — minimal, dev-only this round. Real web SSO/OIDC is a follow-up.       #
# --------------------------------------------------------------------------- #

@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path == "/healthz":
        return await handler(request)
    if os.environ.get("WHISTLER_AUTH_ALLOW_ANY") != "true":
        return web.Response(
            status=401,
            text="Portal web auth (SSO/OIDC) is not implemented yet. "
                 "For dev, set WHISTLER_AUTH_ALLOW_ANY=true and pass ?user=<name>.",
        )
    # Dev: identity from header or query; first segment is the real user (mirrors
    # the SSH server's username convention).
    raw = request.headers.get("X-Whistler-User") or request.query.get("user") or "tester"
    request["user"] = raw.split("-")[0]
    return await handler(request)


async def _run(request, func, *args):
    """Run a blocking KubeConfigManager method off the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)


# --------------------------------------------------------------------------- #
# Pages (inline; guacamole-common-js loaded from a CDN)                         #
# --------------------------------------------------------------------------- #

GUAC_JS_URL = os.environ.get(
    "PORTAL_GUAC_JS_URL",
    "https://cdn.jsdelivr.net/npm/guacamole-common-js@1.5.0/dist/guacamole-common.min.js",
)


def _render_index(user, templates, sessions):
    tpl_rows = "".join(
        f"<li><form method=post action='/launch'>"
        f"<input type=hidden name=template value='{html.escape(t['fullName'])}'>"
        f"<input type=hidden name=user value='{html.escape(user)}'>"
        f"{html.escape(t['name'])} ({html.escape(t.get('backend','pod'))}/"
        f"{html.escape(t.get('protocol','rdp'))}) "
        f"<input name=name placeholder='session name'>"
        f"<button>Launch</button></form></li>"
        for t in templates
    ) or "<li><em>no templates</em></li>"

    sess_rows = "".join(
        f"<li><a href='/connect/{html.escape(s['name'])}?user={html.escape(user)}'>"
        f"{html.escape(s['name'])}</a> — {html.escape(str(s.get('phase')))} "
        f"({html.escape(str(s.get('backend')))})</li>"
        for s in sessions
    ) or "<li><em>no sessions</em></li>"

    return (
        "<!doctype html><meta charset=utf-8><title>Whistler</title>"
        f"<h1>Whistler desktops — {html.escape(user)}</h1>"
        f"<h2>Templates</h2><ul>{tpl_rows}</ul>"
        f"<h2>Sessions</h2><ul>{sess_rows}</ul>"
    )


# Placeholders are replaced (not .format) so JS braces stay intact.
_CONNECT_HTML = """<!doctype html><meta charset=utf-8><title>__ID__</title>
<style>html,body{margin:0;height:100%;background:#000}#status{color:#ccc;font:14px sans-serif;padding:8px}</style>
<div id=status>Waiting for session…</div><div id=display></div>
<script src="__GUAC_JS__"></script>
<script>
const id = "__ID__", user = "__USER__";
const statusEl = document.getElementById('status');
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function waitReady() {
  for (;;) {
    try {
      const r = await fetch(`/status/${id}?user=${encodeURIComponent(user)}`);
      if (r.ok) { const j = await r.json(); statusEl.textContent = 'Status: ' + j.phase;
        if (j.phase === 'Ready') return; }
    } catch (e) {}
    await sleep(2000);
  }
}
function connect() {
  const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
  const url = proto + location.host + '/ws/' + id + '?user=' + encodeURIComponent(user);
  const tunnel = new Guacamole.WebSocketTunnel(url);
  const client = new Guacamole.Client(tunnel);
  const display = client.getDisplay();
  document.getElementById('display').appendChild(display.getElement());
  tunnel.onerror = client.onerror = e => { statusEl.textContent = 'Error: ' + (e && e.message || e); };
  client.onstatechange = s => { if (s === 3) statusEl.style.display = 'none'; };
  client.connect('');
  const el = display.getElement();
  const mouse = new Guacamole.Mouse(el);
  mouse.onmousedown = mouse.onmouseup = mouse.onmousemove = st => client.sendMouseState(st);
  const kb = new Guacamole.Keyboard(document);
  kb.onkeydown = k => client.sendKeyEvent(1, k);
  kb.onkeyup = k => client.sendKeyEvent(0, k);
}
waitReady().then(connect);
</script>"""


def _render_connect(user, session_id):
    return (_CONNECT_HTML
            .replace("__ID__", html.escape(session_id))
            .replace("__USER__", html.escape(user))
            .replace("__GUAC_JS__", html.escape(GUAC_JS_URL)))


# --------------------------------------------------------------------------- #
# Handlers                                                                      #
# --------------------------------------------------------------------------- #

async def index(request):
    cm, user = request.app["cm"], request["user"]
    templates = await _run(request, cm.get_user_desktop_templates, user)
    sessions = await _run(request, cm.get_user_desktop_sessions, user)
    return web.Response(text=_render_index(user, templates, sessions), content_type="text/html")


async def launch(request):
    cm, user = request.app["cm"], request["user"]
    form = await request.post()
    template = form.get("template")
    name = (form.get("name") or "").strip() or f"d{secrets.token_hex(3)}"
    if not template:
        return web.Response(status=400, text="missing template")
    ok = await _run(request, cm.add_desktop_session, user, template, name)
    if not ok:
        return web.Response(status=500, text="failed to create session")
    raise web.HTTPFound(f"/connect/{name}?user={user}")


async def connect(request):
    return web.Response(text=_render_connect(request["user"], request.match_info["id"]),
                        content_type="text/html")


async def status(request):
    cm, user = request.app["cm"], request["user"]
    sessions = await _run(request, cm.get_user_desktop_sessions, user)
    sess = resolve_session(sessions, request.match_info["id"])
    if not sess:
        return web.json_response({"error": "not found"}, status=404)
    return web.json_response({
        "phase": sess.get("phase"),
        "address": sess.get("address"),
        "displayPort": sess.get("displayPort"),
    })


async def ws(request):
    cm, user = request.app["cm"], request["user"]
    session_id = request.match_info["id"]

    sessions = await _run(request, cm.get_user_desktop_sessions, user)
    sess = resolve_session(sessions, session_id)
    if not sess:
        return web.Response(status=404, text="unknown session")
    if sess.get("phase") != "Ready":
        return web.Response(status=409, text=f"session not ready (phase={sess.get('phase')})")

    templates = await _run(request, cm.get_user_desktop_templates, user)
    template = next((t for t in templates
                     if t.get("fullName") == sess.get("template")
                     or t.get("name") == sess.get("template")), {})
    protocol = template.get("protocol") or "rdp"
    params = _build_guacd_params(template, sess["address"], sess["displayPort"])

    wsr = web.WebSocketResponse()
    await wsr.prepare(request)
    logger.info(f"Opening relay for {user}/{session_id} -> guacd ({protocol} {sess['address']})")

    try:
        reader, writer = await asyncio.open_connection(guacd.GUACD_HOST, guacd.GUACD_PORT)
    except OSError as e:
        await wsr.close(message=f"guacd unreachable: {e}".encode())
        return wsr

    try:
        _conn_id, leftover = await handshake(reader, writer, protocol=protocol, params=params)
        if leftover:
            await wsr.send_str(leftover.decode("utf-8"))
        await _relay(wsr, reader, writer)
    except Exception as e:
        logger.error(f"Relay for {user}/{session_id} failed: {e}")
        if not wsr.closed:
            await wsr.close(message=str(e).encode())
    finally:
        writer.close()
    return wsr


async def healthz(request):
    return web.Response(text="ok")


async def _relay(wsr, reader, writer):
    """Bidirectional byte pump between the browser WS and the guacd socket.
    Parses nothing — guacd<->browser is opaque after the handshake."""
    async def guacd_to_ws():
        dec = codecs.getincrementaldecoder("utf-8")()
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                text = dec.decode(data)
                if text:
                    await wsr.send_str(text)
        finally:
            if not wsr.closed:
                await wsr.close()

    async def ws_to_guacd():
        try:
            async for msg in wsr:
                if msg.type == web.WSMsgType.TEXT:
                    writer.write(msg.data.encode("utf-8"))
                    await writer.drain()
                elif msg.type == web.WSMsgType.BINARY:
                    writer.write(msg.data)
                    await writer.drain()
                else:
                    break
        finally:
            try:
                writer.close()
            except Exception:
                pass

    await asyncio.gather(guacd_to_ws(), ws_to_guacd(), return_exceptions=True)


def build_app(config_manager):
    app = web.Application(middlewares=[auth_middleware])
    app["cm"] = config_manager
    app.add_routes([
        web.get("/", index),
        web.post("/launch", launch),
        web.get("/connect/{id}", connect),
        web.get("/status/{id}", status),
        web.get("/ws/{id}", ws),
        web.get("/healthz", healthz),
    ])
    return app
