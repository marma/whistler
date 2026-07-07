"""The portal aiohttp app: launch + connect to desktop sessions in a browser.

Routes (all under a dev-only auth gate — see ``auth_middleware``):
  GET  /                 list templates + sessions, launch form
  POST /launch           create a DesktopSession, redirect to /connect/<id>
  GET  /connect/<id>     desktop viewer page (websockets viewer — see below)
  GET  /status/<id>      JSON readiness poll
  GET  /term/<id>        web-terminal page
  GET  /term-status/<id> JSON terminal-readiness poll
  GET  /ws-term/<id>     WebSocket <-> in-pod shell (kubectl exec) relay
  GET  /healthz          readiness probe

The only display backend is the **websockets viewer**: the browser connects to
the in-pod Selkies 2.x (pixelflux) server, which serves H.264 over plain
WebSockets straight to the browser's decoder — no guacd, no coturn/TURN. The
reverse-proxy that carries that HTTP/WS stream to the pod is not wired yet, so
``/connect`` currently serves a placeholder (see ``_render_connect``); the web
terminal is fully functional.

State lives in CRs; the portal holds none. Blocking KubeConfigManager calls run
in an executor so the event loop stays responsive.
"""
import asyncio
import html
import logging
import os
import secrets

from aiohttp import web

from whistler.portal import terminal

logger = logging.getLogger("whistler.portal")


# --------------------------------------------------------------------------- #
# Auth — minimal, dev-only this round. Real web SSO/OIDC is a follow-up.       #
# --------------------------------------------------------------------------- #

@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path == "/healthz" or request.path.startswith("/static/"):
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


def _resolve_session(sessions, short_name):
    """Find a desktop session by its short (per-user) name in a list from
    ``get_user_desktop_sessions``."""
    return next((s for s in sessions if s.get("name") == short_name), None)


# --------------------------------------------------------------------------- #
# Static assets (xterm.js for the web terminal), served locally not from a CDN.#
# Whitelisted by name so /static/{filename} can't read arbitrary files.        #
# --------------------------------------------------------------------------- #

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
_STATIC_FILES = {
    "xterm.min.js": "application/javascript",
    "xterm.min.css": "text/css",
    "xterm-addon-fit.min.js": "application/javascript",
    "style.css": "text/css",
}
_static_cache: dict[str, bytes] = {}


async def static_file(request):
    name = request.match_info["filename"]
    content_type = _STATIC_FILES.get(name)
    if content_type is None:
        return web.Response(status=404, text="not found")
    if name not in _static_cache:
        with open(os.path.join(_STATIC_DIR, name), "rb") as f:
            _static_cache[name] = f.read()
    return web.Response(body=_static_cache[name], content_type=content_type, charset="utf-8")


# --------------------------------------------------------------------------- #
# Pages                                                                        #
# --------------------------------------------------------------------------- #

def _render_index(user, templates, sessions):
    tpl_rows = "".join(
        f"<li><form method=post action='/launch'>"
        f"<input type=hidden name=template value='{html.escape(t['fullName'])}'>"
        f"<input type=hidden name=user value='{html.escape(user)}'>"
        f"{html.escape(t['name'])} ({html.escape(t.get('runtime','container'))}/"
        f"{html.escape(t.get('viewer','websockets'))}) "
        f"<input name=name placeholder='session name'>"
        f"<button>Launch</button></form></li>"
        for t in templates
    ) or "<li><em>no templates</em></li>"

    sess_rows = "".join(
        f"<li><a href='/connect/{html.escape(s['name'])}?user={html.escape(user)}'>"
        f"{html.escape(s['name'])}</a> — {html.escape(str(s.get('phase')))} "
        f"({html.escape(str(s.get('runtime')))})</li>"
        for s in sessions
    ) or "<li><em>no sessions</em></li>"

    return (
        "<!doctype html><meta charset=utf-8><title>Whistler</title>"
        f"<h1>Whistler desktops — {html.escape(user)}</h1>"
        f"<h2>Templates</h2><ul>{tpl_rows}</ul>"
        f"<h2>Sessions</h2><ul>{sess_rows}</ul>"
    )


# Desktop viewer page (websockets viewer). The in-pod Selkies 2.x server serves
# its own web client + H.264-over-WebSocket stream; the portal will reverse-proxy
# that HTTP/WS to the pod. That proxy seam is not wired yet, so for now this page
# waits for the session to be Ready and reports the pending work rather than
# erroring. Placeholders are replaced (not .format) so JS braces survive.
_CONNECT_HTML = """<!doctype html><meta charset=utf-8><title>__ID__</title>
<style>
  html,body{margin:0;height:100%;background:#202020;overflow:hidden;
            color:#e0e0e0;font:15px/1.5 system-ui,sans-serif}
  #msg{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
       text-align:center;padding:2rem;box-sizing:border-box}
</style>
<div id=msg>Connecting…</div>
<script>
const id = "__ID__", user = "__USER__";
const msgEl = document.getElementById('msg');
const setStatus = m => { msgEl.textContent = m; };
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function waitReady() {
  setStatus('Waiting for session…');
  for (;;) {
    try {
      const r = await fetch(`/status/${id}?user=${encodeURIComponent(user)}`);
      if (r.ok) {
        const j = await r.json();
        setStatus('Session: ' + (j.phase || 'unknown'));
        if (j.phase === 'Ready') return;
      }
    } catch (e) {}
    await sleep(2000);
  }
}

waitReady().then(() => {
  // Seam for the Selkies 2.x websockets viewer: once the portal reverse-proxies
  // the in-pod Selkies HTTP/WS to a path here, redirect the browser to it.
  setStatus('Session ready. The Selkies 2.x (websockets) viewer is not wired ' +
            'into the portal yet — connect to the pod directly for now.');
}).catch(e => setStatus('Fatal error: ' + e));
</script>"""


def _render_connect(user, session_id):
    return (_CONNECT_HTML
            .replace("__ID__", html.escape(session_id))
            .replace("__USER__", html.escape(user)))


# Web terminal page: xterm.js + fit addon over a plain WebSocket to /ws-term.
# Mirrors the connect page's wait-for-ready pattern. Placeholders replaced (not
# .format) so JS braces survive.
_TERM_HTML = """<!doctype html><meta charset=utf-8><title>__ID__ — terminal</title>
<link rel=stylesheet href="/static/xterm.min.css">
<style>
  html,body{margin:0;height:100%;background:#000;overflow:hidden}
  #term{position:absolute;inset:0;padding:4px;box-sizing:border-box}
  #overlay{position:absolute;inset:0;z-index:20;background:rgba(0,0,0,.85);
           display:flex;align-items:center;justify-content:center;transition:opacity .4s}
  #overlay.hidden{opacity:0;pointer-events:none}
  #overlay-msg{color:#e0e0e0;font:bold 1.5rem/1.5 system-ui,sans-serif;text-align:center;max-width:80%}
</style>
<div id=term></div>
<div id=overlay><div id=overlay-msg>Connecting…</div></div>
<script src="/static/xterm.min.js"></script>
<script src="/static/xterm-addon-fit.min.js"></script>
<script>
const id = "__ID__", user = "__USER__";
const overlayEl = document.getElementById('overlay');
const overlayMsg = document.getElementById('overlay-msg');
const setStatus = m => { overlayMsg.textContent = m; overlayEl.classList.remove('hidden'); };
const hideStatus = () => overlayEl.classList.add('hidden');
const sleep = ms => new Promise(r => setTimeout(r, ms));

const term = new Terminal({ cursorBlink: true, fontFamily: 'monospace', fontSize: 14,
                            theme: { background: '#000000' } });
const fit = new FitAddon.FitAddon();
term.loadAddon(fit);
term.open(document.getElementById('term'));
fit.fit();

async function waitReady() {
  setStatus('Waiting for session…');
  for (;;) {
    try {
      const r = await fetch(`/term-status/${id}?user=${encodeURIComponent(user)}`);
      if (r.ok) {
        const j = await r.json();
        if (j.unsupported) { setStatus('Terminal not available for this session'); return false; }
        setStatus(j.ready ? 'Opening shell…' : 'Session: ' + j.phase);
        if (j.ready) return true;
      }
    } catch (e) {}
    await sleep(2000);
  }
}

function connect() {
  const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
  const ws = new WebSocket(proto + location.host + '/ws-term/' + id +
                           '?user=' + encodeURIComponent(user));
  ws.binaryType = 'arraybuffer';
  const sendResize = () => { if (ws.readyState === 1) ws.send(JSON.stringify({ resize: [term.cols, term.rows] })); };
  ws.onopen = () => { hideStatus(); fit.fit(); sendResize(); term.focus(); };
  ws.onmessage = e => term.write(typeof e.data === 'string' ? e.data : new Uint8Array(e.data));
  ws.onclose = e => setStatus(e.reason ? 'Session closed: ' + e.reason : 'Session closed');
  ws.onerror = () => setStatus('Connection error');
  term.onData(d => { if (ws.readyState === 1) ws.send(d); });
  let rt; window.addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(() => { fit.fit(); sendResize(); }, 150); });
}

waitReady().then(ok => { if (ok) connect(); });
</script>"""


def _render_term(user, session_id):
    return (_TERM_HTML
            .replace("__ID__", html.escape(session_id))
            .replace("__USER__", html.escape(user)))


def _resolve_target(instances, desktop_sessions, name):
    """Locate a Session by its short name across ssh instances and desktop
    sessions, returning a uniform dict for the terminal: pod/namespace to exec
    into, readiness, and whether a terminal is even possible (VM-runtime desktop
    sessions have no pod to exec into)."""
    for i in instances:
        if i["name"] == name:
            return {
                "podName": i.get("podName"), "namespace": i.get("namespace"),
                "phase": i.get("status"), "runtime": "container",
                "ready": i.get("status") == "Running" and bool(i.get("podName")),
                "supported": True,
            }
    for s in desktop_sessions:
        if s["name"] == name:
            runtime = s.get("runtime")
            return {
                "podName": s.get("podName"), "namespace": s.get("namespace"),
                "phase": s.get("phase"), "runtime": runtime,
                "ready": s.get("phase") == "Ready" and bool(s.get("podName")),
                "supported": runtime != "vm",
            }
    return None


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
    """Serve the desktop viewer page and nudge the session's pod awake (same
    reconcile trigger the SSH server / web terminal fire on connect) so it
    becomes Ready while the page polls /status."""
    cm, user = request.app["cm"], request["user"]
    name = request.match_info["id"]
    await _run(request, cm.trigger_instance_start, user, name)
    return web.Response(text=_render_connect(user, name), content_type="text/html")


async def status(request):
    cm, user = request.app["cm"], request["user"]
    sessions = await _run(request, cm.get_user_desktop_sessions, user)
    sess = _resolve_session(sessions, request.match_info["id"])
    if not sess:
        return web.json_response({"error": "not found"}, status=404)
    return web.json_response({
        "phase": sess.get("phase"),
        "address": sess.get("address"),
        "displayPort": sess.get("displayPort"),
    })


async def term(request):
    """Serve the web-terminal page and nudge the session's pod awake (same
    reconcile trigger the SSH server fires on connect) so it's Ready by the time
    the page finishes polling."""
    cm, user = request.app["cm"], request["user"]
    name = request.match_info["id"]
    await _run(request, cm.trigger_instance_start, user, name)
    return web.Response(text=_render_term(user, name), content_type="text/html")


async def term_status(request):
    cm, user = request.app["cm"], request["user"]
    instances, desktop_sessions = await asyncio.gather(
        _run(request, cm.get_user_instances, user),
        _run(request, cm.get_user_desktop_sessions, user),
    )
    target = _resolve_target(instances, desktop_sessions, request.match_info["id"])
    if not target:
        return web.json_response({"error": "not found"}, status=404)
    if not target["supported"]:
        return web.json_response({"unsupported": True, "phase": target["phase"]})
    return web.json_response({"phase": target["phase"], "ready": target["ready"]})


async def ws_term(request):
    cm, user = request.app["cm"], request["user"]
    name = request.match_info["id"]

    instances, desktop_sessions = await asyncio.gather(
        _run(request, cm.get_user_instances, user),
        _run(request, cm.get_user_desktop_sessions, user),
    )
    target = _resolve_target(instances, desktop_sessions, name)
    if not target:
        return web.Response(status=404, text="unknown session")
    if not target["supported"]:
        return web.Response(status=400, text="terminal not available for this session")
    if not target["ready"] or not target["podName"]:
        return web.Response(status=409, text=f"session not ready (phase={target['phase']})")

    wsr = web.WebSocketResponse()
    await wsr.prepare(request)
    logger.info(f"Opening terminal for {user}/{name} -> {target['podName']}")
    try:
        await terminal.relay_terminal(wsr, target["podName"], target["namespace"])
    except Exception as e:
        logger.error(f"Terminal for {user}/{name} failed: {e}")
        if not wsr.closed:
            await wsr.close(message=str(e).encode())
    return wsr


async def healthz(request):
    return web.Response(text="ok")


def build_app(config_manager):
    app = web.Application(middlewares=[auth_middleware])
    app["cm"] = config_manager
    app.add_routes([
        web.get("/", index),
        web.post("/launch", launch),
        web.get("/connect/{id}", connect),
        web.get("/status/{id}", status),
        web.get("/term/{id}", term),
        web.get("/term-status/{id}", term_status),
        web.get("/ws-term/{id}", ws_term),
        web.get("/healthz", healthz),
        web.get("/static/{filename}", static_file),
    ])
    return app
