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

from whistler.portal import guacd, terminal
from whistler.portal.guacd import handshake, resolve_session, _build_guacd_params
from whistler.portal.protocol import take_complete_instructions

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


# --------------------------------------------------------------------------- #
# Pages (inline; guacamole-common-js served locally from the portal)            #
# --------------------------------------------------------------------------- #

# guacamole-common-js is vendored (whistler/portal/static/) and served by the
# portal rather than pulled from a CDN. We pin 1.6.0: the npm-published builds
# stop at 1.5.0, whose renderer mishandles guacd's save-under copy ops (the
# selection-rectangle / drag artifacts). 1.6.0 must be advertised to guacd too —
# see guacd._CLIENT_PROTOCOL_VERSION. Overridable to a URL for experiments.
_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
_GUAC_JS_FILENAME = "guacamole-common.min.js"
GUAC_JS_URL = os.environ.get("PORTAL_GUAC_JS_URL", "/static/" + _GUAC_JS_FILENAME)

_guac_js_cache = None


async def guac_js(request):
    global _guac_js_cache
    if _guac_js_cache is None:
        with open(os.path.join(_STATIC_DIR, _GUAC_JS_FILENAME), "rb") as f:
            _guac_js_cache = f.read()
    return web.Response(body=_guac_js_cache, content_type="application/javascript",
                        charset="utf-8")


# Vendored web-terminal assets (xterm.js + fit addon), served locally like the
# guacamole bundle above rather than from a CDN. Whitelisted by name so the
# dynamic /static/{filename} route can't be used to read arbitrary files.
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


def _render_index(user, templates, sessions):
    tpl_rows = "".join(
        f"<li><form method=post action='/launch'>"
        f"<input type=hidden name=template value='{html.escape(t['fullName'])}'>"
        f"<input type=hidden name=user value='{html.escape(user)}'>"
        f"{html.escape(t['name'])} ({html.escape(t.get('runtime','container'))}/"
        f"{html.escape(t.get('protocol','rdp'))}) "
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


# Placeholders are replaced (not .format) so JS braces stay intact.
_CONNECT_HTML = """<!doctype html><meta charset=utf-8><title>__ID__</title>
<style>
  html,body{margin:0;height:100%;background:#202020;overflow:hidden}
  /* Guacamole gives its layer canvases z-index:-1, which paints them BEHIND the
     page background — so the desktop renders but stays hidden under body's
     background. Make #display its own stacking context (position + z-index, plus
     isolation for good measure) so that -1 resolves inside #display, on top of
     the body background, instead of behind the whole page. */
  #display{position:absolute;inset:0;z-index:0;isolation:isolate}
  /* No interpolation. On a hidpi screen the browser upscales the canvas from CSS
     px to device px; the default bilinear filter makes that blurry (reads as
     "compression"). pixelated/crisp-edges force nearest-neighbour -> jaggies
     instead of blur. With 1:1 rendering (below) there's no resampling at all at
     100%; this matters for the 2x mode and fractional DPRs. */
  #display canvas{image-rendering:pixelated;image-rendering:crisp-edges}
  #log{position:absolute;top:0;left:0;z-index:10;color:#0f0;font:12px/1.4 monospace;
       background:rgba(0,0,0,.7);padding:6px;max-width:95%;white-space:pre-wrap;pointer-events:none}
  #q{position:absolute;bottom:6px;right:6px;z-index:21;font:12px monospace;cursor:pointer;
     background:rgba(0,0,0,.6);color:#0f0;border:1px solid #0f0;border-radius:3px;padding:2px 6px}
  #overlay{position:absolute;inset:0;z-index:20;background:rgba(0,0,0,.65);
           display:flex;align-items:center;justify-content:center;
           transition:opacity .4s}
  #overlay.hidden{opacity:0;pointer-events:none}
  #overlay-msg{color:#e0e0e0;font:bold 2rem/1.5 system-ui,sans-serif;text-align:center;
               text-shadow:0 2px 8px rgba(0,0,0,.9);max-width:80%}
</style>
<div id=display></div>
<div id=overlay><div id=overlay-msg>Connecting…</div></div>
<pre id=log></pre>
<button id=q title="Toggle remote render resolution (1:1 physical vs half)">100%</button>
<!-- guacamole-common-js ships a CommonJS bundle: it sets a global `var Guacamole`
     but ends with an unguarded `module.exports`. Define a dummy `module` so that
     last line is a harmless no-op instead of a ReferenceError. -->
<script>var module = {};</script>
<!-- guacamole-common-js prefers createImageBitmap when present, but Chrome's
     createImageBitmap intermittently throws "source image could not be decoded"
     on valid Blob input under load, and the rejected promise permanently stalls
     guacamole's draw queue (updates freeze after the first such error). Disabling
     it *before* the library loads forces the robust <img>-based decode path:
     slower per image, but failures hit img.onerror instead of freezing the
     session. Must run before the library script below. -->
<script>try { window.createImageBitmap = undefined; } catch (e) {}</script>
<script src="__GUAC_JS__"></script>
<script>
const id = "__ID__", user = "__USER__";
// The on-screen log is opt-in via ?debug; otherwise everything still goes to the
// JS console, so the desktop fills the page cleanly during normal use.
const DEBUG = new URLSearchParams(location.search).has('debug');
const logEl = document.getElementById('log');
if (!DEBUG) logEl.style.display = 'none';
const NL = String.fromCharCode(10);
function log(m) { if (DEBUG) logEl.textContent += m + NL; try { console.log("[whistler]", m); } catch (e) {} }
const STATES = ["IDLE","CONNECTING","WAITING","CONNECTED","DISCONNECTING","DISCONNECTED"];
if (!window.Guacamole) log("FATAL: Guacamole library not loaded");
const sleep = ms => new Promise(r => setTimeout(r, ms));

const overlayEl = document.getElementById('overlay');
const overlayMsg = document.getElementById('overlay-msg');
function setStatus(msg) { overlayMsg.textContent = msg; overlayEl.classList.remove('hidden'); }
function hideStatus() { overlayEl.classList.add('hidden'); }

async function waitReady() {
  setStatus('Waiting for session…');
  for (;;) {
    try {
      const r = await fetch(`/status/${id}?user=${encodeURIComponent(user)}`);
      if (r.ok) {
        const j = await r.json(); log("status: " + j.phase);
        setStatus(j.phase === 'Ready' ? 'Opening display…' : 'Session: ' + j.phase);
        if (j.phase === 'Ready') return;
      } else { log("status http " + r.status); }
    } catch (e) { log("status error: " + e); }
    await sleep(2000);
  }
}
// Size the remote in PHYSICAL device pixels, not CSS pixels: on a hidpi/scaled
// display (e.g. 4K @ 150%) window.innerWidth is in CSS px, but the screen has
// innerWidth*devicePixelRatio real pixels — rendering at CSS px then leaves the
// browser to upscale, which is soft. We render at innerWidth*dpr and scale the
// canvas back down by 1/dpr to fill the viewport, so one remote pixel == one
// device pixel (crisp, no resampling).
function remoteSize() {
  const dpr = window.devicePixelRatio || 1;
  return {
    w: Math.max(640, Math.min(8192, Math.round(window.innerWidth  * dpr))),
    h: Math.max(480, Math.min(8192, Math.round(window.innerHeight * dpr))),
    scale: 1 / dpr,
  };
}
function connect() {
  log("connecting…");
  const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
  // No query string here: WebSocketTunnel appends `?`+connect-data itself, so a
  // pre-existing query would produce a doubled `?` and corrupt the params.
  const url = proto + location.host + '/ws/' + id;
  const tunnel = new Guacamole.WebSocketTunnel(url);
  const client = new Guacamole.Client(tunnel);
  const display = client.getDisplay();
  document.getElementById('display').appendChild(display.getElement());

  let connected = false;
  client.onstatechange = s => {
    connected = (s === 3); log("state: " + (STATES[s] || s));
    if (s === 3) hideStatus();
    else if (s === 5) setStatus('Disconnected');
  };
  client.onerror = e => { log("client error: " + (e && e.message || e)); setStatus('Error: ' + (e && e.message || String(e))); };
  tunnel.onerror = e => { log("tunnel error: " + (e && e.message || e)); setStatus('Connection error'); };

  // Initial size travels in the connect data -> WS query -> guacd handshake, so
  // the RDP session starts at the right resolution (no fixed 1024x768 then resize).
  const r0 = remoteSize();
  let remoteW = r0.w, remoteH = r0.h;   // actual remote size, updated by onresize
  let halfMode = (window.devicePixelRatio || 1) > 1;

  // Compute the target remote resolution and the canvas scale needed for it to
  // fill the viewport. halfMode requests half the device pixels (less to encode,
  // visibly blurry but same viewport coverage).
  function targetSize() {
    const dpr = window.devicePixelRatio || 1;
    const div = halfMode ? 2 : 1;
    return {
      w: Math.max(640, Math.min(8192, Math.round(window.innerWidth  * dpr / div))),
      h: Math.max(480, Math.min(8192, Math.round(window.innerHeight * dpr / div))),
      scale: div / dpr,
    };
  }

  function applySize() {
    const t = targetSize();
    try {
      // Optimistic: apply the scale for the requested target dimensions immediately.
      // Correct for xrdp (honors sendSize). For grd (ignores sendSize), the
      // fallback below re-fits using the actual fixed remote dimensions.
      display.scale(t.scale);
      if (connected) client.sendSize(t.w, t.h);
      log('target ' + t.w + 'x' + t.h + ' @' + t.scale.toFixed(3));
      clearTimeout(applySize._fix);
      applySize._fix = setTimeout(() => {
        try { display.scale(Math.min(window.innerWidth / remoteW, window.innerHeight / remoteH)); } catch (e) {}
      }, 400);
    } catch (e) { log('size error: ' + e); }
  }

  // When the remote actually resizes (xrdp), cancel the fallback and lock in the
  // exact fit based on the real dimensions.
  display.onresize = (width, height) => {
    clearTimeout(applySize._fix);
    remoteW = width; remoteH = height;
    try { display.scale(Math.min(window.innerWidth / width, window.innerHeight / height)); } catch (e) {}
  };

  const t0 = targetSize();
  display.scale(t0.scale);
  client.connect('user=' + encodeURIComponent(user) + '&w=' + t0.w + '&h=' + t0.h);

  let rt;
  window.addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(applySize, 300); });

  // 50%/100%: both fill the viewport, but 50% sends half the remote pixels
  // (lower quality, less bandwidth). Does NOT change the canvas size.
  const qBtn = document.getElementById('q');
  qBtn.textContent = halfMode ? '50%' : '100%';
  qBtn.onclick = () => {
    halfMode = !halfMode;
    qBtn.textContent = halfMode ? '50%' : '100%';
    applySize();
  };

  try {
    const el = display.getElement();
    const mouse = new Guacamole.Mouse(el);
    // sendMouseState(state, true): the `true` makes guac divide the coords by the
    // display scale itself (and move the cursor to match). Do NOT pre-scale or
    // mutate the state — guac reuses one state object across events and fires
    // mousedown WITHOUT recomputing position, so mutating it double-scales the
    // click anchor (correct on drag-move, 2x off on the initial press).
    mouse.onmousedown = mouse.onmouseup = mouse.onmousemove = st => client.sendMouseState(st, true);
    const kb = new Guacamole.Keyboard(document);
    kb.onkeydown = k => client.sendKeyEvent(1, k);
    kb.onkeyup = k => client.sendKeyEvent(0, k);
  } catch (e) { log("input setup error: " + e); }
}
waitReady().then(connect).catch(e => { log("fatal: " + e); setStatus('Fatal error: ' + e); });
</script>"""


def _render_connect(user, session_id):
    return (_CONNECT_HTML
            .replace("__ID__", html.escape(session_id))
            .replace("__USER__", html.escape(user))
            .replace("__GUAC_JS__", html.escape(GUAC_JS_URL)))


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

    # Initial display size from the browser viewport (?w/?h on the connect data),
    # clamped; falls back to a sane default. Resizes after connect go via the
    # client's `size` instruction, not here.
    def _dim(key, default):
        try:
            return max(320, min(8192, int(request.query.get(key, ""))))
        except (TypeError, ValueError):
            return default
    width, height = _dim("w", 1024), _dim("h", 768)

    # guacamole-common-js connects with the "guacamole" subprotocol.
    wsr = web.WebSocketResponse(protocols=("guacamole",))
    await wsr.prepare(request)
    logger.info(f"Opening relay for {user}/{session_id} -> guacd ({protocol} {sess['address']})")

    try:
        reader, writer = await asyncio.open_connection(guacd.GUACD_HOST, guacd.GUACD_PORT)
    except OSError as e:
        await wsr.close(message=f"guacd unreachable: {e}".encode())
        return wsr

    try:
        _conn_id, leftover = await handshake(reader, writer, protocol=protocol, params=params,
                                             width=width, height=height)
        # `leftover` may end mid-multibyte-char, so it must be fed through the
        # relay's incremental decoder (not decoded standalone) or the partial
        # tail corrupts/raises.
        await _relay(wsr, reader, writer, initial=leftover)
    except Exception as e:
        logger.error(f"Relay for {user}/{session_id} failed: {e}")
        if not wsr.closed:
            await wsr.close(message=str(e).encode())
    finally:
        writer.close()
    return wsr


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


async def _relay(wsr, reader, writer, initial=b""):
    """Bidirectional byte pump between the browser WS and the guacd socket.
    Parses nothing — guacd<->browser is opaque after the handshake. ``initial``
    is the bytes that arrived glued to ``ready`` during the handshake; it is fed
    through the same incremental decoder so a multibyte char split at that
    boundary is never lost or mis-decoded."""
    async def guacd_to_ws():
        # guacamole-common-js parses each WS message independently and does NOT
        # buffer a partial instruction across messages (it raises "Incomplete
        # instruction" / corrupts its parse, hanging the page). So we must never
        # split an instruction across messages: decode bytes, forward only whole
        # instructions, and hold the partial tail for the next read.
        utf8 = codecs.getincrementaldecoder("utf-8")()
        buf = ""

        async def forward(data: bytes):
            nonlocal buf
            buf += utf8.decode(data)
            complete, buf = take_complete_instructions(buf)
            if complete:
                await wsr.send_str(complete)

        try:
            if initial:
                await forward(initial)
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                await forward(data)
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
        web.get("/term/{id}", term),
        web.get("/term-status/{id}", term_status),
        web.get("/ws-term/{id}", ws_term),
        web.get("/healthz", healthz),
        web.get("/static/" + _GUAC_JS_FILENAME, guac_js),
        web.get("/static/{filename}", static_file),
    ])
    return app
