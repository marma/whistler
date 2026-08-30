"""The portal aiohttp app: launch + connect to desktop sessions in a browser.

Routes (all under a dev-only auth gate — see ``auth_middleware``):
  GET  /                 list templates + sessions, launch form
  POST /launch           create a DesktopSession, redirect to /connect/<id>
  GET  /connect/<id>     desktop viewer page: waits for Ready, then redirects
                         to /desktop/<id>/ or /vnc/<id> by viewer
  ANY  /desktop/<id>/*   reverse proxy (HTTP + WebSocket) to the in-pod
                         Selkies 2.x server — see whistler.portal.proxy
  GET  /vnc/<id>         noVNC *desktop* page (viewer:vnc VM sessions)
  GET  /ws-vnc/<id>      WebSocket <-> KubeVirt VNC subresource relay
  GET  /console/<id>     the machine console (ADMIN): the same emulated
                         display, but attached from power-on rather than
                         waiting for Ready — firmware, bootloader, kernel
  GET  /ws-console/<id>  WebSocket <-> the same subresource, admin-gated
  GET  /status/<id>      JSON readiness poll
  GET  /term/<id>        web-terminal page
  GET  /term-status/<id> JSON terminal-readiness poll
  GET  /ws-term/<id>     WebSocket <-> in-pod shell (kubectl exec) or VMI
                         serial console relay
  GET  /screenshot/<id>  latest desktop thumbnail (PNG) — see
                         whistler.portal.screenshots
  GET  /kiosk*           the kiosk surface (login + session grid + full-screen
                         desktop, and nothing else) — see whistler.portal.kiosk
  GET  /kiosk/lock       lock this browser; while locked, kiosk.lock_middleware
                         refuses every route above it but the lock screen
  GET  /healthz          readiness probe

Two display backends, chosen per session by ``status.viewer``:

* **websockets** — the Selkies 2.x (pixelflux) server (streamer sidecar for
  pods, or a guest-run Selkies in a VM) serves its own web client and streams
  H.264 over plain WebSockets — no guacd, no coturn/TURN. The portal
  reverse-proxies both (the client is fully page-relative, so no rewriting)
  under ``/desktop/<id>/``.
* **vnc** — VM sessions only: the portal serves its vendored noVNC client and
  bridges it to the VMI's VNC subresource on the API server (raw RFB; guest
  needs no agent, works from BIOS onward) — see whistler.portal.kubevirt.

Either way the browser only ever talks to the portal origin: one TLS endpoint
satisfies the client's secure-context requirement for every session.

State lives in CRs; the portal holds none. Blocking KubeConfigManager calls run
in an executor so the event loop stays responsive.
"""
import asyncio
import html
import logging
import os
import secrets
import time
from functools import partial

import aiohttp
from aiohttp import web

from whistler.config import CHANNEL_TERMINAL, ENTRY_KIOSK, ENTRY_PORTAL
from whistler.portal import kiosk, kubevirt, proxy, screenshots, terminal
from whistler.portal.login import USER_COOKIE as _USER_COOKIE
from whistler.portal.login import render_notice

logger = logging.getLogger("whistler.portal")


# --------------------------------------------------------------------------- #
# Auth — minimal, dev-only this round. Real web SSO/OIDC is a follow-up.       #
#                                                                              #
# This app deliberately keeps a fallback identity where the kiosk and the      #
# management portal now have a login screen (whistler/portal/login.py): its    #
# pages are linked into from those, and the requests a browser then makes on   #
# its own — Selkies assets, the display WebSocket, a thumbnail <img> — carry   #
# no ?user=, so the identity that loaded the page is echoed back by cookie.    #
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
    # the SSH server's username convention). The cookie fallback exists for the
    # /desktop/<id>/* proxy: the Selkies client's own asset/WS requests carry no
    # ?user=, so the identity that loaded the page is echoed back via cookie.
    explicit = request.headers.get("X-Whistler-User") or request.query.get("user")
    raw = explicit or request.cookies.get(_USER_COOKIE) or "tester"
    request["user"] = raw.split("-")[0]
    response = await handler(request)
    if explicit and isinstance(response, web.StreamResponse) and not response.prepared:
        response.set_cookie(_USER_COOKIE, raw, path="/", samesite="Lax")
    return response


async def _run(request, func, *args):
    """Run a blocking KubeConfigManager method off the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)


# --------------------------------------------------------------------------- #
# Entry points — the identity half of the kiosk situation (design/security.md, #
# "Closing the fourth axis"), enforced here for this app's share of the doors. #
# --------------------------------------------------------------------------- #

# This app serves *both* surfaces, so which entry point a request belongs to is
# a path question. Three sets, and the interesting one is the middle:
#
# - Open: the lock screen is made of /static, and a binding must not take a pod
#   out of service, so /healthz stays reachable.
# - Shared: the desktop itself. Both the kiosk's session page and the portal's
#   connect page end up here, so these carry no entry point of their own —
#   which is also what keeps a Selkies asset or WebSocket frame off the
#   per-request User CR read below.
# - Everything else is the portal's: the launch form on /, the web terminal,
#   the machine console. Written as "everything else" on purpose: a route added
#   later is checked by default rather than exempt by omission.
_OPEN_PREFIXES = ("/static/",)
_SHARED_PREFIXES = ("/connect", "/desktop", "/vnc", "/ws-vnc", "/status",
                    "/screenshot")

# The kiosk's own way in and out, always reachable — the same reasoning as the
# lock's _LOCK_ALLOWED. A browser holding a kiosk cookie for an account the
# surface now refuses has to be able to sign out, or nobody else can sign in on
# that machine; and refusing the login *POST* is the form's job (kiosk_login),
# which can say why instead of showing a wall.
_KIOSK_OPEN = ("/kiosk/login", "/kiosk/logout")


def _required_entry_point(path: str):
    """Which entry point ``path`` needs, or None when it needs none."""
    if path == "/healthz" or path.startswith(_OPEN_PREFIXES):
        return None
    if path in _KIOSK_OPEN or path.startswith(_SHARED_PREFIXES):
        return None
    if path == "/kiosk" or path.startswith("/kiosk/"):
        return ENTRY_KIOSK
    return ENTRY_PORTAL


@web.middleware
async def entry_point_middleware(request: web.Request, handler):
    """Refuse a surface this user is not granted.

    The check costs one User CR read, so it deliberately runs only on requests
    that *have* an entry point — never on the desktop paths above, which is
    where the request volume is (every Selkies asset, every WebSocket frame's
    handshake, every thumbnail).

    A refusal is a page, not a redirect: behind the bundled proxy the other
    surface is one origin away, but in a split-port dev run it is not, and a
    303 would land on a 404 instead of an explanation. Non-navigations get a
    bare 403 for the same reason the lock returns 423 to them."""
    needed = _required_entry_point(request.path)
    # On the kiosk paths, "who is this" is the *kiosk's* answer, not the auth
    # middleware's: that one falls back to a cookie (and then to a default
    # name), so a browser that once visited the portal would be turned away
    # from the kiosk's own login screen — on a shared machine, with no way to
    # become somebody else. Nobody signed in is nobody to refuse; the form
    # refuses the wrong account when it is offered one.
    user = (kiosk.kiosk_identity(request) if needed == ENTRY_KIOSK
            else request.get("user"))
    if needed is None or not user:
        return await handler(request)
    if await _run(request, request.app["cm"].may_enter, user, needed):
        return await handler(request)
    logger.warning(f"Refusing {user} at {request.path}: no '{needed}' entry point")
    if (request.headers.get("Sec-Fetch-Mode") == "navigate"
            or "text/html" in request.headers.get("Accept", "")):
        if needed == ENTRY_PORTAL:
            heading, href, label = ("Kiosk only", "/kiosk", "Go to the kiosk")
            message = ("This account may only be used through the kiosk. "
                       "The portal, the web terminal and SSH are not available "
                       "to it.")
        else:
            heading, href, label = ("Not a kiosk account", "/", "Go to the portal")
            message = ("This account is not granted the kiosk surface. "
                       "Use the portal instead.")
        return web.Response(text=render_notice(heading=heading, message=message,
                                               href=href, link_label=label),
                            status=403, content_type="text/html",
                            headers={"Cache-Control": "no-store"})
    return web.Response(status=403, text=f"no '{needed}' entry point")


# Admin overrides, read the same way the management app reads them so the two
# halves of the portal cannot disagree about who is an admin.
_ADMIN_USERS = set(
    u.strip() for u in os.environ.get("WHISTLER_ADMIN_USERS", "").split(",") if u.strip())
_ALLOW_ADMIN = os.environ.get("WHISTLER_AUTH_ALLOW_ADMIN", "false").lower() == "true"


async def _is_admin(request, user):
    if _ALLOW_ADMIN or user in _ADMIN_USERS:
        return True
    return bool(await _run(request, request.app["cm"].is_user_admin, user))


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
    "favicon.svg": "image/svg+xml",
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


# Desktop viewer page (websockets viewer): waits for the session to be Ready,
# then hands the browser to the reverse-proxied Selkies client at
# /desktop/<id>/. The auth cookie set by this page's own request is what
# authorizes the proxied asset/WebSocket requests that follow (they carry no
# ?user=). Placeholders are replaced (not .format) so JS braces survive.
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
        if (j.phase === 'Ready') return j.viewer;
      }
    } catch (e) {}
    await sleep(2000);
  }
}

waitReady().then(viewer => {
  setStatus('Session ready — opening desktop…');
  if (viewer === 'vnc') {
    location.replace(`/vnc/${encodeURIComponent(id)}`);
  } else {
    // The trailing slash matters: the Selkies client resolves every asset, API
    // and WebSocket URL relative to the page *directory*.
    location.replace(`/desktop/${encodeURIComponent(id)}/`);
  }
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


# noVNC page, serving two different products off the same KubeVirt subresource:
#
#   /vnc/<id>      the *desktop* of a viewer:vnc VM. Any owner may open it, and
#                  it waits for Ready, because a desktop you can use is the
#                  point.
#   /console/<id>  the *machine console*. Admin-only, and attaches as soon as
#                  there is a VM instance at all — firmware, bootloader and
#                  kernel messages all happen long before Ready, so waiting for
#                  it would mean the boot output has already scrolled past, and
#                  a guest that never becomes Ready could never be looked at.
#
# Same RFB client, two gates and two waiting rules. __READY__ is the predicate
# that decides when to attach. Placeholders are replaced (not .format) so JS
# braces survive.
_VNC_HTML = """<!doctype html><meta charset=utf-8><title>__ID__ — __KIND__</title>
<style>
  html,body{margin:0;height:100%;background:#202020;overflow:hidden}
  #screen{position:absolute;inset:0}
  #msg{position:absolute;inset:0;z-index:20;display:flex;align-items:center;justify-content:center;
       color:#e0e0e0;font:15px/1.5 system-ui,sans-serif;background:rgba(32,32,32,.9)}
  #msg.hidden{display:none}
</style>
<div id=screen></div>
<div id=msg>Connecting…</div>
<script type=module>
import RFB from '/static/novnc/core/rfb.js';

const id = "__ID__", user = "__USER__";
const msgEl = document.getElementById('msg');
const setStatus = m => { msgEl.textContent = m; msgEl.classList.remove('hidden'); };
const hideStatus = () => msgEl.classList.add('hidden');
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function waitReady() {
  for (;;) {
    try {
      const r = await fetch(`/status/${id}?user=${encodeURIComponent(user)}`);
      if (r.ok) {
        const j = await r.json();
        const phase = j.phase || 'unknown';
        setStatus('__KIND_LABEL__: ' + phase);
        if (__READY__) return;
      }
    } catch (e) {}
    await sleep(2000);
  }
}

async function run() {
  for (;;) {
    await waitReady();
    setStatus('__OPENING__');
    const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
    const rfb = new RFB(document.getElementById('screen'),
                        proto + location.host + '__WSPATH__' + encodeURIComponent(id) +
                        '?user=' + encodeURIComponent(user));
    rfb.scaleViewport = true;
    const closed = new Promise(resolve => {
      rfb.addEventListener('connect', hideStatus);
      rfb.addEventListener('disconnect', e => resolve(e));
    });
    await closed;
    setStatus('Disconnected — retrying…');
    await sleep(2000);
  }
}
run();
</script>"""


def _render_vnc(user, session_id, console=False):
    """Render the noVNC page for either product (see _VNC_HTML).

    ``console=True`` is the machine console: it attaches as soon as a VM
    instance exists rather than waiting for Ready, which is the whole reason
    to have it — the boot sequence is over by the time a guest is Ready."""
    if console:
        kind, label, opening, ws = ("console", "Machine",
                                    "Attaching to console…", "/ws-console/")
        # Anything but Stopped means a VM instance exists (or is being made),
        # and qemu drives the emulated display from power-on.
        ready = "phase && phase !== 'Stopped'"
    else:
        kind, label, opening, ws = ("desktop", "Session",
                                    "Opening desktop…", "/ws-vnc/")
        ready = "phase === 'Ready'"
    return (_VNC_HTML
            .replace("__READY__", ready)
            .replace("__KIND_LABEL__", label)
            .replace("__KIND__", kind)
            .replace("__OPENING__", opening)
            .replace("__WSPATH__", ws)
            .replace("__ID__", html.escape(session_id))
            .replace("__USER__", html.escape(user)))


async def _channel_denied(request, user, name, channel):
    """``None`` when ``user`` may use ``channel`` on session ``name``, else the
    403 to return.

    The portal is one of the doors the channel grant has to hold at — a grant
    missed at one entry point leaks the whole identity half of the border
    (design/security.md, "Closing the fourth axis"). Resolving costs a couple
    of cluster reads, which is why this is called on the terminal handshake
    and not on every asset request."""
    channels = await _run(request, request.app["cm"].session_channels, user, name)
    if channel in channels:
        return None
    logger.warning(f"{channel} denied for {user} on {name} "
                   f"(granted: {sorted(channels)})")
    return web.Response(
        status=403, text=f"the {channel} channel is not available to you for "
                         f"this session")


def _resolve_target(instances, desktop_sessions, name):
    """Locate a Session by its short name across ssh instances and desktop
    sessions, returning a uniform dict for the terminal: what to attach to
    (pod for container/kata — kubectl exec; VMI for vm — the KubeVirt serial
    console subresource), plus namespace and readiness."""
    for i in instances:
        if i["name"] == name:
            # An ssh-mode session is a container *or* a VM (images/devbase):
            # attach to the pod via kubectl exec for the former, to the KubeVirt
            # serial console for the latter, exactly as the desktop branch below
            # does. The VM phase vocabulary is the operator's ("Ready"), not the
            # pod's ("Running").
            runtime = i.get("runtime") or "container"
            if runtime == "vm":
                ready = i.get("status") == "Ready" and bool(i.get("vmiName"))
            else:
                ready = i.get("status") == "Running" and bool(i.get("podName"))
            return {
                "podName": i.get("podName"), "namespace": i.get("namespace"),
                "phase": i.get("status"), "runtime": runtime,
                "vmiName": i.get("vmiName"),
                "ready": ready,
                "supported": True,
            }
    for s in desktop_sessions:
        if s["name"] == name:
            runtime = s.get("runtime")
            if runtime == "vm":
                ready = s.get("phase") == "Ready" and bool(s.get("vmiName"))
            else:
                ready = s.get("phase") == "Ready" and bool(s.get("podName"))
            return {
                "podName": s.get("podName"), "namespace": s.get("namespace"),
                "phase": s.get("phase"), "runtime": runtime,
                "vmiName": s.get("vmiName"),
                "ready": ready,
                "supported": True,
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
        # The CR create is not idempotent (409 on an existing name), but
        # re-launching your own session should just reconnect to it — only a
        # name collision with a *different* template is a real conflict.
        sessions = await _run(request, cm.get_user_desktop_sessions, user)
        existing = _resolve_session(sessions, name)
        if existing is None:
            return web.Response(status=500, text="failed to create session")
        if existing.get("template") != template:
            return web.Response(
                status=409,
                text=f"session '{name}' already exists with template "
                     f"'{existing.get('template')}' — pick another name or "
                     f"delete it first")
    raise web.HTTPFound(f"/connect/{name}?user={user}")


async def connect(request):
    """Serve the desktop viewer page and nudge the session awake so it becomes
    Ready while the page polls /status.

    The nudge is `ensure_instance_running`, not a plain start: this page is
    very often arrived at by a 303 from the portal's start dialog, and a plain
    start clears spec.runOverrides — the answers that dialog just gave."""
    cm, user = request.app["cm"], request["user"]
    name = request.match_info["id"]
    await _run(request, cm.ensure_instance_running, user, name)
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
        "viewer": sess.get("viewer"),
    })


# --------------------------------------------------------------------------- #
# Desktop reverse proxy (/desktop/<id>/*) — resolution + dispatch. The relay    #
# mechanics live in whistler.portal.proxy; this half owns *which* backend a     #
# request may reach: the session must exist in the requesting user's namespace  #
# and be Ready. Resolved targets are cached briefly because one dashboard load  #
# is a dozen asset requests and each resolution is two cluster list calls.      #
# --------------------------------------------------------------------------- #

_TARGET_TTL_SECONDS = 10.0


async def _resolve_desktop_base(request):
    """Return ``(base_url, error_response)`` for the session in the URL. Only
    Ready sessions resolve (and only those get cached); anything else is an
    error response for the proxy handler to return as-is."""
    cm, user = request.app["cm"], request["user"]
    name = request.match_info["id"]
    cache = request.app["desktop_targets"]
    key = (user, name)
    hit = cache.get(key)
    if hit and hit[0] > time.monotonic():
        return hit[1], None

    sessions = await _run(request, cm.get_user_desktop_sessions, user)
    sess = _resolve_session(sessions, name)
    if not sess:
        return None, web.Response(status=404, text="unknown session")
    if sess.get("phase") != "Ready":
        return None, web.Response(
            status=409, text=f"session not ready (phase={sess.get('phase')})")
    display_port = sess.get("displayPort")
    if not display_port:
        return None, web.Response(status=409, text="session has no display port")
    # status.address lags the operator's probe timer; the per-session Service
    # name is deterministic (<user>-<name>), so fall back to constructing it.
    address = sess.get("address") or \
        f"{user}-{name}.{sess['namespace']}.svc.cluster.local"
    base = f"http://{address}:{display_port}"
    cache[key] = (time.monotonic() + _TARGET_TTL_SECONDS, base)
    return base, None


async def desktop_redirect(request):
    """/desktop/<id> -> /desktop/<id>/ — the Selkies client only works from a
    directory URL (all its asset/WS paths are page-relative)."""
    suffix = f"?{request.query_string}" if request.query_string else ""
    raise web.HTTPFound(f"/desktop/{request.match_info['id']}/{suffix}")


async def desktop_proxy(request):
    base, err = await _resolve_desktop_base(request)
    if err is not None:
        return err
    target = f"{base}/{request.match_info.get('tail', '')}"
    if request.query_string:
        target += f"?{request.query_string}"
    client = request.app["proxy_client"]
    if proxy.is_websocket_upgrade(request):
        return await proxy.relay_ws(request, target, client)
    return await proxy.relay_http(request, target, client)


async def term(request):
    """Serve the web-terminal page and nudge the session awake so it's Ready by
    the time the page finishes polling. `ensure_instance_running` for the same
    reason as /connect: the start dialog may have just chosen this run."""
    cm, user = request.app["cm"], request["user"]
    name = request.match_info["id"]
    denied = await _channel_denied(request, user, name, CHANNEL_TERMINAL)
    if denied is not None:
        # Refuse before the reconcile nudge: a channel the user may not use is
        # not a reason to boot their session.
        return denied
    await _run(request, cm.ensure_instance_running, user, name)
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

    # The boundary, not the page: the page is a convenience, this is the
    # socket that carries bytes.
    denied = await _channel_denied(request, user, name, CHANNEL_TERMINAL)
    if denied is not None:
        return denied

    instances, desktop_sessions = await asyncio.gather(
        _run(request, cm.get_user_instances, user),
        _run(request, cm.get_user_desktop_sessions, user),
    )
    target = _resolve_target(instances, desktop_sessions, name)
    if not target:
        return web.Response(status=404, text="unknown session")
    if not target["supported"]:
        return web.Response(status=400, text="terminal not available for this session")
    if not target["ready"]:
        return web.Response(status=409, text=f"session not ready (phase={target['phase']})")

    if target["runtime"] == "vm":
        # No pod to exec into. Default: SSH into the guest with the portal's
        # per-user access key (real login semantics — fresh session, MOTD,
        # exit works). WHISTLER_VM_TERMINAL=console falls back to the shared
        # serial console (works even for guests without sshd, but it is the
        # boot console: shared TTY, no resize, autologin respawn on exit).
        if os.environ.get("WHISTLER_VM_TERMINAL", "ssh") == "console":
            try:
                upstream = await kubevirt.open_subresource_ws(
                    request.app["kube_ws_client"], target["namespace"],
                    target["vmiName"], "console")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"VM console for {user}/{name} failed to dial: {e}")
                return web.Response(status=502, text="VM console unreachable")
            wsr = web.WebSocketResponse()
            await wsr.prepare(request)
            logger.info(f"Opening serial console for {user}/{name} -> {target['vmiName']}")
            try:
                await kubevirt.relay_console(wsr, upstream)
            except Exception as e:
                logger.error(f"VM console for {user}/{name} failed: {e}")
                if not wsr.closed:
                    await wsr.close(message=str(e).encode())
            return wsr

        address, private_key = await asyncio.gather(
            _run(request, cm.get_vmi_address, user, name),
            _run(request, cm.get_vm_access_private_key, user),
        )
        if not address:
            return web.Response(status=409, text="VM has no address yet")
        if not private_key:
            return web.Response(status=502, text="no VM access key for this user")
        wsr = web.WebSocketResponse()
        await wsr.prepare(request)
        logger.info(f"Opening SSH terminal for {user}/{name} -> {address}")
        try:
            await kubevirt.relay_ssh(wsr, address, user, private_key)
        except Exception as e:
            logger.error(f"VM SSH terminal for {user}/{name} failed: {e}")
            if not wsr.closed:
                await wsr.close(message=str(e).encode())
        return wsr

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


async def vnc_page(request):
    """Serve the noVNC viewer page (VM sessions) and nudge the VM awake, like
    /connect does for the Selkies path.

    This is the session's **desktop** — `/connect` redirects viewer:vnc VMs
    here — so it is open to the owner like any other desktop. The admin-only
    machine console is `/console/<id>` (console_page); it dials a different
    websocket and attaches without waiting for Ready."""
    cm, user = request.app["cm"], request["user"]
    name = request.match_info["id"]
    await _run(request, cm.ensure_instance_running, user, name)
    return web.Response(text=_render_vnc(user, name), content_type="text/html")


async def console_page(request):
    """Serve the machine console: the emulated display from power-on —
    firmware, bootloader, kernel messages — rather than the desktop.

    **Admin-only**, enforced here and not merely by hiding the dashboard
    button, because a link is not an access control. It is a diagnostic view
    rather than one a user can act on (boot output is rarely where the problem
    is; that is usually Kubernetes-side), and on any instance with more than
    one member it is inherently unscoped: one machine, one console."""
    cm, user = request.app["cm"], request["user"]
    if not await _is_admin(request, user):
        return web.Response(status=403, text="The machine console is admin-only")
    name = request.match_info["id"]
    await _run(request, cm.ensure_instance_running, user, name)
    return web.Response(text=_render_vnc(user, name, console=True),
                        content_type="text/html")


async def ws_vnc(request, console=False):
    """Bridge the browser's RFB websocket to the VMI's VNC subresource. Same
    ownership boundary as ws_term: the session must resolve in the requesting
    user's namespace.

    Two entry points share this. ``/ws-vnc`` is the *desktop* of a viewer:vnc
    VM and waits for Ready like any other desktop. ``/ws-console`` is the
    machine console: admin-only, and attached as soon as there is a VM
    instance — the emulated display is driven from power-on, and Ready means
    "the desktop or sshd is answering", i.e. exactly the point at which the
    boot output someone came to watch has already scrolled past. A guest that
    never reaches Ready is also the one most worth looking at.

    The admin check is here as well as on the page because the websocket is
    reachable without ever loading it, and it runs before the session lookup
    so 403-vs-404 cannot be used to probe which sessions exist."""
    cm, user = request.app["cm"], request["user"]
    if console and not await _is_admin(request, user):
        return web.Response(status=403, text="The machine console is admin-only")
    name = request.match_info["id"]

    sessions = await _run(request, cm.get_user_desktop_sessions, user)
    sess = _resolve_session(sessions, name)
    if not sess:
        return web.Response(status=404, text="unknown session")
    if sess.get("runtime") != "vm":
        return web.Response(status=400, text="VNC is only available for VM sessions")
    if not sess.get("vmiName"):
        return web.Response(
            status=409,
            text=f"no VM instance to attach to yet (phase={sess.get('phase')})")
    if not console and sess.get("phase") != "Ready":
        return web.Response(status=409, text=f"session not ready (phase={sess.get('phase')})")

    try:
        upstream = await kubevirt.open_subresource_ws(
            request.app["kube_ws_client"], sess["namespace"], sess["vmiName"], "vnc")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"VNC for {user}/{name} failed to dial: {e}")
        return web.Response(status=502, text="VM VNC unreachable")

    downstream = web.WebSocketResponse(max_msg_size=0)
    await downstream.prepare(request)
    logger.info(f"Opening VNC for {user}/{name} -> {sess['vmiName']}")
    try:
        # Raw RFB bytes both ways; proxy._pump is already byte-transparent.
        done, pending = await asyncio.wait(
            [asyncio.ensure_future(proxy._pump(downstream, upstream)),
             asyncio.ensure_future(proxy._pump(upstream, downstream))],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.warning(f"VNC relay for {user}/{name} error: {exc}")
    finally:
        await upstream.close()
        if not downstream.closed:
            await downstream.close()
    return downstream


async def screenshot(request):
    """The latest thumbnail for one of the requesting user's sessions.

    No cluster round trip and no ownership check beyond the key: the store is
    keyed by (user, session) and the middleware already resolved the user, so
    another user's session simply isn't in this lookup. ``no-store`` because
    the bytes at this URL change under the browser every pass."""
    user, name = request["user"], request.match_info["id"]
    shot = screenshots.STORE.get(user, name)
    if shot is None:
        return web.Response(status=404, text="no screenshot for this session yet")
    captured_at, png = shot
    return web.Response(
        body=png, content_type="image/png",
        headers={
            "Cache-Control": "no-store",
            "X-Whistler-Captured-At": str(int(captured_at)),
        },
    )


async def healthz(request):
    return web.Response(text="ok")


async def _proxy_client_ctx(app):
    """Shared upstream HTTP client for the desktop proxy. auto_decompress=False
    keeps the relay byte-transparent (Content-Encoding passes through); no total
    timeout because the H.264 WebSocket lives for the whole session."""
    app["proxy_client"] = aiohttp.ClientSession(
        auto_decompress=False,
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=10),
    )
    app["desktop_targets"] = {}
    yield
    await app["proxy_client"].close()


def build_app(config_manager):
    # Order matters. auth_middleware resolves the identity the other two ask
    # about; entry_point_middleware then decides whether this user gets this
    # surface at all, and lock_middleware — innermost — whether this browser
    # gets anything right now (whistler/portal/kiosk.py).
    app = web.Application(middlewares=[auth_middleware, entry_point_middleware,
                                       kiosk.lock_middleware])
    app["cm"] = config_manager
    app.cleanup_ctx.append(_proxy_client_ctx)
    app.cleanup_ctx.append(kubevirt.kube_ws_client_ctx)
    app.cleanup_ctx.append(screenshots.screenshot_ctx)
    app.add_routes([
        web.get("/", index),
        web.post("/launch", launch),
        web.get("/connect/{id}", connect),
        web.get("/desktop/{id}", desktop_redirect),
        web.route("*", "/desktop/{id}/{tail:.*}", desktop_proxy),
        web.get("/vnc/{id}", vnc_page),
        web.get("/ws-vnc/{id}", ws_vnc),
        # The machine console: same subresource, admin-only, no wait for Ready.
        web.get("/console/{id}", console_page),
        web.get("/ws-console/{id}", partial(ws_vnc, console=True)),
        web.get("/status/{id}", status),
        web.get("/term/{id}", term),
        web.get("/term-status/{id}", term_status),
        web.get("/ws-term/{id}", ws_term),
        web.get("/screenshot/{id}", screenshot),
        web.get("/healthz", healthz),
        # The vendored noVNC tree (ES modules with relative imports) is served
        # whole; aiohttp's static handler is traversal-safe. The flat whitelist
        # handler below still covers the xterm.js/style single files.
        web.static("/static/novnc", os.path.join(_STATIC_DIR, "novnc")),
        web.get("/static/{filename}", static_file),
    ])
    # The kiosk surface (/kiosk) lives on this app so its session page can frame
    # /connect same-origin and its cards can load /screenshot — see kiosk.py.
    kiosk.add_routes(app)
    return app
