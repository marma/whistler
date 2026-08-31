"""The kiosk surface: ``/kiosk`` — the whole portal, for a user who should have
nothing else.

This is Whistler's half of the "kiosk situation" (design/security.md, "Closing
the fourth axis") rendered as a page: a login, a grid of the sessions this user
already has, and a full-screen desktop. **No configuration** — no template
picker, no create form, no volume or zone controls, no web terminal, no admin
anything. A kiosk-bound user is meant to be able to reach their desktops and
nothing more, so the surface itself has nothing more on it.

Routes (registered onto the viewer app, so ``/connect``, ``/desktop`` and
``/screenshot`` are same-origin — the session page depends on that):

  GET  /kiosk                the login screen, or the session grid once known
  POST /kiosk/login          credentials -> the second factor (see below)
  GET  /kiosk/otp            enrol an authenticator, or ask for a code
  POST /kiosk/otp            check the code -> identity cookies -> /kiosk
  POST /kiosk/logout         drop the cookies -> the login screen
  GET  /kiosk/sessions       JSON the grid repaints itself from
  GET  /kiosk/session/{id}   the desktop, full-bleed, under an idle watcher
  GET  /kiosk/lock           lock this browser; ?next= is where unlocking
                             returns to (see "The lock" below)

The lock
--------
The thin client watches for inactivity itself — it is outside Selkies and
outside this app, which is the right place for it — and navigates to
``/kiosk/lock?next=<where it was>``. What makes that a lock rather than a
screensaver is that the portal answers it by setting an **HttpOnly cookie**
holding the return path, and ``lock_middleware`` then refuses *every* route on
this app but the lock screen itself. So the live session cookie stops being
enough: a locked browser cannot reach /kiosk/session/<id>, /desktop/<id>/,
/screenshot/<id> or the display WebSocket, whether or not it has an address bar
to type them into. Only a password clears the cookie, and only for the user who
was locked — unlocking is not a login, and there is a separate Sign out for
when the next person is someone else.

The return path is read from the cookie, never from the query string of the
request being answered, so it cannot be rewritten mid-lock; ``_safe_return``
also refuses anything that is not a kiosk path, which is what keeps ?next= from
being an open redirect.

**In dev mode the lock is a mechanism with no secret behind it.** ``/kiosk`` and
the lock screen share ``_verify_credentials``, so while ``WHISTLER_AUTH_ALLOW_ANY``
is on, any password unlocks. The lock screen says so on its face rather than
implying an assurance it cannot give.

The second factor
-----------------
``/kiosk/otp`` sits between the password and the identity cookie: the password
earns only ``PENDING_COOKIE``, and ``_sign_in`` happens on the far side of a
six-digit code. It is **a mock**, and every piece of it says which parts are
real:

* **The algorithm is real and standard.** ``whistler/portal/totp.py`` is RFC
  6238 in the standard library, checked against the RFC's own vectors, and the
  QR holds a plain ``otpauth://`` URI — so this is not tied to Google
  Authenticator or to any one app. A code from a real authenticator that
  enrolled from this screen verifies here today.
* **The QR is real too.** ``segno`` (pure Python, no Pillow) encodes the
  provisioning URI and ``_qr_svg`` inlines it, so the enrolment screen works
  the way a user expects: point a phone at it. Inline rather than a separate
  image URL because that image *is* the shared secret.
* **The store is not.** ``_MOCK_ENROLMENTS`` is an in-process dict standing in
  for a Kubernetes Secret; it is per-replica and gone on restart, and there is
  nowhere to record the last counter accepted, so a code is replayable inside
  its step. ``_verify_otp`` therefore also accepts any six digits in dev mode
  and says so on the page.
* **The pending cookie is not a token.** It carries a bare username the client
  supplies, which is fine while the code prompt is a mock and an escalation the
  moment it is not; it has to become a signed, single-use, short-lived token
  bound to the password check that issued it.

The dev ``?user=`` auto-forward still skips the whole thing, as it skips the
password: it is the portal's dev identity shortcut, not a login.

What is *not* here yet, deliberately:

* **A password store.** Whistler has none — ``User`` CRs carry public keys, and
  the portal's web auth is still the dev gate the rest of ``app.py`` describes.
  So the form is real and ``verify_credentials`` (whistler/portal/login.py, now
  shared with the management portal) is the one place a real check goes, but
  today it only answers in dev mode (``WHISTLER_AUTH_ALLOW_ANY``), where any
  password is accepted. Outside dev mode the middleware 401s this path like
  every other. The OTP step above is the second factor for that same function,
  mocked to the same standard: no store, so no assurance.
* **A screen lock driven from inside the desktop.** The lock above is entered
  by the *client*, so it covers a person walking away from the browser. A guest
  that locks its own X session is a different and complementary thing.
* **Reacting to the guest's own "Log off".** That one is an XFCE/GNOME menu
  item the portal only sees indirectly, and since the desktop runs as a
  ``Restart=always`` system service it simply respawns a fresh session two
  seconds later — which is a reasonable kiosk answer, but it is not the portal
  noticing anything. **"Power off" now works**: VMs run under
  ``runStrategy: RerunOnFailure``, so a guest that shuts itself down stays
  down, and the operator records it as an ordinary stop
  (``_record_guest_shutdown``) instead of KubeVirt booting the machine back up
  under the user. The way back to the grid remains the corner control on the
  session page, or the idle timer.
"""
import asyncio
import html
import logging
import os

import segno
from aiohttp import web

from whistler.config import ENTRY_KIOSK
from whistler.portal import totp
# The form itself, its furniture and the one credential check all live in
# whistler/portal/login.py — the management portal shows the same screen (minus
# the second factor below), and one login is not two screens.
from whistler.portal.login import BASE_CSS as _BASE_CSS
from whistler.portal.login import dev_auth as _dev_auth
from whistler.portal.login import render_login as _render_login_form
from whistler.portal.login import USER_COOKIE as _USER_COOKIE
from whistler.portal.login import verify_credentials as _verify_credentials
from whistler.status import status_group

logger = logging.getLogger("whistler.portal")

# Marks a browser that came through the kiosk login. Identity itself rides the
# same _USER_COOKIE the auth middleware reads, so the proxied /desktop asset and
# WebSocket requests — which carry no ?user= — are authorized exactly as they
# are for the ordinary viewer pages. This second cookie is what makes "logged
# out of the kiosk" distinguishable from "has been to the portal at some point".
KIOSK_COOKIE = "whistler_kiosk"

# Set when a browser locks, cleared only by a correct password. HttpOnly and
# holding the return path, because both facts are load-bearing: the page it
# guards must not be able to clear it, and the place unlocking returns to must
# not be re-supplied by whoever is asking. Its presence — not a URL parameter —
# is what lock_middleware refuses on.
LOCK_COOKIE = "whistler_kiosk_lock"

# Set between "the password was right" and "the second factor was right". It
# holds the half-authenticated name, and nothing else on this app reads it —
# kiosk_identity keys on KIOSK_COOKIE, so a browser that stops at the OTP step has
# no identity and reaches nothing. HttpOnly for the same reason as the lock: the
# page it gates must not be able to write it.
#
# **In the real thing this cannot be a bare username in a cookie.** A cookie is
# client-supplied, so as written a browser that sets whistler_kiosk_pending=root
# skips straight to the code prompt — harmless while the code prompt is itself a
# mock, and a privilege escalation the moment it is not. It has to become a
# signed, short-lived (~2 min), single-use token naming the user, bound to the
# password check that issued it. Noted here rather than in a design doc because
# this is the line that has to change.
PENDING_COOKIE = "whistler_kiosk_pending"

# How long a kiosk may sit untouched. On a session page the timer returns to
# the grid; on the grid it logs out, because a kiosk that leaves the previous
# person's session list on screen has not really ended their visit. <=0
# disables, matching WHISTLER_SCREENSHOT_INTERVAL's convention.
_DEFAULT_IDLE_SECONDS = 900

# One colour per user-facing state from whistler/status.py. The management UI
# renders the same states as Fomantic label colours; the viewer app has no
# Fomantic, so these are the hex equivalents. test_kiosk asserts this covers
# every group, so a new state cannot silently render as "no colour".
_STATE_COLORS = {
    "Running":  "#21ba45",
    "Starting": "#fbbd08",
    "Pending":  "#2185d0",
    "Stopping": "#f2711c",
    "Stopped":  "#767676",
    "Error":    "#db2828",
}


def _idle_seconds() -> int:
    try:
        return int(os.environ.get("WHISTLER_KIOSK_IDLE_TIMEOUT",
                                  _DEFAULT_IDLE_SECONDS))
    except ValueError:
        return _DEFAULT_IDLE_SECONDS


async def _run(func, *args):
    """Run a blocking KubeConfigManager method off the event loop (the viewer
    app's own helper takes a request it never uses; this one doesn't)."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func, *args)


def kiosk_identity(request: web.Request):
    """Who this kiosk request is, or None to show the login screen.

    Public because the viewer app's entry-point middleware asks it: on a kiosk
    path the kiosk's answer is the identity that matters, and the auth
    middleware's fallback would refuse an anonymous browser its own login
    screen (whistler/portal/app.py, entry_point_middleware).

    Deliberately *not* the middleware's answer: that falls back to a default
    name so the viewer pages always have one, which for a login screen would
    mean nobody is ever logged out."""
    explicit = request.query.get("user")
    if explicit and _dev_auth():
        # Dev auto-forward: ?user= skips the form, matching the rest of the
        # portal's dev identity.
        return explicit.split("-")[0]
    if request.cookies.get(KIOSK_COOKIE) != "1":
        return None
    raw = request.cookies.get(_USER_COOKIE)
    return raw.split("-")[0] if raw else None


# The kiosk's own paths. Everything else on this app is refused while a browser
# is locked, so this set is the lock's blast radius and is written out rather
# than pattern-matched.
LOCK_PATH = "/kiosk/lock"
OTP_PATH = "/kiosk/otp"
# The OTP step is deliberately *not* here: it belongs to a browser that is not
# signed in yet, and a locked browser is one that already is. Nothing on it
# would help someone standing in front of a lock.
_LOCK_ALLOWED = frozenset({LOCK_PATH, "/kiosk/login", "/kiosk/logout"})


def _safe_return(path) -> str:
    """Sanitise a ?next=. Only a kiosk path is accepted — which also disposes
    of protocol-relative "//host" and absolute URLs, since neither can start
    with "/kiosk" — and anything else falls back to the grid rather than being
    refused, because a thin client sending a stale URL should still lock."""
    if not path or not isinstance(path, str) or len(path) > 512:
        return "/kiosk"
    if any(c in path for c in "\r\n\\"):
        return "/kiosk"
    if path == "/kiosk" or path.startswith("/kiosk/"):
        # Returning *to* the lock screen would be a loop.
        return "/kiosk" if path.startswith(LOCK_PATH) else path
    return "/kiosk"


def _locked(request: web.Request):
    """The path unlocking should return to, or None when not locked."""
    return request.cookies.get(LOCK_COOKIE) or None


def _lock(response: web.StreamResponse, return_to: str) -> web.StreamResponse:
    response.set_cookie(LOCK_COOKIE, return_to, path="/", samesite="Lax",
                        httponly=True)
    return response


def _unlock(response: web.StreamResponse) -> web.StreamResponse:
    response.del_cookie(LOCK_COOKIE, path="/")
    return response


# _sign_in/_sign_out stamp the redirect that carries them. Both are *raised*
# by their handlers (aiohttp deprecated returning an HTTPException), and the
# cookies ride on the exception object itself — which is also why the auth
# middleware's own cookie write is skipped on these paths and the kiosk sets
# the identity cookie explicitly rather than leaning on it.
def _sign_in(response: web.StreamResponse, user: str) -> web.StreamResponse:
    response.set_cookie(_USER_COOKIE, user, path="/", samesite="Lax")
    response.set_cookie(KIOSK_COOKIE, "1", path="/", samesite="Lax",
                        httponly=True)
    response.del_cookie(PENDING_COOKIE, path="/")
    return _unlock(response)


def _sign_out(response: web.StreamResponse) -> web.StreamResponse:
    response.del_cookie(_USER_COOKIE, path="/")
    response.del_cookie(KIOSK_COOKIE, path="/")
    response.del_cookie(PENDING_COOKIE, path="/")
    return _unlock(response)


def _pend(response: web.StreamResponse, user: str) -> web.StreamResponse:
    """Park a browser between the two factors."""
    response.set_cookie(PENDING_COOKIE, user, path="/", samesite="Lax",
                        httponly=True)
    return response


def _pending(request: web.Request):
    """The half-authenticated user, or None. Never an identity: the caller may
    only show the code prompt with it, never a session."""
    return request.cookies.get(PENDING_COOKIE) or None


# --------------------------------------------------------------------------- #
# Pages. Hand-rolled like the rest of the viewer app (the Jinja/Fomantic       #
# templates belong to the management app on the other port). Placeholders are  #
# replaced rather than .format-ed so the JS braces survive.                    #
# --------------------------------------------------------------------------- #

def _render_login(error: str = None) -> str:
    return _render_login_form(action="/kiosk/login", error=error)


# The lock screen. Same furniture as the login form, and deliberately a
# different statement: it names who is locked and asks only for their password,
# because unlocking returns a person to their own session rather than starting
# one. Signing out is the escape hatch when the next person is someone else —
# it is a button, not the default, so a passer-by cannot end a session by
# guessing at the form.
_LOCK_HTML = """<!doctype html><meta charset=utf-8><title>Locked</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<link rel=icon type=image/svg+xml href=/static/favicon.svg>
<style>__BASE_CSS__
  body{display:flex;align-items:center;justify-content:center;padding:2rem}
  .box{width:100%;max-width:22rem;display:flex;flex-direction:column;gap:1rem}
  h1{text-align:center}
  .who{text-align:center;font-size:.95rem;margin:-.4rem 0 .2rem}
  form{display:flex;flex-direction:column;gap:1rem;margin:0}
  label{display:flex;flex-direction:column;gap:.35rem;font-size:.82rem;
        text-transform:uppercase;letter-spacing:.08em;color:#9a9a9a}
  input{font:inherit;text-transform:none;letter-spacing:normal;color:#e8e8e8;
        background:#242424;border:1px solid #383838;border-radius:6px;
        padding:.6rem .7rem}
  input:focus{outline:none;border-color:#4b8bd6;background:#282828}
  button{font:inherit;font-weight:600;color:#fff;background:#2f6fb5;border:0;
         border-radius:6px;padding:.65rem;cursor:pointer}
  button:hover{background:#3b81cd}
  .signout button{background:none;border:1px solid #3a3a3a;color:#9a9a9a;
                  font-weight:400;font-size:.82rem;width:100%}
  .signout button:hover{color:#fff;border-color:#5a5a5a}
  .error{background:#3a1e1e;border:1px solid #6e2b2b;border-radius:6px;
         padding:.55rem .7rem;font-size:.88rem;color:#f3b6b6}
  .note{font-size:.78rem;text-align:center;margin:0}
  hr{border:0;border-top:1px solid #2c2c2c;margin:.2rem 0}
</style>
<div class=box>
  <h1>Locked</h1>
  <p class="who muted">__USER__</p>
  __ERROR__
  <form method=post action=/kiosk/login>
    <label>Password<input name=password type=password autofocus
           autocomplete=current-password></label>
    <button type=submit>Unlock</button>
  </form>
  <p class="note muted">__NOTE__</p>
  <hr>
  <form class=signout method=post action=/kiosk/logout>
    <button type=submit>Sign out &mdash; a different person is using this screen</button>
  </form>
</div>"""


def _render_lock(user: str, error: str = None) -> str:
    note = ("Development mode — any password unlocks."
            if _dev_auth() else "")
    err = (f"<div class=error>{html.escape(error)}</div>" if error else "")
    return (_LOCK_HTML
            .replace("__BASE_CSS__", _BASE_CSS)
            .replace("__ERROR__", err)
            .replace("__NOTE__", html.escape(note))
            .replace("__USER__", html.escape(user)))


# The grid paints itself from /kiosk/sessions and repaints on a timer, so there
# is one renderer for the cards rather than a server-rendered copy that has to
# agree with it. Thumbnails are (re)fetched only when a card appears or changes
# state — /screenshot is no-store, and a poll-rate refresh would flicker for no
# gain against a capture interval measured in minutes.
_GRID_HTML = """<!doctype html><meta charset=utf-8><title>Whistler</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<link rel=icon type=image/svg+xml href=/static/favicon.svg>
<style>__BASE_CSS__
  body{display:flex;flex-direction:column}
  header{display:flex;align-items:center;justify-content:space-between;
         padding:1.1rem 1.6rem;flex:0 0 auto}
  header .who{font-size:.9rem}
  header form{margin:0}
  header button{font:inherit;font-size:.82rem;color:#bdbdbd;background:none;
                border:1px solid #3a3a3a;border-radius:6px;padding:.35rem .8rem;
                cursor:pointer}
  header button:hover{color:#fff;border-color:#5a5a5a}
  main{flex:1 1 auto;display:flex;align-items:center;justify-content:center;
       padding:1rem 1.6rem 3rem}
  /* 68rem is four cards plus their gaps: the common case for a kiosk screen
     is a handful of sessions, and wrapping 4 to 3+1 for the sake of a round
     number reads as a mistake. */
  #grid{display:flex;flex-wrap:wrap;gap:1.4rem;justify-content:center;
        max-width:68rem}
  .card{width:15.5rem;background:#242424;border:1px solid #333;border-radius:10px;
        overflow:hidden;display:flex;flex-direction:column;cursor:pointer;
        transition:border-color .15s,transform .15s}
  .card:hover{border-color:#4b8bd6;transform:translateY(-2px)}
  .shot{aspect-ratio:16/10;background:#1c1c1c;display:flex;align-items:center;
        justify-content:center;overflow:hidden}
  .shot img{width:100%;height:100%;object-fit:cover;display:block}
  .shot .glyph{font-size:2.2rem;color:#3d3d3d}
  .body{padding:.75rem .85rem .85rem;display:flex;flex-direction:column;gap:.3rem}
  .name{font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .tpl{font-size:.78rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .state{display:flex;align-items:center;gap:.45rem;font-size:.8rem;
         margin-top:.15rem}
  .dot{width:.55rem;height:.55rem;border-radius:50%;flex:0 0 auto}
  .empty{text-align:center;line-height:1.8}
</style>
<header>
  <h1>Whistler</h1>
  <div style="display:flex;align-items:center;gap:1rem">
    <span class="who muted">__USER__</span>
    <form method=post action=/kiosk/logout><button type=submit>Log out</button></form>
  </div>
</header>
<main><div id=grid></div></main>
<script>
const IDLE_MS = __IDLE_MS__, POLL_MS = 5000;
const grid = document.getElementById('grid');
const seen = new Map();   // name -> last rendered state

function paint(sessions) {
  if (!sessions.length) {
    grid.innerHTML = '<p class="empty muted">No sessions available.<br>' +
                     'Ask an administrator to set one up for you.</p>';
    seen.clear();
    return;
  }
  const names = new Set(sessions.map(s => s.name));
  for (const [name, el] of seen) {
    if (!names.has(name)) { el.node.remove(); seen.delete(name); }
  }
  for (const s of sessions) {
    let entry = seen.get(s.name);
    if (!entry) {
      const node = document.createElement('a');
      node.className = 'card';
      node.href = '/kiosk/session/' + encodeURIComponent(s.name);
      node.innerHTML =
        '<div class=shot><span class=glyph>\\u25a2</span></div>' +
        '<div class=body><span class=name></span>' +
        '<span class="tpl muted"></span>' +
        '<span class=state><span class=dot></span><span class=label></span></span></div>';
      node.querySelector('.name').textContent = s.name;
      node.querySelector('.tpl').textContent = s.template || '';
      grid.appendChild(node);
      entry = { node, state: null };
      seen.set(s.name, entry);
    }
    if (entry.state !== s.state) {
      entry.state = s.state;
      entry.node.querySelector('.dot').style.background = s.color;
      entry.node.querySelector('.label').textContent = s.state;
      const shot = entry.node.querySelector('.shot');
      if (s.running) {
        const img = new Image();
        img.onload = () => { shot.replaceChildren(img); };
        img.src = '/screenshot/' + encodeURIComponent(s.name) + '?t=' + Date.now();
      } else {
        shot.replaceChildren(Object.assign(document.createElement('span'),
                                           { className: 'glyph', textContent: '\\u25a2' }));
      }
    }
  }
  // Keep the DOM order stable and alphabetical as sessions come and go.
  for (const s of sessions) grid.appendChild(seen.get(s.name).node);
}

async function poll() {
  try {
    const r = await fetch('/kiosk/sessions', { cache: 'no-store' });
    // 401 = signed out, 423 = locked while this page was up. Either way the
    // answer is /kiosk, which sends a locked browser on to the lock screen.
    if (r.status === 401 || r.status === 423) { location.replace('/kiosk'); return; }
    if (r.ok) paint(await r.json());
  } catch (e) {}
}
poll();
setInterval(poll, POLL_MS);

// Idle on the grid ends the visit: leaving the previous person's session list
// on a kiosk screen is not an ended visit.
if (IDLE_MS > 0) {
  let timer;
  const reset = () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      const f = document.createElement('form');
      f.method = 'post'; f.action = '/kiosk/logout';
      document.body.appendChild(f); f.submit();
    }, IDLE_MS);
  };
  for (const e of ['mousemove', 'mousedown', 'keydown', 'wheel', 'touchstart'])
    document.addEventListener(e, reset, { capture: true, passive: true });
  reset();
}
</script>"""


def _render_grid(user: str) -> str:
    return (_GRID_HTML
            .replace("__BASE_CSS__", _BASE_CSS)
            .replace("__IDLE_MS__", str(_idle_seconds() * 1000))
            .replace("__USER__", html.escape(user)))


# The desktop, full-bleed. It is an *iframe* rather than a navigation for one
# reason: something has to keep watching for idleness while the desktop is on
# screen, and a navigation to /desktop/<id>/ hands the page to the guest's own
# Selkies client, which is not ours to instrument. The frame is same-origin
# (both are served by this app), so a capture-phase listener on its document
# sees every pointer and key event that reaches the canvas — including under
# pointer lock, where the events still land on the document.
#
# The src is /connect/<id>, not /desktop/<id>/ directly: that page already
# nudges the session awake, waits for Ready, and picks the websockets or vnc
# viewer. Its own location.replace fires the frame's load event again, which is
# why the watcher re-attaches on every load.
_SESSION_HTML = """<!doctype html><meta charset=utf-8><title>__ID__</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<link rel=icon type=image/svg+xml href=/static/favicon.svg>
<style>
  html,body{margin:0;height:100%;background:#1a1a1a;overflow:hidden}
  iframe{position:absolute;inset:0;width:100%;height:100%;border:0}
  #back{position:absolute;bottom:.6rem;right:.6rem;z-index:10;opacity:.12;
        font:600 .78rem/1 system-ui,sans-serif;color:#fff;text-decoration:none;
        background:rgba(0,0,0,.65);border:1px solid rgba(255,255,255,.25);
        border-radius:999px;padding:.45rem .85rem;cursor:pointer;
        transition:opacity .18s}
  #back:hover{opacity:1}
</style>
<iframe id=screen src="/connect/__ID__" allow="fullscreen; clipboard-read; clipboard-write"></iframe>
<a id=back href="/kiosk">&#8592; Sessions</a>
<script>
const IDLE_MS = __IDLE_MS__;
const EVENTS = ['mousemove', 'mousedown', 'keydown', 'wheel', 'touchstart'];
let timer;

const reset = () => {
  if (IDLE_MS <= 0) return;
  clearTimeout(timer);
  timer = setTimeout(() => location.replace('/kiosk'), IDLE_MS);
};

function watch(doc) {
  if (!doc) return;
  for (const e of EVENTS)
    doc.addEventListener(e, reset, { capture: true, passive: true });
}

watch(document);
const frame = document.getElementById('screen');
// Same-origin, so contentDocument is readable; the try/catch is only there so a
// deployment that ever ends up serving the viewer from another origin degrades
// to "no idle timeout inside the desktop" instead of a dead page.
const hook = () => { try { watch(frame.contentDocument); } catch (e) {} };
frame.addEventListener('load', hook);
hook();
reset();
</script>"""


def _render_session(session_id: str) -> str:
    return (_SESSION_HTML
            .replace("__IDLE_MS__", str(_idle_seconds() * 1000))
            .replace("__ID__", html.escape(session_id, quote=True)))


# --------------------------------------------------------------------------- #
# The second factor. A MOCK — see the module docstring.                        #
# --------------------------------------------------------------------------- #

# Stands in for storage that does not exist. The real enrolment record is a
# Kubernetes Secret in Whistler's namespace (never the User CR, which is
# world-readable to the portal's admins and backed up as configuration): the
# base32 secret, the timestamp it was confirmed, and the last counter accepted
# so a code cannot be replayed inside its own 30-second step. This dict is
# in-process, so it is per-replica and gone on restart — which is exactly why it
# is a mock and not a control.
_MOCK_ENROLMENTS: dict = {}

# Where a real deployment decides this. Per-user, because "every account has a
# second factor" and "kiosk accounts do" are different policies and the second
# one is likelier first: a `mfa: required` on the User CR, or a group grant, or
# an OIDC provider that has already done it and hands us a claim. True for
# everyone here so the step is visible.
def _otp_required(user: str) -> bool:
    return bool(user)


def _enrolment(user: str) -> dict:
    """This user's enrolment record, creating an unconfirmed one on first sight.

    The secret is generated once and kept, so a reload of the enrolment page
    shows the same QR the phone already scanned rather than silently invalidating
    it — the single most common way a hand-rolled enrolment flow goes wrong."""
    rec = _MOCK_ENROLMENTS.get(user)
    if rec is None:
        rec = {"secret": totp.generate_secret(), "confirmed": False}
        _MOCK_ENROLMENTS[user] = rec
    return rec


def _verify_otp(user: str, code: str) -> bool:
    """The mock gate. Two ways in, and the page says both out loud.

    A code that actually verifies against this user's secret is accepted — so a
    phone that scanned the QR on the enrolment screen enrols and signs in for
    real, algorithm and all, which is the whole point of keeping
    ``whistler/portal/totp.py`` honest. Failing that, dev mode accepts any six
    digits, because there is no enrolment anyone could have completed on a fresh
    process. Outside dev mode neither branch can pass, matching
    ``_verify_credentials``.

    What a real one adds: the replay check (``totp.verify_counter``, store the
    counter), a rate limit — six digits is a million guesses and a 90-second
    window, so this is the one login field that *must* be throttled — and, at
    enrolment, refusing to confirm until a code proves the app holds the secret
    (which is what this screen asks for, and the mock's dev branch waves)."""
    rec = _MOCK_ENROLMENTS.get(user)
    if rec and totp.verify(totp.decode_secret(rec["secret"]), code):
        return True
    given = (code or "").strip()
    if (_dev_auth() and len(given) == totp.DIGITS
            and given.isascii() and given.isdigit()):
        logger.warning(f"Kiosk OTP mock-accepted for {user} (dev mode)")
        return True
    return False


def _qr_svg(payload: str, scale: int = 5) -> str:
    """The provisioning URI as a real, scannable QR code.

    ``segno`` is the whole reason this is three lines: pure Python, no Pillow, no
    C extension, and it emits an SVG string directly. Inline SVG rather than a
    PNG or a data: URI because it costs no second request, stays crisp at any
    DPI, and never becomes a cacheable URL of its own — this image *is* the
    shared secret, so it must not exist anywhere but in the page that already
    contains it.

    The parameters are the ones that decide whether a phone actually reads it
    off a screen:

    * ``error='m'`` — 15% recovery, what authenticator QRs conventionally use.
      Higher pushes the version up and the modules smaller for no gain against a
      clean screen; lower is fragile on a glossy panel.
    * ``micro=False`` — a Micro QR cannot hold a URI this long, and some
      scanners do not read them at all. Being explicit means a shorter payload
      can never silently produce one.
    * ``border=4`` — the quiet zone the spec requires. Cropping it is the
      classic reason a code that looks fine will not scan.

    ``scale=5`` keeps a version-6 symbol (this URI's size) around 245px, which
    phones read easily off a screen and which leaves the whole enrolment screen
    on one page — a kiosk that has to be scrolled to find the code field is its
    own kind of broken.
    """
    qr = segno.make(payload, error="m", micro=False)
    return qr.svg_inline(scale=scale, border=4, dark="#111111", light="#ffffff",
                         svgclass="qr", lineclass=None)


# One page, two states: enrol (QR + key + a code to prove the app has it) and
# verify (the code alone). Same form, same POST — the server knows which state
# the user is in, and a client that guesses wrong just gets the other screen.
_OTP_HTML = """<!doctype html><meta charset=utf-8><title>Verification</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<link rel=icon type=image/svg+xml href=/static/favicon.svg>
<style>__BASE_CSS__
  body{display:flex;align-items:center;justify-content:center;padding:2rem}
  .box{width:100%;max-width:23rem;display:flex;flex-direction:column;gap:1rem}
  h1{text-align:center}
  .who{text-align:center;font-size:.95rem;margin:-.4rem 0 .2rem}
  form{display:flex;flex-direction:column;gap:1rem;margin:0}
  label{display:flex;flex-direction:column;gap:.35rem;font-size:.82rem;
        text-transform:uppercase;letter-spacing:.08em;color:#9a9a9a}
  input{font:inherit;text-transform:none;color:#e8e8e8;background:#242424;
        border:1px solid #383838;border-radius:6px;padding:.6rem .7rem}
  #code{font-size:1.5rem;text-align:center;letter-spacing:.5em;
        font-variant-numeric:tabular-nums;padding-left:.5em}
  input:focus{outline:none;border-color:#4b8bd6;background:#282828}
  button{font:inherit;font-weight:600;color:#fff;background:#2f6fb5;border:0;
         border-radius:6px;padding:.65rem;cursor:pointer}
  button:hover{background:#3b81cd}
  .signout button{background:none;border:1px solid #3a3a3a;color:#9a9a9a;
                  font-weight:400;font-size:.82rem;width:100%}
  .signout button:hover{color:#fff;border-color:#5a5a5a}
  .error{background:#3a1e1e;border:1px solid #6e2b2b;border-radius:6px;
         padding:.55rem .7rem;font-size:.88rem;color:#f3b6b6}
  .note{font-size:.78rem;text-align:center;margin:0}
  .steps{margin:0;padding-left:1.1rem;font-size:.85rem;color:#a8a8a8}
  .steps li{margin:.25rem 0}
  .enrol{display:flex;flex-direction:column;align-items:center;gap:.6rem;
         background:#212121;border:1px solid #303030;border-radius:10px;
         padding:.9rem}
  /* Full contrast and a white surround: this one is meant to be read by a
     phone camera, and dimming it or letting the page background bleed into the
     quiet zone is how a valid code stops scanning. */
  .qr{border-radius:4px;display:block;background:#fff;padding:0}
  details{width:100%;font-size:.8rem}
  summary{cursor:pointer;color:#9a9a9a;text-align:center;padding:.2rem}
  summary:hover{color:#d0d0d0}
  /* Groups of four, and never a group split across lines: the key exists to be
     retyped, and a break mid-group is exactly where a person loses their
     place. Hence spans in a wrapping flex row rather than one string. */
  .key{font:13px/1.6 ui-monospace,SFMono-Regular,Menlo,monospace;
       display:flex;flex-wrap:wrap;gap:.1rem .55rem;justify-content:center;
       background:#1b1b1b;border:1px solid #303030;border-radius:6px;
       padding:.5rem .6rem;width:100%;color:#dcdcdc;letter-spacing:.05em}
  hr{border:0;border-top:1px solid #2c2c2c;margin:.2rem 0}
</style>
<div class=box>
  <h1>__TITLE__</h1>
  <p class="who muted">__USER__</p>
  __ERROR__
  __ENROL__
  <form method=post action=/kiosk/otp>
    <label>__CODE_LABEL__<input id=code name=code inputmode=numeric
           autocomplete=one-time-code pattern="[0-9]*" maxlength=6 autofocus
           required></label>
    <button type=submit>__SUBMIT__</button>
  </form>
  <p class="note muted">__NOTE__</p>
  <hr>
  <form class=signout method=post action=/kiosk/logout>
    <button type=submit>Cancel and start over</button>
  </form>
</div>
<script>
// Submit as soon as six digits are in: on a kiosk the keyboard may be the only
// input device in reach, and the code is fixed-length so there is nothing to
// confirm. Blocking non-digits keeps the field honest about what it accepts.
const f = document.getElementById('code');
f.addEventListener('input', () => {
  f.value = f.value.replace(/\\D/g, '').slice(0, 6);
  if (f.value.length === 6) f.form.requestSubmit();
});
</script>"""

_ENROL_HTML = """<div class=enrol>
  __QR__
  <details>
    <summary>Can&rsquo;t scan it?</summary>
    <div class=key>__SECRET__</div>
  </details>
</div>
<ol class=steps>
  <li>Open any authenticator app &mdash; Google Authenticator, Authy,
      1Password, Bitwarden, Aegis, FreeOTP.</li>
  <li>Scan the code above, or type the key by hand.</li>
  <li>Enter the six digits it shows, to confirm it worked.</li>
</ol>"""


def _render_otp(user: str, enrol: dict = None, error: str = None) -> str:
    """`enrol` is the unconfirmed record when this user still has to add the
    account, or None when they are only being asked for a code."""
    err = (f"<div class=error>{html.escape(error)}</div>" if error else "")
    if enrol is not None:
        uri = totp.provisioning_uri(enrol["secret"], user)
        # Grouped in fours: this is the form a person has to retype without
        # losing their place, and every authenticator app accepts the spaces.
        key = "".join(f"<span>{html.escape(enrol['secret'][i:i + 4])}</span>"
                      for i in range(0, len(enrol["secret"]), 4))
        enrol_html = (_ENROL_HTML
                      .replace("__QR__", _qr_svg(uri))
                      .replace("__SECRET__", key))
    else:
        enrol_html = ""
    if not _dev_auth():
        note = ""
    elif enrol is not None:
        note = ("Development mode — any six digits are accepted, and an app "
                "enrolled from this code also works.")
    else:
        note = "Development mode — any six digits are accepted."
    return (_OTP_HTML
            .replace("__BASE_CSS__", _BASE_CSS)
            .replace("__TITLE__", "Set up verification" if enrol is not None
                     else "Verification")
            .replace("__ERROR__", err)
            .replace("__ENROL__", enrol_html)
            .replace("__CODE_LABEL__", "Code from your app")
            .replace("__SUBMIT__", "Confirm" if enrol is not None else "Verify")
            .replace("__NOTE__", html.escape(note))
            .replace("__USER__", html.escape(user)))


# --------------------------------------------------------------------------- #
# Handlers                                                                     #
# --------------------------------------------------------------------------- #

def _html(body: str, **kw) -> web.Response:
    """Every kiosk page is identity-scoped, and the enrolment one holds a shared
    secret as both text and image, so none of them may sit in a cache — least of
    all on a shared machine with a Back button."""
    return web.Response(text=body, content_type="text/html",
                        headers={"Cache-Control": "no-store"}, **kw)


async def kiosk_index(request: web.Request):
    """The login screen, or the grid. ``?user=`` in dev mode signs in and
    redirects to the bare path so the URL a kiosk browser sits on carries no
    identity.

    A locked browser never gets here — lock_middleware sends it to the lock
    screen first."""
    user = kiosk_identity(request)
    if user is None:
        # Mid-flow: a reload between the two factors returns to the code prompt
        # rather than starting the login over. Cancelling is the button on that
        # screen, which clears the pending cookie explicitly.
        if _pending(request):
            raise web.HTTPSeeOther(OTP_PATH)
        return _html(_render_login())
    if request.query.get("user"):
        raise _sign_in(web.HTTPSeeOther("/kiosk"), user)
    return _html(_render_grid(user))


async def kiosk_lock(request: web.Request):
    """Lock this browser, and serve the lock screen.

    This is the URL the thin client navigates to when it decides the person has
    gone: ``/kiosk/lock?next=<where it was>``. The ?next= is consumed once, into
    the cookie, and the browser is redirected to the bare path — so the parked
    URL says nothing about which session is behind it, and a reload cannot
    change where unlocking goes.

    Locking someone who was never signed in is a no-op: there is nothing to
    lock, and the login screen is already the strictest thing to show."""
    user = kiosk_identity(request)
    if user is None:
        raise _sign_out(web.HTTPSeeOther("/kiosk"))
    if "next" in request.query:
        return_to = _safe_return(request.query.get("next"))
        logger.info(f"Kiosk locked for {user} (returns to {return_to})")
        raise _lock(web.HTTPSeeOther(LOCK_PATH), return_to)
    if _locked(request) is None:
        # Reached without ?next= and not already locked — lock anyway, to the
        # grid. "Show me the lock screen" must never mean "and stay unlocked".
        raise _lock(web.HTTPSeeOther(LOCK_PATH), "/kiosk")
    return _html(_render_lock(user))


async def kiosk_login(request: web.Request):
    """Sign in, or unlock — the same password, two different contracts.

    Unlocking takes the user from the lock cookie, not from the form: the point
    of a lock is to return one specific person to their own screen, so there is
    no username field to fill in and no way to become someone else by filling
    it in anyway. Becoming someone else is Sign out, which is a different
    button with a different effect."""
    form = await request.post()
    password = form.get("password") or ""
    return_to = _locked(request)

    if return_to is not None:
        user = kiosk_identity(request)
        if user is None:                     # lock outlived the identity
            raise _sign_out(web.HTTPSeeOther("/kiosk"))
        if not _verify_credentials(user, password):
            logger.warning(f"Kiosk unlock refused for {user}")
            return _html(_render_lock(user, "Wrong password."), status=401)
        logger.info(f"Kiosk unlocked for {user}")
        raise _unlock(web.HTTPSeeOther(return_to))

    user = (form.get("user") or "").strip().split("-")[0]
    if not _verify_credentials(user, password):
        logger.warning(f"Kiosk login refused for {user!r}")
        return _html(_render_login("Sign-in failed."), status=401)
    # The password was right and this is the wrong door. entry_point_middleware
    # would refuse the grid a moment later anyway (it is the boundary); saying
    # so here is the difference between an explanation and a bounce.
    if not await _run(request.app["cm"].may_enter, user, ENTRY_KIOSK):
        logger.warning(f"Kiosk login refused for {user}: no "
                       f"'{ENTRY_KIOSK}' entry point")
        return _html(_render_login("This account cannot use the kiosk."),
                     status=403)
    if _otp_required(user):
        # Not signed in yet: the password only earns the pending cookie, so a
        # browser stopped here has no identity and can reach nothing.
        logger.info(f"Kiosk password accepted for {user}; second factor pending")
        raise _pend(web.HTTPSeeOther(OTP_PATH), user)
    logger.info(f"Kiosk login for {user}")
    raise _sign_in(web.HTTPSeeOther("/kiosk"), user)


async def kiosk_otp(request: web.Request):
    """The second-factor screen — enrol, or ask for a code.

    Reached only with the pending cookie: an already-signed-in browser is sent
    to the grid (the factor is behind it) and one with neither cookie to the
    login screen, because a code prompt with no name attached has nothing to
    check a code against."""
    user = _pending(request)
    if user is None:
        raise web.HTTPSeeOther("/kiosk")
    rec = _enrolment(user)
    return _html(_render_otp(user, None if rec["confirmed"] else rec))


async def kiosk_otp_verify(request: web.Request):
    """Check the code, and only now sign in.

    Confirming an enrolment and using it afterwards are the same POST on
    purpose: both prove the app holds the secret, and treating enrolment as a
    separate trusted step is how a flow ends up with an account whose second
    factor nobody ever demonstrated."""
    user = _pending(request)
    if user is None:
        raise web.HTTPSeeOther("/kiosk")
    form = await request.post()
    rec = _enrolment(user)
    enrolling = not rec["confirmed"]
    if not _verify_otp(user, form.get("code") or ""):
        logger.warning(f"Kiosk OTP refused for {user}")
        return _html(_render_otp(user, rec if enrolling else None,
                                 "That code was not accepted."), status=401)
    if enrolling:
        rec["confirmed"] = True
        logger.info(f"Kiosk OTP enrolment confirmed for {user} (mock store)")
    logger.info(f"Kiosk login for {user} (second factor passed)")
    raise _sign_in(web.HTTPSeeOther("/kiosk"), user)


async def kiosk_logout(request: web.Request):
    raise _sign_out(web.HTTPSeeOther("/kiosk"))


def _session_view(session: dict) -> dict:
    """One card. ``state`` is the shared user-facing state (whistler/status.py),
    so the kiosk and the management dashboard cannot disagree about what
    "Running" means."""
    state = status_group(session.get("phase"))
    return {
        "name": session["name"],
        "template": session.get("template"),
        "state": state,
        "running": state == "Running",
        "color": _STATE_COLORS[state],
    }


async def kiosk_sessions(request: web.Request):
    """The grid's data. Desktop-mode sessions only: a card's whole purpose is
    to open a desktop, and an ssh-mode session has none — offering one here
    would mean offering a web terminal, which is exactly the surface a
    kiosk-bound user is not supposed to have."""
    user = kiosk_identity(request)
    if user is None:
        return web.json_response({"error": "not signed in"}, status=401)
    sessions = await _run(request.app["cm"].get_user_desktop_sessions, user)
    return web.json_response(
        sorted((_session_view(s) for s in sessions), key=lambda s: s["name"]),
        headers={"Cache-Control": "no-store"})


async def kiosk_session(request: web.Request):
    """The desktop page. Resolves the name against this user's own sessions
    first: an unknown name must be a 404 here rather than a frame pointed at
    /connect, which would leave the user staring at a page that polls forever."""
    user = kiosk_identity(request)
    if user is None:
        raise web.HTTPSeeOther("/kiosk")
    name = request.match_info["id"]
    sessions = await _run(request.app["cm"].get_user_desktop_sessions, user)
    if not any(s.get("name") == name for s in sessions):
        return web.Response(status=404, text="unknown session")
    return _html(_render_session(name))


@web.middleware
async def lock_middleware(request: web.Request, handler):
    """What turns the lock screen into a lock.

    A locked browser still holds a perfectly good identity cookie, so nothing
    in the handlers below would stop it reaching a desktop — /kiosk/session,
    /connect, /desktop/<id>/, the display WebSocket, /screenshot. This refuses
    all of it at the door on the *presence of the lock cookie*, which the
    locked page cannot clear (HttpOnly) and no URL can express. Only the
    password clears it.

    Static assets and /healthz are exempt: the lock screen is made of the
    former, and the latter is the kubelet's, not a user's."""
    if _locked(request) is None:
        return await handler(request)
    path = request.path
    if path in _LOCK_ALLOWED or path == "/healthz" or path.startswith("/static/"):
        return await handler(request)
    # A navigation is sent to the screen the person is actually looking at;
    # everything else — fetch, the display WebSocket, an <img> — gets a status
    # it can act on, since redirecting those to an HTML page just produces a
    # confusing parse error at the other end. Sec-Fetch-Mode is the browser's
    # own answer to "is this a navigation"; Accept is the fallback for clients
    # that do not send it.
    if (request.headers.get("Sec-Fetch-Mode") == "navigate"
            or "text/html" in request.headers.get("Accept", "")):
        raise web.HTTPSeeOther(LOCK_PATH)
    return web.Response(status=423, text="kiosk is locked")


def add_routes(app: web.Application) -> None:
    """Register the kiosk onto the viewer app. Same app, and therefore the same
    origin as /connect, /desktop, /vnc and /screenshot — which the session page
    and the card thumbnails both depend on, and which is also what puts them all
    behind one lock."""
    app.add_routes([
        web.get("/kiosk", kiosk_index),
        web.get(LOCK_PATH, kiosk_lock),
        web.post("/kiosk/login", kiosk_login),
        web.get(OTP_PATH, kiosk_otp),
        web.post(OTP_PATH, kiosk_otp_verify),
        web.post("/kiosk/logout", kiosk_logout),
        web.get("/kiosk/sessions", kiosk_sessions),
        web.get("/kiosk/session/{id}", kiosk_session),
    ])
