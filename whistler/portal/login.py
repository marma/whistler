"""The sign-in screen — one form, shared by the kiosk and the management portal.

The kiosk grew this screen first (whistler/portal/kiosk.py), and there is no
reason for the portal to have a second one: the two surfaces differ in what they
*offer* a signed-in user, not in how a person says who they are. So the form,
its dark furniture and — more importantly — the one function that accepts or
refuses a credential live here, and both surfaces call them.

What each surface adds on top is its own:

* **The kiosk** puts a mocked TOTP step between this form and its identity
  cookie (see kiosk.py, "The second factor"), because a screen standing in a
  corridor is the case that wants a second factor most.
* **The portal** does not. It is the management UI, reached from a workstation,
  and a mocked second factor there would buy nothing but a screen to click
  through — the real one arrives with the real credential store, for both
  surfaces at once, and ``verify_credentials`` is where that lands.

**Passwords** (2026-10-11) are whistler/passwords.py: a hashed record per
user in a Secret, starting as a temporary password that must be replaced at
first sign-in. ``check_password`` is the one decision both surfaces ask; while
``WHISTLER_AUTH_ALLOW_ANY`` is on it accepts anything and asks nothing.

**Cookies are signed** (``sign``/``unsign``): before, the identity cookie was
the bare user name, so a password check in front of it would have guarded
nothing anyone could not type into their browser's cookie jar.
"""
import hashlib
import hmac
import html
import logging
import os
import secrets
import time

from whistler import passwords

logger = logging.getLogger("whistler.portal")

# The identity cookie, read by every surface: the viewer app's auth middleware,
# the kiosk, and the management portal. Defined here — beside the sign-in that
# sets it — rather than in any one of them, so none has to import another for a
# string (whistler/portal/app.py and kiosk.py used to import each other for it).
USER_COOKIE = "whistler_user"

# Shared by every hand-rolled page on the viewer app (login, lock, OTP, kiosk
# grid, session). Lives here because the login screen is the page all of them
# were styled to match.
BASE_CSS = """
  *{box-sizing:border-box}
  html,body{margin:0;height:100%;background:#1a1a1a;color:#e8e8e8;
            font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif;
            -webkit-font-smoothing:antialiased}
  a{color:inherit;text-decoration:none}
  h1{font-size:1.6rem;font-weight:600;letter-spacing:.01em;margin:0}
  .muted{color:#8a8a8a}
"""

_LOGIN_HTML = """<!doctype html><meta charset=utf-8><title>__TITLE__</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<link rel=icon type=image/svg+xml href=/static/favicon.svg>
<style>__BASE_CSS__
  body{display:flex;align-items:center;justify-content:center;padding:2rem}
  form{width:100%;max-width:22rem;display:flex;flex-direction:column;gap:1rem}
  h1{text-align:center;margin-bottom:.5rem}
  label{display:flex;flex-direction:column;gap:.35rem;font-size:.82rem;
        text-transform:uppercase;letter-spacing:.08em;color:#9a9a9a}
  input{font:inherit;text-transform:none;letter-spacing:normal;color:#e8e8e8;
        background:#242424;border:1px solid #383838;border-radius:6px;
        padding:.6rem .7rem}
  input:focus{outline:none;border-color:#4b8bd6;background:#282828}
  button{font:inherit;font-weight:600;color:#fff;background:#2f6fb5;border:0;
         border-radius:6px;padding:.65rem;cursor:pointer}
  button:hover{background:#3b81cd}
  .error{background:#3a1e1e;border:1px solid #6e2b2b;border-radius:6px;
         padding:.55rem .7rem;font-size:.88rem;color:#f3b6b6}
  .note{font-size:.78rem;text-align:center;margin:0}
</style>
<form method=post action=__ACTION__>
  <h1>__HEADING__</h1>
  __ERROR__
  __HIDDEN__
  <label>User<input name=user autocomplete=username autofocus required></label>
  <label>Password<input name=password type=password autocomplete=current-password></label>
  <button type=submit>Sign in</button>
  <p class="note muted">__NOTE__</p>
</form>"""


# A refusal, in the same furniture as the form. Used where a surface has to
# turn somebody away for *who they are* rather than for a wrong password — a
# kiosk-bound user reaching the management portal, or a portal user reaching the
# kiosk. It links to the surface they do have rather than redirecting there:
# behind the bundled proxy both live on one origin, but in a split-port dev run
# they do not, and a redirect would land on a 404 instead of an explanation.
_NOTICE_HTML = """<!doctype html><meta charset=utf-8><title>__TITLE__</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<link rel=icon type=image/svg+xml href=/static/favicon.svg>
<style>__BASE_CSS__
  body{display:flex;align-items:center;justify-content:center;padding:2rem}
  .box{width:100%;max-width:26rem;text-align:center;display:flex;
       flex-direction:column;gap:1rem}
  p{margin:0}
  a.go{align-self:center;font-weight:600;color:#fff;background:#2f6fb5;
       border-radius:6px;padding:.6rem 1.1rem}
  a.go:hover{background:#3b81cd}
</style>
<div class=box>
  <h1>__HEADING__</h1>
  <p class=muted>__MESSAGE__</p>
  __LINK__
</div>"""


def render_notice(*, heading: str, message: str, href: str = None,
                  link_label: str = None, title: str = None) -> str:
    link = ""
    if href:
        link = (f'<a class=go href="{html.escape(href, quote=True)}">'
                f'{html.escape(link_label or href)}</a>')
    return (_NOTICE_HTML
            .replace("__BASE_CSS__", BASE_CSS)
            .replace("__TITLE__", html.escape(title or heading))
            .replace("__HEADING__", html.escape(heading))
            .replace("__MESSAGE__", html.escape(message))
            .replace("__LINK__", link))


def portal_home() -> str:
    """Where a "Go to the portal" link points. Same-origin "/" unless the
    kiosk has a host of its own (kiosk.host), where "/" is the kiosk again
    and the portal is on another origin. The chart sets it scheme-relative
    (//host), so it keeps whatever scheme the browser is on."""
    return os.environ.get("WHISTLER_PORTAL_URL", "").rstrip("/") + "/"


def kiosk_enabled() -> bool:
    """Whether this install serves the kiosk at all (chart: kiosk.enabled).
    Unset is on, so a dev run and the tests keep the surface."""
    return os.environ.get("WHISTLER_KIOSK_ENABLED", "true").lower() != "false"


def kiosk_home():
    """Where a "Go to the kiosk" link points, the mirror of portal_home, or
    None when there is no kiosk to go to."""
    if not kiosk_enabled():
        return None
    return os.environ.get("WHISTLER_KIOSK_URL", "").rstrip("/") + "/kiosk"


def dev_auth() -> bool:
    """Whether the portal's dev auth gate is open. Read per request, not at
    import, so tests and a restarted process agree."""
    return os.environ.get("WHISTLER_AUTH_ALLOW_ANY") == "true"


_THROTTLE = passwords.Throttle()


def check_password(cm, user: str, password: str) -> str:
    """The single point where a sign-in is accepted or refused: passwords.OK,
    passwords.CHANGE (a right temporary password — the caller must have a
    real one chosen before it issues a session) or passwords.REFUSED.

    In dev mode any password is accepted for any name and nobody is asked to
    change anything. Otherwise the user's stored record decides, behind a
    per-name throttle. Blocking (a Secret read and an scrypt), so async
    callers run it in an executor."""
    if not user:
        return passwords.REFUSED
    if dev_auth():
        return passwords.OK
    if _THROTTLE.wait(user) > 0:
        logger.warning(f"Sign-in for {user!r} throttled")
        return passwords.REFUSED
    verdict = passwords.check(cm.get_password_record(user), password)
    if verdict == passwords.REFUSED:
        _THROTTLE.failed(user)
    else:
        _THROTTLE.succeeded(user)
    return verdict


def verify_credentials(cm, user: str, password: str) -> bool:
    """check_password for the callers that only re-confirm a person — the
    kiosk lock, the current password on a change form — where a temporary
    password cannot be the answer: it only ever leads to choosing one."""
    return check_password(cm, user, password) == passwords.OK


def default_note() -> str:
    """What the form admits about itself. A page that asks for a password owes
    the person in front of it the truth about whether one is being checked."""
    if dev_auth():
        return "Development mode — any password is accepted."
    return "Forgot your password? An administrator can reset it."


# --------------------------------------------------------------------------- #
# Signed cookies. Every cookie that says who someone is carries an HMAC of the #
# name, its purpose and when it was issued, under WHISTLER_SESSION_KEY (a      #
# Secret the chart keeps across upgrades) — so it cannot be written by hand,   #
# and a kiosk sign-in cannot be replayed as a portal one.                      #
# --------------------------------------------------------------------------- #

PURPOSE_USER = "user"          # the identity both apps read
PURPOSE_PORTAL = "portal"      # marks a management sign-in
PURPOSE_KIOSK = "kiosk"        # marks a kiosk sign-in
PURPOSE_PENDING = "pending"    # password done, kiosk code still to come
PURPOSE_CHANGE = "change"      # temporary password accepted: choose one now

# Carries PURPOSE_CHANGE between a temporary password and the form that
# replaces it, on either surface.
CHANGE_COOKIE = "whistler_pwchange"

# Short-lived purposes; the rest last a session.
_MAX_AGE = {PURPOSE_PENDING: 600, PURPOSE_CHANGE: 900}

_PROCESS_KEY = secrets.token_bytes(32)
_warned_key = False


def _session_key() -> bytes:
    global _warned_key
    key = os.environ.get("WHISTLER_SESSION_KEY")
    if key:
        return key.encode()
    if not _warned_key:
        _warned_key = True
        logger.warning("WHISTLER_SESSION_KEY is not set: sessions are signed "
                       "with a per-process key and end when the portal "
                       "restarts")
    return _PROCESS_KEY


def session_max_age() -> int:
    try:
        return int(os.environ.get("WHISTLER_SESSION_MAX_AGE", "43200"))
    except ValueError:
        return 43200


def _mac(user: str, purpose: str, issued: int) -> str:
    return hmac.new(_session_key(), f"{purpose}:{user}:{issued}".encode(),
                    hashlib.sha256).hexdigest()


def sign(user: str, purpose: str = PURPOSE_USER,
         now: float = None) -> str:
    issued = int(time.time() if now is None else now)
    return f"{user}:{issued}:{_mac(user, purpose, issued)}"


def unsign(value, purpose: str = PURPOSE_USER, now: float = None):
    """The name a cookie was signed for, or None: forged, for another
    purpose, malformed, or older than its purpose allows."""
    if not value or not isinstance(value, str):
        return None
    try:
        user, issued, mac = value.rsplit(":", 2)
        issued = int(issued)
    except ValueError:
        return None
    if not user or not hmac.compare_digest(mac, _mac(user, purpose, issued)):
        return None
    now = time.time() if now is None else now
    if now - issued > _MAX_AGE.get(purpose, session_max_age()):
        return None
    return user


# --------------------------------------------------------------------------- #
# Choosing a password: after a temporary one (both surfaces), or by choice     #
# (the portal's account page).                                                 #
# --------------------------------------------------------------------------- #

_CHANGE_HTML = """<!doctype html><meta charset=utf-8><title>__TITLE__</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<link rel=icon type=image/svg+xml href=/static/favicon.svg>
<style>__BASE_CSS__
  body{display:flex;align-items:center;justify-content:center;padding:2rem}
  form{width:100%;max-width:22rem;display:flex;flex-direction:column;gap:1rem}
  h1{text-align:center;margin-bottom:.25rem}
  label{display:flex;flex-direction:column;gap:.35rem;font-size:.82rem;
        text-transform:uppercase;letter-spacing:.08em;color:#9a9a9a}
  input{font:inherit;text-transform:none;letter-spacing:normal;color:#e8e8e8;
        background:#242424;border:1px solid #383838;border-radius:6px;
        padding:.6rem .7rem}
  input:focus{outline:none;border-color:#4b8bd6;background:#282828}
  button{font:inherit;font-weight:600;color:#fff;background:#2f6fb5;border:0;
         border-radius:6px;padding:.65rem;cursor:pointer}
  button:hover{background:#3b81cd}
  .error{background:#3a1e1e;border:1px solid #6e2b2b;border-radius:6px;
         padding:.55rem .7rem;font-size:.88rem;color:#f3b6b6}
  .note{font-size:.85rem;text-align:center;margin:0}
</style>
<form method=post action=__ACTION__>
  <h1>Choose a password</h1>
  <p class="note muted">__INTRO__</p>
  __ERROR__
  __HIDDEN__
  <input type=hidden name=user value="__USER__" autocomplete=username>
  __CURRENT__
  <label>New password<input name=password type=password autocomplete=new-password minlength=__MIN__ required autofocus></label>
  <label>Again<input name=confirm type=password autocomplete=new-password minlength=__MIN__ required></label>
  <button type=submit>Save password</button>
</form>"""


def render_change_password(*, action: str, user: str, error: str = None,
                           ask_current: bool = False, hidden: dict = None,
                           title: str = "Whistler") -> str:
    intro = (f"Signed in as {user}." if ask_current else
             f"The password {user} signed in with was temporary. Choose your "
             f"own to continue.")
    intro += f" At least {passwords.MIN_LENGTH} characters."
    current = ("<label>Current password<input name=current type=password "
               "autocomplete=current-password required></label>"
               if ask_current else "")
    fields = "".join(
        f'<input type=hidden name="{html.escape(str(k), quote=True)}" '
        f'value="{html.escape(str(v), quote=True)}">'
        for k, v in (hidden or {}).items() if v)
    err = (f"<div class=error>{html.escape(error)}</div>" if error else "")
    return (_CHANGE_HTML
            .replace("__BASE_CSS__", BASE_CSS)
            .replace("__TITLE__", html.escape(title))
            .replace("__ACTION__", html.escape(action, quote=True))
            .replace("__INTRO__", html.escape(intro))
            .replace("__ERROR__", err)
            .replace("__HIDDEN__", fields)
            .replace("__USER__", html.escape(user, quote=True))
            .replace("__CURRENT__", current)
            .replace("__MIN__", str(passwords.MIN_LENGTH)))


def render_login(*, action: str, error: str = None, note: str = None,
                 hidden: dict = None, title: str = "Whistler",
                 heading: str = "Whistler") -> str:
    """The form. `hidden` carries whatever the caller has to round-trip through
    it — the portal's ?next=, for one — as hidden inputs."""
    fields = "".join(
        f'<input type=hidden name="{html.escape(str(k), quote=True)}" '
        f'value="{html.escape(str(v), quote=True)}">'
        for k, v in (hidden or {}).items() if v)
    err = (f"<div class=error>{html.escape(error)}</div>" if error else "")
    return (_LOGIN_HTML
            .replace("__BASE_CSS__", BASE_CSS)
            .replace("__ACTION__", html.escape(action, quote=True))
            .replace("__TITLE__", html.escape(title))
            .replace("__HEADING__", html.escape(heading))
            .replace("__ERROR__", err)
            .replace("__HIDDEN__", fields)
            .replace("__NOTE__", html.escape(
                default_note() if note is None else note)))
