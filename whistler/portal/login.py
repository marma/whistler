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

**There is still nothing to check a password against.** Whistler stores no
passwords — ``User`` CRs carry public keys — so while ``WHISTLER_AUTH_ALLOW_ANY``
is on any password is accepted for any name, and outside it nothing is. The page
says which of those it is doing rather than implying an assurance it has not
got. When a real credential lands (a ``passwordHash`` on the User CR, an OIDC
handoff) it goes in ``verify_credentials`` and both surfaces gain it together;
keeping that decision in one function is the point of the function.
"""
import html
import os

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


def dev_auth() -> bool:
    """Whether the portal's dev auth gate is open. Read per request, not at
    import, so tests and a restarted process agree."""
    return os.environ.get("WHISTLER_AUTH_ALLOW_ANY") == "true"


def verify_credentials(user: str, password: str) -> bool:
    """The single point where a sign-in is accepted or refused.

    There is nothing to check against yet: Whistler stores no passwords, so in
    dev mode any password is accepted for any name, and outside it nothing is.
    When a real credential lands — a ``passwordHash`` on the User CR, or an
    OIDC handoff — it goes here, and the kiosk's second factor goes here beside
    it. Keeping the decision in one function is the point of the function."""
    if not user:
        return False
    return dev_auth()


def default_note() -> str:
    """What the form admits about itself. A page that asks for a password owes
    the person in front of it the truth about whether one is being checked."""
    if dev_auth():
        return "Development mode — any password is accepted."
    return "No credential store is configured — sign-in is unavailable."


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
