"""FastAPI management app: admin + user web UI for Whistler.

Routes
------
/               user dashboard — list instances and available templates
/instances/*    user instance lifecycle (create, connect, stop, delete)
/admin/*        admin-only: templates, users, volumes, all sessions

/login          the sign-in screen — the kiosk's form, without its second
                factor (whistler/portal/login.py)
/logout         drop the cookies, back to /login

Auth (dev-only)
---------------
There is a real form now, and still no credential store behind it: with
WHISTLER_AUTH_ALLOW_ANY=true any password is accepted for any name and the page
says so, and without it nothing is accepted at all. ``verify_credentials`` in
whistler/portal/login.py is the single place a real check lands, shared with the
kiosk. Signing in sets the identity cookie the viewer app already reads, so
/connect, /term and /screenshot authorize the same person without a ?user= on
every asset request.

?user=<name> (or the X-Whistler-User header) stays as the dev shortcut past the
form — every internal link carries it, and the integration tests and the
skaffold loop are built on it.

Set WHISTLER_ADMIN_USERS=alice,bob to grant admin to specific users, or
WHISTLER_AUTH_ALLOW_ADMIN=true to treat every authenticated user as admin (dev).
"""
import asyncio
import ipaddress
import logging
import os
import re
from typing import Annotated, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from whistler.config import (ACCESS_MODES, CHANNELS, ConfigWriteError,
                             ENFORCED_CHANNELS, GPU_NONE,
                             ENTRY_KIOSK, ENTRY_POINTS, ENTRY_PORTAL,
                             GPU_NODE_LABEL, NEW_USER_ENTRY_POINTS,
                             NEW_USER_ZONES, OVERRIDE_GROUPS)
from whistler.portal.login import (USER_COOKIE, dev_auth, render_login,
                                   render_notice, verify_credentials)
from whistler.status import GROUP_COLORS, status_group

logger = logging.getLogger("whistler.management")

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
_STATIC_DIR   = os.path.join(os.path.dirname(__file__), "static")

templates = Jinja2Templates(directory=_TEMPLATE_DIR)

# The user-facing states live in whistler/status.py — the launcher collapses
# the same phases when it decides whether a row can be connected to or has to
# be started first, and one table beats two that drift.
templates.env.globals["status_label"] = status_group
templates.env.globals["status_color"] = lambda s, ready=True: GROUP_COLORS[status_group(s, ready)]
templates.env.globals["GPU_NODE_LABEL"] = GPU_NODE_LABEL
# The "No GPU" sentinel every GPU-type picker offers. A global rather than a
# per-route context value because three templates need it and a picker that
# omitted it would silently lose the option.
templates.env.globals["GPU_NONE"] = GPU_NONE

_ADMIN_USERS: set[str] = set(
    u.strip() for u in os.environ.get("WHISTLER_ADMIN_USERS", "").split(",") if u.strip()
)
_ALLOW_ADMIN = os.environ.get("WHISTLER_AUTH_ALLOW_ADMIN", "false").lower() == "true"


# --------------------------------------------------------------------------- #
# Auth helpers                                                                 #
# --------------------------------------------------------------------------- #

LOGIN_PATH = "/login"

# Marks a browser that signed in *to the management portal*. Identity itself
# rides USER_COOKIE, shared with the viewer app so the pages this one links into
# — /connect, /term, /vnc, /screenshot — authorize the same person without a
# ?user= on requests the browser makes on its own.
#
# Deliberately not the kiosk's marker. A kiosk sign-in must not hand someone the
# management UI: that is the entire point of a kiosk (kiosk.py, "It is the
# surface, not the binding"), so each surface has its own marker and the shared
# identity cookie by itself opens neither. It is not a *binding* yet either —
# nothing stops a kiosk user signing in here with the same name and password —
# but it does mean the two surfaces are entered separately and one cookie is not
# a pass to both.
PORTAL_COOKIE = "whistler_portal"


class LoginRequired(Exception):
    """No identity on the request. Turned into the login screen (or a 401 for a
    request that could not render one) by ``_login_required``."""


class EntryPointDenied(Exception):
    """A known user, refused this whole surface: they hold no `portal` entry
    point (design/security.md, "Closing the fourth axis"). Not a 401 — signing
    in again cannot help, and offering the form again would suggest it might."""


def _get_identity(request: Request) -> Optional[str]:
    """Who this request is, or None to send them to the login screen.

    There is no fallback identity any more: this used to answer "user" for
    anyone at all while the dev gate was open, which is right for a portal with
    no login and wrong for one that has a form — nobody would ever be logged
    out. The ?user= / X-Whistler-User shortcut stays, dev-gated as before."""
    if dev_auth():
        explicit = (request.headers.get("X-Whistler-User")
                    or request.query_params.get("user"))
        if explicit:
            return explicit.split("-")[0]
    if request.cookies.get(PORTAL_COOKIE) != "1":
        return None
    raw = request.cookies.get(USER_COOKIE)
    return raw.split("-")[0] if raw else None


def require_user(request: Request):
    """Identity, then the door.

    The entry-point check lives here rather than in each route because "here"
    is every route: this app *is* the portal entry point, so a kiosk-bound user
    must not reach the dashboard, an admin page, a home volume or an htmx
    fragment of any of them. One User CR read per request, the same cost as the
    admin check beside it."""
    user = _get_identity(request)
    if not user:
        raise LoginRequired()
    if not request.app.state.cm.may_enter(user, ENTRY_PORTAL):
        raise EntryPointDenied()
    return user


def _is_admin(request: Request, user: str) -> bool:
    if _ALLOW_ADMIN or user in _ADMIN_USERS:
        return True
    return bool(request.app.state.cm.is_user_admin(user))


def require_admin(request: Request):
    user = require_user(request)
    if not _is_admin(request, user):
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user


# --------------------------------------------------------------------------- #
# Dependency: config manager injected by build_management_app()               #
# --------------------------------------------------------------------------- #

def _cm_dep(request: Request):
    return request.app.state.cm


CM   = Annotated[object, Depends(_cm_dep)]
User = Annotated[str, Depends(require_user)]
Admin = Annotated[str, Depends(require_admin)]


def _is_admin_dep(request: Request, user: User) -> bool:
    return _is_admin(request, user)


IsAdmin = Annotated[bool, Depends(_is_admin_dep)]


def _ctx(user: str, is_admin: bool = False, **extra) -> dict:
    """Build template context (request is passed separately to TemplateResponse).
    Callers gated by `Admin` already know is_admin is True; callers gated only
    by `User` should resolve it via the `IsAdmin` dependency (a CR lookup) and
    pass it through, since this helper runs inline in async routes and can't
    itself make a blocking call."""
    return {"current_user": user, "is_admin": is_admin, **extra}


def _tr(url: str, user: str) -> RedirectResponse:
    sep = "&" if "?" in url else "?"
    return RedirectResponse(f"{url}{sep}user={user}", status_code=303)

# --------------------------------------------------------------------------- #
# Sign in / sign out                                                           #
# --------------------------------------------------------------------------- #

def _safe_next(path) -> str:
    """Where signing in should land. Only a path on this app is accepted —
    which also disposes of protocol-relative "//host" and absolute URLs, since
    neither starts with a single "/" — and anything else falls back to the
    dashboard rather than being refused, because a stale bookmark should still
    get someone in."""
    if not path or not isinstance(path, str) or len(path) > 512:
        return "/"
    if any(c in path for c in "\r\n\\") or not path.startswith("/"):
        return "/"
    if path.startswith("//") or path.startswith(LOGIN_PATH):
        return "/"
    return path


def _next_url(path: str, user: str) -> str:
    """The signed-in name wins over whatever ?user= the target already carried:
    `next` may well be a link made while someone else was signed in, and
    ``query_params.get`` returns the *first* value, so a stale one has to be
    dropped rather than appended beside."""
    parts = urlsplit(path)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if k != "user"]
    query.append(("user", user))
    return urlunsplit(("", "", parts.path, urlencode(query), parts.fragment))


def _login_page(next_to: str = "/", error: str = None,
                status_code: int = 200) -> HTMLResponse:
    """The kiosk's form, rendered by the kiosk's renderer. No-store because the
    page is identity-scoped and on a shared machine the Back button is part of
    the problem."""
    return HTMLResponse(
        render_login(action=LOGIN_PATH, error=error,
                     hidden={"next": "" if next_to == "/" else next_to}),
        status_code=status_code, headers={"Cache-Control": "no-store"})


async def login_form(request: Request):
    """Show the form — or skip it, for a browser that is already someone."""
    next_to = _safe_next(request.query_params.get("next"))
    user = _get_identity(request)
    if user:
        return RedirectResponse(_next_url(next_to, user), status_code=303)
    return _login_page(next_to)


async def login_submit(
    request: Request,
    user:     Annotated[str, Form()] = "",
    password: Annotated[str, Form()] = "",
    next:     Annotated[str, Form()] = "",
):
    """Check the credential, and — unlike the kiosk — sign in on the strength
    of it. The second factor there guards a screen that stands in a corridor;
    this is the management UI on a workstation, and a mocked code prompt in
    front of it would be a click, not a control. When the real one arrives it
    arrives in login.py, for both surfaces."""
    name = user.strip().split("-")[0]
    next_to = _safe_next(next)
    if not verify_credentials(name, password):
        logger.warning(f"Portal login refused for {user!r}")
        return _login_page(next_to, "Sign-in failed.", status_code=401)
    # The password was right and the surface is still wrong. Say which, and
    # sign nobody in: a cookie for a surface this account cannot use would only
    # produce the same refusal one navigation later.
    if not await request.app.state.run(
            request.app.state.cm.may_enter, name, ENTRY_PORTAL):
        logger.warning(f"Portal login refused for {name}: no "
                       f"'{ENTRY_PORTAL}' entry point")
        return _entry_denied_page()
    logger.info(f"Portal login for {name}")
    response = RedirectResponse(_next_url(next_to, name), status_code=303)
    response.set_cookie(USER_COOKIE, name, path="/", samesite="lax")
    response.set_cookie(PORTAL_COOKIE, "1", path="/", samesite="lax",
                        httponly=True)
    return response


async def logout(request: Request):
    """Drop both cookies. The identity one is shared with the viewer app, so
    signing out of the portal also ends the desktop pages it linked to."""
    response = RedirectResponse(LOGIN_PATH, status_code=303)
    response.delete_cookie(USER_COOKIE, path="/")
    response.delete_cookie(PORTAL_COOKIE, path="/")
    return response


def _entry_denied_page() -> HTMLResponse:
    """What a kiosk-bound account is told when it arrives at the portal. A page
    rather than a redirect to /kiosk: behind the bundled proxy the kiosk is one
    origin away, in a split-port dev run it is not, and a 303 would land on a
    404 instead of an explanation."""
    return HTMLResponse(
        render_notice(
            heading="Kiosk only",
            message="This account may only be used through the kiosk. The "
                    "management portal, the web terminal and SSH are not "
                    "available to it.",
            href="/kiosk", link_label="Go to the kiosk"),
        status_code=403, headers={"Cache-Control": "no-store"})


async def _entry_point_denied(request: Request, exc: Exception):
    """Every route on this app funnels here through require_user."""
    if (request.headers.get("Sec-Fetch-Mode") == "navigate"
            or "text/html" in request.headers.get("Accept", "")):
        return _entry_denied_page()
    return JSONResponse({"detail": f"No '{ENTRY_PORTAL}' entry point."},
                        status_code=403)


async def _login_required(request: Request, exc: Exception):
    """What an unauthenticated request gets.

    A navigation is sent to the form, with where it was going in tow; anything
    else — an htmx status poll, a fetch — gets a status it can act on, because
    redirecting those to an HTML page produces a parse error at the far end
    instead of a login. Same split the kiosk's lock middleware makes."""
    if (request.headers.get("Sec-Fetch-Mode") == "navigate"
            or "text/html" in request.headers.get("Accept", "")):
        query = request.url.query
        next_to = _safe_next(request.url.path + (f"?{query}" if query else ""))
        target = LOGIN_PATH
        if next_to != "/":
            target += "?" + urlencode({"next": next_to})
        return RedirectResponse(target, status_code=303)
    return JSONResponse({"detail": f"Not signed in. Sign in at {LOGIN_PATH}."},
                        status_code=401)




def _render_status_html(name: str, status: str, user: str, controls: bool,
                        connect_url: str = None, term_url: str = None,
                        ready: bool = True, editable: bool = False,
                        console_url: str = None, can_override: bool = False) -> str:
    """Render the polling status badge. With `controls`, also emit an out-of-band
    swap that re-renders the action buttons (connect/ssh/start/stop/edit) so they
    stay enabled/disabled in step with the status (used on the dashboard; the
    detail view omits it).

    `can_override` has to travel with them for the same reason the console URL
    does: these buttons are re-rendered on every poll, so a flag left out here
    silently reverts the play button to a plain one-click start the first time
    the row refreshes."""
    tpl = "user/_status_controls.html" if controls else "user/_status_badge.html"
    return templates.env.get_template(tpl).render(
        name=name, status=status, user=user, controls=controls,
        connect_url=connect_url, term_url=term_url, ready=ready, editable=editable,
        console_url=console_url, can_override=can_override,
    )


# Both kinds of session are now one `Session` CR (ssh / desktop mode); the user
# dashboard lists them in a single table. Desktop sessions connect in the browser
# through the viewer app (whistler.portal.app), which runs on its own port — so
# the "Connect" link needs that app's base URL. WHISTLER_DESKTOP_PORTAL_URL
# overrides it (e.g. "https://desktops.example.com"); empty means same-origin
# (works when the viewer is reachable on the same host, e.g. behind one ingress).
_DESKTOP_PORTAL_URL = os.environ.get("WHISTLER_DESKTOP_PORTAL_URL", "").rstrip("/")


def _desktop_viewer_url(user: str, name: str) -> str:
    return f"{_DESKTOP_PORTAL_URL}/connect/{name}?user={user}"


def _terminal_url(user: str, name: str) -> str:
    """Web-terminal (xterm.js) page, served by the same viewer app as the desktop
    relay — so it shares _DESKTOP_PORTAL_URL (empty = same-origin via the proxy)."""
    return f"{_DESKTOP_PORTAL_URL}/term/{name}?user={user}"


# Where a start can be headed. The desktop, the web terminal and the machine
# console all *start* a stopped instance on their way in (the viewer app fires
# the same reconcile nudge), so each is a moment the start dialog has to be
# able to precede — and then hand the browser on to. Keyed by the name the
# button puts in `?then=`, so an unknown value simply isn't a door.
_START_DESTINATIONS = {
    "desktop":  lambda user, name: _desktop_viewer_url(user, name),
    "terminal": lambda user, name: _terminal_url(user, name),
    "console":  lambda user, name: _console_url(user, name),
}


def _console_url(user: str, name: str) -> str:
    """The VM's *hardware* console (KubeVirt VNC subresource) — the emulated
    display from power-on: firmware, bootloader, kernel messages, the login
    prompt. Distinct from the desktop stream, which is a userspace X capture
    and only exists once the session is up.

    **Admin-only.** Not because it is more powerful than the desktop — it is
    the diagnostic view when everything else is broken — but because it is not
    a view a user can act on: boot output is rarely where the problem is
    (that is usually Kubernetes-side), and one console per machine means it is
    inherently unscoped on any instance with more than one member."""
    return f"{_DESKTOP_PORTAL_URL}/console/{name}?user={user}"


def _merge_sessions(instances: list, desktop_sessions: list, user: str,
                    is_admin: bool = False) -> list[dict]:
    """Flatten ssh instances + desktop sessions into one list of rows with a
    common shape (name/template/status/mode), so the dashboard can render them in
    a single table. Desktop rows carry the browser viewer URL. Every row gets a
    web-terminal URL (VM sessions get the KubeVirt serial console)."""
    rows: list[dict] = []
    for i in instances:
        # An ssh instance can be a VM (images/devbase). Containers have no
        # firmware and no emulated display, but a VM does — so an ssh-mode VM
        # gets the machine console for admins, like the desktop VM rows below.
        is_vm = i.get("runtime") == "vm"
        rows.append({
            "name": i["name"], "template": i.get("template"),
            "status": i.get("status"), "ready": i.get("ready", True),
            "mode": "ssh", "connect_url": None,
            "term_url": _terminal_url(user, i["name"]),
            "console_url": (_console_url(user, i["name"])
                            if is_vm and is_admin else None),
        })
    for s in desktop_sessions:
        is_vm = s.get("runtime") == "vm"
        rows.append({
            "name": s["name"], "template": s.get("template"),
            "status": s.get("phase"), "ready": True, "mode": "desktop",
            "connect_url": _desktop_viewer_url(user, s["name"]),
            "term_url": _terminal_url(user, s["name"]),
            "console_url": (_console_url(user, s["name"])
                            if is_vm and is_admin else None),
        })
    rows.sort(key=lambda r: r["name"])
    return rows


async def _user_templates(request: Request, cm, user: str) -> list:
    """Every template this user may launch, ssh and desktop alike — one list,
    since the picker offers them together and the create form resolves the
    chosen name against the same set."""
    ssh_tpls, desk_tpls = await asyncio.gather(
        request.app.state.run(cm.get_user_templates, user),
        request.app.state.run(cm.get_user_desktop_templates, user),
    )
    return ssh_tpls + desk_tpls


async def _override_form_context(request: Request, cm, user: str) -> dict:
    """The context every override form needs: the user's grants, plus the
    catalogs filtered to what they hold.

    Three surfaces ask for this — create, edit, and the start dialog — and the
    filtering is the part that must not drift between them: the pickers offer
    only what _apply_policy would accept, so a user granted no zone is offered
    none rather than being shown the whole catalog and refused at reconcile.
    Every allow is explicit (2026-08-25), which is why there is no "empty means
    any" branch here."""
    (gpu_types, allowed_gpu_types,
     overrides, zones, allowed_zones) = await asyncio.gather(
        request.app.state.run(cm.get_gpu_types),
        request.app.state.run(cm.get_user_allowed_gpu_types, user),
        request.app.state.run(cm.get_user_overrides, user),
        request.app.state.run(cm.get_zones),
        request.app.state.run(cm.get_user_allowed_zones, user),
    )
    return {
        "gpu_types": [g for g in gpu_types if g in allowed_gpu_types],
        "allowed_gpu_types": allowed_gpu_types,
        "overrides": overrides,
        "zones": [z for z in zones if z in allowed_zones],
    }


def _may_override(grants: dict) -> bool:
    """Whether this user can change anything for a run. Decides both the shape
    of the play button and whether the start dialog has a question to ask; with
    no grant at all, start stays one click."""
    return any((grants or {}).values())


# --------------------------------------------------------------------------- #
# User — dashboard                                                             #
# --------------------------------------------------------------------------- #

async def user_index(request: Request, cm: CM, user: User, is_admin: IsAdmin):
    instances, desktop_sessions, grants = await asyncio.gather(
        request.app.state.run(cm.get_user_instances, user),
        request.app.state.run(cm.get_user_desktop_sessions, user),
        request.app.state.run(cm.get_user_overrides, user),
    )
    return templates.TemplateResponse(
        request=request, name="user/index.html",
        context=_ctx(user, is_admin=is_admin,
                     instances=_merge_sessions(instances, desktop_sessions, user,
                                               is_admin=is_admin),
                     can_override=_may_override(grants)),
    )


async def instance_template_picker(request: Request, cm: CM, user: User,
                                   is_admin: IsAdmin):
    """The template catalog, as a modal fragment. It left the dashboard when
    "New Instance" grew a picker — the list of machines you have should not be
    sitting on top of the list of machines you could make."""
    return templates.TemplateResponse(
        request=request, name="user/_template_picker.html",
        context=_ctx(user, is_admin=is_admin,
                     tpls=await _user_templates(request, cm, user)),
    )


# --------------------------------------------------------------------------- #
# User — instance CRUD                                                         #
# --------------------------------------------------------------------------- #

async def instance_create_form(request: Request, cm: CM, user: User, is_admin: IsAdmin):
    tpls, form_ctx, home_volumes_ = await asyncio.gather(
        _user_templates(request, cm, user),
        _override_form_context(request, cm, user),
        request.app.state.run(cm.get_home_volumes, user),
    )
    # Normally the picker sent us here with a template already chosen, so the
    # form shows what was picked. An unresolvable (or absent) ?template= falls
    # back to the select rather than 404ing — a stale bookmark should still be
    # able to create something.
    wanted = request.query_params.get("template")
    selected_tpl = next((t for t in tpls
                         if t.get("fullName") == wanted or t.get("name") == wanted),
                        None) if wanted else None
    return templates.TemplateResponse(
        request=request, name="user/create_instance.html",
        context=_ctx(user, is_admin=is_admin, tpls=tpls,
                     selected_template=wanted, selected_tpl=selected_tpl,
                     home_volumes=home_volumes_, current_home_volume=None,
                     **form_ctx),
    )


async def instance_create(
    request: Request, cm: CM, user: User,
    template_name: Annotated[str, Form()],
    instance_name: Annotated[str, Form()],
    preemptible:   Annotated[Optional[str], Form()] = None,
    home_volume:   Annotated[Optional[str], Form()] = None,
    override_cpu:          Annotated[Optional[str], Form()] = None,
    override_memory:       Annotated[Optional[str], Form()] = None,
    override_gpu_type:     Annotated[Optional[str], Form()] = None,
    override_gpu_count:    Annotated[Optional[str], Form()] = None,
    override_uid:          Annotated[Optional[str], Form()] = None,
    override_gid:          Annotated[Optional[str], Form()] = None,
    override_run_as_user:  Annotated[Optional[str], Form()] = None,
    override_run_as_group: Annotated[Optional[str], Form()] = None,
    override_fs_group:     Annotated[Optional[str], Form()] = None,
    override_zone:         Annotated[Optional[str], Form()] = None,
):
    name = instance_name.strip()
    # The template carries the access mode; create the matching Session. Desktop
    # sessions are connected from the desktop portal, ssh ones via the SSH bridge.
    tpls = await _user_templates(request, cm, user)
    tpl = next((t for t in tpls
                if t.get("fullName") == template_name or t.get("name") == template_name), None)
    mode = (tpl or {}).get("mode", "ssh")

    overrides = _build_session_overrides(
        cpu=override_cpu, memory=override_memory,
        gpu_type=override_gpu_type, gpu_count=override_gpu_count,
        uid=override_uid, gid=override_gid,
        run_as_user=override_run_as_user, run_as_group=override_run_as_group,
        fs_group=override_fs_group, zone=override_zone,
    )

    if mode == "desktop":
        ok = await request.app.state.run(
            cm.add_desktop_session, user, template_name, name, overrides,
        )
        if not ok:
            raise HTTPException(status_code=500, detail="Failed to create desktop session.")
        return _tr("/", user)

    ok = await request.app.state.run(
        cm.add_instance, user, template_name, name, preemptible == "on",
        overrides, False, (home_volume or "").strip() or None,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create instance.")
    return _tr(f"/instances/{name}", user)


async def instance_edit_form(request: Request, cm: CM, user: User, is_admin: IsAdmin, name: str):
    (instances, desktop_sessions, tpls, form_ctx, cur, home_volumes_) = \
        await asyncio.gather(
            request.app.state.run(cm.get_user_instances, user),
            request.app.state.run(cm.get_user_desktop_sessions, user),
            _user_templates(request, cm, user),
            _override_form_context(request, cm, user),
            request.app.state.run(cm.get_instance_config, user, name),
            request.app.state.run(cm.get_home_volumes, user),
        )
    # ssh instances and desktop/VM sessions are both Session CRs with an editable
    # spec.overrides — resolve either (desktop phase normalises to "status").
    inst = next((i for i in instances if i["name"] == name), None)
    if inst is None:
        sess = next((s for s in desktop_sessions if s["name"] == name), None)
        if sess is not None:
            inst = {**sess, "status": sess.get("phase"), "ready": True}
    if inst is None or cur is None:
        raise HTTPException(status_code=404, detail="Instance not found.")
    # Overrides only take effect on the next start/reboot, so only allow editing
    # once the instance is fully at rest (no pod). While it's Pending/Starting/
    # Running a change would silently not apply to the live session.
    if status_group(inst["status"], inst.get("ready", True)) not in ("Stopped", "Error"):
        raise HTTPException(status_code=409, detail="Stop the instance before editing it.")

    return templates.TemplateResponse(
        request=request, name="user/edit_instance.html",
        context=_ctx(user, is_admin=is_admin, inst=inst, cur=cur, tpls=tpls,
                     home_volumes=home_volumes_,
                     current_home_volume=cur.get("homeVolume"), **form_ctx),
    )


async def instance_update(
    request: Request, cm: CM, user: User, name: str,
    preemptible:   Annotated[Optional[str], Form()] = None,
    home_volume:   Annotated[Optional[str], Form()] = None,
    override_cpu:          Annotated[Optional[str], Form()] = None,
    override_memory:       Annotated[Optional[str], Form()] = None,
    override_gpu_type:     Annotated[Optional[str], Form()] = None,
    override_gpu_count:    Annotated[Optional[str], Form()] = None,
    override_uid:          Annotated[Optional[str], Form()] = None,
    override_gid:          Annotated[Optional[str], Form()] = None,
    override_run_as_user:  Annotated[Optional[str], Form()] = None,
    override_run_as_group: Annotated[Optional[str], Form()] = None,
    override_fs_group:     Annotated[Optional[str], Form()] = None,
    override_zone:         Annotated[Optional[str], Form()] = None,
):
    overrides = _build_session_overrides(
        cpu=override_cpu, memory=override_memory,
        gpu_type=override_gpu_type, gpu_count=override_gpu_count,
        uid=override_uid, gid=override_gid,
        run_as_user=override_run_as_user, run_as_group=override_run_as_group,
        fs_group=override_fs_group, zone=override_zone,
    )
    ok = await request.app.state.run(
        cm.update_instance, user, name, preemptible == "on", overrides,
        (home_volume or "").strip() or None,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to update instance.")
    return _tr(f"/instances/{name}", user)


async def instance_detail(request: Request, cm: CM, user: User, is_admin: IsAdmin, name: str):
    instances, desktop_sessions, grants = await asyncio.gather(
        request.app.state.run(cm.get_user_instances, user),
        request.app.state.run(cm.get_user_desktop_sessions, user),
        request.app.state.run(cm.get_user_overrides, user),
    )
    inst = next((i for i in instances if i["name"] == name), None)
    if inst is None:
        # Desktop sessions carry their state under "phase"; normalise to "status"
        # so the shared detail template renders them too.
        sess = next((s for s in desktop_sessions if s["name"] == name), None)
        if sess is not None:
            inst = {**sess, "status": sess.get("phase"), "ready": True}
    if not inst:
        raise HTTPException(status_code=404, detail="Instance not found.")
    # Both ssh instances and desktop/VM sessions are Session CRs whose
    # spec.overrides can be edited while at rest.
    inst = {**inst, "editable": True}
    return templates.TemplateResponse(
        request=request, name="user/instance_detail.html",
        context=_ctx(user, is_admin=is_admin, inst=inst,
                     can_override=_may_override(grants)),
    )


async def _status_badge_response(request: Request, cm, user: str, name: str,
                                 controls: bool,
                                 is_admin: bool = False) -> HTMLResponse:
    """Look up the current consolidated status (ssh pod phase or desktop CR phase)
    and render the polling badge (with Start/Stop buttons when `controls`)."""
    instances, desktop_sessions, grants = await asyncio.gather(
        request.app.state.run(cm.get_user_instances, user),
        request.app.state.run(cm.get_user_desktop_sessions, user),
        request.app.state.run(cm.get_user_overrides, user),
    )
    can_override = _may_override(grants)
    inst = next((i for i in instances if i["name"] == name), None)
    # Both ssh instances and desktop/VM sessions are Session CRs with an editable
    # spec.overrides, so both get an Edit action.
    editable = False
    if inst:
        status, connect_url = inst["status"], None
        ready = inst.get("ready", True)
        term_url = _terminal_url(user, name)
        # An ssh instance is a container *or* a VM (images/devbase): containers
        # have no emulated display, a VM does. Recomputed here for the same
        # reason as the desktop branch below — this badge is re-rendered
        # out-of-band on every poll, so hardcoding None made the machine-console
        # button vanish the moment the VM started and polling kicked in.
        console_url = (_console_url(user, name)
                       if is_admin and inst.get("runtime") == "vm" else None)
        editable = True
    else:
        sess = next((s for s in desktop_sessions if s["name"] == name), None)
        if sess:
            status = sess["phase"]
            ready = True
            connect_url = _desktop_viewer_url(user, name)
            term_url = _terminal_url(user, name)
            # Re-rendered out-of-band on every status poll, so it has to be
            # recomputed here too — otherwise the admin console button
            # disappears the first time the badge refreshes.
            console_url = (_console_url(user, name)
                           if is_admin and sess.get("runtime") == "vm" else None)
            editable = True
        else:
            status, connect_url, term_url, console_url = "Unknown", None, None, None
            ready = True
    return HTMLResponse(
        _render_status_html(name, status, user, controls, connect_url, term_url,
                            ready, editable, console_url, can_override))


async def instance_status_badge(request: Request, cm: CM, user: User, name: str,
                                is_admin: IsAdmin):
    """HTMX polling endpoint — returns the status badge span. The dashboard passes
    ?controls=1 to also refresh the Start/Stop buttons (out-of-band); the detail
    view polls without it and gets just the badge."""
    controls = request.query_params.get("controls") == "1"
    return await _status_badge_response(request, cm, user, name, controls, is_admin)


async def instance_start_dialog(request: Request, cm: CM, user: User,
                                is_admin: IsAdmin, name: str):
    """The start dialog: the instance's saved overrides, editable, as a modal
    fragment.

    Prefilled from spec.overrides because those are the instance's *defaults*,
    and answered into spec.runOverrides, which lives for one run — so this is
    the last moment before a run where a value can still be changed, and
    changing it costs the instance nothing. Portal only: the launcher TUI's `s`
    key stays a single keystroke (design/proxyjump.md, "TUI diet").

    `then=` names where the browser is headed once it has started: the desktop,
    the web terminal or the machine console, each of which starts a stopped
    instance on its way in and so has to be able to ask first. `hx=` picks how
    a dialog with nowhere to go submits — the dashboard swaps the status badge
    in place, the detail page posts normally and follows its redirect."""
    form_ctx, cur = await asyncio.gather(
        _override_form_context(request, cm, user),
        request.app.state.run(cm.get_instance_config, user, name),
    )
    if cur is None:
        raise HTTPException(status_code=404, detail="Instance not found.")
    return templates.TemplateResponse(
        request=request, name="user/_start_dialog.html",
        context=_ctx(user, is_admin=is_admin, name=name, cur=cur,
                     has_overrides=_may_override(form_ctx["overrides"]),
                     hx=request.query_params.get("hx", "1") != "0",
                     then=_start_destination(request.query_params.get("then")),
                     **form_ctx),
    )


def _start_destination(then: Optional[str]) -> Optional[str]:
    """The `?then=` a button asked for, or None. Unknown values are dropped
    rather than refused: the parameter only decides which of three of our own
    pages the browser lands on next, so a stale or mangled one should still
    start the instance — and it can never be turned into a URL we do not own."""
    return then if then in _START_DESTINATIONS else None


async def instance_connect(
    request: Request, cm: CM, user: User, name: str, is_admin: IsAdmin,
    apply_overrides: Annotated[Optional[str], Form()] = None,
    override_cpu:          Annotated[Optional[str], Form()] = None,
    override_memory:       Annotated[Optional[str], Form()] = None,
    override_gpu_type:     Annotated[Optional[str], Form()] = None,
    override_gpu_count:    Annotated[Optional[str], Form()] = None,
    override_uid:          Annotated[Optional[str], Form()] = None,
    override_gid:          Annotated[Optional[str], Form()] = None,
    override_run_as_user:  Annotated[Optional[str], Form()] = None,
    override_run_as_group: Annotated[Optional[str], Form()] = None,
    override_fs_group:     Annotated[Optional[str], Form()] = None,
    override_zone:         Annotated[Optional[str], Form()] = None,
):
    """Start an instance, with the overrides this run is to use.

    The plain play button posts nothing and this is the annotation bump it
    always was. The start dialog posts `apply_overrides=1`, and the fields it
    submitted become the run's overrides — **for that run only**. They do not
    touch spec.overrides, so what the instance starts with next time is
    unchanged; the edit form is still the only thing that moves an instance's
    defaults. The dialog is prefilled from those defaults and submits the whole
    picture, so a cleared field means "not this run" rather than an unanswered
    question, and `or {}` is what lets a dialog with everything cleared say
    "this run: nothing" instead of falling back to the defaults.

    Both slices are written in the same act as the start (see
    trigger_instance_start), which is what keeps a run's values from outliving
    its run."""
    run_overrides = None
    if apply_overrides == "1":
        run_overrides = _build_session_overrides(
            cpu=override_cpu, memory=override_memory,
            gpu_type=override_gpu_type, gpu_count=override_gpu_count,
            uid=override_uid, gid=override_gid,
            run_as_user=override_run_as_user, run_as_group=override_run_as_group,
            fs_group=override_fs_group, zone=override_zone,
        ) or {}

    ok = await request.app.state.run(cm.trigger_instance_start, user, name,
                                     run_overrides)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to start instance.")
    # A dialog opened from the desktop/terminal/console button submits into a
    # new tab and is sent on to that page, which is where the click was going
    # before the dialog stood in front of it. The viewer app does its own
    # authorization there (the console's admin check included), so this is a
    # hand-off, not a grant.
    then = _start_destination(request.query_params.get("then"))
    if then:
        return RedirectResponse(_START_DESTINATIONS[then](user, name),
                                status_code=303)
    # Dashboard buttons post via HTMX: swap the status badge (and buttons) in place
    # instead of navigating to the detail view. Plain form posts (detail) redirect.
    if request.headers.get("HX-Request"):
        return await _status_badge_response(request, cm, user, name, controls=True,
                                            is_admin=is_admin)
    return _tr(f"/instances/{name}", user)


async def instance_stop(request: Request, cm: CM, user: User, name: str,
                        is_admin: IsAdmin):
    await request.app.state.run(cm.stop_instance, user, name)
    if request.headers.get("HX-Request"):
        return await _status_badge_response(request, cm, user, name, controls=True,
                                            is_admin=is_admin)
    return _tr(f"/instances/{name}", user)


async def instance_delete(request: Request, cm: CM, user: User, name: str):
    await request.app.state.run(cm.delete_instance, user, name)
    return _tr("/", user)


# --------------------------------------------------------------------------- #
# Admin — overview                                                             #
# --------------------------------------------------------------------------- #

def _resource_meter_view(summary: dict, *, unit: str, to_display=float, decimals: int = 1) -> dict:
    """Turn a get_cluster_resources() summary ({total, free, whistler,
    whistlerPreemptible, other}) into pre-computed display values + bar
    percentages, so the dashboard template stays free of arithmetic."""
    parts = {k: to_display(summary[k]) for k in ("whistler", "whistlerPreemptible", "other", "free")}
    total = to_display(summary["total"])
    pct = {k: (v / total * 100 if total else 0) for k, v in parts.items()}
    return {
        "unit": unit,
        "total": round(total, decimals),
        "used": round(total - parts["free"], decimals),
        "parts": {k: round(v, decimals) for k, v in parts.items()},
        "pct": pct,
    }


def _dashboard_resource_views(resources: dict) -> dict:
    """Build the CPU/memory/per-GPU-type meter view-models for dashboard.html."""
    return {
        "cpu": _resource_meter_view(resources["cpu"], unit="cores"),
        "memory": _resource_meter_view(
            resources["memory"], unit="GiB", to_display=lambda v: float(v) / (1024 ** 3)),
        "gpus": [
            {"type": g["type"], **_resource_meter_view(g, unit="GPUs", to_display=int, decimals=0)}
            for g in resources["gpus"]
        ],
    }


# --------------------------------------------------------------------------- #
# Dashboard — cluster-wide capacity + running instances (all users)           #
# --------------------------------------------------------------------------- #

async def dashboard(request: Request, cm: CM, user: User, is_admin: IsAdmin):
    resources, all_instances, desktop_sessions = await asyncio.gather(
        request.app.state.run(cm.get_cluster_resources),
        request.app.state.run(cm.get_all_instances),
        request.app.state.run(cm.list_all_desktop_sessions),
    )
    # Two vocabularies: a pod reports "Running", a VM session reports the
    # operator's "Ready". Both mean the same thing on this dashboard.
    # get_all_instances is ssh-mode only, so desktop sessions come in via
    # list_all_desktop_sessions (whose rows say "user"/"phase", not
    # "username"/"status") — without them a running desktop VM is invisible
    # here.
    running = [i for i in all_instances
               if i.get("status") in ("Running", "Ready")]
    running += [
        {"username": s["user"], "name": s["name"], "template": s.get("template"),
         "preemptible": s.get("preemptible", False)}
        for s in desktop_sessions if s.get("phase") in ("Running", "Ready")
    ]
    running.sort(key=lambda i: (i["username"], i["name"]))
    return templates.TemplateResponse(
        request=request, name="dashboard.html",
        context=_ctx(user, is_admin=is_admin, res=_dashboard_resource_views(resources),
                     running_instances=running),
    )


async def admin_index(request: Request, cm: CM, admin: Admin):
    all_instances = await request.app.state.run(cm.get_all_instances)
    all_users     = await request.app.state.run(cm.list_all_users)
    all_templates = await request.app.state.run(cm.get_all_templates)
    return templates.TemplateResponse(
        request=request, name="admin/index.html",
        context=_ctx(admin, is_admin=True, all_instances=all_instances,
                     all_users=all_users, all_templates=all_templates),
    )


# --------------------------------------------------------------------------- #
# Home volumes (the user's own; design/security.md)                            #
# --------------------------------------------------------------------------- #
#
# A home is a named disk the user owns and an instance selects, not something
# created and destroyed with the instance. So these are user-facing routes,
# not admin ones: it is their data.

async def _home_volume_rows(request: Request, cm, username: str):
    """Each volume plus the running instance holding it, if any. The holder is
    what makes the one-live-attach rule legible instead of a surprise at
    start."""
    volumes = await request.app.state.run(cm.get_home_volumes, username)
    rows = []
    for vol in volumes:
        holder = await request.app.state.run(
            cm.home_volume_holder, username, vol)
        rows.append({**vol,
                     "pvcName": cm.home_volume_pvc_name(vol),
                     "inUseBy": holder})
    return rows


async def home_volumes(request: Request, cm: CM, user: User, is_admin: IsAdmin):
    # Only zones the user may already enter. Creating a home in one of those
    # is not an escalation — they could already start an instance there with a
    # fresh home — which is what makes this self-service instead of a ticket.
    zones, allowed_zones, access = await asyncio.gather(
        request.app.state.run(cm.get_zones),
        request.app.state.run(cm.get_user_allowed_zones, user),
        request.app.state.run(cm.get_user_volume_access, user),
    )
    return templates.TemplateResponse(
        request=request, name="user/home_volumes.html",
        context=_ctx(user, is_admin=is_admin,
                     volumes=await _home_volume_rows(request, cm, user),
                     zones=[z for z in zones if z in allowed_zones],
                     access=access),
    )


async def home_volume_create(
    request: Request, cm: CM, user: User,
    name:        Annotated[str, Form()],
    zone:        Annotated[str, Form()],
    size:        Annotated[Optional[str], Form()] = None,
    description: Annotated[Optional[str], Form()] = None,
):
    name = name.strip()
    if not re.fullmatch(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", name):
        raise HTTPException(
            status_code=400,
            detail="Home volume name must be a DNS-1123 label (lowercase "
                   "alphanumerics and '-', max 63 chars).")
    # The zone must be one the user already holds, checked server-side: the
    # form only offers those, but the form is not the boundary. Granting
    # themselves a zone they cannot enter would be a real escalation, since
    # the cell outlives whatever their allowedZones say later.
    zone = zone.strip()
    zones, allowed_zones = await asyncio.gather(
        request.app.state.run(cm.get_zones),
        request.app.state.run(cm.get_user_allowed_zones, user),
    )
    if zone not in zones or zone not in allowed_zones:
        raise HTTPException(
            status_code=400,
            detail=f"You do not have access to zone '{zone}'.")
    ok = await request.app.state.run(cm.save_home_volume, user, {
        "name": name,
        "size": (size or "").strip() or None,
        "description": (description or "").strip() or None,
    })
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create home volume.")
    # The volume is useless without a cell: the matrix has no defaults. This
    # is the one entry a user writes themselves, and only for a zone they
    # already hold — every other cell is an administrator's decision.
    if not await request.app.state.run(
            cm.grant_own_volume_access, user, zone, name):
        raise HTTPException(
            status_code=500,
            detail="Created the volume but could not grant it in that zone. "
                   "An administrator can add the entry in the access matrix.")
    return _tr("/homes", user)


async def home_volume_delete(
    request: Request, cm: CM, user: User, name: str,
    delete_data: Annotated[Optional[str], Form()] = None,
):
    ok = await request.app.state.run(
        cm.delete_home_volume, user, name, delete_data == "on")
    if not ok:
        raise HTTPException(
            status_code=400,
            detail="Could not delete that home volume. A volume attached to a "
                   "running instance must be released first — stop the "
                   "instance and try again.")
    return _tr("/homes", user)


# --------------------------------------------------------------------------- #
# Admin — templates                                                            #
# --------------------------------------------------------------------------- #

async def admin_templates(request: Request, cm: CM, admin: Admin):
    tpls = await request.app.state.run(cm.get_all_templates)
    return templates.TemplateResponse(
        request=request, name="admin/templates.html",
        context=_ctx(admin, is_admin=True, tpls=tpls),
    )


async def admin_template_new(request: Request, cm: CM, admin: Admin):
    images, gpu_types, zones = await asyncio.gather(
        request.app.state.run(cm.get_available_images),
        request.app.state.run(cm.get_gpu_types),
        request.app.state.run(cm.get_zones),
    )
    return templates.TemplateResponse(
        request=request, name="admin/template_form.html",
        context=_ctx(admin, is_admin=True, tpl=None, available_images=images,
                     gpu_types=gpu_types, zones=zones),
    )


async def admin_template_create(
    request: Request, cm: CM, admin: Admin,
    display_name:   Annotated[str, Form()],
    slug:           Annotated[str, Form()],
    image:          Annotated[str, Form()],
    description:    Annotated[Optional[str], Form()] = None,
    cpu:            Annotated[Optional[str], Form()] = None,
    memory:         Annotated[Optional[str], Form()] = None,
    gpu:            Annotated[Optional[str], Form()] = None,
    gpu_type:       Annotated[Optional[str], Form()] = None,
    personal_mount: Annotated[Optional[str], Form()] = "/userdata",
    mode:           Annotated[str, Form()] = "ssh",
    runtime:        Annotated[str, Form()] = "container",
    privileged:     Annotated[Optional[str], Form()] = None,
    fuse:           Annotated[Optional[str], Form()] = None,
    display_port:   Annotated[Optional[str], Form()] = None,
    protocol:       Annotated[Optional[str], Form()] = None,
    zone:           Annotated[Optional[str], Form()] = None,
):
    data = _template_form_data(
        name=slug.strip(), display_name=display_name, image=image,
        description=description, cpu=cpu, memory=memory, gpu=gpu, gpu_type=gpu_type,
        personal_mount=personal_mount,
        mode=mode, runtime=runtime, privileged=privileged, fuse=fuse,
        display_port=display_port, protocol=protocol, zone=zone,
    )
    ok = await request.app.state.run(cm.save_system_template, data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create template.")
    return _tr("/admin/templates", admin)


async def admin_template_edit(request: Request, cm: CM, admin: Admin, name: str):
    tpls, images, gpu_types, zones = await asyncio.gather(
        request.app.state.run(cm.get_all_templates),
        request.app.state.run(cm.get_available_images),
        request.app.state.run(cm.get_gpu_types),
        request.app.state.run(cm.get_zones),
    )
    tpl = next((t for t in tpls if t.get("fullName") == name or t.get("name") == name), None)
    if not tpl:
        raise HTTPException(status_code=404, detail="Template not found.")
    return templates.TemplateResponse(
        request=request, name="admin/template_form.html",
        context=_ctx(admin, is_admin=True, tpl=tpl, available_images=images,
                     gpu_types=gpu_types, zones=zones),
    )


async def admin_template_update(
    request: Request, cm: CM, admin: Admin, name: str,
    display_name:   Annotated[str, Form()],
    image:          Annotated[str, Form()],
    description:    Annotated[Optional[str], Form()] = None,
    cpu:            Annotated[Optional[str], Form()] = None,
    memory:         Annotated[Optional[str], Form()] = None,
    gpu:            Annotated[Optional[str], Form()] = None,
    gpu_type:       Annotated[Optional[str], Form()] = None,
    personal_mount: Annotated[Optional[str], Form()] = "/userdata",
    mode:           Annotated[str, Form()] = "ssh",
    runtime:        Annotated[str, Form()] = "container",
    privileged:     Annotated[Optional[str], Form()] = None,
    fuse:           Annotated[Optional[str], Form()] = None,
    display_port:   Annotated[Optional[str], Form()] = None,
    protocol:       Annotated[Optional[str], Form()] = None,
    zone:           Annotated[Optional[str], Form()] = None,
):
    data = _template_form_data(
        name=name, display_name=display_name, image=image,
        description=description, cpu=cpu, memory=memory, gpu=gpu, gpu_type=gpu_type,
        personal_mount=personal_mount,
        mode=mode, runtime=runtime, privileged=privileged, fuse=fuse,
        display_port=display_port, protocol=protocol, zone=zone,
    )
    ok = await request.app.state.run(cm.save_system_template, data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to update template.")
    return _tr("/admin/templates", admin)


async def admin_template_delete(request: Request, cm: CM, admin: Admin, name: str):
    await request.app.state.run(cm.delete_system_template, name)
    return _tr("/admin/templates", admin)


# --------------------------------------------------------------------------- #
# Admin — users                                                                #
# --------------------------------------------------------------------------- #

async def admin_users(request: Request, cm: CM, admin: Admin):
    users = await request.app.state.run(cm.list_all_users)
    return templates.TemplateResponse(
        request=request, name="admin/users.html",
        context=_ctx(admin, is_admin=True, users=users),
    )


async def admin_user_new(request: Request, cm: CM, admin: Admin):
    return templates.TemplateResponse(
        request=request, name="admin/user_form.html",
        context=_ctx(admin, is_admin=True, user_obj=None),
    )


async def admin_user_create(
    request: Request, cm: CM, admin: Admin,
    name:         Annotated[str, Form()],
    public_keys:  Annotated[Optional[str], Form()] = None,
    uid:          Annotated[Optional[str], Form()] = None,
    run_as_user:  Annotated[Optional[str], Form()] = None,
    run_as_group: Annotated[Optional[str], Form()] = None,
    fs_group:     Annotated[Optional[str], Form()] = None,
    is_admin_flag: Annotated[Optional[str], Form(alias="admin")] = None,
):
    user_data = _build_user_data(name, public_keys, run_as_user, run_as_group, fs_group,
                                  uid, is_admin_flag)
    # Every allow is explicit, so a user created with nothing ticked holds
    # nothing — including a door to come in through and a zone to launch in.
    # Seed those two here rather than in _build_user_data, which the *edit*
    # form shares: handing them back on every save would make narrowing them
    # impossible. The grants that cost something (volumes, GPU types) stay
    # empty; the admin lands on the detail page and grants them there.
    user_data["entryPoints"] = list(NEW_USER_ENTRY_POINTS)
    user_data["allowedZones"] = list(NEW_USER_ZONES)
    ok = await request.app.state.run(cm.save_user, user_data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create user.")
    return _tr(f"/admin/users/{name}", admin)


async def admin_user_detail(request: Request, cm: CM, admin: Admin, username: str):
    """The user's own grants (what the forms edit) *and* their effective ones
    (own unioned with every group they belong to).

    The distinction is the whole reason this page has two columns of truth:
    the checkboxes must reflect and save the User CR's own fields only. Saving
    the effective set would quietly copy a project's grants onto the user, and
    they would keep them after leaving the group."""
    all_users = await request.app.state.run(cm.list_all_users)
    user_obj  = next((u for u in all_users if u.get("name") == username), None)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found.")
    instances = await request.app.state.run(cm.get_user_instances, username)
    gpu_types = await request.app.state.run(cm.get_gpu_types)
    allowed_gpu_types = await request.app.state.run(cm.get_user_allowed_gpu_types, username)
    user_overrides = await request.app.state.run(cm.get_user_overrides, username)
    zones = await request.app.state.run(cm.get_zones)
    allowed_zones = await request.app.state.run(cm.get_user_allowed_zones, username)
    user_groups = await request.app.state.run(cm.get_user_groups, username)
    own_access = (user_obj.get("volumeAccess") or {})
    effective_access = await request.app.state.run(
        cm.get_user_volume_access, username)
    access_sections = _sections_with_values(
        await _matrix_sections(request, cm, username),
        zones, own_access, effective_access)
    channel_grant = await request.app.state.run(cm.get_user_channels, username)
    return templates.TemplateResponse(
        request=request, name="admin/user_detail.html",
        context=_ctx(admin, is_admin=True, user_obj=user_obj, instances=instances,
                     gpu_types=gpu_types, allowed_gpu_types=allowed_gpu_types,
                     user_overrides=user_overrides, override_groups=OVERRIDE_GROUPS,
                     zones=zones, allowed_zones=allowed_zones,
                     user_groups=user_groups,
                     channels=CHANNELS, enforced_channels=ENFORCED_CHANNELS,
                     own_channels=user_obj.get("channels"),
                     entry_points=ENTRY_POINTS,
                     own_entry_points=user_obj.get("entryPoints") or [],
                     allowed_entry_points=await request.app.state.run(
                         cm.get_user_entry_points, username),
                     channel_grant=sorted(channel_grant) if channel_grant is not None else None,
                     access_sections=access_sections,
                     own_zones=user_obj.get("allowedZones") or [],
                     own_gpu_types=user_obj.get("allowedGpuTypes") or [],
                     own_overrides=user_obj.get("overrides") or {}),
    )


async def admin_user_update(
    request: Request, cm: CM, admin: Admin, username: str,
    public_keys:  Annotated[Optional[str], Form()] = None,
    uid:          Annotated[Optional[str], Form()] = None,
    run_as_user:  Annotated[Optional[str], Form()] = None,
    run_as_group: Annotated[Optional[str], Form()] = None,
    fs_group:     Annotated[Optional[str], Form()] = None,
    is_admin_flag: Annotated[Optional[str], Form(alias="admin")] = None,
):
    user_data = _build_user_data(username, public_keys, run_as_user, run_as_group, fs_group,
                                  uid, is_admin_flag)
    ok = await request.app.state.run(cm.save_user, user_data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to update user.")
    return _tr(f"/admin/users/{username}", admin)


async def admin_user_delete(request: Request, cm: CM, admin: Admin, username: str):
    await request.app.state.run(cm.delete_user, username)
    return _tr("/admin/users", admin)


async def admin_user_set_gpu_types(
    request: Request, cm: CM, admin: Admin, username: str,
    gpu_types: Annotated[Optional[list[str]], Form()] = None,
):
    await request.app.state.run(cm.set_user_allowed_gpu_types, username, gpu_types or [])
    return _tr(f"/admin/users/{username}", admin)


async def admin_user_set_zones(
    request: Request, cm: CM, admin: Admin, username: str,
    zone_names: Annotated[Optional[list[str]], Form()] = None,
):
    await request.app.state.run(cm.set_user_allowed_zones, username, zone_names or [])
    return _tr(f"/admin/users/{username}", admin)


async def admin_user_set_overrides(
    request: Request, cm: CM, admin: Admin, username: str,
    override_groups: Annotated[Optional[list[str]], Form()] = None,
):
    checked = set(override_groups or [])
    overrides = {g: g in checked for g in OVERRIDE_GROUPS}
    await request.app.state.run(cm.set_user_overrides, username, overrides)
    return _tr(f"/admin/users/{username}", admin)


async def admin_user_set_entry_points(
    request: Request, cm: CM, admin: Admin, username: str,
    entry_points: Annotated[Optional[list[str]], Form()] = None,
):
    """Which doors this user may use. No boxes means **no door** — the same
    explicit-allow rule as zones and volumes, and the reason the form says so
    next to the checkboxes rather than leaving an admin to discover that
    unchecking everything is a lockout.

    An admin can bind *themselves* out of the portal here. That is allowed on
    purpose: the alternative is a special case that makes the field mean
    something different for one account, and `kubectl edit usr` is the way
    back."""
    await request.app.state.run(cm.set_user_entry_points, username,
                                entry_points or [])
    if username == admin and ENTRY_PORTAL not in (entry_points or []):
        logger.warning(f"Admin {admin} just removed their own "
                       f"'{ENTRY_PORTAL}' entry point")
    return _tr(f"/admin/users/{username}", admin)


async def admin_user_set_channels(
    request: Request, cm: CM, admin: Admin, username: str,
    restrict:  Annotated[Optional[str], Form()] = None,
    channels:  Annotated[Optional[list[str]], Form()] = None,
):
    """`restrict` unchecked clears the user's own grant (this user narrows
    nothing); checked writes exactly the boxes ticked — including none of
    them, which is a real grant of nothing and the reason the toggle exists."""
    if not restrict:
        await request.app.state.run(cm.set_user_channels, username, None)
    else:
        await request.app.state.run(
            cm.set_user_channels, username,
            [c for c in CHANNELS if c in set(channels or [])])
    return _tr(f"/admin/users/{username}", admin)


# --------------------------------------------------------------------------- #
# Admin — groups (a project's shared grants; Group CRs)                        #
# --------------------------------------------------------------------------- #

async def admin_groups(request: Request, cm: CM, admin: Admin):
    group_defs = await request.app.state.run(cm.get_group_definitions)
    groups = [{"name": name, **(spec or {})}
              for name, spec in sorted(group_defs.items())]
    return templates.TemplateResponse(
        request=request, name="admin/groups.html",
        context=_ctx(admin, is_admin=True, groups=groups),
    )


async def _group_form_context(request, admin, group=None):
    """Everything the group editor needs to render: the catalogs it grants
    from, plus the group itself when editing.

    There is no separate volume catalog any more. A group used to carry both a
    `volumes` allow-list (with per-member rw/ro) and a `volumeAccess` matrix,
    and only the first was enforced; the matrix is now the only one, so the
    editor has one table where it had two panels."""
    cm = request.app.state.cm
    zones, gpu_types, all_users = await asyncio.gather(
        request.app.state.run(cm.get_zones),
        request.app.state.run(cm.get_gpu_types),
        request.app.state.run(cm.list_all_users),
    )
    own_access = ((group or {}).get("volumeAccess") or {})
    access_sections = _sections_with_values(
        await _matrix_sections(request, cm), zones, own_access, own_access)
    return _ctx(admin, is_admin=True, group=group, zones=zones,
                gpu_types=gpu_types, all_users=all_users,
                entry_points=ENTRY_POINTS,
                channels=CHANNELS, enforced_channels=ENFORCED_CHANNELS,
                override_groups=OVERRIDE_GROUPS,
                access_sections=access_sections)


async def admin_group_new(request: Request, cm: CM, admin: Admin):
    return templates.TemplateResponse(
        request=request, name="admin/group_form.html",
        context=await _group_form_context(request, admin),
    )


async def admin_group_edit(request: Request, cm: CM, admin: Admin, name: str):
    group_defs = await request.app.state.run(cm.get_group_definitions)
    if name not in group_defs:
        raise HTTPException(status_code=404, detail="Group not found.")
    group = {"name": name, **(group_defs[name] or {})}
    return templates.TemplateResponse(
        request=request, name="admin/group_form.html",
        context=await _group_form_context(request, admin, group=group),
    )


async def _save_group_or_400(request, verb: str, name: str, form):
    """Run save_group and turn its two failure modes into HTTP.

    A ConfigWriteError carries the cluster's own reason (most usefully "the
    CRD is not installed") — showing it beats a flat "Failed to create group."
    that sends an admin to the pod logs for a message the server already had.
    """
    try:
        ok = await request.app.state.run(
            request.app.state.cm.save_group, _build_group_data(name, form))
    except ConfigWriteError as e:
        raise HTTPException(status_code=500,
                            detail=f"Failed to {verb} group: {e}") from e
    if not ok:
        raise HTTPException(status_code=400,
                            detail=f"Failed to {verb} group: it needs a name.")


async def admin_group_create(request: Request, cm: CM, admin: Admin):
    form = await request.form()
    name = (form.get("name") or "").strip()
    if not re.fullmatch(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", name):
        raise HTTPException(status_code=400,
                            detail="Group name must be a DNS-1123 label "
                                   "(lowercase alphanumerics and '-', max 63 chars).")
    await _save_group_or_400(request, "create", name, form)
    return _tr("/admin/groups", admin)


async def admin_group_update(request: Request, cm: CM, admin: Admin, name: str):
    await _save_group_or_400(request, "update", name, await request.form())
    return _tr("/admin/groups", admin)


async def admin_group_delete(request: Request, cm: CM, admin: Admin, name: str):
    try:
        await request.app.state.run(cm.delete_group, name)
    except ConfigWriteError as e:
        raise HTTPException(status_code=500,
                            detail=f"Failed to delete group: {e}") from e
    return _tr("/admin/groups", admin)


# --------------------------------------------------------------------------- #
# Admin — volumes                                                              #
# --------------------------------------------------------------------------- #

# --------------------------------------------------------------------------- #
# Admin — zones (network postures; Zone CRs)                                    #
# --------------------------------------------------------------------------- #

async def admin_zones(request: Request, cm: CM, admin: Admin):
    zone_defs = await request.app.state.run(cm.get_zone_definitions)
    zones = [{"name": name, **(cfg or {})} for name, cfg in sorted(zone_defs.items())]
    return templates.TemplateResponse(
        request=request, name="admin/zones.html",
        context=_ctx(admin, is_admin=True, zones=zones),
    )


async def admin_zone_new(request: Request, cm: CM, admin: Admin):
    return templates.TemplateResponse(
        request=request, name="admin/zone_form.html",
        context=_ctx(admin, is_admin=True, zone=None,
                     allow_cidrs_text="", block_cidrs_text="", dns_servers_text=""),
    )


async def admin_zone_create(
    request: Request, cm: CM, admin: Admin,
    name:             Annotated[str, Form()],
    description:      Annotated[Optional[str], Form()] = None,
    allow_cidrs:      Annotated[Optional[str], Form()] = None,
    block_cidrs:      Annotated[Optional[str], Form()] = None,
    dns_cluster_only: Annotated[Optional[str], Form()] = None,
    dns_servers:      Annotated[Optional[str], Form()] = None,
):
    name = name.strip()
    if not re.fullmatch(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", name):
        raise HTTPException(status_code=400,
                            detail="Zone name must be a DNS-1123 label "
                                   "(lowercase alphanumerics and '-', max 63 chars).")
    zone_data = _build_zone_data(name, description, allow_cidrs, block_cidrs,
                                 dns_cluster_only, dns_servers)
    ok = await request.app.state.run(cm.save_zone, zone_data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create zone.")
    return _tr("/admin/zones", admin)


async def admin_zone_edit(request: Request, cm: CM, admin: Admin, name: str):
    zone_defs = await request.app.state.run(cm.get_zone_definitions)
    if name not in zone_defs:
        raise HTTPException(status_code=404, detail="Zone not found.")
    cfg = zone_defs[name] or {}
    egress = cfg.get("egress") or {}
    dns = cfg.get("dns") or {}
    return templates.TemplateResponse(
        request=request, name="admin/zone_form.html",
        context=_ctx(admin, is_admin=True, zone={"name": name, **cfg},
                     allow_cidrs_text=_format_allow_cidrs(egress.get("allowCIDRs")),
                     block_cidrs_text="\n".join(egress.get("blockCIDRs") or []),
                     dns_servers_text=", ".join(dns.get("servers") or [])),
    )


async def admin_zone_update(
    request: Request, cm: CM, admin: Admin, name: str,
    description:      Annotated[Optional[str], Form()] = None,
    allow_cidrs:      Annotated[Optional[str], Form()] = None,
    block_cidrs:      Annotated[Optional[str], Form()] = None,
    dns_cluster_only: Annotated[Optional[str], Form()] = None,
    dns_servers:      Annotated[Optional[str], Form()] = None,
):
    zone_data = _build_zone_data(name, description, allow_cidrs, block_cidrs,
                                 dns_cluster_only, dns_servers)
    ok = await request.app.state.run(cm.save_zone, zone_data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to update zone.")
    return _tr("/admin/zones", admin)


async def admin_zone_delete(request: Request, cm: CM, admin: Admin, name: str):
    ok = await request.app.state.run(cm.delete_zone, name)
    if not ok:
        raise HTTPException(
            status_code=400,
            detail="Failed to delete zone. The default zone cannot be deleted.")
    return _tr("/admin/zones", admin)


# --------------------------------------------------------------------------- #
# Admin — home volumes (every user's) and the access matrix                    #
# --------------------------------------------------------------------------- #

async def admin_home_volumes(request: Request, cm: CM, admin: Admin):
    """Every home volume in the cluster: who owns it, how big, which instance
    is holding it. The question this answers that the per-user page cannot is
    "where has the storage gone", so it is deliberately a flat list rather
    than a per-user drill-down."""
    users = await request.app.state.run(cm.list_all_users)
    rows = []
    for user in users:
        username = user.get("name")
        if not username:
            continue
        for vol in await request.app.state.run(cm.get_home_volumes, username):
            holder = await request.app.state.run(
                cm.home_volume_holder, username, vol)
            access = await request.app.state.run(
                cm.get_user_volume_access, username)
            rows.append({
                **vol,
                "user": username,
                "pvcName": cm.home_volume_pvc_name(vol),
                "inUseBy": holder,
                # Which zones this volume is actually usable in, straight from
                # the matrix — there is nowhere else it could come from.
                "zones": sorted(z for z, cells in access.items()
                                if vol.get("name") in (cells or {})),
            })
    rows.sort(key=lambda r: (r["user"], r.get("name") or ""))
    return templates.TemplateResponse(
        request=request, name="admin/home_volumes.html",
        context=_ctx(admin, is_admin=True, volumes=rows),
    )


async def admin_home_volume_delete(request: Request, cm: CM, admin: Admin,
                                   username: str, name: str,
                                   delete_data: Annotated[Optional[str], Form()] = None):
    ok = await request.app.state.run(
        cm.delete_home_volume, username, name, delete_data == "on")
    if not ok:
        raise HTTPException(
            status_code=400,
            detail="Could not delete that home volume. One attached to a "
                   "running instance must be released first.")
    return _tr("/admin/homevolumes", admin)


async def _matrix_sections(request: Request, cm, username: str = None):
    """Rows of the access grid, grouped by kind.

    ``username`` set     -> that user's home volumes + every dataset.
    ``username`` None    -> datasets only (the group grid).

    A group grid has no home volumes on purpose: a home's name only resolves
    in its owner's own namespace, so a group granting one could never be
    honoured by any member. Groups grant shared data, which is what datasets
    are. PVC volumes are not offered anywhere — the primitive stays in the
    code for templates to use, but it is not something to hand out from this
    screen, and nothing here gates it.
    """
    datasets = await request.app.state.run(cm.get_dataset_definitions)
    sections = []
    if username:
        homes = await request.app.state.run(cm.get_home_volumes, username)
        sections.append({
            "kind": "home", "title": "Home volumes", "enforced": True,
            "note": "Enforced: a session is refused if its home is not "
                    "granted in the zone it runs in.",
            "rows": [{"key": v["name"], "label": v["name"],
                      "description": v.get("description")} for v in homes],
        })
    sections.append({
        "kind": "dataset", "title": "Datasets", "enforced": True,
        "note": "Enforced twice over: the mount is only written for a cell "
                "that exists, and the dataset proxy's NetworkPolicy admits "
                "only this user's pods carrying this zone's label.",
        "rows": [{"key": n, "label": n,
                  "description": (spec or {}).get("description")}
                 for n, spec in sorted((datasets or {}).items())],
    })
    return sections


def _sections_with_values(sections, zones, own, effective):
    """Decorate rows with their own/effective cell per zone, for rendering."""
    out = []
    for section in sections:
        rows = []
        for row in section["rows"]:
            rows.append({**row,
                         "own": {z: (own.get(z) or {}).get(row["key"])
                                 for z in zones},
                         "effective": {z: (effective.get(z) or {}).get(row["key"])
                                       for z in zones}})
        out.append({**section, "rows": rows})
    return out


def _parse_matrix_form(form, zones, sections) -> dict:
    """Read the grid back as ``{zone: {volume: mode}}``.

    A straight replace of the subject's matrix is correct because the grid
    renders every row the subject could be granted, blanks included — so a
    cell absent from the form is one the admin saw and left empty.
    """
    matrix = {}
    for section in sections:
        for row in section["rows"]:
            for zone in zones:
                mode = (form.get(f"access__{zone}__{row['key']}") or "").strip()
                if mode in ACCESS_MODES:
                    matrix.setdefault(zone, {})[row["key"]] = mode
    return matrix


async def admin_user_set_access(request: Request, cm: CM, admin: Admin,
                                username: str):
    form = await request.form()
    zones = await request.app.state.run(cm.get_zones)
    sections = await _matrix_sections(request, cm, username)
    ok = await request.app.state.run(
        cm.set_user_volume_access, username,
        _parse_matrix_form(form, zones, sections))
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to save access.")
    return _tr(f"/admin/users/{username}", admin)


async def admin_computed_access(request: Request, cm: CM, admin: Admin,
                                name: str = None):
    """What a user's access actually resolves to, and where each cell came
    from. Read-only by construction: it is a *derived* view, and offering an
    edit control on it would mean guessing whether the admin meant to change
    the user or the group that granted it.
    """
    users = await request.app.state.run(cm.list_all_users)
    usernames = sorted(u["name"] for u in users if u.get("name"))
    if not name:
        name = usernames[0] if usernames else None
    zones = await request.app.state.run(cm.get_zones)
    sections, groups, effective = [], [], {}
    if name:
        own = next((u.get("volumeAccess") or {}
                    for u in users if u.get("name") == name), {})
        groups = await request.app.state.run(cm.get_user_groups, name)
        effective = await request.app.state.run(cm.get_user_volume_access, name)
        raw = await _matrix_sections(request, cm, name)
        sections = []
        for section in raw:
            rows = []
            for row in section["rows"]:
                cells = {}
                for zone in zones:
                    value = (effective.get(zone) or {}).get(row["key"])
                    # Provenance: the winning value is the most permissive of
                    # the user's own and every group's, so name every source
                    # that actually holds it rather than guessing one.
                    sources = []
                    if (own.get(zone) or {}).get(row["key"]) == value and value:
                        sources.append("own")
                    for group in groups:
                        cell = ((group.get("volumeAccess") or {}).get(zone)
                                or {}).get(row["key"])
                        if cell == value and value:
                            sources.append(group.get("name") or "group")
                    cells[zone] = {"value": value, "sources": sources}
                rows.append({**row, "cells": cells})
            sections.append({**section, "rows": rows})
    return templates.TemplateResponse(
        request=request, name="admin/computed_access.html",
        context=_ctx(admin, is_admin=True, subject=name, usernames=usernames,
                     zones=zones, sections=sections,
                     groups=[g.get("name") for g in groups]),
    )


# --------------------------------------------------------------------------- #
# Admin — datasets (shared S3 data; Dataset CRs)                               #
# --------------------------------------------------------------------------- #
#
# A dataset is NOT a volume, and is deliberately edited somewhere else: every
# entry in the volume catalog is a Kubernetes volume source copied straight
# into a pod spec, which an S3 definition is not. Datasets are granted (in the
# user/group editors) and mounted by cloud-init at boot — never picked as an
# instance mount. See design/storage.md.

def _build_dataset_data(name, description, endpoint, bucket, prefix, region,
                        provider, credentials_secret, read_only):
    return {
        "name": name.strip(),
        "description": (description or "").strip() or None,
        "endpoint": (endpoint or "").strip() or None,
        "bucket": (bucket or "").strip() or None,
        "prefix": (prefix or "").strip().strip("/") or None,
        "region": (region or "").strip() or None,
        "provider": (provider or "").strip() or None,
        "credentialsSecret": (credentials_secret or "").strip() or None,
        # False must survive as an explicit value, not be dropped as empty:
        # unticking "read-only" is how an admin grants write access.
        "readOnly": bool(read_only),
    }


async def _dataset_rows(request: Request, cm):
    defs = await request.app.state.run(cm.get_dataset_definitions)
    rows = []
    for name, spec in sorted(defs.items()):
        has_creds = await request.app.state.run(cm.has_dataset_credentials, name)
        rows.append({"name": name, **(spec or {}), "hasCredentials": has_creds})
    return rows


async def admin_datasets(request: Request, cm: CM, admin: Admin):
    return templates.TemplateResponse(
        request=request, name="admin/datasets.html",
        context=_ctx(admin, is_admin=True,
                     datasets=await _dataset_rows(request, cm)),
    )


async def admin_dataset_new(request: Request, cm: CM, admin: Admin):
    return templates.TemplateResponse(
        request=request, name="admin/dataset_form.html",
        context=_ctx(admin, is_admin=True, dataset=None, has_credentials=False),
    )


async def admin_dataset_create(
    request: Request, cm: CM, admin: Admin,
    name:               Annotated[str, Form()],
    bucket:             Annotated[str, Form()],
    description:        Annotated[Optional[str], Form()] = None,
    endpoint:           Annotated[Optional[str], Form()] = None,
    prefix:             Annotated[Optional[str], Form()] = None,
    region:             Annotated[Optional[str], Form()] = None,
    provider:           Annotated[Optional[str], Form()] = None,
    credentials_secret: Annotated[Optional[str], Form()] = None,
    read_only:          Annotated[Optional[str], Form()] = None,
    access_key_id:      Annotated[Optional[str], Form()] = None,
    secret_access_key:  Annotated[Optional[str], Form()] = None,
):
    name = name.strip()
    if not re.fullmatch(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", name):
        raise HTTPException(
            status_code=400,
            detail="Dataset name must be a DNS-1123 label (lowercase "
                   "alphanumerics and '-', max 63 chars).")
    return await _save_dataset(request, cm, admin, name, description, endpoint,
                               bucket, prefix, region, provider,
                               credentials_secret, read_only, access_key_id,
                               secret_access_key)


async def admin_dataset_edit(request: Request, cm: CM, admin: Admin, name: str):
    defs = await request.app.state.run(cm.get_dataset_definitions)
    if name not in defs:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    has_creds = await request.app.state.run(cm.has_dataset_credentials, name)
    return templates.TemplateResponse(
        request=request, name="admin/dataset_form.html",
        context=_ctx(admin, is_admin=True,
                     dataset={"name": name, **(defs[name] or {})},
                     has_credentials=has_creds),
    )


async def admin_dataset_update(
    request: Request, cm: CM, admin: Admin, name: str,
    bucket:             Annotated[str, Form()],
    description:        Annotated[Optional[str], Form()] = None,
    endpoint:           Annotated[Optional[str], Form()] = None,
    prefix:             Annotated[Optional[str], Form()] = None,
    region:             Annotated[Optional[str], Form()] = None,
    provider:           Annotated[Optional[str], Form()] = None,
    credentials_secret: Annotated[Optional[str], Form()] = None,
    read_only:          Annotated[Optional[str], Form()] = None,
    access_key_id:      Annotated[Optional[str], Form()] = None,
    secret_access_key:  Annotated[Optional[str], Form()] = None,
):
    return await _save_dataset(request, cm, admin, name, description, endpoint,
                               bucket, prefix, region, provider,
                               credentials_secret, read_only, access_key_id,
                               secret_access_key)


async def _save_dataset(request, cm, admin, name, description, endpoint,
                        bucket, prefix, region, provider, credentials_secret,
                        read_only, access_key_id, secret_access_key):
    """Shared by create and update.

    The credential is written FIRST and separately: blank credential fields
    mean "leave it alone", so an edit that only changes the bucket must not
    wipe the key. Nothing here ever reads a credential back — an admin can
    replace one but not retrieve it.
    """
    access_key_id = (access_key_id or "").strip()
    secret_access_key = (secret_access_key or "").strip()
    credentials_secret = (credentials_secret or "").strip()
    if access_key_id and secret_access_key:
        ok = await request.app.state.run(
            cm.save_dataset_credentials, name, access_key_id,
            secret_access_key)
        if not ok:
            raise HTTPException(status_code=500,
                                detail="Failed to store dataset credentials.")
        # Whistler manages this Secret, so point the dataset at it unless the
        # admin named one of their own.
        credentials_secret = (credentials_secret
                              or cm.dataset_credentials_secret_name(name))
    elif access_key_id or secret_access_key:
        raise HTTPException(
            status_code=400,
            detail="Give both the access key and the secret, or neither "
                   "(neither leaves the stored credential unchanged).")
    elif not credentials_secret:
        # Blank everything means "leave the credential alone" — including the
        # LINK to it. Dropping the reference while keeping the Secret leaves a
        # dataset that looks configured and cannot authenticate, which is what
        # an edit that only changed the description used to do.
        existing = await request.app.state.run(cm.get_dataset_definitions)
        credentials_secret = (existing.get(name) or {}).get("credentialsSecret")

    data = _build_dataset_data(name, description, endpoint, bucket, prefix,
                              region, provider, credentials_secret, read_only)
    ok = await request.app.state.run(cm.save_dataset, data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to save dataset.")
    return _tr("/admin/datasets", admin)


async def admin_dataset_delete(request: Request, cm: CM, admin: Admin, name: str):
    ok = await request.app.state.run(cm.delete_dataset, name)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to delete dataset.")
    return _tr("/admin/datasets", admin)


# --------------------------------------------------------------------------- #
# Admin — sessions (all users)                                                 #
# --------------------------------------------------------------------------- #

async def admin_sessions(request: Request, cm: CM, admin: Admin):
    all_instances = await request.app.state.run(cm.get_all_instances)
    return templates.TemplateResponse(
        request=request, name="admin/sessions.html",
        context=_ctx(admin, is_admin=True, all_instances=all_instances),
    )


async def admin_session_stop(request: Request, cm: CM, admin: Admin, username: str, name: str):
    await request.app.state.run(cm.stop_instance, username, name)
    return _tr("/admin/sessions", admin)


async def admin_session_delete(request: Request, cm: CM, admin: Admin, username: str, name: str):
    await request.app.state.run(cm.delete_instance, username, name)
    return _tr("/admin/sessions", admin)


async def admin_sessions_rows(request: Request, cm: CM, admin: Admin):
    """HTMX partial — returns only the <tr> rows for the sessions table."""
    all_instances = await request.app.state.run(cm.get_all_instances)
    return templates.TemplateResponse(
        request=request, name="admin/_sessions_rows.html",
        context=_ctx(admin, is_admin=True, all_instances=all_instances),
    )


async def healthz():
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# Misc helpers                                                                 #
# --------------------------------------------------------------------------- #

def _nonempty(d: dict) -> dict:
    return {k: v.strip() for k, v in d.items() if v and v.strip()}


def _build_session_overrides(*, cpu=None, memory=None,
                             gpu_type=None, gpu_count=None,
                             uid=None, gid=None,
                             run_as_user=None, run_as_group=None,
                             fs_group=None, zone=None) -> Optional[dict]:
    """Assemble a Session spec.overrides payload from the create-instance
    form. A group is only included when the form actually supplied a value
    for it — the form only renders fields for groups the user's User CR
    `overrides` grants (see instance_create_form), but an unfilled field
    should still be a no-op rather than an empty override the operator has
    to authorize against nothing."""
    overrides: dict = {}

    resources = _nonempty({"cpu": cpu, "memory": memory})
    if resources:
        overrides["resources"] = resources

    gpu_type = (gpu_type or "").strip()
    if gpu_type:
        overrides["gpuType"] = gpu_type

    # "No GPU" answers the count question too, so the count field is ignored
    # rather than fought with — the form disables it, but a submission that
    # slipped past that must not end up asking for a card and forbidding one
    # in the same breath. _apply_overrides would resolve it the same way; it
    # is cheaper not to write the contradiction down.
    if gpu_count not in (None, "") and gpu_type != GPU_NONE:
        overrides["gpuCount"] = int(gpu_count)

    if uid not in (None, ""):
        overrides["uid"] = int(uid)
    if gid not in (None, ""):
        overrides["gid"] = int(gid)

    security_context = {}
    if run_as_user not in (None, ""):
        security_context["runAsUser"] = int(run_as_user)
    if run_as_group not in (None, ""):
        security_context["runAsGroup"] = int(run_as_group)
    if fs_group not in (None, ""):
        security_context["fsGroup"] = int(fs_group)
    if security_context:
        overrides["securityContext"] = security_context

    if zone and zone.strip():
        overrides["zone"] = zone.strip()

    return overrides or None


def _template_form_data(*, name, display_name, image, description, cpu, memory,
                        personal_mount, mode, runtime, privileged, fuse,
                        display_port, protocol, gpu=None, gpu_type=None,
                        zone=None) -> dict:
    """Assemble a save_system_template payload from the admin template form,
    including the access mode / runtime / privileged toggles. Desktop-only
    fields are only included when mode == 'desktop'. gpu (count) maps to
    resources.gpu; gpu_type (from the live GPU catalog) maps to
    nodeSelector[GPU_NODE_LABEL], the key _apply_policy checks against a
    user's allowedGpuTypes. zone names a network zone; empty means the
    implicit default, which is "default" and gated like any other zone.

    Every GPU answer names something. ``gpu_type`` is either GPU_NONE — **No
    GPU**: no node selector and no resources.gpu, whatever the count box said —
    or a type from the live catalog. There is no "any available type": it read
    as a choice but was the absence of one, working by luck on a single-GPU
    cluster and meaning nothing on a mixed one.

    So an absent or empty ``gpu_type`` is No GPU too. The form cannot submit
    one (the select is `required`, and the only blank option is the disabled
    placeholder shown for a legacy template that names no type), and treating
    it as "no GPU" rather than "some GPU, unspecified" is the reading that
    keeps every saved template answerable. A count of 0 is No GPU said the long
    way round, normalised here so the two spellings cannot produce different
    specs."""
    no_gpu = (gpu_type or "").strip() in ("", GPU_NONE) or str(gpu or "").strip() == "0"
    data = {
        "name": name,
        "displayName": display_name.strip(),
        "image": image.strip(),
        "description": (description or "").strip(),
        "resources": _nonempty({"cpu": cpu, "memory": memory,
                                "gpu": "" if no_gpu else gpu}),
        "nodeSelector": _nonempty({GPU_NODE_LABEL: "" if no_gpu else gpu_type}),
        "personalMountPath": personal_mount or "/userdata",
        "mode": mode if mode in ("ssh", "desktop") else "ssh",
        "runtime": runtime if runtime in ("container", "kata", "vm") else "container",
        "privileged": privileged == "on",
        "fuse": fuse == "on",
    }
    # Always present (form select; "" = implicit default) so switching a
    # template back to the default zone actually clears the field — the
    # save merges present keys over the existing spec.
    data["zone"] = (zone or "").strip()
    if data["mode"] == "desktop":
        if display_port and display_port.strip():
            data["displayPort"] = int(display_port)
        if protocol:
            data["protocol"] = protocol
    return data


def _build_user_data(name, public_keys, run_as_user, run_as_group, fs_group,
                     uid=None, admin=None) -> dict:
    keys = [k.strip() for k in (public_keys or "").splitlines() if k.strip()]
    # admin is always set (not omitted when falsy) so unchecking the "Admin"
    # box in the edit form actually revokes it, rather than leaving whatever
    # was already on the CR untouched (save_user only merges present keys).
    data: dict = {"name": name.strip(), "publicKeys": keys, "admin": bool(admin)}
    if uid and str(uid).strip():
        data["uid"] = int(uid)
    sec_ctx = _nonempty({
        "runAsUser":  run_as_user  or "",
        "runAsGroup": run_as_group or "",
        "fsGroup":    fs_group     or "",
    })
    if sec_ctx:
        data["securityContext"] = {k: int(v) for k, v in sec_ctx.items()}
    return data


def _parse_members(text) -> list:
    """One username per line (blank lines and stray commas tolerated). A
    textarea rather than a checkbox list of existing users on purpose: a group
    may legitimately name a user who hasn't been created yet, which is how a
    project is provisioned before its people arrive."""
    members = []
    for line in (text or "").replace(",", "\n").splitlines():
        name = line.strip()
        if name and name not in members:
            members.append(name)
    return members


def _build_group_data(name: str, form) -> dict:
    """Build a Group spec from the editor's form.

    Takes the raw form rather than typed parameters because the access grid's
    field names are dynamic (``access__<zone>__<volume>``).

    ``channels`` is omitted entirely unless the "restrict channels" box is
    ticked: absent means "this group does not narrow the zone ceiling", while
    an empty list means "nothing but the desktop stream". Collapsing the two
    would make the second unwritable."""
    data: dict = {
        "name": name,
        "description": (form.get("description") or "").strip(),
        "members": _parse_members(form.get("members")),
    }

    # The access matrix, read the same way the user grid is. Absent means the
    # group grants nothing there — no defaults, as everywhere in this table.
    matrix: dict = {}
    for key in form.keys():
        if not key.startswith("access__"):
            continue
        try:
            _, zone, volume = key.split("__", 2)
        except ValueError:
            continue
        mode = (form.get(key) or "").strip()
        if mode in ACCESS_MODES:
            matrix.setdefault(zone, {})[volume] = mode
    if matrix:
        data["volumeAccess"] = matrix

    zones = [z.strip() for z in form.getlist("zone_names") if z.strip()]
    if zones:
        data["allowedZones"] = zones
    gpu_types = [g.strip() for g in form.getlist("gpu_types") if g.strip()]
    if gpu_types:
        data["allowedGpuTypes"] = gpu_types

    entry_points = set(form.getlist("entry_points"))
    if entry_points:
        # Omitted when nothing is ticked, unlike `channels` above: a group
        # only ever widens, so an empty list and an absent one grant the same
        # nothing, and the absent one says it without looking like a decision.
        data["entryPoints"] = [e for e in ENTRY_POINTS if e in entry_points]

    if form.get("restrict_channels"):
        checked = set(form.getlist("channels"))
        data["channels"] = [c for c in CHANNELS if c in checked]

    checked_overrides = set(form.getlist("override_groups"))
    overrides = {g: True for g in OVERRIDE_GROUPS if g in checked_overrides}
    if overrides:
        data["overrides"] = overrides
    return data




def _parse_block_cidrs(text) -> list:
    """One CIDR per line. Invalid entries are a 400, not a silent drop — a
    typo'd blockCIDR would otherwise widen the zone."""
    cidrs = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            cidrs.append(str(ipaddress.IPv4Network(line, strict=False)))
        except ValueError:
            raise HTTPException(status_code=400,
                                detail=f"Invalid CIDR in blocked list: {line!r}")
    return cidrs


def _parse_allow_cidrs(text) -> list:
    """One entry per line: ``CIDR`` (all ports) or ``CIDR port/proto ...``
    (e.g. ``203.0.113.0/24 443/tcp 53/udp``). This is the flat-text form of
    the Zone CR's egress.allowCIDRs; _format_allow_cidrs is its inverse."""
    entries = []
    for line in (text or "").splitlines():
        parts = line.split()
        if not parts:
            continue
        try:
            cidr = str(ipaddress.IPv4Network(parts[0], strict=False))
        except ValueError:
            raise HTTPException(status_code=400,
                                detail=f"Invalid CIDR in allowed list: {parts[0]!r}")
        entry: dict = {"cidr": cidr}
        ports = []
        for spec in parts[1:]:
            m = re.fullmatch(r"(\d{1,5})/(tcp|udp|sctp)", spec.lower())
            if not m or not 0 < int(m.group(1)) <= 65535:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid port spec {spec!r} (expected port/tcp, "
                           f"port/udp or port/sctp)")
            ports.append({"port": int(m.group(1)), "protocol": m.group(2).upper()})
        if ports:
            entry["ports"] = ports
        entries.append(entry)
    return entries


def _format_allow_cidrs(entries) -> str:
    lines = []
    for entry in entries or []:
        parts = [entry.get("cidr", "")]
        for p in entry.get("ports") or []:
            parts.append(f"{p.get('port')}/{str(p.get('protocol', 'TCP')).lower()}")
        lines.append(" ".join(parts))
    return "\n".join(lines)


def _parse_dns_servers(text) -> list:
    servers = []
    for token in re.split(r"[,\s]+", text or ""):
        if not token:
            continue
        try:
            servers.append(str(ipaddress.IPv4Address(token)))
        except ValueError:
            raise HTTPException(status_code=400,
                                detail=f"Invalid DNS server IP: {token!r}")
    return servers


def _build_zone_data(name, description, allow_cidrs, block_cidrs,
                     dns_cluster_only, dns_servers) -> dict:
    """Assemble a save_zone payload from the zone form. Always carries the
    full config: save_zone replaces the CR spec, so cleared fields clear."""
    zone: dict = {"name": name.strip()}
    if description and description.strip():
        zone["description"] = description.strip()
    egress: dict = {}
    allow = _parse_allow_cidrs(allow_cidrs)
    block = _parse_block_cidrs(block_cidrs)
    if allow:
        egress["allowCIDRs"] = allow
    if block:
        egress["blockCIDRs"] = block
    if egress:
        zone["egress"] = egress
    dns: dict = {}
    if dns_cluster_only == "on":
        dns["clusterOnly"] = True
    servers = _parse_dns_servers(dns_servers)
    if servers:
        dns["servers"] = servers
    if dns:
        zone["dns"] = dns
    return zone


# --------------------------------------------------------------------------- #
# App factory                                                                  #
# --------------------------------------------------------------------------- #

def build_management_app(config_manager):
    import asyncio

    app = FastAPI(title="Whistler Management")
    app.state.cm = config_manager

    async def _run(func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    app.state.run = _run

    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

    # Every route below is gated by require_user, which raises LoginRequired
    # rather than answering 401 itself — so one handler decides what an
    # unauthenticated request sees, and /login and /logout below are the only
    # routes that do not ask.
    app.add_exception_handler(LoginRequired, _login_required)
    app.add_exception_handler(EntryPointDenied, _entry_point_denied)
    app.add_api_route(LOGIN_PATH,  login_form,   methods=["GET"], response_class=HTMLResponse)
    app.add_api_route(LOGIN_PATH,  login_submit, methods=["POST"])
    app.add_api_route("/logout",   logout,       methods=["POST"])

    # User routes
    app.add_api_route("/",                                user_index,            methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/dashboard",                       dashboard,             methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/new",                   instance_create_form,  methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/templates",             instance_template_picker, methods=["GET"], response_class=HTMLResponse)
    app.add_api_route("/instances",                       instance_create,       methods=["POST"])
    app.add_api_route("/instances/{name}",                instance_detail,       methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/{name}/edit",           instance_edit_form,    methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/{name}/update",         instance_update,       methods=["POST"])
    app.add_api_route("/instances/{name}/status-badge",   instance_status_badge, methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/{name}/start-dialog",   instance_start_dialog, methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/{name}/connect",        instance_connect,      methods=["POST"])
    app.add_api_route("/instances/{name}/stop",           instance_stop,         methods=["POST"])
    app.add_api_route("/instances/{name}/delete",         instance_delete,       methods=["POST"])

    # Admin routes
    app.add_api_route("/admin",                                   admin_index,            methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/templates",                         admin_templates,        methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/templates/new",                     admin_template_new,     methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/templates",                         admin_template_create,  methods=["POST"])
    app.add_api_route("/admin/templates/{name}/edit",             admin_template_edit,    methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/templates/{name}/update",           admin_template_update,  methods=["POST"])
    app.add_api_route("/admin/templates/{name}/delete",           admin_template_delete,  methods=["POST"])
    app.add_api_route("/admin/users",                             admin_users,            methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/users/new",                         admin_user_new,         methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/users",                             admin_user_create,      methods=["POST"])
    app.add_api_route("/admin/users/{username}",                  admin_user_detail,      methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/users/{username}/update",           admin_user_update,      methods=["POST"])
    app.add_api_route("/admin/users/{username}/delete",           admin_user_delete,      methods=["POST"])
    app.add_api_route("/admin/users/{username}/gpu-types",        admin_user_set_gpu_types, methods=["POST"])
    app.add_api_route("/admin/users/{username}/zones",            admin_user_set_zones, methods=["POST"])
    app.add_api_route("/admin/users/{username}/overrides",        admin_user_set_overrides, methods=["POST"])
    app.add_api_route("/admin/users/{username}/entry-points",     admin_user_set_entry_points, methods=["POST"])
    app.add_api_route("/admin/users/{username}/channels",         admin_user_set_channels, methods=["POST"])
    app.add_api_route("/admin/groups",                            admin_groups,           methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/groups/new",                        admin_group_new,        methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/groups",                            admin_group_create,     methods=["POST"])
    app.add_api_route("/admin/groups/{name}/edit",                admin_group_edit,       methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/groups/{name}",                     admin_group_update,     methods=["POST"])
    app.add_api_route("/admin/groups/{name}/delete",              admin_group_delete,     methods=["POST"])
    app.add_api_route("/admin/zones",                             admin_zones,            methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/zones/new",                         admin_zone_new,         methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/zones",                             admin_zone_create,      methods=["POST"])
    app.add_api_route("/admin/zones/{name}/edit",                 admin_zone_edit,        methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/zones/{name}",                      admin_zone_update,      methods=["POST"])
    app.add_api_route("/admin/zones/{name}/delete",               admin_zone_delete,      methods=["POST"])
    app.add_api_route("/homes",                                   home_volumes,           methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/homes",                                   home_volume_create,     methods=["POST"])
    app.add_api_route("/homes/{name}/delete",                     home_volume_delete,     methods=["POST"])
    app.add_api_route("/admin/computed-access",                   admin_computed_access,  methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/users/{username}/access",           admin_user_set_access,  methods=["POST"])
    app.add_api_route("/admin/homevolumes",                       admin_home_volumes,     methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/homevolumes/{username}/{name}/delete", admin_home_volume_delete, methods=["POST"])
    app.add_api_route("/admin/datasets",                          admin_datasets,         methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/datasets/new",                      admin_dataset_new,      methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/datasets",                          admin_dataset_create,   methods=["POST"])
    app.add_api_route("/admin/datasets/{name}/edit",              admin_dataset_edit,     methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/datasets/{name}",                   admin_dataset_update,   methods=["POST"])
    app.add_api_route("/admin/datasets/{name}/delete",            admin_dataset_delete,   methods=["POST"])
    app.add_api_route("/admin/sessions",                          admin_sessions,         methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/sessions/{username}/{name}/stop",   admin_session_stop,     methods=["POST"])
    app.add_api_route("/admin/sessions/{username}/{name}/delete", admin_session_delete,   methods=["POST"])
    app.add_api_route("/admin/sessions/rows",                     admin_sessions_rows,    methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/healthz",                                 healthz,                methods=["GET"])

    return app
