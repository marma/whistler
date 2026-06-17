"""FastAPI management app: admin + user web UI for Whistler.

Routes
------
/               user dashboard — list instances and available templates
/instances/*    user instance lifecycle (create, connect, stop, delete)
/admin/*        admin-only: templates, users, volumes, all sessions

Auth (dev-only)
---------------
Set WHISTLER_AUTH_ALLOW_ANY=true and pass ?user=<name> (or X-Whistler-User header).
Set WHISTLER_ADMIN_USERS=alice,bob to grant admin to specific users, or
WHISTLER_AUTH_ALLOW_ADMIN=true to treat every authenticated user as admin (dev).
"""
import asyncio
import logging
import os
from typing import Annotated, Optional

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

logger = logging.getLogger("whistler.management")

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
_STATIC_DIR   = os.path.join(os.path.dirname(__file__), "static")

templates = Jinja2Templates(directory=_TEMPLATE_DIR)

# The various raw pod/CR phases are consolidated into a few user-facing states.
_STATUS_GROUPS = {
    "running":      "Running",
    "ready":        "Running",
    "pending":      "Starting",
    "initializing": "Starting",
    "provisioning": "Starting",
    "booting":      "Starting",
    "stopping":     "Stopping …",
    "terminating":  "Stopping …",
    "stopped":      "Stopped",
    "unknown":      "Stopped",
    "failed":       "Error",
}
_GROUP_COLORS = {
    "Running":    "green",
    "Starting":   "yellow",
    "Stopping …": "orange",
    "Stopped":    "grey",
    "Error":      "red",
}


def _status_group(status: str) -> str:
    """Collapse a raw pod/CR phase into one of the user-facing states."""
    return _STATUS_GROUPS.get((status or "").lower(), "Stopped")


# Jinja2 globals: consolidated status label + its Fomantic UI label color.
templates.env.globals["status_label"] = _status_group
templates.env.globals["status_color"] = lambda s: _GROUP_COLORS[_status_group(s)]

_ADMIN_USERS: set[str] = set(
    u.strip() for u in os.environ.get("WHISTLER_ADMIN_USERS", "").split(",") if u.strip()
)
_ALLOW_ADMIN = os.environ.get("WHISTLER_AUTH_ALLOW_ADMIN", "false").lower() == "true"


# --------------------------------------------------------------------------- #
# Auth helpers                                                                 #
# --------------------------------------------------------------------------- #

def _get_identity(request: Request) -> Optional[str]:
    if os.environ.get("WHISTLER_AUTH_ALLOW_ANY") != "true":
        return None
    raw = request.headers.get("X-Whistler-User") or request.query_params.get("user") or "user"
    return raw.split("-")[0]


def require_user(request: Request):
    user = _get_identity(request)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Portal auth not configured. Set WHISTLER_AUTH_ALLOW_ANY=true for dev.",
        )
    return user


def require_admin(request: Request):
    user = require_user(request)
    if not _ALLOW_ADMIN and user not in _ADMIN_USERS:
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


def _ctx(user: str, **extra) -> dict:
    """Build template context (request is passed separately to TemplateResponse)."""
    is_admin = _ALLOW_ADMIN or user in _ADMIN_USERS
    return {"current_user": user, "is_admin": is_admin, **extra}


def _tr(url: str, user: str) -> RedirectResponse:
    sep = "&" if "?" in url else "?"
    return RedirectResponse(f"{url}{sep}user={user}", status_code=303)


def _render_status_html(name: str, status: str, user: str, controls: bool,
                        connect_url: str = None, term_url: str = None) -> str:
    """Render the polling status badge. With `controls`, also emit an out-of-band
    swap that re-renders the action buttons (connect/ssh/start/stop) so they stay
    enabled/disabled in step with the status (used on the dashboard; the detail
    view omits it)."""
    tpl = "user/_status_controls.html" if controls else "user/_status_badge.html"
    return templates.env.get_template(tpl).render(
        name=name, status=status, user=user, controls=controls,
        connect_url=connect_url, term_url=term_url,
    )


# Both kinds of session are now one `Session` CR (ssh / desktop mode); the user
# dashboard lists them in a single table. Desktop sessions connect in the browser
# through the guacd relay (whistler.portal.app), which runs on its own port — so
# the "Connect" link needs that relay's base URL. WHISTLER_DESKTOP_PORTAL_URL
# overrides it (e.g. "https://desktops.example.com"); empty means same-origin
# (works when the relay is reachable on the same host, e.g. behind one ingress).
_DESKTOP_PORTAL_URL = os.environ.get("WHISTLER_DESKTOP_PORTAL_URL", "").rstrip("/")


def _desktop_viewer_url(user: str, name: str) -> str:
    return f"{_DESKTOP_PORTAL_URL}/connect/{name}?user={user}"


def _terminal_url(user: str, name: str) -> str:
    """Web-terminal (xterm.js) page, served by the same viewer app as the desktop
    relay — so it shares _DESKTOP_PORTAL_URL (empty = same-origin via the proxy)."""
    return f"{_DESKTOP_PORTAL_URL}/term/{name}?user={user}"


def _merge_sessions(instances: list, desktop_sessions: list, user: str) -> list[dict]:
    """Flatten ssh instances + desktop sessions into one list of rows with a
    common shape (name/template/status/mode), so the dashboard can render them in
    a single table. Desktop rows carry the browser viewer URL. Every row gets a
    web-terminal URL except VM-runtime desktops (no pod to exec into)."""
    rows: list[dict] = []
    for i in instances:
        rows.append({
            "name": i["name"], "template": i.get("template"),
            "status": i.get("status"), "mode": "ssh", "connect_url": None,
            "term_url": _terminal_url(user, i["name"]),
        })
    for s in desktop_sessions:
        rows.append({
            "name": s["name"], "template": s.get("template"),
            "status": s.get("phase"), "mode": "desktop",
            "connect_url": _desktop_viewer_url(user, s["name"]),
            "term_url": None if s.get("runtime") == "vm" else _terminal_url(user, s["name"]),
        })
    rows.sort(key=lambda r: r["name"])
    return rows


# --------------------------------------------------------------------------- #
# User — dashboard                                                             #
# --------------------------------------------------------------------------- #

async def user_index(request: Request, cm: CM, user: User):
    instances, ssh_tpls, desk_tpls, desktop_sessions = await asyncio.gather(
        request.app.state.run(cm.get_user_instances, user),
        request.app.state.run(cm.get_user_templates, user),
        request.app.state.run(cm.get_user_desktop_templates, user),
        request.app.state.run(cm.get_user_desktop_sessions, user),
    )
    return templates.TemplateResponse(
        request=request, name="user/index.html",
        context=_ctx(user, instances=_merge_sessions(instances, desktop_sessions, user),
                     tpls=ssh_tpls + desk_tpls),
    )


# --------------------------------------------------------------------------- #
# User — instance CRUD                                                         #
# --------------------------------------------------------------------------- #

async def instance_create_form(request: Request, cm: CM, user: User):
    ssh_tpls, desk_tpls, volumes, allowed = await asyncio.gather(
        request.app.state.run(cm.get_user_templates, user),
        request.app.state.run(cm.get_user_desktop_templates, user),
        request.app.state.run(cm.get_volumes),
        request.app.state.run(cm.get_user_allowed_volumes, user),
    )
    return templates.TemplateResponse(
        request=request, name="user/create_instance.html",
        context=_ctx(user, tpls=ssh_tpls + desk_tpls, volumes=volumes,
                     allowed_volumes=allowed),
    )


async def instance_create(
    request: Request, cm: CM, user: User,
    template_name: Annotated[str, Form()],
    instance_name: Annotated[str, Form()],
    preemptible:   Annotated[Optional[str], Form()] = None,
):
    name = instance_name.strip()
    # The template carries the access mode; create the matching Session. Desktop
    # sessions are connected from the desktop portal, ssh ones via the SSH bridge.
    ssh_tpls, desk_tpls = await asyncio.gather(
        request.app.state.run(cm.get_user_templates, user),
        request.app.state.run(cm.get_user_desktop_templates, user),
    )
    tpl = next((t for t in (ssh_tpls + desk_tpls)
                if t.get("fullName") == template_name or t.get("name") == template_name), None)
    mode = (tpl or {}).get("mode", "ssh")

    if mode == "desktop":
        ok = await request.app.state.run(cm.add_desktop_session, user, template_name, name)
        if not ok:
            raise HTTPException(status_code=500, detail="Failed to create desktop session.")
        return _tr("/", user)

    ok = await request.app.state.run(
        cm.add_instance, user, template_name, name, preemptible == "on",
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create instance.")
    return _tr(f"/instances/{name}", user)


async def instance_detail(request: Request, cm: CM, user: User, name: str):
    instances, desktop_sessions = await asyncio.gather(
        request.app.state.run(cm.get_user_instances, user),
        request.app.state.run(cm.get_user_desktop_sessions, user),
    )
    inst = next((i for i in instances if i["name"] == name), None)
    if inst is None:
        # Desktop sessions carry their state under "phase"; normalise to "status"
        # so the shared detail template renders them too.
        sess = next((s for s in desktop_sessions if s["name"] == name), None)
        if sess is not None:
            inst = {**sess, "status": sess.get("phase")}
    if not inst:
        raise HTTPException(status_code=404, detail="Instance not found.")
    return templates.TemplateResponse(
        request=request, name="user/instance_detail.html",
        context=_ctx(user, inst=inst),
    )


async def _status_badge_response(request: Request, cm, user: str, name: str,
                                 controls: bool) -> HTMLResponse:
    """Look up the current consolidated status (ssh pod phase or desktop CR phase)
    and render the polling badge (with Start/Stop buttons when `controls`)."""
    instances, desktop_sessions = await asyncio.gather(
        request.app.state.run(cm.get_user_instances, user),
        request.app.state.run(cm.get_user_desktop_sessions, user),
    )
    inst = next((i for i in instances if i["name"] == name), None)
    if inst:
        status, connect_url = inst["status"], None
        term_url = _terminal_url(user, name)
    else:
        sess = next((s for s in desktop_sessions if s["name"] == name), None)
        if sess:
            status = sess["phase"]
            connect_url = _desktop_viewer_url(user, name)
            term_url = None if sess.get("runtime") == "vm" else _terminal_url(user, name)
        else:
            status, connect_url, term_url = "Unknown", None, None
    return HTMLResponse(
        _render_status_html(name, status, user, controls, connect_url, term_url))


async def instance_status_badge(request: Request, cm: CM, user: User, name: str):
    """HTMX polling endpoint — returns the status badge span. The dashboard passes
    ?controls=1 to also refresh the Start/Stop buttons (out-of-band); the detail
    view polls without it and gets just the badge."""
    controls = request.query_params.get("controls") == "1"
    return await _status_badge_response(request, cm, user, name, controls)


async def instance_connect(request: Request, cm: CM, user: User, name: str):
    ok = await request.app.state.run(cm.trigger_instance_start, user, name)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to start instance.")
    # Dashboard buttons post via HTMX: swap the status badge (and buttons) in place
    # instead of navigating to the detail view. Plain form posts (detail) redirect.
    if request.headers.get("HX-Request"):
        return await _status_badge_response(request, cm, user, name, controls=True)
    return _tr(f"/instances/{name}", user)


async def instance_stop(request: Request, cm: CM, user: User, name: str):
    await request.app.state.run(cm.stop_instance, user, name)
    if request.headers.get("HX-Request"):
        return await _status_badge_response(request, cm, user, name, controls=True)
    return _tr(f"/instances/{name}", user)


async def instance_delete(request: Request, cm: CM, user: User, name: str):
    await request.app.state.run(cm.delete_instance, user, name)
    return _tr("/", user)


# --------------------------------------------------------------------------- #
# Admin — overview                                                             #
# --------------------------------------------------------------------------- #

async def admin_index(request: Request, cm: CM, admin: Admin):
    all_instances = await request.app.state.run(cm.get_all_instances)
    all_users     = await request.app.state.run(cm.list_all_users)
    all_templates = await request.app.state.run(cm.get_all_templates)
    return templates.TemplateResponse(
        request=request, name="admin/index.html",
        context=_ctx(admin, all_instances=all_instances,
                     all_users=all_users, all_templates=all_templates),
    )


# --------------------------------------------------------------------------- #
# Admin — templates                                                            #
# --------------------------------------------------------------------------- #

async def admin_templates(request: Request, cm: CM, admin: Admin):
    tpls = await request.app.state.run(cm.get_all_templates)
    return templates.TemplateResponse(
        request=request, name="admin/templates.html",
        context=_ctx(admin, tpls=tpls),
    )


async def admin_template_new(request: Request, cm: CM, admin: Admin):
    images = await request.app.state.run(cm.get_available_images)
    return templates.TemplateResponse(
        request=request, name="admin/template_form.html",
        context=_ctx(admin, tpl=None, available_images=images),
    )


async def admin_template_create(
    request: Request, cm: CM, admin: Admin,
    display_name:   Annotated[str, Form()],
    slug:           Annotated[str, Form()],
    image:          Annotated[str, Form()],
    description:    Annotated[Optional[str], Form()] = None,
    cpu:            Annotated[Optional[str], Form()] = None,
    memory:         Annotated[Optional[str], Form()] = None,
    personal_mount: Annotated[Optional[str], Form()] = "/userdata",
    mode:           Annotated[str, Form()] = "ssh",
    runtime:        Annotated[str, Form()] = "container",
    privileged:     Annotated[Optional[str], Form()] = None,
    fuse:           Annotated[Optional[str], Form()] = None,
    display_port:   Annotated[Optional[str], Form()] = None,
    protocol:       Annotated[Optional[str], Form()] = None,
):
    data = _template_form_data(
        name=slug.strip(), display_name=display_name, image=image,
        description=description, cpu=cpu, memory=memory, personal_mount=personal_mount,
        mode=mode, runtime=runtime, privileged=privileged, fuse=fuse,
        display_port=display_port, protocol=protocol,
    )
    ok = await request.app.state.run(cm.save_system_template, data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create template.")
    return _tr("/admin/templates", admin)


async def admin_template_edit(request: Request, cm: CM, admin: Admin, name: str):
    tpls, images = await asyncio.gather(
        request.app.state.run(cm.get_all_templates),
        request.app.state.run(cm.get_available_images),
    )
    tpl = next((t for t in tpls if t.get("fullName") == name or t.get("name") == name), None)
    if not tpl:
        raise HTTPException(status_code=404, detail="Template not found.")
    return templates.TemplateResponse(
        request=request, name="admin/template_form.html",
        context=_ctx(admin, tpl=tpl, available_images=images),
    )


async def admin_template_update(
    request: Request, cm: CM, admin: Admin, name: str,
    display_name:   Annotated[str, Form()],
    image:          Annotated[str, Form()],
    description:    Annotated[Optional[str], Form()] = None,
    cpu:            Annotated[Optional[str], Form()] = None,
    memory:         Annotated[Optional[str], Form()] = None,
    personal_mount: Annotated[Optional[str], Form()] = "/userdata",
    mode:           Annotated[str, Form()] = "ssh",
    runtime:        Annotated[str, Form()] = "container",
    privileged:     Annotated[Optional[str], Form()] = None,
    fuse:           Annotated[Optional[str], Form()] = None,
    display_port:   Annotated[Optional[str], Form()] = None,
    protocol:       Annotated[Optional[str], Form()] = None,
):
    data = _template_form_data(
        name=name, display_name=display_name, image=image,
        description=description, cpu=cpu, memory=memory, personal_mount=personal_mount,
        mode=mode, runtime=runtime, privileged=privileged, fuse=fuse,
        display_port=display_port, protocol=protocol,
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
        context=_ctx(admin, users=users),
    )


async def admin_user_new(request: Request, cm: CM, admin: Admin):
    return templates.TemplateResponse(
        request=request, name="admin/user_form.html",
        context=_ctx(admin, user_obj=None),
    )


async def admin_user_create(
    request: Request, cm: CM, admin: Admin,
    name:         Annotated[str, Form()],
    public_keys:  Annotated[Optional[str], Form()] = None,
    run_as_user:  Annotated[Optional[str], Form()] = None,
    run_as_group: Annotated[Optional[str], Form()] = None,
    fs_group:     Annotated[Optional[str], Form()] = None,
):
    user_data = _build_user_data(name, public_keys, run_as_user, run_as_group, fs_group)
    ok = await request.app.state.run(cm.save_user, user_data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create user.")
    return _tr(f"/admin/users/{name}", admin)


async def admin_user_detail(request: Request, cm: CM, admin: Admin, username: str):
    all_users = await request.app.state.run(cm.list_all_users)
    user_obj  = next((u for u in all_users if u.get("name") == username), None)
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found.")
    instances = await request.app.state.run(cm.get_user_instances, username)
    volumes   = await request.app.state.run(cm.get_volumes)
    allowed   = await request.app.state.run(cm.get_user_allowed_volumes, username)
    return templates.TemplateResponse(
        request=request, name="admin/user_detail.html",
        context=_ctx(admin, user_obj=user_obj, instances=instances,
                     volumes=volumes, allowed_volumes=allowed),
    )


async def admin_user_update(
    request: Request, cm: CM, admin: Admin, username: str,
    public_keys:  Annotated[Optional[str], Form()] = None,
    run_as_user:  Annotated[Optional[str], Form()] = None,
    run_as_group: Annotated[Optional[str], Form()] = None,
    fs_group:     Annotated[Optional[str], Form()] = None,
):
    user_data = _build_user_data(username, public_keys, run_as_user, run_as_group, fs_group)
    ok = await request.app.state.run(cm.save_user, user_data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to update user.")
    return _tr(f"/admin/users/{username}", admin)


async def admin_user_delete(request: Request, cm: CM, admin: Admin, username: str):
    await request.app.state.run(cm.delete_user, username)
    return _tr("/admin/users", admin)


async def admin_user_set_volumes(
    request: Request, cm: CM, admin: Admin, username: str,
    volume_names: Annotated[Optional[list[str]], Form()] = None,
):
    await request.app.state.run(cm.set_user_allowed_volumes, username, volume_names or [])
    return _tr(f"/admin/users/{username}", admin)


# --------------------------------------------------------------------------- #
# Admin — volumes                                                              #
# --------------------------------------------------------------------------- #

async def admin_volumes(request: Request, cm: CM, admin: Admin):
    volumes = await request.app.state.run(cm.get_volumes)
    return templates.TemplateResponse(
        request=request, name="admin/volumes.html",
        context=_ctx(admin, volumes=volumes),
    )


async def admin_volume_new(request: Request, cm: CM, admin: Admin):
    return templates.TemplateResponse(
        request=request, name="admin/volume_form.html",
        context=_ctx(admin, vol=None),
    )


async def admin_volume_create(
    request: Request, cm: CM, admin: Admin,
    name:     Annotated[str, Form()],
    pvc_name: Annotated[str, Form()],
    sub_path: Annotated[Optional[str], Form()] = None,
):
    vol_data = _build_volume_data(name, pvc_name, sub_path)
    ok = await request.app.state.run(cm.save_volume, vol_data)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create volume.")
    return _tr("/admin/volumes", admin)


async def admin_volume_delete(request: Request, cm: CM, admin: Admin, name: str):
    await request.app.state.run(cm.delete_volume, name)
    return _tr("/admin/volumes", admin)


# --------------------------------------------------------------------------- #
# Admin — sessions (all users)                                                 #
# --------------------------------------------------------------------------- #

async def admin_sessions(request: Request, cm: CM, admin: Admin):
    all_instances = await request.app.state.run(cm.get_all_instances)
    return templates.TemplateResponse(
        request=request, name="admin/sessions.html",
        context=_ctx(admin, all_instances=all_instances),
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
        context=_ctx(admin, all_instances=all_instances),
    )


async def healthz():
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# Misc helpers                                                                 #
# --------------------------------------------------------------------------- #

def _nonempty(d: dict) -> dict:
    return {k: v.strip() for k, v in d.items() if v and v.strip()}


def _template_form_data(*, name, display_name, image, description, cpu, memory,
                        personal_mount, mode, runtime, privileged, fuse,
                        display_port, protocol) -> dict:
    """Assemble a save_system_template payload from the admin template form,
    including the access mode / runtime / privileged toggles. Desktop-only
    fields are only included when mode == 'desktop'."""
    data = {
        "name": name,
        "displayName": display_name.strip(),
        "image": image.strip(),
        "description": (description or "").strip(),
        "resources": _nonempty({"cpu": cpu, "memory": memory}),
        "personalMountPath": personal_mount or "/userdata",
        "mode": mode if mode in ("ssh", "desktop") else "ssh",
        "runtime": runtime if runtime in ("container", "kata", "vm") else "container",
        "privileged": privileged == "on",
        "fuse": fuse == "on",
    }
    if data["mode"] == "desktop":
        if display_port and display_port.strip():
            data["displayPort"] = int(display_port)
        if protocol:
            data["protocol"] = protocol
    return data


def _build_user_data(name, public_keys, run_as_user, run_as_group, fs_group) -> dict:
    keys = [k.strip() for k in (public_keys or "").splitlines() if k.strip()]
    data: dict = {"name": name.strip(), "publicKeys": keys}
    sec_ctx = _nonempty({
        "runAsUser":  run_as_user  or "",
        "runAsGroup": run_as_group or "",
        "fsGroup":    fs_group     or "",
    })
    if sec_ctx:
        data["securityContext"] = {k: int(v) for k, v in sec_ctx.items()}
    return data


def _build_volume_data(name, pvc_name, sub_path) -> dict:
    vol: dict = {
        "name": name.strip(),
        "persistentVolumeClaim": {"claimName": pvc_name.strip()},
    }
    if sub_path and sub_path.strip():
        vol["subPath"] = sub_path.strip()
    return vol


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

    # User routes
    app.add_api_route("/",                                user_index,            methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/new",                   instance_create_form,  methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances",                       instance_create,       methods=["POST"])
    app.add_api_route("/instances/{name}",                instance_detail,       methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/{name}/status-badge",   instance_status_badge, methods=["GET"],  response_class=HTMLResponse)
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
    app.add_api_route("/admin/users/{username}/volumes",          admin_user_set_volumes, methods=["POST"])
    app.add_api_route("/admin/volumes",                           admin_volumes,          methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/volumes/new",                       admin_volume_new,       methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/volumes",                           admin_volume_create,    methods=["POST"])
    app.add_api_route("/admin/volumes/{name}/delete",             admin_volume_delete,    methods=["POST"])
    app.add_api_route("/admin/sessions",                          admin_sessions,         methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/sessions/{username}/{name}/stop",   admin_session_stop,     methods=["POST"])
    app.add_api_route("/admin/sessions/{username}/{name}/delete", admin_session_delete,   methods=["POST"])
    app.add_api_route("/admin/sessions/rows",                     admin_sessions_rows,    methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/healthz",                                 healthz,                methods=["GET"])

    return app
