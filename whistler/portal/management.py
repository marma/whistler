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
import ipaddress
import logging
import os
import re
from typing import Annotated, Optional

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from whistler.config import GPU_NODE_LABEL, OVERRIDE_GROUPS

logger = logging.getLogger("whistler.management")

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
_STATIC_DIR   = os.path.join(os.path.dirname(__file__), "static")

templates = Jinja2Templates(directory=_TEMPLATE_DIR)

# The various raw pod/CR phases are consolidated into a few user-facing states,
# mirroring the actual pod lifecycle: no pod -> Stopped, deleting -> Stopping,
# scheduled but not started -> Pending, running but not yet ready -> Starting,
# running and ready -> Running.
_STATUS_GROUPS = {
    "running":      "Running",
    "ready":        "Running",
    "pending":      "Pending",
    "initializing": "Starting",
    "provisioning": "Starting",
    "booting":      "Starting",
    "importing":    "Starting",
    "stopping":     "Stopping",
    "terminating":  "Stopping",
    "stopped":      "Stopped",
    "unknown":      "Stopped",
    "failed":       "Error",
}
_GROUP_COLORS = {
    "Running":  "green",
    "Starting": "yellow",
    "Pending":  "blue",
    "Stopping": "orange",
    "Stopped":  "grey",
    "Error":    "red",
}


def _status_group(status: str, ready: bool = True) -> str:
    """Collapse a raw pod/CR phase into one of the user-facing states. A
    "running" phase pod whose containers aren't all ready yet reads as
    Starting rather than Running."""
    s = (status or "").lower()
    if s == "running" and not ready:
        return "Starting"
    return _STATUS_GROUPS.get(s, "Stopped")


# Jinja2 globals: consolidated status label + its Fomantic UI label color.
templates.env.globals["status_label"] = _status_group
templates.env.globals["status_color"] = lambda s, ready=True: _GROUP_COLORS[_status_group(s, ready)]
templates.env.globals["GPU_NODE_LABEL"] = GPU_NODE_LABEL

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


def _render_status_html(name: str, status: str, user: str, controls: bool,
                        connect_url: str = None, term_url: str = None,
                        ready: bool = True, editable: bool = False) -> str:
    """Render the polling status badge. With `controls`, also emit an out-of-band
    swap that re-renders the action buttons (connect/ssh/start/stop/edit) so they
    stay enabled/disabled in step with the status (used on the dashboard; the
    detail view omits it)."""
    tpl = "user/_status_controls.html" if controls else "user/_status_badge.html"
    return templates.env.get_template(tpl).render(
        name=name, status=status, user=user, controls=controls,
        connect_url=connect_url, term_url=term_url, ready=ready, editable=editable,
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


def _screenshot_url(user: str, name: str) -> str:
    """Latest desktop thumbnail, served by the viewer app (same base URL as the
    connect/terminal links). 404 until the first capture pass covers the
    session, so the template hides the image on error rather than reserving
    space for one that may never arrive."""
    return f"{_DESKTOP_PORTAL_URL}/screenshot/{name}?user={user}"


def _terminal_url(user: str, name: str) -> str:
    """Web-terminal (xterm.js) page, served by the same viewer app as the desktop
    relay — so it shares _DESKTOP_PORTAL_URL (empty = same-origin via the proxy)."""
    return f"{_DESKTOP_PORTAL_URL}/term/{name}?user={user}"


def _merge_sessions(instances: list, desktop_sessions: list, user: str) -> list[dict]:
    """Flatten ssh instances + desktop sessions into one list of rows with a
    common shape (name/template/status/mode), so the dashboard can render them in
    a single table. Desktop rows carry the browser viewer URL. Every row gets a
    web-terminal URL (VM sessions get the KubeVirt serial console)."""
    rows: list[dict] = []
    for i in instances:
        rows.append({
            "name": i["name"], "template": i.get("template"),
            "status": i.get("status"), "ready": i.get("ready", True),
            "mode": "ssh", "connect_url": None,
            "term_url": _terminal_url(user, i["name"]),
            # ssh instances have no X display, so nothing to screenshot.
            "screenshot_url": None,
        })
    for s in desktop_sessions:
        rows.append({
            "name": s["name"], "template": s.get("template"),
            "status": s.get("phase"), "ready": True, "mode": "desktop",
            "connect_url": _desktop_viewer_url(user, s["name"]),
            "term_url": _terminal_url(user, s["name"]),
            "screenshot_url": _screenshot_url(user, s["name"]),
        })
    rows.sort(key=lambda r: r["name"])
    return rows


# --------------------------------------------------------------------------- #
# User — dashboard                                                             #
# --------------------------------------------------------------------------- #

async def user_index(request: Request, cm: CM, user: User, is_admin: IsAdmin):
    instances, ssh_tpls, desk_tpls, desktop_sessions = await asyncio.gather(
        request.app.state.run(cm.get_user_instances, user),
        request.app.state.run(cm.get_user_templates, user),
        request.app.state.run(cm.get_user_desktop_templates, user),
        request.app.state.run(cm.get_user_desktop_sessions, user),
    )
    return templates.TemplateResponse(
        request=request, name="user/index.html",
        context=_ctx(user, is_admin=is_admin,
                     instances=_merge_sessions(instances, desktop_sessions, user),
                     tpls=ssh_tpls + desk_tpls),
    )


# --------------------------------------------------------------------------- #
# User — instance CRUD                                                         #
# --------------------------------------------------------------------------- #

async def instance_create_form(request: Request, cm: CM, user: User, is_admin: IsAdmin):
    (ssh_tpls, desk_tpls, volumes, allowed, gpu_types, allowed_gpu_types,
     overrides, zones, allowed_zones) = \
        await asyncio.gather(
            request.app.state.run(cm.get_user_templates, user),
            request.app.state.run(cm.get_user_desktop_templates, user),
            request.app.state.run(cm.get_volumes),
            request.app.state.run(cm.get_user_allowed_volumes, user),
            request.app.state.run(cm.get_gpu_types),
            request.app.state.run(cm.get_user_allowed_gpu_types, user),
            request.app.state.run(cm.get_user_overrides, user),
            request.app.state.run(cm.get_zones),
            request.app.state.run(cm.get_user_allowed_zones, user),
        )
    # The zone picker offers only what _apply_policy would accept: an empty
    # allowedZones means every defined zone.
    selectable_zones = [z for z in zones if not allowed_zones or z in allowed_zones]
    return templates.TemplateResponse(
        request=request, name="user/create_instance.html",
        context=_ctx(user, is_admin=is_admin, tpls=ssh_tpls + desk_tpls, volumes=volumes,
                     allowed_volumes=allowed, gpu_types=gpu_types,
                     allowed_gpu_types=allowed_gpu_types, overrides=overrides,
                     zones=selectable_zones),
    )


async def instance_create(
    request: Request, cm: CM, user: User,
    template_name: Annotated[str, Form()],
    instance_name: Annotated[str, Form()],
    preemptible:   Annotated[Optional[str], Form()] = None,
    volume_names:  Annotated[Optional[list[str]], Form()] = None,
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
    ssh_tpls, desk_tpls = await asyncio.gather(
        request.app.state.run(cm.get_user_templates, user),
        request.app.state.run(cm.get_user_desktop_templates, user),
    )
    tpl = next((t for t in (ssh_tpls + desk_tpls)
                if t.get("fullName") == template_name or t.get("name") == template_name), None)
    mode = (tpl or {}).get("mode", "ssh")

    # Volume mount paths are keyed per-volume (mount_path__<name>) rather than
    # a parallel list, since volume_names only carries the *checked* boxes —
    # a positional zip would misalign whenever a box in the middle is left
    # unchecked.
    volumes_override = None
    if volume_names:
        form = await request.form()
        volumes_override = {
            v: (form.get(f"mount_path__{v}") or f"/mnt/{v}").strip()
            for v in volume_names
        }
    overrides = _build_session_overrides(
        volumes=volumes_override,
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
        cm.add_instance, user, template_name, name, preemptible == "on", overrides,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create instance.")
    return _tr(f"/instances/{name}", user)


async def instance_edit_form(request: Request, cm: CM, user: User, is_admin: IsAdmin, name: str):
    (instances, desktop_sessions, ssh_tpls, desk_tpls, volumes, allowed, gpu_types,
     allowed_gpu_types, overrides, zones, allowed_zones, cur) = \
        await asyncio.gather(
            request.app.state.run(cm.get_user_instances, user),
            request.app.state.run(cm.get_user_desktop_sessions, user),
            request.app.state.run(cm.get_user_templates, user),
            request.app.state.run(cm.get_user_desktop_templates, user),
            request.app.state.run(cm.get_volumes),
            request.app.state.run(cm.get_user_allowed_volumes, user),
            request.app.state.run(cm.get_gpu_types),
            request.app.state.run(cm.get_user_allowed_gpu_types, user),
            request.app.state.run(cm.get_user_overrides, user),
            request.app.state.run(cm.get_zones),
            request.app.state.run(cm.get_user_allowed_zones, user),
            request.app.state.run(cm.get_instance_config, user, name),
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
    if _status_group(inst["status"], inst.get("ready", True)) not in ("Stopped", "Error"):
        raise HTTPException(status_code=409, detail="Stop the instance before editing it.")

    selectable_zones = [z for z in zones if not allowed_zones or z in allowed_zones]
    return templates.TemplateResponse(
        request=request, name="user/edit_instance.html",
        context=_ctx(user, is_admin=is_admin, inst=inst, cur=cur,
                     tpls=ssh_tpls + desk_tpls, volumes=volumes,
                     allowed_volumes=allowed, gpu_types=gpu_types,
                     allowed_gpu_types=allowed_gpu_types, overrides=overrides,
                     zones=selectable_zones),
    )


async def instance_update(
    request: Request, cm: CM, user: User, name: str,
    preemptible:   Annotated[Optional[str], Form()] = None,
    volume_names:  Annotated[Optional[list[str]], Form()] = None,
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
    # Same override-assembly as instance_create (per-volume mount paths keyed by
    # name so unchecked boxes don't misalign a positional zip).
    volumes_override = None
    if volume_names:
        form = await request.form()
        volumes_override = {
            v: (form.get(f"mount_path__{v}") or f"/mnt/{v}").strip()
            for v in volume_names
        }
    overrides = _build_session_overrides(
        volumes=volumes_override,
        cpu=override_cpu, memory=override_memory,
        gpu_type=override_gpu_type, gpu_count=override_gpu_count,
        uid=override_uid, gid=override_gid,
        run_as_user=override_run_as_user, run_as_group=override_run_as_group,
        fs_group=override_fs_group, zone=override_zone,
    )
    ok = await request.app.state.run(
        cm.update_instance, user, name, preemptible == "on", overrides,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to update instance.")
    return _tr(f"/instances/{name}", user)


async def instance_detail(request: Request, cm: CM, user: User, is_admin: IsAdmin, name: str):
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
            inst = {**sess, "status": sess.get("phase"), "ready": True}
    if not inst:
        raise HTTPException(status_code=404, detail="Instance not found.")
    # Both ssh instances and desktop/VM sessions are Session CRs whose
    # spec.overrides can be edited while at rest.
    inst = {**inst, "editable": True}
    return templates.TemplateResponse(
        request=request, name="user/instance_detail.html",
        context=_ctx(user, is_admin=is_admin, inst=inst),
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
    # Both ssh instances and desktop/VM sessions are Session CRs with an editable
    # spec.overrides, so both get an Edit action.
    editable = False
    if inst:
        status, connect_url = inst["status"], None
        ready = inst.get("ready", True)
        term_url = _terminal_url(user, name)
        editable = True
    else:
        sess = next((s for s in desktop_sessions if s["name"] == name), None)
        if sess:
            status = sess["phase"]
            ready = True
            connect_url = _desktop_viewer_url(user, name)
            term_url = _terminal_url(user, name)
            editable = True
        else:
            status, connect_url, term_url = "Unknown", None, None
            ready = True
    return HTMLResponse(
        _render_status_html(name, status, user, controls, connect_url, term_url,
                            ready, editable))


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
    resources, all_instances = await asyncio.gather(
        request.app.state.run(cm.get_cluster_resources),
        request.app.state.run(cm.get_all_instances),
    )
    running = sorted(
        (i for i in all_instances if i.get("status") == "Running"),
        key=lambda i: (i["username"], i["name"]),
    )
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
    gpu_types = await request.app.state.run(cm.get_gpu_types)
    allowed_gpu_types = await request.app.state.run(cm.get_user_allowed_gpu_types, username)
    user_overrides = await request.app.state.run(cm.get_user_overrides, username)
    zones = await request.app.state.run(cm.get_zones)
    allowed_zones = await request.app.state.run(cm.get_user_allowed_zones, username)
    return templates.TemplateResponse(
        request=request, name="admin/user_detail.html",
        context=_ctx(admin, is_admin=True, user_obj=user_obj, instances=instances,
                     volumes=volumes, allowed_volumes=allowed,
                     gpu_types=gpu_types, allowed_gpu_types=allowed_gpu_types,
                     user_overrides=user_overrides, override_groups=OVERRIDE_GROUPS,
                     zones=zones, allowed_zones=allowed_zones),
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


async def admin_user_set_volumes(
    request: Request, cm: CM, admin: Admin, username: str,
    volume_names: Annotated[Optional[list[str]], Form()] = None,
):
    await request.app.state.run(cm.set_user_allowed_volumes, username, volume_names or [])
    return _tr(f"/admin/users/{username}", admin)


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


# --------------------------------------------------------------------------- #
# Admin — volumes                                                              #
# --------------------------------------------------------------------------- #

async def admin_volumes(request: Request, cm: CM, admin: Admin):
    volumes = await request.app.state.run(cm.get_volumes)
    return templates.TemplateResponse(
        request=request, name="admin/volumes.html",
        context=_ctx(admin, is_admin=True, volumes=volumes),
    )


async def admin_volume_new(request: Request, cm: CM, admin: Admin):
    return templates.TemplateResponse(
        request=request, name="admin/volume_form.html",
        context=_ctx(admin, is_admin=True, vol=None),
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


def _build_session_overrides(*, volumes=None, cpu=None, memory=None,
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

    if gpu_type and gpu_type.strip():
        overrides["gpuType"] = gpu_type.strip()

    if gpu_count not in (None, ""):
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

    if volumes:
        overrides["volumes"] = volumes

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
    resources.gpu; gpu_type (from the gpuTypes catalog) maps to
    nodeSelector[GPU_NODE_LABEL], the key _apply_policy checks against a
    user's allowedGpuTypes. zone names a network zone; empty means the
    implicit default."""
    data = {
        "name": name,
        "displayName": display_name.strip(),
        "image": image.strip(),
        "description": (description or "").strip(),
        "resources": _nonempty({"cpu": cpu, "memory": memory, "gpu": gpu}),
        "nodeSelector": _nonempty({GPU_NODE_LABEL: gpu_type}),
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


def _build_volume_data(name, pvc_name, sub_path) -> dict:
    vol: dict = {
        "name": name.strip(),
        "persistentVolumeClaim": {"claimName": pvc_name.strip()},
    }
    if sub_path and sub_path.strip():
        vol["subPath"] = sub_path.strip()
    return vol


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

    # User routes
    app.add_api_route("/",                                user_index,            methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/dashboard",                       dashboard,             methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/new",                   instance_create_form,  methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances",                       instance_create,       methods=["POST"])
    app.add_api_route("/instances/{name}",                instance_detail,       methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/{name}/edit",           instance_edit_form,    methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/instances/{name}/update",         instance_update,       methods=["POST"])
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
    app.add_api_route("/admin/users/{username}/gpu-types",        admin_user_set_gpu_types, methods=["POST"])
    app.add_api_route("/admin/users/{username}/zones",            admin_user_set_zones, methods=["POST"])
    app.add_api_route("/admin/users/{username}/overrides",        admin_user_set_overrides, methods=["POST"])
    app.add_api_route("/admin/volumes",                           admin_volumes,          methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/volumes/new",                       admin_volume_new,       methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/volumes",                           admin_volume_create,    methods=["POST"])
    app.add_api_route("/admin/volumes/{name}/delete",             admin_volume_delete,    methods=["POST"])
    app.add_api_route("/admin/zones",                             admin_zones,            methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/zones/new",                         admin_zone_new,         methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/zones",                             admin_zone_create,      methods=["POST"])
    app.add_api_route("/admin/zones/{name}/edit",                 admin_zone_edit,        methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/zones/{name}",                      admin_zone_update,      methods=["POST"])
    app.add_api_route("/admin/zones/{name}/delete",               admin_zone_delete,      methods=["POST"])
    app.add_api_route("/admin/sessions",                          admin_sessions,         methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/admin/sessions/{username}/{name}/stop",   admin_session_stop,     methods=["POST"])
    app.add_api_route("/admin/sessions/{username}/{name}/delete", admin_session_delete,   methods=["POST"])
    app.add_api_route("/admin/sessions/rows",                     admin_sessions_rows,    methods=["GET"],  response_class=HTMLResponse)
    app.add_api_route("/healthz",                                 healthz,                methods=["GET"])

    return app
