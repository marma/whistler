"""Roles: what an account may *do* in the portal (config.ROLE_PERMISSIONS).

basic starts and connects; standard also makes its own instances and homes;
manager also acts for other accounts below it; admin does everything. The
routes are called directly, with the role on the request the way
require_user leaves it."""
import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from whistler.config import (PERM_ADMIN, PERM_OTHERS_INSTANCES, PERM_OWN_HOMES,
                             PERM_OWN_INSTANCES, PERM_USER_ACCESS, ROLE_ADMIN,
                             ROLE_BASIC, ROLE_MANAGER, ROLE_STANDARD, ROLES,
                             may_manage_user, role_may, user_role)
from whistler.portal import management as mgmt


# --- the table ------------------------------------------------------------- #

@pytest.mark.parametrize("spec, expected", [
    ({}, ROLE_STANDARD),                      # what a legacy account could do
    ({"admin": True}, ROLE_ADMIN),            # legacy admin stays one
    ({"admin": False}, ROLE_STANDARD),
    ({"role": "basic", "admin": True}, ROLE_BASIC),   # role wins
    ({"role": "manager"}, ROLE_MANAGER),
    ({"role": "superuser"}, ROLE_BASIC),      # unknown: fail closed, not up
    (None, ROLE_STANDARD),
])
def test_user_role_reads_role_then_the_legacy_flag(spec, expected):
    assert user_role(spec) == expected


def test_each_role_holds_everything_the_one_below_does():
    from whistler.config import ROLE_PERMISSIONS
    for lower, higher in zip(ROLES, ROLES[1:]):
        assert ROLE_PERMISSIONS[lower] < ROLE_PERMISSIONS[higher]


def test_basic_may_not_make_anything():
    assert not role_may(ROLE_BASIC, PERM_OWN_INSTANCES)
    assert not role_may(ROLE_BASIC, PERM_OWN_HOMES)
    assert role_may(ROLE_STANDARD, PERM_OWN_INSTANCES)
    assert not role_may(ROLE_STANDARD, PERM_OTHERS_INSTANCES)
    assert role_may(ROLE_MANAGER, PERM_USER_ACCESS)
    assert not role_may(ROLE_MANAGER, PERM_ADMIN)
    assert not role_may(None, PERM_OWN_INSTANCES)


@pytest.mark.parametrize("actor, target, ok", [
    (ROLE_ADMIN, ROLE_ADMIN, True),
    (ROLE_ADMIN, ROLE_BASIC, True),
    (ROLE_MANAGER, ROLE_BASIC, True),
    (ROLE_MANAGER, ROLE_STANDARD, True),
    (ROLE_MANAGER, ROLE_MANAGER, False),   # including themselves
    (ROLE_MANAGER, ROLE_ADMIN, False),
    (ROLE_STANDARD, ROLE_BASIC, False),
    (ROLE_BASIC, ROLE_BASIC, False),
])
def test_who_may_manage_whom(actor, target, ok):
    assert may_manage_user(actor, target) is ok


def test_the_abc_reads_a_legacy_admin_through_is_user_admin(make_config):
    cm = make_config(users={"old": {"name": "old", "admin": True},
                            "new": {"name": "new", "role": "basic"},
                            "plain": {"name": "plain"}})
    assert cm.get_user_role("old") == ROLE_ADMIN
    assert cm.get_user_role("new") == ROLE_BASIC
    assert cm.get_user_role("plain") == ROLE_STANDARD


# --- routes ---------------------------------------------------------------- #

def _request(cm, role, query=None):
    async def run(func, *args):
        return func(*args)
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(cm=cm, run=run)),
        state=SimpleNamespace(role=role),
        query_params=query or {}, headers={},
        url=SimpleNamespace(path="/"))


TPL = {"name": "devbase", "fullName": "devbase", "mode": "ssh",
       "runtime": "vm"}


@pytest.fixture
def cm(make_config):
    return make_config(
        users={"boss": {"name": "boss", "role": "admin"},
               "mgr": {"name": "mgr", "role": "manager"},
               "mgr2": {"name": "mgr2", "role": "manager"},
               "std": {"name": "std", "role": "standard"},
               "kid": {"name": "kid", "role": "basic"}},
        templates={u: [TPL] for u in ("boss", "mgr", "mgr2", "std", "kid")})


def _create(cm, actor, role, owner=None):
    return asyncio.run(mgmt.instance_create(
        request=_request(cm, role), cm=cm, user=actor,
        template_name="devbase", instance_name="box", owner=owner))


def test_basic_cannot_create_an_instance(cm):
    with pytest.raises(HTTPException) as exc:
        _create(cm, "kid", ROLE_BASIC)
    assert exc.value.status_code == 403
    assert not cm.get_user_instances("kid")


def test_basic_cannot_edit_or_delete(cm):
    req = _request(cm, ROLE_BASIC)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.instance_update(request=req, cm=cm, user="kid",
                                         name="box"))
    assert exc.value.status_code == 403
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.instance_delete(request=req, cm=cm, user="kid",
                                         name="box"))
    assert exc.value.status_code == 403


def test_basic_cannot_manage_home_volumes(cm):
    req = _request(cm, ROLE_BASIC)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.home_volume_create(request=req, cm=cm, user="kid",
                                            name="h", zone="default"))
    assert exc.value.status_code == 403
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.home_volume_delete(request=req, cm=cm, user="kid",
                                            name="h"))
    assert exc.value.status_code == 403


def test_standard_creates_its_own(cm):
    resp = _create(cm, "std", ROLE_STANDARD)
    assert resp.status_code == 303
    assert [i["name"] for i in cm.get_user_instances("std")] == ["box"]


def test_standard_cannot_create_for_someone_else(cm):
    with pytest.raises(HTTPException) as exc:
        _create(cm, "std", ROLE_STANDARD, owner="kid")
    assert exc.value.status_code == 403
    assert not cm.get_user_instances("kid")


def test_a_manager_creates_for_a_basic_account(cm):
    resp = _create(cm, "mgr", ROLE_MANAGER, owner="kid")
    # Built in the owner's name, and back to the owner's account page.
    assert [i["name"] for i in cm.get_user_instances("kid")] == ["box"]
    assert not cm.get_user_instances("mgr")
    assert resp.headers["location"].startswith("/admin/users/kid")


@pytest.mark.parametrize("owner", ["boss", "mgr2"])
def test_a_manager_cannot_create_for_a_peer_or_an_admin(cm, owner):
    with pytest.raises(HTTPException) as exc:
        _create(cm, "mgr", ROLE_MANAGER, owner=owner)
    assert exc.value.status_code == 403


def test_an_admin_creates_for_anyone(cm):
    _create(cm, "boss", ROLE_ADMIN, owner="mgr")
    assert [i["name"] for i in cm.get_user_instances("mgr")] == ["box"]


def test_creating_for_a_user_who_does_not_exist_is_a_404(cm):
    with pytest.raises(HTTPException) as exc:
        _create(cm, "boss", ROLE_ADMIN, owner="ghost")
    assert exc.value.status_code == 404


def test_the_create_form_is_filled_from_the_owners_catalog(make_config):
    cm = make_config(users={"mgr": {"name": "mgr", "role": "manager"},
                            "kid": {"name": "kid", "role": "basic"}},
                     templates={"mgr": [], "kid": [TPL]})
    resp = asyncio.run(mgmt.instance_create_form(
        request=_request(cm, ROLE_MANAGER,
                         query={"owner": "kid", "template": "devbase"}),
        cm=cm, user="mgr", is_admin=False))
    assert resp.context["owner"] == "kid"
    assert resp.context["selected_tpl"]["name"] == "devbase"
    body = resp.body.decode()
    assert 'name="owner" value="kid"' in body


def test_a_manager_edits_access_of_standard_but_not_of_an_admin(cm):
    asyncio.run(mgmt.admin_user_set_zones(
        _request(cm, ROLE_MANAGER), cm, "mgr", "std", zone_names=["default"]))
    assert cm.users["std"]["allowedZones"] == ["default"]
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.admin_user_set_zones(
            _request(cm, ROLE_MANAGER), cm, "mgr", "boss", zone_names=[]))
    assert exc.value.status_code == 403
    # Nor their own: that would be granting themselves anything.
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.admin_user_set_zones(
            _request(cm, ROLE_MANAGER), cm, "mgr", "mgr", zone_names=["x"]))
    assert exc.value.status_code == 403


def test_a_manager_cannot_reach_the_admin_catalog(cm):
    req = _request(cm, ROLE_MANAGER)
    with pytest.raises(HTTPException) as exc:
        mgmt._need(req, PERM_ADMIN)
    assert exc.value.status_code == 403


def test_an_admin_cannot_change_their_own_role(cm):
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.admin_user_update(
            _request(cm, ROLE_ADMIN), cm, "boss", "boss", role="manager"))
    assert exc.value.status_code == 400
    assert cm.users["boss"]["role"] == "admin"


def test_saving_a_role_mirrors_the_legacy_flag(cm):
    asyncio.run(mgmt.admin_user_update(
        _request(cm, ROLE_ADMIN), cm, "boss", "std", role="admin"))
    assert cm.users["std"]["role"] == "admin"
    assert cm.users["std"]["admin"] is True


def test_an_unknown_role_is_refused(cm):
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.admin_user_update(
            _request(cm, ROLE_ADMIN), cm, "boss", "std", role="root"))
    assert exc.value.status_code == 400


# --- what the pages show --------------------------------------------------- #

def _render(template, role, **context):
    request = SimpleNamespace(url=SimpleNamespace(path="/"),
                              state=SimpleNamespace(role=role))
    return mgmt.templates.env.get_template(template).render(
        request=request, current_user="u", **context)


def test_a_basic_dashboard_offers_no_new_instance_and_no_edit():
    rows = [{"name": "box", "template": "devbase", "status": "Stopped",
             "ready": True, "mode": "ssh", "term_url": "/term/box"}]
    html = _render("user/index.html", ROLE_BASIC, instances=rows,
                   can_override=False)
    assert "New Instance" not in html
    assert "/instances/box/edit" not in html
    assert "Home volumes" not in html      # nav
    assert "/dashboard" not in html        # nav
    assert 'title="Start"' in html         # still starts


def test_a_standard_dashboard_does():
    html = _render("user/index.html", ROLE_STANDARD, instances=[],
                   can_override=False)
    assert "New Instance" in html
    assert "Home volumes" in html


def test_a_managers_nav_says_manage():
    html = _render("user/index.html", ROLE_MANAGER, instances=[],
                   can_override=False)
    assert "Manage" in html and ">Admin<" not in html


def test_a_managers_admin_index_leaves_out_the_catalog():
    html = _render("admin/index.html", ROLE_MANAGER, all_users=[],
                   all_templates=[], all_instances=[], backups_enabled=True,
                   backup_health=None)
    assert "/admin/users" in html and "/admin/sessions" in html
    for path in ("/admin/templates", "/admin/zones", "/admin/datasets",
                 "/admin/groups", "/admin/backups", "/admin/homevolumes"):
        assert path not in html, path


@pytest.mark.parametrize("role", [ROLE_MANAGER, ROLE_ADMIN])
def test_the_user_page_offers_instances_for_them(role):
    html = _render("admin/user_detail.html", role,
                   user_obj={"name": "kid", "publicKeys": []},
                   user_role="basic",
                   instances=[{"name": "box", "template": "devbase",
                               "status": "Stopped"}],
                   desktop_sessions=[{"name": "desk", "template": "gnome",
                                      "phase": "Ready"}],
                   user_groups=[], access_sections=[], grid_zones=[],
                   zones=[], gpu_types=[], allowed_gpu_types=[],
                   override_groups=[], user_overrides={}, own_overrides={},
                   allowed_zones=[], own_zones=[], own_gpu_types=[],
                   channels=[], enforced_channels=[], entry_points=[],
                   own_entry_points=[], allowed_entry_points=[])
    assert "owner=kid" in html                       # picker + edit
    assert "/instances/box/edit?user=u&owner=kid" in html
    assert "desk" in html                            # desktops listed too
    assert ("Delete User" in html) is (role == ROLE_ADMIN)
    assert ("Edit Profile" in html) is (role == ROLE_ADMIN)
