"""Portal surface for named home volumes: the picker and the management page.

The volume itself is the user's data, so these are user routes rather than
admin ones. See design/security.md, "Core model: the access matrix", and
tests/unit/test_home_volumes.py for the config layer.
"""
import re
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from whistler.portal import management as mgmt


def _render(name, path="/homes", **context):
    request = SimpleNamespace(url=SimpleNamespace(path=path))
    return mgmt.templates.env.get_template(name).render(request=request, **context)


VOLUMES = [
    {"name": "research", "size": "50Gi", "description": "Follows me",
     "pvcName": "whistler-home-research", "inUseBy": None},
    {"name": "restricted", "size": None, "description": None,
     "pvcName": "whistler-home-restricted", "inUseBy": "alice-locked"},
]


def test_page_shows_which_volume_is_held_and_by_what():
    # The holder is what makes the one-live-attach rule legible instead of a
    # refusal that only appears at start.
    html = _render("user/home_volumes.html", current_user="alice",
                   is_admin=False, volumes=VOLUMES)
    assert "research" in html and "restricted" in html
    assert "alice-locked" in html
    assert "free" in html


def test_delete_is_disabled_while_a_volume_is_held():
    html = _render("user/home_volumes.html", current_user="alice",
                   is_admin=False, volumes=VOLUMES)
    held = html.split("restricted")[-1]
    assert "disabled" in held


def test_deleting_keeps_the_data_unless_explicitly_asked():
    # A dropdown must not be able to destroy a home directory. The checkbox is
    # opt-in and the confirm text says which way it defaults.
    html = _render("user/home_volumes.html", current_user="alice",
                   is_admin=False, volumes=VOLUMES)
    assert 'name="delete_data"' in html
    assert "KEPT" in html


def test_empty_state_says_instances_have_their_own():
    html = _render("user/home_volumes.html", current_user="alice",
                   is_admin=False, volumes=[])
    assert "No named home volumes yet" in html


@pytest.mark.parametrize("template,path", [
    ("user/create_instance.html", "/instances/new"),
    ("user/edit_instance.html", "/instances/x/edit"),
])
def test_picker_defaults_to_a_new_home_for_this_instance(template, path):
    # The default reproduces the pre-named-volumes behaviour exactly, so it
    # has to be the selected option rather than merely available.
    html = _render(template, path=path, current_user="alice", is_admin=False,
                   tpls=[], gpu_types=[],
                   allowed_gpu_types=[], overrides={}, zones=["default"],
                   home_volumes=[{"name": "research", "size": "50Gi"}],
                   current_home_volume=None,
                   inst={"name": "x", "status": "Stopped", "ready": True},
                   cur={"templateRef": "t", "preemptible": False,
                        "homeVolume": None, "overrides": {}})
    assert 'value="" selected' in html.replace('value=""  selected', 'value="" selected')
    assert "research" in html


def test_picker_preselects_the_instances_current_volume():
    html = _render("user/edit_instance.html", path="/instances/x/edit",
                   current_user="alice", is_admin=False, tpls=[],
                   gpu_types=[], allowed_gpu_types=[],
                   overrides={}, zones=["default"],
                   home_volumes=[{"name": "research", "size": "50Gi"}],
                   current_home_volume="research",
                   inst={"name": "x", "status": "Stopped", "ready": True},
                   cur={"templateRef": "t", "preemptible": False,
                        "homeVolume": "research", "overrides": {}})
    picked = html.split('value="research"')[1][:40]
    assert "selected" in picked


@pytest.mark.parametrize("bad", ["Has-Caps", "under_score", "-leading", ""])
def test_bad_volume_names_are_refused(bad):
    import asyncio
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.home_volume_create(
            request=None, cm=None, user="alice", name=bad, zone="open"))
    assert exc.value.status_code == 400


def test_a_zone_the_user_cannot_enter_is_refused():
    # The form only offers zones the user holds, but the form is not the
    # boundary: granting yourself a cell in a zone you cannot enter would be a
    # real escalation, because the cell outlives whatever allowedZones says
    # later. Checked server-side.
    import asyncio
    from tests.conftest import FakeConfigManager

    class _App:
        class state:
            @staticmethod
            async def run(fn, *a, **kw):
                return fn(*a, **kw)
    cm = FakeConfigManager(users={"alice": {"name": "alice",
                                            "allowedZones": ["open"]}},
                           zones={"open": {}, "restricted": {}})
    request = SimpleNamespace(app=_App)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.home_volume_create(
            request=request, cm=cm, user="alice", name="sneaky",
            zone="restricted"))
    assert exc.value.status_code == 400
    assert "do not have access" in exc.value.detail


# --- the access grid (shared by the User and Group editors) ----------------- #

def _grid(sections, zones=("open", "restricted"), show_group_mark=True):
    request = SimpleNamespace(url=SimpleNamespace(path="/admin/users/alice"))
    return mgmt.templates.env.get_template("admin/_access_grid.html").render(
        request=request, current_user="admin", is_admin=True,
        zones=list(zones), sections=sections, show_group_mark=show_group_mark)


def _section(kind="home", title="Home volumes", enforced=True, rows=None):
    return {"kind": kind, "title": title, "enforced": enforced,
            "note": "note", "rows": rows or []}


def _row(key="alice-desk", own=None, eff=None):
    return {"key": key, "label": key, "description": None,
            "own": own or {"open": None, "restricted": None},
            "effective": eff or {"open": None, "restricted": None}}


def test_rows_are_grouped_into_sections_by_kind():
    # Home volumes and datasets are different things with different
    # enforcement, so they are read as separate blocks rather than one flat
    # list distinguished only by a label.
    html = _grid([_section(rows=[_row()]),
                  _section("dataset", "Datasets", False, [_row("corpus")])])
    assert "Home volumes" in html and "Datasets" in html
    assert html.index("Home volumes") < html.index("Datasets")
    # Only the unenforced section carries the warning.
    assert html.count("not enforced yet") == 1


def test_every_cell_posts_a_value_including_the_blank_ones():
    # Three states as icons over a hidden input, so a cell nobody touches
    # still posts what it was rendered with. "No access" is a state you can
    # see and leave alone, not an absence.
    html = _grid([_section(rows=[_row(own={"open": None,
                                           "restricted": "read-only"})])])
    assert 'name="access__open__alice-desk"' in html
    assert 'name="access__restricted__alice-desk"' in html
    assert 'value="read-only"' in html


def test_the_three_states_are_the_documented_icons():
    html = _grid([_section(rows=[_row()])])
    for icon in ("red window close icon", "yellow eye icon",
                 "green check circle icon"):
        assert icon in html


def test_group_marks_show_on_the_user_grid_only():
    # On a user grid a cell may come from a group while the control still
    # edits the user's own value. A group grid has no second source, so the
    # marker there would be meaningless.
    rows = [_row(eff={"open": "allowed", "restricted": None})]
    assert "teal users icon" in _grid([_section(rows=rows)],
                                      show_group_mark=True)
    assert "teal users icon" not in _grid([_section(rows=rows)],
                                          show_group_mark=False)


def test_zone_columns_trim_and_a_buffer_absorbs_the_slack():
    # Icons are narrower than any zone name, so the zone columns size to their
    # heading and the buffer takes the rest — otherwise they stretch and the
    # grid reads as mostly empty space.
    html = _grid([_section(rows=[_row()])])
    header = html.split("<thead>")[1].split("</thead>")[0]
    assert re.findall(r'<th class="([^"]*)"', header) == [
        "collapsing", "access-buffer", "access-zone", "access-zone"]


def test_computed_view_offers_no_way_to_edit():
    # It is a derived value: an edit control would have to guess whether the
    # admin meant to change the user or the group that granted the cell.
    request = SimpleNamespace(url=SimpleNamespace(path="/admin/computed-access"))
    html = mgmt.templates.env.get_template("admin/computed_access.html").render(
        request=request, current_user="admin", is_admin=True, subject="alice",
        usernames=["alice"], zones=["open"], groups=["lab-staff"],
        sections=[{"kind": "home", "title": "Home volumes", "enforced": True,
                   "rows": [{"key": "alice-desk", "label": "alice-desk",
                             "cells": {"open": {"value": "allowed",
                                                "sources": ["lab-staff"]}}}]}])
    assert "access-cell" not in html
    assert 'name="access__' not in html
    assert "<form" in html and 'method="get"' in html   # the picker only
    # The one POST on any page is the nav's Sign out (base.html); nothing on
    # this view submits a cell.
    assert html.count('method="post"') == 1 and 'action="/logout"' in html
    # Provenance is the point of the view.
    assert "lab-staff" in html
