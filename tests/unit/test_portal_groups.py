"""Group form parsing (whistler.portal.management).

The admin group editor speaks flat text (a members textarea, checkbox lists,
and the access grid's `access__<zone>__<volume>` hidden inputs); these helpers
translate to and from the Group CR's spec. The encoding that carries meaning
and is easy to get wrong: an **omitted** `channels` key means "this group
narrows nothing", while an empty list means "nothing but the desktop stream" —
the toggle is what keeps both writable.

The editor had a second volume panel until 2026-08-29 — a `volumes` allow-list
with per-member rw/ro exceptions, which was the one actually enforced while the
access grid beside it was only recorded. The grid is now the only one.
"""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from whistler.config import (CHANNELS, ConfigWriteError, ENFORCED_CHANNELS,
                             ENTRY_POINTS, OVERRIDE_GROUPS)
from whistler.portal import management as mgmt
from whistler.portal.management import (
    _build_group_data,
    _parse_members,
)


class _Form(dict):
    """The subset of Starlette's FormData the builder uses."""

    def getlist(self, key):
        value = self.get(key, [])
        return value if isinstance(value, list) else [value]


def test_members_parse_from_lines_or_commas_without_duplicates():
    assert _parse_members("alice\n bob ,carol\n\nalice") == ["alice", "bob", "carol"]


def test_build_group_data_assembles_the_cr_spec_shape():
    data = _build_group_data("lab-staff", _Form({
        "description": " Imaging project ",
        "members": "alice\nbob",
        "access__restricted__project": "allowed",
        "access__restricted__corpus": "read-only",
        "access__default__project": "",
        "zone_names": ["restricted"],
        "gpu_types": ["A100"],
        "restrict_channels": "on",
        "channels": ["ssh", "terminal"],
        "override_groups": ["zone"],
    }))
    assert data == {
        "name": "lab-staff",
        "description": "Imaging project",
        "members": ["alice", "bob"],
        "volumeAccess": {"restricted": {"project": "allowed",
                                        "corpus": "read-only"}},
        "allowedZones": ["restricted"],
        "allowedGpuTypes": ["A100"],
        "channels": ["ssh", "terminal"],
        "overrides": {"zone": True},
    }


def test_a_grid_with_every_cell_blank_writes_no_matrix_at_all():
    # Absent means the group grants nothing here, which is not the same
    # statement as an empty object and reads better in `kubectl get grp -o yaml`.
    data = _build_group_data("g", _Form({"access__restricted__project": ""}))
    assert "volumeAccess" not in data


def test_an_unrecognised_cell_mode_is_dropped_rather_than_granted():
    data = _build_group_data("g", _Form({"access__restricted__project": "rw"}))
    assert "volumeAccess" not in data


def test_channels_are_omitted_unless_the_restrict_box_is_ticked():
    data = _build_group_data("g", _Form({"channels": ["ssh"]}))
    assert "channels" not in data  # narrows nothing, whatever the boxes say


def test_restricting_with_no_boxes_writes_an_empty_grant():
    data = _build_group_data("g", _Form({"restrict_channels": "on"}))
    assert data["channels"] == []  # the desktop stream and nothing else


def test_channels_keep_the_canonical_order_and_drop_unknowns():
    data = _build_group_data("g", _Form({
        "restrict_channels": "on",
        "channels": ["screenshots", "telepathy", "ssh"],
    }))
    assert data["channels"] == ["ssh", "screenshots"]


# --------------------------------------------------------------------------- #
# Handlers, called directly (no ASGI client in the unit tier)                  #
# --------------------------------------------------------------------------- #

def _request(cm, form=None):
    async def run(func, *args):
        return func(*args)
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(cm=cm, run=run)),
        form=lambda: _AwaitableForm(form or _Form()),
        url=SimpleNamespace(path="/admin/groups"),
    )


class _AwaitableForm:
    def __init__(self, form):
        self.form = form

    def __await__(self):
        async def _get():
            return self.form
        return _get().__await__()


async def test_create_writes_the_group_and_redirects(make_config):
    cm = make_config(users={"alice": {"name": "alice"}})
    request = _request(cm, _Form({"name": "lab", "members": "alice"}))
    response = await mgmt.admin_group_create(request, cm, "alice")
    assert response.status_code in (302, 303, 307)
    assert cm.groups["lab"]["members"] == ["alice"]


async def test_create_rejects_a_name_that_is_not_a_dns_label(make_config):
    cm = make_config()
    request = _request(cm, _Form({"name": "Lab Staff"}))
    with pytest.raises(HTTPException) as exc:
        await mgmt.admin_group_create(request, cm, "alice")
    assert exc.value.status_code == 400
    assert cm.groups == {}


async def test_a_cluster_refusal_reaches_the_browser_with_its_reason(make_config):
    """The failure that cost an afternoon: a 404 because the CRD was never
    installed (helm upgrade does not update CRDs) surfaced as a flat "Failed
    to create group." and the reason lived only in the pod log."""
    cm = make_config()

    def explode(_group_data):
        raise ConfigWriteError(
            "could not save group 'lab': the groups.whistler.martinmalmsten.net "
            "CRD is not installed in this cluster.")

    cm.save_group = explode
    request = _request(cm, _Form({"name": "lab"}))
    with pytest.raises(HTTPException) as exc:
        await mgmt.admin_group_create(request, cm, "alice")
    assert exc.value.status_code == 500
    assert "CRD is not installed" in exc.value.detail


async def test_a_nameless_group_is_a_400_not_a_500(make_config):
    cm = make_config()
    cm.save_group = lambda _data: False
    with pytest.raises(HTTPException) as exc:
        await mgmt.admin_group_update(_request(cm, _Form()), cm, "alice", "")
    assert exc.value.status_code == 400


async def test_editing_a_missing_group_is_a_404(make_config):
    cm = make_config()
    with pytest.raises(HTTPException) as exc:
        await mgmt.admin_group_edit(_request(cm), cm, "alice", "nope")
    assert exc.value.status_code == 404


async def test_delete_removes_the_group(make_config):
    cm = make_config(groups={"lab": {"members": ["alice"]}})
    await mgmt.admin_group_delete(_request(cm), cm, "alice", "lab")
    assert cm.groups == {}


async def test_user_channel_toggle_clears_and_sets_the_grant(make_config):
    cm = make_config(users={"alice": {"name": "alice"}})
    # Unticked: the field is removed, so the user narrows nothing of their
    # own — which is NOT the same as being granted an empty set.
    await mgmt.admin_user_set_channels(_request(cm), cm, "admin", "alice",
                                       restrict=None, channels=["ssh"])
    assert "channels" not in cm.users["alice"]
    assert cm.get_user_channels("alice") is None
    # Ticked: exactly the boxes, in canonical order.
    await mgmt.admin_user_set_channels(_request(cm), cm, "admin", "alice",
                                       restrict="on", channels=["terminal", "ssh"])
    assert cm.users["alice"]["channels"] == ["ssh", "terminal"]


# --------------------------------------------------------------------------- #
# Templates: cheap render smoke tests, so a Jinja typo fails here and not in   #
# a browser.                                                                   #
# --------------------------------------------------------------------------- #

_GROUP = {"name": "lab", "description": "Imaging", "members": ["alice"],
          "volumeAccess": {"restricted": {"project": "read-only"}},
          "allowedZones": ["restricted"], "channels": ["ssh"],
          "overrides": {"zone": True}}
_SECTIONS = [{"kind": "dataset", "title": "Datasets", "enforced": True,
              "note": "Enforced.",
              "rows": [{"key": "project", "label": "project",
                        "description": None,
                        "own": {"default": None, "restricted": "read-only"},
                        "effective": {"default": None,
                                      "restricted": "read-only"}}]}]


def _render(name, **context):
    request = SimpleNamespace(url=SimpleNamespace(path="/admin/groups"))
    return mgmt.templates.env.get_template(name).render(request=request, **context)


@pytest.mark.parametrize("groups", [[_GROUP], []])
def test_groups_list_renders(groups):
    assert _render("admin/groups.html", current_user="alice", is_admin=True,
                   groups=groups)


@pytest.mark.parametrize("group", [_GROUP, None])
def test_group_form_renders_for_new_and_existing(group):
    html = _render("admin/group_form.html", current_user="alice", is_admin=True,
                   group=group, zones=["default", "restricted"],
                   gpu_types=["A100"], all_users=[{"name": "alice"}],
                   entry_points=ENTRY_POINTS,
                   channels=CHANNELS, enforced_channels=ENFORCED_CHANNELS,
                   override_groups=OVERRIDE_GROUPS,
                   access_sections=_SECTIONS)
    # The clipboard channel must be visibly marked as not enforced wherever it
    # is offered — an admin ticking it deserves to know it is a declaration.
    assert "not enforced" in html


def test_user_detail_shows_group_provenance():
    html = _render(
        "admin/user_detail.html", current_user="alice", is_admin=True,
        user_obj={"name": "alice", "publicKeys": [], "admin": True},
        instances=[],
        gpu_types=["A100"], allowed_gpu_types=["A100"],
        user_overrides={"zone": True}, override_groups=OVERRIDE_GROUPS,
        zones=["default", "restricted"], allowed_zones=["restricted"],
        user_groups=[_GROUP],
        channels=CHANNELS, enforced_channels=ENFORCED_CHANNELS,
        own_channels=None, channel_grant=["ssh"],
        entry_points=ENTRY_POINTS, own_entry_points=[],
        allowed_entry_points=["portal"],
        access_sections=[{**_SECTIONS[0],
                          "rows": [{**_SECTIONS[0]["rows"][0], "own": {
                              "default": None, "restricted": None}}]}],
        own_zones=[], own_gpu_types=[], own_overrides={})
    # Every checkbox grant on this page comes from the group, so each one says
    # so, and none of the boxes is ticked — the user holds nothing in their own
    # right. The access grid says it with an icon instead, which is the one
    # marker the user page renders and the group page does not.
    assert html.count("from group") == 4
    assert "Granted by a group" in html
