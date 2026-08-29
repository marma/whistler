"""The portal's two new modals: the template picker behind "New Instance", and
the override dialog in front of Start.

Both exist for the same reason. The template catalog is what you consult while
*making* an instance, so it belongs to that act rather than sitting permanently
under the list of instances you already have. And an instance's spec.overrides
are only *defaults*: the operator reads them at reconcile, so start is the last
moment before a run where changing one still takes effect — which makes it the
moment to ask, for a user who holds a grant.

Portal only. The launcher TUI's `s` key stays a single keystroke
(design/proxyjump.md, "TUI diet").
"""
import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from whistler.config import OVERRIDE_GROUPS
from whistler.portal import management as mgmt


def _render(template, path="/", **context):
    request = SimpleNamespace(url=SimpleNamespace(path=path))
    return mgmt.templates.env.get_template(template).render(request=request,
                                                            **context)


TPLS = [
    {"name": "devbase", "fullName": "devbase", "source": "system", "mode": "ssh",
     "runtime": "vm", "image": "ghcr.io/x/devbase:latest",
     "resources": {"cpu": "4", "memory": "8Gi"}},
    {"name": "scratch", "fullName": "alice-scratch", "source": "user",
     "mode": "ssh", "runtime": "container", "image": "ubuntu:26.04"},
]

ALL_GRANTS = {g: True for g in OVERRIDE_GROUPS}
NO_GRANTS = {g: False for g in OVERRIDE_GROUPS}


class _FakeRequest:
    """Enough Request for the route bodies: the executor shim, a form, headers."""

    def __init__(self, form=None, headers=None, query=None, path="/"):
        self._form = dict(form or {})
        self.url = SimpleNamespace(path=path)
        self.headers = headers or {}
        self.query_params = query or {}

        async def _run(func, *args):
            return func(*args)

        self.app = SimpleNamespace(state=SimpleNamespace(run=_run))

    async def form(self):
        return self._form


# --------------------------------------------------------------------------- #
# The template catalog moved into a picker                                     #
# --------------------------------------------------------------------------- #

def test_dashboard_no_longer_carries_the_template_catalog():
    html = _render("user/index.html", current_user="alice", is_admin=False,
                   instances=[], can_override=False)
    assert "Available Templates" not in html
    assert "Use Template" not in html


def test_new_instance_opens_the_picker():
    # Both entry points — the header button and the empty-state one — have to
    # open it, or the first instance is created through a door that no longer
    # exists.
    html = _render("user/index.html", current_user="alice", is_admin=False,
                   instances=[], can_override=False)
    assert html.count('hx-get="/instances/templates?user=alice"') == 2
    assert 'hx-target="#modal-host"' in html
    assert 'id="modal-host"' in html


def test_picker_lists_each_template_and_links_to_the_create_form():
    html = _render("user/_template_picker.html", current_user="alice",
                   is_admin=False, tpls=TPLS)
    assert "/instances/new?user=alice&template=devbase" in html
    assert "/instances/new?user=alice&template=alice-scratch" in html
    # Modals only work if they open themselves — htmx runs the script it swaps in.
    assert "modal('show')" in html


def test_picker_with_nothing_to_offer_says_who_can_fix_it():
    # An empty modal reads as a broken button. Under explicit access a new
    # account really can hold no template, so this is a state, not an edge case.
    html = _render("user/_template_picker.html", current_user="alice",
                   is_admin=False, tpls=[])
    assert "No templates available to you" in html
    assert "admin" in html.lower()


def test_create_form_shows_the_picked_template_rather_than_a_select():
    html = _render("user/create_instance.html", path="/instances/new",
                   current_user="alice", is_admin=False, tpls=TPLS,
                   selected_template="devbase", selected_tpl=TPLS[0],
                   volumes=[], allowed_volumes=[], gpu_types=[],
                   allowed_gpu_types=[], overrides=NO_GRANTS, zones=["default"],
                   home_volumes=[], current_home_volume=None)
    assert 'name="template_name" value="devbase"' in html
    assert "select a template" not in html


def test_create_form_still_has_a_select_for_a_bare_url():
    # A bookmark on /instances/new, or a picker with nothing to offer, must
    # still be able to create something.
    html = _render("user/create_instance.html", path="/instances/new",
                   current_user="alice", is_admin=False, tpls=TPLS,
                   selected_template=None, selected_tpl=None,
                   volumes=[], allowed_volumes=[], gpu_types=[],
                   allowed_gpu_types=[], overrides=NO_GRANTS, zones=["default"],
                   home_volumes=[], current_home_volume=None)
    assert "select a template" in html
    assert 'value="alice-scratch"' in html


def test_create_form_ignores_a_template_it_cannot_resolve(make_config):
    # The route resolves ?template= against the user's own set, so a name from
    # someone else's catalog falls back to the select instead of being trusted
    # into a hidden field.
    cm = make_config(users={"alice": {"name": "alice"}},
                     templates={"alice": [TPLS[1]]})
    request = _FakeRequest(query={"template": "someone-elses"})
    resp = asyncio.run(mgmt.instance_create_form(
        request=request, cm=cm, user="alice", is_admin=False))
    assert resp.context["selected_tpl"] is None


# --------------------------------------------------------------------------- #
# Start, with the overrides that are the user's to change                      #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("can_override,expected", [
    (True, "/instances/box/start-dialog?user=alice"),
    (False, "/instances/box/connect?user=alice"),
])
def test_play_button_asks_only_when_something_is_overridable(can_override, expected):
    html = _render("user/_instance_buttons.html", name="box", status="Stopped",
                   ready=True, user="alice", can_override=can_override)
    assert expected in html


def test_a_running_row_still_cannot_be_started():
    # The dialog must not become a way past the state machine: a Running
    # instance has nothing to start, grant or no grant.
    html = _render("user/_instance_buttons.html", name="box", status="Running",
                   ready=True, user="alice", can_override=True)
    play = html.split('title="Start')[1].split("</button>")[0]
    assert "start-dialog" not in play
    assert "disabled" in play


def test_dialog_prefills_the_instances_recorded_overrides():
    # This is the whole point of "the instance's values are the defaults" — an
    # empty form would silently propose dropping every override it did not show.
    html = _render("user/_start_dialog.html", current_user="alice", name="box",
                   has_overrides=True, hx=True, overrides=ALL_GRANTS,
                   volumes=[{"name": "scratch"}], allowed_volumes=["scratch"],
                   gpu_types=["A100"], zones=["default", "open"],
                   cur={"preemptible": False, "homeVolume": "research",
                        "overrides": {"resources": {"cpu": "8", "memory": "16Gi"},
                                      "gpuType": "A100", "gpuCount": 2,
                                      "zone": "open",
                                      "volumes": {"scratch": "/mnt/s"}}})
    assert 'value="8"' in html and 'value="16Gi"' in html
    assert 'value="A100" selected' in html
    assert 'value="2"' in html
    assert 'value="open" selected' in html
    assert 'value="/mnt/s"' in html
    assert "checked" in html


def test_dialog_marks_the_submission_as_carrying_overrides():
    # Without the flag a submit is indistinguishable from the plain play button,
    # and the connect route must not touch spec.overrides on that path.
    html = _render("user/_start_dialog.html", current_user="alice", name="box",
                   has_overrides=True, hx=True, overrides=ALL_GRANTS,
                   volumes=[], allowed_volumes=[], gpu_types=[], zones=[],
                   cur={"overrides": {}})
    assert 'name="apply_overrides" value="1"' in html


def test_dialog_without_a_grant_is_a_plain_confirm():
    # A grant can be withdrawn between rendering a row and pressing its button.
    html = _render("user/_start_dialog.html", current_user="alice", name="box",
                   has_overrides=False, hx=True, overrides=NO_GRANTS,
                   volumes=[], allowed_volumes=[], gpu_types=[], zones=[],
                   cur={"overrides": {}})
    assert "apply_overrides" not in html
    assert "nothing to choose" in html


def test_dialog_posts_in_place_for_the_dashboard_and_plainly_for_the_detail_page():
    args = dict(current_user="alice", name="box", has_overrides=False,
                overrides=NO_GRANTS, volumes=[], allowed_volumes=[],
                gpu_types=[], zones=[], cur={"overrides": {}})
    hx = _render("user/_start_dialog.html", hx=True, **args)
    plain = _render("user/_start_dialog.html", hx=False, **args)
    assert 'hx-post="/instances/box/connect?user=alice"' in hx
    assert 'hx-target="#status-box"' in hx
    assert 'method="post"' in plain and "hx-post" not in plain


def test_dialog_404s_for_an_instance_that_is_not_there(make_config):
    cm = make_config(users={"alice": {"name": "alice"}})
    with pytest.raises(HTTPException) as exc:
        asyncio.run(mgmt.instance_start_dialog(
            request=_FakeRequest(), cm=cm, user="alice", is_admin=False,
            name="ghost"))
    assert exc.value.status_code == 404


# --------------------------------------------------------------------------- #
# What the submission does to the CR                                           #
# --------------------------------------------------------------------------- #

def _instance(**extra):
    return {"name": "box", "status": "Stopped", "template": "devbase", **extra}


def _connect(cm, form, headers=None):
    return asyncio.run(mgmt.instance_connect(
        request=_FakeRequest(form=form, headers=headers), cm=cm, user="alice",
        name="box", is_admin=False, **{k: v for k, v in form.items()
                                       if k != "volume_names"}))


def test_a_plain_start_leaves_the_overrides_alone(make_config):
    cm = make_config(users={"alice": {"name": "alice"}},
                     instances={"alice": [_instance(overrides={"gpuCount": 1})]})
    _connect(cm, {})
    assert cm.started == [("alice", "box")]
    assert cm._instances["alice"][0]["overrides"] == {"gpuCount": 1}


def test_a_plain_start_clears_a_previous_runs_overrides(make_config):
    # This is what keeps a one-shot value one-shot. The launcher, the jump and
    # the plain play button all arrive here with nothing to say, and an
    # instance started that way must run exactly as it is configured — not as
    # someone last configured it for one run in the portal.
    cm = make_config(users={"alice": {"name": "alice"}},
                     instances={"alice": [_instance(overrides={"gpuCount": 1},
                                                    runOverrides={"gpuCount": 8})]})
    _connect(cm, {})
    assert cm.run_overrides == [("alice", "box", None)]
    assert "runOverrides" not in cm._instances["alice"][0]


def test_the_dialog_applies_to_this_run_and_not_to_the_defaults(make_config):
    # The whole point: what you type in the popup is for the run you are
    # starting. The instance still starts with what its edit form says next
    # time — that form is the only thing that moves a default.
    cm = make_config(users={"alice": {"name": "alice", "overrides": ALL_GRANTS}},
                     instances={"alice": [_instance(overrides={"gpuCount": 1})]})
    _connect(cm, {"apply_overrides": "1", "override_gpu_count": "4"})
    inst = cm._instances["alice"][0]
    assert inst["runOverrides"] == {"gpuCount": 4}
    assert inst["overrides"] == {"gpuCount": 1}
    assert cm.started == [("alice", "box")]


def test_clearing_a_field_means_not_this_run_not_forever(make_config):
    # The form is prefilled from the defaults, so a blank field is a deliberate
    # "run without it" — and the default it was showing survives untouched.
    cm = make_config(users={"alice": {"name": "alice", "overrides": ALL_GRANTS}},
                     instances={"alice": [_instance(
                         overrides={"resources": {"cpu": "8"}, "gpuCount": 2})]})
    _connect(cm, {"apply_overrides": "1", "override_cpu": "",
                  "override_gpu_count": "2"})
    inst = cm._instances["alice"][0]
    assert inst["runOverrides"] == {"gpuCount": 2}
    assert inst["overrides"] == {"resources": {"cpu": "8"}, "gpuCount": 2}


def test_a_dialog_cleared_of_everything_says_so_rather_than_nothing(make_config):
    # Empty and absent are different: {} is "this run, no overrides at all",
    # while dropping the key would hand the run straight back to the defaults
    # the user just cleared.
    cm = make_config(users={"alice": {"name": "alice", "overrides": ALL_GRANTS}},
                     instances={"alice": [_instance(overrides={"gpuCount": 2})]})
    _connect(cm, {"apply_overrides": "1", "override_cpu": "",
                  "override_gpu_count": ""})
    assert cm.run_overrides == [("alice", "box", {})]
    assert cm._instances["alice"][0]["overrides"] == {"gpuCount": 2}


def test_starting_never_touches_preemptible_or_the_home_volume(make_config):
    # Neither is an override and the dialog does not ask about them, so a start
    # has no business rewriting the editable slice they live in.
    cm = make_config(users={"alice": {"name": "alice", "overrides": ALL_GRANTS}},
                     instances={"alice": [_instance(preemptible=True,
                                                    homeVolume="research",
                                                    overrides={})]})
    _connect(cm, {"apply_overrides": "1", "override_cpu": "2"})
    inst = cm._instances["alice"][0]
    assert inst["preemptible"] is True
    assert inst["homeVolume"] == "research"
    assert inst["runOverrides"] == {"resources": {"cpu": "2"}}


def test_volume_mount_paths_come_from_the_named_form_fields(make_config):
    # Same misalignment trap as the create form: only the *checked* boxes post a
    # volume_names entry, so the paths are keyed by name, not zipped.
    cm = make_config(users={"alice": {"name": "alice", "overrides": ALL_GRANTS}},
                     instances={"alice": [_instance(overrides={})]})
    form = {"apply_overrides": "1", "mount_path__data": "/mnt/data"}
    asyncio.run(mgmt.instance_connect(
        request=_FakeRequest(form=form), cm=cm, user="alice", name="box",
        is_admin=False, apply_overrides="1", volume_names=["data"]))
    assert cm._instances["alice"][0]["runOverrides"] == {
        "volumes": {"data": "/mnt/data"}}


# --------------------------------------------------------------------------- #
# Who gets asked at all                                                        #
# --------------------------------------------------------------------------- #

def test_a_user_with_no_grant_is_never_offered_the_dialog(make_config):
    cm = make_config(users={"alice": {"name": "alice"}},
                     instances={"alice": [_instance()]})
    resp = asyncio.run(mgmt.user_index(request=_FakeRequest(), cm=cm,
                                       user="alice", is_admin=False))
    assert resp.context["can_override"] is False


def test_a_group_grant_is_enough_to_be_asked(make_config):
    # Same composition rule as everything else: the union of the user's own
    # grants and every group's. A project granting `resources` makes its members
    # people the dialog has a question for.
    cm = make_config(users={"alice": {"name": "alice"}},
                     groups={"proj": {"members": ["alice"],
                                      "overrides": {"resources": True}}},
                     instances={"alice": [_instance()]})
    resp = asyncio.run(mgmt.user_index(request=_FakeRequest(), cm=cm,
                                       user="alice", is_admin=False))
    assert resp.context["can_override"] is True


def test_the_detail_pages_dialog_posts_plainly():
    # That page has no #actions-<name> span for the badge's out-of-band swap, so
    # its dialog has to submit as a normal form and follow the redirect back.
    html = _render("user/instance_detail.html", path="/instances/box",
                   current_user="alice", is_admin=False, can_override=True,
                   inst={"name": "box", "status": "Stopped", "ready": True,
                         "template": "devbase", "namespace": "whistler-alice",
                         "editable": True})
    assert "/instances/box/start-dialog?user=alice&hx=0" in html
    assert 'id="modal-host"' in html


def test_a_status_poll_does_not_quietly_turn_start_back_into_one_click(make_config):
    # The buttons are re-rendered out of band on every poll, so a grant that
    # doesn't travel with them reverts the play button five seconds after load.
    cm = make_config(users={"alice": {"name": "alice", "overrides": ALL_GRANTS}},
                     instances={"alice": [_instance()]})
    resp = asyncio.run(mgmt._status_badge_response(
        request=_FakeRequest(), cm=cm, user="alice", name="box", controls=True))
    assert b"/instances/box/start-dialog?user=alice" in resp.body


# --------------------------------------------------------------------------- #
# The viewer buttons start things too, so they ask as well                     #
# --------------------------------------------------------------------------- #

VIEWER_URLS = dict(connect_url="/connect/box?user=alice",
                   term_url="/term/box?user=alice",
                   console_url="/console/box?user=alice")


@pytest.mark.parametrize("title,door", [
    ("Connect to desktop", "desktop"),
    ("Connect using SSH", "terminal"),
    ("Machine console", "console"),
])
def test_a_viewer_button_asks_before_it_starts_something(title, door):
    # Each of these opens a page that starts a stopped instance on its way in,
    # so on a stopped row they are start buttons wearing a destination — and
    # they must not start one without asking a user who could have chosen.
    html = _render("user/_instance_buttons.html", name="box", status="Stopped",
                   ready=True, user="alice", can_override=True, **VIEWER_URLS)
    button = [b for b in html.split("<a ") if title in b][0]
    assert f"/instances/box/start-dialog?user=alice&then={door}" in button
    assert "href" not in button


@pytest.mark.parametrize("title", ["Connect to desktop", "Connect using SSH"])
def test_a_running_instance_is_not_being_started_so_it_does_not_ask(title):
    # Nothing is chosen for a run that is already going; the click is just a
    # link to a page.
    html = _render("user/_instance_buttons.html", name="box", status="Running",
                   ready=True, user="alice", can_override=True, **VIEWER_URLS)
    button = [b for b in html.split("<a ") if title in b][0]
    assert "start-dialog" not in button
    assert 'target="_blank"' in button


def test_without_a_grant_the_viewer_buttons_stay_plain_links():
    html = _render("user/_instance_buttons.html", name="box", status="Stopped",
                   ready=True, user="alice", can_override=False, **VIEWER_URLS)
    assert "start-dialog" not in html
    assert 'href="/term/box?user=alice"' in html


def test_a_dialog_with_somewhere_to_go_submits_into_a_new_tab():
    # A plain form with target=_blank, because the click that opened this
    # dialog was going to open a tab — and window.open() from an htmx response
    # is no longer a user gesture, which is exactly what popup blockers catch.
    html = _render("user/_start_dialog.html", current_user="alice", name="box",
                   has_overrides=True, hx=True, then="desktop",
                   overrides=ALL_GRANTS, volumes=[], allowed_volumes=[],
                   gpu_types=[], zones=[], cur={"overrides": {}})
    assert 'target="_blank"' in html
    assert 'action="/instances/box/connect?user=alice&then=desktop"' in html
    assert "hx-post" not in html
    assert "Start and open the desktop" in html


@pytest.mark.parametrize("door,landing", [
    ("desktop", "/connect/box?user=alice"),
    ("terminal", "/term/box?user=alice"),
    ("console", "/console/box?user=alice"),
])
def test_starting_hands_the_browser_on_to_where_it_was_going(make_config, door, landing):
    cm = make_config(users={"alice": {"name": "alice", "overrides": ALL_GRANTS}},
                     instances={"alice": [_instance(overrides={})]})
    resp = asyncio.run(mgmt.instance_connect(
        request=_FakeRequest(query={"then": door}), cm=cm, user="alice",
        name="box", is_admin=False))
    assert resp.status_code == 303
    assert resp.headers["location"] == landing
    assert cm.started == [("alice", "box")]


def test_an_unknown_destination_still_starts_but_leads_nowhere_new(make_config):
    # ?then= only picks between three of our own pages, so a stale or mangled
    # value falls back to the ordinary redirect rather than becoming a URL.
    cm = make_config(users={"alice": {"name": "alice"}},
                     instances={"alice": [_instance()]})
    resp = asyncio.run(mgmt.instance_connect(
        request=_FakeRequest(query={"then": "https://evil.example/"}), cm=cm,
        user="alice", name="box", is_admin=False))
    assert resp.headers["location"] == "/instances/box?user=alice"
    assert cm.started == [("alice", "box")]


def test_the_run_overrides_are_applied_on_the_way_to_the_desktop(make_config):
    # The whole reason the button asks: what is chosen has to reach the CR
    # before the viewer page nudges the instance awake.
    cm = make_config(users={"alice": {"name": "alice", "overrides": ALL_GRANTS}},
                     instances={"alice": [_instance(overrides={"gpuCount": 1})]})
    resp = asyncio.run(mgmt.instance_connect(
        request=_FakeRequest(query={"then": "desktop"}), cm=cm, user="alice",
        name="box", is_admin=False, apply_overrides="1", override_gpu_count="8"))
    assert resp.headers["location"] == "/connect/box?user=alice"
    inst = cm._instances["alice"][0]
    assert inst["runOverrides"] == {"gpuCount": 8}
    assert inst["overrides"] == {"gpuCount": 1}
