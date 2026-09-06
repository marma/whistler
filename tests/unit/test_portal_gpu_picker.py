"""The "No GPU" option, across the three surfaces that pick a GPU.

Zero GPUs is a thing a user has to be able to *say*, and until 2026-08-29 the
only way to say it was to leave a count box empty (template form) or to hold
the separate `gpuCount` grant and type a 0 (overrides). Both were oblique, and
the second was worse than oblique: `resources.gpu: 0` is not "no GPU" to either
spec builder — they keyed on the field being present — so a template with the
GPU turned off still attached a card to its VM.

So "No GPU" is an option in the GPU *type* picker, its value is `GPU_NONE`, and
it resolves to the ABSENCE of resources.gpu.

**Every answer names something.** The template picker offers `GPU_NONE` and the
types in the live catalog, and nothing else — there is no "any available type".
That option read as a choice but was the absence of one: it worked by luck on a
cluster with a single kind of card and meant nothing on a mixed one, where a
template that cannot say which card it wants is a template that cannot start.
A legacy template in that state is not coerced either way; the form refuses to
guess and makes the admin pick (see the `tpl_unpinned` tests below).

The overrides picker has one more option, and it is not vague: the blank
`— keep template's value —`, which is "do not override this" — the same thing
the plain play button does.
"""
import re
from types import SimpleNamespace

import pytest

from whistler.config import GPU_NODE_LABEL, GPU_NONE
from whistler.portal import management as mgmt
from whistler.portal.management import _build_session_overrides, _template_form_data


def test_the_sentinel_can_never_be_a_real_gpu_type():
    # Catalog names are `nvidia.com/gpu.product` label VALUES, and a label
    # value must begin with an alphanumeric. So no cluster, however labelled,
    # can produce a type that collides with this.
    assert GPU_NONE.startswith("_")
    assert not re.match(r"^[A-Za-z0-9]", GPU_NONE)


# --- the template form ------------------------------------------------------ #

_TPL_BASE = dict(name="t", display_name="T", image="i", description="",
                 cpu="1", memory="2Gi", personal_mount="/userdata", mode="ssh",
                 runtime="vm", privileged=None, fuse=None, display_port=None,
                 viewer=None)


def _tpl(**gpu):
    return _template_form_data(**_TPL_BASE, **gpu)


def test_no_gpu_writes_neither_a_count_nor_a_node_selector():
    data = _tpl(gpu_type=GPU_NONE, gpu="1")
    assert "gpu" not in data["resources"]
    assert data["nodeSelector"] == {}
    # The rest of the form is untouched.
    assert data["resources"] == {"cpu": "1", "memory": "2Gi"}


def test_a_count_of_zero_is_no_gpu_said_the_long_way():
    # Normalised here so the two spellings cannot produce different specs —
    # and so a template saved this way before today stops attaching a card.
    data = _tpl(gpu_type="", gpu="0")
    assert "gpu" not in data["resources"]
    assert data["nodeSelector"] == {}


def test_a_zero_count_also_drops_a_pinned_type():
    # Zero GPUs of an A100 is no GPU, so pinning the session to A100 nodes it
    # has no use for would be the wrong half to keep.
    data = _tpl(gpu_type="A100", gpu="0")
    assert "gpu" not in data["resources"]
    assert data["nodeSelector"] == {}


def test_a_count_with_no_type_named_is_no_gpu():
    """There is no "some GPU, unspecified". The picker cannot submit a blank
    (it is `required`, and its only blank option is the disabled placeholder),
    so a blank reaching here is not a choice being expressed — and reading it
    as No GPU is what keeps every saved template answerable on a cluster with
    more than one kind of card."""
    data = _tpl(gpu_type="", gpu="2")
    assert "gpu" not in data["resources"]
    assert data["nodeSelector"] == {}


def test_a_named_type_pins_the_node_selector():
    data = _tpl(gpu_type="A100", gpu="1")
    assert data["resources"]["gpu"] == "1"
    assert data["nodeSelector"] == {GPU_NODE_LABEL: "A100"}


# --- what the pickers render ------------------------------------------------ #

def _render(name, **context):
    request = SimpleNamespace(url=SimpleNamespace(path="/admin/templates"))
    return mgmt.templates.env.get_template(name).render(request=request, **context)


def _selected(html, select_id):
    block = html.split(f'id="{select_id}"')[1].split("</select>")[0]
    return re.findall(r'<option value="([^"]*)"[^>]*\bselected\b', block)


def _template_form(tpl):
    return _render("admin/template_form.html", current_user="alice",
                   is_admin=True, tpl=tpl, images=["img"],
                   gpu_types=["A100", "RTX-4090"], zones=["default"])


@pytest.mark.parametrize("tpl,expected", [
    # A new template defaults to No GPU — the common case, and the one that
    # was previously reachable only by leaving a box blank.
    (None, GPU_NONE),
    ({"name": "t", "resources": {}, "nodeSelector": {}}, GPU_NONE),
    # A zero already on the CR reads back as what it means.
    ({"name": "t", "resources": {"gpu": "0"}, "nodeSelector": {}}, GPU_NONE),
    # Count but no pin: nothing real is preselected — see the test below.
    ({"name": "t", "resources": {"gpu": "1"},
      "nodeSelector": {GPU_NODE_LABEL: "A100"}}, "A100"),
])
def test_template_form_preselects_the_right_one_of_the_three(tpl, expected):
    assert _selected(_template_form(tpl), "tpl-gpu-type") == [expected]


def test_template_picker_offers_only_no_gpu_and_named_types():
    block = _template_form(None).split('id="tpl-gpu-type"')[1].split("</select>")[0]
    assert re.findall(r'<option value="([^"]*)"', block) == [
        GPU_NONE, "A100", "RTX-4090"]


def test_a_template_naming_no_type_is_made_to_choose_rather_than_guessed_at():
    """The one state the picker cannot represent: N GPUs, no type. Preselecting
    No GPU would throw the GPU away on the next save; preselecting a type would
    invent an answer nobody gave. So neither — a disabled placeholder holds the
    selection and the browser will not submit the form until the admin picks."""
    tpl = {"name": "t", "resources": {"gpu": "2"}, "nodeSelector": {}}
    html = _template_form(tpl)
    block = html.split('id="tpl-gpu-type"')[1].split("</select>")[0]
    assert "required" in html.split('id="tpl-gpu-type"')[1].split(">")[0]
    # The selection is held by a disabled placeholder, so nothing real is
    # picked and the form cannot be submitted as it stands.
    assert '<option value="" selected disabled>' in block
    assert _selected(html, "tpl-gpu-type") == [""]
    # ...and the admin is told why.
    assert "names no type" in html


# --- the override fields (start dialog, create and edit instance) ----------- #

def _override_fields(**context):
    ctx = dict(current_user="alice", overrides={}, gpu_types=["A100"],
               zones=["default"], cur=None)
    ctx.update(context)
    return _render("user/_override_fields.html", **ctx)


def test_overrides_offer_no_gpu_beside_keeping_the_templates_value():
    html = _override_fields(overrides={"gpuType": True})
    assert f'value="{GPU_NONE}"' in html
    assert ">No GPU<" in html
    # And the empty option keeps its own, different meaning.
    assert "keep template&#39;s value" in html or "keep template's value" in html


def test_overrides_preselect_no_gpu_when_the_run_already_asked_for_it():
    html = _override_fields(overrides={"gpuType": True},
                            cur={"overrides": {"gpuType": GPU_NONE}})
    assert _selected(html, "override-gpu-type") == [GPU_NONE]


def test_no_gpu_is_not_offered_without_the_gpu_type_grant():
    # The whole block is grant-gated; No GPU is a gpuType answer, not a fourth
    # thing that appears for everyone.
    assert GPU_NONE not in _override_fields(overrides={"gpuCount": True})


def test_the_count_box_is_only_wired_to_the_picker_when_both_are_granted():
    both = _override_fields(overrides={"gpuType": True, "gpuCount": True})
    assert 'id="override-gpu-count-field"' in both
    assert "override-gpu-type" in both.split("<script>")[-1]
    # With only the count granted there is no picker to listen to, and the
    # script would throw on a null element.
    count_only = _override_fields(overrides={"gpuCount": True})
    assert "override-gpu-count-field" in count_only
    assert "addEventListener" not in count_only


# --- turning a GPU off has to survive the save ------------------------------ #

def test_editing_a_gpu_template_to_no_gpu_actually_clears_the_cr():
    """The one that would be easy to get wrong. save_system_template merges the
    incoming spec over the existing one, so a field that is merely *omitted*
    keeps its old value — which for `resources` would make "No GPU" a no-op on
    every template that already had one. It works because the merge is
    top-level and an incoming `resources` replaces the whole map; pinned here
    so a future move to a recursive merge does not quietly break it."""
    from whistler.config import KubeConfigManager

    existing = {"spec": {"user": "system", "image": "i", "runtime": "vm",
                         "resources": {"cpu": "8", "gpu": "1"},
                         "nodeSelector": {GPU_NODE_LABEL: "A100"}},
                "metadata": {"resourceVersion": "7"}}
    written = {}

    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group, cm.version, cm.namespace = "g", "v1", "whistler"
    cm.api = SimpleNamespace(
        get_namespaced_custom_object=lambda *a, **k: existing,
        replace_namespaced_custom_object=(
            lambda g, v, ns, plural, name, body: written.update(body["spec"])),
    )
    cm._load_templates = lambda: None

    assert cm.save_system_template(_tpl(gpu_type=GPU_NONE, gpu="1")) is True
    assert "gpu" not in written["resources"]
    assert written["nodeSelector"] == {}
    # ...and the rest of the template is still there.
    assert written["resources"]["cpu"] == "1"
    assert written["image"] == "i"


# --- the viewer select ------------------------------------------------------ #

def test_desktop_template_records_the_viewer():
    data = _template_form_data(**{**_TPL_BASE, "mode": "desktop",
                                  "viewer": "websockets"})
    assert data["viewer"] == "websockets"


def test_viewer_is_desktop_only_and_enum_bound():
    # ssh templates have no display, so the (always-submitted) select is dropped.
    assert "viewer" not in _template_form_data(**{**_TPL_BASE, "viewer": "vnc"})
    # Anything outside the CRD enum is left off so save_system_template's
    # merge keeps the existing value instead of failing validation.
    for bad in (None, "", "rdp"):
        d = _template_form_data(**{**_TPL_BASE, "mode": "desktop", "viewer": bad})
        assert "viewer" not in d, bad
