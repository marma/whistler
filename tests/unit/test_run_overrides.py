"""One-shot overrides: what a run is built from, and how it gets there.

An instance carries two slices. ``spec.overrides`` are its *defaults* — the
edit form's values, meant to persist. ``spec.runOverrides`` is the answer the
portal's start dialog gave for the run that is starting, and it is written and
cleared by the start itself, so choosing a value there changes what happens now
and nothing about what the instance starts with next time.

The reason it can't simply be one slice is the operator: ``ensure_session``
reads the CR at reconcile, so a start-time value that isn't on the CR is a
value that doesn't happen. The reason it can't be a merge is the dialog: it is
prefilled from the defaults and submits the whole picture, so a field left
blank means "not this run" and merging would put the default back.
"""
from unittest.mock import MagicMock

import pytest
from kubernetes.client.rest import ApiException

from whistler.config import (KubeConfigManager, START_ANNOTATION,
                             STOP_ANNOTATION, effective_session_overrides)


# --- the rule --------------------------------------------------------------- #

def test_no_run_slice_means_the_instances_own_defaults():
    spec = {"overrides": {"gpuCount": 1}}
    assert effective_session_overrides(spec) == {"gpuCount": 1}


def test_a_run_slice_wins_outright():
    # Outright, not merged: `resources` is absent from the run's answer because
    # the user cleared those fields, and putting the default back would make a
    # cleared field impossible to express.
    spec = {"overrides": {"gpuCount": 1, "resources": {"cpu": "8"}},
            "runOverrides": {"gpuCount": 4}}
    assert effective_session_overrides(spec) == {"gpuCount": 4}


def test_an_empty_run_slice_is_a_choice_not_an_absence():
    # "This run: no overrides at all" has to be expressible, or a dialog with
    # every field cleared would silently run with the defaults it just cleared.
    spec = {"overrides": {"gpuCount": 1}, "runOverrides": {}}
    assert effective_session_overrides(spec) == {}


def test_neither_slice_is_no_overrides():
    assert effective_session_overrides({}) is None
    assert effective_session_overrides(None) is None


# --- how it is written ------------------------------------------------------ #

def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.api = MagicMock()
    cm._get_user_namespace = lambda u: f"whistler-user-{u}"
    return cm


def test_a_plain_start_clears_the_run_slice_in_the_same_patch():
    # An explicit null is how a merge patch removes a key. It has to ride along
    # with the start mark: a run's overrides must never be visible without a
    # start, nor survive one that did not choose them.
    cm = _manager()
    assert cm.trigger_instance_start("alice", "box") is True

    body = cm.api.patch_namespaced_custom_object.call_args[0][5]
    assert float(body["metadata"]["annotations"][START_ANNOTATION]) > 0
    assert body["spec"]["runOverrides"] is None
    assert cm.api.replace_namespaced_custom_object.call_count == 0


def test_a_start_carrying_overrides_replaces_rather_than_patches():
    # Merge-patching a map cannot delete entries, so a volume mount chosen for
    # the previous run would survive into a run that did not ask for it.
    cm = _manager()
    cm.api.get_namespaced_custom_object.return_value = {
        "metadata": {"name": "alice-box", "annotations": {}},
        "spec": {"overrides": {"gpuCount": 1},
                 "runOverrides": {"volumes": {"old": "/mnt/old"}}},
    }
    assert cm.trigger_instance_start("alice", "box", {"gpuCount": 4}) is True

    body = cm.api.replace_namespaced_custom_object.call_args[0][5]
    assert body["spec"]["runOverrides"] == {"gpuCount": 4}
    assert body["spec"]["overrides"] == {"gpuCount": 1}   # defaults untouched
    assert float(body["metadata"]["annotations"][START_ANNOTATION]) > 0


def test_an_empty_run_slice_is_written_not_dropped():
    cm = _manager()
    cm.api.get_namespaced_custom_object.return_value = {
        "metadata": {"annotations": {}}, "spec": {"overrides": {"gpuCount": 1}},
    }
    cm.trigger_instance_start("alice", "box", {})

    body = cm.api.replace_namespaced_custom_object.call_args[0][5]
    assert body["spec"]["runOverrides"] == {}


def test_a_failed_start_is_reported():
    cm = _manager()
    cm.api.patch_namespaced_custom_object.side_effect = ApiException(status=404)
    assert cm.trigger_instance_start("alice", "gone") is False


def test_a_failed_read_does_not_start_anything():
    # The overrides and the start are one act; if the CR can't be read there is
    # nothing to start and nothing to write.
    cm = _manager()
    cm.api.get_namespaced_custom_object.side_effect = ApiException(status=404)
    assert cm.trigger_instance_start("alice", "gone", {"gpuCount": 1}) is False
    assert cm.api.replace_namespaced_custom_object.call_count == 0


# --- the nudge that must not answer the question ---------------------------- #
#
# The viewer pages (/connect, /term, /vnc, /console) start the instance they are
# about to show. When the start dialog stands in front of one of those buttons,
# the browser lands on the viewer page by 303 *milliseconds after* the dialog's
# own start — so a plain start there erased the run's overrides before the
# operator ever read them (a GPU picked in the dialog booted with none).

def test_a_viewer_nudge_leaves_a_running_instances_run_slice_alone():
    cm = _manager()
    cm.api.get_namespaced_custom_object.return_value = {
        "metadata": {"annotations": {START_ANNOTATION: "1000.0"}},
        "spec": {"runOverrides": {"gpuType": "NVIDIA-GeForce-RTX-4090"}},
    }
    assert cm.ensure_instance_running("alice", "box") is True

    body = cm.api.patch_namespaced_custom_object.call_args[0][5]
    assert float(body["metadata"]["annotations"][START_ANNOTATION]) > 1000.0
    assert "spec" not in body           # the run's answers are not this call's business
    assert cm.api.replace_namespaced_custom_object.call_count == 0


def test_a_viewer_nudge_on_a_stopped_instance_is_an_ordinary_start():
    # Nothing chose anything for this run, so it must not inherit what the
    # previous one chose: a plain start, clearing the slice.
    cm = _manager()
    cm.api.get_namespaced_custom_object.return_value = {
        "metadata": {"annotations": {START_ANNOTATION: "1000.0",
                                     STOP_ANNOTATION: "2000.0"}},
        "spec": {"runOverrides": {"gpuType": "__none__"}},
    }
    assert cm.ensure_instance_running("alice", "box") is True

    body = cm.api.patch_namespaced_custom_object.call_args[0][5]
    assert body["spec"]["runOverrides"] is None


def test_a_viewer_nudge_on_an_unreadable_instance_reports_failure():
    cm = _manager()
    cm.api.get_namespaced_custom_object.side_effect = ApiException(status=404)
    assert cm.ensure_instance_running("alice", "gone") is False
    assert cm.api.patch_namespaced_custom_object.call_count == 0


# --- what reads it ---------------------------------------------------------- #

@pytest.mark.parametrize("spec,expected", [
    ({"overrides": {"zone": "open"}}, "open"),
    ({"overrides": {"zone": "open"}, "runOverrides": {"zone": "locked"}}, "locked"),
    ({"overrides": {"zone": "open"}, "runOverrides": {}}, None),
])
def test_the_gateway_sees_the_zone_the_run_is_actually_in(spec, expected):
    # resolve_ssh_target derives the ssh posture and channel ceiling from the
    # session's zone. If it read the defaults while the workload was built from
    # the run's slice, the gateway would enforce one zone's posture on a
    # session running in another.
    assert (effective_session_overrides(spec) or {}).get("zone") == expected
