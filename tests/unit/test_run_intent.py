"""Run intent: who decides a session should be running, and where.

Start and stop are both annotations on the Session CR, and the operator's
reconcile is the only thing that touches a pod or a VirtualMachine. That is
what lets the SSH gateway — the process that terminates untrusted SSH — hold
no KubeVirt write at all (design/proxyjump.md, "Stopping through the
operator").

The rule under test is "latest mark wins", and the reason it is two timestamps
rather than one desired-state field is in the STOP_ANNOTATION comment: a
one-shot request the operator clears would ping-pong, because clearing it is
itself an update whose reconcile sees the surviving start mark.
"""
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from whistler.config import (KubeConfigManager, START_ANNOTATION,
                             STOP_ANNOTATION, run_intent)


# --- the rule --------------------------------------------------------------- #

def test_no_marks_means_stopped():
    # A freshly created session has neither: it must not start running before
    # anyone has asked to use it.
    assert run_intent({}) is False
    assert run_intent(None) is False


def test_a_start_mark_alone_means_running():
    assert run_intent({START_ANNOTATION: "1000.0"}) is True


def test_a_newer_stop_mark_wins():
    assert run_intent({START_ANNOTATION: "1000.0",
                       STOP_ANNOTATION: "1001.0"}) is False


def test_a_newer_start_mark_wins_so_restart_works():
    """The stop is not sticky: pressing start after a stop has to boot it."""
    assert run_intent({START_ANNOTATION: "1002.0",
                       STOP_ANNOTATION: "1001.0"}) is True


def test_a_tie_goes_to_stopped():
    # Two writes inside one clock tick are a stop landing on a start, and
    # stopped is the state that runs nothing and holds no home volume.
    assert run_intent({START_ANNOTATION: "1000.0",
                       STOP_ANNOTATION: "1000.0"}) is False


def test_the_two_annotation_formats_are_comparable():
    """`trigger_instance_start` wrote an ISO string for a long time and live
    CRs still hold one; the gateway writes an epoch float. Comparing them as
    strings would have made "2026-…" > "17874…" always true."""
    iso_2026 = "2026-08-23T11:19:26.250000"
    epoch_1970 = "1000.0"
    assert run_intent({START_ANNOTATION: iso_2026,
                       STOP_ANNOTATION: epoch_1970}) is True
    assert run_intent({START_ANNOTATION: epoch_1970,
                       STOP_ANNOTATION: iso_2026}) is False


def test_an_undatable_start_mark_still_starts():
    """Backward compatibility, and the integration fixture writes the literal
    "test": presence alone has always meant "wants to run", and with no stop
    mark to compare against that still holds."""
    assert run_intent({START_ANNOTATION: "test"}) is True


def test_an_uncomparable_pair_fails_closed_to_stopped():
    assert run_intent({START_ANNOTATION: "test",
                       STOP_ANNOTATION: "1000.0"}) is False
    assert run_intent({START_ANNOTATION: "1000.0",
                       STOP_ANNOTATION: "nonsense"}) is False


# --- stop_instance writes a CR annotation and nothing else ------------------ #

def _manager():
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.api = MagicMock()
    cm._get_user_namespace = lambda u: f"whistler-user-{u}"
    return cm


def test_stop_patches_the_session_cr():
    cm = _manager()
    assert cm.stop_instance("alice", "box") is True

    group, version, ns, plural, name, body = \
        cm.api.patch_namespaced_custom_object.call_args[0]
    assert (group, version, ns, plural, name) == (
        "whistler.martinmalmsten.net", "v1", "whistler-user-alice",
        "sessions", "alice-box")
    stamp = body["metadata"]["annotations"][STOP_ANNOTATION]
    assert float(stamp) > 0          # an epoch float, comparable with a start


def test_stop_touches_no_kubevirt_object():
    """The whole point: stopping is a CR write, so no caller of it needs a
    KubeVirt verb. A regression here is a silent RBAC dependency — it would
    only show up as a 403 in whichever process happened to call stop."""
    cm = _manager()
    cm.stop_instance("alice", "box")

    for call in cm.api.patch_namespaced_custom_object.call_args_list:
        assert call[0][0] != "kubevirt.io"
    assert cm.api.create_namespaced_custom_object.call_count == 0
    assert cm.api.delete_namespaced_custom_object.call_count == 0


def test_stopping_an_absent_session_succeeds():
    # The caller's goal — nothing running under that name — is already met.
    cm = _manager()
    cm.api.patch_namespaced_custom_object.side_effect = ApiException(status=404)
    assert cm.stop_instance("alice", "gone") is True


def test_a_failed_patch_is_reported():
    cm = _manager()
    cm.api.patch_namespaced_custom_object.side_effect = ApiException(status=403)
    assert cm.stop_instance("alice", "box") is False


def test_start_writes_a_comparable_mark():
    """Start and stop must be in the same units, or "latest wins" is a coin
    flip. This is the regression guard for the ISO string it used to write."""
    cm = _manager()
    assert cm.trigger_instance_start("alice", "box") is True
    body = cm.api.patch_namespaced_custom_object.call_args[0][5]
    assert float(body["metadata"]["annotations"][START_ANNOTATION]) > 0
