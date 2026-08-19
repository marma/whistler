"""Shared datasets as their own kind (Dataset CRs), catalog + mode ceiling.

Datasets moved out of the volume catalog because they are not Kubernetes
volume sources: every other entry there is copied straight into a pod spec by
_build_volume_wiring. They are granted by name like a volume, mounted by
cloud-init, and never chosen as an instance mount. See design/storage.md.
"""
from whistler.config import KubeConfigManager


def _manager(**attrs):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.namespace = "whistler"
    for k, v in attrs.items():
        setattr(cm, k, v)
    return cm


# --- the readOnly ceiling --------------------------------------------------- #

def test_read_only_dataset_overrides_a_read_write_grant():
    # A ceiling, not a default. This is the only way to say "nobody writes
    # this", and it matters because the composition rule cuts the other way:
    # an empty allow-list means unrestricted, so without it a dataset is
    # writable by every user who has no list at all.
    ro_dataset = {"bucket": "b", "readOnly": True}
    assert KubeConfigManager.dataset_mode(ro_dataset, "rw") == "ro"
    assert KubeConfigManager.dataset_mode(ro_dataset, "ro") == "ro"


def test_without_the_ceiling_the_grant_decides():
    plain = {"bucket": "b"}
    assert KubeConfigManager.dataset_mode(plain, "rw") == "rw"
    assert KubeConfigManager.dataset_mode(plain, "ro") == "ro"
    # Anything unrecognised is read-write, the pre-groups default.
    assert KubeConfigManager.dataset_mode(plain, "") == "rw"
    assert KubeConfigManager.dataset_mode({}, "rw") == "rw"
    assert KubeConfigManager.dataset_mode(None, "rw") == "rw"


def test_a_read_only_dataset_leaves_its_rw_proxy_admitting_nobody():
    # The ceiling has to reach the fencing policy, not just the mount: the
    # guest is root and can point rclone anywhere, so what stops a write is
    # that the rw proxy admits no one.
    cm = _manager(
        users={"alice": {"name": "alice"}},
        groups={"proj": {"members": ["alice"],
                         "volumes": [{"name": "refdata", "mode": "rw"}]}},
        datasets={"refdata": {"bucket": "b", "readOnly": True}},
    )
    assert cm.s3_proxy_users("refdata", "rw") == []
    assert cm.s3_proxy_users("refdata", "ro") == ["alice"]


# --- catalog ---------------------------------------------------------------- #

def test_legacy_type_s3_volumes_are_still_datasets():
    # Values written before the Dataset kind keep working.
    volumes = [
        {"name": "scratch", "persistentVolumeClaim": {"claimName": "p"}},
        {"name": "old", "type": "s3", "bucket": "b"},
    ]
    assert list(KubeConfigManager.s3_volume_definitions(volumes)) == ["old"]


def test_a_dataset_cr_wins_over_a_legacy_volume_of_the_same_name():
    # The CR is the one an admin can edit in the portal, so it must be the one
    # that takes effect — otherwise a saved change would appear to do nothing.
    cm = _manager(volumes=[{"name": "refdata", "type": "s3", "bucket": "old"}],
                  datasets=None)

    def _fake_list(*a, **kw):
        return {"items": [{"metadata": {"name": "refdata"},
                           "spec": {"bucket": "new"}}]}

    class _Api:
        list_namespaced_custom_object = staticmethod(_fake_list)
    cm.api = _Api()
    cm.group, cm.version = "whistler.martinmalmsten.net", "v1"
    cm._load_datasets()
    assert cm.datasets["refdata"]["bucket"] == "new"


def test_a_failed_load_keeps_the_previous_catalog():
    # An empty catalog would silently unmount every dataset on the next
    # session build, so a transient API error must not produce one.
    cm = _manager(datasets={"refdata": {"bucket": "b"}})
    cm._load_datasets()  # no .api at all -> AttributeError path
    assert cm.datasets == {"refdata": {"bucket": "b"}}


# --- one bad dataset must not stop the cluster ------------------------------ #

def test_a_dataset_with_no_credential_is_refused_not_raised():
    # Measured 2026-08-18. A dataset saved without a credentialsSecret made
    # _build_s3_proxy_manifests raise KeyError inside the session reconcile,
    # which kopf retried forever — so ONE malformed dataset stopped every VM
    # in the cluster from starting. Datasets are admin-editable, so malformed
    # ones are ordinary; refusing this dataset is right, raising is not.
    cm = _manager(s3_proxy_image="rclone/rclone:latest", s3_proxy_resources={})
    assert cm.ensure_s3_proxy("refdata", "ro", {"bucket": "b"}) is False
    assert cm.ensure_s3_proxy("refdata", "ro", {}) is False
    assert cm.ensure_s3_proxy("refdata", "ro", None) is False


def test_one_broken_dataset_does_not_block_the_others():
    # The guarantee that matters: a session comes up with the datasets that
    # work, rather than not coming up at all.
    cm = _manager(
        users={"alice": {"name": "alice"}}, groups={},
        datasets={"broken": {"bucket": "b"},                      # no credential
                  "good": {"bucket": "b", "credentialsSecret": "s"}},
    )
    prepared = []

    def _ensure(volume, mode, definition):
        if volume == "broken":
            raise RuntimeError("boom")     # anything at all
        prepared.append(volume)
        return False                       # stop before the cluster calls

    cm.ensure_s3_proxy = _ensure
    cm._refresh_s3_proxy_policies = lambda name: None
    cm.get_user_allowed_volumes = lambda u: []
    cm.get_user_volume_modes = lambda u: {}
    assert cm.session_shared_datasets("alice") == []
    # "broken" raised and was skipped; "good" was still reached.
    assert prepared == ["good"]
