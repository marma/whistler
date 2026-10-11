"""External reach defaults to Nowhere and gates the `external` grid cell;
claim datasets name the uid/gid their server reads and writes as.

design/storage.md, "Datasets from existing claims". Both measured on
k3s-metal: a claim on an NFS export owned 2000:2000 (mode 755) was written
by a server with uid/gid 2000 — multipart included, every file landing
2000:2000 — and refused to the default 1000 (as an S3 500, not a 403).
"""
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment, FileSystemLoader

import whistler
from whistler.config import EXTERNAL_ZONE, KubeConfigManager, external_fenced
from whistler.portal import management as mgmt
from whistler.portal.management import (_build_dataset_data,
                                        _keep_fenced_external,
                                        _parse_external_sources)

CLAIM = {"source": "claim", "claimName": "corpus-data"}
FENCED = {**CLAIM, "externalSources": []}
OPEN = {"source": "managed", "size": "1Gi"}            # absent = Anywhere
LISTED = {**CLAIM, "externalSources": ["192.0.2.0/24"]}


# --- reach ------------------------------------------------------------------ #

@pytest.mark.parametrize("spec, fenced", [
    (FENCED, True),
    ({**OPEN, "externalSources": []}, True),
    (OPEN, False),
    (LISTED, False),
    # An S3 dataset has no external listener to fence.
    ({"bucket": "b", "externalSources": []}, False),
    (None, False),
])
def test_external_fenced(spec, fenced):
    assert external_fenced(spec) is fenced


def test_a_form_that_does_not_answer_creates_a_fenced_dataset():
    assert _parse_external_sources(None, None, default="nowhere") == []
    # An edit keeps reading a missing answer as it always did.
    assert _parse_external_sources(None, None) is None
    assert _parse_external_sources("anywhere", None, default="nowhere") is None


def _render_form(dataset):
    env = Environment(loader=FileSystemLoader(
        Path(whistler.__file__).parent / "portal" / "templates"))
    env.globals.update(can=lambda *a: True)
    request = SimpleNamespace(url=SimpleNamespace(path="/admin/datasets"),
                              state=SimpleNamespace(role="admin"),
                              query_params={})
    return env.get_template("admin/dataset_form.html").render(
        dataset=dataset, request=request, current_user="admin",
        external_enabled=True, external_can_filter=True,
        external_source_filter="traefik", has_credentials=False,
        is_admin=True)


def _checked_reach(html):
    for reach in ("anywhere", "listed", "nowhere"):
        tag = html.split(f'value="{reach}" id="reach-{reach}"')[1][:60]
        if "checked" in tag:
            return reach


def test_a_new_dataset_form_starts_at_nowhere():
    assert _checked_reach(_render_form(None)) == "nowhere"


def test_an_existing_dataset_without_the_field_is_still_anywhere():
    assert _checked_reach(_render_form({"name": "m", **OPEN})) == "anywhere"


# --- the locked cell -------------------------------------------------------- #

DATASETS = {"fenced": FENCED, "open": OPEN}


def test_a_fenced_cell_keeps_what_it_had():
    previous = {EXTERNAL_ZONE: {"fenced": "read-only"}}
    posted = {EXTERNAL_ZONE: {"fenced": "allowed", "open": "allowed"},
              "lab": {"fenced": "allowed"}}
    assert _keep_fenced_external(posted, previous, DATASETS) == {
        EXTERNAL_ZONE: {"fenced": "read-only", "open": "allowed"},
        "lab": {"fenced": "allowed"}}


def test_a_fenced_cell_cannot_be_newly_granted():
    posted = {EXTERNAL_ZONE: {"fenced": "allowed"}}
    assert _keep_fenced_external(posted, {}, DATASETS) == {}


def test_a_fenced_cell_cannot_be_cleared_either():
    # Locked means locked: revoking is done on an open dataset, or by
    # deleting the grant where the dataset is reachable — not by a cell the
    # admin could not see change.
    previous = {EXTERNAL_ZONE: {"fenced": "allowed"}}
    assert _keep_fenced_external({}, previous, DATASETS) == previous


def test_the_grid_marks_fenced_rows():
    import asyncio
    defs = {"fenced": FENCED, "open": OPEN}
    cm = SimpleNamespace(
        get_dataset_definitions=lambda: defs,
        is_archived_dataset=KubeConfigManager.is_archived_dataset,
        has_dataset_server=KubeConfigManager.has_dataset_server)

    async def run(func, *args):
        return func(*args)
    request = SimpleNamespace(app=SimpleNamespace(
        state=SimpleNamespace(run=run)))
    rows = asyncio.run(mgmt._matrix_sections(request, cm))[-1]["rows"]
    assert {r["key"]: r["external_fenced"] for r in rows} == {
        "fenced": True, "open": False}


class _Form(dict):
    def getlist(self, key):
        value = self.get(key, [])
        return value if isinstance(value, list) else [value]


async def test_saving_a_group_pins_its_fenced_cell(make_config):
    cm = make_config()
    cm.datasets = DATASETS
    cm.groups["lab"] = {"volumeAccess": {EXTERNAL_ZONE: {"fenced": "read-only"}}}

    async def run(func, *args):
        return func(*args)
    request = SimpleNamespace(app=SimpleNamespace(
        state=SimpleNamespace(cm=cm, run=run)))
    await mgmt._save_group_or_400(request, "update", "lab", _Form({
        f"access__{EXTERNAL_ZONE}__fenced": "allowed",
        f"access__{EXTERNAL_ZONE}__open": "read-only"}))
    assert cm.groups["lab"]["volumeAccess"] == {
        EXTERNAL_ZONE: {"fenced": "read-only", "open": "read-only"}}


# --- identity --------------------------------------------------------------- #

@pytest.mark.parametrize("spec, problem", [
    ({**CLAIM, "uid": 2000, "gid": 2000}, None),
    ({**CLAIM, "uid": 2000}, None),
    ({**CLAIM, "uid": 0}, "UID must be a whole number from 1"),
    ({**CLAIM, "gid": -5}, "GID must be"),
    ({**CLAIM, "uid": "abc"}, "UID must be"),
    ({**CLAIM, "uid": True}, "UID must be"),
    ({**CLAIM, "uid": 2 ** 31}, "UID must be"),
    # Elsewhere it would be saved and do nothing.
    ({**OPEN, "uid": 2000}, "only be set on a dataset from an existing claim"),
    ({"bucket": "b", "gid": 2000}, "only be set"),
])
def test_identity_problems(spec, problem):
    got = KubeConfigManager.dataset_spec_problem(spec)
    if problem is None:
        assert got is None
    else:
        assert problem in got


def _security(definition):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.namespace = "whistler"
    deployment, _ = cm._build_dataset_server_manifests(
        volume="corpus", definition=definition,
        image="ghcr.io/versity/versitygw:v1.7.0", claim="corpus-data",
        root_secret="r", keys_secret="k")
    return deployment["spec"]["template"]["spec"]["securityContext"]


def test_the_server_runs_as_the_datasets_identity():
    sc = _security({**CLAIM, "uid": 2000, "gid": 3000})
    assert (sc["runAsUser"], sc["runAsGroup"]) == (2000, 3000)
    assert sc["runAsNonRoot"] is True and "fsGroup" not in sc


def test_the_identity_defaults_to_1000():
    sc = _security(CLAIM)
    assert (sc["runAsUser"], sc["runAsGroup"]) == (1000, 1000)


def test_a_managed_dataset_ignores_an_identity():
    sc = _security({**OPEN, "uid": 2000})
    assert (sc["runAsUser"], sc["fsGroup"]) == (1000, 1000)


def test_the_portal_reads_the_identity_fields():
    data = _build_dataset_data("c", "", "", "", "", "", "", "", True,
                               source="claim", claim_name="corpus-data",
                               uid=" 2000 ", gid="")
    assert data["uid"] == 2000 and data["gid"] is None
    # Typed junk is kept so the save refuses it by name.
    assert _build_dataset_data("c", "", "", "", "", "", "", "", True,
                               source="claim", claim_name="x",
                               uid="2k")["uid"] == "2k"
    for source in ("managed", "s3"):
        data = _build_dataset_data("c", "", "", "b", "", "", "", "", True,
                                   source=source, size="1Gi", uid="2000",
                                   gid="2000")
        assert data["uid"] is None and data["gid"] is None
