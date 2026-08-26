"""What an account is *born* holding, now that empty grants nothing.

Every allow is explicit (2026-08-25), which turns "a User CR with no lists" —
the shape every account used to start life in — from the most permissive state
into the most useless one. Two creation paths therefore seed the two grants
that decide whether an account can be used at all: a door to come in through,
and the zone every unzoned template lands in. Nothing else is seeded, because
volumes and GPU types are grants an admin means to make.

The seed is a *creation* default and never a floor: narrowing it afterwards has
to stick, or the kiosk binding would be undone by the next operator restart.
"""
from types import SimpleNamespace

import pytest

from whistler.config import (DEFAULT_ZONE, ENTRY_POINTS, KubeConfigManager,
                             NEW_USER_ENTRY_POINTS, NEW_USER_ZONES)
from whistler.portal.management import admin_user_create


def test_the_seed_covers_every_door_and_the_default_zone():
    # Pinned because these two constants are the whole answer to "why is a
    # fresh install not locked out of itself".
    assert set(NEW_USER_ENTRY_POINTS) == set(ENTRY_POINTS)
    assert NEW_USER_ZONES == [DEFAULT_ZONE]


# --- the operator's bootstrap admin ---------------------------------------- #

class _Api:
    """Just enough custom-objects API to watch a create go by."""

    def __init__(self, existing=None):
        self.existing = existing or {}
        self.created = []

    def get_namespaced_custom_object(self, group, version, ns, plural, name):
        if name in self.existing:
            return self.existing[name]
        raise _not_found()

    def create_namespaced_custom_object(self, group, version, ns, plural, body):
        self.created.append(body)
        return body


def _not_found():
    from kubernetes.client.rest import ApiException
    return ApiException(status=404, reason="Not Found")


def _manager(api):
    cm = KubeConfigManager.__new__(KubeConfigManager)
    cm.group = "whistler.martinmalmsten.net"
    cm.version = "v1"
    cm.namespace = "whistler"
    cm.api = api
    return cm


def test_the_bootstrap_admin_is_created_able_to_sign_in(tmp_path, monkeypatch):
    seed = tmp_path / "bootstrapAdmin.yaml"
    seed.write_text("name: marma\npublicKeys: ['ssh-ed25519 AAAA marma']\n")
    monkeypatch.setattr("whistler.config.BOOTSTRAP_ADMIN_FILE", str(seed))
    api = _Api()
    _manager(api).ensure_bootstrap_admin()

    spec = api.created[0]["spec"]
    assert spec["admin"] is True
    # Without these the account this exists to create could not open the
    # portal it exists to be used from.
    assert set(spec["entryPoints"]) == set(ENTRY_POINTS)
    assert spec["allowedZones"] == [DEFAULT_ZONE]


def test_an_existing_admin_is_never_re_seeded(tmp_path, monkeypatch):
    """Create-if-absent, so an admin who has narrowed their own grants — or
    bound themselves to the kiosk — does not get them handed back on the next
    operator restart."""
    seed = tmp_path / "bootstrapAdmin.yaml"
    seed.write_text("name: marma\n")
    monkeypatch.setattr("whistler.config.BOOTSTRAP_ADMIN_FILE", str(seed))
    api = _Api(existing={"marma": {"spec": {"entryPoints": ["kiosk"]}}})
    _manager(api).ensure_bootstrap_admin()
    assert api.created == []


# --- the portal's "New User" form ------------------------------------------ #

class _CM:
    def __init__(self):
        self.saved = None

    def save_user(self, data):
        self.saved = data
        return True


def _request(cm):
    async def run(func, *args):
        return func(*args)
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(cm=cm, run=run)))


async def test_a_user_created_in_the_portal_can_sign_in_and_launch():
    cm = _CM()
    await admin_user_create(_request(cm), cm, "admin", name="alice",
                            public_keys=None, uid=None, run_as_user=None,
                            run_as_group=None, fs_group=None, is_admin_flag=None)
    assert set(cm.saved["entryPoints"]) == set(ENTRY_POINTS)
    assert cm.saved["allowedZones"] == [DEFAULT_ZONE]
    # The grants that cost something are not seeded: the admin grants those on
    # the detail page, one at a time, which is the point of explicit access.
    assert "allowedVolumes" not in cm.saved
    assert "allowedGpuTypes" not in cm.saved


async def test_editing_a_user_does_not_re_seed_the_grants():
    """The edit form shares _build_user_data with create. If the seed lived
    there, narrowing a user's entry points would be impossible — every save
    would hand them back."""
    from whistler.portal.management import admin_user_update
    cm = _CM()
    await admin_user_update(_request(cm), cm, "admin", "alice",
                            public_keys=None, uid=None, run_as_user=None,
                            run_as_group=None, fs_group=None, is_admin_flag=None)
    assert "entryPoints" not in cm.saved
    assert "allowedZones" not in cm.saved
