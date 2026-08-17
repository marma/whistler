"""cloud-init userData builder (whistler.cloudinit) — pure, no cluster."""
import os
import subprocess

import pytest
import yaml

from whistler.cloudinit import (
    build_user_data, resolve_uid, resolve_gid, HOME_DISK_PATH,
    HOME_DISK_SERIAL,
)

MOUNT_SCRIPT = "/usr/local/sbin/whistler-mount-home"


def _doc(**overrides):
    args = dict(username="alice", uid=1001,
                ssh_keys=["ssh-ed25519 AAA alice"], hostname="desk")
    args.update(overrides)
    text = build_user_data(**args)
    assert text.startswith("#cloud-config\n")
    return yaml.safe_load(text.split("\n", 1)[1])


def _script(**overrides):
    return next(f for f in _doc(**overrides)["write_files"]
                if f["path"] == MOUNT_SCRIPT)["content"]


def test_user_created_with_uid_keys_and_locked_password():
    (user,) = _doc()["users"]
    assert user["name"] == "alice"
    assert user["uid"] == "1001"
    assert user["lock_passwd"] is True
    assert user["sudo"] == "ALL=(ALL) NOPASSWD:ALL"
    assert user["ssh_authorized_keys"] == ["ssh-ed25519 AAA alice"]


def test_no_default_user_entry():
    # Listing only our user suppresses the image's built-in `default` user,
    # freeing uid 1000 for the fallback.
    doc = _doc()
    assert "default" not in doc["users"]


def test_home_is_a_local_disk_not_a_share():
    # The home is a per-instance virtio-blk disk now (design/storage.md), so
    # there is nothing for cloud-init's `mounts` module to do and no server
    # to name. A `mounts` entry would also put `mount -a` back on the boot
    # path, which is what `noauto` used to exist to prevent.
    doc = _doc()
    assert "mounts" not in doc
    body = _script()
    for gone in ("nfs", "NFS", "getent hosts", "mount.nfs4", "vers=4.2"):
        assert gone not in body


def test_disk_addressed_by_id_never_by_device_order():
    # /dev/vdb is a bet on probe order; losing it means formatting or
    # mounting the wrong disk as someone's home, silently. udev builds the
    # by-id path out of the serial _build_vm_spec stamps on the disk.
    body = _script()
    assert f"DISK={HOME_DISK_PATH}" in body
    assert HOME_DISK_SERIAL in HOME_DISK_PATH
    assert "/dev/vd" not in body


def test_mkfs_only_runs_when_blkid_finds_no_filesystem():
    # THE dangerous line in this module: reformatting on the second boot
    # destroys a home. blkid failing is the only thing that may trigger mkfs.
    body = _script()
    assert 'if ! blkid "$DISK" >/dev/null 2>&1; then' in body
    mkfs = next(l for l in body.splitlines()
                if l.strip().startswith("mkfs.ext4"))
    # never -F: if blkid is somehow wrong, mkfs's own refusal is the last
    # thing between a reboot and an empty home.
    assert " -F" not in mkfs and "--force" not in mkfs


def test_missing_blkid_refuses_to_format_rather_than_guess():
    # A missing blkid would fail exactly like an empty disk, so it must fail
    # closed. A guest with no home is recoverable; a wiped home is not.
    body = _script()
    assert "command -v blkid" in body
    guard = body.index("command -v blkid")
    assert "refusing" in body[guard:body.index("mkfs.ext4")]


def test_mount_root_is_chowned_but_never_recursively():
    # The mount root must end up owned by the user, but chown -R would be
    # slow on a large home and would undo ownership the user set on purpose.
    body = _script()
    assert "OWNER=1001:1001" in body
    assert 'chown "$OWNER" "$HOME_DIR"' in body
    assert "chown -R" not in body


def test_no_credentials_file_written():
    # A local disk has no credential at all — nothing to leak into a guest
    # whose user has root.
    doc = _doc()
    assert not any("credential" in f["path"] for f in doc["write_files"])


def test_no_package_install_and_mount_unit_armed():
    # NO packages: — with the default locked-down egress apt burns ~50s
    # timing out on unreachable mirrors, and the packages module runs before
    # runcmd, delaying the home that long past the login prompt. Nothing here
    # needs a package: mkfs.ext4 and blkid are in every image. The unit is
    # started non-blocking (the disk-probe wait must not stall boot) and
    # enabled so persistent-root guests remount on later boots.
    doc = _doc()
    assert "packages" not in doc
    cmds = doc["runcmd"]
    assert "systemctl enable whistler-home.service" in cmds
    assert cmds[-1] == "systemctl start --no-block whistler-home.service"


def test_ephemeral_session_gets_no_home_disk_machinery():
    # Ephemeral data is discarded anyway, so /home stays on the root disk and
    # none of the mount machinery is emitted — no script, no unit, no poller.
    doc = _doc(home_disk=False)
    paths = [f["path"] for f in doc["write_files"]]
    assert MOUNT_SCRIPT not in paths
    assert "/etc/systemd/system/whistler-home.service" not in paths
    assert not any("whistler-mount-home" in c for c in doc["bootcmd"])
    assert not any("whistler-home.service" in c for c in doc["runcmd"])
    # The user is still created, with the same identity.
    (user,) = doc["users"]
    assert user["uid"] == "1001"


def test_bootcmd_kicks_mount_before_runcmd_stage():
    # runcmd sits behind multi-user.target (snapd.seeded holds it ~30s on
    # stock Ubuntu, well past the login prompt), so a detached bootcmd
    # poller runs the mount script as soon as write_files lands it. It must
    # detach (setsid, backgrounded) — bootcmd must never block boot.
    _, kick = _doc()["bootcmd"]
    assert "/usr/local/sbin/whistler-mount-home" in kick
    assert kick.startswith("setsid ")
    assert kick.endswith("&")
    # The pre-mount home stays root-owned on purpose ("not ready" signal):
    # no chown/install anywhere in bootcmd.
    assert "install" not in kick and "chown" not in kick


def test_getty_respawned_after_mount_lands():
    # A console shell opened pre-mount keeps the shadowed root-disk dir as
    # its cwd forever; the mount script respawns the autologin getty after a
    # successful mount so fresh consoles land in the real home.
    script = next(f for f in _doc()["write_files"]
                  if f["path"] == "/usr/local/sbin/whistler-mount-home")
    assert "systemctl try-restart serial-getty@ttyS0.service" in script["content"]

    no_autologin = next(f for f in _doc(autologin=False)["write_files"]
                        if f["path"] == "/usr/local/sbin/whistler-mount-home")
    assert "serial-getty" not in no_autologin["content"]


def test_mount_unit_and_script_written():
    doc = _doc()
    unit = next(f for f in doc["write_files"]
                if f["path"] == "/etc/systemd/system/whistler-home.service")
    assert f"ExecStart={MOUNT_SCRIPT}" in unit["content"]
    assert "WantedBy=multi-user.target" in unit["content"]
    # A local disk waits on udev, not on the network.
    assert "local-fs.target" in unit["content"]
    assert "network-online" not in unit["content"]

    script = next(f for f in doc["write_files"] if f["path"] == MOUNT_SCRIPT)
    assert script["permissions"] == "0755"
    body = script["content"]
    assert "mount -o nosuid,nodev \"$DISK\" \"$HOME_DIR\"" in body
    # The virtio disk can probe in after bootcmd has fired, so wait for it
    # rather than failing the boot.
    assert 'while [ ! -e "$DISK" ]' in body
    # bootcmd kicks this off before cloud-init's mounts module exists to make
    # the mountpoint, so the script makes it itself.
    assert 'mkdir -p "$HOME_DIR"' in body


def test_hostname_set():
    assert _doc()["hostname"] == "desk"


AUTOLOGIN_DROPIN = "/etc/systemd/system/serial-getty@ttyS0.service.d/autologin.conf"


def test_serial_autologin_dropin():
    doc = _doc()
    dropin = next(f for f in doc["write_files"]
                  if f["path"] == AUTOLOGIN_DROPIN)
    assert "--autologin alice" in dropin["content"]
    assert "systemctl daemon-reload" in doc["runcmd"]


def test_autologin_disabled_omits_dropin():
    doc = _doc(autologin=False)
    assert not any(f["path"] == AUTOLOGIN_DROPIN for f in doc["write_files"])
    assert not any("serial-getty" in cmd for cmd in doc["runcmd"])


def test_authorized_keys_on_root_disk():
    # Keys live on the root disk, not the network-mounted home (StrictModes,
    # and the share is absent for most of first boot).
    doc = _doc()
    keyfile = next(f for f in doc["write_files"]
                   if f["path"] == "/etc/ssh/authorized_keys.d/alice")
    assert "ssh-ed25519 AAA alice" in keyfile["content"]
    assert keyfile["permissions"] == "0644"
    sshd_conf = next(f for f in doc["write_files"]
                     if f["path"] == "/etc/ssh/sshd_config.d/60-whistler.conf")
    assert "/etc/ssh/authorized_keys.d/%u" in sshd_conf["content"]


# --- host certificate (design/proxyjump.md, whistler/hostca.py) ----------- #

HOST_KEY = b"-----BEGIN OPENSSH PRIVATE KEY-----\nAAAA\n-----END OPENSSH PRIVATE KEY-----\n"
HOST_CERT = "ssh-ed25519-cert-v01@openssh.com AAAAcert alice-box"


def _certified(**overrides):
    return _doc(host_key=HOST_KEY, host_cert=HOST_CERT, **overrides)


def _file(doc, path):
    return next(f for f in doc["write_files"] if f["path"] == path)


def test_host_key_and_cert_written_with_sane_modes():
    doc = _certified()
    key = _file(doc, "/etc/ssh/whistler_host_ed25519_key")
    cert = _file(doc, "/etc/ssh/whistler_host_ed25519_key-cert.pub")
    assert key["content"] == HOST_KEY.decode()
    assert key["permissions"] == "0600"   # sshd refuses a world-readable key
    assert cert["content"] == HOST_CERT + "\n"
    assert cert["permissions"] == "0644"


def test_sshd_configured_to_offer_the_certificate():
    conf = _file(_certified(), "/etc/ssh/sshd_config.d/60-whistler.conf")["content"]
    assert "HostKey /etc/ssh/whistler_host_ed25519_key\n" in conf
    assert ("HostCertificate /etc/ssh/whistler_host_ed25519_key-cert.pub\n"
            in conf)
    # The authorized_keys directive is untouched.
    assert "/etc/ssh/authorized_keys.d/%u" in conf


def test_image_host_keys_are_listed_alongside_ours():
    """A HostKey directive REPLACES sshd's defaults rather than adding to
    them, so naming only the Whistler key makes sshd depend entirely on a file
    cloud-init writes — and a guest that cannot load it exits with "no
    hostkeys available", i.e. no SSH at all rather than SSH without a
    certificate. Regression guard for exactly that."""
    conf = _file(_certified(), "/etc/ssh/sshd_config.d/60-whistler.conf")["content"]
    for path in ("/etc/ssh/ssh_host_ed25519_key",
                 "/etc/ssh/ssh_host_rsa_key",
                 "/etc/ssh/ssh_host_ecdsa_key"):
        assert f"HostKey {path}\n" in conf
    # And ours comes last, so the certificate matches the key just above it.
    keys = [l for l in conf.splitlines() if l.startswith("HostKey ")]
    assert keys[-1].endswith("whistler_host_ed25519_key")


@pytest.mark.skipif(not os.path.exists("/usr/sbin/sshd"),
                    reason="needs a real sshd to validate the config")
def test_generated_sshd_config_survives_missing_key_files(tmp_path):
    """The claim above, checked against the real parser: with every key file
    absent except one, sshd must still accept the config. (`sshd -t` reports
    unloadable keys but only fails when none load.)"""
    conf = _file(_certified(), "/etc/ssh/sshd_config.d/60-whistler.conf")["content"]
    # Point the whole set at a directory holding exactly one real key, the way
    # a guest whose Whistler key never landed would look.
    real = tmp_path / "ssh_host_ed25519_key"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(real)],
                   check=True)
    conf = conf.replace("/etc/ssh/", f"{tmp_path}/")
    # Drop the certificate line: the cert file is absent in this scenario too,
    # and a HostCertificate without its key is a separate (non-fatal) warning.
    conf = "\n".join(l for l in conf.splitlines()
                     if not l.startswith(("HostCertificate", "AuthorizedKeysFile")))
    cfg = tmp_path / "sshd_config"
    cfg.write_text(conf + "\n")
    result = subprocess.run(["/usr/sbin/sshd", "-t", "-f", str(cfg)],
                            capture_output=True, text=True)
    assert "no hostkeys available" not in result.stderr, result.stderr


def test_sshd_reloaded_in_case_it_started_first():
    # Normally redundant (write_files lands in the init stage), but a guest
    # whose sshd raced ahead would otherwise serve its uncertified key until
    # the next boot. Reload, not restart: established connections survive.
    runcmd = " ".join(_certified()["runcmd"])
    assert "try-reload-or-restart ssh" in runcmd


def test_no_certificate_bits_without_one():
    """A cluster with no CA yet boots exactly as before — an uncertified guest
    is the pre-CA status quo, not a broken one."""
    doc = _doc()
    paths = [f["path"] for f in doc["write_files"]]
    assert not any("whistler_ed25519" in p for p in paths)
    conf = _file(doc, "/etc/ssh/sshd_config.d/60-whistler.conf")["content"]
    assert "HostCertificate" not in conf
    assert "try-reload-or-restart" not in " ".join(doc["runcmd"])


def test_half_a_certificate_is_ignored():
    # Both or neither: a HostCertificate line pointing at a file that was
    # never written stops sshd from starting at all.
    for kwargs in ({"host_key": HOST_KEY}, {"host_cert": HOST_CERT}):
        conf = _file(_doc(**kwargs),
                     "/etc/ssh/sshd_config.d/60-whistler.conf")["content"]
        assert "HostCertificate" not in conf


# --- desktop mode (viewer=websockets VM images, e.g. vm-xfce-selkies) ----- #

STREAMER_ENV = "/etc/whistler/streamer.env"


def test_desktop_enables_per_user_session_unit():
    # The image bakes whistler-desktop@.service but can't know the username;
    # cloud-init enables it (enable --now, so CDI persistent-root guests
    # restart the desktop on later boots too).
    doc = _doc(desktop=True)
    assert "systemctl enable --now whistler-desktop@alice.service" \
        in doc["runcmd"]


def test_desktop_streamer_env_written_and_streamer_kicked():
    doc = _doc(desktop=True, display_port=9000,
               streamer_env={"SELKIES_H264_STREAMING_MODE": "true"})
    env = next(f for f in doc["write_files"] if f["path"] == STREAMER_ENV)
    lines = env["content"].splitlines()
    assert "SELKIES_H264_STREAMING_MODE=true" in lines
    # SELKIES_PORT comes last: displayPort (what the Service/portal dial)
    # must beat any streamerEnv override or the viewer can't connect.
    assert lines[-1] == "SELKIES_PORT=9000"
    # The baked streamer may already be up when runcmd runs; kick it so it
    # rereads the env file.
    assert "systemctl try-restart whistler-streamer.service" in doc["runcmd"]


def test_desktop_without_env_skips_env_file_and_kick():
    doc = _doc(desktop=True)
    assert not any(f["path"] == STREAMER_ENV for f in doc["write_files"])
    assert not any("whistler-streamer" in c for c in doc["runcmd"])


def test_non_desktop_has_no_desktop_bits():
    doc = _doc()
    assert not any(f["path"] == STREAMER_ENV for f in doc["write_files"])
    assert not any("whistler-desktop@" in c for c in doc["runcmd"])
    assert not any("whistler-streamer" in c for c in doc["runcmd"])


# --- resolve_uid fallback chain ------------------------------------------ #

def test_resolve_uid_explicit_field_wins():
    assert resolve_uid({"uid": 42, "securityContext": {"runAsUser": 7}}) == 42


def test_resolve_uid_falls_back_to_run_as_user():
    assert resolve_uid({"securityContext": {"runAsUser": 1234}}) == 1234


def test_resolve_uid_defaults_to_1000():
    # get_user() returns a bare {"name": u} record for unknown users.
    assert resolve_uid({"name": "ghost"}) == 1000
    assert resolve_uid(None) == 1000
    assert resolve_uid({"securityContext": {}}) == 1000


# --- resolve_gid fallback chain ------------------------------------------- #

def test_resolve_gid_explicit_field_wins():
    assert resolve_gid({"gid": 42, "uid": 7, "securityContext": {"runAsGroup": 9}}) == 42


def test_resolve_gid_falls_back_to_run_as_group():
    assert resolve_gid({"uid": 7, "securityContext": {"runAsGroup": 9}}) == 9


def test_resolve_gid_falls_back_to_resolved_uid():
    assert resolve_gid({"uid": 1234}) == 1234
    assert resolve_gid({"name": "ghost"}) == 1000
    assert resolve_gid(None) == 1000


# --- gid becomes the guest's real primary group --------------------------- #
# NFS passes numeric owners through untranslated (the export sets
# Only_Numeric_Owners), so there is no client-side gid= remapping left to
# paper over a mismatch: the guest group must genuinely hold the PVC's gid.

def test_primary_group_created_before_the_user():
    # cloud-init's bootcmd runs before its users-groups module, which is what
    # lets `primary_group` reference a gid useradd would otherwise invent.
    doc = _doc(uid=1001, gid=2001)
    groupadd, _kick = doc["bootcmd"]
    assert "getent group 2001" in groupadd
    assert "groupadd -g 2001 alice" in groupadd
    (user,) = doc["users"]
    assert user["uid"] == "1001"
    assert user["primary_group"] == "2001"


def test_primary_group_defaults_to_uid_when_gid_omitted():
    doc = _doc(uid=1001)
    (user,) = doc["users"]
    assert user["primary_group"] == "1001"
    assert "groupadd -g 1001 alice" in doc["bootcmd"][0]


# --------------------------------------------------------------------------- #
# Directory delegations                                                        #

