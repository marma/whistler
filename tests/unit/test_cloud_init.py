"""cloud-init userData builder (whistler.cloudinit) — pure, no cluster."""
import os
import subprocess

import pytest
import yaml

from whistler.cloudinit import (
    build_user_data, resolve_uid, resolve_gid, NFS_EXPORT,
)

NFS_HOST = "whistler-storage-alice.whistler-user-alice.svc.cluster.local"


def _doc(**overrides):
    args = dict(username="alice", uid=1001,
                ssh_keys=["ssh-ed25519 AAA alice"], hostname="desk",
                nfs_host=NFS_HOST)
    args.update(overrides)
    text = build_user_data(**args)
    assert text.startswith("#cloud-config\n")
    return yaml.safe_load(text.split("\n", 1)[1])


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


def test_home_mounted_via_nfs_from_gateway():
    (mount,) = _doc()["mounts"]
    fs_spec, fs_file, fs_vfstype, fs_mntops, freq, passno = mount
    assert fs_spec == f"{NFS_HOST}:{NFS_EXPORT}"
    assert fs_file == "/home/alice"
    assert fs_vfstype == "nfs4"
    assert (freq, passno) == ("0", "0")
    opts = fs_mntops.split(",")
    # 4.2 pinned: the gateway refuses 4.0, whose callbacks would need a
    # server->client connection its ingress fencing can't allow.
    assert "vers=4.2" in opts
    assert "nosuid" in opts and "nodev" in opts
    # hard (not soft): a gateway blip blocks I/O instead of corrupting it;
    # nofail + _netdev keep first boot alive when the gateway is absent.
    assert "hard" in opts
    assert "nofail" in opts and "_netdev" in opts
    # noauto: cloud-init's mounts module runs `mount -a` synchronously in the
    # init stage, where one TCP connect budget against a black-holed gateway
    # measured 181s — past the login prompt. Mounting is whistler-home's job,
    # off the boot path; this entry exists only to hold the options.
    assert "noauto" in opts
    # retry=0 then bounds each of those attempts to one try.
    assert "retry=0" in opts
    # No client-side identity remapping and no cifs workarounds: NFS carries
    # real owners, modes, symlinks and byte-range locks.
    for gone in ("uid=", "gid=", "posix", "mfsymlinks", "nobrl", "seal",
                 "credentials="):
        assert gone not in fs_mntops


def test_no_credentials_file_written():
    # AUTH_SYS has no per-share secret to leak into the guest; the boundary
    # is the gateway's fencing NetworkPolicy.
    doc = _doc()
    assert not any("credential" in f["path"] for f in doc["write_files"])


def test_no_package_install_and_mount_unit_armed():
    # NO packages: [nfs-common] — with the default locked-down egress apt
    # burns ~50s timing out on unreachable mirrors, and the packages module
    # runs before runcmd, delaying the mount that long past the login
    # prompt. The raw kernel mount needs no helper. The unit is started
    # non-blocking — its retry loop must not stall the rest of first boot —
    # and enabled so persistent-root (CDI) guests remount on later boots.
    doc = _doc()
    assert "packages" not in doc
    cmds = doc["runcmd"]
    assert "systemctl enable whistler-home.service" in cmds
    assert cmds[-1] == "systemctl start --no-block whistler-home.service"


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


def test_mount_unit_and_fallback_script_written():
    doc = _doc()
    unit = next(f for f in doc["write_files"]
                if f["path"] == "/etc/systemd/system/whistler-home.service")
    assert "ExecStart=/usr/local/sbin/whistler-mount-home" in unit["content"]
    assert "WantedBy=multi-user.target" in unit["content"]

    script = next(f for f in doc["write_files"]
                  if f["path"] == "/usr/local/sbin/whistler-mount-home")
    assert script["permissions"] == "0755"
    body = script["content"]
    # Prefers the fstab/mount.nfs4 path...
    assert "command -v mount.nfs4" in body
    # ...and falls back to a raw kernel mount when nfs-common is absent
    # (locked-down egress: no package mirrors). The kernel's NFSv4 text mount
    # interface can't resolve DNS, so the script resolves and passes addr=.
    assert f"HOST={NFS_HOST}" in body
    assert "getent hosts" in body
    assert f'mount -t nfs4 "$HOST:{NFS_EXPORT}"' in body
    assert "-o \"addr=$ip,vers=4.2,hard,nosuid,nodev\"" in body
    # The fallback options must not carry mount.nfs-only or fstab-only ones:
    # the kernel rejects options it doesn't know.
    assert "retry=0" not in body
    assert "nofail" not in body and "_netdev" not in body and \
        "noauto" not in body
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
# --------------------------------------------------------------------------- #

def test_directory_delegations_are_disabled_before_mounting():
    """Linux 6.19+ clients (Ubuntu 26.04 ships 7.0) ask for NFSv4.1 directory
    delegations; nfs-ganesha answers NFS4ERR_OP_ILLEGAL, which fails the whole
    compound and hands the application EREMOTEIO on any freshly created
    directory. Turn the feature off client-side, before the mount."""
    files = {f["path"]: f for f in _doc()["write_files"]}
    script = files["/usr/local/sbin/whistler-mount-home"]["content"]
    # The knob is /sys/module/nfsv4/parameters/directory_delegations
    # (verified on kernel 7.0); nfs/ and the alternative spelling are tried
    # too, since the posted patch and the merged code disagree on the name.
    assert "directory_delegations" in script
    assert "nfsv4" in script
    # Modules have to be loaded for the sysfs files to exist at all.
    assert script.index("modprobe nfs") < script.index("mount_once")


def test_delegations_are_not_disabled_via_modprobe_d():
    """NEVER `options nfs <name>=0`: if the parameter does not exist on this
    kernel the module fails to load and the guest loses its home entirely —
    worse than the bug being fixed."""
    doc = _doc()
    paths = [f["path"] for f in doc["write_files"]]
    assert not any("modprobe.d" in p for p in paths)


def test_only_the_directory_delegation_knob_is_touched():
    """That sysfs directory also holds delegation_watermark, a FILE-delegation
    tuning value. A *deleg* glob would zero it — match exact names only."""
    files = {f["path"]: f for f in _doc()["write_files"]}
    script = files["/usr/local/sbin/whistler-mount-home"]["content"]
    # Comments may name it (they explain why it is avoided); the CODE may not.
    code = [l for l in script.splitlines() if not l.lstrip().startswith("#")]
    assert not any("watermark" in l for l in code)
    assert not any("*deleg*" in l for l in code)          # no glob write
    assert any("directory_delegations" in l for l in code)


def test_disabling_delegations_cannot_fail_the_mount():
    """A kernel without the feature has no such file; the script must carry on."""
    files = {f["path"]: f for f in _doc()["write_files"]}
    script = files["/usr/local/sbin/whistler-mount-home"]["content"]
    head = script[:script.index("mount_once")]
    assert "[ -w " in head          # guarded write
    assert "2>/dev/null" in head
