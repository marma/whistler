"""cloud-init userData builder (whistler.cloudinit) — pure, no cluster."""
import yaml

from whistler.cloudinit import build_user_data, resolve_uid, SMB_CREDENTIALS_PATH

SMB_HOST = "whistler-storage-alice.whistler-user-alice.svc.cluster.local"


def _doc(**overrides):
    args = dict(username="alice", uid=1001,
                ssh_keys=["ssh-ed25519 AAA alice"], hostname="desk",
                smb_host=SMB_HOST, smb_password="s3cret")
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


def test_home_mounted_via_cifs_from_gateway():
    (mount,) = _doc()["mounts"]
    fs_spec, fs_file, fs_vfstype, fs_mntops, freq, passno = mount
    assert fs_spec == f"//{SMB_HOST}/home"
    assert fs_file == "/home/alice"
    assert fs_vfstype == "cifs"
    assert (freq, passno) == ("0", "0")
    opts = fs_mntops.split(",")
    # Server-side identity: uid=/gid=/*_mode shape the in-guest view only.
    assert f"credentials={SMB_CREDENTIALS_PATH}" in opts
    assert "vers=3.1.1" in opts
    assert "seal" in opts
    assert "uid=1001" in opts and "gid=1001" in opts
    assert "file_mode=0664" in opts and "dir_mode=0775" in opts
    assert "nosuid" in opts and "nodev" in opts
    # hard (not soft): a gateway blip blocks I/O instead of corrupting it;
    # nofail + _netdev keep first boot alive while cifs-utils installs.
    assert "hard" in opts
    assert "nofail" in opts and "_netdev" in opts


def test_smb_credentials_root_only_on_root_disk():
    doc = _doc()
    creds = next(f for f in doc["write_files"]
                 if f["path"] == SMB_CREDENTIALS_PATH)
    assert creds["permissions"] == "0600"
    assert "username=alice" in creds["content"]
    assert "password=s3cret" in creds["content"]


def test_no_package_install_and_mount_unit_armed():
    # NO packages: [cifs-utils] — with the default locked-down egress apt
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
    (kick,) = _doc()["bootcmd"]
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
    # Prefers the fstab/mount.cifs path...
    assert "command -v mount.cifs" in body
    # ...and falls back to a raw kernel mount when cifs-utils is absent
    # (locked-down egress: no package mirrors). The kernel can't resolve
    # DNS or parse credentials=, so the script does both.
    assert f"HOST={SMB_HOST}" in body
    assert "getent hosts" in body
    assert f". {SMB_CREDENTIALS_PATH}" in body
    assert "ip=$ip,username=$username,password=$password" in body
    assert "vers=3.1.1,seal" in body
    # The fallback options must not carry the fstab-isms.
    assert "nofail" not in body and "_netdev" not in body and \
        "credentials=" not in body.replace(f". {SMB_CREDENTIALS_PATH}", "")


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
