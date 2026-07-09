"""cloud-init userData builder for KubeVirt VM sessions.

Pure (no Kubernetes imports) so it is unit-testable without a cluster, like
`KubeConfigManager._build_pod_spec`. The generated ``#cloud-config`` document
creates the Whistler user with their real username/uid inside the guest and
mounts their home from the per-user SMB storage gateway
(``whistler-storage-<user>``, see images/storage-gateway/) over cifs.

Why SMB and not virtiofs: KubeVirt (<= 1.8) runs virtiofsd unprivileged
(hardcoded uid 107, --sandbox=none, caps dropped — upstream kubevirt#13028),
which makes a virtiofs-shared home read-only for the guest user. The gateway
gives server-side identity instead: client uids are never trusted, and its
``force user`` lands every write on the PVC as the user's real uid.

Mount mechanics: a systemd oneshot (whistler-home.service, written by
write_files, kicked off by runcmd and enabled for later boots) retries the
mount. It prefers mount.cifs (the fstab entry with ``credentials=``) when
the image ships cifs-utils, and otherwise does a raw kernel-cifs mount:
it resolves the gateway itself (getent; DNS egress is open — the kernel
can't resolve) and passes username=/password= from the credentials file
(``credentials=`` is parsed by mount.cifs only, never by the kernel).
Deliberately NO ``packages: [cifs-utils]``: the default user-namespace
egress is DNS + gateway-SMB only, so apt just burns ~50s timing out on
unreachable mirrors — and the packages module runs *before* runcmd, which
delayed the mount by that much past the login prompt (measured live). The
raw kernel path needs no userspace helper and is the verified primary. A
unit rather than a bare runcmd mount so persistent-root (CDI) guests
remount on every boot — runcmd is once-per-instance.

Mount latency: runcmd alone is too late a trigger — cloud-final waits for
multi-user.target, which snapd.seeded holds up for ~30s on stock Ubuntu
images (measured), all after the login prompt. So a detached ``bootcmd``
poller kicks the mount script the moment write_files lands it (seconds
into boot, typically before the first getty); the runcmd start remains as
the systemd-visible belt-and-braces.

The pre-mount window (getty/sshd up seconds before the mount lands): the
mountpoint stays root-owned until the share arrives — deliberately, as an
honest "home not ready" signal; anything written there would be shadowed
by the mount. The mount script respawns the autologin getty once the share
lands so a console shell opened early doesn't keep the shadowed directory
as its cwd. ``useradd -m`` neither chowns nor skels the existing mountpoint
— harmless; in-guest ownership after the mount comes from the uid=/gid=
mount options.

Console access is via serial-getty autologin rather than a generated
password: the portal's auth + RBAC already gate who can reach the console
websocket, the same trust model as the kubectl-exec web terminal for pods.
(The alternative — chpasswd with a per-session password stored in a Secret —
was considered and deferred; it adds a Secret lifecycle and UI surface for no
additional isolation.)
"""

import yaml

SMB_CREDENTIALS_PATH = "/etc/whistler/smb-credentials"
STREAMER_ENV_PATH = "/etc/whistler/streamer.env"


def build_user_data(*, username: str, uid: int, ssh_keys: list,
                    hostname: str, smb_host: str, smb_password: str,
                    autologin: bool = True, desktop: bool = False,
                    streamer_env: dict = None,
                    display_port: int = None) -> str:
    """Return a ``#cloud-config`` userData document for cloudInitNoCloud.

    ``smb_host`` is the storage gateway Service DNS name;
    ``smb_password`` the user's password from Secret whistler-smb-<user>.

    ``desktop=True`` targets a Whistler desktop-VM image (viewer:
    websockets — e.g. desktops/vm-xfce-selkies): the image ships the
    Selkies streamer baked in and always-on, but the DE session unit is the
    per-user template ``whistler-desktop@<user>.service`` — only cloud-init
    knows the username, so it enables the unit here. ``streamer_env`` (the
    template's streamerEnv) and ``display_port`` land in
    ``/etc/whistler/streamer.env``, the EnvironmentFile of the baked
    whistler-streamer.service — the VM analog of the sidecar's env
    injection. SELKIES_PORT is written last so displayPort (which the
    per-session Service and the portal proxy are built from) always wins
    over a stray streamerEnv override — they must agree or the viewer
    can't reach the streamer.
    """
    home = f"/home/{username}"
    keys = list(ssh_keys or [])
    # Mount options: server-side identity means the client-side uid=/gid=/
    # *_mode only shape the in-guest VIEW (what lands on the PVC is decided
    # by the gateway's force user + masks). vers=3.1.1 + seal to match the
    # gateway's `server min protocol` / `server smb encrypt = required`;
    # hard (not soft) so a gateway blip blocks I/O instead of corrupting it;
    # nofail + _netdev so boot survives the gateway being unreachable.
    base_opts = (
        f"vers=3.1.1,seal,uid={uid},gid={uid},"
        "file_mode=0664,dir_mode=0775,nosuid,nodev,hard"
    )
    mount_opts = (
        f"credentials={SMB_CREDENTIALS_PATH},{base_opts},nofail,_netdev"
    )
    # Fallback mounter (see module docstring): raw kernel-cifs mount for
    # guests without cifs-utils. The credentials file's username=/password=
    # lines are deliberately valid shell, so the same file both feeds
    # mount.cifs (credentials=) and is sourced here. After a successful
    # mount the autologin getty is respawned: a console shell spawned in
    # the pre-mount window keeps the now-shadowed directory as its cwd.
    getty_respawn = (
        "systemctl try-restart serial-getty@ttyS0.service\n" if autologin
        else "")
    mount_script = f"""#!/bin/sh
# Written by Whistler cloud-init; run by whistler-home.service.
set -u
HOST={smb_host}
HOME_DIR={home}
mountpoint -q "$HOME_DIR" && exit 0
mount_once() {{
    if command -v mount.cifs >/dev/null 2>&1; then
        mount "$HOME_DIR"
    else
        modprobe cifs 2>/dev/null || true
        ip="$(getent hosts "$HOST" | cut -d' ' -f1)"
        [ -n "$ip" ] || return 1
        . {SMB_CREDENTIALS_PATH}
        mount -t cifs "//$HOST/home" "$HOME_DIR" \\
            -o "ip=$ip,username=$username,password=$password,{base_opts}"
    fi
}}
i=0
while [ "$i" -lt 30 ]; do
    if mount_once; then
        {getty_respawn.strip() or ":"}
        exit 0
    fi
    i=$((i + 1))
    sleep 5
done
echo "whistler: could not mount $HOME_DIR from $HOST" >&2
exit 1
"""
    # Authorized keys live on the ROOT disk (/etc/ssh/authorized_keys.d), not
    # in ~/.ssh: the home is a network share that is not mounted for the
    # whole of first boot (see module docstring), and a root-owned path on
    # the root disk is StrictModes-clean and not writable through the share.
    # The drop-in keeps ~/.ssh/authorized_keys as a secondary source.
    write_files = [
        {
            "path": f"/etc/ssh/authorized_keys.d/{username}",
            "permissions": "0644",
            "content": "\n".join(keys) + "\n" if keys else "",
        },
        {
            "path": "/etc/ssh/sshd_config.d/60-whistler.conf",
            "content": (
                "AuthorizedKeysFile .ssh/authorized_keys "
                "/etc/ssh/authorized_keys.d/%u\n"
            ),
        },
        # Root-only cifs credentials; referenced from the fstab entry so the
        # password never appears in the mount table or process lists.
        {
            "path": SMB_CREDENTIALS_PATH,
            "permissions": "0600",
            "content": f"username={username}\npassword={smb_password}\n",
        },
        {
            "path": "/usr/local/sbin/whistler-mount-home",
            "permissions": "0755",
            "content": mount_script,
        },
        {
            "path": "/etc/systemd/system/whistler-home.service",
            "content": (
                "[Unit]\n"
                "Description=Mount the Whistler home share\n"
                "Wants=network-online.target\n"
                "After=network-online.target\n"
                "\n"
                "[Service]\n"
                "Type=oneshot\n"
                "RemainAfterExit=yes\n"
                "ExecStart=/usr/local/sbin/whistler-mount-home\n"
                "\n"
                "[Install]\n"
                "WantedBy=multi-user.target\n"
            ),
        },
    ]
    doc = {
        "hostname": hostname,
        # Early mount kick (see module docstring): bootcmd runs in the init
        # stage BEFORE write_files, so poll for the script and run it as
        # soon as it lands — detached (setsid, fds closed) so cloud-init is
        # never blocked. Waiting for runcmd instead would sit behind
        # snapd.seeded/multi-user, ~30s past the login prompt.
        "bootcmd": [
            "setsid sh -c 'i=0; while [ ! -x /usr/local/sbin/whistler-mount-home ]"
            " && [ $i -lt 120 ]; do i=$((i+1)); sleep 1; done;"
            " exec /usr/local/sbin/whistler-mount-home'"
            " </dev/null >/dev/null 2>&1 &",
        ],
        # [fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno].
        "mounts": [
            [f"//{smb_host}/home", home, "cifs", mount_opts, "0", "0"],
        ],
        # No `default` entry: this suppresses the image's built-in user
        # (uid 1000 in containerdisks images), freeing that uid for ours.
        "users": [
            {
                "name": username,
                "uid": str(uid),
                "shell": "/bin/bash",
                "sudo": "ALL=(ALL) NOPASSWD:ALL",
                "groups": ["sudo"],
                "lock_passwd": True,
                "ssh_authorized_keys": keys,
            }
        ],
        "write_files": write_files,
        # Final stage: arm the mount unit (enable makes CDI persistent-root
        # guests remount on later boots) and kick it off now, non-blocking —
        # its retry loop must not stall the rest of first boot.
        "runcmd": [
            "systemctl daemon-reload",
            "systemctl enable whistler-home.service",
            "systemctl start --no-block whistler-home.service",
        ],
    }
    if desktop:
        env_lines = [f"{k}={v}" for k, v in (streamer_env or {}).items()]
        if display_port:
            env_lines.append(f"SELKIES_PORT={display_port}")
        if env_lines:
            write_files.append({
                "path": STREAMER_ENV_PATH,
                "content": "\n".join(env_lines) + "\n",
            })
            # write_files lands in cloud-init's init stage, normally before
            # multi-user services start — but nothing guarantees that on a
            # slow boot, so kick the (Restart=always) streamer to be sure it
            # runs with this env.
            doc["runcmd"].append(
                "systemctl try-restart whistler-streamer.service")
        # enable + start (not just start): persistent-root (CDI) guests must
        # bring the desktop back on later boots, where runcmd doesn't re-run.
        doc["runcmd"].append(
            f"systemctl enable --now whistler-desktop@{username}.service")
    if autologin:
        write_files.append(
            {
                "path": "/etc/systemd/system/serial-getty@ttyS0.service.d/autologin.conf",
                "content": (
                    "[Service]\n"
                    "ExecStart=\n"
                    f"ExecStart=-/sbin/agetty --autologin {username} "
                    "--keep-baud 115200,57600,38400,9600 %I $TERM\n"
                ),
            }
        )
        # After the base runcmd's daemon-reload, which covers this drop-in too.
        doc["runcmd"].insert(
            1, "systemctl try-restart serial-getty@ttyS0.service")
    return "#cloud-config\n" + yaml.safe_dump(doc, sort_keys=False)


def resolve_uid(user_details) -> int:
    """POSIX uid for a user record: explicit ``uid`` field, else the pod
    securityContext's runAsUser, else 1000. Tolerates the bare
    ``{"name": ...}`` record get_user() returns for unknown users."""
    user_details = user_details or {}
    uid = user_details.get("uid")
    if uid is None:
        uid = (user_details.get("securityContext") or {}).get("runAsUser")
    if uid is None:
        uid = 1000
    return int(uid)
