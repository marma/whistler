"""cloud-init userData builder for KubeVirt VM sessions.

Pure (no Kubernetes imports) so it is unit-testable without a cluster, like
`KubeConfigManager._build_pod_spec`. The generated ``#cloud-config`` document
creates the Whistler user with their real username/uid inside the guest and
mounts their home from the per-user NFS storage gateway
(``whistler-storage-<user>``, see images/storage-gateway/) over NFSv4.2.

Why a gateway and not virtiofs: KubeVirt (<= 1.8) runs virtiofsd unprivileged
(hardcoded uid 107, --sandbox=none, caps dropped — upstream kubevirt#13028),
which makes a virtiofs-shared home read-only for the guest user. The gateway
gives server-side identity instead: client uids are never trusted, and its
``Squash = All_Squash`` to the user's real uid/gid lands every write on the
PVC as that uid (the successor to Samba's ``force user``).

Mount mechanics: a systemd oneshot (whistler-home.service, written by
write_files, kicked off by runcmd and enabled for later boots) retries the
mount. It prefers mount.nfs4 (the fstab entry) when the image ships
nfs-common, and otherwise does a raw kernel mount: the kernel's NFSv4 text
mount interface cannot resolve DNS, so the script resolves the gateway
itself (getent; DNS egress is open) and passes the result as ``addr=``.
Both paths verified against the real gateway image.
Deliberately NO ``packages: [nfs-common]``: the default user-namespace
egress is DNS + gateway-NFS only, so apt just burns ~50s timing out on
unreachable mirrors — and the packages module runs *before* runcmd, which
delayed the mount by that much past the login prompt (measured live). The
raw kernel path needs no userspace helper. A unit rather than a bare runcmd
mount so persistent-root (CDI) guests remount on every boot — runcmd is
once-per-instance.

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
— harmless; ownership after the mount is whatever the PVC says, which the
gateway's entrypoint has already claimed for the user.

Console access is via serial-getty autologin rather than a generated
password: the portal's auth + RBAC already gate who can reach the console
websocket, the same trust model as the kubectl-exec web terminal for pods.
(The alternative — chpasswd with a per-session password stored in a Secret —
was considered and deferred; it adds a Secret lifecycle and UI surface for no
additional isolation.)
"""

import yaml

from .hostca import (DEFAULT_HOST_KEY_PATHS, GUEST_HOST_CERT_PATH,
                     GUEST_HOST_KEY_PATH)

STREAMER_ENV_PATH = "/etc/whistler/streamer.env"
# The gateway's Ganesha pseudo-path (images/storage-gateway/ganesha.conf.template).
NFS_EXPORT = "/home"


def build_user_data(*, username: str, uid: int, ssh_keys: list,
                    hostname: str, nfs_host: str,
                    gid: int = None,
                    autologin: bool = True, desktop: bool = False,
                    streamer_env: dict = None,
                    display_port: int = None,
                    host_key: bytes = None, host_cert: str = None) -> str:
    """Return a ``#cloud-config`` userData document for cloudInitNoCloud.

    ``nfs_host`` is the storage gateway Service DNS name. There is no
    credential to pass: NFS AUTH_SYS has none, and reaching the export is the
    whole permission (fenced by NetworkPolicy — see design/security.md).

    ``host_key``/``host_cert`` install a Whistler-CA-signed host certificate
    (see hostca.py) so clients verify the guest against one
    ``@cert-authority`` line instead of a per-instance TOFU prompt. Both or
    neither; the image's own host key is left in place either way, so a
    client that has not adopted the CA still connects as before.

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
    gid = uid if gid is None else gid
    # Mount options. Unlike cifs there is no client-side uid=/gid= remapping:
    # NFSv4 with AUTH_SYS and numeric owners (the gateway sets
    # Only_Numeric_Owners) passes the PVC's real ids straight through, and the
    # guest user is created with exactly those ids below — so the view in the
    # guest and the ownership on disk are the same thing, not two things kept
    # in sync. What lands on the PVC is still decided server-side by the
    # export's Squash = All_Squash, whatever uid the (root) guest asserts.
    #
    # Nothing here corresponds to the old `posix` / `mfsymlinks` / `nobrl`
    # trio: NFS carries per-file modes, real symlinks and byte-range locks
    # natively. The last of those is why this is NFS at all — cifs answered
    # SQLite's past-EOF F_WRLCK with EACCES, costing a flat 100s stall and
    # then "database is locked" for nautilus, dconf, gnome-keyring and
    # Chrome's profile; the same lock over NFSv4 is granted immediately.
    #
    # vers=4.2 pins the dialect (the gateway allows 4.1+ only: NFSv4.0 puts
    # callbacks on a server->client connection its ingress fencing can never
    # permit). hard (not soft) so a gateway blip blocks I/O instead of
    # corrupting it; nofail + _netdev so boot survives an absent gateway.
    #
    # `noauto` is load-bearing for boot latency. cloud-init's `mounts` module
    # runs `mount -a` synchronously in the init stage, and a gateway that
    # silently drops packets (rather than refusing the connection) costs one
    # TCP connect budget there — measured at 181s against a black hole, well
    # past the login prompt. noauto keeps `mount -a` and systemd's fstab
    # generator off this entry entirely, leaving whistler-home.service as the
    # only mounter; it runs detached, so a slow attempt delays the home and
    # nothing else. The entry still exists because the script mounts by
    # mountpoint (`mount $HOME_DIR`) and reads its options from here.
    #
    # retry=0 then bounds each of those attempts to a single try instead of
    # mount.nfs retrying internally for two minutes on top. It is a mount.nfs
    # option, so it is deliberately absent from the raw kernel options below,
    # which the kernel would reject.
    base_opts = "vers=4.2,hard,nosuid,nodev"
    mount_opts = f"{base_opts},retry=0,noauto,nofail,_netdev"
    # Fallback mounter (see module docstring): raw kernel mount for guests
    # without nfs-common. The kernel's NFSv4 text mount interface cannot
    # resolve DNS, so the script resolves the gateway and passes addr=.
    # After a successful mount the autologin getty is respawned: a console
    # shell spawned in the pre-mount window keeps the now-shadowed directory
    # as its cwd.
    getty_respawn = (
        "systemctl try-restart serial-getty@ttyS0.service\n" if autologin
        else "")
    mount_script = f"""#!/bin/sh
# Written by Whistler cloud-init; run by whistler-home.service.
set -u
HOST={nfs_host}
HOME_DIR={home}
mountpoint -q "$HOME_DIR" && exit 0
# Own the mountpoint rather than waiting for cloud-init's mounts module to
# make it: bootcmd kicks this script off well before that module runs. Left
# root-owned on purpose — see the module docstring, it is the "home not
# ready" signal.
mkdir -p "$HOME_DIR"
mount_once() {{
    if command -v mount.nfs4 >/dev/null 2>&1; then
        mount "$HOME_DIR"
    else
        modprobe nfsv4 2>/dev/null || true
        ip="$(getent hosts "$HOST" | cut -d' ' -f1)"
        [ -n "$ip" ] || return 1
        mount -t nfs4 "$HOST:{NFS_EXPORT}" "$HOME_DIR" \\
            -o "addr=$ip,{base_opts}"
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
    sshd_conf = (
        "AuthorizedKeysFile .ssh/authorized_keys "
        "/etc/ssh/authorized_keys.d/%u\n"
    )
    if host_key and host_cert:
        # The image's own host keys have to be listed explicitly alongside
        # ours. A HostKey directive REPLACES sshd's built-in defaults rather
        # than adding to them (fill_default_server_options only supplies
        # defaults when num_host_key_files == 0), so naming only the Whistler
        # key would make sshd depend entirely on a file cloud-init writes —
        # and if it were missing or written late, sshd exits outright:
        #
        #     Unable to load host key: /etc/ssh/ssh_host_whistler_ed25519_key
        #     sshd: no hostkeys available -- exiting.
        #
        # which is a guest with no SSH at all, not a guest without a
        # certificate. Listing all of them makes a missing file a warning
        # (sshd only refuses to start when *none* load), so the certificate is
        # an upgrade and never a way to lose the service. Verified against a
        # real sshd both ways.
        for path in DEFAULT_HOST_KEY_PATHS:
            sshd_conf += f"HostKey {path}\n"
        sshd_conf += (
            f"HostKey {GUEST_HOST_KEY_PATH}\n"
            f"HostCertificate {GUEST_HOST_CERT_PATH}\n"
        )

    write_files = [
        {
            "path": f"/etc/ssh/authorized_keys.d/{username}",
            "permissions": "0644",
            "content": "\n".join(keys) + "\n" if keys else "",
        },
        {
            "path": "/etc/ssh/sshd_config.d/60-whistler.conf",
            "content": sshd_conf,
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
    if host_key and host_cert:
        key_pem = host_key.decode() if isinstance(host_key, bytes) else host_key
        write_files.extend([
            {
                "path": GUEST_HOST_KEY_PATH,
                "permissions": "0600",
                "owner": "root:root",
                "content": key_pem if key_pem.endswith("\n") else key_pem + "\n",
            },
            {
                "path": GUEST_HOST_CERT_PATH,
                "permissions": "0644",
                "owner": "root:root",
                "content": host_cert.strip() + "\n",
            },
        ])
    doc = {
        "hostname": hostname,
        # Early mount kick (see module docstring): bootcmd runs in the init
        # stage BEFORE write_files, so poll for the script and run it as
        # soon as it lands — detached (setsid, fds closed) so cloud-init is
        # never blocked. Waiting for runcmd instead would sit behind
        # snapd.seeded/multi-user, ~30s past the login prompt.
        "bootcmd": [
            # The guest's primary group must really BE the PVC's gid: NFS
            # passes numeric owners through untranslated, so unlike the old
            # cifs gid= there is nothing remapping the view. useradd would
            # otherwise invent a user-private group at whatever gid happens
            # to be free. bootcmd runs before cloud-init's users-groups
            # module (and on every boot, so it is idempotent by construction),
            # which is what lets `primary_group` below reference this gid.
            f"getent group {gid} >/dev/null || groupadd -g {gid} {username}"
            " || true",
            "setsid sh -c 'i=0; while [ ! -x /usr/local/sbin/whistler-mount-home ]"
            " && [ $i -lt 120 ]; do i=$((i+1)); sleep 1; done;"
            " exec /usr/local/sbin/whistler-mount-home'"
            " </dev/null >/dev/null 2>&1 &",
        ],
        # [fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno].
        "mounts": [
            [f"{nfs_host}:{NFS_EXPORT}", home, "nfs4", mount_opts, "0", "0"],
        ],
        # No `default` entry: this suppresses the image's built-in user
        # (uid 1000 in containerdisks images), freeing that uid for ours.
        "users": [
            {
                "name": username,
                "uid": str(uid),
                # Numeric: useradd --gid takes a gid, and bootcmd has just
                # guaranteed a group holds it.
                "primary_group": str(gid),
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
    if host_key and host_cert:
        # Normally redundant — write_files lands in the init stage, well
        # before sshd starts — but a guest whose sshd came up first would
        # otherwise serve its uncertified key until the next boot.
        # try-reload-or-restart: SIGHUP where the unit supports it (existing
        # connections survive sshd re-exec'ing), no-op when sshd isn't
        # running, and the name differs across distros.
        doc["runcmd"].append(
            "systemctl try-reload-or-restart ssh 2>/dev/null"
            " || systemctl try-reload-or-restart sshd || true")
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


def resolve_gid(user_details) -> int:
    """POSIX gid the home is owned by: explicit ``gid`` field, else the pod
    securityContext's runAsGroup, else the resolved uid (single-user-group
    convention, matching the guest's own user-private group). It is both the
    export's Anonymous_Gid and the guest user's primary group — with numeric
    NFSv4 owners those are the same number by construction."""
    user_details = user_details or {}
    gid = user_details.get("gid")
    if gid is None:
        gid = (user_details.get("securityContext") or {}).get("runAsGroup")
    if gid is None:
        gid = resolve_uid(user_details)
    return int(gid)
