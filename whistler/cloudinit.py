"""cloud-init userData builder for KubeVirt VM sessions.

Pure (no Kubernetes imports) so it is unit-testable without a cluster, like
`KubeConfigManager._build_pod_spec`. The generated ``#cloud-config`` document
creates the Whistler user with their real username/uid inside the guest and
mounts their home from a **per-instance virtio-blk disk** — an ordinary PVC
carrying a ``disk.img``, formatted ext4 by the guest on first boot.

Why a disk and not a share (see design/storage.md): the production storage
class is NFS-backed, and ganesha cannot re-export one — `FSAL_VFS` refuses to
build the export and `FSAL_PROXY_V4` overflows its read buffer in every
released version. virtiofs is not the answer either: KubeVirt (<= 1.8) runs
virtiofsd unprivileged (kubevirt#13028), making a shared home read-only for
the guest user. A block device sidesteps all of it — ownership is whatever the
guest writes, and locking is ordinary local kernel locking on ext4.

**Per instance, not per user.** A home that followed a user would carry data
across zones: write in a restricted zone, reboot into a permissive one (zone
membership changes on reboot), read it out. The disk belongs to the instance,
and the instance is pinned to its zone.

Finding the disk: by ``/dev/disk/by-id/virtio-<serial>``, never ``/dev/vdb``.
Device order is not guaranteed, and mounting the wrong disk as ``$HOME`` fails
silently.

Formatting is guarded: ``mkfs.ext4`` runs only when ``blkid`` reports no
filesystem on the device, and never with ``-F``. If ``blkid`` is missing the
script refuses to format at all — a guest with no home is recoverable, a
reformatted home is not. This is the one line in this module that can destroy
a user's data.

Mount latency: runcmd alone is too late a trigger — cloud-final waits for
multi-user.target, which snapd.seeded holds up for ~30s on stock Ubuntu
images (measured), all after the login prompt. So a detached ``bootcmd``
poller kicks the script the moment write_files lands it (seconds into boot,
typically before the first getty); the runcmd start remains as the
systemd-visible belt-and-braces. A unit rather than a bare runcmd mount so
persistent-root (CDI) guests remount on every boot — runcmd is
once-per-instance.

The pre-mount window (getty/sshd up seconds before the mount lands): the
mountpoint stays root-owned until the disk arrives — deliberately, as an
honest "home not ready" signal; anything written there would be shadowed
by the mount. The script respawns the autologin getty once the home lands
so a console shell opened early doesn't keep the shadowed directory as its
cwd. ``useradd -m`` neither chowns nor skels the existing mountpoint —
harmless; the script chowns the mount root to the user after mounting.

``home_disk=False`` emits none of the machinery above. Every VM gets a home
disk today — gating it on ``persistence`` gave the desktop templates (which
are ``persistence: ephemeral`` and live for weeks) no home at all — so the
flag is the honest interface rather than a live configuration.

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
# KubeVirt stamps this on the home disk (domain.devices.disks[].serial) and
# udev turns it into /dev/disk/by-id/virtio-<serial>. Both sides must agree:
# _build_vm_spec sets it, the guest script looks it up. Keep it short and
# alphanumeric — KubeVirt caps the serial and udev does not escape it.
HOME_DISK_SERIAL = "whistlerhome"
HOME_DISK_PATH = f"/dev/disk/by-id/virtio-{HOME_DISK_SERIAL}"


def build_user_data(*, username: str, uid: int, ssh_keys: list,
                    hostname: str, home_disk: bool = True,
                    gid: int = None,
                    autologin: bool = True, desktop: bool = False,
                    streamer_env: dict = None,
                    display_port: int = None,
                    host_key: bytes = None, host_cert: str = None) -> str:
    """Return a ``#cloud-config`` userData document for cloudInitNoCloud.

    ``home_disk`` is whether this session has a per-instance home disk to
    format and mount. False for ephemeral sessions, whose home simply lives on
    the root disk — the mount script, its unit and its bootcmd poller are then
    not emitted at all.

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
    # After a successful mount the autologin getty is respawned: a console
    # shell spawned in the pre-mount window keeps the now-shadowed directory
    # as its cwd.
    getty_respawn = (
        "systemctl try-restart serial-getty@ttyS0.service\n" if autologin
        else "")
    # nosuid,nodev: the home is user-controlled storage, and nothing in it has
    # any business being setuid or a device node. No _netdev/nofail dance —
    # this is a local disk, not a share whose server might be missing.
    mount_opts = "nosuid,nodev"
    mount_script = f"""#!/bin/sh
# Written by Whistler cloud-init; run by whistler-home.service.
set -u
DISK={HOME_DISK_PATH}
HOME_DIR={home}
OWNER={uid}:{gid}

mountpoint -q "$HOME_DIR" && exit 0
# Own the mountpoint rather than waiting for cloud-init's mounts module to
# make it: bootcmd kicks this script off well before that module runs. Left
# root-owned until the disk lands — see the module docstring, it is the
# "home not ready" signal.
mkdir -p "$HOME_DIR"

# The virtio disk can probe in after bootcmd has already fired.
i=0
while [ ! -e "$DISK" ] && [ "$i" -lt 60 ]; do
    i=$((i + 1))
    sleep 1
done
if [ ! -e "$DISK" ]; then
    echo "whistler: home disk $DISK never appeared" >&2
    exit 1
fi

# Format ONLY on a device carrying no filesystem.
#
# This is the one place in Whistler that can destroy a user's data, so it is
# guarded twice. blkid exits non-zero when it finds no signature, which is the
# actual test; but if blkid is missing its failure would look exactly like an
# empty disk and we would reformat a populated home on every boot. So a
# missing blkid refuses to format instead of assuming. A guest that comes up
# with no home is recoverable in a way that a wiped home is not.
#
# mkfs is deliberately NOT given -F: if blkid is somehow wrong, mkfs's own
# "will not make a filesystem here" is the last thing standing between a
# reboot and an empty home.
if ! command -v blkid >/dev/null 2>&1; then
    echo "whistler: blkid missing; refusing to risk formatting $DISK" >&2
    exit 1
fi
if ! blkid "$DISK" >/dev/null 2>&1; then
    echo "whistler: no filesystem on $DISK, creating ext4" >&2
    mkfs.ext4 -q -L whistler-home "$DISK" || exit 1
fi

mount -o {mount_opts} "$DISK" "$HOME_DIR" || exit 1
# Non-recursive on purpose: claim the mount root so the user owns their home,
# but never walk the tree — that would be slow on a large home and would undo
# any ownership the user set deliberately.
chown "$OWNER" "$HOME_DIR"
chmod 0755 "$HOME_DIR"
{getty_respawn.strip() or ":"}
exit 0
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
        #     Unable to load host key: /etc/ssh/whistler_host_ed25519_key
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
    ]
    if home_disk:
        write_files.extend([
            {
                "path": "/usr/local/sbin/whistler-mount-home",
                "permissions": "0755",
                "content": mount_script,
            },
            {
                "path": "/etc/systemd/system/whistler-home.service",
                "content": (
                    "[Unit]\n"
                    "Description=Mount the Whistler home disk\n"
                    # local-fs, not network-online: this is a virtio disk now,
                    # so the thing worth waiting for is udev having populated
                    # /dev/disk/by-id (the script polls for it anyway).
                    "Wants=local-fs.target systemd-udev-settle.service\n"
                    "After=local-fs.target systemd-udev-settle.service\n"
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
        ])
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
            # The guest's primary group must really BE the gid the rest of
            # Whistler believes the user has: ownership on the home disk is
            # whatever the guest writes, and pod sessions mounting the same
            # user's other volumes expect these ids. useradd would otherwise
            # invent a user-private group at whatever gid happens to be free.
            # bootcmd runs before cloud-init's users-groups module (and on
            # every boot, so it is idempotent by construction), which is what
            # lets `primary_group` below reference this gid.
            f"getent group {gid} >/dev/null || groupadd -g {gid} {username}"
            " || true",
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
        "runcmd": ["systemctl daemon-reload"],
    }
    if home_disk:
        # Early mount kick (see module docstring): bootcmd runs in the init
        # stage BEFORE write_files, so poll for the script and run it as soon
        # as it lands — detached (setsid, fds closed) so cloud-init is never
        # blocked. Waiting for runcmd instead would sit behind
        # snapd.seeded/multi-user, ~30s past the login prompt.
        doc["bootcmd"].append(
            "setsid sh -c 'i=0; while [ ! -x /usr/local/sbin/whistler-mount-home ]"
            " && [ $i -lt 120 ]; do i=$((i+1)); sleep 1; done;"
            " exec /usr/local/sbin/whistler-mount-home'"
            " </dev/null >/dev/null 2>&1 &")
        # Final stage: arm the mount unit (enable makes persistent-root guests
        # remount on later boots) and kick it off now, non-blocking — the
        # disk-probe wait must not stall the rest of first boot.
        doc["runcmd"].extend([
            "systemctl enable whistler-home.service",
            "systemctl start --no-block whistler-home.service",
        ])
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
