"""cloud-init userData builder for KubeVirt VM sessions.

Pure (no Kubernetes imports) so it is unit-testable without a cluster, like
`KubeConfigManager._build_pod_spec`. The generated ``#cloud-config`` document
creates the Whistler user with their real username/uid inside the guest and
mounts the per-user home PVC (exposed to the VM as a virtiofs share).

Ordering guarantees relied on here: cloud-init runs the ``mounts`` module in
the cloud_init stage and ``users-groups`` in the later cloud_config stage, so
/home/<user> is the live virtiofs share *before* ``useradd -m`` runs. useradd
on an existing non-empty directory skips the skel copy — exactly what a
returning user wants.

Console access is via serial-getty autologin rather than a generated
password: the portal's auth + RBAC already gate who can reach the console
websocket, the same trust model as the kubectl-exec web terminal for pods.
(The alternative — chpasswd with a per-session password stored in a Secret —
was considered and deferred; it adds a Secret lifecycle and UI surface for no
additional isolation.)
"""

import yaml


def build_user_data(*, username: str, uid: int, ssh_keys: list,
                    hostname: str, home_tag: str = "home",
                    autologin: bool = True) -> str:
    """Return a ``#cloud-config`` userData document for cloudInitNoCloud."""
    home = f"/home/{username}"
    keys = list(ssh_keys or [])
    # Authorized keys live on the ROOT disk (/etc/ssh/authorized_keys.d), not
    # in ~/.ssh: the home is a virtiofs share of the per-user PVC, and
    # KubeVirt's virtiofsd runs unprivileged — guest chowns don't stick and
    # created files get remapped uids, so sshd's StrictModes would (rightly)
    # refuse an authorized_keys living there. A root-owned path on the root
    # disk is StrictModes-clean and not writable through the share. The
    # drop-in keeps ~/.ssh/authorized_keys as a secondary source for guests
    # whose home storage has sane ownership.
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
    ]
    doc = {
        "hostname": hostname,
        # [fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno].
        # `nofail` keeps the guest booting even if the virtiofs device is
        # missing (e.g. feature gate off); the mounts module creates the dir.
        "mounts": [
            [home_tag, home, "virtiofs", "defaults,nofail", "0", "0"],
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
        # Best-effort ownership of the shared home: works on storage with
        # honest ownership, EPERMs harmlessly through unprivileged virtiofsd.
        "runcmd": [
            f"chown {uid}:{uid} {home} 2>/dev/null || true",
            f"chmod 750 {home} 2>/dev/null || true",
        ],
    }
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
        doc["runcmd"] = [
            "systemctl daemon-reload",
            "systemctl try-restart serial-getty@ttyS0.service",
        ] + doc["runcmd"]
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
