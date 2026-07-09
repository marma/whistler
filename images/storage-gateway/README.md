# storage-gateway — per-user SMB export of the home PVC

A minimal Samba image. The operator runs one Deployment of it per user
(`whistler-storage-<user>` in `whistler-user-<user>`, created lazily with the
first `runtime: vm` session) that mounts the user's `whistler-data-<user>` PVC
via normal CSI and exports it as a single SMB3 share, `home`. KubeVirt VM
guests mount that share over cifs from cloud-init.

## Why not virtiofs

KubeVirt (≤ 1.8) runs virtiofsd unprivileged: hardcoded uid 107,
`--sandbox=none`, capabilities dropped (upstream kubevirt#13028, verified
empirically). Only guest root or exactly 107:107 can write through the share —
the guest user's home is read-only, and no securityContext or group trick
fixes it.

## Why SMB (over NFS)

- **Server-side identity.** Client uids are never trusted — inherent root
  squash. A rogue guest root gets the same access as the user, no more.
- **`force user`** makes every file land on the PVC as the user's *real* uid,
  keeping the PVC consistent for pod sessions sharing it.
- Per-user read-only enforcement later via `read list`/`write list`.
- Transport encryption (`seal` / `server smb encrypt = required`).
- Windows guests work later for free.

## Contract

| Input | Meaning |
| --- | --- |
| `SMB_USER` env | Whistler username (share account + `force user`) |
| `SMB_UID` env | The user's real uid; the account is created with it |
| `/etc/whistler-smb/password` | Random per-user password (Secret `whistler-smb-<user>`) |
| `/shares/home` mount | The user's home PVC |

Manifests are built in `KubeConfigManager._build_gateway_manifests`
([whistler/config.py](../../whistler/config.py)); the guest mount in
[whistler/cloudinit.py](../../whistler/cloudinit.py).

The share uses a strict single-user profile. The SMB3.1.1 POSIX-extensions
profile (for user-managed group permissions on future *shared* volume
exports) is deliberately not enabled on the home share.
