# storage-gateway — per-user NFS export of the home PVC

A minimal [NFS-Ganesha](https://github.com/nfs-ganesha/nfs-ganesha) image. The
operator runs one Deployment of it per user (`whistler-storage-<user>` in
`whistler-user-<user>`, created lazily with the first `runtime: vm` session)
that mounts the user's `whistler-data-<user>` PVC via normal CSI and exports it
as a single NFSv4.2 share, `/home`. KubeVirt VM guests mount that share from
cloud-init.

## Why not virtiofs

KubeVirt (≤ 1.8) runs virtiofsd unprivileged: hardcoded uid 107,
`--sandbox=none`, capabilities dropped (upstream kubevirt#13028, verified
empirically). Only guest root or exactly 107:107 can write through the share —
the guest user's home is read-only, and no securityContext or group trick
fixes it.

## Why NFS (this replaced SMB)

- **Byte-range locks work.** The reason for the move. cifs forwards SQLite's
  past-EOF `F_WRLCK` (offset `0x40000002`, len 510) to the server, which
  answers EACCES; SQLite reads that as contention and spins its busy handler
  into a flat 100 s stall and then "database is locked" — for nautilus's
  tag store, dconf, gnome-keyring and Chrome's whole profile. The SMB gateway
  had to mount with `nobrl`, which throws locking away entirely. NFSv4 carries
  lock state in the protocol, on the same connection: the same lock is granted
  in ~0 ms.
- **Native POSIX semantics.** Real per-file modes and real symlinks, with no
  `posix`/`mfsymlinks` mount options to get right — and a VM-created symlink is
  a symlink to pod sessions on the same PVC, not an XSym stub.
- **One port, no RPC zoo.** NFSv4 needs no rpcbind, no NLM, no statd: TCP 2049
  is the entire surface to fence.

What it costs, and what replaces it:

- **No per-share credential.** SMB authenticated per user with a password, so a
  session could simply not hold the secret for a share it may not see. NFS
  AUTH_SYS has no such thing — the client *asserts* its uid, and the guest is
  root. The boundary is now entirely "which export exists and who can reach
  it": one export per user, ingress-fenced by NetworkPolicy to that user's own
  session pods in that user's own namespace. `sec=krb5` would restore
  per-principal authentication if it is ever needed
  ([design/security.md](../../design/security.md)).
- **No transport encryption.** SMB mounted with `seal`. NFS AUTH_SYS is
  cleartext on the cluster pod network; krb5p is the answer if that changes.

Identity stays server-side, exactly as `force user` made it: `Squash =
All_Squash` with `Anonymous_Uid`/`Anonymous_Gid` set to the user's **real**
ids maps every request to that one identity, whatever the guest asserts.
Verified: a write by guest uid 4242 lands on the PVC as the user's uid, and a
`chown` to another uid is squashed away (it returns success and changes
nothing — a pin rather than SMB's EPERM). A `chmod` sets the true on-disk
mode, so it is durable across gateway restarts and consistent with pod
sessions that mount the same PVC directly. Guests mount `nosuid`, so a setuid
bit set through the share is inert there; on the PVC it can only ever be
setuid to the user themselves.

## Why Ganesha, not the kernel server

`nfsd` is a kernel-global resource and wants a privileged container; ganesha is
a userspace server, so one ordinary pod per user works. It needs two
capabilities, granted in `_build_gateway_manifests`:

| Capability | Why |
| --- | --- |
| `DAC_READ_SEARCH` | the VFS FSAL addresses files by handle (`open_by_handle_at`) |
| `SYS_RESOURCE` | raising its own file-descriptor limit |

The handle-based FSAL means the PVC's filesystem must support
`name_to_handle_at` — true for ext4/xfs (so for local-path, Longhorn, RBD, and
anything else that hands out a block device), not for an overlayfs.

`Cannot register NFS V4 on TCP` in the log is ganesha failing to reach a
rpcbind that deliberately isn't there. NFSv4 does not use it; the server
listens on 2049 regardless.

## `/etc/mtab`, and why this image can't be trusted under `docker run`

Ganesha builds its filesystem table with `setmntent(/etc/mtab)` and, if that
fails, creates **no exports at all** — while still binding 2049 and logging
`NFS SERVER INITIALIZED`. Guests then mount and get `ENOENT`, and a TCP
readiness probe calls the pod healthy.

debian-slim ships no `/etc/mtab`. **Docker creates the symlink at container
start; containerd does not.** So this image exported fine under every
`docker run` test and exported nothing the moment it ran in Kubernetes. The
Dockerfile now owns the symlink instead of depending on the runtime, and the
entrypoint refuses to start without a readable mount table — a CrashLoop is a
far better failure than a healthy-looking server with no exports.

The lesson generalises: verifying an image change under `docker run` alone is
not sufficient evidence that it works in-cluster.

## Readiness: `gateway-ready`

Because a TCP probe on 2049 cannot tell a working gateway from one with an
empty export list, readiness is an **exec** probe
([`gateway-ready.sh`](gateway-ready.sh)) asserting both halves:

1. something is listening on 2049 (bash `/dev/tcp` — no `nc` in the image), and
2. ganesha itself reports the export, over its DBus interface
   (`org.ganesha.nfsd.exportmgr.ShowExports`).

The entrypoint therefore starts a system bus before ganesha — ganesha registers
its DBus objects once at startup and never retries, so ordering matters — and
treats a bus that will not start as fatal. `dbus-daemon`/`dbus-send` ship with
the `nfs-ganesha` package, so this adds no image weight.

The same script backs a `startupProbe`, so a gateway that never manages to
export restarts into a visible CrashLoop instead of sitting NotReady forever.
Verified both ways: healthy gateway passes; a ganesha started with no
`/etc/mtab` has 2049 open (which is what fooled the old probe) and the probe
correctly fails with *"listening on 2049 but NOT exporting /shares/home"*.

## Contract

| Input | Meaning |
| --- | --- |
| `SHARE_USER` env | Whistler username (logging/labelling only) |
| `SHARE_UID` env | The user's real uid — the export's `Anonymous_Uid` |
| `SHARE_GID` env | The user's real gid — the export's `Anonymous_Gid` |
| `/shares/home` mount | The user's home PVC |

No Secret and no credentials file: there is nothing to authenticate with.

Manifests are built in `KubeConfigManager._build_gateway_manifests`
([whistler/config.py](../../whistler/config.py)); the guest mount in
[whistler/cloudinit.py](../../whistler/cloudinit.py).
