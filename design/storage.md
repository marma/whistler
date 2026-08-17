# Storage direction

**Decided 2026-08-17.** Where a session's data lives, and why each tier is the
shape it is. The measurements this rests on are in
[security.md](security.md#when-the-only-storage-class-is-nfs) and
[proxy-v4-heap-bug.md](../images/storage-gateway/proxy-v4-heap-bug.md); this
document is the resulting plan, not the evidence for it.

## The constraint

`csi-driver-nfs` is the only production storage class available. Block-backed
storage was asked for and refused, so this is not a temporary inconvenience to
wait out — it is the substrate.

The storage gateway cannot run on it. `FSAL_VFS` refuses to export an
NFS-backed PVC at all (`resolve_posix_filesystem` → ENOENT), and
`FSAL_PROXY_V4` — the only FSAL that *does* export one — copies READ replies
into the caller's buffer using the length off the wire, unclamped. That is
byte-identical in **6.5, 9.14 and 14.1**, so it is not a version-bump away
from working, and **14.1 is the worst of the three**: it usually survives and
silently returns bytes that are not the file.

The conclusion that shapes everything below: **NFS leaves the VM data path.**
Not because NFS is wrong, but because the one arrangement that would let us
keep it — ganesha in front of NFS — is a configuration almost nobody runs, and
it shows.

## The shape

| | Now | After Ceph/Rook |
| --- | --- | --- |
| **Home** | one disk image per instance, on a PV | CephFS or RBD |
| **Shared data** | S3, read-mostly datasets | same, backed by RGW |
| **Shared POSIX filesystem** | **not offered** | CephFS |

Nothing here is thrown away at the Ceph transition. Rook ships RGW, which
speaks S3, so the S3 tier survives a change of endpoint; CephFS then covers
the case S3 cannot. Each stage removes a workaround instead of adding one.

## Home: one disk image per instance

A home is an ordinary PVC (`volumeMode: Filesystem`, so the NFS share holds a
`disk.img`) attached to the VM as a **second virtio-blk disk**. The guest
formats it and mounts it at `$HOME`. This is not exotic — it is exactly the
mechanism the VM root disk already uses, and `_build_vm_spec`
([config.py:2063](../whistler/config.py#L2063)) already emits `disks`,
`volumes` and `dataVolumeTemplates`. The docstring's *"the user's home PVC is
NOT attached"* is the line that changes.

**Per instance, not per user.** This is the part that is a decision rather
than a mechanism, and the reason is zone integrity. A home that follows a
*user* is a channel between zones: boot an instance in a restricted zone,
write to home, reboot into a permissive one — zone membership changes on
reboot, the disk follows the user, and the data is out. Per-instance homes
cannot do that, because the image belongs to an instance and the instance is
pinned to its zone.

What that costs, stated plainly:

- **A user with several instances has several homes.** There is no single
  `$HOME` following someone around. Dotfiles, keys and toolchains are
  per-instance unless the user syncs them deliberately.
- **A home stops being one object across session types.** Container sessions
  get their own volume; a pod mounting the PVC directly would see an opaque
  image file. This is closer to the runtime split already decided —
  a container session is a throwaway workspace, the VM is the machine — than
  the shared-PVC arrangement was.
- **No shared homes**, because a block device cannot be safely multi-attached.
  [Shared homes as a deployment option](security.md#shared-homes-as-a-deployment-option)
  still needs a file-level share and therefore still needs the gateway.
- **Admin loses file-level inspection and backup.** It is an image.
- **Resizing** needs a guest-side step after the PVC grows.

What it deletes, which is most of the difficulty this replaced: no ganesha on
the VM path, no FSAL to crash, no `resolve_posix_filesystem` refusal, no
squash identity to get right, no 2049 fencing surface, and no lock-domain
questions — locking becomes ordinary local kernel locking on ext4.

The storage gateway itself stays in the tree. It works correctly on
block-backed storage classes, and shared trees would need it back. It is
simply no longer on the critical path for a home.

## Shared data is S3 — and these are datasets, not filesystems

Shared data lives in S3 buckets, mounted in the guest with `rclone mount`
(chosen for its `--vfs-cache-mode` handling and its willingness to speak to
any S3-compatible endpoint), set up by cloud-init as a systemd unit at a
predictable path.

**The contract is "shared dataset", not "shared filesystem", and the
vocabulary should say so everywhere it appears.** S3 through FUSE does not
give you:

- **byte-range locking** — SQLite, dconf, editors, anything with a lockfile;
- **atomic rename** — it is copy-then-delete, so also `O(size)`;
- **hardlinks**, real ownership, or reliable permission bits;
- **safe concurrent writers** — last writer wins, silently.

That last point deserves the same suspicion as the two-lock-domain hazard
below: it does not error, it loses data. And the first point is exactly the
failure class that cost a day of debugging to characterise in the first place.

So: **read-only by default**, with `rw` a deliberate grant for scratch or
output space that one session owns at a time. Read-mostly, whole-file,
large-object workloads — datasets, model weights, media, corpora — are what
this is good at, and that is genuinely most of what shared volumes are used
for. If someone needs POSIX semantics on shared data, the answer is "not until
Ceph", said out loud, rather than an S3 mount that appears to work until it
does not.

**JuiceFS** is the only serious pre-Ceph option for real POSIX-over-S3
(including `flock`/`fcntl`), and it is deliberately not taken: it needs a
separate metadata engine that becomes a stateful component you must never
lose, since losing it loses the filesystem even though every object is intact.
That is a large operational surface for a case Ceph will cover properly.

## Zones fence the network, not the data

Worth stating plainly because the alternative reading is flattering and false.

Zones are a **reach** control: they decide which endpoints a session's packets
can get to ([`_build_egress_rules`](../whistler/config.py#L1718), CIDR and
selector based). Credentials are an **identity** control. A bearer token has
no zone — once it is in a guest whose user is root, it can be read and used
from anywhere the endpoint is reachable. Credentials do not merely fail to
compose with zones; they route around them.

Which gives the rule everything else follows from:

> **A shared-data endpoint must never be reachable from outside the cluster.**

Then a leaked credential is inert without network reach, reach is what zones
already control, and the two compose as AND rather than as OR. A
publicly-routable bucket with a credential handed to a root-capable guest has
no zone story at all, and no policy language will give it one.

**The honest residual:** zones stop a *session* from reaching the network.
They do not stop a *person* from carrying data. A user with instances in two
zones can move files between them through their own client — the SSH plane is
that channel, not storage. Per-instance homes close the storage channel, which
is the one Whistler was silently providing; the human channel is out of scope
and should be named as such rather than left to be discovered.

## Whistler runs the S3 proxy

Sessions do not talk to the real S3 server. Whistler starts an in-cluster
proxy that holds the connection to it, and zone egress rules reference the
proxy's Service address.

**The failure this prevents.** A zone rule binds to an *address*. If the S3
server is external, Whistler does not own that address and cannot know when it
changes. Move the server, or let a CIDR be reused, and the rule silently means
something other than what it meant when it was written — worst case, data
intended for a restricted zone becomes reachable from a more permissive one.
That failure is **silent and fail-open**, which is the combination this
codebase refuses everywhere else (unknown zones fail closed; an unroutable
instance name refuses the channel). A proxy Whistler creates has a ClusterIP
Whistler assigned, so the binding between "this zone" and "this data" is known
by construction rather than asserted and hoped for.

**It also fixes the credential problem, which is why it outranks the earlier
plan of mounting S3 directly from the guest.** If the proxy holds the
credential, the guest never has one. A root user cannot exfiltrate what it
never received, and revocation becomes real instead of theoretical. It also
makes `mode: ro` meaningful on a VM for the first time — the proxy holds a
read-only credential, and no amount of root in the guest changes what the
proxy will do. Compare today, where a `ro` grant "is not yet a boundary" for
VMs precisely because the guest has root.

What it costs: a component to run and keep available, a bandwidth chokepoint,
and Whistler back in the data path after this document just took it out of the
home path. That last one is worth being uncomfortable about — the difference
is that this proxy is a *credential and routing* boundary, not a filesystem
implementation, so it carries none of the correctness risk that put ganesha on
the floor.

**Open — the proxy's shape is not decided.** Two candidates:

1. **An S3 endpoint proxy.** Presents an S3 API in-cluster, forwards to the
   real one, substitutes credentials. Keeps the guest-side `rclone` story
   exactly as above; the guest points at the proxy and needs no secret.
2. **A share gateway.** Mounts the bucket itself and presents it as a
   filesystem the instance mounts. Removes FUSE from the guest, but re-creates
   a file-serving component with its own lock and consistency semantics —
   which is what this whole document is trying to get out of.

(1) is the smaller thing and preserves the tiering; (2) should only win if
guest-side FUSE turns out to be a real operational problem. Also undecided:
whether a proxy is per zone, per bucket, or per zone-and-bucket, and how it is
kept available.

## Still open

- **Ceph/Rook** is the target state and the only path to shared POSIX
  filesystems. It is now also the only path to shared volumes on VMs at all,
  since block storage was refused — worth stating in the case for it.
- **KubeVirt read-only disks** would make immutable shared datasets viable as
  multi-attached block devices, enforced below the guest. Unverified in the
  KubeVirt version in use.
- **Per-instance home ergonomics.** No single `$HOME` is the right call for
  zone integrity and a real cost to users; whether something (a dotfiles
  volume, a first-boot sync) should soften it is undecided. Whatever it is
  must not become a per-user object that crosses zones, which is the thing
  per-instance homes exist to prevent.
- **Backup** of home images, which are now opaque blobs rather than files.
