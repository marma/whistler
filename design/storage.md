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
| **Home** | one disk image per home volume, on a PV | CephFS or RBD |
| **Shared data** | S3, read-mostly datasets | same, backed by RGW |
| **Shared POSIX filesystem** | **not offered** | CephFS |

Nothing here is thrown away at the Ceph transition. Rook ships RGW, which
speaks S3, so the S3 tier survives a change of endpoint; CephFS then covers
the case S3 cannot. Each stage removes a workaround instead of adding one.

## Home: one disk image per instance (interim; named home volumes next)

A home is an ordinary PVC (`volumeMode: Filesystem`, so the NFS share holds a
`disk.img`) attached to the VM as a **second virtio-blk disk**. The guest
formats it and mounts it at `$HOME`. This is not exotic — it is exactly the
mechanism the VM root disk already uses, and `_build_vm_spec`
([config.py:2063](../whistler/config.py#L2063)) already emits `disks`,
`volumes` and `dataVolumeTemplates`. The docstring's *"the user's home PVC is
NOT attached"* is the line that changes.

**Per instance, not per user — as built, and now an interim.** The reason was
zone integrity. A home that follows a *user* is a channel between zones: boot
an instance in a restricted zone, write to home, reboot into a permissive one
— zone membership changes on reboot, the disk follows the user, and the data
is out. A per-instance home cannot do that, because the image belongs to an
instance and the instance is pinned to its zone.

That closes the hole by removing the choice, which is the right thing to build
first and the wrong thing to stop at: it also forbids the cases a lab actually
needs, such as one home per zone with the open one readable from the
restricted instance. **The direction (decided 2026-08-19) is named home
volumes**, chosen at instance creation, with the
[access matrix](security.md#core-model-the-access-matrix) deciding which zones
each may be mounted in and the one-live-attach rule keeping a volume in one
running instance at a time. The per-instance home then becomes the *default*
— a new instance gets a new home named after it — rather than the only
possibility. Existing `whistler-home-<session>` PVCs are adopted as named
volumes and lose their Session `ownerReference`, so nothing is deleted by the
change.

Until that lands, what the per-instance home costs, stated plainly:

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
that channel, not storage. What Whistler closes is the *storage* channel, the
one it would otherwise provide silently by handing the same volume to two
zones; the human channel is out of scope and should be named as such rather
than left to be discovered.

This is also why the
[access matrix](security.md#core-model-the-access-matrix) does not pretend to
be containment. It becomes a real boundary exactly when the user has no shell
in one of the two zones, because only then is storage the only path — and
Whistler can tell an admin when a cross-zone grant exceeds the channels that
user already holds.

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
proxy will do. Compare a PVC, where a `ro` grant is still "not yet a boundary"
for VMs precisely because the guest has root. Verified against a root guest
that dropped the client-side flag and went at the proxy directly — see the run
notes below.

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
guest-side FUSE turns out to be a real operational problem.

**Built as (1), 2026-08-17.** `rclone serve s3` in Whistler's own namespace,
**one Deployment per (volume, mode)** — a shared dataset cannot live in one
user's namespace, and `--read-only` is a server-wide flag rather than a
per-key one, so `ro` and `rw` cannot share a process. That separation is what
makes a read-only grant a boundary on a VM rather than a suggestion.

Two things enforce access, and they compose as AND:

- **Reach** — each proxy's own NetworkPolicy admits only the namespaces of
  users granted that volume at that mode, and an ungranted dataset yields an
  empty ingress list, which NetworkPolicy reads as deny-all. The baseline
  egress carve-out letting sessions reach the proxies is irrevocable by a zone
  (allows are union'd), which is safe only because reaching a *proxy* is not
  reaching a *dataset*.
- **Credential** — a generated per-(volume, mode) key pair opens the proxy.
  The real bucket credential comes from an admin-provided Secret mounted into
  the proxy alone, so it never enters a guest whose user has root.

Still open: a proxy is a single replica with no availability story; grant
changes reached a proxy's policy only on the next session reconcile (pushed
since 2026-10-08, see below); and **datasets are VM-only** — a pod would need `/dev/fuse` to mount
one, so container sessions currently see no S3 volume at all.

### Datasets are their own kind, 2026-08-18

A dataset started life as a `type: s3` entry in the volume catalog. That was
wrong in two ways, and both showed up in practice:

- **A dataset is not a Kubernetes volume source.** Every other entry in that
  catalog is copied straight into a pod spec by `_build_volume_wiring`. An S3
  definition copied there is not a valid volume, so a container session that
  requested one would have produced a pod the API server rejects.
- **It could only be defined in `values.yaml`**, which meant a Helm upgrade to
  add a bucket.

So there is now a `Dataset` CR (`dset`), managed in the portal's Datasets
section exactly the way zones are: the chart renders `whistler.datasets` as
Helm-owned CRs, and admins create further ones in the UI. Legacy `type: s3`
volume entries still resolve, and a Dataset CR of the same name wins.

Two things the move made possible rather than just tidier:

- **`readOnly` on the dataset is a ceiling**, and it is the only way to say
  "nobody writes this". It was needed because the composition rule cuts the
  surprising way: an empty allow-list means *unrestricted*, so before this a
  dataset was writable by every user who had no allow-list at all. The ceiling
  is enforced where it counts — a read-only dataset resolves every grant to
  `ro` and leaves its rw proxy admitting nobody, so it holds against root in a
  guest. Verified end to end: a dataset granted `rw` by a group, refusing a
  root write.
- **The portal can hold the bucket credential.** Typed into the editor, stored
  in a Secret, mounted into that dataset's proxies and never rendered back —
  an admin can replace a credential but not retrieve one. Naming your own
  Secret instead still works and keeps the credential out of Whistler's hands,
  which is the posture this document originally assumed. Writing Secrets is a
  namespaced Role, deliberately not part of the portal's ClusterRole; deleting
  that Role leaves the editor working with admin-provided Secrets only.

**One malformed dataset used to stop every VM in the cluster.** A dataset
saved without a credential raised `KeyError` inside the session reconcile,
which kopf retried forever, so no session anywhere could start. Datasets are
admin-editable, so a malformed one is an ordinary event, not an exceptional
one: preparing a dataset now degrades to "that dataset does not mount" and the
session comes up with the ones that work.

### What the first end-to-end run cost, 2026-08-17

Verified on k3s-metal against a real S3 server (versitygw over a PVC,
`manifests/s3-rig/`), guest to backing store. Four things only the cluster
could have told us, three of which were silent:

- **`rclone serve s3 :s3:<bucket>` loses every file at the dataset's top
  level.** It promotes the served directory's *subdirectories* to buckets, and
  S3 cannot address an object with no bucket — so a dataset of loose files
  mounts **empty, with no error**. Fixed by serving the backend wrapped in a
  `combine` remote, which puts exactly one bucket at the root. The rig's seed
  data now contains a top-level file for precisely this reason.
- **AppArmor forbids the mount point.** Ubuntu's `fusermount3` profile permits
  FUSE mounts only under `@{HOME}`, `/mnt`, `/media`, `/tmp`,
  `@{run}/user/@{uid}` and `/cvmfs`. `/shared/<name>` is denied, reported only
  as `fusermount: mount failed: Permission denied` with the real reason in
  `dmesg`. The profile has no `local/` include, so the mounts moved to
  `/mnt/shared/<name>` with `/shared` symlinked to it — a symlink is not
  subject to the mount rule.
- **A downgrade from `rw` to `ro` did nothing.** Proxies were reconciled only
  for the mode a session actually mounts, so the `rw` proxy kept a policy
  naming the user — and the user is root in their own guest, so they still had
  that proxy's key from the previous session's `rclone.conf`. Every dataset's
  proxies are now re-fenced on each session build, including modes the user is
  losing, which is the case that matters.
- **`s3_proxy_users` called a method that does not exist**, and would have
  failed every VM boot the moment anyone was actually granted a dataset. It
  had no test, because only the pure policy builder did — the list was always
  passed in by hand.

Confirmed working: reads and writes through the whole chain; a `ro` grant
refusing a write from **root** in the guest bypassing the mount entirely, with
the backing store unchanged; and a valid credential from an ungranted
namespace refused at the network, which is the AND composing as designed.

Two rough edges left as-is. The read-only proxy answers a refused write with
**HTTP 500 InternalError** rather than 403, and rclone retries it three times
over ~45s — the boundary holds, the diagnosis is poor. And the proxy's VFS
listing cache (now `--dir-cache-time 1m`) means a dataset changed by another
writer appears stale for up to that long, which is the read-mostly assumption
showing through.

### Datasets are always S3; Whistler creates the storage, 2026-10-08

Decided, not yet built (except the revocation fix at the end).

**A dataset is S3 to the guest, whatever is behind it.** One mount path
(`rclone mount` against an in-cluster endpoint), one contract (no locks, no
atomic rename, last writer wins), and a block- or file-backed volume served
this way cannot be mistaken for a shared filesystem — the S3 interface *is*
the statement that it is not one. Backends live in the server pod, never in
the guest.

**First backend: a managed PVC served by VersityGW.** The portal creates a
dataset with a size; the **operator** creates the claim (default storage
class) and the VersityGW Deployment in the release namespace. The portal marks,
the operator acts, as with home deletion and archiving, so the portal gains no
PVC verbs. Remote S3 (today's `rclone serve s3` over an admin's bucket) is
parked rather than removed.

The question this answers is **how much the portal may define**. Letting it
reference existing storage is the dangerous direction, and specifically so in
this cluster: every home's PV is `Retain` (design/backup.md), so Released
homes of deleted users lie around; the backup claim holds every CR and, with
no passphrase, the secrets; user home claims sit in user namespaces, and a
proxy for one would have to run there. A portal that can only *create* a claim
can point Whistler at none of it. If existing volumes (NFS, CephFS, a
pre-filled PVC) are wanted later, the rule is that a cluster admin hands them
over — a labelled claim in a dedicated namespace, created with kubectl — and
the portal chooses among those and narrows (subPath, read-only), never names a
PV, a namespace or an NFS server. Raw `server:/path` stays out of the portal:
under AUTH_SYS the server pod could claim any uid on any export it can route
to.

What a managed dataset owes, by analogy with homes: `Retain` on its PV, a
`user-data=dataset` label, re-attach on reinstall, and a place in the
backup/uninstall story. Deleting is two acts (2026-10-08, see "Archived
datasets" below): delete archives, and only an archived dataset can be
deleted for good, behind a typed-name confirmation. VersityGW's POSIX
backend makes top-level directories buckets, so the volume is laid out
`/<dataset>/…` — the same trap as `rclone serve s3` above.

**Credentials become per-user, and revocation deletes them.** Today's proxy
key is one per (dataset, mode), generated once and never rotated, and every
guest that ever mounted the dataset holds it in a root-readable
`rclone.conf`. That is tolerable only because reach is the real boundary and
the endpoint is internal: a revoked user keeps the key and cannot connect.
Rotation on restart was considered and rejected — it ties revocation to
whenever something restarts rather than to the admin's act, and rotating a
shared key breaks every other holder's running mount. Instead: one VersityGW
account per `(user, dataset, mode)`, created and deleted by the operator from
the matrix, so removing a cell kills that key everywhere *and* re-fences the
endpoint, without touching anyone else. That moves part of the ro/rw boundary
into VersityGW's authorization, so ro and rw stay **separate processes over
one volume** anyway (two containers in one pod: the ro one mounts the claim
`readOnly`, and per-port NetworkPolicy rules keep the fencing per mode, which
also lets an RWO claim serve both). To verify before building: whether
VersityGW checks credentials per request (S3 signing says it should, and that
is what makes deletion cut an open mount), and whether it has a server-wide
read-only mode or the ro side needs another server.

**External access reverses this document's rule, so it is a grant, not a
flag.** Exposing a dataset outside the cluster means "zones fence the network,
not the data" stops protecting it for whoever holds an outside credential. It
is taken on deliberately, with two conditions:

- **Internal keys never work outside.** The external listener is a separate
  server with its own accounts, behind an Ingress. This is what actually
  protects a kiosk-bound user's restriction: the leak path is not an external
  credential (minted in the portal, which a kiosk-only account cannot enter)
  but the internal key sitting in their guest, where they are root.
- **`external` is a reserved zone in the access matrix.** A cell
  `external: {dataset: read-only}` is the only thing that issues an outside
  credential, per user and revocable like the internal ones. Nobody holds one
  unless an admin writes that cell, kiosk accounts included, and the grid shows
  "this person can take this data off the cluster" where the admin edits
  access, rather than behind a per-dataset switch.

**One server per dataset, not one for all of them.** A shared VersityGW
serving every dataset as a bucket was considered and rejected, for three
reasons, the first of which decides it alone:

- **Fencing.** NetworkPolicy selects pods and ports, not buckets. One server
  turns "alice may reach refdata in zone open" into "alice may reach the
  dataset server in zone open", and per-dataset access then rests on
  VersityGW's authorization alone — the reach half of the AND is gone.
- **Lifecycle.** A pod's volumes are fixed while it runs, so a shared server
  either restarts for every new dataset (cutting everyone's mounts) or serves
  one big claim with a directory per dataset, which gives up per-dataset size,
  `Retain`, archiving and clean deletion.
- **Blast radius.** A crash, a full disk or an authorization bug takes one
  dataset, not all.

The cost is a pod per dataset (VersityGW is one Go binary). Inside it, ro and
rw are two containers, and "read-only" there means **process-wide**: the ro
container cannot write whoever it believes is asking, which is a property of
the process (a startup flag, or the claim mounted `readOnly` so the kernel
refuses), not a per-account permission. That is what keeps a read-only grant
a boundary if VersityGW's ACLs have a bug.

Keys are **stable across restarts and die only with the grant**: the operator
keeps each account's key in a Secret and re-creates the account from it
whenever the server comes back, and the account list lives outside the served
tree — it is derived from the matrix, so there is nothing in it to back up.
For the external listener, one server holding exactly one bucket means an
Ingress can route `https://<host>/<dataset>/…` by path to that dataset's
external container: path-style S3 puts the bucket first in the path, and the
signature covers a path the Ingress does not rewrite (unverified).

Built first, in order: managed datasets on the internal endpoint only; then
per-user keys; then the external listener and the `external` zone, as a
separate decision.

**Phase 1 built, 2026-10-08: managed datasets, internal only, shared keys.**
`Dataset.spec.source: managed` + `size`; the portal offers it as the default
when creating a dataset. The operator builds it when the CR appears
(`ensure_managed_dataset`, kopf handlers on Datasets) in this order: a
deny-all policy for the server pod, the per-mode policies, the claim
(re-attached from its retained PV if one exists, else provisioned), the
Deployment, the Services. Keys are still today's shared per-(dataset, mode)
pair, used as each VersityGW process's root account — per-user keys are the
next step. Deleting a dataset archives it (below); a CR deleted with kubectl
has its server pruned and keeps claim and policies.
The PV is `Retain` + `user-data=dataset`; the uninstall hook retains it and
releases the claim, and a reinstall re-binds it when the Dataset comes back.

**Archived datasets, 2026-10-08.** Deleting a dataset in the portal sets
`spec.archived` rather than removing the record. An archived dataset is
served to nobody — `s3_proxy_peers` admits no one, `session_shared_datasets`
mounts nothing, the managed server is pruned — but keeps its name, its grants
(the access grid still lists it, labelled, because saving the grid writes
every row it shows) and, if managed, its claim and data. Two things can
happen to it:

- **Resurrect** clears the flag. The server comes back on the same claim with
  the same keys (measured: 7s on k3d, file intact). This needed one fix: kopf
  raises no update for it, because no handler ran for the archive, so
  nothing was recorded, and the restored spec equals what kopf last saw. The
  dataset worker now also revives any served managed dataset whose
  Deployment is missing (`revive_dataset_servers`), on every Dataset event
  and the 300s backstop — which also heals a server deleted by hand.
- **Delete for good**, offered only for an archived dataset, behind a modal
  that wants the name typed (checked again server-side). The portal marks
  the record (`whistler/delete-data`); the operator (`purge_dataset`) reads
  it *live* and acts only if it is both archived and marked: stop the pods
  and wait until they are gone (fences must outlive pods), PV to Delete,
  claim, then policies, Services and keys, the record last. An S3 dataset's
  bucket is never touched.

A name held by an archived dataset cannot be re-used: re-creating it would
hand a new definition the old one's grants and data. An archived dataset
cannot be edited either; resurrect it first. Verified end to end through the
portal's own routes on k3d, including the refusals.

**Instances choose their datasets, 2026-10-08.** Until now every dataset a
user was granted in the session's zone was mounted, chosen or not. Now an
instance names the ones it wants — `overrides.datasets`, set on the create
and edit forms — and the start dialog can change the choice for one run
(`runOverrides.datasets`), by the same defaults-versus-this-run rule as every
other override (`session_requested_datasets`). It rides the overrides but is
not one: no override grant gates it, `_apply_overrides` ignores the key, and
the start dialog opens for anyone with a dataset to choose. A dataset is
mounted only if it is chosen **and** granted in the zone the session runs in;
the forms offer what the user holds in any zone (labelled with where and
how), because the zone can change per run. Nothing chosen means nothing
mounted — including for instances that existed before this, which mounted
everything granted. Verified on k3d that the choice survives the API server
in both slices (the Session schema is structured, so `datasets` had to be
added to it or it would have been pruned silently).

Limitation, not new: dataset mounts are written by cloud-init, which re-runs
on every start only for a fresh root — a containerDisk `image`, which is what
devbase and the desktops use. A template with a persistent root (`imageURL`,
a CDI DataVolume) keeps the instance-id across starts, so cloud-init applies
`write_files` on the first boot only, and a changed choice does not reach it.

Verified on a throwaway k3d cluster (local-path, kube-router's NetworkPolicy
enforcement) and against VersityGW v1.7.0 in Docker:

- Granted rw in zone `default`: write and list through the rw Service. Not
  granted ro: the ro port refuses the connection — the matrix, exactly. An
  ungranted namespace holding *both* valid keys: refused on both ports.
- Revoking or granting by editing only the User CR takes effect in ~2.6s
  measured from the client, `kubectl exec` overhead included.
- `--readonly` refuses PutObject, DeleteObject and CreateBucket with **403**
  (rclone's read-only proxy answers 500) — on top of the claim being mounted
  `readOnly` in that container.
- Delete keeps the PV and the data; re-creating as `readOnly` runs one
  container, drops the rw Service, fences rw to nobody, and serves the old
  file. Deleting the claim and re-creating the dataset re-attaches the same
  PV.

What the server taught us:

- **A bucket VersityGW did not create cannot be "created" again.** A `mkdir`'d
  bucket directory has no ACL xattr, and CreateBucket on it is a 500
  (`get bucket acl: no such key`). Object reads and writes inside it work.
  rclone sends CreateBucket before its first upload, so the guest's
  `rclone.conf` now says `no_check_bucket = true` — harmless for the rclone
  proxies, whose bucket always exists too.
- **An unknown access key is a 404** (`XAdminUserNotFound`), not a 403;
  rclone reports it as "directory not found". A wrong secret is
  `SignatureDoesNotMatch`, anonymous is `AccessDenied`. Every request is
  signature-checked, which is what will let deleting a per-user account cut
  an open mount — **after VersityGW's IAM cache expires** (`--iam-cache-ttl`,
  default 120s). Phase 2 should lower or disable it.
- **Sessions need their own egress carve-out to the server.** The baseline
  allow let session pods reach `app=whistler-s3-proxy` on 8080 only, so a
  real guest's mount of a managed dataset **hung** (dropped, not refused) —
  the k3d probes above sat in namespaces with no zone egress policy and
  could not have shown it. The baseline now also allows
  `app=whistler-dataset-server` on 8080 *and* 8081: NetworkPolicy sees the
  pod port after the Service translates it, and the rw Service's 8080 lands
  on 8081. Found on k3s-metal, 2026-10-08.
- It runs fine as uid 1000 on a read-only root filesystem, multipart
  included; multipart parts land in `.sgwtmp/` inside the bucket, which
  listings hide. The image's entrypoint passes arguments straight to the
  binary, and it ships a shell, which the bucket-bootstrap init container
  uses.
- With no `--iam-*` flag it runs in single-account mode, and the guest holds
  that root key. Harmless at phase 1 — root of a process confined to one
  dataset's claim, behind a policy — but per-user keys (phase 2) mean the
  root key must never leave the pod.

**Revocation was lazy; fixed 2026-10-08.** The matrix lives on User and Group
CRs and saving one changed no policy: a removed cell reached a proxy only when
some session, anyone's, was next built, so on a quiet cluster a revoked user
kept their reach indefinitely. And a deleted Dataset's proxy (fenced, not
deleted) still admitted every user whose cells named it, since nothing removes
those cells. Now `s3_proxy_peers` admits nobody to an undefined dataset, and
the operator re-fences every existing proxy (`refence_dataset_proxies`,
driven by the proxies' own policies so a dataset deleted with kubectl is
covered) on any User, Group or Dataset event — stateless `on.event` handlers
waking one debounced worker, with a 300s backstop. Still not cut: an
established connection, if the CNI's conntrack lets it outlive the policy.
Per-user keys are the answer to that.

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
