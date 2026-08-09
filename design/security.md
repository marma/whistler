# Security model: users, volumes, zones, groups

**Status: design sketch. None of this is implemented.** Zones exist today as
egress postures ([`whistler/config.py`](../whistler/config.py), `Zone` CRs);
volumes, taints, security levels and groups do not. This document records the
model and, more importantly, *why* each piece is shaped the way it is, so the
implementation doesn't quietly drop the parts that carry the guarantee.

Claims marked **verified** are a narrower thing: the *mechanism* was exercised
against the real gateway image and behaved as described. That says the
primitive exists and does what the model needs — not that Whistler uses it.

## The problem

The driving scenario is a lab that gives visiting researchers access to
digitized material. Some data — behind APIs and on volumes — must never be
reachable from a session that also reaches the internet. Researchers are
onboarded in an open zone and *then* moved to a restricted one, so the move has
to be possible; but after that move their working directory has seen restricted
material and must never again be mounted somewhere with internet access.

The requirement is therefore about **history as well as concurrency**.
Forbidding two sessions from sharing a directory *at the same time* does not
close it: the same user can mount the same home in the open zone an hour
later, so whatever enforces this has to remember where a volume has been. But
the converse is equally true and was missing from an earlier draft of this
document, which treated history as sufficient — it is not. A volume live in
two zones at once is a live bridge between them, and no amount of remembering
closes that while it is happening. Both invariants are needed; see rules 1–4.

The secondary scenario is data-science staff who legitimately need both the
internet and the restricted data. That must be expressible as policy, not as a
special case in code.

## Assumptions

These are not incidental; the model is built on them.

- **The guest is the user's machine.** VM sessions grant the session user
  passwordless root ([`whistler/cloudinit.py`](../whistler/cloudinit.py)), and
  the credentials for the home share live in the guest. This is a deliberate
  choice, not an oversight. On a project instance it is the *members'*
  machine, plural, and every member is trusted with root over it — so members
  are not isolated from each other there. That is a scoping decision, made
  explicitly in [Shared instances](#shared-instances-and-shared-volumes).
- **Therefore nothing enforced inside the guest counts.** Every boundary must
  be server-side: storage the session cannot authenticate to, or a gateway its
  zone cannot route to.
- **The server bounds the identity set; the guest chooses within it.** This is
  the shape every storage guarantee below takes, and it is worth stating on its
  own because it is weaker than it first looks and stronger than AUTH_SYS
  suggests. The gateway decides *which identities a session can write as* — a
  fixed set, one export each. Which of them a particular write uses is decided
  in the guest, and is therefore not a boundary. What no guest can do is
  produce a write as an identity outside the set, because no export exists that
  would emit one.
- **Withholding `sudo` is not a boundary either** — see [User](#user). It is a
  guideline and a development tool, and the model must never lean on it.
- **Subdirectory mounts are not a boundary.** The storage gateway exports one
  tree ([`images/storage-gateway/ganesha.conf.template`](../images/storage-gateway/ganesha.conf.template),
  `Path = /shares/home` published as `Pseudo = /home`) and the client picks
  which path beneath it to mount. A directory layout is organisation. It is not
  isolation, and it must never be documented as if it were. (This was equally
  true of the SMB gateway it replaced, where `mount.cifs` took the path after
  the share name as a client-side *prefixpath*.)
- **Out of scope:** a malicious administrator. The model constrains mistakes
  and ordinary users, and makes deliberate overrides visible.

### Substrate: the move to NFS (done)

The home was served over SMB; it is now **NFSv4.2** from a per-user
NFS-Ganesha gateway ([`images/storage-gateway/`](../images/storage-gateway/)),
moved primarily to be rid of `nobrl` (see
[Session state](#session-state-cache-config-state)). That changed what the
enforcement primitives are, and the change is not neutral:

- SMB authenticated **per user with a password**, so shares could be scoped by
  credential and a session could simply not hold the secret for a share it may
  not see.
- NFS (AUTH_SYS) has no such thing. The client *asserts* its uid, and the guest
  is root, so it asserts whatever it likes. Squashing (`Squash = All_Squash` +
  `Anonymous_Uid`/`Anonymous_Gid` set to the user's real ids) pins on-disk
  ownership the way `force user` did — verified: a write by guest uid 4242
  lands as the user's uid — but it is **not authentication**. Nor is the
  transport encrypted, as it was under `seal`.

So the boundary now rests entirely on **which export exists, and who can reach
it**: today one export per user, ingress-fenced by NetworkPolicy to that user's
own session pods in that user's own namespace
(`_build_gateway_network_policy`); under this model, per-boundary exports and
per-zone gateways. That is a workable boundary — arguably cleaner than
credential distribution — but it is a *different* one, and **no design below
may assume "the session doesn't have the password"**: there is no longer a
password. Kerberos (`sec=krb5`, and `krb5p` for privacy) would restore
per-principal authentication and encryption if that turns out to be needed.

## Core model: taint plus security level

Two fields carry the whole guarantee.

- A **volume** carries a **taint**: the set of zones it has been mounted in
  writable.
- A **zone** carries a **security level**, `0`–`100`, expressing how restricted
  it is.

From which:

1. Mounting volume `V` into a session in zone `Z` is **forbidden** when
   `Z.securityLevel < max(level(z) for z in V.taint)` — i.e. you may not carry
   a volume down the gradient.
2. A **writable** mount imprints: `V.taint ∪= {Z}`.
3. A **read-only** mount does **not** imprint. Restricted data cannot flow
   *into* a volume that can't be written, so it cannot become a carrier.
4. **Concurrency invariant.** At any instant, over the zones `V` is *currently*
   mounted in: `min(level(z) for z in mounted) >= max(level(z) for z in
   mounted-writable)`. In words: a volume may not be readable somewhere lower
   than it is writable, *right now*. Read "mounted" as **published to a running
   instance** — an actual mount happens inside a guest, where the operator
   cannot see it, and a rule enforced against something unobservable is not a
   rule (see
   [Mounting homes on demand](#mounting-homes-on-demand)).

**Rules 1–3 alone leave a live hole, and it is not a subtle one.** Start an
instance in the open zone: `V.taint = {open}`. Now start a second instance in
the restricted zone: rule 1 permits it (`100 >= 0`), and rule 2 sets
`V.taint = {open, restricted}`. The volume is now mounted writable in both at
once. Restricted material written in the second instance is readable, this
second, from the first — which has internet egress. Rule 1 will refuse the
*next* open-zone mount, so the model self-corrects on reboot; it does nothing
about the window, and by then the data has left.

Rule 4 closes it, and closes it at the right end: the mount that *creates* the
conflict is refused, rather than tearing a mount out from under a session that
was already running and did nothing wrong. It also leaves the onboarding case
(consequence 2 below) intact — writable in open, read-only in restricted gives
`min(0, 100) = 0 >= max(0) = 0`, which passes.

Rule 4 is **evaluated at instance start, not at instance creation**. A created
but stopped instance holds no mount and must neither imprint nor constrain
anything; taint is recorded when the volume is actually attached. Evaluating it
at creation is what produced the hole above.

Three consequences worth stating explicitly, because they are the reason to
prefer this shape over a set of rules about homes and migrations:

- **The one-way property is free.** There is no "migrate" verb and no
  `migrated: true` flag to get wrong. An admin moves an instance from open to
  restricted; the next boot mounts its volumes there; they are imprinted; they
  are now permanently ineligible for the open zone. The ratchet *is* the
  monotonic label.
- **Onboarding works without a hole.** Rule 3 lets a researcher's open-zone
  home be mounted read-only inside the restricted zone — bring your notes in,
  don't take anything out — and it stays usable in the open zone afterwards.
  This must be a read-only *share*, enforced by the gateway; a read-only mount
  option chosen by a guest with root is worth nothing.
- **The data-science case is just a zone.** A zone that permits internet egress
  and still carries a high security level lets staff keep one home and reach
  both. What they lose is the ability to take that home back down to open —
  which is correct, and falls out of rule 1 without a special case.

**Clearing a taint is an explicit, audited operation**, not a configuration
option. A flag that permits crossing gets set once and forgotten; a
declassification action leaves a record and stays rare. That is the intended
balance between enforcing the rule and letting an administrator override it.

**Zones must stay behaviour-free in code.** The level is a number and the
policy is data; no zone name may acquire meaning in the implementation. Whether
a deployment calls its zones open/restricted/internal is the deployment's
business.

## User

Users are `User` CRs (`usr`) today, admin-managed through the portal, with
allow-lists and per-session override grants.

The change this model asks for is that **the home stops being special**. Today
each user gets exactly one per-user PVC for `$HOME`
([`whistler/config.py`](../whistler/config.py), `_ensure_pvc`), created
implicitly. Under this model a home is an ordinary volume that happens to be
bound to a user and mounted at `$HOME`, and a user may own several. Instance
creation chooses which one to use as the home.

That single change removes the need for a rule about homes: a user working in
two zones ends up with two home volumes not because a policy says "one home per
zone" but because the taint rule makes a single shared home impossible. The
mechanism is the same one that governs every other volume.

Existing `User` fields (allowed zones, override grants) stay as they are. What
a user may mount comes from the volume/zone rules and, later, from group
membership.

### `sudo` rights

Grantable **per user and per instance**, and explicitly *not* a security
boundary. Recorded here so nobody later mistakes it for one.

Its two real purposes:

- **Without `sudo`**, a nudge rather than a wall: users install project
  dependencies into user space instead of the instance root, so their work
  survives an image rebuild instead of evaporating with the root filesystem.
- **With `sudo`**, a development tool: someone building a new environment can
  experiment with the global setup live, and only bake an image once the recipe
  has settled — instead of a rebuild per iteration.

Neither purpose is confidentiality, and no rule in this document may depend on
a session lacking root. A user without `sudo` in a VM they otherwise control
should be assumed able to get it; the guarantees have to hold anyway.

## Volume

A new primitive, and the place the guarantee actually lives.

- **Created explicitly** in the admin interface, backed by a PVC, with an
  optional explicit storage class.
- **Owned** by a user or (later) a group.
- **Fields**, at least: name, owner, size, storage class, `taint[]`, access
  mode, and a pinning/shareability property (below).
- **Taint lives on the `Volume` CR**, not as an annotation on the PVC: it has to
  outlive any particular PVC, and it is policy, not implementation detail.
- **Taint is recorded by the operator at mount time**, before the session
  starts — not by anything in the guest.

**Every volume mounted in a session is imprinted, not just the home.** Omitting
this makes the scratch-volume copy-out the trivial bypass: mount a clean
volume in the restricted zone, copy, remount it in the open zone.

**Pinned vs shareable.** Distinct from taint, and the thing "no shared home
directories" was reaching for: a volume may be *pinned* to a single instance, or
be of a kind that can be attached to several. A zone can require that everything
mounted in it be pinned. This is a property of the volume's kind — "can this be
shared at all" — not a statement about two sessions running at once.

**Enforcement.** A `taint` field the operator consults is defeated by anyone who
can reach a gateway from another zone, since the guest has root. The label is
the policy; the mechanism has to be one of:

- per-boundary **gateway pod**, with the zone's NetworkPolicy making the others
  unroutable — the only one of these still available now that the substrate is
  NFS, and therefore the one to design for. Its finer-grained form, one
  *export* per identity rather than one gateway per boundary, is what
  [Shared instances](#shared-instances-and-shared-volumes) is built on; or
- ~~per-boundary **share with its own credentials**~~, which the SMB gateway
  could have done and **NFS/AUTH_SYS cannot** — see
  [Substrate](#substrate-the-move-to-nfs-done). Recorded as closed, not as an
  option.

Neither the field nor the mechanism works alone.

### Session state: cache, config, state

Per-session desktop state is where volume layout, the concurrency hazards and
the taint model all meet. The split:

- **`XDG_CACHE_HOME` never goes on a shared volume.** It is discardable by
  definition, it is where most of the concurrency hazard lives (SQLite indexes,
  thumbnails, font and shader caches, browser disk caches), and network storage
  is bad at exactly its many-small-files pattern. An `emptyDir` on node
  ephemeral storage — not memory, since caches grow — or the equivalent scratch
  disk for VMs.
- **`XDG_CONFIG_HOME` and `XDG_STATE_HOME` must persist**, and can co-exist
  between sessions to a degree — much of it is idempotent or last-writer-wins
  in ways nobody notices. `~/.config/dconf/user` is the known exception: dconf
  rewrites the whole database on any change and coordinates through a
  machine-local flag file, so two sessions on two hosts silently clobber each
  other's settings.
- **One PV per instance for config/state** is preferred over having the gateway
  selectively export one instance's subdirectory from a shared PV. The
  selective-export version is possible but puts the isolation back in the
  gateway's path handling, which
  [Assumptions](#assumptions) says is not a boundary. A separate volume gets the
  same taint logic as everything else for free, and one fewer special case.

The move to NFS removed the need for `nobrl` and with it the cross-host locking
hole — SQLite-backed state (browser profiles, keyrings) stopped being a
corruption risk when two sessions share a home. That was a fix for a real bug,
not a security control, and the two should not be conflated: it bought no
isolation, and it cost the per-share credential
([Substrate](#substrate-the-move-to-nfs-done)).

## Zone

Zones already exist as named egress postures: one egress-only NetworkPolicy per
user namespace, pods selected by a zone label stamped at build time, unknown
zones failing closed, `default` always present, changes taking effect on reboot.
All of that stays.

What this model adds to the `Zone` CR:

- **`securityLevel`** (`0`–`100`) — the gradient in rule 1.
- **A pinning requirement** — may this zone mount shareable volumes at all.
- Possibly: permitted storage classes, and whether read-only cross-level mounts
  are allowed here.

The egress posture and the data posture are two faces of the same object, and
that is the point: a zone is low-level *because* it reaches the internet. Two
separate primitives would let them drift apart.

**Live edits.** The current split — an edited zone re-fences running sessions in
place, while zone *membership* changes need a restart — is worth preserving as
is; instant re-fencing is a feature. A change to `securityLevel` is the awkward
case, since it can retroactively invalidate a mount that is already live, but it
does **not** have to take effect instantly: flag the affected running instances
as needing a reboot (and surface a warning at edit time), then let the rule
apply at next boot. A level change that killed live sessions would be worse than
the exposure it closes, and an unreadable "why did my session die" is worse than
a visible "this instance must restart".

## Group

A `Group` CR holding users and shared settings — available volumes and zones,
and the explicit cross-level mount override (`allowCrossMount` or similar)
referenced in the Volume section.

The field that carries weight is **membership with a per-volume access mode**:
for each volume the group can reach, which members get `rw` and which get `ro`.
That list is not merely a UI affordance — it renders directly to the gateway's
export list, which is where the enforcement lives
([Shared instances](#shared-instances-and-shared-volumes)).

Still to design: naming, nesting (probably none), and who may edit a group.

## Shared instances and shared volumes

A **project instance** is one session several members connect to. It mounts a
shared project volume, and each member's own home. Everything is on one guest,
with one IP and one kernel, and any member may hold root on it. This section
says what the server can still guarantee there, because it is the case where
the single-user mechanisms stop applying and the temptation to hand-wave is
strongest.

This section is about **storage** and holds wherever members have their own
logins. What it takes to give them those logins — separate desktops, on demand,
on one machine — is
[Project instances](#project-instances-several-members-one-machine-separate-desktops),
and none of it exists yet. The mechanisms here do **not** apply to a
shared-screen session (one Selkies pipeline broadcast to many viewers), which
runs as a single Unix user and therefore has a single identity by construction.

The requirement is deliberately *not* member-versus-member isolation. It is
**containment of the identity space**: a project instance may write as any of
its members — including a member impersonating another, including root — and
must never be able to produce a file owned by anyone outside the project.

### The mechanism: one export per member, all onto the same directory

For a volume shared by `{alice, bob, carol}` the gateway publishes three
exports with the **same `Path`**, differing in `Export_Id`, `Pseudo`,
`Filesystem_Id` and `Anonymous_Uid`/`Anonymous_Gid`, each `Squash =
All_Squash`. The guest mounts all of them.

```
EXPORT { Export_Id = 1; Path = /shares/proj; Pseudo = /as/alice;
         Squash = All_Squash; Anonymous_Uid = 1001; Anonymous_Gid = 5000;
         Access_Type = RW; Filesystem_Id = 1.1; ... }
EXPORT { Export_Id = 2; Path = /shares/proj; Pseudo = /as/bob;    ... 1002 ... }
EXPORT { Export_Id = 3; Path = /shares/proj; Pseudo = /as/carol;
         Access_Type = RO; ... }
```

Identity is therefore a property of **which export a write goes through**, not
of anything the client asserts and not of where it comes from. The set of
exports *is* the whitelist; adding a member is adding an export. Nothing here
depends on file modes, on the client's asserted uid or gid list, or on the
guest behaving.

**Verified** against [`images/storage-gateway/`](../images/storage-gateway/) —
one guest, one source IP, root in the guest:

| Guest action | On-disk result |
| --- | --- |
| write via `/as/alice` | `1001:5000` |
| write via `/as/bob` | `1002:5000` |
| write asserting uid 4242 | `1001` — squashed |
| `chown` to a non-member (1009) | stays `1001` |
| `chown` to root | stays `1001` |
| write via `/as/carol` (`RO`) | refused, "Read-only file system" |
| client not in any export | mount refused outright |

**Which export a given process writes through is the guest's business.** The
intended arrangement is a per-login mount namespace (`pam_namespace` or
equivalent) putting that member's export at a fixed path. That is
**convenience, not a boundary** — root can write through any of the mounts —
and it must never be documented as if it were. It doesn't need to be a
boundary: acting as another member is explicitly permitted.

### One mountpoint, and ordinary POSIX modes inside it

The arrangement to aim for is that the shared volume is at **the same path for
everyone** — `/data` — with permissions inside it expressed as ordinary file
modes: a `0700` subdirectory is that member's own, a group-writable one is the
project's. This is not asking for a security guarantee. It is asking that the
obvious thing be true, so people do not leak things to each other by accident;
root defeats it and that is fine.

Per-login mount namespaces give this directly: every member's `/data` is the
same path, bound from their own export. And because `All_Squash` sets the
credential the server *evaluates access with* — not merely the ownership it
stamps on writes — the modes are enforced by the server, which is better than
the requirement asks for. Verified, one directory tree, two members, guest root
asserting a junk uid throughout:

| Subdirectory of `/data` | alice | bob |
| --- | --- | --- |
| `shared` — `2770 alice:project` | read + write | read + write |
| `alice-private` — `0700 alice:project` | read + write | **denied** |

**The limit: one group per member, per volume.** A squashed credential is
exactly `(Anonymous_Uid, Anonymous_Gid)` with **no supplementary groups**, and
`Manage_Gids` does not change this — verified explicitly, with and without it,
against a directory owned `9999:secret` mode `0770` where only a supplementary
group membership could have opened it. Both settings denied a member who is in
`secret` in the gateway's own group database. So a subtree inside `/data`
group-owned by a *subset* of the project is not expressible.

**Express subgroups as volumes, not as groups inside a volume.** A subgroup
gets its own volume with its own export set, published only to its members, and
mounted at its own path. That costs the single-mountpoint property for that
subtree and needs no new mechanism at all — it is the same rule as everything
else here, that the published export set is the access-control decision. The
alternative that would preserve one mountpoint is dropping the squash for
pass-through uids plus `Manage_Gids`, which does restore full multi-group
POSIX — and immediately forfeits identity containment, since the client then
asserts its own uid and can assert a non-member's. That trade is the whole
reason [Kerberos](#open-questions) exists as an option: it is the only way to
have both.

### Why not the obvious alternative

The first design that comes to mind is one export with pass-through uids
(`Squash = Root_Squash`), `Manage_Gids = true` so the server resolves each
uid's groups from its own database instead of trusting the client's list, and a
volume root at `2770 root:project`. A forged non-member uid then fails the
directory check, because the server won't agree that uid is in the group.

It nearly works, and it is rejected because the containment rests on **mode
bits on every directory in the volume**. One member creating a `0777`
subdirectory reopens the hole, silently and legitimately. Guest-settable state
carrying a guarantee is exactly what [Assumptions](#assumptions) refuses. The
export-set version has nothing to misconfigure: a non-member uid is not
expressible.

### A home mounted in several instances at once

**A home volume can be mounted in several running instances simultaneously**,
and the model must treat that as normal rather than exceptional. It is what
rule 4 governs, and it is why the volume's *kind* (pinned vs shareable) is a
field rather than an assumption.

What holds: every consumer goes through one gateway, so byte-range locks are
coherent across instances — the same guarantee verified across exports applies
across sessions, because it is the same server. That is the property `nobrl`
used to destroy, and it is what makes a concurrently-mounted home merely
awkward rather than corrupting.

What does not hold is anything that assumes a single writer:
[Session state](#session-state-cache-config-state) stops being a preference and
becomes a requirement. `~/.config/dconf/user` is the sharp edge — dconf rewrites
the whole database on any change and coordinates through a machine-local flag
file, so two live sessions silently clobber each other's settings, and no
storage-layer fix reaches it. `XDG_CACHE_HOME` off the shared volume, and
config/state on a per-instance volume, are what make concurrent mounts usable.

### Per-user homes on a shared instance

Same mechanism — each member's home is its own volume, exported once with that
member's identity, mounted at `/home/<member>`. Keeping members' processes out
of each other's homes is then the guest's job, and a member with root can read
another member's home.

**This is accepted, not prevented**, and it is the single most likely thing for
a user to assume is false. It should be visible in the UI when a project
instance is created, not buried here. If it ever must be false, the mechanism
is Kerberos: per-uid GSS contexts are the only way one host holds several
*verified* identities at once, and going there also means no root on the
instance, since root can read another member's credential cache.

### What carries over from the single-user case, and what doesn't

- **Locking survives.** A whole-file `F_WRLCK` taken via `/as/alice` was
  refused via `/as/bob` — same server, same file, shared state. The SQLite
  property the NFS move bought is intact on a shared volume.
- **But only with one gateway pod per volume.** NFSv4 lock state is per-server;
  several ganesha processes over one filesystem would not see each other's
  locks and would reintroduce exactly the corruption the move to NFS removed.
  Many exports, one server.
- **`Filesystem_Id` must differ per export.** With the same fsid on two exports
  of one path the client misbehaved. Verified the hard way.
- **Serve every consumer through the gateway.** A pod session mounting the PVC
  directly does not participate in ganesha's lock state, so a volume shared
  between gateway-served VMs and direct-mounting pods loses coherence again.
  As a consequence of routing everything through one gateway, the PVC is
  mounted exactly once and RWO suffices — gateway-mediated sharing does not
  need RWX storage.
- **File modes are guest-chosen.** A member's umask decides whether their files
  are group-writable, so collaboration depends on guest configuration; the
  server-side fix is NFSv4 ACLs with an inheritable ACE
  (`Disable_ACL = false`) rather than mode bits.
- **Membership changes are not retroactive.** Removing a member stops new
  writes as that identity; their existing files stay theirs. This interacts
  with declassification, where "who wrote this" now has several answers.

### Interaction with taint

Taint is unaffected: it ranges over **zones, not users**, so a volume several
members can reach imprints exactly as any other volume does, at mount time, by
the operator. Multi-member changes nothing about rules 1–3.

The general statement the export-set model licenses is that **the set of
exports published to an instance is the access-control decision** — it encodes
which identities that instance may write as *and* whether each volume is `rw`
or `ro` there. Rule 3 then needs no separate machinery: an instance that may
only read a volume is simply published an `RO` export of it and no `RW` one.

## Project instances: several members, one machine, separate desktops

**Status: further out than anything above, and not designed — this records what
it would take.** The storage model in
[Shared instances](#shared-instances-and-shared-volumes) is the part that is
worked out; everything here is the machinery around it.

**The shape.** One VM. Several members, each with their own login, their own
desktop session on their own display, and their own home — running at the same
time. This is deliberately *not* the shared-screen case: Selkies broadcasts one
pipeline to many clients with `controller`/`viewer` roles, which is a different
feature (screen sharing) and is set aside. Here every member gets a real seat.

Two constraints stated up front, because they shape the rest: **a member who is
not using the instance should cost nothing.** No home mounted for someone who
has not logged in, no Xvfb and no Selkies running for someone who has not
opened a desktop. Provisioning all of it at boot would make a ten-member
project instance pay ten times over to serve one person.

### What already fits

- **The session unit is already per-member.** `whistler-desktop@.service` is a
  template with `User=%i`; running several instances of it is what it was
  written for.
- **Per-member storage identity is solved** — one export per member, `/data` at
  one path via per-login mount namespaces, POSIX modes enforced server-side
  ([above](#one-mountpoint-and-ordinary-posix-modes-inside-it)).
- **A guest control path exists.** The portal already holds a per-user VM access
  key and drives SSH into running guests for screenshots, so there is a way to
  act inside a booted VM without inventing an agent.

### What is single-tenant today

Each of these is a concrete artifact that assumes exactly one user per session.

- **The streamer.** `whistler-streamer.service` is a plain unit, baked enabled,
  `WantedBy=multi-user.target`, one Xvfb on `DISPLAY=:0`, one `SELKIES_PORT`
  read from `/etc/whistler/streamer.env`. It has to become
  `whistler-streamer@<member>.service` with a per-member display and port, and
  `whistler-desktop@%i` has to want *its own* streamer instance rather than the
  singleton it names today.
- **Display and port allocation.** Needs to be deterministic — the operator, the
  guest and the portal must agree without negotiating. Allocate from a **stable
  per-member slot recorded on the instance**, not from position in the member
  list, or adding a member renumbers the displays of everyone after them.
- **cloud-init.** Creates one user, enables one desktop unit, writes one
  `streamer.env`. It would need the whole member list: N users with N primary
  groups, N `authorized_keys` drop-ins, N units. Size is not the problem (the
  userData already travels via a Secret, so the 2048-byte inline cap does not
  apply); the problem is that cloud-init is **boot-time only** — see membership
  changes below.
- **The Service and the readiness probe.** `_build_session_service` publishes a
  single display port, and readiness gates on it
  (`readiness_port = display_port if desktop_stream else 22`). With streamers
  started lazily, *no* display port is listening at boot, so **readiness has to
  become sshd**, and the Service has to publish N ports.
- **The portal proxy.** `_resolve_desktop_base` maps a session to one
  `displayPort`, and `get_user_desktop_sessions(user)` scopes sessions to their
  single owner. Both assume one user per session. It needs
  `(session, member) -> port`, and it must refuse to route a member to another
  member's port. **This is the access control for the display plane** — there is
  nothing else enforcing it, since all the streamers listen on the same guest.

### Mounting homes on demand

Do not mount every member's home at boot. The guest-side mechanism is cheap:
`x-systemd.automount` on the per-member fstab entries, so the kernel triggers
the mount on first access, with `x-systemd.idle-timeout` to drop it again. The
entries are inert until touched, and a member who never logs in never causes an
NFS session.

The part that actually matters is on the other side. **Taint must be imprinted
when the export is published, not when the guest mounts it** — the operator
cannot observe a mount that happens inside a guest, and a rule enforced against
something it cannot see is not a rule. What the operator does control is which
exports it publishes to an instance.

So lazy mounting is only honest if **publication is lazy too**: the operator
adds the member's export when that member first connects, and imprints the
taint at that moment. The conservative alternative — publish and imprint every
member's home at instance start — is safe and defeats the purpose. This also
sharpens rule 4: "currently mounted" should be read as "currently published to
a running instance", which is the operator's own state.

### Starting streamers on demand

An Xvfb plus a Selkies per member is the expensive part, especially with GNOME
on llvmpipe or a single passed-through GPU, so a streamer should start when its
member first opens the desktop. Three ways, in descending order of how much
they need to be proven:

1. **systemd socket activation** on the member's port. The clean answer on
   paper, and it puts the trigger exactly where the first connection lands. It
   requires Selkies to accept a passed listening socket (`LISTEN_FDS`), which
   is unverified and probably absent — check before designing around it.
2. **Portal-driven start over the existing SSH path**, when the member opens
   the desktop. Uses machinery that already exists, at the cost of the portal
   knowing about guest internals.
3. **A small in-guest supervisor** the portal pokes. Most control, most new
   code, another thing to keep alive.

Stopping matters as much as starting: the streamer unit is `Restart=always`
today, which would fight any attempt to stop an idle one. A per-member streamer
wants `Restart=on-failure` plus an idle timeout, or an explicit stop when the
last client disconnects.

### Membership changes on a running instance

cloud-init runs at boot. Adding a member to a *running* project instance means
creating a user, a group, an `authorized_keys` drop-in, a mount unit and a
streamer unit inside the guest — none of which cloud-init can do afterwards.

The cheap and consistent answer is that **membership changes take effect on the
next boot**, which is exactly how zone membership already behaves. Adding a
guest-side agent to do it live is the alternative, and it should be a deliberate
decision rather than something that arrives by accident.

Removal is the sharper case and does *not* decompose the same way. Revoking a
member has one part that is immediate and server-side — withdrawing their
export, which stops writes as that identity at once — and one part that is not:
their `authorized_keys` and their running session live in the guest. A
revocation that only removes the export leaves a logged-in member with a live
desktop and no storage, which is a confusing failure rather than a safe one.

### What this costs, in the model's own terms

- **`sudo` becomes all-or-nothing.** On a shared machine, granting one member
  root gives them every other member's data on that box. The per-user,
  per-instance grant in [`sudo` rights](#sudo-rights) cannot mean on a project
  instance what it means on a single-user one. Either no member gets it, or
  every member is told plainly that all of them effectively have it.
- **The guest kernel becomes a boundary**, for the first time in this document,
  and [Assumptions](#assumptions) says it is not one. Members are separated by
  uid inside a single kernel, so a local privilege escalation is a cross-member
  breach. On single-user instances a kernel escape buys the attacker nothing —
  they already own the machine. Here it buys them the project. This has to be
  recorded as **accepted**, with the compensating control being that membership
  is small and known, not that the kernel holds.
- **Resource contention is a security-adjacent concern.** N desktops share CPU,
  RAM and one GPU, so a single member can starve the rest. Per-member cgroup
  limits, or an admission rule relating member count to instance size.
- **Attribution gets weaker.** Audit and declassification now have N candidate
  writers per volume ([Interaction with taint](#interaction-with-taint)).

### What this does *not* need

Worth stating, because each is a plausible-sounding detour:

- **Not Kerberos.** Per-member exports still give identity containment, and the
  members already share a kernel — the storage layer is not the weak link here,
  so buying cryptographic identity for it would be spending in the wrong place.
- **No new taint machinery.** One instance sits in one zone, so rules 1–4 apply
  unchanged; only the "published, not mounted" reading above is a refinement.
- **Not RWX storage.** The gateway remains the only mounter of each PVC.

## Open questions

- **An instance with no persistent home is a data-loss trap.** It is the
  cleanest onboarding path and worth having, but the failure mode is an admin
  forgetting to attach a home volume and a user's work vanishing at the next
  reboot, silently and unrecoverably. It needs a deliberate guardrail —
  instances default to requiring a home, "no persistent home" is an explicit
  choice, and the session says so visibly.
- Audit trail and UI for declassification.
- What `securityLevel` values mean in practice, and whether an unordered taint
  *set* is ever needed instead of a total order (two restricted collections
  that must not mix are not expressible as levels).
- **Kerberos, and the three things that would make it necessary.** Per-member
  exports cover the identity containment this model actually requires, so
  `sec=krb5` is not on the critical path. It becomes the answer — and the only
  answer — if any of these turns up:
  1. **Multi-group POSIX inside one volume.** Squashing pins one gid and drops
     supplementary groups; pass-through restores them and forfeits containment.
     Only real per-user credentials give both
     ([One mountpoint](#one-mountpoint-and-ordinary-posix-modes-inside-it)).
     Expressing subgroups as separate volumes is the cheaper way out, and is
     the current answer.
  2. **Wire encryption.** `krb5p` is the only route back to what `seal` gave
     under SMB. Independent of everything else here, and still open.
  3. **Member isolation that survives root**, which also means no root on the
     instance, since root can steal another member's credential cache.

  Investigating it means a KDC, principal and keytab lifecycle, clock sync, and
  `rpc.gssd` in every guest image — so it should be triggered by one of the
  three above, not adopted pre-emptively.
- **Export lifecycle.** Adding or removing a member, or flipping a volume
  between `rw` and `ro`, means editing exports and reloading ganesha. A reload
  path exists (SIGHUP, or the DBus `ExportMgr` interface — DBus is not wired up
  in the gateway image today), but neither has been tested. Everything in
  [Shared instances](#shared-instances-and-shared-volumes) assumes this works
  without dropping live mounts.
- **Project instances beyond storage.** The storage half is designed above; the
  rest is not. Whose `sudo` policy applies, who may connect to the session at
  all and how that is enforced (the SSH/console path, not the gateway), and
  what happens to a running project instance when membership changes.

**Resolved and folded in:** taint lives on the `Volume` CR; no migration path
for existing homes is needed (nothing is in production); zone level changes flag
instances for reboot rather than applying live; read-only enforcement is a
server-side `Access_Type = RO` export and needs no separate mechanism, since
the published export set already encodes it; multi-user and project-wide
instances are no longer deferred wholesale — their storage model is
[above](#shared-instances-and-shared-volumes).
