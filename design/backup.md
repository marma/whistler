# Backup and restore of state

**Proposed 2026-09-27.** How Whistler's *state* (the Custom Resources and
Secrets it reads its configuration from) survives an uninstall, a
reinstall, and a lost cluster, and how an admin manages all of that from the
portal. Backing up **user data** (the contents of home disks and datasets) is
a separate problem, left for later. What this document does cover is making
sure that data is still there after an uninstall and **re-attaches** to its
owner after a reinstall.

## What this is for

- **Uninstall means uninstall.** After `helm uninstall` (or Flux removing
  the release, which usually deletes the namespace too) no Whistler
  namespaces, CRs or Secrets are left behind. The only things that remain
  are the users' disks and the backups.
- **Reinstall is a normal operation.** Reinstalling to clear out bad state
  must not mean recreating users, groups, zones and templates by hand. The
  first admin to sign in after an install is offered the latest backup.
- **Everything is in the portal:** back up now, schedule, retention,
  download, upload, restore, delete.
- **Repeated install/uninstall cycles are idempotent.** The tenth reinstall
  behaves like the first. No duplicate volumes, no empty homes provisioned
  beside the real ones, no backups overwritten by an empty fresh install.

### Considered and rejected: a state namespace

The previous version of this document (2026-09-26) moved durable state into a
namespace that Helm is told to keep, so a reinstall would need no restore.
It was rejected because it makes a redeployment superficial: an uninstall
that leaves state behind is not an uninstall. It hides the problem instead
of making state portable. This version makes state portable instead. Every
install starts empty, and the backup is the one mechanism for carrying state
across, whether that is to the same cluster after a reinstall or to a new
one.

## What survives an uninstall, as built and as proposed

| | Today | Proposed |
| --- | --- | --- |
| Release namespace (`whistler`) | deleted by Flux, kept by plain `helm uninstall` | deleted either way (a pre-delete hook cleans up what Helm does not own) |
| Users, Groups, Zones, Templates, Datasets | lost with the release namespace | deleted; **in the backup** |
| SSH CA, gateway host key, dataset credentials | lost with the release namespace | deleted; **in the backup** |
| `whistler-user-*` namespaces (HomeVolumes, Sessions, per-user Secrets) | survive: nobody deletes them | deleted by the hook; **HomeVolumes and Sessions in the backup** |
| Home disks (PVs) | survive only while their namespace does. Deleting the namespace deletes the PVC, and under a `Delete` reclaim policy that deletes the data | **always survive**: Retain policy, re-attached on reinstall |
| Backups | nothing to lose | **always survive**, on their own retained PV |
| CRDs | survive (Helm never deletes them) | unchanged. Deleting them afterwards is safe, since nothing is lost that is not in a backup |

**The principle: two kinds of thing outlive an install — PVs holding user
data, and one PV holding backups. Both are cluster-scoped, both are Retain,
and both get re-attached by the same code.** Everything namespaced is
disposable.

## Where the backup volume goes

A PVC cannot be the thing that survives, because it lives in a namespace and
namespaces are what an uninstall removes. The **PV** is cluster-scoped and,
with `persistentVolumeReclaimPolicy: Retain`, outlives its claim. It then
sits `Released`, with a `claimRef` pointing at a claim that no longer exists.
Each install creates a new claim and **rebinds** it to that PV. This is the
same mechanism home disks need, so it is built once (Phase 1) and used for
both.

Three ways to supply it, in `whistler.backup.volume`:

| Mode | How it is found again | For |
| --- | --- | --- |
| `existingVolumeName: <pv>` | by name: a static PV the admin created (e.g. an NFS export path) | **production.** Stable, obvious, and on storage whose lifecycle is not tied to the cluster |
| `storageClassName: <sc>` (default: the cluster default) | by label: on first bind the operator labels the PV `whistler.martinmalmsten.net/backup-volume=true` and sets `Retain` | dev and single-cluster installs |
| `enabled: false` | not at all | backups off: the portal shows no Backups page and never offers a restore |

Setting Retain in the operator, instead of requiring a Retain storage class,
is deliberate. The reclaim policy of a bound PV can be changed, and relying on
the admin having picked the right storage class is exactly the kind of quiet
precondition that turns into data loss.

**Where it physically ends up is the question that decides disaster
recovery**, and it has no single answer:
- On **NFS** (production today: `csi-driver-nfs`), the backups are on the NFS
  server, outside the cluster. They survive losing the cluster, and a static
  PV (`existingVolumeName`) is the natural fit.
- On **in-cluster storage** (Longhorn, Rook, local-path), the backups die
  with the cluster. They still cover every reinstall, but disaster recovery
  needs a copy elsewhere. That copy is what **download** is for, and the
  Backups page says so when the storage class is known to be in-cluster.
- An **S3 target** would cover the in-cluster case properly. It is a natural
  later mode for `whistler.backup.volume`, not in this plan.

Size: state is kilobytes to a few megabytes per backup. `1Gi` is years of
daily backups.

### Who mounts it: a small backup service

The volume is mounted by a new single-replica Deployment,
`<release>-backup`, running the same image (`python -m whistler.backup
serve`). The portal does not mount it, for three reasons:

- **A broken backup volume must not take the admin UI down.** A pod whose PVC
  cannot bind stays `Pending`. If that pod were the portal, the admin would
  lose the only place to see why. Instead the portal shows "backup service
  unavailable" and the claim's events.
- **One mounter works on any access mode.** An RWO volume can be mounted by
  one pod, on one node. The scheduler, the uninstall hook's final backup,
  uploads and downloads all go through the one pod that holds it.
- **It isolates the CA key.** Backups include the SSH CA's private key
  (below). The process that holds files containing it should be neither the
  internet-facing portal nor anything else that is always running.

The portal talks to it over an in-cluster Service. It authenticates by
passing its ServiceAccount token, which the backup service checks with a
**TokenReview**, accepting only the portal's and the uninstall hook's
ServiceAccounts. A NetworkPolicy additionally limits ingress to those pods.
Downloads and uploads are streamed through the portal, so the backup service
is never exposed outside the cluster.

The claim itself is created by the **operator**, not rendered by Helm. The
operator owns PV discovery and rebinding (Phase 1), and a Helm-rendered claim
cannot know at render time which Released PV to bind to (`lookup` does not
work under `helm template`, i.e. Argo). Until the claim exists the backup pod
is `Pending`, which is correct.

## What a backup is

One file, `whistler-backup-<UTC timestamp>-<trigger>.tar.gz`, in the volume's
root:

```
manifest.json    format version, Whistler version, created, trigger
                 (manual | scheduled | uninstall | pre-restore | uploaded),
                 install id, object counts, sha256 of each member,
                 WHISTLER_SSH_DOMAIN_SUFFIX, whether secrets are encrypted
state.yaml       the CRs: plain Kubernetes objects, kubectl-applyable
secrets.yaml     the durable Secrets, or
secrets.enc      the same, encrypted with the backup passphrase
```

`state.yaml` holds ordinary objects reduced to `apiVersion`, `kind`,
`metadata.{name,namespace,labels,annotations}` and `spec`. It omits status,
uid, resourceVersion, managedFields and ownerReferences. Objects are sorted
by kind, namespace and name, with keys sorted, so unchanged state produces an
identical file. That makes two backups diffable, and it lets the scheduler
**skip a backup whose content hash matches the previous one**, so a quiet
install does not fill the volume. Objects that belong in the release
namespace are written *without* a namespace, so they restore into whatever
the target release namespace is called. User-namespace objects keep theirs,
because `whistler-user-<name>` is a fixed pattern.

### What is in it

| Object | Why |
| --- | --- |
| User | identity, grants, the access matrix, admin flag, **uid/gid** (the files on a surviving home disk are owned by them) |
| Group, Zone, Template, Dataset **not** rendered by Helm | admin decisions made in the portal |
| HomeVolume, with the **bound PV's name** recorded as an annotation | the name → disk binding. It is what re-attaches a home, see Phase 1 |
| Session | the user's instances: template, overrides, `homeVolume` |
| Backup settings (schedule, retention) | so a restore brings them back as well |
| SSH CA, gateway host key, Whistler-managed dataset credentials | without them a restore works, but every client meets new keys and every Whistler-managed dataset loses its credential |

**Secrets are included by default.** Without the CA and host key the one
thing a reinstall should be invisible to, users' SSH, breaks. They are
**encrypted when a backup passphrase is set** (scrypt-derived key,
AES-GCM, from `cryptography`, which asyncssh already pulls in). The
passphrase is stored in a Secret so scheduled backups can use it. That Secret
is *not* backed up. It is typed in again at restore time, which is exactly
the property wanted: someone holding a copy of the backup volume, or a
downloaded file, does not hold the CA. Without a passphrase, secrets are
stored in plaintext, and the Backups page says so in the same place it offers
to set one.

### What is not in it

| Object | Why not |
| --- | --- |
| Helm-rendered CRs (`app.kubernetes.io/managed-by: Helm`) | they are in values, in git. Restoring them would fight Helm on the next upgrade |
| Pods, VMs, Services, NetworkPolicies, S3 proxies | derived: the operator rebuilds them from the CRs |
| cloud-init Secrets, session host certs, VM access keys, S3 proxy auth | derived, regenerated on demand. Restored sessions are stopped, so their next boot carries the new ones |
| Secrets a Dataset names with `credentialsSecret` | the admin created them outside Whistler. Restore warns when one is absent |
| Session run state (`whistler/last-connect`, `whistler/last-stop`, `spec.runOverrides`) | **restored sessions are stopped.** With neither mark, `run_intent` answers stopped. A restore that booted every VM at once would contend for GPUs and hugepages nobody has checked are free |
| The backup passphrase | see above |

## Restore

### What restore does

1. **Takes a `pre-restore` backup** of the current state first, so every
   restore can be undone with another restore.
2. **Verifies:** manifest format (refuse a newer format than this Whistler
   understands, migrate an older one), checksums, the CRDs present
   (`crd_missing_hint` text if not), the passphrase if secrets are encrypted.
3. **Previews:** a dry run listing each object as *create*, *replace* or
   *unchanged*. Nothing is written until the admin confirms.
4. **Applies**, create-or-replace by name, in this order: Zones, Groups,
   Users, Templates, Datasets, Secrets, then each user's namespace
   (`_ensure_user_namespace`), HomeVolumes, and Sessions last.
5. **Reads everything back and compares specs.** A field that did not survive
   the write was pruned by a CRD that is older than the backup. That is the
   silent failure CLAUDE.md warns about under "`helm upgrade` never updates
   CRDs", and a restore is where it would otherwise stay invisible. Any
   mismatch is listed, with the `kubectl apply -f crds.yaml` fix.
6. **Records the result** in the install record (below) and resumes the
   schedule.

**Restore never deletes.** Objects in the cluster that are not in the backup
are left alone. On a fresh install the only such object is the bootstrap
admin, and it is replaced if the backup has a user of the same name.
Deleting "what the backup does not know about" would make restoring an old
backup destroy everything made since, which is not something one button
should do.

Restoring the CA and gateway host key has consequences that restore handles
itself:
- **The gateway watches its host key Secret and exits when it changes.** The
  kubelet restarts it with the restored key, and the portal needs no RBAC on
  Deployments.
- **Session host certs are reissued** once `needs_reissue` compares signing
  keys (Phase 0). Running sessions, which are rare right after an install,
  are listed as needing a restart.

### The first-login offer

An install needs an identity that a reinstall changes and an upgrade does
not. The chart renders a ConfigMap `<release>-install` with no data. **Its
metadata uid is the install id.** Helm deletes it on uninstall and creates a
new one on install, while `helm upgrade` keeps it. The decision is stored
beside it, in a ConfigMap `<release>-install-state` that the backup service
writes: `{installId, decision: pending|restored|declined, backup,
decidedBy, decidedAt}`. A decision recorded for any other install id counts
as `pending`.

This works identically whether or not the release namespace was deleted,
which is the point: it does not guess "fresh install" from what happens to
be in the cluster.

While the decision is `pending` and the volume holds at least one backup:
- **An admin's first navigation in the portal lands on the restore offer**:
  the newest backup, with its date, Whistler version, trigger and counts,
  plus a list of the others.
  - *Restore* goes through the preview above.
  - *Start fresh* records `declined`.
  - *Not now* dismisses the offer for this browser session only. It does not
    record a decision.
- **The schedule is paused.** Otherwise a fresh, empty install would back
  itself up and, under retention, eventually delete the backups it should
  have restored.

A non-admin never sees the offer. On a fresh install the bootstrap admin is
normally the only account anyway.

## Scheduling and retention

Configured on the Backups page, with defaults from values
(`whistler.backup.schedule`, `whistler.backup.retain`). The portal's setting
wins once saved, and is itself part of the backup.

- **Schedule:** *off*, *every N hours*, or *daily at HH:MM UTC*. Cron syntax
  is not needed for backups of kilobytes, and not needing it avoids a
  dependency.
- **Retention:** keep the newest *N* scheduled backups **of the current
  install**. Manual, uninstall, pre-restore and uploaded backups, and
  everything from a previous install, are never deleted automatically. Only
  an admin deletes those. This rule is what makes repeated reinstalls
  harmless: nothing a reinstall does can age out the backup it came from.
- **Freshness warning:** the admin dashboard warns when the last successful
  backup is older than twice the interval, or the last attempt failed. A
  backup job that stopped quietly is the usual way backups fail.

## Uninstall

A Helm `pre-delete` hook Job (`python -m whistler.uninstall`, operator
ServiceAccount, which still exists at that point):

1. **Sets `Retain` on every Whistler PV** (homes and the backup volume),
   whatever the storage class says. If this fails, **the uninstall fails**.
   It is the step that turns deleting a namespace from data loss into
   tidying up.
2. **Asks the backup service for an `uninstall` backup.** If that fails, the
   uninstall fails too, unless `whistler.uninstall.requireFinalBackup: false`.
   Without it, state changed since the last scheduled backup is lost with no
   warning.
3. **Deletes the `whistler-user-*` namespaces**, and the CRs and Secrets the
   operator created in the release namespace, which Helm does not own and a
   plain `helm uninstall` would otherwise leave behind.
4. **Deletes the backup claim**, leaving the PV `Released` for the next
   install.

`helm uninstall --no-hooks` and Flux's `uninstall.disableHooks` skip all of
this. What is left then is today's behaviour, and the next install must cope
with it. That is part of what "idempotent" means below.

## Idempotency, stated as rules

Each of these is a test in the plan.

1. **One backup volume, ever.** The operator looks for an existing backup PV
   (by name, or by label) *before* provisioning, and rebinds it if it is
   `Released`. It provisions only if none exists. If several match, it binds
   the one holding the newest backup and the Backups page lists the others.
2. **No empty home next to a real one.** A HomeVolume carrying a recorded
   PV name is **never** provisioned from scratch. If its claim is missing, the
   operator rebinds that PV. If the PV is gone too, the session fails with
   "the disk for home volume X (PV …) no longer exists" in
   `status.statusMessage`. Today `ensure_home_volume_pvc` creates a new,
   empty claim whenever the old one is gone, which after a full uninstall
   would give every restored user an empty home while their real one sits
   `Released`. That silent substitution is the one outcome this plan cannot
   allow.
3. **The same backup restored twice** gives a second preview of all
   *unchanged*, and writes nothing.
4. **The offer comes once per install**, keyed on the install id, not on
   guessing from the cluster's contents.
5. **The schedule cannot write over a restore it has not had.** It is paused
   while the decision is pending, and retention never touches another
   install's backups.
6. **Surviving objects are fine.** If namespaces survived (`--no-hooks`, or a
   plain uninstall today), restore reports them *unchanged* or *replace*,
   rebinding finds claims still `Bound` and leaves them, and nothing is
   duplicated.
7. **Keys are stable across a cycle when the backup has them.** CA
   fingerprint and gateway host key after uninstall → reinstall → restore
   equal those before.

## Implementation plan

Each phase is independently shippable and leaves the tree working.

### Phase 0: prerequisites (small, independent)

**Done 2026-09-27.** `hostca.needs_reissue(..., ca_public=)` and
`hostca.signed_by`, passed the current CA by `ensure_session_host_cert`; and
`server.watch_host_key`, which polls the Secret every 30s, compares *public*
halves (re-exported OpenSSH private keys never match byte for byte), treats a
missing or unreadable Secret as no change, and exits with status 3.
Tests: `tests/unit/test_hostca.py`, `tests/unit/test_host_key_watch.py`.

- **Fix `hostca.needs_reissue`** to reissue when the cert's signing key is
  not the current CA's ([hostca.py:152](../whistler/hostca.py#L152)). Today,
  after a CA change, sessions keep presenting old-CA certs until their
  renewal window. This is a bug on its own, and restore depends on the fix.
  *Test:* a cert signed by a different CA needs reissue.
- **The gateway exits when its host key Secret changes** (watch it after
  startup, `sys.exit` on a changed key), so a restore takes effect without
  Deployment RBAC.

### Phase 1: re-attachable volumes

**Done 2026-09-27**, with three changes from the plan below:
- **No kopf timer.** kopf puts a finalizer on every object a timer watches,
  and a finalizer only a running operator can remove would hang the very
  namespace deletion this is about. A plain thread started from the
  operator's startup handler sweeps every 300s instead (`secure_user_data`).
  The reconcile path also secures a claim it finds bound.
- **Discovery by the stale `claimRef`.** A Released PV still names the claim
  it belonged to, so no label is needed to find one. That covers the pod home
  (`whistler-data-<user>`, which has no CR to record on) and a home secured
  before it was recorded. The labels (`whistler.martinmalmsten.net/user-data`,
  `…/user`) are for the uninstall hook and for people.
- **Deleting with data is declarative.** The portal had no `delete` on PVCs,
  so "also delete the data" had never worked from the portal. Now it
  annotates the HomeVolume (`whistler/delete-data`), and the operator sets
  the PV to Delete, deletes the claim, then the CR (`purge_home_volume`).
  The portal gains no PV rights.

Verified on a throwaway k3d cluster (local-path, a Delete class): Retain
set, namespace deleted, both PVs Released, both claims bound back to the
same PVs with the files intact; a recorded PV that was gone was refused
with no claim created; delete-with-data freed the PV.

- `config.py`: `ensure_retained(pv)` and
  `rebind_released_pv(pv, namespace, claim)`. The latter creates the claim
  with `spec.volumeName` and patches the PV's `claimRef` to the new claim
  (namespace + name, no uid). Pure manifest builders, unit-tested like
  `_build_pod_spec`.
- `ensure_home_volume_pvc`: after first bind, record the PV name on the
  HomeVolume (`whistler/pv-name`), label the PV
  (`whistler.martinmalmsten.net/home-volume=<user>.<name>`) and set Retain.
  On a missing claim: rebind the recorded or labelled PV, else provision
  **only if nothing was ever recorded**, else fail as in rule 2.
- `adopt_legacy_home_disks` gains the same labelling and Retain for existing
  homes, so installs from before this change are covered from their next
  operator start.
- `delete_home_volume(delete_data=True)` must now delete the **PV** as well.
  Under Retain, deleting the claim no longer frees the disk.
- RBAC: the operator gets `get/list/watch/patch` on `persistentvolumes`.
- *Tests:* rebind manifests; never-provision-after-record; delete with data
  removes the PV. *Integration:* delete a user namespace, start the session
  again, the same bytes are on the home.

### Archived homes (added 2026-09-27, between Phases 1 and 2)

An admin can **archive** a home volume: take it away from its user without
destroying it. A claim cannot change namespace, but a retained PV can change
claims. So the operator marks the PV (label `user-data=archived`, no user
label, provenance as JSON in `whistler.martinmalmsten.net/archived`), deletes
the user's claim, and binds the PV to a claim in **`whistler-archive`**
(`WHISTLER_ARCHIVE_NAMESPACE`). It sits there beside a HomeVolume record that
has no user and a `spec.archived` provenance block. The user's record and
their access cells go.
- **From the archive**, an admin can restore a home to *any* user under any
  free name (a new HomeVolume with `spec.fromArchive`; the operator moves the
  disk and clears the field), or delete it with its data (the same
  `whistler/delete-data` path as a user's home). A restored home is granted
  no zone.
- **Refused** while the volume is attached, or while an instance would attach
  it at its next start. That includes an instance with no home chosen whose
  default home is this volume: archiving from under it would give it a fresh
  empty default home at the next start.
- **An archived PV is never given back to a user's claim.** Its stale
  claimRef still names the claim it came from, so without this rule a new
  volume of the same name would quietly pick it up.
- **The PV is the record that survives.** The sweep's `recover_archive`
  rebinds every Released, Retain, archived PV into the archive namespace and
  recreates its record. That covers an uninstall, and an archive interrupted
  after the user's claim was deleted.

For the later phases: the uninstall hook (Phase 5) deletes `whistler-archive`
along with the user namespaces, and the backup (Phase 2) exports its
HomeVolume records like any other.

Verified on a throwaway k3d cluster: archive → the user's list is empty, the
same PV is Bound in `whistler-archive` → a new volume of the same name gets
a new empty PV → deleting `whistler-archive` and running recovery rebuilds it
→ restoring to another user under another name, that user reads the
original file.

### Phase 2: export and restore as a library

**Done 2026-09-27.** `whistler/backup/`: `archive.py` (the file, pure),
`export.py` (what goes in it), `restore.py` (plan, apply, read back), and
the CLI in `__main__.py`. Differences from the plan below:
- **The encrypted member is `secrets.enc`**, a JSON envelope (scrypt,
  AES-256-GCM), not age. `cryptography` is now a declared dependency, where
  before it came in through asyncssh.
- **`restore` previews unless given `--apply`.** The backup arrives on
  stdin, so the CLI cannot ask for confirmation, and the default is the
  choice that writes nothing. `--pre-restore PATH` saves the current state
  first. The backup service (Phase 3) will always do that.
- **A replace keeps what `normalize` drops and nothing else**: kopf's
  bookkeeping, and a running Session's run marks and `runOverrides`. That is
  what makes a second restore all *unchanged*, and why a restore does not
  stop a running session.
- **A Helm-managed object in the target is skipped**, not replaced. Its
  values decide it.
- The operator Deployment now sets `WHISTLER_HOST_KEY_SECRET_NAME`, so a
  backup taken there carries the gateway host key.

Verified on a throwaway k3d cluster with the real `KubeConfigManager`:
populate, export encrypted, **delete the namespaces and the CRDs**, re-apply
the CRDs, restore. The second export had the same `contentHash`, and a
second restore was all *unchanged*. The Session came back with no run marks,
the uid survived, the CA Secret decrypted, and the user namespace got its
zone policies. A wrong passphrase gave a clear error.

- `whistler/backup/archive.py`: build and read the tar.gz (manifest,
  deterministic `state.yaml`, secrets plain or encrypted), content
  hash for skip-if-unchanged.
- `whistler/backup/restore.py`: verify → preview → apply → read-back compare,
  in the order above. Pure planning functions (backup + current objects →
  create/replace/unchanged list) so most of it is unit-tested without a
  cluster.
- Sessions exported without run marks. HomeVolumes exported with the PV name.
  Secrets exported by **role** (`ssh-ca`, `server-host-key`,
  `dataset-credentials`) and restored to the names *this* install uses, since
  those contain the release name.
- A CLI (`python -m whistler.backup export|restore`) on the same code. It
  costs nothing, and it is the way in when the portal is what is broken.
- *Tests:* byte-stable export; exclusions; ordering; role → name mapping under
  a different release name; restore twice = no writes; pruned-field detection.

### Phase 3: the backup service and volume

**Done 2026-09-27.** `whistler/backup/service.py` (aiohttp API and the
scheduler), `schedule.py` (the rules, pure), `store.py` (the directory),
`state.py` (install id, decision, settings, passphrase), `config.
ensure_backup_claim`, and the chart's `templates/backup.yaml` plus the
`backup:` values. Differences from the plan below:
- **The backup PV is labelled `whistler.martinmalmsten.net/user-data=
  backups`**, the same label homes use (value `home`/`pod-home`). The
  uninstall hook then selects everything that must survive with one key.
- **Several retained backup volumes: the newest by creation time is bound**,
  not "the one holding the newest backup". The operator cannot read their
  contents, and rule 1 means a second one only exists if someone made it.
  The others are logged.
- **Settings are not yet in the backup.** They live in a ConfigMap, and the
  backup format carries CRs and Secrets only. After a reinstall the chart's
  `backup.schedule`/`backup.retain` apply until someone saves them again.
  Open.
- **Found live, not by the unit tests:**
  - Kubernetes injects `WHISTLER_BACKUP_PORT=tcp://…` for a Service named
    `whistler-backup`, which was the listen-port variable. It is now
    `WHISTLER_BACKUP_LISTEN_PORT`, and the pod has `enableServiceLinks:
    false`.
  - A freshly created claim's PV kept its class's `Delete` policy until the
    next 5-minute sweep. Creating or re-binding any claim now wakes the sweep
    (`secure_soon`), and an unbound claim brings it back in 15s.
- **A pending decision with nothing from another install to offer is
  recorded as `fresh`** by the service, so this install's own backups are
  never offered back to it.

Verified by installing the real chart on a throwaway k3d cluster:
- **The claim.** The operator created it, and the PV was set to Retain and
  labelled.
- **The API**, called with the portal's own token: status, back up, list.
- **Refusals.** The gateway pod was refused by the NetworkPolicy (connection
  refused, `/healthz` included). A pod wearing the portal's label with another
  ServiceAccount got 403 from TokenReview; no token got 401. The operator was
  allowed.
- **A GitOps-style reinstall** (`helm uninstall` plus deleting the namespace):
  the PV went Released, the reinstall bound the **same** PV (still one PV in
  the cluster), the old backup was on offer, and the schedule was paused
  although due. Restoring through the API brought the user back with its uid,
  and the SSH CA byte for byte. The gateway restarted onto the restored host
  key (Phase 0). The decision was recorded with the caller, and the schedule
  resumed.
- **A second cycle:** the offer came back, all backups intact, the same PV.

- `python -m whistler.backup serve`: an HTTP API for list, create, download,
  upload, delete, preview, restore, settings and status. TokenReview auth. The
  scheduler loop lives here, with the pause-while-pending and retention
  rules.
- The operator ensures the backup claim at startup (Phase 1 functions,
  `whistler.backup.volume` modes).
- Chart: Deployment, ServiceAccount, Service, NetworkPolicy, RBAC (read all
  Whistler CRs and the durable Secrets; write the same plus user namespaces;
  `tokenreviews create`), the `<release>-install` ConfigMap, and values.
- *Tests:* scheduler rules as pure functions (next run, retention selection,
  pause). *Integration:* back up, delete the CRs, restore, back up again, and
  the two state hashes match.

### Phase 4: the portal

**Done 2026-09-27.** `whistler/portal/backups.py` (the client), the Backups
routes and `restore_offer_middleware` in `management.py`, and
`templates/admin/backup{s,_restore,_offer}.html`. How it behaves:
- **The client sends the pod's ServiceAccount token, re-read every call**,
  because projected tokens rotate. It reports two kinds of failure.
  *Unavailable* (unreachable, or refused at 401/403) becomes a message on the
  backup pages, plus the claim's phase when that is why. *Refused* (a 400)
  shows the service's reason. A down backup service never becomes an error
  page anywhere else.
- **The offer is a middleware.** On a GET navigation (not htmx, not a fetch)
  by an admin, outside `/admin/backups`, `/static`, `/login` and `/logout`,
  while the service says `offer`, it redirects to `/admin/backups/offer?next=
  …`. The answer is cached 30s per portal process and dropped after a
  restore, decline, upload or delete. It **fails open**: no answer means no
  offer. "Not now" is a session cookie holding the install id, so it cannot
  hide the next install's offer.
- **Restore is one page**: a preview first, then an optional passphrase, the
  "include secrets" choice, and Preview again / Restore. The result names
  the pre-restore backup, and any fields the cluster dropped with the fix.
- **The admin overview** has a Backups card and warns when the service is
  unavailable, an offer is waiting, or the schedule is stale.
- **Storage inside the cluster is flagged** by a driver heuristic
  (`in_cluster_storage`: local, hostpath, longhorn, rook/ceph, openebs, …),
  worded as a warning, not a guarantee.

Verified through the real portal on a throwaway k3d cluster:
- **First install:** set a passphrase and took a backup; it showed as
  encrypted. It downloaded with `attachment` and `no-store`.
- **After a GitOps-style reinstall:** the admin's navigation to `/dashboard`
  was sent to the offer. A non-admin was not, and nor was a fetch. "Not now"
  held in that browser only.
- **The restore preview** warned that the secrets need a passphrase this
  install does not hold. A wrong passphrase was refused. The right one
  restored the user (with uid) and the SSH CA byte for byte, after a
  pre-restore backup.
- **Afterwards** the offer was gone. Uploading the downloaded file gave an
  `…-uploaded` backup, and random bytes were refused with the reason.

- **Admin → Backups:** volume status (PV, storage class, capacity, bound
  state; an in-cluster-storage warning), list (date, trigger, install,
  version, size, encrypted), Back up now, Download, Upload, Delete, Restore
  (preview → confirm → result, including the pruned-field list). Settings:
  schedule, retention, passphrase set/change.
- **The first-login offer** in `require_user`'s admin path, as above.
- **The freshness warning** on the admin dashboard.
- *Tests:* portal route tests against a fake backup service, the same way the
  existing portal tests fake the config manager.

### Phase 5: uninstall

- The `pre-delete` hook Job and `python -m whistler.uninstall`, with the four
  steps and their failure rules.
- README: what an uninstall now removes and keeps, how to delete a retained
  PV for real, and `--no-hooks`.
- *Integration:* the full cycle, run **twice**: populate → uninstall →
  install → accept the offer → assert users, grants, keys and home bytes;
  repeat. The second cycle is the idempotency test.

## Open questions

- **Several backup PVs found** (rule 1): bind the newest automatically, or
  make the admin choose? Automatic is proposed, since the portal lists the
  others and an admin can switch.
- **Upload size limit and format checks:** uploads come from admins, but the
  archive reader should still treat a file as untrusted input (member names,
  sizes, decompression ratio).
- **S3 as a backup target**, for in-cluster storage and real disaster
  recovery: a later `whistler.backup.volume` mode, sharing everything above
  the storage layer.
- **Multi-replica portal:** unaffected, since the scheduler lives in the
  backup service. The backup service itself stays at one replica.
