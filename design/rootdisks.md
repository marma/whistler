# VM root disks

**Pinned 2026-09-27**, to be picked up after backups (design/backup.md).
Nothing here is decided except the goal.

**The goal: a VM's root disk is ephemeral, and fresh on every restart.** What
a user keeps lives on their home volume (design/storage.md, and the access
matrix in design/security.md). The root is the image, and only the image.
That makes "what survives an uninstall" (design/backup.md) a question about
homes alone. It also makes an instance's behaviour a function of its template
rather than of its history.

This document records how root disks work today, where that differs from
the goal, and the ways forward considered so far.

## How it works today

A VM template names its boot source with one of two mutually exclusive
fields. `_apply_policy` refuses a template that sets both, and whichever is
set must be in `whistler.images.vm`.

### `image`: a containerDisk (ephemeral, fresh per start)

`image` is an OCI image wrapping a qcow2 (`_build_vm_spec`,
`{"containerDisk": {"image": ...}}`). KubeVirt pulls it onto the node and
runs the guest on a **copy-on-write overlay inside the virt-launcher pod**.
The overlay is discarded when the VMI ends, and a Whistler stop ends it:
the operator halts the VM (`runStrategy: Halted`), which deletes the VMI.
So:

| Event | Root disk afterwards |
| --- | --- |
| Stop and start (portal, launcher `s`/`S`, a guest power-off: see CLAUDE.md, "Shutting down gracefully") | **fresh**, back to the image |
| `reboot` inside the guest | **unchanged**: qemu resets the domain, the VMI lives on, the overlay with it |
| Node failure / VMI eviction, restarted under `RerunOnFailure` | fresh |
| Uninstall | nothing to keep |

This is **every template in use**: the Selkies desktops (`vm-xfce-selkies`,
`vm-gnome-selkies` and their `-cuda` variants), all three `devbase*`, and
`ubuntu-vm` / `ubuntu-vm-gpu`. They are baked by the qemu-in-docker pipeline
(`make devbase-image`, `desktops/*/build.sh`). Tag conventions matter for
freshness in a different sense, a stale cached image: see the memory note on
the containerDisk tag cache (`<name>[-cuda]:latest`, no explicit
`imagePullPolicy`).

**`persistence: persistent` does not mean a persistent root.** On a template
it describes the *session* (preemptible or not, cleaned up on disconnect or
not; `ensure_session`). The `devbase*` templates say `persistent` and boot
from a containerDisk, and their roots are fresh on every start.

### `imageURL`: a CDI-imported root PVC (persistent)

`imageURL` is an HTTP(S) qcow2/raw URL. `_build_vm_spec` adds a
`dataVolumeTemplates` entry named `<session>-root` (`source: http`, size
`rootDiskSize`, default 20Gi), and the root volume is that DataVolume. CDI
imports the image into a PVC in the user's namespace, **once per instance**.

- **Owned by the VirtualMachine.** It survives stop/start: the VM object
  stays, halted, and so does the DataVolume. It is deleted when the instance
  is deleted.
- **The operator reads its state.** `_probe_vm_without_vmi` checks the
  `<name>-root` DataVolume, and `VM_ERROR_STATUSES` maps `DataVolumeError`
  and `ErrorDataVolumeNotFound` to a Failed session with a reason.
- **Supporting pieces:** RBAC `get/list/watch` on `cdi.kubevirt.io`
  `datavolumes`, and the CRD fields `imageURL` and `rootDiskSize`.
- **Used by one template:** `ubuntu-vm-cdi` in `values-dev-vm.yaml`. The
  portal's template form does not offer `imageURL`; it is reachable only from
  chart values or `kubectl`.

**This is the one path that contradicts the goal.** The root persists for the
life of the instance, so two instances of one template diverge, and a
user's changes to `/` survive restarts.

### What the uninstall hook does with them (as of Phase 5)

`whistler/uninstall.py` retains only homes, archived homes and the backup
claim. A `<session>-root` PVC is deliberately **not** retained, because
nothing would re-attach it. It is deleted with the user namespace, unless its
storage class retains it, in which case it becomes an orphan PV.

This is a **change** from before Phase 5. Until then nothing deleted user
namespaces on uninstall (neither `helm uninstall` nor Flux), so CDI roots
survived an uninstall, by accident. Under the goal above that is correct
behaviour, not a loss. It is recorded here so nobody rediscovers it as a
regression.

## Where it differs from the goal

1. **`imageURL` roots persist** across restarts (above).
2. **An in-guest `reboot` keeps the overlay** on a containerDisk too. "Fresh
   on every restart" holds for Whistler's stop/start, not for a reboot from
   inside the guest.

## Ways forward

### For `imageURL`

**A. Remove it.** Every root becomes a containerDisk. A template naming
`imageURL` is refused at policy time, with a message pointing at the bake
pipeline. This deletes:
- the `dataVolumeTemplates` branch of `_build_vm_spec`;
- the DataVolume probe in `_probe_vm_without_vmi`, and its two
  `VM_ERROR_STATUSES`;
- the `datavolumes` RBAC;
- `imageURL` and `rootDiskSize` from the CRD;
- the `ubuntu-vm-cdi` template.

The cost is no longer booting straight from a vendor's cloud-image URL:
someone has to wrap it as a containerDisk first. That is a Dockerfile of a
few lines, and the bake pipeline already produces every image in use.

Existing instances of an `imageURL` template: their VM still names the
DataVolume, so either migrate them (delete and recreate from a
containerDisk template), or keep the read path long enough to report
"template no longer supported" instead of an opaque KubeVirt error. The CRD
field removal needs the usual `kubectl apply`, and an existing CR carrying
`imageURL` is then pruned. That is exactly a template losing its boot source
silently, so the policy refusal should come a release before the field goes.

**B. Keep it, fresh per start.** Import each URL **once into a golden PVC**
(per template, keyed by a hash of the URL so a changed URL re-imports).
Every session then boots from a KubeVirt **`ephemeral` volume** on that PVC:
the PVC as a read-only backing image, with a throwaway overlay, the same
semantics as a containerDisk. There is no per-start download and no
per-session PVC. What it adds:
- **golden-image lifecycle**: import, re-import on change, garbage-collect
  images no template names, and "importing…" as a session state;
- **access mode**: running VMs from one golden PVC on several nodes needs
  ReadOnlyMany or ReadWriteMany. NFS (production) has it; local-path does not;
- **where the golden PVC lives** (the release namespace? a dedicated one?) and
  who may read it, since user namespaces cannot mount a claim in another
  namespace. This probably means one golden PVC per user namespace (cloned),
  or KubeVirt's cross-namespace DataVolume clone, which needs its own RBAC.

B is a real feature. A is a deletion. A is recommended unless there is a
concrete need to boot vendor images unmodified.

### For an in-guest reboot

**C. Leave it.** Document that a reboot from inside the guest keeps the
root until the instance is stopped. This matches KubeVirt's model: qemu
resets the domain, and the VMI lives on.

**D. Make a guest reboot a restart.** The guest powers off instead of
rebooting: a systemd `reboot.target` override in the baked images, or
`reboot` → `poweroff`. The operator then starts it again. The graceful
shutdown work already turns a guest power-off into a `whistler/last-stop`
(`_guest_powered_off`), so this needs a way to tell "power off" from
"reboot, please", e.g. a marker the guest leaves via the guest agent, or a
dedicated exit path. It costs a re-bake and one more state to get right, and
the benefit is a subtle one.

C is recommended; revisit D if someone relies on a reboot to clean a guest.

## Things to check before choosing

- **Where containerDisk writes land.** The overlay is in the virt-launcher
  pod's ephemeral storage, i.e. **the node's disk**. Whistler sets no
  `ephemeral-storage` request or limit on VMs, so a guest filling `/` (a
  large `apt install`, a build in `/tmp`) fills the node, up to kubelet
  eviction thresholds. With fresh roots this is the only place root writes
  go, so it needs a limit (and templates sized for it) whichever option is
  chosen.
- **Whether anything relies on a persistent root today.** In-guest state
  that should be on the home (dotfiles in `/root`, `/opt` installs, per-host
  data) disappears on every start. The devbase design (pixi environments,
  CUDA SDK in the image) assumes the image is the toolchain and the home is
  the work, so it should be fine, but check with users of `ubuntu-vm-cdi`, if
  there are any.
- **Identity survives a fresh root.** It does: the per-session host key and
  certificate come from a Secret via cloud-init (`ensure_session_host_cert`),
  and cloud-init's `ssh_deletekeys` already treats every instance as new. A
  fresh root does not change the host a client sees.
