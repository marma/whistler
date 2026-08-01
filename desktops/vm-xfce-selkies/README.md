# vm-xfce-selkies — desktop VM containerDisk (XFCE + in-guest Selkies)

A KubeVirt **containerDisk** (OCI-wrapped qcow2, Ubuntu 26.04 cloud image)
with XFCE and the Selkies 2.x streaming stack **baked into the guest**. It
backs `runtime: vm` desktop templates with `viewer: websockets`
(`ubuntu-vm-selkies` in [values-dev-vm.yaml](../../charts/whistler/values-dev-vm.yaml)):
the portal reverse-proxies the guest's Selkies server exactly like a pod
desktop's sidecar — per-session Service → virt-launcher pod → masquerade →
guest `:8082`.

**Why baked, not cloud-init-installed, and not a sidecar:** the streamer
sidecar model cannot reach into a VM (unix sockets don't cross the VM
boundary), so VMs need an in-guest streamer. Installing it at boot is out:
containerDisk roots are ephemeral (every session boot would pay the full
install), the user-namespace egress policy blocks package mirrors by design,
and Selkies 2.x is an unreleased pinned-commit source build. So the bytes are
baked; cloud-init stays the per-session control plane (user/uid/keys, SMB
home mount, streamer env, session-unit start — see
[whistler/cloudinit.py](../../whistler/cloudinit.py)).

## What's inside the guest

- **`whistler-streamer.service`** (baked enabled, runs as root): Xvfb +
  PulseAudio + Selkies — a port of
  [`streamer-selkies2/entrypoint.sh`](../streamer-selkies2/entrypoint.sh),
  which stays the reference copy; keep them in sync. Reads optional
  per-session knobs from `/etc/whistler/streamer.env` (written by cloud-init
  from the template's `streamerEnv` + `displayPort`). The KubeVirt
  virtio-gpu/VNC console is *not* the desktop — it keeps showing the text
  console and stays available as the agentless rescue path (`viewer: vnc`
  semantics) even on this image.
- **`whistler-desktop@<user>.service`** (template unit, `User=%i`): waits for
  the streamer's X and the SMB home mount, then runs XFCE in the foreground —
  the in-guest analog of [`xfce-plain/entrypoint.sh`](../xfce-plain/entrypoint.sh).
  Only cloud-init knows the username, so the session's userData does
  `systemctl enable --now whistler-desktop@<user>` (enable so CDI
  persistent-root guests resume the desktop on later boots).
- The Selkies venv + web client are **extracted from the
  [`streamer-selkies2`](../streamer-selkies2/) docker build** at bake time:
  one `SELKIES_COMMIT`, client/server lock preserved, binary-compatible
  (both Ubuntu 26.04). The bake also installs `cifs-utils` (so the home mount
  uses mount.cifs instead of the raw-kernel fallback) and purges snapd
  (snapd.seeded otherwise delays every session boot ~30 s).
- **Ubuntu's default wallpaper** (`ubuntu-wallpapers`, which nothing in `xfce4`
  pulls in) instead of the stock Xfce mouse, via the guest's
  [`xfce4-desktop.xml`](guest/etc/xdg/xfce4/xfconf/xfce-perchannel-xml/xfce4-desktop.xml)
  — xfconf reads channel XML under `XDG_CONFIG_DIRS` as read-only defaults that
  `~/.config/xfce4/xfconf/…` shadows, so a user's own wallpaper still wins and
  persists in the SMB home. The backdrop property path embeds the RandR output
  name, which for Xvfb is always `screen`; keep it in sync with the pod copy in
  [`../xfce-plain/xfce4-desktop.xml`](../xfce-plain/xfce4-desktop.xml). A
  backdrop path that no longer exists is silent (xfdesktop just keeps its
  built-in one), so the bake `test -f`s the wallpaper.
- Unlike the pod images there is **no bwrap divert**: the VM has a full
  kernel and Ubuntu ships an AppArmor profile permitting bwrap's user
  namespace, so glycin's image-decode sandbox actually works here. If icons
  die with GTK pixbuf warnings, revisit this (see the symptom table in
  [design/creating_desktops.md](../../design/creating_desktops.md) §5).

## Build

```bash
make vm-desktop-image          # → localhost:5000/whistler-vm-xfce-selkies:latest  (lean, no GPU driver)
make vm-desktop-image CUDA=1   # → …-cuda:latest  (bakes NVIDIA driver + CUDA toolkit for passthrough sessions)
make vm-desktop-image PUSH=1   # …and push to the dev registry
```

**Why `:latest`, and why the variant is in the image name.** These dev images
are mutable — a rebuild overwrites the tag in place — and `:latest` is the only
tag Kubernetes and KubeVirt default to `imagePullPolicy: Always`. Anything else
defaults to `IfNotPresent`, so nodes silently keep booting the stale cached
qcow2 after a rebuild (a fixed image still showing the old bug, fixable only
with `crictl rmi` on the node). `:latest-cuda` would *not* match that rule,
which is why `CUDA=1` suffixes the image NAME instead. `_build_vm_spec` sets no
`imagePullPolicy` at all, so this defaulting is what's in force; production
should use immutable versioned tags, which correctly stay `IfNotPresent`.

The default image carries **no** NVIDIA driver — most sessions have no
passthrough GPU, and the driver is dead weight (plus a first-boot install over
the egress-locked guest net) on them. `CUDA=1` bakes the open driver **and the
CUDA toolkit** (`nvcc` + cuda runtime libs; several GB, so the CUDA build gets a
bigger disk) in and publishes it as `whistler-vm-xfce-selkies-cuda`; only GPU
templates (e.g. `ubuntu-vm-selkies-cuda` in
[values-dev-vm.yaml](../../charts/whistler/values-dev-vm.yaml)) pull that image.
`CUDA_TOOLKIT_PACKAGE` overrides which toolkit (default: the archive
`nvidia-cuda-toolkit`; empty for driver-only).

The driver is **not** display-neutral: it brings `libnvidia-encode`, and
Selkies' `--use-cpu` defaults to false, so on a `-cuda` passthrough session
pixelflux encodes the stream on **NVENC** rather than software x264 — even
though `--encoder=x264enc` (an output-mode label, not an implementation) is
unchanged. XFCE's rendering is llvmpipe either way; only the encode stage moves.
See the comment in
[`guest/usr/local/bin/whistler-streamer`](guest/usr/local/bin/whistler-streamer)
and [`../vm-gnome-selkies/README.md`](../vm-gnome-selkies/README.md#streaming-is-h264--but-not-x264).

Needs docker, `qemu-system-x86_64` and `/dev/kvm` — **no libguestfs**: the
bake boots the Ubuntu cloud image once under qemu with a NoCloud seed served
over HTTP (`-smbios … ds=nocloud-net;s=http://10.0.2.2:<port>/`, so no ISO
tooling either), runs [`bake/user-data.in`](bake/user-data.in), and powers
off; `build.sh` then flattens/compresses the disk and wraps it with
[`Dockerfile.containerdisk`](Dockerfile.containerdisk). Bake console log:
`build/console.log`. amd64-only (the bake runs the target arch under KVM).

The bake ends with `cloud-init clean --machine-id`, so the published image
treats every session's `cloudInitNoCloud` seed as a genuine first boot —
that per-session seed (not the bake's) creates the user, mounts the home and
starts the desktop.

## Verify without a cluster

```bash
desktops/vm-xfce-selkies/test.sh   # boots the baked disk with a session-like
                                   # seed; PASS = Selkies serving on :8082
```

It generates the seed with the real `whistler.cloudinit.build_user_data`
(desktop mode), boots the disk with `hostfwd`, waits for the in-guest Selkies
to serve HTTP, then (over SSH, with a throwaway key) fakes the SMB share
landing with a tmpfs on the home mountpoint — no gateway exists outside the
cluster — and asserts that the XFCE session comes up *and stays up* and that
a fresh SSH connection still authenticates once the mount shadows
`~/.ssh/authorized_keys` (regression check: the bake tar once restored
group-writable modes onto `/etc`, and sshd's StrictModes then refused the
root-disk `/etc/ssh/authorized_keys.d` path — the one the portal's web
terminal depends on after the real mount lands). By default the VM is left
running for the §6-step-5 human check at http://localhost:8082/ (Ctrl-C to
tear down; `KEEP=0` for CI mode).
