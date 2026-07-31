# vm-gnome-selkies — desktop VM containerDisk (real GNOME Shell + in-guest Selkies)

A KubeVirt **containerDisk** (OCI-wrapped qcow2) running the *real* **GNOME
Shell 46** (Activities, dynamic workspaces, the overview) with the Selkies 2.x
streaming stack **baked into the guest**. The GNOME sibling of
[`../vm-xfce-selkies`](../vm-xfce-selkies/) — read that README first; this one
documents only what GNOME forces to be different. It backs `runtime: vm`
desktop templates with `viewer: websockets`
(`ubuntu-vm-gnome-selkies` in [values-dev-vm.yaml](../../charts/whistler/values-dev-vm.yaml)):
the portal reverse-proxies the guest's Selkies server exactly like a pod
desktop's sidecar — per-session Service → virt-launcher pod → masquerade →
guest `:8082`.

**Why baked, not sidecar:** the streamer sidecar can't cross the VM boundary
(unix sockets don't cross it), so VMs run an in-guest streamer. Same reasoning,
same in-guest `whistler-streamer.service` + `whistler-desktop@<user>.service`
split as `vm-xfce-selkies`; cloud-init stays the per-session control plane
(user/uid/keys, SMB home mount, streamer env, session-unit start — see
[whistler/cloudinit.py](../../whistler/cloudinit.py)).

## The crux: Ubuntu 24.04 / GNOME 46 (not 26.04)

GNOME 46 is the last generation with an **X11 backend** (`gnome-shell --x11`),
so the real Shell can composit an Xvfb display that pixelflux captures. On
26.04's GNOME 50 mutter is Wayland-only, and under Wayland the Shell *is* the
display server — nothing left for a display-owning streamer to capture (that
needs a non-X capture point, a later stage of the guest-unaware-display plan).
So this guest is **24.04**, resurrecting the recipe of the retired embedded
`gnome-selkies2` image (git history) — see
[design/creating_desktops.md §8](../../design/creating_desktops.md).

That 24.04 pin drives every difference from `vm-xfce-selkies`:

### 1. The Selkies stack is rebuilt on 24.04, not extracted from `streamer-selkies2`

`vm-xfce-selkies` extracts `/opt/venv` + `/opt/selkies-web` straight from the
26.04 [`../streamer-selkies2`](../streamer-selkies2/) image because its guest is
also 26.04. This guest can't: the streamer's venv is Python 3.13 (26.04) and its
compiled wheels (pixelflux/pcmflux, evdev, xkbcommon-cffi) are the wrong ABI for
24.04's Python 3.12. So [`bake/Dockerfile.builder`](bake/Dockerfile.builder) is
a **24.04 image** that rebuilds the venv (Python 3.12) and the web client from
the same pinned `SELKIES_COMMIT`; `build.sh` extracts its artifacts the same way.

### 2. Vendored libva 2.22 (24.04 ships 2.20)

pixelflux's wheel links the system libva and needs `vaMapBuffer2` (libva
≥ 2.21); 24.04 has 2.20, which fails to load with `undefined symbol:
vaMapBuffer2` — surfaced only as *"Legacy screen_capture_module.so not found"*
when a client connects. The builder builds **libva 2.22** from source; the bake
drops it into `/usr/local/lib` and runs `ldconfig` (that dir precedes `/usr/lib`
in the ld.so order, so it wins over the stock 2.20 GNOME pulls in). Check with
`ldconfig -p | grep libva` if the stream dies on connect.

### 3. logind: we trust the VM's live logind (no `/run/systemd` hack)

The container images (`../gnome-plain`, retired `gnome-selkies2`) do
`rm -rf /run/systemd` before starting gnome-shell. That is **not "removing
systemd"** — it works around a *broken* logind: the systemd package bakes an
empty `/run/systemd/seats` into the image, so gnome-shell picks the systemd
login manager, but with no logind daemon alive (systemd isn't pid 1) every
`login1` call fails hard and the session dies.

In this **VM logind is alive**, so `login1` answers. The desktop is a `User=`
system service, not a logind *session* (those come from PAM logins), so
gnome-shell is in the "logind present, not in a session" state — `login1`
returns a clean *no-session* error, which gnome-shell 46 tolerates (it just
forgoes lock/suspend/idle, meaningless here). So this image **does not** shadow
`/run/systemd`; it runs the Shell against the real logind.

`XDG_RUNTIME_DIR` is still a systemd `RuntimeDirectory` — a `User=` system
service gets no logind-managed `/run/user/<uid>` regardless.

**If** a bake shows gnome-shell dying at startup or apps refusing to launch from
the overview (the one place it might want `systemd-run --user`), the documented
fallback in
[`whistler-desktop@.service`](guest/etc/systemd/system/whistler-desktop@.service)
recreates the container's systemd-invisible condition for that one service (a
private mount namespace over `/run/systemd`) — uncomment the two lines there.
Check `journalctl -u whistler-desktop@<user>` in the guest first.

## What's inside the guest

- **`whistler-streamer.service`** (baked enabled, root): Xvfb + PulseAudio +
  Selkies. Same script as `vm-xfce-selkies`'s streamer **except
  `--h264-streaming-mode` defaults ON** — see below. Reads optional per-session
  knobs from `/etc/whistler/streamer.env` (cloud-init writes it from the
  template's `streamerEnv` + `displayPort`).
- **`whistler-desktop@<user>.service`** (template unit, `User=%i`): waits for
  the streamer's X and the SMB home mount, then runs `gnome-shell --x11` under
  `dbus-run-session` via
  [`gnome-session-launch.sh`](guest/usr/local/bin/gnome-session-launch.sh) —
  which launches the Shell + a **curated** set of gsd plugins directly, NOT
  `gnome-session` (whose required components — gsd-power, gsd-usb-protection, a
  colliding pulseaudio autostart — crash-loop headless and drop the whole
  session to the failed screen). Cloud-init does `systemctl enable --now
  whistler-desktop@<user>` once the user exists.
- The GNOME app set (Terminal, Files, Text Editor, Settings) plus **Firefox from
  Mozilla's APT repo** (24.04's `firefox` apt package is a snap shim, and snapd
  is purged); llvmpipe software GL for mutter; `librsvg2-common` + a regenerated
  gdk-pixbuf loader cache so Adwaita's SVG icons aren't blurry. The CUDA variant
  adds **VirtualGL** and a `vgl` wrapper so GL apps can render on a passthrough
  GPU — see [Three graphics tiers](#three-graphics-tiers).

## Streaming mode is required

The streamer defaults **`--h264-streaming-mode=true`** here (XFCE leaves it
off). mutter is a GL compositor: once it composits a static window it emits no
further damage, so pixelflux's default damage-based capture leaves static
windows **black** on the client until a full repaint. Streaming mode
continuously encodes the whole frame (constant bandwidth/CPU — the right trade
for a GL compositor). Templates should also set
`streamerEnv: { SELKIES_H264_STREAMING_MODE: "true" }` (the
`ubuntu-vm-gnome-selkies` sample does) so the intent is explicit even though the
in-guest default already covers it.

## Streaming is H.264 — but not x264

`--encoder=x264enc` names the **output mode**, not the encoder implementation.
Selkies' enum is only `{x264enc, x264enc-striped, jpeg}` (full-frame H.264 /
striped H.264 / JPEG) — there is no `nvenc` value, and the reported encoder name
never changes. The backend is a *separate* setting, `--use-cpu`
(`SELKIES_USE_CPU`), which defaults to **false** and which the streamer does not
set; with it false pixelflux tries **NVENC** (CUDA driver API) first, then
**VA-API** (`/dev/dri` render node), and falls back to its bundled libx264 only
if both fail.

So a GPU-passthrough session on `:dev-cuda` encodes on the GPU while still
displaying encoder `x264enc` — GPU utilisation with a "software" encoder name is
expected, not a misconfiguration. The lean image and GPU-less sessions get
software x264 from the identical config. `jpeg` and `x264enc-striped` force
`use_cpu=true` server-side and are always CPU.

Only the encode stage moves: capture stays XShm from Xvfb, and GNOME still
renders on llvmpipe (apps can individually opt out of that via `vgl` — see
[Three graphics tiers](#three-graphics-tiers)). To see which backend a session
picked:

```bash
journalctl -u whistler-streamer | grep -Ei 'nvenc|vaapi|x264'
# "NVENC Encoder Initialized successfully."  → GPU
# "VAAPI Encoder Initialized successfully."  → GPU
# "... Falling back to x264" / "to CPU"      → software libx264
nvidia-smi -q -d UTILIZATION | grep -i -A2 encoder   # in-guest confirmation
```

## Three graphics tiers

A passthrough GPU is reachable three different ways, and they are independent:

| Tier | What the GPU does | How you get it |
|---|---|---|
| software | nothing (or NVENC stream encode only) | `:dev` image, or `:dev-cuda` + GPU with no GL apps wrapped |
| compute | CUDA (Cycles, ML) + NVENC encode; all drawing stays llvmpipe | `:dev-cuda` + GPU passthrough — the default behavior |
| accelerated GL (per app) | the wrapped app's OpenGL renders on the GPU | `:dev-cuda` + GPU passthrough + `vgl <app>` |

The reason drawing doesn't accelerate by itself: Xvfb's GLX is Mesa swrast, so
*every* GL context — mutter's compositing, Blender's viewport, GTK4's
renderer — is llvmpipe regardless of what hardware the guest owns. That's why
Blender's Cycles (CUDA) is fast while its viewport (OpenGL) crawls. CUDA and
NVENC bypass the display stack entirely; GLX cannot.

**VirtualGL** (baked into the CUDA variant only, pinned upstream .deb —
`VIRTUALGL_VERSION` in [build.sh](build.sh)) bridges that per app: the guest
wrapper [`vgl`](guest/usr/local/bin/vgl) runs `vglrun -d egl`, which interposes
the app's GLX calls and renders them on the NVIDIA **EGL device** (no
GPU-owning X server needed), then blits the finished frames into the Xvfb
display where mutter composits and pixelflux captures them as usual.

```bash
vgl glxinfo -B     # "OpenGL renderer string" must name the NVIDIA GPU
vgl blender        # viewport now draws on the GPU (Cycles was already CUDA)
```

The desktop itself (mutter, GNOME's own chrome) still composits on llvmpipe —
whole-desktop acceleration would mean replacing Xvfb with an NVIDIA-owning
Xorg, a separate future tier. On the lean image or a GPU-less session `vgl`
fails fast with a clear message instead of silently running llvmpipe.

## Known limitation: overview backdrop at HiDPI

Inherited unchanged from `gnome-selkies2`: gnome-shell 46's Activities/overview
backdrop is created at the shell's startup resolution and, on X11, only ever
*shrinks* — a client driving the framebuffer above ~1920×1080 leaves the
overview backdrop confined to the old rectangle in the top-left. Desktop,
windows, and app launching all work; only the overview backdrop is wrong. The
only full fix is a fixed resolution (no dynamic resize), which we decline. See
[`gnome-session-launch.sh`](guest/usr/local/bin/gnome-session-launch.sh).

## Build

```bash
make vm-gnome-desktop-image          # → localhost:5000/whistler-vm-gnome-selkies:dev  (lean, no GPU driver)
make vm-gnome-desktop-image CUDA=1   # → …:dev-cuda  (bakes NVIDIA driver + CUDA toolkit for passthrough sessions)
make vm-gnome-desktop-image PUSH=1   # …and push to the dev registry
```

Like [`../vm-xfce-selkies`](../vm-xfce-selkies/), the default `:dev` image
carries **no** NVIDIA driver; `CUDA=1` bakes the driver **and the CUDA toolkit**
(`nvcc` + cuda runtime libs; several GB, so the CUDA build gets a bigger disk)
in and tags `:dev-cuda`. GNOME still renders on llvmpipe in both variants (Xvfb
serves Mesa swrast GLX — a passthrough GPU can't change that by itself), but
the two are **not** otherwise identical: the driver brings `libnvidia-encode`,
so on a `:dev-cuda` passthrough session pixelflux encodes the stream on
**NVENC** instead of software x264 (see [Streaming is H.264 — but not
x264](#streaming-is-h264--but-not-x264)). CUDA on top of that is for
GPU-compute workloads in the guest, and **VirtualGL** (`VIRTUALGL_VERSION`,
default 3.1.4) lets individual GL apps draw on the GPU via `vgl <app>` — see
[Three graphics tiers](#three-graphics-tiers). NOTE the 24.04
packages differ from 26.04's: default driver `nvidia-driver-550-open`, default
toolkit the archive `nvidia-cuda-toolkit` (CUDA 12.x); override via
`NVIDIA_DRIVER_PACKAGE` / `CUDA_TOOLKIT_PACKAGE` (a wrong name fails the bake).

Needs docker, `qemu-system-x86_64` and `/dev/kvm` — no libguestfs (the bake
boots the 24.04 cloud image once under qemu with a NoCloud-over-HTTP seed, runs
[`bake/user-data.in`](bake/user-data.in), and powers off; `build.sh` flattens
and wraps it with [`Dockerfile.containerdisk`](Dockerfile.containerdisk)). Bake
console: `build/console.log`. amd64-only. The bake ends with `cloud-init clean
--machine-id`, so the published image treats every session's `cloudInitNoCloud`
seed as a genuine first boot.

`SELKIES_COMMIT` and `LIBVA_VERSION` (both in
[`bake/Dockerfile.builder`](bake/Dockerfile.builder)) pin the streaming stack;
the base cloud image URL (`BASE_IMAGE_URL` in `build.sh`) is **load-bearing at
24.04** — moving to 26.04 forfeits both the X11 Shell and the unprivileged
architecture.

## Verify without a cluster

```bash
desktops/vm-gnome-selkies/test.sh   # boots the baked disk with a session-like
                                    # seed; PASS = Selkies on :8082 + gnome-shell up
```

It generates the seed with the real `whistler.cloudinit.build_user_data`
(desktop mode, H264 streaming mode on), boots the disk with `hostfwd`, waits for
Selkies to serve HTTP, then over SSH fakes the SMB share landing (tmpfs on the
home mountpoint — no gateway outside the cluster) and asserts `gnome-shell` comes
up *and stays up* with windows in the streamer's X, and that a fresh SSH
connection still authenticates once the mount shadows `~/.ssh/authorized_keys`.
**The HTTP/window checks can't see frame coherence** (streaming-mode black
windows, llvmpipe GTK4 garbage) — PASS leaves the VM up for a human browser
check at http://localhost:8082/ (`KEEP=0` for CI mode).
