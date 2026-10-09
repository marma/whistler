# vm-gnome-selkies — desktop VM containerDisk (real GNOME Shell + in-guest Selkies)

A KubeVirt **containerDisk** (OCI-wrapped qcow2) running the *real* **GNOME
Shell 46** (Activities, dynamic workspaces, the overview) with the
**Selkies 2.0** streaming stack **baked into the guest**. It is Whistler's
desktop image, and it backs `runtime: vm` desktop templates with
`viewer: websockets` (`vm-desktop` / `vm-desktop-cuda` in
[values-dev-vm.yaml](../../charts/whistler/values-dev-vm.yaml)): the portal
reverse-proxies the guest's Selkies server — per-session Service →
virt-launcher pod → masquerade → guest `:8082`.

Two system services split the guest: `whistler-streamer.service` (Xvfb +
PulseAudio + Selkies, baked enabled) and `whistler-desktop@<user>.service` (the
user's GNOME session). Cloud-init is the per-session control plane
(user/uid/keys, home mount, streamer env, session-unit start — see
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

The 24.04 pin shapes most of what follows.

### 1. The Selkies stack is built for 24.04, from patched source

[`bake/Dockerfile.builder`](bake/Dockerfile.builder) is a **24.04 image** that
builds the Selkies venv (Python 3.12, so pixelflux/pcmflux get the right ABI)
from the pinned `SELKIES_COMMIT` (2.0.0). It is built rather than installed from
PyPI or the upstream `.deb` because of two patches (see [Keyboard](#keyboard)):
[`bake/mac-cmd-chords.patch`](bake/mac-cmd-chords.patch) in the web client,
which 2.0 bundles into the wheel — so the patched client is built with
upstream's own `scripts/ci/build-web.sh` and the wheel around it — and
[`bake/xkb-active-group.patch`](bake/xkb-active-group.patch) in the server. `build.sh` extracts `/opt/venv`
and the libva tree below. `python-xlib` is installed alongside (2.0 vendors its
own and no longer depends on it) because `whistler-copy-agent` runs on this venv.

### 2. Vendored libva 2.22 (24.04 ships 2.20)

pixelflux's wheel links the system libva and needs `vaMapBuffer2` (libva
≥ 2.21); 24.04 has 2.20, which fails to load with `undefined symbol:
vaMapBuffer2` — surfaced only as *"Legacy screen_capture_module.so not found"*
when a client connects. The builder builds **libva 2.22** from source; the bake
drops it into `/usr/local/lib` and runs `ldconfig` (that dir precedes `/usr/lib`
in the ld.so order, so it wins over the stock 2.20 GNOME pulls in). Check with
`ldconfig -p | grep libva` if the stream dies on connect.

### 3. logind: we trust the VM's live logind (no `/run/systemd` hack)

The retired container desktops (`gnome-plain`, `gnome-selkies2`, in git
history) did `rm -rf /run/systemd` before starting gnome-shell. That is **not "removing
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

**Two consequences of "not a logind session" show up in the system menu**, and
both are handled rather than tolerated:

- **Power Off and Restart were missing.** Not a logind problem at all — the
  shell gates those two on `CanShutdown()` against `org.gnome.SessionManager`,
  and this image runs `gnome-shell` without `gnome-session` (§ the launcher's
  header), so nothing owned that name and the call threw. Suspend, which asks
  logind directly, stayed — which is exactly the shape a user reports as "only
  Suspend and Log off". `guest/usr/local/bin/whistler-session-manager` owns
  that name and maps Shutdown/Reboot/Logout onto logind; `test.sh` makes the
  same `CanShutdown` call the shell does and fails if it is not `true`.
- **The power actions still needed permission.** With no active session,
  polkit's `allow_active: yes` default does not apply and every login1 power
  action came back a challenge with no agent to answer it.
  `guest/etc/polkit-1/rules.d/49-whistler-power.rules` grants power-off and
  reboot to the `sudo` group — which the session user is already in, with
  NOPASSWD — and **denies suspend/hibernate outright**, because a suspended
  KubeVirt guest keeps its VMI Running with a frozen desktop and Whistler has
  no way to wake it. Denying it is also what removes it from the menu.

**If** a bake shows gnome-shell dying at startup or apps refusing to launch from
the overview (the one place it might want `systemd-run --user`), the documented
fallback in
[`whistler-desktop@.service`](guest/etc/systemd/system/whistler-desktop@.service)
recreates the container's systemd-invisible condition for that one service (a
private mount namespace over `/run/systemd`) — uncomment the two lines there.
Check `journalctl -u whistler-desktop@<user>` in the guest first.

## What's inside the guest

- **`whistler-streamer.service`** (baked enabled, root): Xvfb + PulseAudio +
  Selkies, with video streaming mode on (see below). Reads per-session knobs
  from `/etc/whistler/streamer.env`, which cloud-init writes from the
  template's `streamerEnv` + `displayPort`. Selkies 2.0 reads any of its
  settings as `SELKIES_<NAME>` from there (`SELKIES_ENCODER=h265enc`, …; see
  the [settings reference](https://docs.selkies.io/settings)).
- **`whistler-copy-agent`** (started by the streamer): app-aware Cmd-C/Cmd-V
  for Mac clients — see [Keyboard](#keyboard).
- **`whistler-desktop@<user>.service`** (template unit, `User=%i`): waits for
  the streamer's X and the NFS home mount, then runs `gnome-shell --x11` under
  `dbus-run-session` via
  [`gnome-session-launch.sh`](guest/usr/local/bin/gnome-session-launch.sh) —
  which launches the Shell + a **curated** set of gsd plugins directly, NOT
  `gnome-session` (whose required components — gsd-power, gsd-usb-protection, a
  colliding pulseaudio autostart — crash-loop headless and drop the whole
  session to the failed screen). Cloud-init does `systemctl enable --now
  whistler-desktop@<user>` once the user exists.
- The GNOME app set (Terminal, Files, Text Editor, Settings) plus **Firefox from
  Mozilla's APT repo** (24.04's `firefox` apt package is a snap shim, and snapd
  is purged) and **Google Chrome** from Google's APT repo; llvmpipe software GL
  for mutter; `librsvg2-common` + a regenerated gdk-pixbuf loader cache so
  Adwaita's SVG icons aren't blurry. The CUDA variant adds **VirtualGL** and a
  `vgl` wrapper so GL apps can render on a passthrough GPU — see
  [Three graphics tiers](#three-graphics-tiers).
- **Ubuntu Dock** (`gnome-shell-extension-ubuntu-dock`), fixed full-height on
  the left with Chrome/Firefox/Files/Terminal/Text Editor favorites,
  **Ubuntu's default wallpaper** (`ubuntu-wallpapers`) and **Ubuntu's Yaru
  icons** (`yaru-theme-icon`). The session is plain
  user-mode `gnome-shell` (`XDG_CURRENT_DESKTOP=GNOME`), so the `:ubuntu`-qualified
  defaults those packages ship — the dock's own, and `ubuntu-settings`' background
  and icon-theme stanzas — never apply; the guest's
  [`90_whistler-desktop.gschema.override`](guest/usr/share/glib-2.0/schemas/90_whistler-desktop.gschema.override)
  enables the extension, sets the look, the background and the icon theme,
  compiled with `glib-compile-schemas --strict` at bake. `--strict` catches a
  key that stops existing after a package bump but never what a *value* names —
  a `picture-uri` aimed at a file that moved (symptom: black desktop) or an
  `icon-theme` that isn't installed (symptom: Adwaita icons, no error) — so the
  bake `test`s the two wallpapers and the theme directory as well.
  `adwaita-icon-theme` stays installed as Yaru's fallback (Yaru inherits
  `Humanity,hicolor`, but GTK appends Adwaita to every lookup chain); cursors
  stay Adwaita. Defaults, not locks: per-user dconf in the NFS home still wins.

## Streaming mode is required

The streamer runs with **`--video-streaming-mode=true`** (called
`--h264-streaming-mode` before 2.0; a template still setting
`SELKIES_H264_STREAMING_MODE` is honoured). mutter is a GL compositor: once it
composits a static window it emits no further damage, so damage-based capture
leaves static windows **black** on the client until a full repaint. Streaming
mode continuously encodes the whole frame (constant bandwidth/CPU — the right
trade for a GL compositor). 2.0 defaults it on anyway; the streamer passes it
explicitly so a changed upstream default cannot bring the bug back, and the
dev templates set `SELKIES_VIDEO_STREAMING_MODE: "true"` so the intent is
visible on the template too.

## Encoding

The default encoder is `h264enc` (H.264). `h265enc`, `vp8enc`, `vp9enc` and
`av1enc` are available through `SELKIES_ENCODER` in the template's
`streamerEnv`; `h264enc-striped` and `jpeg` are CPU-only. The name is the
**codec**, not the implementation: pixelflux encodes on **NVENC** where the
GPU has the engine (the `-cuda` image's driver brings `libnvidia-encode`),
then **VA-API** (a `/dev/dri` render node), then its software encoder (x264
for H.264). A browser that cannot decode the chosen codec steps down a ladder —
hardware codecs first, then software, then striped H.264, JPEG last — without
a reload. The lean image and GPU-less sessions get software encoding from the
identical config.

Only the encode stage moves: capture stays on the CPU from Xvfb, and GNOME
still renders on llvmpipe (apps can individually opt out of that via `vgl` —
see [Three graphics tiers](#three-graphics-tiers)). The Selkies log says, at
INFO, which capture and encoder path each display took, and the dashboard's
stats panel shows the same:

```bash
journalctl -u whistler-streamer | grep -Ei 'nvenc|vaapi|encoder'
nvidia-smi -q -d UTILIZATION | grep -i -A2 encoder   # in-guest confirmation
```

Not used yet: 2.0's zero-copy X11 capture (NvFBC on NVIDIA, or DRI3 on
upstream's patched Xvfb) would keep frames on the GPU from screen to
bitstream; this image captures a stock Xvfb.

## Keyboard

- **macOS clients**: the web client sends Cmd+key chords as Ctrl+key (the
  dashboard's "Command as Control" switch, on by default).
  [`bake/mac-cmd-chords.patch`](bake/mac-cmd-chords.patch) fixes two things
  upstream still gets wrong: only the *first* chord per Cmd hold worked (Cmd-A
  then Cmd-C typed a `c`), and plain Cmd-C/Cmd-V cannot mean one X chord
  everywhere (Ctrl+C is SIGINT in a terminal; VTE's Shift+Insert pastes
  PRIMARY, not CLIPBOARD — measured). Plain Cmd-C/V are sent as
  XF86Copy/XF86Paste taps, which the in-session
  [`whistler-copy-agent`](guest/usr/local/bin/whistler-copy-agent) re-injects
  as the chord the **focused** window expects — Ctrl+C/V in GUI apps,
  Ctrl+Shift+C/V in terminals (by WM_CLASS). Physical Ctrl-C still means
  SIGINT, PRIMARY/middle-click is untouched, and Cmd-Shift-C/V arrive verbatim
  as Ctrl+Shift+C/V. Full investigation: design/keyboards.md.
- **Option-key characters** (`|`, `@`, `{`, … via Option, e.g. Option+7 for `|`
  on a Swedish Mac layout): the client maps the physical Option key to the
  X11 keysym `Mode_switch`, which no modern keymap binds. `whistler-copy-agent`
  pre-binds `Mode_switch` (and `ISO_Level3_Shift`) to a spare keycode at
  startup so it is present before any client needs it.
- **Several input sources** (e.g. Swedish + US in GNOME Settings) give the X
  keymap several XKB groups, and the server has to type each character in the
  group that is active. 2.0 does XKB placement properly but types a keysym in
  the *lowest* group carrying it, and its group locks are undone by GNOME
  within milliseconds; measured, it typed wrong characters in 3 of the 4
  source-order × active-source combinations (`:` → `Ö`, å ä ö → å ' ;).
  [`bake/xkb-active-group.patch`](bake/xkb-active-group.patch) types in the
  active group when it carries the keysym and otherwise binds it to a spare
  keycode, never moving the user's group; all four combinations are correct
  with it. Re-run that matrix after a Selkies bump (the patch header has it).

## Three graphics tiers

A passthrough GPU is reachable three different ways, and they are independent:

| Tier | What the GPU does | How you get it |
|---|---|---|
| software | nothing (or NVENC stream encode only) | lean image, or `-cuda` + GPU with no GL apps wrapped |
| compute | CUDA (Cycles, ML) + NVENC encode; all drawing stays llvmpipe | `-cuda` image + GPU passthrough — the default behavior |
| accelerated GL (per app) | the wrapped app's OpenGL renders on the GPU | `-cuda` image + GPU passthrough + `vgl <app>` |

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
make vm-gnome-desktop-image          # → localhost:5000/whistler-vm-gnome-selkies:latest  (lean, no GPU driver)
make vm-gnome-desktop-image CUDA=1   # → …-cuda:latest  (bakes the NVIDIA driver for passthrough sessions)
make vm-gnome-desktop-image PUSH=1   # …and push to the dev registry
```

The default lean image carries **no** NVIDIA driver; `CUDA=1` bakes the
driver in and publishes `whistler-vm-gnome-selkies-cuda`. (That suffix is on
the image name, not the tag, so the mutable dev tag stays exactly `:latest` —
the only tag KubeVirt defaults to `imagePullPolicy: Always`; a `:latest-cuda`
tag would leave nodes booting a stale cached qcow2 after every rebuild.)
GNOME still renders on llvmpipe in both variants (Xvfb
serves Mesa swrast GLX — a passthrough GPU can't change that by itself), but
the two are **not** otherwise identical: the driver brings `libnvidia-encode`,
so on a `-cuda` passthrough session pixelflux encodes the stream on
**NVENC** instead of in software (see [Encoding](#encoding)). The driver also carries the whole GPU
compute runtime, and **VirtualGL** (`VIRTUALGL_VERSION`,
default 3.1.4) lets individual GL apps draw on the GPU via `vgl <app>` — see
[Three graphics tiers](#three-graphics-tiers). NOTE the 24.04
driver packages differ from 26.04's: default driver `nvidia-driver-550-open`, override
via `NVIDIA_DRIVER_PACKAGE` (a wrong name fails the bake). Heads-up: in current
noble that package is a **transitional shim** whose only dependency is
`nvidia-driver-580-open`, so the bake really installs 580.x — the "550" in the
default is no longer pinning anything.

**The `-cuda` image ships the GPU runtime, not the CUDA SDK.** What GPU
workloads actually load comes from the driver: `libcuda.so.1`, the PTX JIT,
`libnvoptix` + rtcore (Blender's OptiX backend), `libnvidia-encode` and
`nvidia-smi`. PyTorch's wheels bundle their own cudart/cuBLAS/cuDNN/nvrtc and
only dlopen `libcuda`; Blender ships precompiled Cycles kernels. So
`CUDA_TOOLKIT_PACKAGE` is **empty by default** — `nvcc` exists to *compile*
CUDA C++, needs `g++` as its host compiler and pulls ~2.8 GB (2.4 GB of that
`nvidia-cuda-dev` headers and static libs), none of which a running session
touches. Set `CUDA_TOOLKIT_PACKAGE=nvidia-cuda-toolkit` (CUDA 12.x from the
24.04 archive, or a `cuda-toolkit-XX-Y` from NVIDIA's apt repo) to bake it into
a dev/data-science image; it installs `--no-install-recommends`, which still
excludes the ~1.6 GB of `nsight-systems` / `nsight-compute` /
`nvidia-visual-profiler` (that last one being what used to pull `openjdk-8-jre`
into every GPU desktop). Users who need `nvcc` or compilers ad hoc can install
them into `$HOME` with a user-space package manager such as pixi/conda-forge —
no root, no image rebuild — provided their zone permits the package hosts.

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
                                    # + the data WebSocket stays open
```

It generates the seed with the real `whistler.cloudinit.build_user_data`
(desktop mode, video streaming mode on), boots the disk with `hostfwd`, waits for
Selkies to serve HTTP, then over SSH fakes the NFS export landing (tmpfs on the
home mountpoint — no gateway outside the cluster) and asserts `gnome-shell` comes
up *and stays up* with windows in the streamer's X, and that a fresh SSH
connection still authenticates once the mount shadows `~/.ssh/authorized_keys`.
**The HTTP/window checks can't see frame coherence** (streaming-mode black
windows, llvmpipe GTK4 garbage) — PASS leaves the VM up for a human browser
check at http://localhost:8082/ (`KEEP=0` for CI mode).
