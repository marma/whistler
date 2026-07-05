# gnome-selkies2 — full GNOME Shell over Selkies 2.x (pixelflux / WebSockets)

The **real GNOME Shell** (Activities, dynamic workspaces, the overview) streamed
over the Selkies 2.x H.264/WebSockets path — option 1 from
[`design/creating_desktops.md §8`](../../design/creating_desktops.md). Same
streaming engine as [`../xfce-selkies2`](../xfce-selkies2/), but the actual GNOME
Shell instead of XFCE, and with a **runtime-configurable** desktop user, UID/GID,
sudo, and home volume.

| | |
|---|---|
| Desktop | GNOME Shell 46 (mutter X11 backend) + a curated set of gnome-settings-daemon plugins |
| Apps | GNOME Terminal, Files (nautilus), Text Editor, Settings (control-center), **Firefox** (Mozilla `.deb`, not the snap) |
| Base | **Ubuntu 24.04 / GNOME 46** (deliberately not 26.04 — see below) |
| Viewer | none yet — standalone only (same as xfce-selkies2) |
| Port | `8082` (web client + WebSocket stream, one port) |
| Transport | **WebSockets** (pixelflux H.264 → browser WebCodecs). No WebRTC, **no coturn/TURN** |
| Encoder | `x264enc` via pixelflux (software; no GPU) |
| Arch | multi-arch-capable (pixelflux/pcmflux ship amd64+arm64; verified amd64) |
| Privileged | **no `--privileged`** — but not `--cap-drop=ALL` either; see *Privileges* |
| Upstream | `selkies-project/selkies` `main` @ pinned `SELKIES_COMMIT` (same as xfce-selkies2) |

## Why Ubuntu 24.04 / GNOME 46 (not 26.04)

This is the crux and the reason this image exists as a separate recipe from the
26.04 GNOME images ([`../gnome-flashback-webrtc`](../gnome-flashback-webrtc/),
[`../gnome-grd`](../gnome-grd/)). GNOME 46 is the last generation with **two
escape hatches** that GNOME 50 (26.04) removed:

1. **GNOME Shell still has an X11 backend** (`mutter --x11`). So the *real Shell*
   runs on Xvfb and pixelflux captures it with ordinary X11 screen capture. On
   26.04's GNOME 50 mutter is Wayland-only, which is exactly why
   gnome-flashback-webrtc had to fall back to Flashback (Panel + Metacity) — a
   GNOME-flavored X11 desktop, but *not* the Shell.
2. **It runs unprivileged.** GNOME 50 forces `systemd --user` (→ systemd as
   PID 1 → `--privileged` → Kata in prod) and a root session (26.04's mandatory
   glycin/bwrap image sandbox). On 24.04 none of that applies: gnome-shell
   composits fine under llvmpipe as an ordinary user, and gdk-pixbuf still loads
   images in-process.

Cost of staying on 24.04: GNOME 46 (LTS-supported to 2029, fine), and its libva
is 2.20 while pixelflux needs ≥ 2.21 — so the image **builds libva 2.22 from
source** in a short stage (see the Dockerfile; it's small and dependency-light).

## Configurable identity (the part xfce-selkies2 doesn't have)

The desktop user is created **at container start** from env vars, so a mounted
per-user home volume lines up by ownership:

| env | default | meaning |
|---|---|---|
| `DESKTOP_USER` | `abc` | login/username of the desktop user |
| `PUID` | `1000` | its UID |
| `PGID` | `1000` | its GID |
| `DESKTOP_SUDO` | `false` | `true` → add to `sudo` group + passwordless sudoers drop-in |
| `SELKIES_RESOLUTION` | `1280x720` | initial desktop size (grows on client resize) |

The home volume is mounted at `/home/$DESKTOP_USER` and is **assumed already
owned by `PUID:PGID`** — the entrypoint does *not* `chown -R` it (that's
O(files) and pointless once provisioned); it only ensures the top-level dir
exists and is owned. Provision the volume's contents with the right ownership
out of band.

```bash
docker run --rm -it \
  -e DESKTOP_USER=alice -e PUID=1234 -e PGID=1234 -e DESKTOP_SUDO=true \
  -v /path/to/alice-home:/home/alice \
  -p 8082:8082 whistler-desktop-gnome-selkies2:dev
```

## Build & local test

```bash
docker build -t whistler-desktop-gnome-selkies2:dev desktops/gnome-selkies2
make desktop-gnome-selkies2-local        # build + run, then open http://localhost:8082/
```

Everything is TCP on one port, so a plain `-p 8082:8082` is enough on any host OS
(no TURN, ever). The HTTPS / secure-context rules are identical to
[`../xfce-selkies2`](../xfce-selkies2/#error-this-application-requires-a-secure-connection-https):
`http://localhost:8082` is a secure context; from another machine either
`ssh -L 8082:localhost:8082 <host>` or `SELKIES2_HTTPS=true`.

## Streaming mode is required (GNOME's biggest capture gotcha)

The entrypoint passes **`--h264-streaming-mode=true`** (env `SELKIES_H264_STREAMING_MODE`,
default `true` here; xfce-selkies2 leaves it off). This is not optional for GNOME
Shell. pixelflux's default capture sends only *damaged* regions — perfect for
XFCE, where a static window's pixels stay in the X framebuffer pixelflux reads.
But mutter is an **OpenGL compositor**: once it has composited a static window it
emits no further damage for it, so damage-based capture never re-sends those
regions and the client canvas shows them as **black** until something forces a
full repaint (e.g. opening the Activities overview). The exact symptom is a blue
desktop with a black bar, black window interiors, and one window's content
"bleeding" into another — while a server-side screenshot of the same X display
is perfectly clean. Streaming mode makes pixelflux continuously encode the whole
frame like a normal video stream, so static content is always present. Cost:
constant bandwidth/CPU even on an idle desktop. Verified with a headed browser
decoding the actual stream (not just a server-side X grab).

## Known limitation: app-switcher backdrop at HiDPI / large windows

Dynamic resize is on by default (the desktop resizes to match the browser
window). There is one cosmetic casualty: gnome-shell 46's Activities /
app-switcher **overview backdrop** is created at the resolution the shell starts
at and, on X11, only ever *shrinks* with the monitor — it never grows. So when a
client drives the framebuffer **above ~1920×1080** — which a HiDPI/Retina display
with a browser window larger than 1920×1080 does — the overview's dark backdrop
and app-grid stay confined to the smaller rectangle in the top-left, while the
panel, windows, and the desktop itself use the full size ("two screen
dimensions"). The desktop, all windows, and launching apps work normally; only
the overview *backdrop* is wrong.

This is inherent to changing resolution under mutter on Xvfb — verified that
forcing `scaling-factor=1`, pre-growing to the RandR ceiling, and restarting the
shell all fail to make the backdrop grow. The only complete fix is a **fixed
resolution** (no dynamic resize), where the browser scales a constant-size
desktop to fit; that trades away matching the window, so this image keeps dynamic
resize by choice. If you prefer the fixed-resolution trade, selkies supports it
via `--enable-resize=false` + `--manual-width/--manual-height` (envs
`SELKIES_ENABLE_RESIZE`, `SELKIES_MANUAL_WIDTH/HEIGHT`).

## Session architecture — why not plain `gnome-session`

The session is launched by [`gnome-session-launch.sh`](gnome-session-launch.sh),
which runs `gnome-shell --x11` plus a **curated** set of settings daemons
directly — **not** `gnome-session`. On Ubuntu 24.04, gnome-session's
`gnome.session` marks several plugins as *required* that cannot survive a
headless, logind-less container — `gsd-power` (needs logind + upower),
`gsd-usb-protection` (SIGSEGVs), and a `pulseaudio` autostart that collides with
the entrypoint's already-running PulseAudio. gnome-session drops the entire
desktop to its "Oh no, something has gone wrong" screen the moment any *required*
component crash-loops, even though gnome-shell itself is perfectly happy. So we
launch the Shell and only the daemons that run cleanly headless (xsettings,
keyboard, media-keys, sound, a11y-settings, datetime, housekeeping, color) and
skip the rest. Trade-off: no logind-only features (lock/suspend/seat switching),
which are meaningless for a single-user streamed desktop.

## Privileges

Runs as **root PID 1** (to create the user, `chown`, and `su` to it at start),
then the desktop itself runs as the unprivileged `$DESKTOP_USER`. It does **not**
need `--privileged` — the key architectural win over the 26.04 GNOME images.

Because of the runtime user setup it can't run at full `--cap-drop=ALL` (unlike
xfce-selkies2, whose `abc` is baked at build time). The minimal set is:

```bash
docker run --cap-drop=ALL \
  --cap-add=CHOWN --cap-add=DAC_OVERRIDE --cap-add=FOWNER \
  --cap-add=SETUID --cap-add=SETGID \
  -p 8082:8082 whistler-desktop-gnome-selkies2:dev     # verified: streams
```

(`CHOWN`/`DAC_OVERRIDE`/`FOWNER` for `useradd`/`groupadd`/`chown`; `SETUID`/
`SETGID` for `su`.) These are all in Docker's default set, so a plain
`docker run` works too.

## Verification status

Verified on a Linux/amd64 host (2026-07-05) with
[`scripts/selkies2_probe.py`](../../scripts/selkies2_probe.py), following the
ladder in [design/creating_desktops.md §6](../../design/creating_desktops.md):

- **Boots + serves**: web client HTTP 200, no tracebacks.
- **Streams** (`probe stream` → `PASS`): `VIDEO_STARTED` + H.264 stripes +
  `AUDIO_STARTED`, capturing the real GNOME Shell composited by mutter on
  llvmpipe. The vendored libva 2.22 is confirmed loading (pixelflux would
  otherwise fail with the "Legacy screen_capture_module.so not found" symptom).
- **Client render** (the step the protocol probe can't see): verified by running
  a *headed* Firefox on a second Xvfb display decoding the actual stream and
  screenshotting it — static windows, decorations and background all render
  correctly. This is what caught the streaming-mode bug above; `probe stream`
  passed while the browser still showed black static regions, because the probe
  only asserts that *some* H.264 arrives, not that the whole frame is coherent.
- **Types** (`probe keys`): `a 5!⏎` lands verbatim in a focused GNOME Terminal
  (all three injection paths — XTEST / xdotool / wtype-shim).
- **Least privilege**: streams under `--cap-drop=ALL` + the 5 caps above.
- **Configurable identity**: verified `DESKTOP_USER`/`PUID`/`PGID`, `DESKTOP_SUDO`,
  and a mounted `/home/$USER` volume — Shell runs as the requested user.
- **Human check** (latency/fidelity in a real browser): the one step that needs
  eyes; do it via `make desktop-gnome-selkies2-local`.

## GNOME-46-on-headless gotchas (the expensive-to-find ones)

All encoded in the Dockerfile / entrypoint / launcher comments; summarized for
the next base bump:

- **`/run/systemd/seats` must not exist.** The `systemd` package (pulled in
  transitively by GNOME) bakes an empty `/run/systemd/seats` into the image;
  gnome-shell's `loginManager.js` keys off exactly that path to pick
  `LoginManagerSystemd`, then every `login1` call throws (no systemd PID 1) and
  the session dies. The entrypoint `rm -rf /run/systemd` so gnome-shell uses its
  built-in dummy login manager.
- **Icons need `librsvg2-common`** (the gdk-pixbuf SVG loader). Adwaita's icons
  are scalable SVGs; `--no-install-recommends` drops `librsvg2-common`, and
  without its loader gdk-pixbuf can't rasterize them — gnome-shell then renders
  every app icon as a tiny bitmap scaled up to a blurry blob (the "blurry blue
  diamond" in the dash was actually the Files icon). Text stays crisp, so the
  tell is "text sharp, icons blurry". The Dockerfile installs it and regenerates
  the loader cache (the packaging trigger doesn't reliably fire in a build layer).
- **GTK4 apps need `GSK_RENDERER=cairo`.** Files/nautilus, Text Editor and
  Settings are GTK4; GTK4 renders window *content* through GSK's OpenGL renderer,
  which produces garbage under llvmpipe — the window shows a stale copy of the
  desktop framebuffer instead of its own UI (still resizable/stackable, just
  wrong pixels). Forcing the cairo software renderer in the session env fixes it.
  GTK3 apps (gnome-terminal) and gnome-shell itself (Clutter/Cogl) are
  unaffected, which is why the Terminal always looked fine while Files didn't.
- **Don't set `GNOME_SHELL_SESSION_MODE=ubuntu`** unless you install the Yaru
  shell theme — that mode loads `.../theme/Yaru/gnome-shell-theme.gresource` and
  gnome-shell **SIGSEGVs** if it's missing. The default (unset → `gnome` mode,
  Adwaita) needs nothing extra.
- **`gnome-session --session=gnome-xorg` silently does nothing** — there is no
  `gnome-xorg.session` on 24.04 (only `gnome.session`), so gnome-session loads an
  empty session and shows the failed screen. Irrelevant here since we bypass
  gnome-session, but it cost an afternoon.
- **Firefox must come from Mozilla's APT repo**, not Ubuntu's — the `firefox`
  apt package is a snap transitional shim and snapd doesn't run in a container.
- Plus the whole Selkies 2.x silent-failure catalog (xclip, `output` sink name,
  xdotool/wtype, dash-separated flags, setuptools) — see
  [`../xfce-selkies2/README.md`](../xfce-selkies2/README.md).

## Pinning / bumping

`SELKIES_COMMIT` pins the Selkies server + web client together (kept in sync with
xfce-selkies2). `LIBVA_VERSION` pins the vendored libva. Base image `ubuntu:24.04`
is load-bearing — see *Why Ubuntu 24.04 / GNOME 46* before bumping it; moving to
26.04/GNOME 50 forfeits both the X11-Shell and the unprivileged architecture.
