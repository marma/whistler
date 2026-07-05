# Creating desktop images

Practical guidelines for adding a new image under [`desktops/`](../desktops/),
distilled from building the existing catalog (xfce-rdp, xfce-webrtc,
gnome-flashback-webrtc, gnome-grd, xfce-selkies2). Read together with
[`desktops/README.md`](../desktops/README.md) (the catalog + conventions) and
[`design/vdi.md`](vdi.md) (viewer architecture). Written with the next image —
a full GNOME desktop — in mind.

## 1. Choose the streaming stack first

This decision shapes everything else in the image:

| Stack | Viewer | When |
|---|---|---|
| **Selkies 2.x / pixelflux** ([`xfce-selkies2`](../desktops/xfce-selkies2/)) | websockets (portal viewer TBD) | **Default for all new images.** H.264 in the browser, one TCP port, no coturn, multi-arch, unprivileged. |
| gnome-remote-desktop | `guacd` (RDP) | Wayland-native GNOME Shell specifically ([`gnome-grd`](../desktops/gnome-grd/)) — keeps the existing guacd contract at the cost of re-rasterized frames. |
| xrdp via [`base-rdp`](../desktops/base-rdp/) | `guacd` (RDP) | Legacy/simple X11 DEs where guacd-quality output is fine. |
| Selkies 1.x GStreamer | `webrtc` | **Don't build new images on this.** Kept for xfce-webrtc / gnome-flashback-webrtc until they're ported to 2.x. |

The Selkies 2.x stack is unreleased upstream: pin `SELKIES_COMMIT` and build the
Python server **and** the web client from that same commit (see the
xfce-selkies2 Dockerfile) — client/server version lock by construction. Treat
`pixelflux`/`pcmflux` (PyPI, linuxserver-maintained) as pinned upstream *code*
dependencies; we do not consume anyone's prebuilt desktop images.

## 2. Choose the process architecture

Two patterns exist; the DE dictates which one you get, not preference:

- **Plain entrypoint** (xfce-*): the entrypoint starts each piece in order
  (dbus → pulseaudio → Xvfb → DE session → selkies in the foreground) and the
  container lifecycle tracks the streaming server. Unprivileged; verified down
  to `--cap-drop=ALL`. Always prefer this when the DE allows it.
- **systemd as PID 1** (gnome-*): required the moment the session needs
  `systemd --user` — GNOME Session hard-requires it with no opt-out. This
  forces `privileged: true` on the template, which production coerces to the
  Kata runtime (`forceKataForPrivileged`). Accept this only when the DE leaves
  no choice, and say so in the Dockerfile header.

Rule of thumb from the catalog so far: **privilege requirements come from the
DE, never from the streaming layer.** If your image needs a capability, the
justification must name the DE component that demands it (e.g. GNOME's
glycin/bwrap image-loading sandbox needing real root under
`apparmor_restrict_unprivileged_userns=1` — see gnome-flashback-webrtc).

## 3. Image anatomy and conventions

- One directory per catalog entry: `Dockerfile`, `entrypoint.sh` (or systemd
  units), `README.md`. The README documents build, standalone local test, and
  a `| |` table of viewer/port/creds/arch/privileged facts.
- **Self-contained and standalone-testable**: the image must be verifiable
  with plain `docker run -p` and a browser, no cluster/portal/coturn. This is
  the bottom of the test pyramid; every image gets a `make desktop-*-local`
  target.
- Display server self-starts as the entrypoint; the pod spec overrides no
  command. Ports: 8082 for selkies (any generation), 3389 RDP, 590x VNC.
- Fixed well-known credentials (`abc`/`abc`) or none; the per-session
  NetworkPolicy is the security boundary, never the desktop login.
- **Multi-arch (amd64+arm64) unless a dependency forbids it** — with Selkies
  2.x nothing does anymore; don't add `--platform` pins without a reason
  documented in the header.
- Base on **Ubuntu 26.04** for new images (matches gnome-*, and Selkies 2.x
  needs its libva ≥ 2.21 — see §6).
- Comment discipline: every non-obvious package and entrypoint line carries
  the *why*, including the failure mode it prevents. The Dockerfiles are the
  institutional memory of this catalog — the gotchas below were all
  expensive to find and cheap to write down.

## 4. Runtime assembly checklist (Selkies 2.x / X11)

The pieces an entrypoint must provide, and the traps in each:

- **X server**: Xvfb started at the RandR ceiling (`7680x4320`), *not* at the
  default resolution — Xvfb bakes its maximum size in at startup and can never
  grow past it, so starting small permanently caps dynamic resize. Shrink to
  the default afterwards with `selkies-resize WxH` in a background retry loop
  (a fresh Xvfb rejects new modes for the first ~15–20 s).
- **Resize tooling**: selkies still shells out to `xrandr` + `cvt`. Install
  `x11-xserver-utils` and `xcvt` (which ships `cvt` without dragging in
  xserver-xorg-core). Without `cvt`, every resize fails silently.
- **Audio**: headless PulseAudio with a null sink **named `output`** —
  pcmflux captures the literal device `output.monitor`. Clear stale pulse
  state (`/tmp/pulse-*`, cookie dirs) on start or a container *restart* breaks
  audio while a fresh pod works.
- **Input tools**: `xdotool` (space/Enter/arrows/F-keys) and a `wtype` shim
  (digits/punctuation/unicode) — see
  [`desktops/xfce-selkies2/wtype-x11-shim`](../desktops/xfce-selkies2/wtype-x11-shim).
  Only a–z is injected via XTEST directly; the other two paths swallow their
  errors, so missing binaries present as "only letters type".
- **Clipboard**: `xclip` (2.x; 1.x used `xsel`). A missing xclip kills the
  per-client WS loop on connect — presents as "video never starts".
- **Python runtime**: selkies in its own venv; add `setuptools` explicitly
  (GPUtil imports `distutils`, gone since Python 3.12).
- **Session**: run the DE as the unprivileged `abc` user on the shared
  DISPLAY (`Xvfb -ac`); selkies runs as container-root. GNOME images deviate
  (root session) only for the documented glycin reason.
- **Flags**: selkies 2.x CLI flags are dash-separated (`--web-root`) and the
  parser uses `parse_known_args` — **misspelled flags are ignored silently**
  and env vars (`SELKIES_*`) are overridden by explicit flags. Basic auth is
  ON by default; disable it explicitly.
- **HTTPS**: the 2.x client hard-requires a browser secure context (WebCodecs
  doesn't exist outside one). `http://localhost` qualifies; anything else in
  dev needs `SELKIES_ENABLE_HTTPS=true` (images bake Debian's `ssl-cert`
  snakeoil pair, which selkies' cert-path defaults point at). In-cluster the
  portal's own HTTPS origin covers it — but this makes portal-HTTPS a hard
  requirement of the future websockets viewer.

## 5. Silent-failure catalog

The recurring lesson of this catalog: **the streaming stack rarely tells you
what's missing.** Symptoms observed → actual cause:

| Symptom | Cause |
|---|---|
| "video never starts", no error to the client | missing `xclip` (exception kills the WS message loop) |
| "Legacy screen_capture_module.so not found" on client connect | pixelflux wheel can't load: system libva < 2.21 (`undefined symbol: vaMapBuffer2`) — use Ubuntu 26.04 |
| only a–z types; space/digits/symbols dead | missing `xdotool` / no `wtype` shim (both paths fail silently) |
| audio track never starts; `pa_simple_new() failed: No such entity` (async, after "started successfully") | pulse sink not named `output` |
| a setting has no effect | underscore flag spelling (`--web_root`) silently ignored; or an env var overridden by an explicit flag |
| every dynamic resize fails; desktop stuck at boot resolution | missing `cvt` (xcvt), or Xvfb started at the target size instead of the ceiling |
| client shows "requires a secure connection (HTTPS)" | browsing a non-localhost origin over plain HTTP (WebCodecs needs a secure context) |
| whole app crashes at import (`No module named 'distutils'`) | missing `setuptools` in the venv (Python ≥ 3.12) |
| audio broken only after container restart (fresh pod fine) | stale PulseAudio pid/socket state in the writable layer |

When you hit a new one: fix it, then add the symptom→cause line here and the
*why* comment at the fix site.

## 6. Verification

Work up this ladder before calling an image done; each step catches what the
previous can't. [`scripts/selkies2_probe.py`](../scripts/selkies2_probe.py)
automates the protocol steps for Selkies 2.x images:

1. **Boots + serves**: `docker run -p 8082:8082`, `curl` the index (HTTP 200),
   scan logs for tracebacks.
2. **Streams**: `selkies2_probe.py stream` inside the container — must print
   `PASS` (VIDEO_STARTED + large type-4 H.264 stripes + AUDIO_STARTED). A
   static desktop sending ~nothing is *correct* (damage-based capture); the
   probe wiggles the mouse to force frames.
3. **Types**: focus a terminal running `cat > /tmp/typed.txt` in the pod, run
   `selkies2_probe.py keys`, and confirm the file contains exactly `a 5!\n` —
   one key per injection path.
4. **Least privilege**: repeat step 2 with `--cap-drop=ALL
   --security-opt=no-new-privileges`. If it now fails, either remove the
   dependency or document precisely which DE component needs what.
5. **Human check**: `make desktop-<name>-local`, real browser, look at
   latency/fidelity, type into a terminal, resize the window, play audio.
6. **Cluster**: only after all of the above — template in `values.yaml`,
   `skaffold dev`, connect through the portal (once the websockets viewer
   exists; until then 2.x images are standalone-only).

Media quality (step 5) is the only step that genuinely needs eyes; everything
else is scriptable and should stay scripted so the next stack bump can re-run
it.

## 7. Wiring a finished image into the product

Checklist of files to touch (grep for `xfce-selkies2` to see a complete
example of the first three):

- `desktops/<name>/` — the image itself.
- `Makefile` — `desktop-<name>-local` target + image var + `.PHONY`.
- `desktops/README.md` — catalog table row.
- `charts/whistler/values.yaml` — `desktopTemplates` entry (image, viewer,
  ports, `privileged`/`fuse` flags with their why-comments, resources).
- `skaffold.yaml` — build artifact + template image override for local dev.
- Portal/CRD — only if the image introduces a new `viewer:` type (the
  Selkies 2.x websockets viewer is the pending case: a reverse-proxy of
  HTTP+WS to the pod's 8082, no coturn, no vendored client JS).

## 8. Notes for the full-GNOME image

What the existing GNOME images already established:

- GNOME Shell has **no X11 backend anymore** — X11-based capture can never
  show the real Shell. GNOME Flashback (Panel + Metacity) is the X11-native
  stand-in; GNOME Shell itself runs only headless-Wayland (gnome-grd).
- GNOME Session hard-requires `systemd --user` → systemd-PID1 + privileged +
  Kata coercion (§2), and the session currently runs as root (glycin/bwrap).
  Both facts are documented in the gnome-flashback-webrtc Dockerfile header.

For "full GNOME on Selkies 2.x" there are three candidate shapes, in
increasing order of unknowns:

1. **GNOME Flashback on X11 + Selkies 2.x** — a straight port of
   gnome-flashback-webrtc onto the xfce-selkies2 recipe (swap the 1.x
   GStreamer stack for the venv + pixelflux + tools from §4). Lowest risk;
   real GNOME look-and-feel, not the real Shell.
2. **GNOME Shell headless + gnome-remote-desktop** — already prototyped as
   gnome-grd (guacd/RDP viewer, not selkies). Real Shell, re-rasterized
   pixels.
3. **GNOME Shell under Selkies' Wayland mode** — unproven territory: in
   `PIXELFLUX_WAYLAND=true` mode **pixelflux is itself the compositor**
   (clients connect to *its* Wayland socket). GNOME Shell/mutter is also a
   compositor, so it can't simply run as a client; whether a nested-mutter
   arrangement works under pixelflux is an open question to spike separately.
   Selkies' own Wayland desktops use wlroots compositors (labwc), not mutter.

Recommended order: build 1 now (it exercises the 2.x + systemd-PID1
combination, which no image has yet), keep 2 as the real-Shell fallback, and
time-box a spike of 3 before betting on it.
