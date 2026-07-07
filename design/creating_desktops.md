# Creating desktop images

Practical guidelines for adding a new image under [`desktops/`](../desktops/),
distilled from building the catalog ([`xfce-selkies2`](../desktops/xfce-selkies2/),
[`gnome-selkies2`](../desktops/gnome-selkies2/)) and the guacd/RDP and
Selkies-1.x/WebRTC spikes that preceded it (removed — see the banner in
[`design/vdi.md`](vdi.md)). Read together with
[`desktops/README.md`](../desktops/README.md) (the catalog + conventions).

## 1. The streaming stack: Selkies 2.x / pixelflux

There is one stack, and every new image uses it: **Selkies 2.x (pixelflux)** with
the portal's **`websockets`** viewer. H.264 reaches the browser's decoder over a
single TCP port (plain WebSockets), no coturn/TURN, multi-arch (amd64+arm64),
unprivileged. The alternatives that were evaluated and dropped — guacd/RDP
(re-rasterized frames, extra daemon) and Selkies 1.x + WebRTC/coturn (amd64-only,
needs a TURN relay) — are recorded in [`vdi.md`](vdi.md) and recoverable from git
history; the one thing guacd/VNC offered that Selkies can't is *agentless*
VM-console capture (KubeVirt QEMU framebuffer), if that is ever needed.

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
`apparmor_restrict_unprivileged_userns=1`, a systemd-PID1 base — which is why
`gnome-selkies2` deliberately pins GNOME 46 / Ubuntu 24.04 to avoid it).

## 3. Image anatomy and conventions

- One directory per catalog entry: `Dockerfile`, `entrypoint.sh` (or systemd
  units), `README.md`. The README documents build, standalone local test, and
  a `| |` table of viewer/port/creds/arch/privileged facts.
- **Self-contained and standalone-testable**: the image must be verifiable
  with plain `docker run -p` and a browser, no cluster or portal. This is
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
| GNOME session dies to "Oh no, something has gone wrong" | a *required* gnome-session component crash-looped: gnome-shell can't reach `login1` (`/run/systemd/seats` present → it picks systemd login mgr with no systemd behind it — `rm -rf /run/systemd`), or a required gsd plugin fails headless (`gsd-power`/`gsd-usb-protection`), or wrong `--session` name (`gnome-xorg` doesn't exist on 24.04) |
| gnome-shell SIGSEGVs immediately at startup | `GNOME_SHELL_SESSION_MODE=ubuntu` with no Yaru theme installed — the mode loads a missing `.../theme/Yaru/gnome-shell-theme.gresource`; use the default `gnome` mode (Adwaita) or install the theme |
| Firefox uninstallable / won't launch in a container | 24.04's `firefox` apt package is a snap transitional shim; install the real `.deb` from Mozilla's APT repo with an apt pin |
| GNOME Shell: client shows black static windows / one window's content bleeds into another, but a server-side X screenshot is clean | pixelflux's default damage-based capture never re-sends regions mutter (a GL compositor) composited once and stopped damaging; set `--h264-streaming-mode=true` (`SELKIES_H264_STREAMING_MODE`) so it continuously streams full frames. Not needed for XFCE. **The protocol probe won't catch this** — it only checks H.264 arrives, not frame coherence; verify GNOME with a headed browser. |
| A GTK4 app (Files/nautilus, Text Editor, Settings) shows a stale copy of the desktop instead of its own UI — even in a *server-side* screenshot | GTK4 renders content via GSK's OpenGL renderer, which is garbage under llvmpipe; set `GSK_RENDERER=cairo` in the session env for the software renderer. GTK3 apps (gnome-terminal) and gnome-shell itself (Clutter/Cogl) are unaffected, so the symptom is "some apps render, others are garbage". |
| App/shell icons render as blurry scaled-up blobs while text stays crisp | the gdk-pixbuf SVG loader is missing — `--no-install-recommends` dropped `librsvg2-common`, so Adwaita's scalable-SVG icons can't be rasterized. Install `librsvg2-common` and regenerate the loader cache (`gdk-pixbuf-query-loaders --update-cache`; the dpkg trigger is unreliable in a build layer). |
| GNOME Shell app-switcher/overview backdrop confined to a top-left rectangle at large (HiDPI) resolutions, while windows/panel/desktop are full-size | mutter's overview backdrop on X11 is created at the shell's startup resolution and only *shrinks* with the monitor, never grows; a client driving the framebuffer above that size (HiDPI + a >1920×1080 browser window) leaves the backdrop stuck small. Not scale-related (forcing `scaling-factor=1` doesn't help), not fixable by pre-growing or restarting the shell. Only a **fixed resolution** (no dynamic resize) avoids it — accept the cosmetic quirk or trade away window-matching. Cosmetic: launching apps still works. |

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

What the GNOME spikes established (the losing options were removed with the
guacd/webrtc cleanup — see the banner in [vdi.md](vdi.md) — leaving
[`gnome-selkies2`](../desktops/gnome-selkies2/) as the built answer):

- GNOME Shell **46 is the last gen with an X11 backend**; newer Shell runs only
  headless-Wayland. Selkies 2.x captures the X11 framebuffer, so GNOME 46 on
  Ubuntu 24.04 is what lets us stream the *real* Shell unprivileged.
- On newer bases GNOME Session hard-requires `systemd --user` → systemd-PID1 +
  privileged + Kata coercion (§2), and the session runs as root (glycin/bwrap).
  Pinning to GNOME 46 / 24.04 avoids all of that — hence `gnome-selkies2` needs
  **no `--privileged`**.

**The goal is the real GNOME Shell experience** — Activities, dynamic
workspaces, extensions; as close to a stock desktop as possible. That goal
rules the ranking below: options that substitute the Shell (Flashback) are
fallbacks, not targets.

Candidate shapes for "full GNOME on Selkies 2.x":

1. **GNOME Shell 46 on Xorg (Ubuntu 24.04) + Selkies 2.x** — the only shape
   with the actual `gnome-shell` (not Flashback's Panel+Metacity stand-in)
   *and* the once-encoded H.264-to-browser pipeline (not guacd's
   re-rasterized canvas tiles); every other option gives up one or the
   other. GNOME 46 is the last generation with
   both escape hatches 26.04's GNOME 50 removed: a Shell X11 backend (so
   Xvfb + pixelflux X11 capture just works) and — likely — a session that
   can start without `systemd --user` (distros ran GNOME 46 on elogind), so
   possibly no systemd-PID1/privileged/Kata at all. Costs: GNOME 46 not 50
   (LTS-supported until 2029), and 24.04's libva is 2.20 while pixelflux
   needs ≥ 2.21 — vendor a newer libva (small, dependency-light; a short
   extra build stage) into the image. **Verify early**: (a) session startup
   without systemd — this is the load-bearing assumption for staying
   unprivileged; (b) Shell-on-llvmpipe performance under Xvfb (Shell
   animations are heavier than XFCE/Metacity).
2. **GNOME Flashback on X11 (26.04) + Selkies 2.x** — a straight port of
   gnome-flashback-webrtc onto the xfce-selkies2 recipe. Lowest technical
   risk and current GNOME components, but Panel + Metacity is *not* the
   Shell experience — fallback, not target.
3. **GNOME Shell headless + gnome-remote-desktop** — already prototyped as
   gnome-grd (guacd/RDP viewer, not selkies). Real, current Shell;
   re-rasterized pixels and the systemd-PID1/privileged architecture.
4. **GNOME Shell under Selkies' Wayland mode** — unproven territory: in
   `PIXELFLUX_WAYLAND=true` mode **pixelflux is itself the compositor**
   (clients connect to *its* Wayland socket). GNOME Shell/mutter is also a
   compositor, so it can't simply run as a client; whether a nested-mutter
   arrangement works under pixelflux is an open question to spike
   separately. Selkies' own Wayland desktops use wlroots compositors
   (labwc), not mutter. This is the only shape that could ever deliver a
   *current* Shell over the selkies path — the long-term watch item as
   option 1's GNOME 46 ages.

Recommended order: spike 1 first (it is the only shape that delivers the
actual goal today), with 3 as the real-Shell fallback if its verify items
fail, 2 only if both fail, and a time-boxed look at 4 before GNOME 46's
runway runs out.

### Option 1, built and verified — [`gnome-selkies2`](../desktops/gnome-selkies2/)

Spike 1 is done and streams the real GNOME Shell on llvmpipe/Xvfb, unprivileged.
How the two load-bearing verify items resolved, and the traps that weren't on
the radar:

- **(b) Shell-on-llvmpipe: fine.** Xvfb (`+extension GLX`) + Mesa
  `libgl1-mesa-dri`/`libglx-mesa0` gives llvmpipe GL 4.5; gnome-shell 46
  composits as an X11 WM with no GPU. `LIBGL_ALWAYS_SOFTWARE=1` in the session
  env. No performance wall at 1280×720.
- **(a) Session without `systemd --user`: yes, but not via `gnome-session`.**
  gnome-session 46 *does* fall back to non-systemd startup, but Ubuntu's
  `gnome.session` lists gsd plugins as *required* that can't run headless
  (`gsd-power` needs logind+upower, `gsd-usb-protection` SIGSEGVs, a `pulseaudio`
  autostart collides with ours) — and one crash-looping required component sends
  the whole session to the "Oh no" failed screen. The working shape is to
  **bypass gnome-session**: launch `gnome-shell --x11` + a curated set of gsd
  plugins directly (see the image's `gnome-session-launch.sh`). This keeps it
  unprivileged; the price is logind-only features (lock/suspend/seat switching),
  irrelevant for a single-user stream.
- **libva:** the 24.04-ships-2.20 problem (§6) is solved by building libva 2.22
  from source in a stage and dropping it in `/usr/local/lib` (ahead of `/usr/lib`
  in the ld order) — cheaper than it sounds, ~30 s, no runtime downside.
- **Capture needs `--h264-streaming-mode=true`.** The one finding that only the
  human/headed-browser check surfaced (§5, §6 step 5): pixelflux's damage-based
  default leaves static GNOME windows black on the client because mutter's GL
  compositor stops emitting damage for them. Streaming mode (continuous
  full-frame encode) fixes it. This makes the "static desktop sends ~nothing"
  property (§6 step 2) **false for GNOME** — expect constant traffic — and it's a
  reminder that the scriptable `probe stream` PASS is necessary but not
  sufficient for a GL-compositor DE; a headed browser decoding the real stream
  is the only check that catches frame-coherence bugs.
- **GTK4 apps need `GSK_RENDERER=cairo`** (§5). GTK4's GL renderer is garbage
  under llvmpipe — Files/Text-Editor/Settings show a stale desktop copy instead
  of their UI. The Shell (Clutter/Cogl) and GTK3 apps are fine, so the tell is
  "some windows render, others don't". Also a headed-browser-only find, since a
  server-side X grab shows the same garbage (it's a render bug, not capture).
- **Result:** no `--privileged`, no Kata — the whole point over the 26.04 GNOME
  images. It does need root PID 1 + a handful of caps
  (`CHOWN,DAC_OVERRIDE,FOWNER,SETUID,SETGID`) because it creates the desktop user
  at runtime (configurable UID/GID/sudo), so it's not `--cap-drop=ALL` like
  xfce-selkies2. That's a consequence of the configurable-identity feature, not
  the DE.
