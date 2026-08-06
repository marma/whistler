# Creating desktop images

Practical guidelines for adding a new **workload image** under
[`desktops/`](../desktops/) and for maintaining the **streamer sidecar**
([`desktops/streamer-selkies2`](../desktops/streamer-selkies2/)), distilled
from building the catalog and the guacd/RDP, Selkies-1.x/WebRTC, and embedded
Selkies-2.x rounds that preceded it (all removed — see the banner in
[`design/vdi.md`](vdi.md); recoverable from git history). Read together with
[`desktops/README.md`](../desktops/README.md) (the catalog + conventions).

The display model is fixed: every desktop pod = one display-unaware workload
container + the streamer sidecar. (`runtime: vm` desktops are the one
exception — the sidecar can't cross the VM boundary, so
[`desktops/vm-xfce-selkies`](../desktops/vm-xfce-selkies/) bakes the same
stack into the guest; its README covers the differences. §§1, 4 and 5 apply
to its in-guest streamer all the same.) A **workload image** needs none of the
streaming stack — no Selkies, no Xvfb, no PulseAudio daemon; just a DE/app
plus a session entrypoint against the injected `DISPLAY`/`PULSE_SERVER`
([`desktops/xfce-plain`](../desktops/xfce-plain/) is the minimal model,
[`desktops/gnome-plain`](../desktops/gnome-plain/) the full-DE one). The
streaming sections below (§1, §4, most of §5) concern the **streamer image**;
the session/DE sections (§2, the GNOME notes) concern workload images.

## 1. The streaming stack: Selkies 2.x / pixelflux (streamer image)

There is one stack, and it lives in one image: **Selkies 2.x (pixelflux)** in
`streamer-selkies2`, spoken to by the portal's **`websockets`** viewer. H.264
reaches the browser's decoder over a single TCP port (plain WebSockets), no
coturn/TURN, multi-arch (amd64+arm64), unprivileged. The alternatives that
were evaluated and dropped — guacd/RDP (re-rasterized frames, extra daemon)
and Selkies 1.x + WebRTC/coturn (amd64-only, needs a TURN relay) — are
recorded in [`vdi.md`](vdi.md); the one thing guacd/VNC offered that Selkies
can't is *agentless* VM-console capture (KubeVirt QEMU framebuffer), if that
is ever needed.

The Selkies 2.x stack is unreleased upstream: pin `SELKIES_COMMIT` and build the
Python server **and** the web client from that same commit (see the
streamer-selkies2 Dockerfile) — client/server version lock by construction. Treat
`pixelflux`/`pcmflux` (PyPI, linuxserver-maintained) as pinned upstream *code*
dependencies; we do not consume anyone's prebuilt desktop images.

**`--encoder` is an output mode, not an encoder.** Selkies' enum is only
`{x264enc, x264enc-striped, jpeg}` = full-frame H.264 / striped H.264 / JPEG,
and the name it reports never changes. The *backend* is a separate setting,
`--use-cpu` / `SELKIES_USE_CPU`, which defaults to **false** and which none of
our streamers set — so pixelflux tries **NVENC** (CUDA driver API), then
**VA-API** (`/dev/dri` render node), and only then its bundled libx264. Consequences
worth remembering before reading a profile or a bug report:

- A session with a GPU **encodes on the GPU** while still displaying encoder
  `x264enc`. GPU utilisation under a "software" encoder name is correct
  behaviour, not a misconfiguration. This needs the encode libraries present:
  pods get them from the nvidia runtime class (`resources.gpu`), VMs from the
  `-cuda` image's baked driver (`libnvidia-encode`) — so the CUDA variant of
  a VM image is *not* display-neutral.
- VA-API never applies to pods: `_build_pod_spec` exposes no `/dev/dri`.
- `jpeg` and `x264enc-striped` force `use_cpu=true` server-side — always CPU.
- Only the encode stage can move. Capture is XShm from Xvfb, and DE rendering
  stays llvmpipe (Xvfb's GLX is Mesa swrast; a passthrough GPU does not change
  that) — so "has a GPU" never means "renders on the GPU" in this architecture.
- The tell is in the streamer's log: `NVENC Encoder Initialized successfully.` /
  `VAAPI Encoder Initialized successfully.` / `... Falling back to x264|CPU`.

## 2. Choose the process architecture (workload image)

Two patterns exist; the DE dictates which one you get, not preference:

- **Plain entrypoint** (xfce-plain, gnome-plain): the entrypoint waits for the
  shared display (a no-op in-cluster — the sidecar's startupProbe gates it),
  starts the session plumbing (system dbus, identity), and execs the DE
  session in the foreground so the container lifecycle tracks it.
  Unprivileged. Always prefer this when the DE allows it.
- **systemd as PID 1**: required the moment the session needs
  `systemd --user` — modern GNOME Session hard-requires it with no opt-out.
  This forces `privileged: true` on the template, which production coerces to
  the Kata runtime (`forceKataForPrivileged`). Accept this only when the DE
  leaves no choice, and say so in the Dockerfile header.

Rule of thumb from the catalog so far: **privilege requirements come from the
DE, never from the display layer** (which isn't even in the workload image
anymore). If your image needs a capability, the justification must name the
DE component that demands it (e.g. GNOME 50's glycin/bwrap image-loading
sandbox needing real root under `apparmor_restrict_unprivileged_userns=1`, a
systemd-PID1 base — which is why `gnome-plain` deliberately pins GNOME 46 /
Ubuntu 24.04 to avoid it).

## 3. Image anatomy and conventions

- One directory per catalog entry: `Dockerfile`, `entrypoint.sh` (or systemd
  units), `README.md`.
- **Cluster-free testable**: the workload image must be verifiable by pairing
  it with the streamer in plain docker — the parameterized
  [`desktops/compose-sidecar.yaml`](../desktops/compose-sidecar.yaml) +
  `make desktop-*-sidecar-local` targets do this. This is the bottom of the
  test pyramid.
- The session self-starts as the entrypoint (foreground); the pod spec
  overrides no command. Include the wait-for-X + fail-loudly guard for
  non-k8s runs (copy it from xfce-plain).
- Fixed well-known user (`abc`, or gnome-plain's runtime `PUID`/`PGID`
  identity) and no desktop login; the per-session NetworkPolicy is the
  security boundary.
- **Multi-arch (amd64+arm64) unless a dependency forbids it**; don't add
  `--platform` pins without a reason documented in the header.
- Base on **Ubuntu 26.04** for new workload images unless the DE forces
  otherwise (gnome-plain's GNOME-46 pin is the documented exception; the
  libva ≥ 2.21 constraint applies only to the streamer image).
- Comment discipline: every non-obvious package and entrypoint line carries
  the *why*, including the failure mode it prevents. The Dockerfiles are the
  institutional memory of this catalog — the gotchas below were all
  expensive to find and cheap to write down.

## 4. Runtime assembly checklist (streamer image, Selkies 2.x / X11)

The pieces the streamer's entrypoint provides, and the traps in each (kept
here as the maintenance guide for `streamer-selkies2`):

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
  [`desktops/streamer-selkies2/wtype-x11-shim`](../desktops/streamer-selkies2/wtype-x11-shim).
  Only a–z is injected via XTEST directly; the other two paths swallow their
  errors, so missing binaries present as "only letters type".
- **Clipboard**: `xclip` (2.x; 1.x used `xsel`). A missing xclip kills the
  per-client WS loop on connect — presents as "video never starts".
- **Python runtime**: selkies in its own venv; add `setuptools` explicitly
  (GPUtil imports `distutils`, gone since Python 3.12).
- **Session**: lives in the *workload* container — the streamer only serves
  the display (`Xvfb -ac`; any process that reaches the shared socket may
  connect, the pod is the trust boundary) and runs selkies as container-root.
  Cross-container GL (GNOME/mutter) additionally needs Xvfb's
  `+extension GLX` and the pod's shared IPC namespace for MIT-SHM (compose
  needs explicit `ipc:` pairing).
- **Flags**: selkies 2.x CLI flags are dash-separated (`--web-root`) and the
  parser uses `parse_known_args` — **misspelled flags are ignored silently**
  and env vars (`SELKIES_*`) are overridden by explicit flags. Basic auth is
  ON by default; disable it explicitly.
- **HTTPS**: the 2.x client hard-requires a browser secure context (WebCodecs
  doesn't exist outside one). `http://localhost` qualifies; anything else in
  dev needs `SELKIES_ENABLE_HTTPS=true` (images bake Debian's `ssl-cert`
  snakeoil pair, which selkies' cert-path defaults point at). In-cluster the
  portal's own HTTPS origin covers it — but this makes portal-HTTPS a hard
  requirement of the websockets viewer (now wired: the portal reverse-proxies
  the in-pod Selkies server under `/desktop/<id>/`).

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
| GNOME Shell: client shows black static windows / one window's content bleeds into another, but a server-side X screenshot is clean | pixelflux's default damage-based capture never re-sends regions mutter (a GL compositor) composited once and stopped damaging; set `SELKIES_H264_STREAMING_MODE: "true"` in the template's `streamerEnv` so it continuously streams full frames. Not needed for XFCE. **The protocol probe won't catch this** — it only checks H.264 arrives, not frame coherence; verify GNOME with a headed browser. |
| A GTK4 app (Files/nautilus, Text Editor, Settings) shows a stale copy of the desktop instead of its own UI — even in a *server-side* screenshot | GTK4 renders content via GSK's OpenGL renderer, which is garbage under llvmpipe; set `GSK_RENDERER=cairo` in the session env for the software renderer. GTK3 apps (gnome-terminal) and gnome-shell itself (Clutter/Cogl) are unaffected, so the symptom is "some apps render, others are garbage". |
| App/shell icons render as blurry scaled-up blobs while text stays crisp | the gdk-pixbuf SVG loader is missing — `--no-install-recommends` dropped `librsvg2-common`, so Adwaita's scalable-SVG icons can't be rasterized. Install `librsvg2-common` and regenerate the loader cache (`gdk-pixbuf-query-loaders --update-cache`; the dpkg trigger is unreliable in a build layer). |
| GNOME Shell app-switcher/overview backdrop confined to a top-left rectangle at large (HiDPI) resolutions, while windows/panel/desktop are full-size | mutter's overview backdrop on X11 is created at the shell's startup resolution and only *shrinks* with the monitor, never grows; a client driving the framebuffer above that size (HiDPI + a >1920×1080 browser window) leaves the backdrop stuck small. Not scale-related (forcing `scaling-factor=1` doesn't help), not fixable by pre-growing or restarting the shell. Only a **fixed resolution** (no dynamic resize) avoids it — accept the cosmetic quirk or trade away window-matching. Cosmetic: launching apps still works. |
| Ubuntu-branded defaults (wallpaper, dock, Yaru) simply don't apply, though the packages are installed | Ubuntu ships them as **`:ubuntu`-qualified** stanzas (`10_ubuntu-settings.gschema.override`, `10_ubuntu-dock.gschema.override`), which only apply under `GNOME_SHELL_SESSION_MODE=ubuntu`. These images run plain user-mode `gnome-shell` with `XDG_CURRENT_DESKTOP=GNOME` (the `ubuntu` mode SIGSEGVs without Yaru — see above), so each wanted default must be re-declared **unqualified** in our own `90_whistler-desktop.gschema.override`. |
| Desktop is solid **black** behind the shell (windows and panel fine) | `org.gnome.desktop.background picture-uri` points at a file that isn't there — e.g. `ubuntu-wallpapers` not installed (nothing in `gnome-shell` pulls it in), or a wallpaper renamed by a release bump. `glib-compile-schemas --strict` validates key *names* only, never URI targets, so this ships green; `test -f` the wallpapers next to the compile step. |
| XFCE ignores a system-wide icon theme: `xfconf-query -c xsettings -p /Net/IconThemeName` in the session reports something you never set (`elementary-xfce-dark`) | two different shadowings, and the query tells you which. **`/etc/xdg/…/xsettings.xml` is a dpkg conffile of `xfce4-settings`** already containing `elementary-xfce-dark` (a theme that only ships with `elementary-xfce-icon-theme`, a Recommends `--no-install-recommends` drops — so stock Ubuntu XFCE has always fallen through to Adwaita here). Our override has to *replace* that file, after the apt/bake step that installs it; adding a file alongside achieves nothing. If `/etc/xdg` is correct and the session still disagrees, it's the **persistent home**: xfconfd rewrites the whole channel into `~/.config/xfce4/xfconf/xfce-perchannel-xml/xsettings.xml` the first time any property in it changes, freezing whatever the default was *then*. `xfconf-query … -s Yaru` fixes it live; deleting the user file restores inheritance. Same trap applies to `xfce4-desktop.xml` and the wallpaper. |
| A GNOME session ignores *every* appearance default we ship (icons, GTK theme, font) and instead looks like **XFCE** — `dconf dump /org/gnome/desktop/interface/` shows `gtk-theme='Greybird'`, `icon-theme='elementary-xfce-dark'`, `font-name='Sans 10'` | the user ran an **XFCE template with the same `$HOME`** and it wrote those `org.gnome.desktop.*` keys into the shared dconf (Xfce 4.20 syncs its settings into the GNOME schemas for cross-toolkit consistency — harmless on a single-desktop machine, not here). Per-user dconf beats any `90_whistler-desktop.gschema.override`, so the pollution is permanent and follows the user across templates. Confirmed on a `vm-gnome-selkies` session whose home had previously run `vm-xfce-selkies`. Clear with `dconf reset /org/gnome/desktop/interface/<key>` or, if the whole set is Xfce leftovers, `rm ~/.config/dconf/user` — **with the session stopped**, and note the desktop must be restarted before the change is picked up. Suspect this first whenever a default "doesn't apply" for one user but works on a fresh home. |
| Icons are plain **Adwaita** though a theme (`Yaru`) is selected, with no warning anywhere | the name doesn't resolve to a directory under `/usr/share/icons` — package not installed (`yaru-theme-icon` is pulled in by nothing), or a typo. Both GNOME's `org.gnome.desktop.interface icon-theme` and XFCE's xsettings `Net/IconThemeName` are unvalidated strings, and GTK's built-in Adwaita fallback makes "wrong name" and "no override at all" look identical. `test -d /usr/share/icons/<Theme>` at build/bake time; in a live session `gsettings get org.gnome.desktop.interface icon-theme` / `xfconf-query -c xsettings -p /Net/IconThemeName` tells you what was asked for, not what was found. That same Adwaita fallback is why `adwaita-icon-theme` should stay installed: Yaru inherits `Humanity,hicolor`, so it is GTK's fallback, not the inheritance chain, that covers icons Yaru lacks. |
| XFCE keeps the stock Xfce backdrop despite a system-wide `xfce4-desktop.xml` default | the backdrop property path embeds the **RandR output name** (`/backdrop/screen0/monitor<output>/workspace0/last-image`) and yours doesn't match — on Xvfb the single output is always literally `screen`, so the path is `monitorscreen`. Same silence if `last-image` names a missing file. Check with `xfconf-query -c xfce4-desktop -lv` inside a running session. |
| Session crash-loops **only in Kubernetes** (fine under `docker run`/compose, same image): GTK warns "Could not load a pixbuf from icon theme … pixbuf loaders or the mime database could not be found", then `Wnck:ERROR:…default_icon_at_size: assertion failed: (base)` aborts xfce4-panel | Ubuntu 26.04's gdk-pixbuf decodes via **glycin**, which runs each decode in a nested bwrap/user-namespace sandbox. Under containerd/k3d the sandbox fails (`bwrap: loopback: Failed RTM_NEWADDR: Operation not permitted`) with an error string glycin's blocked-sandbox detection does **not** recognize, so it never falls back to unsandboxed loading — every decode returns NULL. Fix in the image: `dpkg-divert` bwrap to a stub that fails with `"Creating new namespace failed"` (a string glycin does recognize) → loaders run unsandboxed; the pod is the sandbox (see `xfce-plain/Dockerfile`). Applies to any 26.04-based workload image with GTK. |

When you hit a new one: fix it, then add the symptom→cause line here and the
*why* comment at the fix site.

## 6. Verification

Work up this ladder before calling an image done; each step catches what the
previous can't. [`scripts/selkies2_probe.py`](../scripts/selkies2_probe.py)
automates the protocol steps (run it inside the *streamer* container — that's
where the selkies venv lives):

1. **Boots + pairs**: bring the pair up with the compose file
   (`make desktop-<name>-sidecar-local` or `SIDECAR_WORKLOAD=<name> docker
   compose -f desktops/compose-sidecar.yaml up --build`), `curl` the index
   (HTTP 200), scan both containers' logs for tracebacks, and confirm the
   workload's windows exist in the streamer's X
   (`docker compose ... exec streamer xdotool search --name '' getwindowname %@`).
2. **Streams**: `selkies2_probe.py stream` in the streamer container — must
   print `PASS` (VIDEO_STARTED + large type-4 H.264 stripes + AUDIO_STARTED).
   A static desktop sending ~nothing is *correct* in damage mode; with
   `SELKIES_H264_STREAMING_MODE=true` (GNOME) expect constant traffic instead.
3. **Types**: focus a terminal running `cat > /tmp/typed.txt` in the desktop,
   run `selkies2_probe.py keys`, and confirm the file contains exactly
   `a 5!\n` — one key per injection path.
4. **Least privilege**: run the *workload* container with `--cap-drop=ALL
   --security-opt=no-new-privileges` where its identity model allows (fixed-
   user images like xfce-plain should survive; gnome-plain's runtime
   PUID/PGID setup documents its needed caps). If it fails, either remove the
   dependency or document precisely which DE component needs what.
5. **Human check**: the make target + a real browser — latency/fidelity,
   typing into a terminal, window resize, audio, and (GL compositors) frame
   coherence of *static* windows, which no script catches.
6. **Cluster**: only after all of the above — template in `values.yaml`
   (with any required `streamerEnv`), `skaffold dev`, connect through the
   portal.

Media quality (step 5) is the only step that genuinely needs eyes; everything
else is scriptable and should stay scripted so the next stack bump can re-run
it.

## 7. Wiring a finished image into the product

Checklist of files to touch (grep for `xfce-plain` to see a complete example):

- `desktops/<name>/` — the image itself.
- `Makefile` — a `desktop-<name>-sidecar-local` target (a thin wrapper
  setting `SIDECAR_WORKLOAD`/`SIDECAR_WORKLOAD_IMAGE` for the shared compose
  file) + `.PHONY`.
- `desktops/README.md` — catalog table row.
- `charts/whistler/values.yaml` — `templates` entry (image, `displayPort`,
  any required `streamerEnv` with its why-comment, resources) and the
  `whistler.images.desktop` allow-list.
- `skaffold.yaml` — build artifact + template image override + allow-list
  override for local dev.
- Portal/CRD — only if the image needs a new `viewer:` type or a new
  streamer-level knob (add it as env in the streamer image and pass it via
  `streamerEnv`, not as a new CRD field, unless the operator must act on it).

## 8. Notes for the full-GNOME image

What the GNOME spikes established (the losing options were removed with the
guacd/webrtc cleanup — see the banner in [vdi.md](vdi.md) — leaving GNOME
Shell 46 on X11 as the built answer: today
[`gnome-plain`](../desktops/gnome-plain/) + the streamer sidecar; its embedded
ancestor `gnome-selkies2` is in git history):

- GNOME Shell **46 is the last gen with an X11 backend**; newer Shell runs only
  headless-Wayland. Selkies 2.x captures the X11 framebuffer, so GNOME 46 on
  Ubuntu 24.04 is what lets us stream the *real* Shell unprivileged.
- On newer bases GNOME Session hard-requires `systemd --user` → systemd-PID1 +
  privileged + Kata coercion (§2), and the session runs as root (glycin/bwrap).
  Pinning to GNOME 46 / 24.04 avoids all of that — hence `gnome-plain` needs
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

### Option 1, built and verified — today [`gnome-plain`](../desktops/gnome-plain/) (+ streamer sidecar)

Spike 1 was built as the embedded `gnome-selkies2` (git history) and then
split into `gnome-plain` + the streamer; it streams the real GNOME Shell on
llvmpipe/Xvfb, unprivileged. How the two load-bearing verify items resolved,
and the traps that weren't on the radar:

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
- **libva:** the embedded image had to vendor libva 2.22 from source because
  pixelflux needs ≥ 2.21 and 24.04 ships 2.20. **Obsolete since the sidecar
  split**: pixelflux lives in the 26.04-based streamer, so the GNOME workload
  image carries no libva hack at all.
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
