# gnome-flashback-webrtc — GNOME Flashback over WebRTC (Selkies), Ubuntu 26.04

GNOME Flashback (GNOME Panel + Metacity — real GNOME technologies and Yaru
theming, on X11) streamed to the browser over **real WebRTC** via
[Selkies-GStreamer](https://github.com/selkies-project/selkies-gstreamer),
software-encoded with **x264** (no GPU). This is the GNOME analogue of
[`../xfce-webrtc`](../xfce-webrtc/), on Ubuntu 26.04. See `design/vdi.md` →
*Round 3* for the `viewer: webrtc` architecture and why a TURN server (coturn)
is mandatory.

| | |
|---|---|
| Viewer | `webrtc` |
| Signaling port | `8082` (must match the template's `signalPort`) |
| Encoder | `x264enc` (software, no GPU) |
| Privileged | **yes** (systemd as PID 1 — see below) |
| Media path | browser ⇄ coturn ⇄ pod (DTLS/SRTP); portal relays only signaling |

## Why this image looks different from xfce-webrtc

Getting a GNOME desktop onto this pipeline took three wrong turns, each worth
knowing before touching this image:

1. **GNOME Shell can't be used at all.** Selkies captures via `ximagesrc`,
   which needs a real X11 display. GNOME Shell on this GNOME version has
   dropped its X11/nested backend entirely (`mutter --help` only offers
   `--wayland` now) — there's no `xsessions/*.desktop` for it anymore, only
   `wayland-sessions/ubuntu.desktop`. `gnome-session-flashback` is still
   shipped and is genuinely X11-native (GNOME Panel + Metacity), so that's
   what this image runs instead of the dynamic-workspaces shell.
2. **GNOME Session needs `systemd --user`.** `gnome-session` (v50) aborts
   immediately ("Failed to obtain session bus") without a real `systemd --user`
   instance, and `systemd --user` refuses to start unless the container was
   booted with systemd as PID 1. There's no flag to disable this. So — unlike
   xfce-webrtc — this image needs the same systemd-PID1 / `--privileged`
   (Kata-isolated in prod) architecture as [`../gnome-grd`](../gnome-grd/),
   with Xvfb+Selkies subbed in for gnome-shell's Wayland+grd path. See
   `gnome-desktop.service`/`session.sh` for the PAM/logind plumbing that
   provides that bus.
3. **GNOME's image loading needs root.** Every image load in this Ubuntu
   version's GTK (icons included) is sandboxed through `glycin-loaders` via
   `bwrap` — it's a hard dependency of `libgdk-pixbuf-2.0-0`, not optional.
   `bwrap`'s sandbox setup needs an unprivileged user+network namespace, which
   many hosts block for non-root callers via
   `kernel.apparmor_restrict_unprivileged_userns=1` — even inside a privileged
   container, since that's an AppArmor confinement check, not a container
   capability. Without it, `metacity`/`gnome-panel`/`gnome-flashback` treat a
   failed icon load as fatal and abort in a crash loop. Real root bypasses
   that specific kernel check, so `gnome-desktop.service` runs the session as
   **root**, not an unprivileged desktop user like xfce-webrtc/gnome-grd use.
   We're already inside a systemd-PID1/privileged container, so this isn't a
   new trust boundary — just a different uid inside one that already has full
   host-kernel-mediated access.

Also unlike xfce-webrtc: Selkies' prebuilt GStreamer release only ships
ubuntu20.04/22.04/24.04 builds (no 26.04 asset), so this image uses **Ubuntu
26.04's own apt GStreamer (1.28)** instead of Selkies' bundle — it's new
enough to cover everything Selkies needs (`ximagesrc`, `x264enc`, `webrtcbin`,
`pulsesrc`, `gstreamer1.0-nice`). Two non-obvious packages were needed beyond
the plugin sets themselves (see Dockerfile comments for the full story):
`python3-gst-1.0` (GStreamer's PyGObject overrides — without it,
`Gst.Fraction(60, 1)` raises `TypeError: Fraction() takes no arguments`) and
`python3-setuptools` (restores `import distutils`, removed from the stdlib in
Python 3.12+, which `selkies_gstreamer`'s `GPUtil` dependency needs). Selkies
1.6.2 also calls the old `asyncio.get_event_loop()` idiom, which Python 3.14
turned into a hard `RuntimeError`; the Dockerfile patches both call sites to
`new_event_loop()`+`set_event_loop()`.

Because there's no prebuilt-binary dependency, this image is **not** pinned to
`linux/amd64` like xfce-webrtc — it should build multi-arch, though only
amd64 has been verified.

## Build

```bash
docker build \
  --build-arg SELKIES_VERSION=1.6.2 \
  -t ghcr.io/marma/whistler-desktop-gnome-flashback-webrtc:dev \
  desktops/gnome-flashback-webrtc
```

## Local test (no cluster, no portal, no shared coturn)

Like xfce-webrtc, the image serves its own signaling + HTML5 client, so you
can validate "does this stream a desktop over WebRTC" without k3d or the
portal — but it needs `--privileged` (systemd as PID 1, see above):

```bash
docker run --rm -it --privileged \
  -p 8082:8082 \
  -p 3478:3478/udp -p 3478:3478/tcp \
  -p 49160-49200:49160-49200/udp \
  -e SELKIES_USE_INTERNAL_TURN=1 \
  ghcr.io/marma/whistler-desktop-gnome-flashback-webrtc:dev
```

Open <http://localhost:8082/>. On a Linux host with `--network host` instead
of the TURN/port-publish flags above, ICE connects direct with no relay
needed — see xfce-webrtc's README for the full Linux-vs-Docker-Desktop
explanation (it applies unchanged here).

Verify the session came up cleanly (no crash-loop) before chasing anything in
the browser:

```bash
docker exec <container> systemctl status gnome-desktop.service
docker exec <container> bash -c \
  'export XDG_RUNTIME_DIR=/run/user/0 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/0/bus; systemctl --user --failed'
```

`NRestarts` on `gnome-desktop.service` should stay at `0` and `--failed`
should list nothing. If `metacity.service`/`gnome-panel.service` show up
there, you've likely hit the AppArmor userns restriction described above on a
host that doesn't allow it even for root — check
`journalctl -u gnome-desktop.service`.

## Version lock (read before bumping Selkies)

Same rule as xfce-webrtc: the in-pod Selkies **server** and the Selkies
**client JS** the portal serves (`whistler/portal/static/selkies-core.js`)
must be the same Selkies version. See that README's "Version lock" section
for the re-vendoring steps — identical here, just point `docker create` at
this image instead.

> **Status:** EXPERIMENTAL. Verified: clean systemd boot, GNOME Flashback
> renders (panel, desktop icons, Metacity) confirmed via framebuffer
> screenshot, Selkies signaling/web server comes up and serves the client
> page. Full browser WebRTC media e2e needs a headed browser + real TURN and
> hasn't been verified here, same caveat as xfce-webrtc.

## Run it via Whistler

1. Enable coturn and point it at a browser-reachable address:
   ```bash
   helm upgrade ... \
     --set coturn.enabled=true \
     --set coturn.externalHost=<node-ip-or-lb>
   ```
2. The `gnome-flashback-webrtc-desktop` template ships in
   `charts/whistler/values.yaml` (`desktopTemplates`). Launch it from the
   portal and open `/connect/<id>`.

## TURN

Same contract as xfce-webrtc: Selkies reads `SELKIES_TURN_HOST` /
`SELKIES_TURN_PORT` / `SELKIES_TURN_PROTOCOL` / `SELKIES_TURN_SHARED_SECRET`,
injected by the operator from the chart's `coturn` values
(`whistler/config.py` → `_selkies_turn_env`); the portal mints a matching
credential for the browser at `/ice`.
