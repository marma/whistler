# gnome-plain — display-unaware GNOME Shell workload image

The real GNOME Shell (Activities, dynamic workspaces) with nothing
display-protocol-specific inside: no X server, no PulseAudio daemon, no
Selkies, no vendored libva. Pairs with the
[`../streamer-selkies2`](../streamer-selkies2/) sidecar, which shares
`/tmp/.X11-unix` / `/tmp/pulse` and injects `DISPLAY` / `PULSE_SERVER`.

Two things to know beyond the xfce-plain model:

1. **X11, not Wayland — by architecture.** Under Wayland, GNOME Shell *is* the
   display server, so there is nothing for a display-owning sidecar to own.
   Hence Ubuntu 24.04 / GNOME 46 (last X11-backend Shell that also runs
   without systemd-PID1 — full rationale in the [Dockerfile](Dockerfile)
   header). Wayland-native GNOME needs a non-X capture point (later stage of
   the guest-unaware-display plan).
2. **Templates must set** `streamerEnv: { SELKIES_H264_STREAMING_MODE: "true" }`.
   mutter is a GL compositor and emits no damage for static content; with the
   streamer's default damage-based capture, static windows render **black** on
   the client. (The `gnome-sidecar` sample template in the chart does this.)

Shell rendering is Mesa llvmpipe *in this container*, presented via the
streamer's Xvfb GLX (the streamer starts Xvfb with `+extension GLX` for
exactly this). MIT-SHM between the containers needs a shared IPC namespace —
free inside a pod, explicit `ipc:` wiring in compose.

The desktop shows **Ubuntu's default wallpaper** rather than upstream Adwaita:
`ubuntu-wallpapers` plus
[`90_whistler-desktop.gschema.override`](90_whistler-desktop.gschema.override).
Ubuntu ships its own background defaults in `ubuntu-settings`, but every stanza
there is `:ubuntu`-qualified and this session is plain `gnome-shell --x11` with
`XDG_CURRENT_DESKTOP=GNOME`, so only an unqualified override applies. It is a
default, not a lock — a user changing the wallpaper in Settings wins and
persists in the mounted home. The VM sibling
[`../vm-gnome-selkies/guest/usr/share/glib-2.0/schemas/90_whistler-desktop.gschema.override`](../vm-gnome-selkies/guest/usr/share/glib-2.0/schemas/90_whistler-desktop.gschema.override)
carries the same background stanzas plus Ubuntu Dock, which this image does not
install.

Identity is runtime-configurable
(`DESKTOP_USER`/`PUID`/`PGID`/`DESKTOP_SUDO`); the known GNOME-46 HiDPI
overview-backdrop limitation documented in
[`gnome-session-launch.sh`](gnome-session-launch.sh) applies unchanged.

Run locally without a cluster: `make desktop-gnome-sidecar-local` (see
[`../streamer-selkies2/README.md`](../streamer-selkies2/README.md)).
