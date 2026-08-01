# Plain **GNOME Shell** workload image — the real Shell (Activities, dynamic
# workspaces, extensions) with no display plane inside. No X server, no
# PulseAudio daemon, no Selkies, no vendored libva: the streamer sidecar
# (../streamer-selkies2) owns all of that and this image runs the GNOME
# session against the injected DISPLAY / PULSE_SERVER (shared /tmp/.X11-unix
# and /tmp/pulse emptyDirs).
#
# NOT Wayland, by architecture: under Wayland, GNOME Shell/mutter *is* the
# display server — there is nothing left for a display-owning sidecar to own
# (and pixelflux's Wayland mode only works when pixelflux itself is the
# compositor, which rules out Shell). A Wayland-native GNOME needs the capture
# point moved out of X entirely (stage 2/3 of the guest-unaware-display plan).
#
# Why Ubuntu 24.04 / GNOME 46 specifically (NOT 26.04 like the other images):
# GNOME 46 is the last generation with the two escape hatches GNOME 50 (26.04)
# removed —
#   1. GNOME Shell still has an **X11 backend** (`gnome-shell --x11`), so it
#      can composit a foreign X server's display. On 26.04's GNOME 50 mutter
#      only has `--wayland`, and per the above that can never pair with a
#      display-owning sidecar.
#   2. GNOME 46's gnome-session starts **without `systemd --user`**: when
#      systemd is not PID 1 (`sd_booted()` false) it uses its built-in
#      component launcher instead of session .target units. GNOME 50 made
#      systemd --user a hard requirement ("Failed to obtain session bus"
#      otherwise), which would force systemd-PID1 + `--privileged` + Kata.
#      Staying on 46 keeps this image unprivileged with a plain entrypoint.
# 24.04 also dodges 26.04's mandatory glycin/bwrap image-loading sandbox
# (which needs unprivileged userns) — on 24.04 gdk-pixbuf still loads images
# in-process, so the session runs as an unprivileged user.
#
# (The embedded ancestor gnome-selkies2 — same session architecture with
# Selkies/Xvfb/Pulse and a vendored libva 2.22 baked in — is in git history;
# the libva vendoring died with it: pixelflux needs libva >= 2.21 and lives in
# the 26.04-based streamer now.)
#
# IMPORTANT pairing note: templates using this image must set
#   streamerEnv: { SELKIES_H264_STREAMING_MODE: "true" }
# — mutter is a GL compositor and emits no damage for static content, so the
# streamer's default damage-based capture leaves static windows black.

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# --- Firefox from Mozilla's APT repo, NOT Ubuntu's ---------------------------
# On 24.04 the `firefox` apt package is a transitional shim that installs the
# Firefox *snap*, and snapd does not run inside a container. Mozilla's own APT
# repo ships a real .deb; the pin keeps the snap shim from winning.
RUN apt-get update && apt-get install -y --no-install-recommends wget gpg ca-certificates \
 && install -d -m 0755 /etc/apt/keyrings \
 && wget -qO- https://packages.mozilla.org/apt/repo-signing-key.gpg > /etc/apt/keyrings/packages.mozilla.org.asc \
 && echo "deb [signed-by=/etc/apt/keyrings/packages.mozilla.org.asc] https://packages.mozilla.org/apt mozilla main" \
      > /etc/apt/sources.list.d/mozilla.list \
 && printf 'Package: *\nPin: origin packages.mozilla.org\nPin-Priority: 1000\n' \
      > /etc/apt/preferences.d/mozilla

RUN apt-get update && apt-get install -y --no-install-recommends \
      # gnome-session provides the session manager; gnome-shell is mutter +
      # the Shell UI (X11 backend still present on 46). gnome-settings-daemon
      # is a soft dep pinned explicitly so --no-install-recommends can't drop
      # it (a Shell with no settings-daemon has no cursor theme / xsettings
      # and looks broken).
      gnome-session \
      gnome-shell \
      gnome-settings-daemon \
      # Minimal application set: Terminal, Files, Text Editor, Settings —
      # usable but deliberately small; firefox from the Mozilla repo above.
      gnome-terminal \
      nautilus \
      gnome-text-editor \
      gnome-control-center \
      firefox \
      # Ubuntu's wallpaper set, including the release-independent default
      # aliases the schema override below points at. Nothing in gnome-shell
      # pulls this in — without it the desktop is stock Adwaita.
      ubuntu-wallpapers \
      # glib-compile-schemas, for that override. Not in the base image, and
      # not guaranteed by anything above.
      libglib2.0-bin \
      # Adwaita icon/cursor theme + sane default fonts so the Shell isn't a
      # sea of missing-icon boxes.
      adwaita-icon-theme \
      fonts-ubuntu \
      fonts-dejavu-core \
      # librsvg2-common provides the gdk-pixbuf *SVG loader*. Adwaita's icons
      # are scalable SVGs; without it every icon renders as a tiny bitmap
      # scaled to a blurry blob. Normally pulled via Recommends, which
      # --no-install-recommends drops, so it MUST be explicit — and the loader
      # cache is regenerated below because the dpkg trigger is unreliable in a
      # docker build layer (loader present on disk but not in loaders.cache =
      # still blurry).
      librsvg2-common \
      # Software OpenGL: mutter hard-requires GL; llvmpipe renders client-side
      # here and presents via the *streamer's* Xvfb GLX (started with
      # +extension GLX for exactly this).
      libgl1-mesa-dri \
      libglx-mesa0 \
      mesa-utils \
      dbus \
      dbus-x11 \
      gnome-keyring \
      sudo \
      # Client-side audio tools only (pactl for the gsd-sound path and
      # debugging); the PulseAudio *daemon* lives in the streamer.
      pulseaudio-utils \
      # xdpyinfo for the entrypoint's X-ready wait (outside k8s there is no
      # startupProbe gating us).
      x11-utils \
      ca-certificates \
 && /usr/lib/x86_64-linux-gnu/gdk-pixbuf-2.0/gdk-pixbuf-query-loaders --update-cache \
 && rm -rf /var/lib/apt/lists/*

ENV DISPLAY=:0 \
    # Runtime identity (applied by the entrypoint so a mounted home volume
    # lines up by ownership).
    DESKTOP_USER=abc \
    PUID=1000 \
    PGID=1000 \
    DESKTOP_SUDO=false

# System-wide desktop defaults (Ubuntu's wallpaper — see the file's header for
# why an unqualified override is the only thing that applies to this session).
# --strict so a key that stops existing after a package bump fails the build;
# the test -f guards the half --strict cannot see, a URI aimed at a path that
# no longer exists, whose only symptom is a black desktop at runtime.
COPY 90_whistler-desktop.gschema.override /usr/share/glib-2.0/schemas/
RUN test -f /usr/share/backgrounds/warty-final-ubuntu.png \
 && test -f /usr/share/backgrounds/ubuntu-wallpaper-d.png \
 && glib-compile-schemas --strict /usr/share/glib-2.0/schemas

# The session launcher (gnome-shell + curated settings daemons, not plain
# gnome-session — see the script header for why).
COPY gnome-session-launch.sh /usr/local/bin/gnome-session-launch.sh

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/gnome-session-launch.sh

ENTRYPOINT ["/entrypoint.sh"]
