# Plain **GNOME Shell** workload image — the sidecar-mode counterpart of
# ../gnome-selkies2, exactly as ../xfce-plain is to ../xfce-selkies2. No X
# server, no PulseAudio daemon, no Selkies, no vendored libva: the streamer
# sidecar (../streamer-selkies2) owns the whole display plane and this image
# runs the real GNOME Shell session against the injected DISPLAY /
# PULSE_SERVER (shared /tmp/.X11-unix and /tmp/pulse emptyDirs).
#
# NOT Wayland, by architecture: under Wayland, GNOME Shell/mutter *is* the
# display server — there is nothing left for a display-owning sidecar to own
# (and pixelflux's Wayland mode only works when pixelflux itself is the
# compositor, which rules out Shell). So sidecar-GNOME means the Shell's X11
# backend, which is why this stays on **Ubuntu 24.04 / GNOME 46** — the last
# generation with (1) an X11-backend Shell and (2) a gnome-session that runs
# without systemd --user; the full rationale is ../gnome-selkies2/Dockerfile's
# header. A Wayland-native GNOME needs the capture point moved out of X
# entirely (stage 2/3 of the guest-unaware-display plan).
#
# What the sidecar split removes vs ../gnome-selkies2: the selkies web+server
# build stages, Xvfb/PulseAudio and all the X11 injection tooling
# (xclip/xdotool/xcvt/wtype), ssl-cert, the python toolchain — and the whole
# vendored-libva-2.22 stage (that existed only because pixelflux needs
# libva >= 2.21 and 24.04 ships 2.20; pixelflux now lives in the 26.04-based
# streamer, so the constraint left with it).
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

# Package set = ../gnome-selkies2 minus everything the streamer now owns. The
# per-package rationale (why gnome-settings-daemon and librsvg2-common must be
# explicit under --no-install-recommends, why the SVG loader cache is rebuilt,
# etc.) is documented there and applies unchanged.
RUN apt-get update && apt-get install -y --no-install-recommends \
      gnome-session \
      gnome-shell \
      gnome-settings-daemon \
      gnome-terminal \
      nautilus \
      gnome-text-editor \
      gnome-control-center \
      firefox \
      adwaita-icon-theme \
      fonts-ubuntu \
      fonts-dejavu-core \
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
    # lines up by ownership — same contract as ../gnome-selkies2).
    DESKTOP_USER=abc \
    PUID=1000 \
    PGID=1000 \
    DESKTOP_SUDO=false

# The session launcher (gnome-shell + curated settings daemons, not plain
# gnome-session — see the script header for why). Shared verbatim with
# ../gnome-selkies2; keep the two in sync.
COPY gnome-session-launch.sh /usr/local/bin/gnome-session-launch.sh

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/gnome-session-launch.sh

ENTRYPOINT ["/entrypoint.sh"]
