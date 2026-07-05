# Full **GNOME Shell** desktop streamed via **Selkies 2.x** (pixelflux over
# WebSockets). This is option 1 from design/creating_desktops.md §8: the *real*
# GNOME Shell (Activities, dynamic workspaces, extensions) over the *real* H.264
# path — the only shape that delivers the actual "stock GNOME" goal today.
#
# Why Ubuntu 24.04 / GNOME 46 specifically (NOT 26.04 like the other new images):
# GNOME 46 is the last generation with the two escape hatches GNOME 50 (26.04)
# removed —
#   1. GNOME Shell still has an **X11 backend**, so it runs on Xvfb and pixelflux
#      can capture it with plain X11 screen capture. On 26.04's GNOME 50 `mutter`
#      only has `--wayland` (see ../gnome-flashback-webrtc header) and X11 capture
#      of the real Shell is impossible — which is exactly why that image had to
#      fall back to Flashback (Panel + Metacity), NOT the Shell.
#   2. GNOME 46's gnome-session can start **without `systemd --user`**: when
#      systemd is not PID 1 (`sd_booted()` false) it uses its built-in component
#      launcher instead of session .target units. GNOME 50 made systemd --user a
#      hard requirement ("Failed to obtain session bus" otherwise), which forces
#      systemd-PID1 + `--privileged` + Kata (see ../gnome-flashback-webrtc /
#      ../gnome-grd). Staying on 46 lets this image keep the unprivileged
#      plain-entrypoint architecture of ../xfce-selkies2.
# 24.04 also dodges 26.04's mandatory glycin/bwrap image-loading sandbox (the
# reason gnome-flashback-webrtc must run its session as root under
# apparmor_restrict_unprivileged_userns=1) — on 24.04 gdk-pixbuf still loads
# images in-process, so the session runs as an unprivileged user.
#
# The one cost of 24.04: its libva is 2.20, but pixelflux's manylinux wheel needs
# libva >= 2.21 (its bundled libavutil calls vaMapBuffer2; a 2.20 libva.so.2
# fails to load with "undefined symbol: vaMapBuffer2", which selkies only
# surfaces as "Legacy screen_capture_module.so not found" on client connect —
# see design/creating_desktops.md §5). We vendor libva 2.22 from source in a
# short build stage below; it is small and dependency-light.
#
# Runtime-configurable identity: unlike the hardcoded `abc` in ../xfce-selkies2,
# the desktop user's name/UID/GID, sudo rights, and $HOME come from env vars and
# are applied by the entrypoint at container start (PUID/PGID/DESKTOP_USER/
# DESKTOP_SUDO) so a mounted per-user home volume lines up by ownership. See
# entrypoint.sh.

# Selkies upstream commit (main branch, 2.x line). Kept in sync with
# ../xfce-selkies2 — server wheel and web client both derive from it, so the
# client/server version lock falls out by construction.
ARG SELKIES_COMMIT=5686f6c4d20ed63a27e253bac00fb89ef99828c8

##############################################################################
# Stage 1 — web client. Identical recipe to ../xfce-selkies2 (React/Vite
# dashboard around the protocol lib): build selkies-web-core first, then the
# dashboard which vendors the core's dist, then stage the core lib + jsdb assets
# into the served dist where the runtime expects them.
##############################################################################
FROM node:22-bookworm-slim AS web
ARG SELKIES_COMMIT

ADD https://github.com/selkies-project/selkies/archive/${SELKIES_COMMIT}.tar.gz /tmp/selkies.tar.gz
RUN mkdir /src && tar -xzf /tmp/selkies.tar.gz -C /src --strip-components=1

RUN cd /src/addons/selkies-web-core \
 && npm install \
 && npm run build
RUN cd /src/addons/selkies-dashboard \
 && cp ../selkies-web-core/dist/selkies-core.js src/ \
 && npm install \
 && npm run build \
 && mkdir -p dist/src \
 && cp ../selkies-web-core/dist/selkies-core.js dist/src/ \
 && cp -r ../selkies-web-core/dist/jsdb dist/

##############################################################################
# Stage 2 — libva 2.22 from source. 24.04's libva is 2.20; pixelflux needs the
# vaMapBuffer2 symbol added in libva 2.21 (VA-API 1.21). We build the X11 and
# DRM backends (pixelflux's module links libva.so.2 / libva-drm.so.2 /
# libva-x11.so.2) and stage them into /dest; the runtime copies them into
# /usr/local/lib, which precedes /usr/lib in the default ld.so search order.
##############################################################################
FROM ubuntu:24.04 AS libva
ENV DEBIAN_FRONTEND=noninteractive
ARG LIBVA_VERSION=2.22.0
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates meson ninja-build build-essential pkg-config \
      libdrm-dev libx11-dev libxext-dev libxfixes-dev \
 && rm -rf /var/lib/apt/lists/*
ADD https://github.com/intel/libva/archive/refs/tags/${LIBVA_VERSION}.tar.gz /tmp/libva.tar.gz
RUN mkdir /src && tar -xzf /tmp/libva.tar.gz -C /src --strip-components=1 \
 && cd /src \
 # libva auto-detects backends from the dev libs present: we installed
 # libdrm-dev + libx11/xext/xfixes-dev (so libva-drm.so.2 and libva-x11.so.2
 # build) but not wayland's, so the wayland backend is skipped. No -Dwith_*
 # flags — those were removed; detection is by dependency now.
 && meson setup build --prefix=/usr/local --libdir=lib \
 && ninja -C build \
 && DESTDIR=/dest ninja -C build install

##############################################################################
# Stage 3 — runtime: GNOME Shell (X11) + Xvfb + PulseAudio + the selkies Python
# server, on Ubuntu 24.04.
##############################################################################
FROM ubuntu:24.04
ARG SELKIES_COMMIT

ENV DEBIAN_FRONTEND=noninteractive

# --- Firefox from Mozilla's APT repo, NOT Ubuntu's ---------------------------
# On 24.04 the `firefox` apt package is a transitional shim that installs the
# Firefox *snap*, and snapd does not run inside a container — the browser would
# be uninstallable/unlaunchable. Mozilla's own APT repo ships a real .deb; the
# apt pin (priority 1000) keeps Ubuntu's snap-transitional package from ever
# winning. Requires wget + gpg key handling, done here before the big install.
RUN apt-get update && apt-get install -y --no-install-recommends wget gpg ca-certificates \
 && install -d -m 0755 /etc/apt/keyrings \
 && wget -qO- https://packages.mozilla.org/apt/repo-signing-key.gpg > /etc/apt/keyrings/packages.mozilla.org.asc \
 && echo "deb [signed-by=/etc/apt/keyrings/packages.mozilla.org.asc] https://packages.mozilla.org/apt mozilla main" \
      > /etc/apt/sources.list.d/mozilla.list \
 && printf 'Package: *\nPin: origin packages.mozilla.org\nPin-Priority: 1000\n' \
      > /etc/apt/preferences.d/mozilla

RUN apt-get update && apt-get install -y --no-install-recommends \
      # --- GNOME Shell session (the actual desktop) --------------------------
      # gnome-session provides the session manager + the gnome-xorg session
      # definition the entrypoint launches; gnome-shell is mutter + the Shell UI
      # (X11 backend still present on 46). gnome-settings-daemon is what applies
      # theming/xsettings and is a soft dep we pin explicitly so --no-install-
      # recommends can't drop it (a Shell with no settings-daemon has no cursor
      # theme / xsettings and looks broken).
      gnome-session \
      gnome-shell \
      gnome-settings-daemon \
      # --- Minimal application set -------------------------------------------
      # Terminal, Files, Text Editor, and Settings (control-center) — a usable
      # but deliberately small desktop. firefox comes from the Mozilla repo
      # pinned above. Everything else (LibreOffice etc.) that ubuntu-desktop
      # would drag in is intentionally omitted.
      gnome-terminal \
      nautilus \
      gnome-text-editor \
      gnome-control-center \
      firefox \
      # Adwaita icon/cursor theme + a sane default font so the Shell isn't a
      # sea of missing-icon boxes (see gnome-flashback-webrtc's glycin note for
      # how fatal missing icons can be; on 24.04 they're merely ugly, not fatal).
      adwaita-icon-theme \
      fonts-ubuntu \
      fonts-dejavu-core \
      # librsvg2-common provides the gdk-pixbuf *SVG loader*
      # (libpixbufloader-svg.so). Adwaita's icons are scalable SVGs; without this
      # loader gdk-pixbuf can't rasterize them and gnome-shell renders every icon
      # as a tiny bitmap scaled up to a blurry blob (text stays crisp — only
      # icons are affected). It is normally pulled via Recommends, which
      # --no-install-recommends drops, so it MUST be listed explicitly. The
      # loader cache is regenerated below (the dpkg trigger is unreliable here).
      librsvg2-common \
      # --- Software OpenGL for GNOME Shell -----------------------------------
      # GNOME Shell/mutter hard-require OpenGL. There is no GPU here, so Xvfb's
      # GLX must fall back to Mesa's llvmpipe software rasterizer:
      # libgl1-mesa-dri ships swrast_dri/llvmpipe, libglx-mesa0 the GLX vendor
      # lib. The entrypoint forces LIBGL_ALWAYS_SOFTWARE=1 (see there). This is
      # verify item (b) in design/creating_desktops.md §8 — Shell animations are
      # heavier than XFCE/Metacity under llvmpipe.
      libgl1-mesa-dri \
      libglx-mesa0 \
      mesa-utils \
      # --- Session plumbing --------------------------------------------------
      # dbus-x11 provides dbus-run-session (the per-session bus GNOME needs when
      # there is no systemd --user) and dbus-launch; dbus-user-session is a
      # no-op without systemd but harmless. gnome-keyring so app secret storage
      # doesn't error. sudo is required for the optional DESKTOP_SUDO path.
      dbus \
      dbus-x11 \
      gnome-keyring \
      sudo \
      # --- Audio (same arrangement as ../xfce-selkies2) ----------------------
      # Headless PulseAudio null sink named `output`; pcmflux captures
      # `output.monitor`. pulseaudio-utils provides pactl.
      pulseaudio \
      pulseaudio-utils \
      # --- X server + Selkies 2.x X11 tooling (identical to ../xfce-selkies2) -
      xvfb \
      # xrandr (+ cvt from xcvt) back Selkies' dynamic resize; without cvt every
      # resize silently fails (design/creating_desktops.md §5).
      x11-xserver-utils \
      xcvt \
      # xdpyinfo for the entrypoint's X-ready wait loop.
      x11-utils \
      # Clipboard sync — missing xclip kills the per-client WS loop on connect
      # ("video never starts"), it is NOT just a clipboard problem.
      xclip \
      # Keyboard injection: xdotool backs space/Enter/arrows/F-keys and the
      # wtype shim (digits/punctuation/unicode). Missing => "only a-z types".
      xdotool \
      # Snakeoil cert for SELKIES_ENABLE_HTTPS (WebCodecs needs a secure context
      # for any non-localhost origin — see design/creating_desktops.md §4).
      ssl-cert \
      python3 \
      python3-venv \
      # Native pip builds: evdev (pynput dep) against Python + kernel-uapi
      # headers, the xkbcommon cffi binding against libxkbcommon headers.
      python3-dev \
      build-essential \
      libxkbcommon-dev \
      ca-certificates \
 # Regenerate the gdk-pixbuf loader cache so the SVG loader from librsvg2-common
 # (above) is actually registered — the packaging trigger doesn't reliably fire
 # in a docker build layer, and without this the loader is present on disk but
 # not in loaders.cache, so icons stay blurry.
 && /usr/lib/x86_64-linux-gnu/gdk-pixbuf-2.0/gdk-pixbuf-query-loaders --update-cache \
 && rm -rf /var/lib/apt/lists/*

# Vendored libva 2.22 (see stage 2). /usr/local/lib is ahead of /usr/lib in the
# default ld.so.conf order, so this libva.so.2 wins over anything a GNOME package
# might have pulled in; ldconfig rebuilds the cache to make it authoritative.
COPY --from=libva /dest/usr/local /usr/local
RUN ldconfig

# Selkies server from the pinned commit, in its own venv (same as
# ../xfce-selkies2). setuptools is required at runtime, not by selkies: GPUtil
# does `from distutils import spawn` and distutils left the stdlib in 3.12.
ADD https://github.com/selkies-project/selkies/archive/${SELKIES_COMMIT}.tar.gz /tmp/selkies.tar.gz
RUN mkdir /tmp/selkies-src \
 && tar -xzf /tmp/selkies.tar.gz -C /tmp/selkies-src --strip-components=1 \
 && python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir /tmp/selkies-src setuptools \
 && rm -rf /tmp/selkies-src /tmp/selkies.tar.gz

# Web client built from the same commit; served by selkies' own aiohttp.
COPY --from=web /src/addons/selkies-dashboard/dist /opt/selkies-web

ENV PATH=/opt/venv/bin:$PATH \
    # Single WS/web port, matching the DesktopTemplate signalPort convention.
    SELKIES_PORT=8082 \
    DISPLAY=:0 \
    # Runtime identity defaults (overridable at `docker run`). The entrypoint
    # creates this user/uid/gid at start so a mounted home volume matches by
    # ownership; see entrypoint.sh.
    DESKTOP_USER=abc \
    PUID=1000 \
    PGID=1000 \
    DESKTOP_SUDO=false

# `wtype` is Wayland-only; on this X11 image selkies' wtype calls are served by
# an xdotool-backed shim (shared verbatim with ../xfce-selkies2).
COPY wtype-x11-shim /usr/local/bin/wtype

# The session launcher (gnome-shell + curated settings daemons, not plain
# gnome-session — see the script header for why).
COPY gnome-session-launch.sh /usr/local/bin/gnome-session-launch.sh

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/wtype /usr/local/bin/gnome-session-launch.sh

EXPOSE 8082
ENTRYPOINT ["/entrypoint.sh"]
