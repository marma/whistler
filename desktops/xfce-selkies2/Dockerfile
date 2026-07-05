# XFCE desktop streamed via **Selkies 2.x** (pixelflux) — the successor-stack
# spike next to ../xfce-webrtc (Selkies-GStreamer 1.6.2). What changes with 2.x:
#
#  - **Transport**: WebSockets by default (`--mode=websockets`) — pixelflux
#    encodes H.264/JPEG in-pod and the browser decodes via WebCodecs over a
#    plain WS connection on the web port. No ICE, no TURN, **no coturn**.
#    (`--mode=webrtc` still exists but is not used here.)
#  - **No GStreamer**: capture+encode is `pixelflux`, audio is `pcmflux` (both
#    PyPI manylinux wheels, MPL-2.0, maintained under the linuxserver org —
#    see design decision in the repo memory / desktops/README.md).
#  - **Multi-arch**: pixelflux/pcmflux publish amd64 *and* arm64 wheels, so
#    unlike xfce-webrtc there is no `--platform` pin here.
#  - **Unreleased upstream**: Selkies 2.x has no tagged release yet (latest tag
#    is still v1.6.2); upstream `main` is the 2.x line and downstreams (e.g.
#    linuxserver's baseimage-selkies) install it by commit. We pin
#    SELKIES_COMMIT below and build BOTH the Python server and the web client
#    from that same commit — the client/server version lock that xfce-webrtc
#    maintains by hand falls out by construction.
#
# Wayland note: 2.x can capture Wayland (PIXELFLUX_WAYLAND=true, pixelflux as
# the compositor), but XFCE is an X11 DE, so this image stays on Xvfb/X11.
# The Wayland path is a separate follow-up image.

# Selkies upstream commit (main branch, 2.x line). Bump deliberately: server
# wheel and web client both derive from it, so a bump upgrades both in lock.
ARG SELKIES_COMMIT=5686f6c4d20ed63a27e253bac00fb89ef99828c8

##############################################################################
# Stage 1 — web client. The 2.x frontend is a React/Vite dashboard
# (addons/selkies-dashboard) around the protocol lib (addons/selkies-web-core,
# built first; the dashboard vendors its dist/selkies-core.js). Build recipe
# mirrors how linuxserver's baseimage-selkies assembles the same dist.
##############################################################################
FROM node:22-bookworm-slim AS web
ARG SELKIES_COMMIT

ADD https://github.com/selkies-project/selkies/archive/${SELKIES_COMMIT}.tar.gz /tmp/selkies.tar.gz
RUN mkdir /src && tar -xzf /tmp/selkies.tar.gz -C /src --strip-components=1

RUN cd /src/addons/selkies-web-core \
 && npm install \
 && npm run build
RUN cd /src/addons/selkies-dashboard \
 # The dashboard imports the built core lib from its own src/ at build time…
 && cp ../selkies-web-core/dist/selkies-core.js src/ \
 && npm install \
 && npm run build \
 # …and expects it (plus the core lib's jsdb assets) inside the served dist
 # at runtime.
 && mkdir -p dist/src \
 && cp ../selkies-web-core/dist/selkies-core.js dist/src/ \
 && cp -r ../selkies-web-core/dist/jsdb dist/

##############################################################################
# Stage 2 — runtime: XFCE + Xvfb + PulseAudio + the selkies Python server.
#
# Ubuntu 26.04 (not 24.04) is required: pixelflux's manylinux wheel bundles
# its own ffmpeg/x264 but links the SYSTEM libva, and its libavutil needs
# vaMapBuffer2 (libva >= 2.21). 24.04 ships libva 2.20 and the module fails to
# load with "undefined symbol: vaMapBuffer2" — which selkies surfaces only as
# "Legacy screen_capture_module.so not found" when a client connects. 26.04
# also matches the base the gnome-* desktop images already use.
##############################################################################
FROM ubuntu:26.04
ARG SELKIES_COMMIT

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      xfce4 \
      xfce4-terminal \
      # dbus-x11 gives dbus-launch (session bus); the base dbus package pulls
      # dbus-system-bus-common for the entrypoint's `dbus-daemon --system`.
      dbus \
      dbus-x11 \
      # pcmflux captures audio from PulseAudio; the entrypoint runs a headless
      # daemon with a null sink (same arrangement as xfce-webrtc). The pip deps
      # pasimple/pulsectl-asyncio also talk to it (mic path); pulseaudio-utils
      # provides `pactl` for the default-source setup.
      pulseaudio \
      pulseaudio-utils \
      xvfb \
      # Selkies 2.x still resizes by shelling out to xrandr (+ cvt for the
      # modeline — same trap as 1.x: without cvt every dynamic resize silently
      # fails). xcvt ships `cvt` without dragging in xserver-xorg-core.
      x11-xserver-utils \
      xcvt \
      # xdpyinfo for the entrypoint's X-ready wait loop.
      x11-utils \
      # Clipboard sync: 2.x shells out to `xclip` on X11 (`wl-paste` on
      # Wayland) — NOT 1.x's `xsel`. Missing xclip is not graceful: the
      # FileNotFoundError kills the per-client WS message loop on connect,
      # which presents as "video never starts", not as a clipboard problem.
      xclip \
      # Keyboard injection: only alphabetic keys go through XTEST directly.
      # Space/Enter/arrows/F-keys are injected by shelling out to `xdotool`,
      # and digits/punctuation/unicode via `wtype` (see wtype-x11-shim, COPYied
      # below). Both failures are swallowed silently — a missing xdotool
      # presents as "only a-z types".
      xdotool \
      # Self-signed "snakeoil" cert generated at install time — selkies'
      # https_cert/https_key defaults point exactly at it. Only used when
      # HTTPS is enabled (SELKIES_ENABLE_HTTPS=true): the 2.x web client hard-
      # requires a secure context (WebCodecs doesn't exist outside one), so
      # any access NOT via http://localhost needs HTTPS even in dev.
      ssl-cert \
      python3 \
      python3-venv \
      # Native builds during `pip install`: evdev (pynput dep) compiles against
      # Python + kernel-uapi headers; the xkbcommon cffi binding needs
      # libxkbcommon headers.
      python3-dev \
      build-essential \
      libxkbcommon-dev \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Selkies server from the pinned commit, in its own venv (2.x needs no system
# site-packages — the GStreamer/PyGObject coupling is gone with 1.x). Pulls
# pixelflux/pcmflux (+ the WebRTC stack, unused in websockets mode) from PyPI.
ADD https://github.com/selkies-project/selkies/archive/${SELKIES_COMMIT}.tar.gz /tmp/selkies.tar.gz
RUN mkdir /tmp/selkies-src \
 && tar -xzf /tmp/selkies.tar.gz -C /tmp/selkies-src --strip-components=1 \
 && python3 -m venv /opt/venv \
 # setuptools is not a selkies dependency but IS required at runtime: GPUtil
 # does `from distutils import spawn`, and distutils left the stdlib in Python
 # 3.12 — setuptools' shim puts it back. (linuxserver's baseimage installs it
 # for the same reason.)
 && /opt/venv/bin/pip install --no-cache-dir /tmp/selkies-src setuptools \
 && rm -rf /tmp/selkies-src /tmp/selkies.tar.gz

# The web client built from the same commit. Served by selkies itself
# (aiohttp) via --web_root — no nginx needed.
COPY --from=web /src/addons/selkies-dashboard/dist /opt/selkies-web

# Well-known desktop user (no login prompt — Selkies captures this X session).
ARG DESKTOP_USER=abc
RUN useradd -m -s /bin/bash "${DESKTOP_USER}"

ENV PATH=/opt/venv/bin:$PATH \
    # Web/WS port — the single port everything flows over in websockets mode.
    # Kept at 8082 to match the DesktopTemplate signalPort convention.
    SELKIES_PORT=8082 \
    DISPLAY=:0 \
    DESKTOP_USER=abc

# `wtype` is Wayland-only; on this X11 image selkies' wtype calls are served
# by an xdotool-backed shim (see the file's header for the full story).
COPY wtype-x11-shim /usr/local/bin/wtype

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/wtype

EXPOSE 8082
ENTRYPOINT ["/entrypoint.sh"]
