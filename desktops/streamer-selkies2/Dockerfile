# Streamer **sidecar**: Xvfb + PulseAudio + Selkies 2.x (pixelflux/WebSockets)
# and nothing else — no desktop environment. This is ../xfce-selkies2 with the
# XFCE half removed: it owns the *display plane* of a desktop pod and shares it
# with a separate workload container over two emptyDirs:
#
#   /tmp/.X11-unix  — the X socket (Xvfb here, DE/apps in the workload)
#   /tmp/pulse      — the PulseAudio native socket (playback from the workload,
#                     pcmflux capture here)
#
# The workload container only needs DISPLAY=:0 and PULSE_SERVER set (the
# operator injects both, see KubeConfigManager._build_pod_spec) and an
# entrypoint that starts a DE session or app. It never sees Selkies — the
# streaming protocol is this image's problem, swappable per template.
#
# Run as a *native sidecar* (initContainer, restartPolicy=Always) with a
# startupProbe on SELKIES_PORT: Selkies only listens after X is up, so the
# workload container starts with a guaranteed-live display and needs no wait
# loop. Outside Kubernetes the same image pairs with a workload container via
# two shared docker volumes — see README.md.
#
# This image is the single home of the Selkies/pixelflux streaming stack
# (descended from the embedded xfce-selkies2/gnome-selkies2 images, removed in
# favor of the sidecar — recoverable from git history).

# Selkies upstream commit (main branch, 2.x line). Selkies 2.x has no tagged
# release yet (latest tag is still v1.6.2); upstream `main` is the 2.x line and
# downstreams install it by commit. Bump deliberately: the Python server wheel
# and the web client below BOTH derive from this commit, so a bump upgrades the
# two in lock (the client/server version match falls out by construction).
ARG SELKIES_COMMIT=5686f6c4d20ed63a27e253bac00fb89ef99828c8

##############################################################################
# Stage 1 — web client. The 2.x frontend is a React/Vite dashboard
# (addons/selkies-dashboard) around the protocol lib (addons/selkies-web-core,
# built first; the dashboard vendors its dist/selkies-core.js and expects it —
# plus the core lib's jsdb assets — inside the served dist at runtime).
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
# Stage 2 — runtime: Xvfb + PulseAudio + the selkies Python server. No DE.
#
# Ubuntu 26.04 (not 24.04) is required: pixelflux's manylinux wheel bundles
# its own ffmpeg/x264 but links the SYSTEM libva, and its libavutil needs
# vaMapBuffer2 (libva >= 2.21). 24.04 ships libva 2.20 and the module fails to
# load with "undefined symbol: vaMapBuffer2" — which selkies surfaces only as
# "Legacy screen_capture_module.so not found" when a client connects.
##############################################################################
FROM ubuntu:26.04
ARG SELKIES_COMMIT

ENV DEBIAN_FRONTEND=noninteractive

# No DE and no session bus here — the workload container owns its own session.
# Each package matters; most failure modes are SILENT and misleading:
RUN apt-get update && apt-get install -y --no-install-recommends \
      # System bus for PulseAudio's module loading (dbus-daemon --system in the
      # entrypoint).
      dbus \
      # pcmflux captures audio from PulseAudio; the entrypoint runs a headless
      # daemon with a null sink whose socket is shared with the workload. The
      # pip deps pasimple/pulsectl-asyncio also talk to it (mic path);
      # pulseaudio-utils provides `pactl` for the default-source setup.
      pulseaudio \
      pulseaudio-utils \
      # The headless X server (software framebuffer) pixelflux captures.
      xvfb \
      # Selkies 2.x resizes by shelling out to xrandr (+ cvt for the modeline —
      # without cvt every dynamic resize silently fails). xcvt ships `cvt`
      # without dragging in xserver-xorg-core.
      x11-xserver-utils \
      xcvt \
      # xdpyinfo for the entrypoint's X-ready wait loop.
      x11-utils \
      # Clipboard sync: 2.x shells out to `xclip` on X11 — NOT 1.x's `xsel`.
      # Missing xclip is not graceful: the FileNotFoundError kills the
      # per-client WS message loop on connect, which presents as "video never
      # starts", not as a clipboard problem.
      xclip \
      # Keyboard injection: only alphabetic keys go through XTEST directly.
      # Space/Enter/arrows/F-keys are injected by shelling out to `xdotool`,
      # and digits/punctuation/unicode via `wtype` (see wtype-x11-shim, COPYied
      # below). Both failures are swallowed silently — a missing xdotool
      # presents as "only a-z types".
      xdotool \
      # Self-signed "snakeoil" cert generated at install time — selkies'
      # https_cert/https_key defaults point exactly at it. Only used when
      # SELKIES_ENABLE_HTTPS=true: the 2.x web client hard-requires a secure
      # context (WebCodecs doesn't exist outside one), so any access NOT via
      # http://localhost needs HTTPS even in dev.
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

# Selkies server from the pinned commit, in its own venv. Pulls
# pixelflux/pcmflux (+ the WebRTC stack, unused in websockets mode) from PyPI —
# both ship amd64 AND arm64 manylinux wheels, so the image is multi-arch.
# setuptools is not a selkies dependency but IS required at runtime: GPUtil
# does `from distutils import spawn`, and distutils left the stdlib in Python
# 3.12 — setuptools' shim puts it back.
ADD https://github.com/selkies-project/selkies/archive/${SELKIES_COMMIT}.tar.gz /tmp/selkies.tar.gz
RUN mkdir /tmp/selkies-src \
 && tar -xzf /tmp/selkies.tar.gz -C /tmp/selkies-src --strip-components=1 \
 && python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir /tmp/selkies-src setuptools \
 && rm -rf /tmp/selkies-src /tmp/selkies.tar.gz

# The web client built from the same commit. Served by selkies itself
# (aiohttp) via --web-root — no nginx needed.
COPY --from=web /src/addons/selkies-dashboard/dist /opt/selkies-web

ENV PATH=/opt/venv/bin:$PATH \
    SELKIES_PORT=8082 \
    DISPLAY=:0

COPY wtype-x11-shim /usr/local/bin/wtype
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/wtype

EXPOSE 8082
ENTRYPOINT ["/entrypoint.sh"]
