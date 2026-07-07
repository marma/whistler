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
# operator injects both, see KubeConfigManager._build_pod_spec streamer=sidecar)
# and an entrypoint that starts a DE session or app. It never sees Selkies —
# the streaming protocol is this image's problem, swappable per template.
#
# Run as a *native sidecar* (initContainer, restartPolicy=Always) with a
# startupProbe on SELKIES_PORT: Selkies only listens after X is up, so the
# workload container starts with a guaranteed-live display and needs no wait
# loop. Outside Kubernetes the same image pairs with a workload container via
# two shared docker volumes — see README.md.
#
# Everything below (base, pinned commit, package rationale) tracks
# ../xfce-selkies2/Dockerfile; keep the two in sync when bumping.

# Selkies upstream commit (main branch, 2.x line). Bump deliberately: server
# wheel and web client both derive from it, so a bump upgrades both in lock.
ARG SELKIES_COMMIT=5686f6c4d20ed63a27e253bac00fb89ef99828c8

##############################################################################
# Stage 1 — web client (identical to ../xfce-selkies2, see its header).
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
# Ubuntu 26.04 for libva >= 2.21 (pixelflux hard-requires vaMapBuffer2) — same
# constraint as ../xfce-selkies2.
##############################################################################
FROM ubuntu:26.04
ARG SELKIES_COMMIT

ENV DEBIAN_FRONTEND=noninteractive

# Package set = ../xfce-selkies2 minus xfce4/xfce4-terminal/dbus-x11 (no DE and
# no session bus here — the workload container owns its own session). Each
# remaining package matters; the failure modes for missing ones are silent and
# documented in ../xfce-selkies2/Dockerfile.
RUN apt-get update && apt-get install -y --no-install-recommends \
      dbus \
      pulseaudio \
      pulseaudio-utils \
      xvfb \
      x11-xserver-utils \
      xcvt \
      x11-utils \
      xclip \
      xdotool \
      ssl-cert \
      python3 \
      python3-venv \
      python3-dev \
      build-essential \
      libxkbcommon-dev \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ADD https://github.com/selkies-project/selkies/archive/${SELKIES_COMMIT}.tar.gz /tmp/selkies.tar.gz
RUN mkdir /tmp/selkies-src \
 && tar -xzf /tmp/selkies.tar.gz -C /tmp/selkies-src --strip-components=1 \
 && python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir /tmp/selkies-src setuptools \
 && rm -rf /tmp/selkies-src /tmp/selkies.tar.gz

COPY --from=web /src/addons/selkies-dashboard/dist /opt/selkies-web

ENV PATH=/opt/venv/bin:$PATH \
    SELKIES_PORT=8082 \
    DISPLAY=:0

COPY wtype-x11-shim /usr/local/bin/wtype
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/wtype

EXPOSE 8082
ENTRYPOINT ["/entrypoint.sh"]
