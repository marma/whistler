# Prebuilt Selkies-stack artifacts for the vm-gnome-selkies guest, built on the
# SAME Ubuntu release as the guest (24.04) so they're ABI-compatible when
# extracted into it. build.sh docker-cp's the fixed paths below into the bake
# tar; the guest apt-installs the GNOME DE + streamer runtime libs and drops
# these on top.
#
# This is the resurrected build half of the retired embedded `gnome-selkies2`
# image (git history, commit that removed the embedded display path): the
# streamer sidecar can't cross a VM boundary, so — exactly like
# ../../vm-xfce-selkies bakes the streamer stack into its 26.04 guest — this VM
# bakes it into a 24.04 guest instead. It CANNOT reuse ../../streamer-selkies2's
# artifacts the way vm-xfce-selkies does, for two release-locked reasons:
#   1. that venv is built on 26.04 / Python 3.13; this guest is 24.04 / 3.12,
#      so its compiled wheels (pixelflux/pcmflux, evdev, xkbcommon-cffi) are the
#      wrong ABI.
#   2. GNOME Shell with an X11 backend only exists up to GNOME 46 (Ubuntu 24.04);
#      newer mutter is Wayland-only and can't be captured by a display-owning
#      streamer. So the guest MUST be 24.04 — where libva is 2.20 but pixelflux's
#      wheel needs the vaMapBuffer2 symbol from libva >= 2.21 (or it fails to
#      load, surfacing only as "Legacy screen_capture_module.so not found" on
#      client connect). We therefore vendor libva 2.22 from source (stage 2).
#
# Fixed artifact paths this image guarantees (build.sh reads exactly these):
#   /opt/venv                  selkies server venv (Python 3.12, 24.04 ABI)
#   /opt/selkies-web           built web client (static JS; release-agnostic)
#   /opt/libva/usr/local       vendored libva 2.22 tree (→ guest /usr/local)
#   /usr/local/bin/wtype       X11 wtype shim

# Kept in sync with ../../streamer-selkies2 (server wheel + web client both
# derive from this commit, so the client/server version lock is by construction).
ARG SELKIES_COMMIT=5686f6c4d20ed63a27e253bac00fb89ef99828c8

# pixelflux/pcmflux MUST be pinned. The pinned selkies commit does NOT pin its
# pcmflux/pixelflux deps, and their 2.0.0 releases are a BREAKING API change
# (selkies at this commit does `from pcmflux import AudioChunkCallback`, which
# 2.0.0 removed -> ImportError, streamer crash-loops, nothing on :8082). The
# 26.04 streamer image happens to still run because it was built before 2.0.0
# and cached 1.x; a fresh build (like this one) resolves 2.0.0 and breaks. Pin
# to exactly what streamer-selkies2's working venv carries. Bump these in
# lockstep with SELKIES_COMMIT (and fix ../../streamer-selkies2, which has the
# same latent unpinned-dep timebomb).
ARG PCMFLUX_VERSION=1.0.8
ARG PIXELFLUX_VERSION=1.6.4

##############################################################################
# Stage 1 — web client. Identical recipe to ../../streamer-selkies2: build
# selkies-web-core first, then the dashboard which vendors the core's dist,
# then stage the core lib + jsdb assets into the served dist. Pure JS — the
# one artifact that IS release-agnostic and could in principle be reused from
# the 26.04 streamer, but building it here keeps this image self-contained and
# pinned to the same commit as the venv below.
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
# vaMapBuffer2 symbol added in libva 2.21 (VA-API 1.21). Build the X11 and DRM
# backends (pixelflux's module links libva.so.2 / libva-drm.so.2 /
# libva-x11.so.2) and stage them into /dest; the guest copies them into
# /usr/local, which precedes /usr/lib in the default ld.so search order.
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
 # libva auto-detects backends from the dev libs present: libdrm-dev +
 # libx11/xext/xfixes-dev give libva-drm.so.2 and libva-x11.so.2; wayland's
 # dev libs are absent so that backend is skipped. Detection is by dependency
 # now (the old -Dwith_* flags were removed).
 && meson setup build --prefix=/usr/local --libdir=lib \
 && ninja -C build \
 && DESTDIR=/dest ninja -C build install

##############################################################################
# Stage 3 — the selkies venv, built on 24.04 (Python 3.12) so the compiled
# wheels match the guest ABI. Mirrors ../../streamer-selkies2's venv stage:
# pulls pixelflux/pcmflux (+ the unused-in-websockets WebRTC stack) from PyPI.
# setuptools is not a selkies dep but IS needed at runtime (GPUtil does
# `from distutils import spawn`, gone from the 3.12 stdlib).
##############################################################################
FROM ubuntu:24.04 AS venv
ARG SELKIES_COMMIT
ARG PCMFLUX_VERSION
ARG PIXELFLUX_VERSION
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-venv \
      # Native pip builds: evdev (pynput dep) against Python + kernel-uapi
      # headers, the xkbcommon cffi binding against libxkbcommon headers.
      python3-dev build-essential libxkbcommon-dev \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*
ADD https://github.com/selkies-project/selkies/archive/${SELKIES_COMMIT}.tar.gz /tmp/selkies.tar.gz
RUN mkdir /tmp/selkies-src \
 && tar -xzf /tmp/selkies.tar.gz -C /tmp/selkies-src --strip-components=1 \
 && python3 -m venv /opt/venv \
 # Pin pcmflux/pixelflux on the command line so they win over selkies' unpinned
 # deps (see the ARG header — 2.0.0 breaks this commit). setuptools is required
 # at runtime: GPUtil does `from distutils import spawn`, gone from 3.12 stdlib.
 && /opt/venv/bin/pip install --no-cache-dir \
      "pcmflux==${PCMFLUX_VERSION}" "pixelflux==${PIXELFLUX_VERSION}" \
      /tmp/selkies-src setuptools \
 && rm -rf /tmp/selkies-src /tmp/selkies.tar.gz \
 # Fail the build loudly if a resolver quirk overrode the pins, rather than
 # shipping a crash-looping streamer nobody sees until :8082 is dead. Assert the
 # installed versions (not an import: pcmflux's .so links libX11, absent in this
 # minimal builder stage but present in the guest, so importing would false-fail).
 && /opt/venv/bin/pip show pcmflux  | grep -qx "Version: ${PCMFLUX_VERSION}" \
 && /opt/venv/bin/pip show pixelflux | grep -qx "Version: ${PIXELFLUX_VERSION}"

##############################################################################
# Final — collect every artifact at the fixed paths build.sh extracts. FROM
# ubuntu:24.04 so /opt/venv's `python` symlink resolves to a real 3.12 here
# (harmless; the guest provides its own matching python3.12).
##############################################################################
FROM ubuntu:24.04
COPY --from=venv /opt/venv /opt/venv
COPY --from=web  /src/addons/selkies-dashboard/dist /opt/selkies-web
COPY --from=libva /dest/usr/local /opt/libva/usr/local
COPY wtype-x11-shim /usr/local/bin/wtype
RUN chmod +x /usr/local/bin/wtype
