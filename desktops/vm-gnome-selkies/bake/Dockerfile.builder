# Prebuilt Selkies-stack artifacts for the vm-gnome-selkies guest, built on the
# SAME Ubuntu release as the guest (24.04) so they're ABI-compatible when
# extracted into it. build.sh docker-cp's the fixed paths below into the bake
# tar; the guest apt-installs the GNOME DE + streamer runtime libs and drops
# these on top.
#
# Why build at all rather than `pip install selkies` / the upstream .deb:
#   1. Two patches: mac-cmd-chords.patch in the web client (which 2.0 bundles
#      into the wheel as selkies/selkies_web) and xkb-active-group.patch in the
#      server, so the wheel is built here from patched source.
#   2. GNOME Shell with an X11 backend only exists up to GNOME 46 (Ubuntu
#      24.04); newer mutter is Wayland-only. So the guest is 24.04 — where
#      libva is 2.20, but pixelflux's capture module needs vaMapBuffer2 from
#      libva >= 2.21 (without it the module fails to load on client connect).
#      We vendor libva 2.22 from source (stage 2).
#
# Fixed artifact paths this image guarantees (build.sh reads exactly these):
#   /opt/venv                  selkies venv (Python 3.12, 24.04 ABI), web
#                              client bundled as package data
#   /opt/libva/usr/local       vendored libva 2.22 tree (→ guest /usr/local)

# Selkies 2.0.0. The wheel pins its own pixelflux/pcmflux (~=2.1.0), so unlike
# the 1.x line nothing in the capture stack is pinned separately here.
ARG SELKIES_COMMIT=3ec56fb1538cf077c27156f5ab75b6595a83c461

##############################################################################
# Stage 1 — web client, through upstream's own scripts/ci/build-web.sh (what
# the wheel, the .deb and the images all run), on source patched with
# mac-cmd-chords.patch. Output: src/selkies/selkies_web, which stage 3 copies
# into the source tree before building the wheel.
##############################################################################
FROM node:26-bookworm-slim AS web
ARG SELKIES_COMMIT

RUN apt-get update && apt-get install -y --no-install-recommends patch \
 && rm -rf /var/lib/apt/lists/*

ADD https://github.com/selkies-project/selkies/archive/${SELKIES_COMMIT}.tar.gz /tmp/selkies.tar.gz
RUN mkdir /src && tar -xzf /tmp/selkies.tar.gz -C /src --strip-components=1

# Repeated Cmd chords from macOS clients type the letter instead of firing the
# shortcut, and plain Cmd-C/V need app-aware handling — see the patch header.
# `patch` exits non-zero, failing the build, if a pin bump makes it stale.
COPY mac-cmd-chords.patch /tmp/
RUN patch -p1 -d /src < /tmp/mac-cmd-chords.patch

RUN cd /src && sh scripts/ci/build-web.sh

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
# wheels match the guest ABI. pixelflux/pcmflux come from PyPI at the version
# the selkies wheel pins.
#
# python-xlib is not a selkies dependency since 2.0 (it vendors its own fork
# as selkies.Xlib), but whistler-copy-agent runs on this venv and imports it.
##############################################################################
FROM ubuntu:24.04 AS venv
ARG SELKIES_COMMIT
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-venv ca-certificates patch \
 && rm -rf /var/lib/apt/lists/*
ADD https://github.com/selkies-project/selkies/archive/${SELKIES_COMMIT}.tar.gz /tmp/selkies.tar.gz
COPY --from=web /src/src/selkies/selkies_web /tmp/selkies_web
# Typing with two GNOME input sources produced the wrong characters unless the
# first source was the active one — see the patch header for the measured
# matrix. `patch` exits non-zero, failing the build, if a pin bump makes it
# stale.
COPY xkb-active-group.patch /tmp/
RUN mkdir /tmp/selkies-src \
 && tar -xzf /tmp/selkies.tar.gz -C /tmp/selkies-src --strip-components=1 \
 && patch -p1 -d /tmp/selkies-src < /tmp/xkb-active-group.patch \
 && cp -a /tmp/selkies_web /tmp/selkies-src/src/selkies/selkies_web \
 # The archive's pyproject says 0.0.0.dev0; upstream's release build stamps
 # the version the same way, and `selkies --version` / the stats panel show it.
 && sed -i -e 's|^version =.*|version = "2.0.0"|' /tmp/selkies-src/pyproject.toml \
 && python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir /tmp/selkies-src python-xlib \
 && rm -rf /tmp/selkies-src /tmp/selkies_web /tmp/selkies.tar.gz \
 # Fail the build if the bundled client did not make it into the install —
 # the server would otherwise start and serve nothing.
 && test -f /opt/venv/lib/python3.12/site-packages/selkies/selkies_web/index.html

##############################################################################
# Final — collect every artifact at the fixed paths build.sh extracts. FROM
# ubuntu:24.04 so /opt/venv's `python` symlink resolves to a real 3.12 here
# (harmless; the guest provides its own matching python3.12).
##############################################################################
FROM ubuntu:24.04
COPY --from=venv /opt/venv /opt/venv
COPY --from=libva /dest/usr/local /opt/libva/usr/local
