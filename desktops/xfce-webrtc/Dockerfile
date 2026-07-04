# check=skip=FromPlatformFlagConstDisallowed

# XFCE desktop streamed over **WebRTC** via Selkies-GStreamer (the webrtc viewer),
# software-encoded with x264 — no GPU required.
#
# **amd64 only.** Selkies' prebuilt GStreamer bundle is published for amd64 only
# (no arm64 asset as of v1.6.2), so this image is pinned to linux/amd64. On Apple
# Silicon / arm64 hosts it runs under emulation — fine for a functional smoke
# test, slow for real use. arm64-native would require building GStreamer from
# source (out of scope). The platform pin makes `docker build` do the right thing
# without a `--platform` flag.
#
# Unlike the RDP images, nothing re-rasterizes the frames: Selkies encodes H.264
# in-pod and the browser decodes it. The portal relays only signaling; media
# flows browser <-> coturn <-> this pod. See design/vdi.md "Round 3".
#
# The in-pod Selkies server version MUST match the selkies client JS the portal
# vendors (whistler/portal/static/selkies-core.js) — the exact analogue of the
# guacamole-common-js <-> guacd version match. Pin both to SELKIES_VERSION.
FROM --platform=linux/amd64 ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Selkies release to install (server wheel + GStreamer build + web client). Keep
# in sync with the vendored portal client JS.
ARG SELKIES_VERSION=1.6.2
ARG DISTRIB_RELEASE=24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
      xfce4 \
      xfce4-terminal \
      # dbus-x11 gives dbus-launch (session bus); the base dbus package pulls
      # dbus-system-bus-common, which ships /usr/share/dbus-1/system.conf — the
      # entrypoint's `dbus-daemon --system` needs it (Ubuntu 24.04 split it out).
      dbus \
      dbus-x11 \
      # Selkies always opens a second WebRTC connection for audio and captures it
      # with `pulsesrc`. With no PulseAudio the audio track never connects, and
      # the Selkies web client keeps the "Waiting for stream" overlay up forever
      # (it clears only when BOTH video AND audio report connected). The entrypoint
      # runs a headless PulseAudio with a null sink to give pulsesrc a silent
      # source. pulseaudio-utils provides `pactl` (used to set the default source).
      pulseaudio \
      pulseaudio-utils \
      xvfb \
      x11-xserver-utils \
      # Selkies' dynamic-resize path shells out to `cvt` to generate an xrandr
      # modeline for the browser's requested resolution; without it `cvt` isn't
      # found, the modeline comes back empty, and every resize silently fails
      # ("xrandr: failed to parse '<res> ' as a mode specification" — note the
      # trailing space from the empty modeline). xcvt is the small standalone
      # package providing it (not the full xserver-xorg-core, which drags in
      # systemd/udev).
      xcvt \
      # Selkies shells out to `xsel` for clipboard sync between the browser and
      # the X session; without it Selkies logs FileNotFoundError: 'xsel' and
      # clipboard support is silently disabled.
      xsel \
      python3 \
      python3-pip \
      # Base PyGObject. Selkies' GStreamer bundle ships only the gi/overrides
      # (Gst.py …), not a full gi — they layer onto this via PyGObject's
      # extend_path. Without it: "No module named 'gi'" / a partial gi.
      python3-gi \
      # selkies' deps include evdev, which compiles a C extension against the
      # Python + kernel-uapi headers at install time.
      python3-dev \
      build-essential \
      ca-certificates \
      curl \
      # Optional in-container TURN relay for single-host testing without the
      # cluster's shared coturn (SELKIES_USE_INTERNAL_TURN; see entrypoint/README).
      coturn \
      # GStreamer runtime deps the Selkies build links against.
      libgstreamer1.0-0 \
      libgstreamer-plugins-base1.0-0 \
      libgstreamer-plugins-bad1.0-0 \
      gstreamer1.0-plugins-base \
      gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad \
      gstreamer1.0-plugins-ugly \
      gstreamer1.0-x \
 && rm -rf /var/lib/apt/lists/*

# Selkies' self-contained GStreamer build (carries the WebRTC + x264 elements).
# amd64-only asset (see header) — the image is pinned to linux/amd64 to match.
RUN curl -fsSL -o /tmp/gst.tar.gz \
      "https://github.com/selkies-project/selkies-gstreamer/releases/download/v${SELKIES_VERSION}/gstreamer-selkies_gpl_v${SELKIES_VERSION}_ubuntu${DISTRIB_RELEASE}_amd64.tar.gz" \
 && mkdir -p /opt/gstreamer \
 # The tarball nests everything under a top-level gstreamer/ dir; strip it so the
 # bundle root is /opt/gstreamer (matching the GSTREAMER_PATH env below).
 && tar -xzf /tmp/gst.tar.gz -C /opt/gstreamer --strip-components=1 \
 && rm /tmp/gst.tar.gz

# Selkies server wheel (the signaling + WebRTC process). Installed straight from
# the URL so pip sees the real PEP 427 filename (a renamed *.whl is rejected).
RUN PIP_BREAK_SYSTEM_PACKAGES=1 pip3 install --no-cache-dir \
      "https://github.com/selkies-project/selkies-gstreamer/releases/download/v${SELKIES_VERSION}/selkies_gstreamer-${SELKIES_VERSION}-py3-none-any.whl"

# Selkies web/client bundle. We serve our own page from the portal, but the
# client JS is sourced from this same release for the version match — extract it
# into the portal static dir at build time (see README) from /opt/gst-web.
RUN curl -fsSL -o /tmp/web.tar.gz \
      "https://github.com/selkies-project/selkies-gstreamer/releases/download/v${SELKIES_VERSION}/selkies-gstreamer-web_v${SELKIES_VERSION}.tar.gz" \
 && mkdir -p /opt/gst-web \
 # Nested like the GStreamer tarball — strip so the web root is /opt/gst-web.
 && tar -xzf /tmp/web.tar.gz -C /opt/gst-web --strip-components=1 \
 && rm /tmp/web.tar.gz

# Local patches on top of the vendored client bundle above (app.js + index.html):
# adds a "Pixel-doubled (half resolution)" toggle next to Selkies' own resize
# switch — requests half the physical resolution from the server (normal-sized
# UI, no HiDPI upscale of the desktop) and stretches that smaller stream to
# fill the window with nearest-neighbor scaling (crisp 2x2 blocks) instead of
# native full-resolution rendering. Vendored as full files rather than a diff/
# sed patch since a SELKIES_VERSION bump can freely change surrounding text —
# re-derive gst-web-overrides/ by hand against the new bundle when that happens
# (same version-lock caveat as the client JS itself, see README).
COPY gst-web-overrides/app.js gst-web-overrides/index.html /opt/gst-web/

# Well-known desktop user (no login prompt — Selkies captures this X session).
ARG DESKTOP_USER=abc
RUN useradd -m -s /bin/bash "${DESKTOP_USER}"

# Selkies' bundled GStreamer is self-contained and NOT on the system paths — it
# ships its own libs + Python gi bindings under /opt/gstreamer. Selkies aborts
# with "could not find working GStreamer-Python installation / No module named
# 'gi'" unless this whole block points at the bundle (arch is fixed amd64, so
# x86_64-linux-gnu is hard-coded). Mirrors the export block in Selkies' docs.
ENV GSTREAMER_PATH=/opt/gstreamer \
    PATH=/opt/gstreamer/bin:$PATH \
    LD_LIBRARY_PATH=/opt/gstreamer/lib/x86_64-linux-gnu \
    GST_PLUGIN_PATH=/opt/gstreamer/lib/x86_64-linux-gnu/gstreamer-1.0 \
    GST_PLUGIN_SYSTEM_PATH=/opt/gstreamer/lib/x86_64-linux-gnu/gstreamer-1.0:/usr/lib/x86_64-linux-gnu/gstreamer-1.0 \
    GI_TYPELIB_PATH=/opt/gstreamer/lib/x86_64-linux-gnu/girepository-1.0:/usr/lib/x86_64-linux-gnu/girepository-1.0 \
    PYTHONPATH=/opt/gstreamer/lib/python3/dist-packages \
    # Signaling/web port — must match the DesktopTemplate's signalPort (default 8082).
    SELKIES_PORT=8082 \
    DISPLAY=:0 \
    GST_DEBUG=*:1 \
    DESKTOP_USER=abc

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8082
ENTRYPOINT ["/entrypoint.sh"]
