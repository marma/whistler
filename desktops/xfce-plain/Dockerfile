# Plain XFCE **workload** image. No X server, no PulseAudio daemon, no
# Selkies: the pod's streamer sidecar (../streamer-selkies2) owns all of that
# and this image just runs the desktop session against the display it finds
# via the injected DISPLAY / PULSE_SERVER env (shared /tmp/.X11-unix and
# /tmp/pulse emptyDirs).
#
# That is the entire contract, and it is the point of the sidecar model: a
# workload image is an ordinary "DE or app + its tools" image with a session
# entrypoint — nothing display-protocol-specific to keep in sync. Outside
# Kubernetes the same image runs against a real X server (e.g. a laptop's,
# with the host's /tmp/.X11-unix bind-mounted in) or the streamer container
# via docker volumes — see ../streamer-selkies2/README.md.

FROM ubuntu:26.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      xfce4 \
      xfce4-terminal \
      # dbus-x11 gives dbus-launch (session bus for the XFCE session); the
      # base dbus package pulls what `dbus-daemon --system` needs.
      dbus \
      dbus-x11 \
      # xdpyinfo for the entrypoint's X-ready wait (only exercised outside
      # Kubernetes — in-cluster the sidecar's startupProbe already gates us).
      x11-utils \
      # pactl/paplay: client tools to verify the shared PulseAudio path.
      pulseaudio-utils \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Ubuntu 26.04's gdk-pixbuf decodes images through glycin, which sandboxes
# every decode in a bwrap (user-namespace) worker. Inside a Kubernetes pod that
# nested sandbox cannot be set up, and the failure mode is nasty: glycin only
# falls back to unsandboxed loading when bwrap fails with an error string it
# recognizes, and the error you actually get under containerd/k3d ("bwrap:
# loopback: Failed RTM_NEWADDR: Operation not permitted") is not on that list —
# so glycin keeps using bwrap, every loader worker dies instantly, every
# icon/PNG decode returns NULL, GTK warns "Could not load a pixbuf from icon
# theme", and libwnck's g_assert on its fallback icon aborts xfce4-panel,
# taking the whole session down (CrashLoopBackOff). The same image is fine
# under plain `docker run`, which is what makes this so misleading.
# Fix: divert bwrap to a stub that fails with an error glycin's detection DOES
# recognize -> glycin runs loaders unsandboxed. The pod (and its
# NetworkPolicy) is the security boundary here; a nested userns sandbox inside
# an unprivileged container is not something we can support anyway.
RUN dpkg-divert --rename --add /usr/bin/bwrap \
 && printf '#!/bin/sh\necho "bwrap: Creating new namespace failed: disabled in this container image (see Dockerfile)" >&2\nexit 1\n' > /usr/bin/bwrap \
 && chmod +x /usr/bin/bwrap

# Well-known desktop user (no login prompt — the streamer captures whatever
# this session draws). gnome-plain upgrades this to runtime-configurable
# PUID/PGID identity; here a fixed user keeps the image minimal.
ARG DESKTOP_USER=abc
RUN useradd -m -s /bin/bash "${DESKTOP_USER}"

ENV DESKTOP_USER=abc \
    DISPLAY=:0

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
