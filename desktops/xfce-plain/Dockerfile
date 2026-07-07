# Plain XFCE **workload** image — the sidecar-mode counterpart of
# ../xfce-selkies2. No X server, no PulseAudio daemon, no Selkies: the pod's
# streamer sidecar (../streamer-selkies2) owns all of that and this image just
# runs the desktop session against the display it finds via the injected
# DISPLAY / PULSE_SERVER env (shared /tmp/.X11-unix and /tmp/pulse emptyDirs).
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

# Well-known desktop user (no login prompt — the streamer captures whatever
# this session draws). Same convention as the *-selkies2 images.
ARG DESKTOP_USER=abc
RUN useradd -m -s /bin/bash "${DESKTOP_USER}"

ENV DESKTOP_USER=abc \
    DISPLAY=:0

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
