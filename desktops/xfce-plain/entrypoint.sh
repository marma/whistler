#!/usr/bin/env bash
# Start an XFCE session on a display somebody else owns (the streamer sidecar
# in-cluster, any X server elsewhere). The container's lifecycle tracks the
# session: when XFCE exits, the container exits and the pod restarts it against
# the still-running display.
set -e

USER_NAME="${DESKTOP_USER:-abc}"

# In-cluster the sidecar's startupProbe guarantees the display is live before
# this container starts, so this loop falls through immediately. Outside
# Kubernetes (docker compose / manual `docker run`) there is no such gating —
# wait briefly, then fail *loudly*: without this, startxfce4 would die in an
# instant-restart loop with the reason buried in scrollback.
for i in $(seq 1 60); do
  xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1 && break
  [ "$i" = 1 ] && echo "[entrypoint] waiting for X display ${DISPLAY}..."
  sleep 0.5
done
if ! xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1; then
  echo "[entrypoint] ERROR: no X server on DISPLAY=${DISPLAY} after 30s." >&2
  echo "[entrypoint] This image renders to a display it does not own — run it" >&2
  echo "[entrypoint] next to a streamer (see desktops/streamer-selkies2) with" >&2
  echo "[entrypoint] /tmp/.X11-unix shared, or point DISPLAY at a real X server." >&2
  exit 1
fi

# System D-Bus for the XFCE session (logind-less).
mkdir -p /var/run/dbus && rm -f /var/run/dbus/pid
dbus-daemon --system --fork

# XFCE as the desktop user, in the foreground. `su -` scrubs the environment,
# so DISPLAY / PULSE_SERVER (injected by the operator or docker -e) are
# re-exported explicitly.
exec su - "${USER_NAME}" -c \
  "env DISPLAY='${DISPLAY}' PULSE_SERVER='${PULSE_SERVER}' dbus-launch --exit-with-session startxfce4"
