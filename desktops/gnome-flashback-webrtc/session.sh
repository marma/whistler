#!/usr/bin/env bash
# Session script — runs as root via gnome-desktop.service (see that unit file
# for why root, not an unprivileged desktop user).
#
# With systemd as PID 1 and PAMName=login in the service unit, pam_systemd has
# already called logind CreateSession and started user@UID.service (systemd
# --user), which provides the session D-Bus at $XDG_RUNTIME_DIR/bus. We reuse
# THAT bus (not a fresh dbus-launch one) because gnome-session's own startup
# sequence is implemented as systemd --user units and won't work on any other
# bus — see ../gnome-grd/session.sh for the same pattern, and the Dockerfile
# header for why this is required at all.
set -e

SELKIES_PORT="${SELKIES_PORT:-8082}"
RES="${SELKIES_RESOLUTION:-1280x720}"

export DISPLAY=:0
export XDG_CURRENT_DESKTOP=GNOME-Flashback:GNOME
export XDG_SESSION_TYPE=x11
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"

for _i in $(seq 60); do
  [ -S "$XDG_RUNTIME_DIR/bus" ] && break
  sleep 1
done
export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"

# Optional in-container TURN relay for single-host testing WITHOUT the
# cluster's shared coturn — same knobs as ../xfce-webrtc/entrypoint.sh.
if [ -n "${SELKIES_USE_INTERNAL_TURN:-}" ] && [ "${SELKIES_USE_INTERNAL_TURN}" != "0" ]; then
  TURN_SECRET="${SELKIES_TURN_SHARED_SECRET:-internal}"
  TURN_PORT="${INTERNAL_TURN_PORT:-3478}"
  TURN_EXTERNAL_IP="${INTERNAL_TURN_EXTERNAL_IP:-127.0.0.1}"
  echo "[session] starting internal coturn on ${TURN_PORT} (external-ip=${TURN_EXTERNAL_IP})"
  turnserver -n --no-cli --no-tls --no-dtls \
    --realm=whistler --use-auth-secret --static-auth-secret="${TURN_SECRET}" \
    --listening-port="${TURN_PORT}" \
    --min-port="${INTERNAL_TURN_MIN_PORT:-49160}" \
    --max-port="${INTERNAL_TURN_MAX_PORT:-49200}" \
    --external-ip="${TURN_EXTERNAL_IP}" \
    --log-file=stdout --simple-log &
  export SELKIES_TURN_HOST="${TURN_EXTERNAL_IP}"
  export SELKIES_TURN_PORT="${TURN_PORT}"
  export SELKIES_TURN_PROTOCOL="${SELKIES_TURN_PROTOCOL:-udp}"
  export SELKIES_TURN_SHARED_SECRET="${TURN_SECRET}"
fi

# Headless PulseAudio with a null sink — see ../xfce-webrtc/entrypoint.sh for
# why this is required (Selkies' audio track never connects without one).
export PULSE_SERVER="unix:${XDG_RUNTIME_DIR}/pulse/native"
rm -rf "${HOME}/.config/pulse" "${XDG_RUNTIME_DIR}/pulse"
mkdir -p "${XDG_RUNTIME_DIR}/pulse"
pulseaudio --daemonize=false --realtime=false --disallow-exit --exit-idle-time=-1 \
  --load="module-native-protocol-unix auth-anonymous=1 socket=${XDG_RUNTIME_DIR}/pulse/native" \
  --load="module-null-sink sink_name=virtual_speaker sink_properties=device.description=virtual_speaker" \
  --log-target=stderr &
for _i in $(seq 30); do [ -S "${XDG_RUNTIME_DIR}/pulse/native" ] && break; sleep 0.2; done
pactl set-default-source virtual_speaker.monitor 2>/dev/null || \
  echo "[session] warning: could not set default pulse source (audio track may not connect)"

# Headless X server (software framebuffer) that Selkies' ximagesrc captures.
# Start big, then shrink to RES via Selkies' own resize_display() — see
# ../xfce-webrtc/entrypoint.sh for why a just-started Xvfb can't be started
# directly at RES (it bakes in a RandR maximum it can never grow past later).
XVFB_MAX_RES="7680x4320"
Xvfb "${DISPLAY}" -screen 0 "${XVFB_MAX_RES}x24" -ac +extension RANDR &
for _i in $(seq 30); do xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1 && break; sleep 0.2; done
( python3 -c "
import os, sys, time
import selkies_gstreamer
sys.path.insert(0, os.path.dirname(selkies_gstreamer.__file__))
from resize import resize_display
for attempt in range(60):
    if resize_display('${RES}'):
        break
    time.sleep(1)
else:
    print('[session] warning: could not set initial resolution to ${RES} (staying at ${XVFB_MAX_RES})', file=sys.stderr)
" & )

# GNOME Flashback (Metacity) on that display, using the systemd --user bus
# from above — NOT a fresh dbus-launch session (see header).
gnome-session --session=gnome-flashback-metacity &

# Selkies: software x264 encoder, signaling+web on SELKIES_PORT, dynamic
# resize disabled (the portal sizes the video element). System GStreamer (no
# GSTREAMER_PATH bundle — see Dockerfile header), so no PATH/LD_LIBRARY_PATH/
# GST_PLUGIN_PATH overrides are needed here.
exec selkies-gstreamer \
  --addr=0.0.0.0 \
  --port="${SELKIES_PORT}" \
  --web_root=/opt/gst-web \
  --enable_https=false \
  --enable_basic_auth=false \
  --encoder=x264enc \
  --enable_resize=false
