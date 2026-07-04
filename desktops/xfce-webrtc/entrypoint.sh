#!/usr/bin/env bash
# Bring up a software-rendered X session and stream it over WebRTC with Selkies.
# No systemd / no display-manager: we run the pieces directly so the container
# lifecycle tracks the Selkies server in the foreground.
set -e

USER_NAME="${DESKTOP_USER:-abc}"
SELKIES_PORT="${SELKIES_PORT:-8082}"
RES="${SELKIES_RESOLUTION:-1280x720}"

# Optional in-container TURN relay for single-host testing WITHOUT the cluster's
# shared coturn. Mainly for Docker Desktop (macOS/Windows), where the container
# runs in a VM and the browser can't reach the container's host candidates so
# media won't flow direct. On a Linux host with `--network host` you usually
# don't need this — host candidates are directly reachable. See README.
#
# We start a local coturn and point Selkies at it via the same SELKIES_TURN_*
# env Selkies reads in-cluster; Selkies then hands that TURN config to its own
# bundled web client, so the standalone UI just works.
if [ -n "${SELKIES_USE_INTERNAL_TURN:-}" ] && [ "${SELKIES_USE_INTERNAL_TURN}" != "0" ]; then
  TURN_SECRET="${SELKIES_TURN_SHARED_SECRET:-internal}"
  TURN_PORT="${INTERNAL_TURN_PORT:-3478}"
  # Address the browser uses to reach the relay. 127.0.0.1 works when the TURN
  # listening + relay ports are published to the same host the browser runs on
  # (see the README's docker run). Override for a LAN client.
  TURN_EXTERNAL_IP="${INTERNAL_TURN_EXTERNAL_IP:-127.0.0.1}"
  echo "[entrypoint] starting internal coturn on ${TURN_PORT} (external-ip=${TURN_EXTERNAL_IP})"
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

# System D-Bus for the XFCE session.
mkdir -p /var/run/dbus && rm -f /var/run/dbus/pid
dbus-daemon --system --fork

# Headless PulseAudio with a null sink. Selkies always opens a second WebRTC
# connection for audio (captured via `pulsesrc`); the Selkies web client only
# clears its "Waiting for stream" overlay once BOTH the video AND audio tracks
# report connected. Without a running PulseAudio the audio track never connects
# and the overlay hangs forever even though video is fine. The null sink's
# monitor is a valid (silent) capture source; we make it the default source so
# Selkies' device-less pulsesrc picks it up. Runs as root (container) with an
# anonymous unix socket that the root Selkies process connects to via PULSE_SERVER.
export PULSE_SERVER="unix:/tmp/pulse/native"
# Clear PulseAudio's runtime state (pid file, sockets) left behind by a previous
# run of this same container (e.g. `docker restart`, or a k8s container restart
# that reuses the writable layer). A stale pid file makes the daemon below
# refuse to start ("Daemon already running") even though the old process is
# long dead, silently breaking audio (and the "Waiting for stream" overlay)
# until the pod is recreated from scratch instead of just restarted.
rm -rf /root/.config/pulse /tmp/pulse-*
mkdir -p /tmp/pulse && chmod 777 /tmp/pulse
pulseaudio --daemonize=false --realtime=false --disallow-exit --exit-idle-time=-1 \
  --load="module-native-protocol-unix auth-anonymous=1 socket=/tmp/pulse/native" \
  --load="module-null-sink sink_name=virtual_speaker sink_properties=device.description=virtual_speaker" \
  --log-target=stderr &
# Wait for the socket, then make the null sink's monitor the default source.
for i in $(seq 1 30); do [ -S /tmp/pulse/native ] && break; sleep 0.2; done
pactl set-default-source virtual_speaker.monitor 2>/dev/null || \
  echo "[entrypoint] warning: could not set default pulse source (audio track may not connect)"

# Headless X server (software framebuffer) that Selkies' ximagesrc captures.
Xvfb "${DISPLAY}" -screen 0 "${RES}x24" -ac +extension RANDR &
for i in $(seq 1 30); do xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1 && break; sleep 0.2; done

# XFCE on that display, as the desktop user.
su - "${USER_NAME}" -c "env DISPLAY=${DISPLAY} dbus-launch --exit-with-session startxfce4" &

# Selkies: software x264 encoder, signaling+web on SELKIES_PORT, dynamic resize
# disabled (the portal sizes the video element). TURN is taken from the
# SELKIES_TURN_* env the operator injected (see config._selkies_turn_env); absent
# TURN falls back to host candidates (only works with host networking).
exec selkies-gstreamer \
  --addr=0.0.0.0 \
  --port="${SELKIES_PORT}" \
  --web_root=/opt/gst-web \
  --enable_https=false \
  --enable_basic_auth=false \
  --encoder=x264enc \
  --enable_resize=false
