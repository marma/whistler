#!/usr/bin/env bash
# Bring up a software-rendered X session and stream it with Selkies 2.x
# (pixelflux over WebSockets). No systemd / no display-manager: the pieces run
# directly so the container lifecycle tracks the selkies server in the
# foreground. Structure mirrors ../xfce-webrtc/entrypoint.sh minus everything
# TURN/WebRTC — websockets mode needs no relay, so there is no internal-coturn
# option here at all.
set -e

USER_NAME="${DESKTOP_USER:-abc}"
SELKIES_PORT="${SELKIES_PORT:-8082}"
RES="${SELKIES_RESOLUTION:-1280x720}"

# System D-Bus for the XFCE session.
mkdir -p /var/run/dbus && rm -f /var/run/dbus/pid
dbus-daemon --system --fork

# Headless PulseAudio with a null sink. pcmflux (and the mic path) capture from
# PulseAudio; the null sink's monitor is a valid silent source. Runs as root
# (container) with an anonymous unix socket selkies reaches via PULSE_SERVER.
export PULSE_SERVER="unix:/tmp/pulse/native"
# Clear runtime state left by a previous run of this same container (docker/k8s
# restart reusing the writable layer): a stale pid file makes the daemon refuse
# to start ("Daemon already running") and silently kills audio until the pod is
# recreated instead of restarted.
rm -rf /root/.config/pulse /tmp/pulse-*
mkdir -p /tmp/pulse && chmod 777 /tmp/pulse
# The sink MUST be named `output`: selkies 2.x tells pcmflux to capture from
# the device literally named "output.monitor" (the sink name its own images
# use), and pa_simple_new() fails with "No such entity" for anything else.
pulseaudio --daemonize=false --realtime=false --disallow-exit --exit-idle-time=-1 \
  --load="module-native-protocol-unix auth-anonymous=1 socket=/tmp/pulse/native" \
  --load="module-null-sink sink_name=output sink_properties=device.description=output" \
  --log-target=stderr &
# Wait for the socket, then make the null sink's monitor the default source.
for i in $(seq 1 30); do [ -S /tmp/pulse/native ] && break; sleep 0.2; done
pactl set-default-source output.monitor 2>/dev/null || \
  echo "[entrypoint] warning: could not set default pulse source (audio may not work)"

# Headless X server (software framebuffer) that pixelflux captures. Started at
# the RandR ceiling, not at RES: Xvfb bakes both its *current* and *maximum*
# size in from this `-screen` argument and can never grow past it, so starting
# at RES would permanently cap every later dynamic resize (BadMatch /
# RRAddOutputMode). Start big, shrink to RES below. Same trap as 1.x — Selkies
# 2.x still resizes via xrandr + cvt (see display_utils.py).
XVFB_MAX_RES="7680x4320"
Xvfb "${DISPLAY}" -screen 0 "${XVFB_MAX_RES}x24" -ac +extension RANDR &
for i in $(seq 1 30); do xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1 && break; sleep 0.2; done
# Shrink from XVFB_MAX_RES down to RES in the background: a just-started Xvfb
# rejects --newmode/--addmode for the first ~15-20s (its RandR provider
# settling), and retrying inline would stall the whole connect flow for that
# long. `selkies-resize` is 2.x's console-script wrapper around the same
# xrandr resize path the server uses. Worst case the desktop stays at
# XVFB_MAX_RES until a client connects and resizes it.
( for attempt in $(seq 1 60); do
    selkies-resize "${RES}" >/dev/null 2>&1 && break
    sleep 1
  done & )

# XFCE on that display, as the desktop user.
su - "${USER_NAME}" -c "env DISPLAY=${DISPLAY} dbus-launch --exit-with-session startxfce4" &

# Selkies 2.x: pixelflux capture of $DISPLAY, x264 software encode, web client
# + WebSocket streaming on one port. Basic auth is ON by default in 2.x — turn
# it off explicitly; the per-session NetworkPolicy is the security boundary
# (same rationale as every other desktop image, see desktops/README.md).
#
# Flag spelling matters: unlike 1.x, 2.x CLI flags are dash-separated
# (--web-root, not --web_root) and the parser uses parse_known_args, so a
# misspelled flag is IGNORED SILENTLY and the default applies. Booleans take
# =true/=false. (Everything also exists as SELKIES_<NAME> env vars.)
# HTTPS is off by default (in-cluster the portal fronts the pod), but the 2.x
# web client refuses to run outside a browser secure context (WebCodecs is
# only defined there). http://localhost is a secure context; any other origin
# is not. SELKIES_ENABLE_HTTPS=true serves the image's self-signed snakeoil
# cert (ssl-cert package) so a dev browser on another machine can click
# through the warning and get a secure context.
exec selkies \
  --addr=0.0.0.0 \
  --port="${SELKIES_PORT}" \
  --mode="${SELKIES_MODE:-websockets}" \
  --encoder="${SELKIES_ENCODER:-x264enc}" \
  --enable-https="${SELKIES_ENABLE_HTTPS:-false}" \
  --enable-basic-auth=false \
  --web-root=/opt/selkies-web
