#!/usr/bin/env bash
# Display plane of a desktop pod: X server + audio daemon + Selkies 2.x
# streamer. No desktop environment — the workload container brings that and
# finds the display through the shared sockets. No systemd / no display
# manager: the pieces run directly so the container lifecycle tracks the
# selkies server in the foreground.
set -e

SELKIES_PORT="${SELKIES_PORT:-8082}"
RES="${SELKIES_RESOLUTION:-1280x720}"

# System D-Bus for PulseAudio's module loading; the workload container runs its
# own buses for the DE session.
mkdir -p /var/run/dbus && rm -f /var/run/dbus/pid
dbus-daemon --system --fork

# Headless PulseAudio with a null sink; pcmflux captures the sink's monitor.
# The native socket lives in /tmp/pulse — a shared emptyDir, so workload apps
# reach the same daemon via PULSE_SERVER and their playback is what gets
# streamed. auth-anonymous because the pod is the trust boundary.
export PULSE_SERVER="unix:/tmp/pulse/native"
# Clear runtime state left by a previous run of this same container (restart
# reusing the writable layer / the shared emptyDirs): a stale pid file makes
# the daemon refuse to start and silently kills audio; a stale X lock or
# socket makes Xvfb exit immediately.
rm -rf /root/.config/pulse /tmp/pulse-* /tmp/pulse/* /tmp/.X*-lock /tmp/.X11-unix/X*
mkdir -p /tmp/pulse && chmod 777 /tmp/pulse
# The sink MUST be named `output`: selkies 2.x tells pcmflux to capture from
# the device literally named "output.monitor" and pa_simple_new() fails with
# "No such entity" for anything else.
pulseaudio --daemonize=false --realtime=false --disallow-exit --exit-idle-time=-1 \
  --load="module-native-protocol-unix auth-anonymous=1 socket=/tmp/pulse/native" \
  --load="module-null-sink sink_name=output sink_properties=device.description=output" \
  --log-target=stderr &
for i in $(seq 1 30); do [ -S /tmp/pulse/native ] && break; sleep 0.2; done
pactl set-default-source output.monitor 2>/dev/null || \
  echo "[entrypoint] warning: could not set default pulse source (audio may not work)"

# /tmp/.X11-unix is a shared emptyDir here (root-owned 0755 by default); open
# it up so the workload container's (differently-uid'd) X clients can reach
# the socket Xvfb drops in it. Sticky like a real X11 dir.
mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix

# Headless X server (software framebuffer) that pixelflux captures. Started at
# the RandR ceiling, not at RES: Xvfb bakes its *maximum* size in from this
# `-screen` argument and can never grow past it, so starting at RES would
# permanently cap dynamic resize. Start big, shrink to RES below. `-ac` turns
# off access control: any process that can reach the socket may connect —
# within the pod that is exactly the workload container, and the pod (plus its
# NetworkPolicy) is the security boundary.
# +extension GLX because the workload container's compositor may need a
# (software) GL context — GNOME Shell/mutter hard-requires one and renders via
# the workload's own Mesa llvmpipe against this server's GLX. Harmless for
# non-GL workloads like XFCE.
XVFB_MAX_RES="7680x4320"
Xvfb "${DISPLAY}" -screen 0 "${XVFB_MAX_RES}x24" -ac +extension RANDR +extension GLX &
for i in $(seq 1 30); do xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1 && break; sleep 0.2; done
# Shrink to RES in the background: a just-started Xvfb rejects RandR mode adds
# for its first ~15-20s, and retrying inline would stall startup (and with it
# the startupProbe gating the workload container) for that long.
( for attempt in $(seq 1 60); do
    selkies-resize "${RES}" >/dev/null 2>&1 && break
    sleep 1
  done & )

# Selkies 2.x: pixelflux capture of $DISPLAY, x264 software encode, web client
# + WebSocket streaming on one port. Flag spelling matters: 2.x CLI flags are
# dash-separated (--web-root, not --web_root) and the parser uses
# parse_known_args, so a misspelled flag is IGNORED SILENTLY and the default
# applies; booleans take =true/=false. (Everything also exists as
# SELKIES_<NAME> env vars.) Basic auth is ON by default in 2.x — off here
# explicitly; the per-session NetworkPolicy is the security boundary. This
# port is also the startupProbe target: it only opens once X is up, which is
# what lets the workload container assume DISPLAY works.
# --h264-streaming-mode: OFF by default (damage-based capture — static content
# sends ~nothing, right for XFCE-style X11 DEs) but REQUIRED =true for GL
# compositors like GNOME Shell: mutter composits once and emits no further
# damage for static windows, so damage mode never (re)sends those regions and
# the client canvas shows them BLACK until something forces a full repaint.
# Streaming mode continuously encodes the full frame like an ordinary video
# stream (cost: constant bandwidth/CPU even idle — the right trade for a GL
# compositor). The streamer can't know what the workload runs, so the template
# says so via streamerEnv (SELKIES_H264_STREAMING_MODE: "true").
exec selkies \
  --addr=0.0.0.0 \
  --port="${SELKIES_PORT}" \
  --mode="${SELKIES_MODE:-websockets}" \
  --encoder="${SELKIES_ENCODER:-x264enc}" \
  --h264-streaming-mode="${SELKIES_H264_STREAMING_MODE:-false}" \
  --enable-https="${SELKIES_ENABLE_HTTPS:-false}" \
  --enable-basic-auth=false \
  --web-root=/opt/selkies-web
