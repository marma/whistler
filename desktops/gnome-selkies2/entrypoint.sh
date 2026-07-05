#!/usr/bin/env bash
# Bring up a software-rendered GNOME Shell (X11) session and stream it with
# Selkies 2.x (pixelflux over WebSockets). Same plain-entrypoint architecture as
# ../xfce-selkies2 — no systemd, no display manager: the pieces start in order
# (identity → dbus → pulse → Xvfb → GNOME session → selkies in the foreground)
# and the container lifecycle tracks the selkies server.
#
# The load-bearing bet (design/creating_desktops.md §8, verify item a): GNOME 46
# gnome-session starts here WITHOUT `systemd --user`. It does because systemd is
# not PID 1, so gnome-session's `sd_booted()` is false and it uses its built-in
# component launcher instead of session .target units. If a future base bump
# regresses this, the symptom is gnome-session aborting with "Failed to obtain
# session bus" and the fix is the systemd-PID1 architecture of
# ../gnome-flashback-webrtc (which also costs --privileged + Kata in prod).
set -e

# --- Runtime-configurable identity -------------------------------------------
# Unlike ../xfce-selkies2's hardcoded `abc`, name/UID/GID/sudo come from env so a
# per-user home volume mounted at /home/$USER_NAME lines up by ownership. We do
# NOT `chown -R` the home volume: per the image contract its contents are
# assumed already owned by PUID:PGID (checking every start is O(files) and
# pointless once provisioned).
USER_NAME="${DESKTOP_USER:-abc}"
PUID="${PUID:-1000}"
PGID="${PGID:-1000}"
HOME_DIR="/home/${USER_NAME}"

# Make DESKTOP_USER/PUID/PGID authoritative. Ubuntu 24.04's base image ships a
# stock `ubuntu` user+group at UID/GID 1000; if a *different* identity already
# holds our target UID/GID we must remove it first, or the desktop silently runs
# as that identity (e.g. `ubuntu` out of /home/ubuntu) and DESKTOP_USER is
# ignored. We only delete when the holder isn't already our user, so a container
# restart reusing the layer stays idempotent.
cur_u="$(getent passwd "${PUID}" | cut -d: -f1 || true)"
if [ -n "${cur_u}" ] && [ "${cur_u}" != "${USER_NAME}" ]; then
  userdel "${cur_u}" 2>/dev/null || true
fi
cur_g="$(getent group "${PGID}" | cut -d: -f1 || true)"
if [ -n "${cur_g}" ] && [ "${cur_g}" != "${USER_NAME}" ]; then
  groupdel "${cur_g}" 2>/dev/null || true
fi

if ! getent group "${PGID}" >/dev/null; then
  groupadd -g "${PGID}" "${USER_NAME}"
fi
# -M because the home volume is pre-provisioned and we must not overwrite it with
# skel; -o tolerates a non-unique UID defensively.
if ! getent passwd "${PUID}" >/dev/null; then
  useradd -o -u "${PUID}" -g "${PGID}" -d "${HOME_DIR}" -s /bin/bash -M "${USER_NAME}"
fi
USER_NAME="$(getent passwd "${PUID}" | cut -d: -f1)"

# Ensure $HOME exists and is at least top-level owned by the user, so a *fresh*
# run with no mounted volume still has a working home. This is a single chown of
# the directory node, not a recursive walk of a populated volume.
mkdir -p "${HOME_DIR}"
chown "${PUID}:${PGID}" "${HOME_DIR}"

# Optional passwordless sudo. Off by default; the per-session NetworkPolicy is
# the security boundary, not the desktop's privilege model (see desktops/README).
if [ "${DESKTOP_SUDO:-false}" = "true" ]; then
  usermod -aG sudo "${USER_NAME}"
  echo "${USER_NAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/whistler-desktop
  chmod 0440 /etc/sudoers.d/whistler-desktop
fi

# XDG_RUNTIME_DIR: GNOME (dconf, gvfs, keyring, the session bus socket dir) needs
# a per-user 0700 runtime dir. logind would normally make it; here we do.
export XDG_RUNTIME_DIR="/run/user/${PUID}"
mkdir -p "${XDG_RUNTIME_DIR}"
chown "${PUID}:${PGID}" "${XDG_RUNTIME_DIR}"
chmod 700 "${XDG_RUNTIME_DIR}"

# Force gnome-shell onto its built-in *dummy* login manager. The `systemd`
# package is pulled in transitively by GNOME deps, and its maintainer scripts
# bake an empty /run/systemd/seats into the image layer. gnome-shell's
# loginManager.js keys off exactly `GLib.access('/run/systemd/seats')` to decide
# systemd-logind is present, then every login1 D-Bus call throws
# ("org.freedesktop.login1 ... exited with status 1") because nothing is
# actually managing logind without systemd as PID 1 — and gnome-session gives up
# to the "Oh no, something has gone wrong" failed screen. Removing the dir makes
# haveSystemd() false, so gnome-shell uses LoginManagerDummy and comes up clean.
# (We forgo logind-only features — lock/suspend/seat switching — which are
# meaningless for a single-user streamed desktop anyway.) This is the
# unprivileged analogue of ../gnome-flashback-webrtc's systemd-PID1 path.
rm -rf /run/systemd

SELKIES_PORT="${SELKIES_PORT:-8082}"
RES="${SELKIES_RESOLUTION:-1280x720}"

# --- System D-Bus -------------------------------------------------------------
mkdir -p /var/run/dbus && rm -f /var/run/dbus/pid
dbus-daemon --system --fork

# --- Headless PulseAudio (null sink named `output`) ---------------------------
# Identical to ../xfce-selkies2: pcmflux captures the device literally named
# "output.monitor"; the sink MUST be named `output` or pa_simple_new() fails
# with "No such entity". Clear stale runtime state so a container *restart* (vs a
# fresh pod) doesn't hit "Daemon already running" and silently lose audio.
export PULSE_SERVER="unix:/tmp/pulse/native"
rm -rf "${HOME_DIR}/.config/pulse" /root/.config/pulse /tmp/pulse-*
mkdir -p /tmp/pulse && chmod 777 /tmp/pulse
pulseaudio --daemonize=false --realtime=false --disallow-exit --exit-idle-time=-1 \
  --load="module-native-protocol-unix auth-anonymous=1 socket=/tmp/pulse/native" \
  --load="module-null-sink sink_name=output sink_properties=device.description=output" \
  --log-target=stderr &
for i in $(seq 1 30); do [ -S /tmp/pulse/native ] && break; sleep 0.2; done
pactl set-default-source output.monitor 2>/dev/null || \
  echo "[entrypoint] warning: could not set default pulse source (audio may not work)"

# --- Headless X server --------------------------------------------------------
# Started at the RandR ceiling (not RES): Xvfb bakes its maximum size in at
# startup and can never grow past it, so starting at RES would permanently cap
# dynamic resize. +extension GLX so mutter/Shell can get a (software) GL context.
XVFB_MAX_RES="7680x4320"
Xvfb "${DISPLAY}" -screen 0 "${XVFB_MAX_RES}x24" -ac +extension RANDR +extension GLX &
for i in $(seq 1 30); do xdpyinfo -display "${DISPLAY}" >/dev/null 2>&1 && break; sleep 0.2; done
# Shrink to RES in the background: a fresh Xvfb rejects new modes for ~15-20s.
( for attempt in $(seq 1 60); do
    selkies-resize "${RES}" >/dev/null 2>&1 && break
    sleep 1
  done & )

# --- GNOME Shell session, as the unprivileged desktop user --------------------
# gnome-session-launch.sh runs gnome-shell + a curated set of settings daemons
# directly (see that file for why NOT plain `gnome-session`). Env we set for it:
#   LIBGL_ALWAYS_SOFTWARE/GALLIUM_DRIVER/__GLX_VENDOR_LIBRARY_NAME - force Mesa
#     llvmpipe: there is no GPU, so mutter's GL context must come from Xvfb's
#     software GLX (verified: gnome-shell 46 composits fine this way).
#   XDG_SESSION_TYPE=x11 / GDK_BACKEND=x11 - keep the Shell and GTK apps on X11;
#     without them mutter may try Wayland on Xvfb and fail.
#   XDG_CURRENT_DESKTOP=GNOME - app-visibility / portal hints expect it.
#   GSK_RENDERER=cairo - REQUIRED for GTK4 apps (Files/nautilus, Text Editor,
#     Settings). GTK4 renders window content via GSK's OpenGL renderer by
#     default, which produces GARBAGE under llvmpipe: the window shows a stale
#     copy of the desktop framebuffer instead of its own content (it's still
#     resizable and stacks correctly — only the pixels are wrong). The cairo
#     software renderer fixes it. Does NOT affect gnome-shell/mutter
#     (Clutter/Cogl, not GSK — designed for GL, works) or GTK3 apps like
#     gnome-terminal (cairo already). See this image's README.
#   NO_AT_BRIDGE / GTK_A11Y=none - no accessibility bus here; silence at-spi.
# dbus-run-session gives the session its own bus (no systemd --user). `su -` is a
# login shell so PAM limits/env are sane; we re-export what su drops.
# NOTE: everything between the quotes is ONE shell string passed to su -c — no
# `#` comments inside it (they would be literal argv, not comments).
su - "${USER_NAME}" -c "\
  env DISPLAY=${DISPLAY} \
      XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR} \
      PULSE_SERVER=${PULSE_SERVER} \
      XDG_SESSION_TYPE=x11 \
      GDK_BACKEND=x11 \
      XDG_CURRENT_DESKTOP=GNOME \
      LIBGL_ALWAYS_SOFTWARE=1 \
      GALLIUM_DRIVER=llvmpipe \
      __GLX_VENDOR_LIBRARY_NAME=mesa \
      GSK_RENDERER=cairo \
      NO_AT_BRIDGE=1 \
      GTK_A11Y=none \
      dbus-run-session -- /usr/local/bin/gnome-session-launch.sh" &

# --- Selkies 2.x --------------------------------------------------------------
# pixelflux capture of $DISPLAY, x264 software encode, web client + WebSocket on
# one port, run as container-root. Flag spelling matters (dash-separated,
# parse_known_args ignores typos silently); basic auth is ON by default so we
# disable it explicitly — the per-session NetworkPolicy is the security boundary.
# HTTPS off by default (http://localhost is a secure context; other origins in
# dev need SELKIES_ENABLE_HTTPS=true). See design/creating_desktops.md §4.
#
# --h264-streaming-mode=true is REQUIRED for GNOME Shell (default off, and the
# xfce-selkies2 image leaves it off). pixelflux normally sends only *damaged*
# regions — great for XFCE, where a static window keeps its pixels in the X
# framebuffer that pixelflux reads. But GNOME Shell's mutter is an OpenGL
# compositor: once it has composited a static window it emits no further damage
# for it, so damage-based capture never (re)sends those regions and the client
# canvas shows them as BLACK until something forces a full repaint (opening the
# overview, etc.). Streaming mode makes pixelflux continuously encode the full
# frame like an ordinary video stream, so static content is always present.
# Cost: constant bandwidth/CPU even on an idle desktop (the "static sends ~nothing"
# property of damage mode is gone) — the right trade for a GL compositor. Env-
# overridable, but turning it off brings the black-static-window bug straight
# back. See design/creating_desktops.md §5 and this image's README.
exec selkies \
  --addr=0.0.0.0 \
  --port="${SELKIES_PORT}" \
  --mode="${SELKIES_MODE:-websockets}" \
  --encoder="${SELKIES_ENCODER:-x264enc}" \
  --h264-streaming-mode="${SELKIES_H264_STREAMING_MODE:-true}" \
  --enable-https="${SELKIES_ENABLE_HTTPS:-false}" \
  --enable-basic-auth=false \
  --web-root=/opt/selkies-web
