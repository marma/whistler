#!/usr/bin/env bash
# Start the real GNOME Shell (X11 backend) on a display somebody else owns —
# the streamer sidecar in-cluster, any X server elsewhere. No systemd, no
# display manager: identity → dbus → session, with the display/audio plane
# (Xvfb, PulseAudio, Selkies) being the streamer's job.
#
# The load-bearing bet: GNOME 46 gnome-session starts WITHOUT `systemd --user`
# because systemd is not PID 1 (sd_booted() false → built-in component
# launcher). If a base bump regresses this, the symptom is "Failed to obtain
# session bus" — and the fix costs systemd-PID1 + --privileged (see the
# Dockerfile header).
set -e

# --- Runtime-configurable identity -------------------------------------------
# Name/UID/GID/sudo come from env so a per-user home volume mounted at
# /home/$USER_NAME lines up by ownership. No recursive chown — the volume's
# contents are assumed already owned by PUID:PGID.
USER_NAME="${DESKTOP_USER:-abc}"
PUID="${PUID:-1000}"
PGID="${PGID:-1000}"
HOME_DIR="/home/${USER_NAME}"

# Make DESKTOP_USER/PUID/PGID authoritative over 24.04's stock `ubuntu`
# user/group at 1000:1000 (delete only a *different* holder, so restarts stay
# idempotent).
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
if ! getent passwd "${PUID}" >/dev/null; then
  useradd -o -u "${PUID}" -g "${PGID}" -d "${HOME_DIR}" -s /bin/bash -M "${USER_NAME}"
fi
USER_NAME="$(getent passwd "${PUID}" | cut -d: -f1)"

mkdir -p "${HOME_DIR}"
chown "${PUID}:${PGID}" "${HOME_DIR}"

# Optional passwordless sudo (off by default; the per-session NetworkPolicy is
# the security boundary).
if [ "${DESKTOP_SUDO:-false}" = "true" ]; then
  usermod -aG sudo "${USER_NAME}"
  echo "${USER_NAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/whistler-desktop
  chmod 0440 /etc/sudoers.d/whistler-desktop
fi

# XDG_RUNTIME_DIR: GNOME (dconf, gvfs, keyring, session bus socket dir) needs a
# per-user 0700 runtime dir; logind would normally make it, here we do.
export XDG_RUNTIME_DIR="/run/user/${PUID}"
mkdir -p "${XDG_RUNTIME_DIR}"
chown "${PUID}:${PGID}" "${XDG_RUNTIME_DIR}"
chmod 700 "${XDG_RUNTIME_DIR}"

# Force gnome-shell onto its built-in *dummy* login manager: the systemd
# package (pulled transitively) bakes an empty /run/systemd/seats into the
# layer, and gnome-shell's loginManager.js keys off exactly
# `GLib.access('/run/systemd/seats')` to decide logind is present — then every
# login1 D-Bus call fails ("org.freedesktop.login1 ... exited with status 1")
# and gnome-session gives up to the "Oh no, something has gone wrong" screen.
# Removing the dir makes haveSystemd() false → LoginManagerDummy → clean boot.
# We forgo logind-only features (lock/suspend/seat switching), meaningless for
# a single-user streamed desktop.
rm -rf /run/systemd

# --- Wait for the shared display ----------------------------------------------
# In-cluster the sidecar's startupProbe guarantees the display before we start,
# so this falls through immediately; outside k8s (compose/manual docker) wait
# briefly, then fail loudly instead of crash-looping into scrollback.
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

# --- System D-Bus --------------------------------------------------------------
mkdir -p /var/run/dbus && rm -f /var/run/dbus/pid
dbus-daemon --system --fork

# --- GNOME Shell session, foreground, as the unprivileged desktop user ---------
# The env block, line by line:
#   LIBGL_ALWAYS_SOFTWARE/GALLIUM_DRIVER/__GLX_VENDOR_LIBRARY_NAME — force Mesa
#     llvmpipe: no GPU here, so mutter's GL context comes from the streamer's
#     Xvfb software GLX (verified: gnome-shell 46 composits fine this way).
#   XDG_SESSION_TYPE=x11 / GDK_BACKEND=x11 — keep Shell and GTK on X11;
#     without them mutter may try Wayland and fail.
#   XDG_CURRENT_DESKTOP=GNOME — app-visibility / portal hints expect it.
#   GSK_RENDERER=cairo — REQUIRED for GTK4 apps (Files, Text Editor,
#     Settings). GTK4's default GSK OpenGL renderer produces GARBAGE under
#     llvmpipe: the window shows a stale copy of the desktop framebuffer
#     instead of its own content (still resizable, stacks correctly — only the
#     pixels are wrong). Cairo fixes it; gnome-shell itself (Clutter/Cogl, not
#     GSK) and GTK3 apps are unaffected.
#   NO_AT_BRIDGE / GTK_A11Y=none — no accessibility bus here; silence at-spi.
# dbus-run-session gives the session its own bus (no systemd --user); the
# session is exec'd in the FOREGROUND so the container tracks it and a Shell
# crash restarts the container against the still-running display.
# NOTE: one shell string passed to su -c — no `#` comments inside it.
exec su - "${USER_NAME}" -c "\
  env DISPLAY=${DISPLAY} \
      XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR} \
      PULSE_SERVER='${PULSE_SERVER}' \
      XDG_SESSION_TYPE=x11 \
      GDK_BACKEND=x11 \
      XDG_CURRENT_DESKTOP=GNOME \
      LIBGL_ALWAYS_SOFTWARE=1 \
      GALLIUM_DRIVER=llvmpipe \
      __GLX_VENDOR_LIBRARY_NAME=mesa \
      GSK_RENDERER=cairo \
      NO_AT_BRIDGE=1 \
      GTK_A11Y=none \
      dbus-run-session -- /usr/local/bin/gnome-session-launch.sh"
