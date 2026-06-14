#!/usr/bin/env bash
# Root half of the GNOME-over-grd bring-up. Starts the system services that
# GNOME Shell needs (a system D-Bus + systemd-logind), then drops to the
# unprivileged desktop user to run the actual session — gnome-shell refuses to
# run as root. We deliberately do NOT run systemd as PID 1: just logind, so the
# pod stays unprivileged like the other desktops.
set -e

DESKTOP_USER="${DESKTOP_USER:-abc}"

# System D-Bus — org.freedesktop.login1 lives on the system bus.
mkdir -p /run/dbus
rm -f /run/dbus/pid
dbus-daemon --system --fork

# systemd-logind standalone. GNOME Shell 50 instantiates LoginManagerSystemd
# unconditionally and aborts if login1 is absent. logind also creates the
# /run/systemd/seats compat dir GNOME probes. These dirs must pre-exist.
mkdir -p /run/systemd/seats /run/systemd/sessions /run/systemd/users /run/systemd/machines
/usr/lib/systemd/systemd-logind &

# Give the system bus + logind a moment to own their names before the session
# starts querying them.
sleep 1

# Drop to the desktop user. `runuser -l` runs the PAM stack (pam_systemd) so the
# user gets a registered login1 session. Env that session.sh needs is passed
# explicitly because the login shell resets the environment.
exec runuser -l "$DESKTOP_USER" -c \
  "WHISTLER_RESOLUTION='${WHISTLER_RESOLUTION:-1920x1080}' \
   DESKTOP_USER='${DESKTOP_USER}' \
   DESKTOP_PASSWORD='${DESKTOP_PASSWORD:-abc}' \
   exec /usr/local/bin/session.sh"
