#!/usr/bin/env bash
# Start xrdp in the foreground (no systemd in a container). sesman is the session
# manager xrdp hands authenticated connections to; both must run.
set -e

# Clear any stale pid files from a previous run / image layer.
rm -f /var/run/xrdp/*.pid 2>/dev/null || true

# A system dbus is needed for the desktop session.
mkdir -p /var/run/dbus
rm -f /var/run/dbus/pid
dbus-daemon --system --fork

# Session manager (daemonizes), then the xrdp listener in the foreground so the
# container's lifecycle tracks it.
/usr/sbin/xrdp-sesman
exec /usr/sbin/xrdp --nodaemon
