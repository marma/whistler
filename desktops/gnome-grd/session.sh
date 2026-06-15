#!/usr/bin/env bash
# Session script — runs as the desktop user via gnome-desktop.service.
#
# With systemd as PID 1 and PAMName=login in the service unit:
#   - pam_systemd calls logind CreateSession → gnome-shell gets a real login1 user
#   - user@UID.service starts systemd --user, which provides the session D-Bus
#     at $XDG_RUNTIME_DIR/bus and socket-activates pipewire/wireplumber
#   - XDG_RUNTIME_DIR is set by pam_systemd before this script runs
#
# We wait for the session bus, then hand off to gnome-shell + grd.
set -e

export NO_AT_BRIDGE=1
export XDG_SESSION_TYPE=wayland
export DISPLAY=:0
export GDK_BACKEND=wayland,x11
export GSK_RENDERER=cairo
export WAYLAND_DISPLAY=wayland-0
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"

# Wait for systemd --user to create the session bus socket. This also acts as a
# gate: once the socket exists, logind has the user registered (NoSuchUser gone)
# and pipewire.socket is active for socket activation.
for _i in $(seq 60); do
  [ -S "$XDG_RUNTIME_DIR/bus" ] && break
  sleep 1
done
export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"

# --- gnome-keyring (grd stores RDP creds via libsecret) ------------------------
eval "$(printf '\n' | gnome-keyring-daemon --unlock --components=secrets)"
export GNOME_KEYRING_CONTROL SSH_AUTH_SOCK

# --- TLS cert (grd always uses TLS; guacd connects with ignore-cert=true) ------
CERT_DIR="$HOME/.local/share/grd-tls"
mkdir -p "$CERT_DIR"
if [ ! -f "$CERT_DIR/cert.pem" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -subj "/CN=whistler-gnome-grd" \
    -keyout "$CERT_DIR/key.pem" -out "$CERT_DIR/cert.pem"
fi

# --- headless GNOME Shell (Wayland compositor) ---------------------------------
# No --virtual-monitor: grd --headless creates virtual monitors per-session at
# the client's requested size, enabling dynamic resize via DisplayControl PDUs.
gnome-shell --headless --wayland-display wayland-0 &

gdbus wait --session --timeout 30 org.gnome.Mutter.RemoteDesktop \
  || echo "WARN: timed out waiting for org.gnome.Mutter.RemoteDesktop" >&2
gdbus wait --session --timeout 10 org.gnome.Mutter.ScreenCast \
  || echo "WARN: timed out waiting for org.gnome.Mutter.ScreenCast" >&2
sleep 1

dbus-update-activation-environment --all || true


# --- gnome-remote-desktop (headless mode) --------------------------------------
grdctl --headless rdp set-tls-cert "$CERT_DIR/cert.pem"
grdctl --headless rdp set-tls-key  "$CERT_DIR/key.pem"
grdctl --headless rdp set-credentials "$DESKTOP_USER" "$DESKTOP_PASSWORD"
grdctl --headless rdp disable-view-only
gsettings set org.gnome.desktop.remote-desktop.rdp.headless enable true
gsettings set org.gnome.desktop.remote-desktop.rdp tls-cert "$CERT_DIR/cert.pem"
gsettings set org.gnome.desktop.remote-desktop.rdp tls-key  "$CERT_DIR/key.pem"

exec /usr/libexec/gnome-remote-desktop-daemon --headless
