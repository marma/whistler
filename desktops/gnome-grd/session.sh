#!/usr/bin/env bash
# Unprivileged half: the actual GNOME session + grd, run as the desktop user.
#
# Pipeline:
#   gnome-shell --headless --virtual-monitor  -> Mutter RemoteDesktop/ScreenCast
#                                                 D-Bus API + a PipeWire stream
#   gnome-remote-desktop-daemon               -> serves that stream as RDP/3389
set -e

RES="${WHISTLER_RESOLUTION:-1920x1080}"

# --- runtime dir + session bus -------------------------------------------------
# /run/user/<uid> is managed by logind/pam, but in a bare container it may not be
# set up, so use a user-writable XDG_RUNTIME_DIR.
export XDG_RUNTIME_DIR="/tmp/xdg-runtime-$(id -u)"
mkdir -p "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"
eval "$(dbus-launch --sh-syntax)"
export DBUS_SESSION_BUS_ADDRESS DBUS_SESSION_BUS_PID

# --- gnome-keyring (grd stores RDP creds via libsecret) ------------------------
# Unlocked with an empty password — acceptable here, the password is not the
# security boundary (the per-session NetworkPolicy is).
eval "$(printf '\n' | gnome-keyring-daemon --unlock --components=secrets)"
export GNOME_KEYRING_CONTROL SSH_AUTH_SOCK

# --- PipeWire (ScreenCast transport grd relies on) -----------------------------
pipewire &
wireplumber &
pipewire-pulse &

# --- TLS cert (grd always uses TLS; guacd connects with ignore-cert=true) ------
CERT_DIR="$HOME/.local/share/grd-tls"
mkdir -p "$CERT_DIR"
if [ ! -f "$CERT_DIR/cert.pem" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -subj "/CN=whistler-gnome-grd" \
    -keyout "$CERT_DIR/key.pem" -out "$CERT_DIR/cert.pem"
fi

# --- headless GNOME Shell (Wayland compositor + virtual monitor) ---------------
# --headless uses Mutter's headless backend (no DRM/KMS/seat; llvmpipe).
gnome-shell --headless --virtual-monitor "$RES" &

# Wait for the Shell to own the Mutter RemoteDesktop/ScreenCast D-Bus names that
# grd screen-shares through — configuring grd before they exist races (grd logs
# "Error connecting to the screencast service"). gdbus wait blocks until the name
# appears; the trailing sleep gives ScreenCast a beat to follow RemoteDesktop.
gdbus wait --session --timeout 30 org.gnome.Mutter.RemoteDesktop \
  || echo "WARN: timed out waiting for org.gnome.Mutter.RemoteDesktop" >&2
gdbus wait --session --timeout 10 org.gnome.Mutter.ScreenCast \
  || echo "WARN: timed out waiting for org.gnome.Mutter.ScreenCast" >&2
sleep 1

# --- configure + run gnome-remote-desktop --------------------------------------
grdctl rdp set-tls-cert "$CERT_DIR/cert.pem"
grdctl rdp set-tls-key  "$CERT_DIR/key.pem"
grdctl rdp set-credentials "$DESKTOP_USER" "$DESKTOP_PASSWORD"
grdctl rdp disable-view-only   # allow remote control, not just viewing
# `grdctl rdp enable` also tries to flip a systemd *user* unit on, which aborts
# before it sets the gsettings switch when there's no systemd user manager (our
# case). Set the switch directly — that's what the daemon actually reads to open
# the RDP listener.
gsettings set org.gnome.desktop.remote-desktop.rdp enable true

# Daemon in the foreground so the container lifecycle tracks it. Shares the
# headless gnome-shell session started above (same session bus).
#
# VERIFIED locally: grd opens the RDP listener on 3389 and completes the RDP
# X.224 negotiation handshake. NOT yet verified: the actual pixel path — grd
# capturing the headless monitor via the Mutter ScreenCast/PipeWire stream and a
# real client (guacd) rendering a desktop. A "Error connecting to the screencast
# service" line in the log before the first client connect is expected (grd binds
# the stream lazily); if the desktop stays black once guacd connects, that's the
# place to look (PipeWire / Mutter ScreenCast availability in this session).
exec /usr/libexec/gnome-remote-desktop-daemon
