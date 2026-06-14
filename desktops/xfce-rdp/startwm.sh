#!/bin/sh
# Started by xrdp/sesman for each session. XFCE needs a *session* D-Bus and an
# XDG_RUNTIME_DIR; without the session bus you get a black screen with only the
# mouse cursor (the WM/panel fail to start).
if [ -r /etc/profile ]; then . /etc/profile; fi

export XDG_RUNTIME_DIR="/tmp/xdg-runtime-$(id -u)"
mkdir -p "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"

# (No forced full repaint here: RDP delivers a full initial surface on every
# connection, and the portal opens a fresh guacd->RDP connection per browser
# connect — so both new sessions and reconnects get a complete first frame. The
# old staggered xrefresh only ran once per session, so it never helped reconnects
# anyway; the real cause of the early partial paints was the relay's
# instruction-framing bug, now fixed.)

exec dbus-launch --exit-with-session startxfce4
