#!/bin/sh
# Started by xrdp/sesman for each session. This is the desktop-agnostic half of
# the bridge: every DE needs a *session* D-Bus and an XDG_RUNTIME_DIR or you get
# a black screen with only the mouse cursor (the WM/panel fail to start). The
# actual desktop is whatever WHISTLER_SESSION_CMD a derived image sets — e.g.
# `ENV WHISTLER_SESSION_CMD=startxfce4` in an xfce image's Dockerfile.
if [ -r /etc/profile ]; then . /etc/profile; fi

export XDG_RUNTIME_DIR="/tmp/xdg-runtime-$(id -u)"
mkdir -p "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"

# (No forced full repaint here: RDP delivers a full initial surface on every
# connection, and the portal opens a fresh guacd->RDP connection per browser
# connect — so both new sessions and reconnects get a complete first frame.)

# Fail loud rather than dropping the user into a black screen if a base-only
# image (no DE) is run by mistake.
: "${WHISTLER_SESSION_CMD:?WHISTLER_SESSION_CMD is unset — this is the base-rdp image, build a desktop image FROM it and set WHISTLER_SESSION_CMD to its session command}"

# Word-splitting is intentional: WHISTLER_SESSION_CMD may include arguments.
# shellcheck disable=SC2086
exec dbus-launch --exit-with-session ${WHISTLER_SESSION_CMD}
