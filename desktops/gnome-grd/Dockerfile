# Wayland-native GNOME over RDP — served by gnome-remote-desktop (grd), NOT xrdp.
# GNOME defaults to Wayland and is dropping the GNOME-on-Xorg session, so this
# image does NOT build on base-rdp (which is xorgxrdp/X11). Instead it runs a
# headless gnome-shell (Wayland compositor + virtual monitor) and lets grd serve
# that session over RDP on 3389 — the same port/contract guacd already expects.
# Multi-arch (amd64 + arm64); software-rendered (llvmpipe), no GPU required.
#
# GNOME Shell 50 hard-requires logind (org.freedesktop.login1) and aborts without
# it. We do NOT run systemd as PID 1; instead the entrypoint starts a system
# D-Bus + systemd-logind standalone, then drops to an unprivileged user (see
# entrypoint.sh / session.sh). This keeps the lightweight, non-privileged pod
# model the other desktops use.
#
# guacd connects on 3389 with the well-known creds below (ignore-cert=true; grd
# always uses TLS, so the cert is self-signed at startup). Transport safety comes
# from the per-session NetworkPolicy, not the password — see design/vdi.md.
#
# EXPERIMENTAL — verified to bring up grd in a container; see session.sh.
FROM ubuntu:26.04

ENV DEBIAN_FRONTEND=noninteractive

# gnome-shell + gnome-remote-desktop are the core; PipeWire/WirePlumber carry the
# ScreenCast stream grd shares; gnome-keyring backs grd's credential store;
# llvmpipe (libgl1-mesa-dri) renders the shell without a GPU; xwayland lets the
# headless shell host X11 apps (and silences a fatal init path). libpam-systemd
# lets `runuser -l` register a logind session via pam_systemd. The rest is a
# usable baseline (terminal, files, settings, icons, fonts, wallpapers).
RUN apt-get update && apt-get install -y --no-install-recommends \
      gnome-shell \
      gnome-remote-desktop \
      gnome-session-bin \
      gnome-terminal \
      nautilus \
      gnome-control-center \
      gnome-backgrounds \
      adwaita-icon-theme \
      fonts-dejavu-core \
      pipewire \
      pipewire-pulse \
      wireplumber \
      gnome-keyring \
      libsecret-1-0 \
      dbus-x11 \
      libpam-systemd \
      xwayland \
      libgl1-mesa-dri \
      openssl \
 && rm -rf /var/lib/apt/lists/*

# Well-known desktop login. Override at build time for other catalog entries;
# keep in sync with the DesktopTemplate's connectionParams.
ARG DESKTOP_USER=abc
ARG DESKTOP_PASSWORD=abc
ENV DESKTOP_USER=${DESKTOP_USER} \
    DESKTOP_PASSWORD=${DESKTOP_PASSWORD}

RUN useradd -m -s /bin/bash "${DESKTOP_USER}"

# entrypoint.sh runs as root (system D-Bus + logind), then runuser's to
# DESKTOP_USER to run session.sh (gnome-shell refuses to run as root).
COPY entrypoint.sh /entrypoint.sh
COPY session.sh /usr/local/bin/session.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/session.sh

EXPOSE 3389
ENTRYPOINT ["/entrypoint.sh"]
