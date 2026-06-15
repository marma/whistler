# Wayland-native GNOME over RDP — served by gnome-remote-desktop (grd), NOT xrdp.
# GNOME defaults to Wayland and is dropping the GNOME-on-Xorg session, so this
# image does NOT build on base-rdp (which is xorgxrdp/X11). Instead it runs a
# headless gnome-shell (Wayland compositor + virtual monitor) and lets grd serve
# that session over RDP on 3389 — the same port/contract guacd already expects.
# Multi-arch (amd64 + arm64); software-rendered (llvmpipe), no GPU required.
#
# Requires a privileged container (--privileged) so systemd can run as PID 1.
# In production, pair with a Kata Containers RuntimeClass to restore VM-level
# isolation without managing full VM images.
#
# guacd connects on 3389 with the well-known creds below (ignore-cert=true; grd
# always uses TLS, so the cert is self-signed at startup). Transport safety comes
# from the per-session NetworkPolicy, not the password — see design/vdi.md.
#
# EXPERIMENTAL — verified to bring up grd in a container; see session.sh.
FROM ubuntu:26.04

ENV DEBIAN_FRONTEND=noninteractive

# systemd + systemd-sysv: PID 1 and the sysvinit compatibility shims.
# Everything else is as before; libpam-systemd is pulled in by systemd but
# listed explicitly since pam_systemd is what registers the logind session.
RUN apt-get update && apt-get install -y --no-install-recommends \
      systemd \
      systemd-sysv \
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
      dbus-user-session \
      libpam-systemd \
      xwayland \
      libgl1-mesa-dri \
      openssl \
 && rm -rf /var/lib/apt/lists/*

# Mask units that fail or are pointless in a privileged container. udevd in a
# privileged container would try to manage the host's devices — mask it. The
# getty and timer units just add noise. Everything else (dbus, logind,
# user@.service, user-runtime-dir@.service) is kept so the full session stack
# works as it would on a real machine.
RUN systemctl mask \
      systemd-remount-fs.service \
      dev-hugepages.mount \
      sys-fs-fuse-connections.mount \
      systemd-udevd.service \
      systemd-udevd-control.socket \
      systemd-udevd-kernel.socket \
      getty@tty1.service \
      console-getty.service \
      systemd-timesyncd.service \
      apt-daily.service \
      apt-daily.timer \
      apt-daily-upgrade.service \
      apt-daily-upgrade.timer

# Well-known desktop login. Override at build time for other catalog entries;
# keep in sync with the DesktopTemplate's connectionParams.
ARG DESKTOP_USER=abc
ARG DESKTOP_PASSWORD=abc
ENV DESKTOP_USER=${DESKTOP_USER} \
    DESKTOP_PASSWORD=${DESKTOP_PASSWORD}

RUN useradd -m -s /bin/bash "${DESKTOP_USER}"

# Systemd unit that runs the GNOME session as the desktop user with a full PAM
# (logind) session. User= is substituted at build time from the ARG above.
COPY gnome-desktop.service /etc/systemd/system/gnome-desktop.service
RUN sed -i "s/^User=abc$/User=${DESKTOP_USER}/" /etc/systemd/system/gnome-desktop.service \
 && systemctl enable gnome-desktop.service

COPY session.sh /usr/local/bin/session.sh
RUN chmod +x /usr/local/bin/session.sh

EXPOSE 3389
# SIGRTMIN+3 is systemd's clean-shutdown signal; without it docker stop hangs.
STOPSIGNAL SIGRTMIN+3
ENTRYPOINT ["/lib/systemd/systemd"]
