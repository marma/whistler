# GNOME Flashback desktop streamed over **WebRTC** via Selkies-GStreamer,
# software-encoded with x264 — no GPU required. This is the GNOME analogue of
# ../xfce-webrtc, on Ubuntu 26.04.
#
# Two things learned building xfce-webrtc/gnome-grd changed the recipe here:
#
# 1. GNOME Shell (Ubuntu's actual default desktop) dropped its X11 backend
#    entirely as of this GNOME version — `mutter --help` has no X11/nested mode
#    anymore, only `--wayland`. Selkies captures via `ximagesrc`, which needs a
#    real X11 display, so GNOME Shell is a dead end for this viewer (see
#    ../gnome-grd/Dockerfile, which instead runs GNOME Shell headless-Wayland
#    behind gnome-remote-desktop's RDP encoder). `gnome-session-flashback` is
#    still shipped and X11-native (GNOME Panel + Metacity) — real GNOME
#    technologies/theming, just not the dynamic-workspaces shell.
# 2. GNOME Session itself (v50) hard-requires a running `systemd --user`
#    instance to start ANY session — it aborts with "Failed to obtain session
#    bus" without one. `systemd --user` in turn refuses to run unless the
#    container was booted with systemd as PID 1 ("system has not been booted
#    with systemd"). There is no flag to disable this. So — unlike
#    xfce-webrtc — this image needs the same systemd-PID1 / --privileged
#    (Kata-isolated in prod) architecture as ../gnome-grd, just with
#    Xvfb+Selkies subbed in for gnome-shell's Wayland+grd path.
# 3. Ubuntu 26.04's GTK stack routes EVERY image load (icons included, not
#    just exotic formats) through `glycin-loaders`, which sandboxes each load
#    in a `bwrap` subprocess — it's a hard dependency of libgdk-pixbuf-2.0-0
#    itself (`apt remove glycin-loaders` cascades into removing gtk3/gtk4/
#    gnome-session-flashback entirely, there's no opting out). bwrap's sandbox
#    setup needs an unprivileged user+network namespace, which is blocked for
#    non-root callers on hosts with `kernel.apparmor_restrict_unprivileged_
#    userns=1` (a common Ubuntu hardening default) — even inside a privileged
#    container, since that check is about AppArmor confinement, not container
#    capabilities. The practical effect: metacity/gnome-panel/gnome-flashback
#    treat a failed icon load as fatal (`g_error`+abort) and crash-loop. Real
#    root bypasses that specific kernel check, which is why gnome-desktop.
#    service runs as root instead of an unprivileged desktop user like
#    xfce-webrtc/gnome-grd — see that unit file for the full explanation.
#
# GStreamer note: Selkies' prebuilt GStreamer release bundle only ships
# ubuntu20.04/22.04/24.04 builds (no 26.04 asset). Ubuntu 26.04's own apt
# GStreamer (1.28) is new enough to cover everything Selkies needs natively —
# ximagesrc, x264enc, webrtcbin, pulsesrc, gstreamer1.0-nice — so this image
# uses system packages instead of Selkies' bundle. Two non-obvious extra
# packages were required beyond the obvious plugin sets:
#   - python3-gst-1.0: the actual PyGObject *overrides* for GStreamer
#     (Gst.Fraction(60, 1) etc). python3-gi + the gir1.2-gst-* typelibs alone
#     import fine but raise "Fraction() takes no arguments" — the convenience
#     constructors live in this separate package, which is what Selkies'
#     prebuilt bundle carries under its own gi/overrides (see xfce-webrtc's
#     Dockerfile comment) and what's easy to miss when going the system-apt
#     route instead.
#   - python3-setuptools: selkies_gstreamer's GPUtil dependency does
#     `from distutils import spawn`; distutils was removed from the stdlib in
#     Python 3.12+ (Ubuntu 26.04 ships 3.14). setuptools' bundled compat shim
#     is what makes `import distutils` work again.
# Because there's no prebuilt-binary dependency, this image is NOT pinned to
# linux/amd64 like xfce-webrtc — it should build multi-arch, though only
# amd64 has been verified.
#
# EXPERIMENTAL — verified: builds, systemd/gnome-session/Selkies all start and
# the elements/typelibs resolve (see README). Full browser WebRTC e2e needs a
# headed browser + real TURN, same verification caveat as xfce-webrtc.
FROM ubuntu:26.04

ENV DEBIAN_FRONTEND=noninteractive

# Selkies release to install (server wheel + web client only — see header for
# why the GStreamer C-library bundle itself is skipped). Keep in sync with the
# vendored portal client JS (whistler/portal/static/selkies-core.js).
ARG SELKIES_VERSION=1.6.2

# systemd + systemd-sysv: PID 1 and the sysvinit compatibility shims, required
# so gnome-session's systemd-unit-based startup sequence (gnome-session-x11-
# services.target etc.) has a real systemd --user to talk to. libpam-systemd
# is what registers the logind session when the desktop user logs in (see
# gnome-desktop.service's PAMName=login).
RUN apt-get update && apt-get install -y --no-install-recommends \
      systemd \
      systemd-sysv \
      libpam-systemd \
      dbus \
      dbus-x11 \
      dbus-user-session \
      # Traditional X11 GNOME session (GNOME Panel + Metacity) — GNOME Shell
      # itself can't run under Selkies' ximagesrc capture, see header.
      gnome-session-flashback \
      gnome-control-center \
      gnome-terminal \
      gnome-keyring \
      # Selkies always opens a second WebRTC connection for audio and captures
      # it with `pulsesrc`; without a running PulseAudio the audio track never
      # connects and the web client's "Waiting for stream" overlay hangs
      # forever (see xfce-webrtc). pulseaudio-utils provides `pactl`.
      pulseaudio \
      pulseaudio-utils \
      xvfb \
      x11-xserver-utils \
      # Provides `cvt`, which Selkies' dynamic-resize path shells out to for
      # generating xrandr modelines (see xfce-webrtc).
      xcvt \
      # Selkies shells out to `xsel` for browser<->X11 clipboard sync.
      xsel \
      python3 \
      python3-pip \
      python3-gi \
      # GStreamer's PyGObject overrides (Gst.Fraction(...) etc.) — a separate
      # package from python3-gi itself. See header.
      python3-gst-1.0 \
      # Restores `import distutils` for selkies_gstreamer's GPUtil dependency
      # on Python 3.12+. See header.
      python3-setuptools \
      # selkies' deps include evdev, which compiles a C extension against the
      # Python + kernel-uapi headers at install time.
      python3-dev \
      build-essential \
      ca-certificates \
      curl \
      # Optional in-container TURN relay for single-host testing without the
      # cluster's shared coturn (SELKIES_USE_INTERNAL_TURN; see session.sh).
      coturn \
      openssl \
      # System GStreamer (1.28 on 26.04) in place of Selkies' prebuilt bundle
      # — see header. gstreamer1.0-nice provides libnice for webrtcbin's ICE;
      # gstreamer1.0-ugly provides x264enc; gstreamer1.0-x provides ximagesrc.
      gir1.2-gst-plugins-base-1.0 \
      gir1.2-gst-plugins-bad-1.0 \
      gir1.2-gstreamer-1.0 \
      gstreamer1.0-plugins-base \
      gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad \
      gstreamer1.0-plugins-ugly \
      gstreamer1.0-nice \
      gstreamer1.0-pulseaudio \
      gstreamer1.0-x \
 && rm -rf /var/lib/apt/lists/*

# Mask units that fail or are pointless in a privileged container — same list
# as ../gnome-grd (udevd would try to manage the host's devices; getty/timers
# just add noise).
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

# Selkies server wheel (signaling + WebRTC process). Installed straight from
# the URL so pip sees the real PEP 427 filename (a renamed *.whl is rejected).
RUN PIP_BREAK_SYSTEM_PACKAGES=1 pip3 install --no-cache-dir \
      "https://github.com/selkies-project/selkies-gstreamer/releases/download/v${SELKIES_VERSION}/selkies_gstreamer-${SELKIES_VERSION}-py3-none-any.whl"

# selkies_gstreamer 1.6.2 calls the old `asyncio.get_event_loop()` idiom to
# create its main loop, relying on it implicitly creating one when none is
# running yet. Python 3.14 (Ubuntu 26.04's default python3) made that a hard
# RuntimeError ("There is no current event loop in thread 'MainThread'")
# instead of the older DeprecationWarning-and-create behavior — the same
# category of "pinned release meets a newer platform" issue as the
# python3-gst-1.0/GSTREAMER_PATH ones above, just on the Python side instead
# of the GStreamer side. Both call sites (__main__.py and signalling_web.py)
# unconditionally want a fresh loop at that point in startup anyway, so
# swapping in new_event_loop()+set_event_loop() is behavior-preserving.
RUN sed -i \
      's/loop = asyncio.get_event_loop()/loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)/' \
      /usr/local/lib/python3*/dist-packages/selkies_gstreamer/__main__.py \
      /usr/local/lib/python3*/dist-packages/selkies_gstreamer/signalling_web.py

# Selkies web/client bundle — same version-lock caveat as xfce-webrtc: this
# MUST match the selkies client JS the portal vendors.
RUN curl -fsSL -o /tmp/web.tar.gz \
      "https://github.com/selkies-project/selkies-gstreamer/releases/download/v${SELKIES_VERSION}/selkies-gstreamer-web_v${SELKIES_VERSION}.tar.gz" \
 && mkdir -p /opt/gst-web \
 && tar -xzf /tmp/web.tar.gz -C /opt/gst-web --strip-components=1 \
 && rm /tmp/web.tar.gz

# Same local client patches as xfce-webrtc (pixel-doubled toggle) — see that
# Dockerfile's comment for why these are vendored as full files.
COPY gst-web-overrides/app.js gst-web-overrides/index.html /opt/gst-web/

ENV SELKIES_PORT=8082 \
    DISPLAY=:0 \
    GST_DEBUG=*:1

# Systemd unit that runs the GNOME Flashback session as root with a full PAM
# (logind) session — see gnome-desktop.service for why root, not an
# unprivileged desktop user like ../gnome-grd's gnome-desktop.service.
COPY gnome-desktop.service /etc/systemd/system/gnome-desktop.service
RUN systemctl enable gnome-desktop.service

COPY session.sh /usr/local/bin/session.sh
RUN chmod +x /usr/local/bin/session.sh

EXPOSE 8082
# SIGRTMIN+3 is systemd's clean-shutdown signal; without it docker stop hangs.
STOPSIGNAL SIGRTMIN+3
ENTRYPOINT ["/lib/systemd/systemd"]
