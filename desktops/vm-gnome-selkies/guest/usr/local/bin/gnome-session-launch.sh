#!/usr/bin/env bash
# The GNOME session itself. Runs under `dbus-run-session` (so gnome-shell and
# the settings daemons below share one session bus) as the unprivileged desktop
# user, started by whistler-desktop-session. Carried over verbatim from the
# retired gnome-selkies2 (and shared in spirit with ../../gnome-plain) — the
# session shape is identical whether the display comes from a sidecar's Xvfb or
# this VM's in-guest streamer.
#
# We deliberately do NOT use `gnome-session`. On Ubuntu 24.04 its gnome.session
# RequiredComponents include gsd-power (needs logind + upower), gsd-usb-
# protection (SIGSEGVs headless), and a pulseaudio autostart that collides with
# the streamer's already-running PulseAudio. gnome-session sends the whole
# session to its "Oh no, something has gone wrong" failed screen the moment any
# *required* component crash-loops — so those three take the desktop down even
# though gnome-shell itself is perfectly happy on llvmpipe/Xvfb. Launching
# gnome-shell + a curated daemon set directly sidesteps that entirely and keeps
# this session UNPRIVILEGED — the whole reason design/creating_desktops.md §8
# picks GNOME 46/24.04. The cost is only the logind-tied features (lock/suspend/
# seat switching), which are meaningless for a single-user streamed desktop.
set -e

# gnome-settings-daemon plugins that run cleanly without logind/upower/hardware.
# These provide the things you'd actually notice missing:
#   xsettings  - GTK theme / fonts / Xft / cursor / scaling (the big one)
#   keyboard   - XKB layout + repeat
#   media-keys - volume/brightness/screenshot keybindings
#   sound      - system sounds + volume<->PulseAudio glue
#   a11y-settings, datetime, housekeeping (trash/temp cleanup), color
# Excluded on purpose (verified to crash or be pointless headless): power,
# usb-protection, rfkill, smartcard, wacom, print-notifications,
# screensaver-proxy, sharing, print. Add one back only if you confirm it
# survives standalone here (many need logind or real hardware).
for plugin in xsettings keyboard media-keys sound a11y-settings datetime housekeeping color; do
  /usr/libexec/gsd-"${plugin}" &
done

# KNOWN LIMITATION (not fixed here): gnome-shell 46's Activities/app-switcher
# overview backdrop is created at the resolution present when the shell starts
# and, on X11, only ever SHRINKS with the monitor — it never grows. A client that
# drives the desktop ABOVE that size (HiDPI + a browser window larger than
# ~1920x1080) leaves the overview's dark backdrop / app-grid confined to the old
# smaller rectangle in the top-left, while the panel and windows use the full
# size. The desktop, windows and app launching all work; only the overview's
# backdrop is wrong. This is inherent to dynamic resolution changes under mutter
# on Xvfb (verified: forcing scaling-factor=1, pre-growing to the ceiling, and
# restarting the shell all fail to make the backdrop grow). The only full fix is
# a fixed resolution (no resize), which trades away matching the browser window —
# we keep dynamic resize by choice. See this image's README.

# gnome-shell IS the session: run it in the foreground so the container/session
# lifecycle tracks it, and dbus-run-session tears the bus down when it exits.
# --x11 forces mutter 46's X11 backend (still present in 46, gone in 50) so the
# real Shell composits the Xvfb display that pixelflux captures — this is what
# makes it the *actual* GNOME Shell over the H.264 path, not a Flashback/Xrdp
# stand-in.
exec gnome-shell --x11
