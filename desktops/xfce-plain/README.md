# xfce-plain — display-unaware XFCE workload image

XFCE and nothing display-protocol-specific: no X server, no PulseAudio daemon,
no Selkies. Runs the desktop session against a display somebody else owns —
in-cluster that is the [`../streamer-selkies2`](../streamer-selkies2/) sidecar
(injected into every desktop pod), which shares `/tmp/.X11-unix` and
`/tmp/pulse` and injects `DISPLAY` / `PULSE_SERVER`.

This is the proof-of-model for the guest-unaware-display direction: catalog
images are just "DE-or-app + tools + a session entrypoint" — copy this
directory and swap the DE.

The entrypoint waits briefly for the display (a no-op in-cluster, where the
sidecar's startupProbe already gates this container) and then execs
`startxfce4` as the desktop user (`DESKTOP_USER`, default `abc`) in the
foreground, so the container tracks the session and a crashed DE restarts
against the still-running display.

The desktop shows **Ubuntu's default wallpaper** rather than the stock Xfce
mouse, and **Ubuntu's Yaru icons** rather than Adwaita: `ubuntu-wallpapers` and
`yaru-theme-icon` (nothing in `xfce4` pulls either in) plus
[`xfce4-desktop.xml`](xfce4-desktop.xml) and [`xsettings.xml`](xsettings.xml)
installed to `/etc/xdg/xfce4/xfconf/`, which xfconf reads as read-only defaults
that `~/.config/xfce4/xfconf/…` shadows — so a user's own choice wins and
persists in the mounted home. The backdrop property path embeds the RandR
output name, always `screen` on the streamer's Xvfb; the icon theme reaches the
session through xfsettingsd's XSETTINGS, so nothing else needs telling. Only
the icon set is ours — the GTK widget theme stays Xfce's default, and
`adwaita-icon-theme` (an `xfce4` dependency) remains as the fallback for icons
Yaru doesn't define. Keep both files in sync with the VM copies in
[`../vm-xfce-selkies/guest/etc/xdg/xfce4/xfconf/xfce-perchannel-xml/`](../vm-xfce-selkies/guest/etc/xdg/xfce4/xfconf/xfce-perchannel-xml/).

For running the pair without a cluster, see
[`../streamer-selkies2/README.md`](../streamer-selkies2/README.md).
