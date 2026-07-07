# xfce-plain — display-unaware XFCE workload image

XFCE and nothing display-protocol-specific: no X server, no PulseAudio daemon,
no Selkies. Runs the desktop session against a display somebody else owns —
in-cluster that is the [`../streamer-selkies2`](../streamer-selkies2/) sidecar
(injected into every desktop pod), which shares `/tmp/.X11-unix` and
`/tmp/pulse` and injects `DISPLAY` / `PULSE_SERVER`.

This is the proof-of-model for the guest-unaware-display direction: catalog
images are just "DE-or-app + tools + a session entrypoint" — copy this
directory and swap the DE. (Its embedded ancestor `xfce-selkies2`, which
bundled X + Selkies in-image, is in git history.)

The entrypoint waits briefly for the display (a no-op in-cluster, where the
sidecar's startupProbe already gates this container) and then execs
`startxfce4` as the desktop user (`DESKTOP_USER`, default `abc`) in the
foreground, so the container tracks the session and a crashed DE restarts
against the still-running display.

For running the pair without a cluster, see
[`../streamer-selkies2/README.md`](../streamer-selkies2/README.md).
