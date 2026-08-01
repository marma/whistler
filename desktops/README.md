# Desktop images

Container images behind Whistler's desktop sessions. The display model is the
**streamer sidecar**: every desktop pod pairs one *display-unaware workload
image* (a catalog entry below) with the
[`streamer-selkies2`](streamer-selkies2/) sidecar, which owns Xvfb +
PulseAudio + **Selkies 2.x** (pixelflux) and streams **H.264 over plain
WebSockets** to the browser (the portal's **websockets** viewer reverse-proxies
it; **no guacd, no coturn/TURN**).

The workload image's entire display contract:

- `DISPLAY` / `PULSE_SERVER` env (injected by the operator),
- the shared `/tmp/.X11-unix` and `/tmp/pulse` sockets (pod emptyDirs),
- an entrypoint that starts a DE or app session in the foreground.

Nothing streaming-related lives in workload images, so the streaming protocol
is a property of the sidecar image — swappable per template (`streamerImage`)
without touching the catalog. Workload-dependent streaming knobs go in the
template's `streamerEnv` (e.g. GNOME's mandatory
`SELKIES_H264_STREAMING_MODE: "true"`). The template's `displayPort` (default
`8082`) is where the sidecar's Selkies server listens.

| Image | Port | Notes |
|-------|------|-------|
| [`xfce-plain`](xfce-plain/) | 8082 (sidecar) | XFCE. Ubuntu 26.04. The minimal model for new workload images — copy it and swap the DE. |
| [`gnome-plain`](gnome-plain/) | 8082 (sidecar) | **Real GNOME Shell** (X11 backend — Wayland is architecturally incompatible with a display-owning sidecar). Ubuntu 24.04 / GNOME 46, unprivileged, runtime `PUID`/`PGID` identity. Template **must set** `streamerEnv: {SELKIES_H264_STREAMING_MODE: "true"}`. |
| [`streamer-selkies2`](streamer-selkies2/) | 8082 | *Not a catalog entry* — the sidecar itself, injected by the operator (`whistler.streamer.image`, per-template `streamerImage` override). Single home of the Selkies/pixelflux stack. |
| [`vm-xfce-selkies`](vm-xfce-selkies/) | 8082 (in-guest) | **KubeVirt containerDisk, not an OCI workload**: XFCE + the same Selkies stack baked *into the guest* (the sidecar can't cross the VM boundary). For `runtime: vm` + `viewer: websockets` templates. Built by a qemu/KVM bake (`make vm-desktop-image`), not skaffold. |

## VM desktops

The sidecar model stops at the VM boundary (the shared X/Pulse sockets can't
cross it), so `runtime: vm` desktops carry the streamer **inside the guest**:
[`vm-xfce-selkies`](vm-xfce-selkies/) bakes the DE and the identical Selkies
stack (extracted from the streamer image's build — one `SELKIES_COMMIT`) into
a bootable disk; per-session cloud-init starts the user's session unit. The
portal path is unchanged — `viewer: websockets` proxies the per-session
Service, which reaches the guest through the launcher pod's masquerade. The
agentless `viewer: vnc` (noVNC over the KubeVirt VNC subresource) remains the
rescue path / default for unbaked VM images.

## Client keyboard notes

- **macOS clients**: the Selkies web client remaps Cmd+key chords to
  Ctrl+key in the session, patched at image build time
  (`streamer-selkies2/mac-cmd-chords.patch`) to survive repeated chords per
  Cmd hold **and** to give plain Cmd-C/Cmd-V native Mac semantics: they are
  sent as XF86Copy/XF86Paste taps, which the in-session
  [`whistler-copy-agent`](streamer-selkies2/whistler-copy-agent) (launched by
  every streamer flavor) re-injects as the chord the **focused** window
  expects — Ctrl+C/V in GUI apps, Ctrl+Shift+C/V in terminals (by WM_CLASS).
  So Cmd-C copies to the real CLIPBOARD everywhere, Cmd-V pastes it, physical
  Ctrl-C still means SIGINT, and PRIMARY/middle-click is untouched. No single
  X chord could do this (Ctrl+C is SIGINT in a terminal; VTE's Shift+Insert
  pastes PRIMARY, not CLIPBOARD — measured). Cmd-Shift-C/V still arrive
  verbatim as Ctrl+Shift+C/V.
- **Non-US client layouts**: the session's X server starts with a US keymap,
  so characters outside it (å/ä/ö, …) are injected via a temporary
  keymap-rewrite fallback — it works, but each such character stalls ~1s while
  the compositor re-parses the keymap. Fix: add your layout inside the session
  (GNOME Settings → Keyboard → Input Sources, or `setxkbmap se`) — with the
  keysym present in the keymap, injection takes the direct XTEST path
  (measured: ~1s → 0 ms). The choice persists in dconf on the home volume.
- **AltGr-style Mac chords** (`|`, `@`, `{`, … via Option — e.g. Option+7 for
  `|` on a Swedish Mac layout): the client remaps the physical Option key to
  the X11 keysym `Mode_switch`, which — unlike layout characters — is a
  pre-XKB legacy keysym that **no modern keymap ever binds** (layouts use
  `ISO_Level3_Shift` instead), so it hit the same 1s keymap-rewrite stall on
  every guest regardless of layout, and adding a layout couldn't fix it. Selkies'
  own fallback chain made it worse: its injector tries `pynput` first, which
  throws when the keysym is unmapped, then falls back to `xdotool` wrapped in
  a **hardcoded 1-second timeout** — a slow remap under a busy compositor can
  graze that ceiling and abandon the temporary keycode mid-flight, which is
  what produced the stray character on rapid repeats. Fixed for good in
  `whistler-copy-agent`: it pre-binds `Mode_switch` (and, defensively,
  `ISO_Level3_Shift`) to a spare keycode at startup, so every guest has both
  present before any client ever needs them — no session-side action required.

## Conventions

- **Workload images self-start their session** as the image entrypoint (the
  pod spec overrides no command) and include a short wait-for-X +
  fail-loudly guard for use outside Kubernetes (in-cluster the sidecar's
  startupProbe already gates them).
- **Security boundary** is the per-session NetworkPolicy (only the portal can
  reach the pod), not credentials baked into the image. See
  [../design/vdi.md](../design/vdi.md).
- Read **[design/creating_desktops.md](../design/creating_desktops.md)**
  before adding an image or touching the streamer.

## Build & use

`skaffold dev` builds all images and points the sample templates + the
operator's streamer image at the fresh builds; launch a desktop from the
portal at `http://localhost:30080/?user=<name>` and the viewer opens at
`/desktop/<session>/` (the portal reverse-proxies the sidecar's Selkies
server — HTTP and the H.264 WebSocket both). Without a cluster:

```bash
make desktop-sidecar-local        # streamer + XFCE pair, http://localhost:8082/
make desktop-gnome-sidecar-local  # streamer + GNOME Shell pair
make desktop-sidecar-local-down   # stop either pair, remove shared volumes
```

(backed by [`compose-sidecar.yaml`](compose-sidecar.yaml); manual `docker run`
spelling in [`streamer-selkies2/README.md`](streamer-selkies2/README.md)).
