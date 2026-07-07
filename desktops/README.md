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

> **History:** earlier rounds explored other display paths — XFCE/GNOME over
> **guacd/RDP** (`base-rdp`, `xfce-rdp`, `gnome-grd`), **Selkies 1.x WebRTC**
> with a coturn relay (`xfce-webrtc`, `gnome-flashback-webrtc`), and
> **embedded Selkies 2.x** images that bundled the display plane in-image
> (`xfce-selkies2`, `gnome-selkies2`). All removed as the project consolidated
> on the sidecar (see [design/vdi.md](../design/vdi.md)); everything is
> recoverable from git history if e.g. an agentless VM-console path (KubeVirt
> QEMU framebuffer) is ever needed.

## Conventions

- **Workload images self-start their session** as the image entrypoint (the
  pod spec overrides no command) and include a short wait-for-X +
  fail-loudly guard for use outside Kubernetes (in-cluster the sidecar's
  startupProbe already gates them).
- **Multi-arch**: prefer base images/packages with arm64 builds so desktops
  run natively on Apple-Silicon/arm64 clusters. Build multi-arch for a
  registry with `docker buildx build --platform linux/amd64,linux/arm64 -t
  <ref> --push .`.
- **Security boundary** is the per-session NetworkPolicy (only the portal can
  reach the pod), not credentials baked into the image. See
  [../design/vdi.md](../design/vdi.md).
- Read **[design/creating_desktops.md](../design/creating_desktops.md)**
  before adding an image or touching the streamer.

## Build & use

`skaffold dev` builds all images and points the sample templates + the
operator's streamer image at the fresh builds. Without a cluster:

```bash
make desktop-sidecar-local        # streamer + XFCE pair, http://localhost:8082/
make desktop-gnome-sidecar-local  # streamer + GNOME Shell pair
make desktop-sidecar-local-down   # stop either pair, remove shared volumes
```

(backed by [`compose-sidecar.yaml`](compose-sidecar.yaml); manual `docker run`
spelling in [`streamer-selkies2/README.md`](streamer-selkies2/README.md)).
