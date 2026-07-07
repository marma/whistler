# Desktop images

Container images that provide a graphical desktop reachable by the Whistler
portal. Each subdirectory is one catalog entry (except the streamer sidecar,
see below); a `DesktopTemplate` references the built image and the portal
serves it through the **websockets** viewer:

- `viewer: websockets` (the only viewer) — browser ⇄ portal ⇄ in-pod **Selkies
  2.x** (pixelflux) server. The pod streams **H.264 over plain WebSockets**
  straight to the browser's decoder; the portal reverse-proxies the HTTP/WS
  stream to the pod. **No guacd, no coturn/TURN.** The template gives the
  `displayPort` the in-pod Selkies server listens on (default `8082`).

Where the Selkies server *runs* is the template's `streamer` field:

- `streamer: embedded` (default) — the image itself starts X + Selkies
  (`xfce-selkies2`, `gnome-selkies2`).
- `streamer: sidecar` — the operator injects a **streamer sidecar**
  ([`streamer-selkies2`](streamer-selkies2/), a native init sidecar) that owns
  Xvfb + PulseAudio + Selkies and shares the X/Pulse sockets over emptyDirs;
  the workload image is **display-unaware** (no Selkies inside — just a session
  entrypoint against the injected `DISPLAY`, e.g. [`xfce-plain`](xfce-plain/)).
  This is stage 1 of the guest-unaware-display direction: the streaming
  protocol becomes a property of the sidecar image, swappable per template
  without touching workload images. The pair also runs without any cluster as
  two plain docker containers — see
  [`streamer-selkies2/README.md`](streamer-selkies2/README.md).

Over time this grows into a catalog for different use-cases (minimal vs full DE,
CPU vs GPU, different toolchains). Add a new image by copying an existing
subdirectory. Then add the matching `DesktopTemplate` in
`charts/whistler/values.yaml` (`templates`). **Read
[design/creating_desktops.md](../design/creating_desktops.md) first** — stack
choice, assembly checklist, silent-failure catalog, and the verification
ladder for new images.

| Image | Streamer | Port | Notes |
|-------|----------|------|-------|
| [`xfce-selkies2`](xfce-selkies2/) | embedded | 8082 | XFCE over **Selkies 2.x** (pixelflux). H.264 over plain WebSockets — no coturn/TURN. Multi-arch (pixelflux ships amd64+arm64 wheels). Ubuntu 26.04. Runs with `--cap-drop=ALL` (verified). |
| [`gnome-selkies2`](gnome-selkies2/) | embedded | 8082 | **Real GNOME Shell** over **Selkies 2.x** (pixelflux). **Ubuntu 24.04 / GNOME 46** — the last gen with an X11-backend Shell *and* an unprivileged (no systemd-PID1) session, so **no `--privileged`**. Runtime-configurable user/UID/GID/sudo + home volume (`DESKTOP_USER`/`PUID`/`PGID`/`DESKTOP_SUDO`). Vendors libva 2.22 (24.04 ships 2.20). Firefox from Mozilla `.deb`. |
| [`xfce-plain`](xfce-plain/) | **sidecar** | 8082 (on the sidecar) | XFCE with **no Selkies inside** — display-unaware workload image paired with the `streamer-selkies2` sidecar via `streamer: sidecar`. Ubuntu 26.04. |
| [`gnome-plain`](gnome-plain/) | **sidecar** | 8082 (on the sidecar) | **Real GNOME Shell** (X11 backend — Wayland is architecturally incompatible with a display-owning sidecar), display-unaware. Ubuntu 24.04 / GNOME 46, unprivileged, PUID/PGID identity like `gnome-selkies2` — minus Selkies/Xvfb/Pulse *and* minus the vendored libva. Template **must set** `streamerEnv: {SELKIES_H264_STREAMING_MODE: "true"}`. |
| [`streamer-selkies2`](streamer-selkies2/) | *(is the sidecar)* | 8082 | Not a catalog entry. Xvfb + PulseAudio + **Selkies 2.x**, injected by the operator (`whistler.streamer.image`, per-template `streamerImage` override). Tracks `xfce-selkies2`'s Dockerfile/pin. |

> **History:** earlier spikes explored other display paths — XFCE/GNOME over
> **guacd/RDP** (`base-rdp`, `xfce-rdp`, `gnome-grd`) and over **Selkies 1.x
> WebRTC** with a coturn relay (`xfce-webrtc`, `gnome-flashback-webrtc`). All were
> removed when the project consolidated on Selkies 2.x (see the banner in
> [design/vdi.md](../design/vdi.md)); they remain recoverable from git history if
> an agentless VM-console path (KubeVirt QEMU framebuffer) is ever needed.

## Conventions

- **Display server self-starts** as the image entrypoint (the desktop pod spec
  overrides no command). The in-pod Selkies server listens on `displayPort`
  (8082 by convention).
- **Multi-arch**: prefer base images/packages with arm64 builds so desktops run
  natively on Apple-Silicon/arm64 clusters. Build multi-arch for a registry with
  `docker buildx build --platform linux/amd64,linux/arm64 -t <ref> --push .`.
- **Security boundary** is the per-session NetworkPolicy (only the portal can
  reach the pod), not credentials baked into the image. See
  [../design/vdi.md](../design/vdi.md).

## Build & use

Locally, `skaffold dev` builds these as additional artifacts and points the
sample `DesktopTemplate`s at the freshly-built images (see `skaffold.yaml`). To
build one by hand:

```bash
docker build -t whistler-desktop-xfce-selkies2:dev desktops/xfce-selkies2

# Multi-arch, pushed to a registry:
docker buildx build --platform linux/amd64,linux/arm64 \
  -t <registry>/whistler-desktop-gnome-selkies2:<tag> --push desktops/gnome-selkies2
```
