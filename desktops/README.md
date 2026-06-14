# Desktop images

Container images that provide a graphical desktop reachable by the Whistler
portal (browser → portal → guacd → this image). Each subdirectory is one catalog
entry; a `DesktopTemplate` references the built image and tells guacd how to
reach it (`protocol`, `displayPort`, `connectionParams`).

Over time this grows into a catalog for different use-cases (minimal vs full DE,
CPU vs GPU, different toolchains). Add a new image either by copying an existing
subdirectory, or — for an RDP desktop — by building `FROM` [`base-rdp`](base-rdp/)
(see below). Then add the matching `DesktopTemplate` in
`charts/whistler/values.yaml` (`desktopTemplates`).

| Image | Protocol | Port | Creds | Notes |
|-------|----------|------|-------|-------|
| [`base-rdp`](base-rdp/) | rdp | 3389 | `abc` / `abc` | **Base layer**, not a catalog entry. xrdp/xorgxrdp plumbing on Ubuntu 26.04, no DE. Build X11 desktop images `FROM` it. Multi-arch (amd64 + arm64). |
| [`xfce-rdp`](xfce-rdp/) | rdp | 3389 | `abc` / `abc` | XFCE over xrdp (Ubuntu 22.04). Multi-arch (amd64 + arm64). |
| [`gnome-grd`](gnome-grd/) | rdp | 3389 | `abc` / `abc` | GNOME over **gnome-remote-desktop** (Wayland-native, headless gnome-shell). Does *not* use xrdp/base-rdp. Ubuntu 26.04. RDP handshake verified; full guacd pixel path not yet. |

### Building an RDP desktop on `base-rdp`

`base-rdp` carries everything common to an RDP desktop — the xrdp listener,
xorgxrdp Xorg backend, the lossless-bitmap `xrdp.ini`, the well-known `abc`/`abc`
login, and a `startwm.sh` that sets up the session D-Bus + `XDG_RUNTIME_DIR` —
but ships **no desktop environment**. A DE image just installs its DE and tells
xrdp how to start it via `WHISTLER_SESSION_CMD`:

```dockerfile
FROM whistler-desktop-base-rdp:dev
RUN apt-get update && apt-get install -y --no-install-recommends \
      xfce4 xfce4-terminal \
 && rm -rf /var/lib/apt/lists/*
ENV WHISTLER_SESSION_CMD=startxfce4
```

Running `base-rdp` directly is a misconfiguration — with no `WHISTLER_SESSION_CMD`
its `startwm.sh` exits loudly rather than dropping you into a black screen.

`base-rdp` is the right base for **X11-native** DEs (XFCE, MATE, …). It is *not*
suitable for GNOME: GNOME defaults to Wayland and is dropping its GNOME-on-Xorg
session, while xorgxrdp is an X11 backend. So [`gnome-grd`](gnome-grd/) takes the
Wayland-native route instead — a headless `gnome-shell` whose session is served
over RDP by `gnome-remote-desktop`, with no xrdp in the picture. It still exposes
RDP on 3389, so guacd and the `DesktopTemplate` contract are unchanged.

## Conventions

- **Display server self-starts** as the image entrypoint (the desktop pod spec
  overrides no command). RDP images listen on 3389; VNC images on 5900/5901.
- **Fixed, well-known credentials** baked into the image, mirrored in the
  template's `connectionParams`. These are not a security boundary — the
  per-session NetworkPolicy is (only guacd can reach the pod). See
  [../design/vdi.md](../design/vdi.md).
- **Multi-arch**: prefer base images/packages with arm64 builds so desktops run
  natively on Apple-Silicon/arm64 clusters. Build multi-arch for a registry with
  `docker buildx build --platform linux/amd64,linux/arm64 -t <ref> --push .`.

## Build & use

Locally, `skaffold dev` builds these as additional artifacts and points the
sample `DesktopTemplate` at the freshly-built image (see `skaffold.yaml`). To
build one by hand:

```bash
docker build -t whistler-desktop-xfce-rdp:dev desktops/xfce-rdp

# The base layer (tag it to match the FROM in any image building on it):
docker build -t whistler-desktop-base-rdp:dev desktops/base-rdp

# Multi-arch base, pushed to a registry:
docker buildx build --platform linux/amd64,linux/arm64 \
  -t <registry>/whistler-desktop-base-rdp:<tag> --push desktops/base-rdp
```
