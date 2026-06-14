# Desktop images

Container images that provide a graphical desktop reachable by the Whistler
portal (browser → portal → guacd → this image). Each subdirectory is one catalog
entry; a `DesktopTemplate` references the built image and tells guacd how to
reach it (`protocol`, `displayPort`, `connectionParams`).

Over time this grows into a catalog for different use-cases (minimal vs full DE,
CPU vs GPU, different toolchains). Add a new image by copying an existing
subdirectory and adjusting the Dockerfile + the matching `DesktopTemplate` in
`charts/whistler/values.yaml` (`desktopTemplates`).

| Image | Protocol | Port | Creds | Notes |
|-------|----------|------|-------|-------|
| [`xfce-rdp`](xfce-rdp/) | rdp | 3389 | `abc` / `abc` | XFCE over xrdp. Multi-arch (amd64 + arm64). |

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
```
