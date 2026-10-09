# Desktop images

Whistler's desktops are **VMs** (design/container_workloads.md): a container
session is a throwaway workspace reached through the portal's web terminal,
and a desktop is a KubeVirt guest with the streamer baked in.

| Image | Port | Notes |
|-------|------|-------|
| [`vm-gnome-selkies`](vm-gnome-selkies/) | 8082 (in-guest) | **KubeVirt containerDisk**: the real GNOME Shell 46 (X11 backend, Ubuntu 24.04) + Selkies 2.0, which streams over plain WebSockets to the browser. For `runtime: vm` + `viewer: websockets` templates. Built by a qemu/KVM bake (`make vm-gnome-desktop-image`, `CUDA=1` for the GPU variant), not skaffold. |

The portal's **websockets** viewer reverse-proxies the guest's Selkies server —
HTTP and the stream WebSocket both — through the per-session Service, which
reaches the guest through the launcher pod's masquerade. There is no guacd and
no coturn/TURN. The agentless `viewer: vnc` (noVNC over the KubeVirt VNC
subresource) shows the VM's real screen and is the rescue path, or the default
for images without a baked streamer.

The SSH-only development VM is not a desktop and lives in
[`../images/devbase`](../images/devbase/).

## History

Earlier desktop images are in git history: the container-desktop pair
(`streamer-selkies2` sidecar + display-unaware `xfce-plain` / `gnome-plain`
workloads) and `vm-xfce-selkies`. The sidecar mechanism, and the case that
would bring GUI containers back (single *apps*, not desktops), is written up
in [design/container_workloads.md](../design/container_workloads.md).

## Conventions

- **Security boundary** is the per-session NetworkPolicy (only the portal can
  reach the guest's streamer), not credentials baked into the image. See
  [../design/vdi.md](../design/vdi.md).
- Read **[design/creating_desktops.md](../design/creating_desktops.md)**
  before adding an image or touching the streamer.
