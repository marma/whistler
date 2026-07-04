# VDI: desktop-pod and KubeVirt-VM backends

Status: **rounds 1–3 implemented** (round 3 partially — see below). Round 1 = provisioning
(desktop pods + KubeVirt VMs). Round 2 = the **guacd** display path (RDP desktop + shared
guacd + a Python WebSocket portal) — see [Round 2: the display path](#round-2-the-display-path).
Round 3 = a parallel **WebRTC** display path (Selkies + coturn) for hardware-class H.264 to
the browser — see [Round 3: the WebRTC display path](#round-3-the-webrtc-display-path). The
KubeVirt VM path remains **unverified end-to-end** (no KubeVirt in any cluster we can test
against yet); the WebRTC media path is likewise **verified only manually** (a headed browser +
real TURN can't run in k3d/CI).

## Why

Whistler began as an SSH-only broker: it provisions exactly one kind of resource — an
interactive pod reached over `standard SSH` — created by the operator's
`reconcile_fn → ensure_pod() → _build_pod_spec()`. This work extends it to broker
**remote-desktop sessions** as well, for users who want a graphical environment (or a full
VM) rather than a shell. The longer-term, web-first vision is in
[../mother-design.md](../mother-design.md); this document describes what was actually built
and the decisions behind it.

Two new resource types, both on demand:

1. a **lightweight desktop pod** — a container image whose entrypoint runs a display server
   (VNC/RDP/SPICE) on a known port, and
2. a **KubeVirt virtual machine** — hardware-isolated, for users who need their own
   kernel / privileged or nested workloads where a pod is unsafe.

This is split into two rounds. **Round 1 (this doc): provisioning + lifecycle only** — the
operator can create a desktop/VM, drive it to readiness, expose it on a stable in-cluster
address, and tear it down. **Round 2 (later): the display path** — Apache Guacamole
(`guacd` + a thin WebSocket tunnel + a web portal) so a user actually sees the pixels.

## Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Operator topology | **Extend** Whistler's existing operator; one image, one operator | Reuses the namespace/PVC/egress/lifecycle spine. "Mother" (a second operator) only becomes relevant if this grows unwieldy — see [../mother-design.md](../mother-design.md). |
| CRD model | **New CRDs** `DesktopTemplate`/`DesktopSession` alongside the SSH `WhistlerTemplate`/`WhistlerInstance` | Desktops/VMs are a distinct concern; the SSH CRs stay untouched. Shared plumbing is reused at the `KubeConfigManager` level, not by overloading one CR. |
| Backend selection | `backend: pod \| vm` field on `DesktopTemplate`; the controller is backend-agnostic | One session model, two implementations. |
| Readiness ownership | The **operator** owns the `DesktopSession` status phase machine | With Guacamole deferred there is no SSH server in the loop to watch readiness (the SSH flow relies on `WhistlerSession._watch_pod_ready`). |
| KubeVirt absence | Every VMI read/create is **guarded**; no VMI watch is ever registered | KubeVirt CRDs are absent in dev/CI clusters; the operator must start and run cleanly without them. |

## Architecture

```
              kubectl / (future) portal
                       │  create DesktopSession CR
                       ▼
        ┌──────────────────────────────┐
        │  Whistler operator (KOPF)     │
        │                               │
        │  reconcile_desktop_fn  ───────┼──► ensure_desktop()
        │    (create/update/resume)     │      ├─ pod:  desktop Pod
        │                               │      └─ vm:   KubeVirt VirtualMachine
        │  desktop_phase_timer  ────────┼──► probe child → status.phase
        │    (every 10s)                │      Provisioning→Booting→Ready/Failed
        │                               │
        │  delete_desktop_fn  ──────────┼──► delete Service + pod/VM
        └──────────────────────────────┘
                       │ owns
                       ▼
   per-user namespace (whistler-user-<user>)
     ├─ desktop Pod  OR  KubeVirt VM       (labels: app=whistler-desktop, session=<name>)
     ├─ per-session ClusterIP Service       (selector: session=<name>, port=displayPort)
     └─ per-user home PVC (whistler-data-<user>)   ← shared with the SSH world
```

No database — all state lives in CRs, Secrets, and ConfigMaps, exactly as the SSH side. The
per-session Service is where round 2's `guacd` will connect.

## CRDs

Single source of truth: [../charts/whistler/crds/crds.yaml](../charts/whistler/crds/crds.yaml)
(Helm installs it automatically from the chart's `crds/` dir; the non-Helm paths
`kubectl apply -f` the same file). Group `whistler.martinmalmsten.net/v1`.

### `DesktopTemplate` (`dt`) — admin/user-curated catalog entry

| Field | Type | Notes |
|---|---|---|
| `user` | string | `system` for chart-curated templates, else the owning user |
| `image` | string | desktop container image (pod) or container-disk image (vm) |
| `backend` | `pod` \| `vm` | selects the implementation |
| `description` | string | |
| `resources` | `{cpu, memory, gpu}` | gpu maps to `nvidia.com/gpu` (pod) or a passthrough device (vm) |
| `nodeSelector` | map | |
| `volumes` | map | extra named volumes, same model as `WhistlerTemplate` |
| `personalMountPath` | string | where the home PVC mounts (default `/userdata`) |
| `persistence` | `ephemeral` \| `persistent` \| `preemptible` | `preemptible` ⇒ `whistler-preemptible` priority class |
| `displayPort` | integer | where the image's display server listens (default 5901) |
| `protocol` | `vnc` \| `rdp` | display protocol guacd uses to connect |
| `connectionParams` | map | string params forwarded verbatim to guacd's `connect` (creds, `security`, `ignore-cert`, `resize-method`, etc.) |
| `instancetype` | string | VM-only; KubeVirt instancetype (supplies cpu/memory) |
| `fuse` | boolean | grant the container `/dev/fuse` (see [Privileged containers](#privileged-containers-fuse-and-systemd) below) |
| `privileged` | boolean | run the container with `securityContext.privileged=true`; required for images that run systemd as PID 1 (see below) |
| `runtimeClassName` | string | Kubernetes RuntimeClass for the pod; set to `kata` (Kata Containers) to restore VM-level host isolation when `privileged: true` is set |

### `DesktopSession` (`ds`) — one live session (status subresource)

- `spec`: `templateRef`, `user`.
- `status` (operator-owned): `phase`, `backend`, `podName`, `vmiName`, `address`, `displayPort`.

CR naming follows the SSH convention: the CR is `{user}-{session}`, and the child pod/VM and
Service all share that name.

## Backend abstraction (`whistler/config.py`)

All cluster state goes through `KubeConfigManager`. The desktop additions mirror the SSH
side's structure — **pure manifest builders** (no API calls, unit-testable) plus
`ensure_*` methods that do the API work:

- `_build_desktop_pod_spec(...)` — like `_build_pod_spec`, but: **no entrypoint override**
  (the desktop image self-starts its display server, unlike the SSH pod's `sleep 3600`),
  exposes the display port as a named container port, labels `app=whistler-desktop` +
  `session=<name>`, mounts the per-user home PVC.
- `_build_vm_spec(...)` — a KubeVirt `VirtualMachine` (`kubevirt.io/v1`, `running: true`):
  ephemeral **container-disk root** from `image` + **persistent home** from the per-user PVC,
  pod network (`masquerade`), GPU passthrough only when `resources.gpu` is set, and
  `instancetype` (mutually exclusive with inline `domain.cpu`/`domain.resources`). The VMI
  launcher pod inherits the `session` label so the Service selects it.
- `_build_session_service(...)` — per-session ClusterIP Service selecting `session=<name>`
  on the display port. This is the stable in-cluster endpoint round 2's tunnel will use.
- `ensure_desktop(user, session)` — resolves the template (user ns → system ns fallback),
  ensures namespace + home PVC (reusing `_ensure_user_namespace` / `_ensure_pvc` /
  `_build_egress_rules` unchanged), dispatches on `backend`, then ensures the Service.
- CR-management mirrors of the SSH methods: `get_user_desktop_templates`,
  `get_user_desktop_sessions`, `add_desktop_session`, `delete_desktop_session`.

The home PVC (`whistler-data-<user>`) is **shared** with the SSH instances, so a user's
files persist across both worlds.

## Lifecycle & phase machine (`whistler/operator.py`)

The operator owns three handlers on `desktopsessions`:

- **`reconcile_desktop_fn`** (`create`/`update`/`resume`) — calls `ensure_desktop`, sets
  `status.phase = Provisioning` (or `Failed` + `TemporaryError` to retry).
- **`desktop_phase_timer`** (`@kopf.on.timer`, every 10s) — probes the child and patches
  `status.{phase, backend, podName|vmiName, address, displayPort}`:
  - pod: `read_namespaced_pod` → 404 ⇒ `Provisioning`; `Running` + all containers ready ⇒
    `Ready` (address = `<name>.<ns>.svc.cluster.local`); `Failed` ⇒ `Failed`; else `Booting`.
  - vm: `get` the VMI (guarded) → `status.phase == Running` ⇒ `Ready`; a 404 (VMI not up
    **or KubeVirt absent**) ⇒ `Booting`.
- **`delete_desktop_fn`** — deletes the Service, then the pod or VM (by `status.backend`),
  each 404-tolerant. ownerReferences also GC these; the explicit deletes are for promptness.

**Why a timer, not pod/VMI event watchers:** a VMI event source cannot be registered when
the `virtualmachineinstances` CRD is absent — kopf would fail at startup. A timer reads on
demand and can guard the VMI read, so the operator works uniformly for both backends and
starts cleanly without KubeVirt.

## RBAC & chart

- Operator ClusterRole ([../charts/whistler/templates/rbac.yaml](../charts/whistler/templates/rbac.yaml))
  gains `desktopsessions`/`desktoptemplates`/`desktopsessions/status` and a
  `kubevirt.io` rule (`virtualmachines`, `virtualmachineinstances`) — harmless when the CRDs
  are absent. `services` were already granted.
- **No new operator mounts**: `ensure_desktop` consumes exactly the already-mounted
  `users.yaml` (securityContext via `get_user`) and `networkpolicy.yaml` (egress).
- Optional admin catalog: `.Values.desktopTemplates` →
  [../charts/whistler/templates/desktop-templates.yaml](../charts/whistler/templates/desktop-templates.yaml)
  emits `DesktopTemplate` CRs with `user: system`.

## Testing

- **Unit** (`tests/unit/`, no cluster): pure-builder assertions —
  `test_desktop_pod_spec.py`, `test_vm_spec.py`, `test_service_spec.py`, and phase-mapping in
  `test_operator_helpers.py`. The VM path is verified **only** here.
- **Integration** (Tier C1, `tests/integration/test_desktop.py`): creates a `backend: pod`
  template + session against a real cluster and asserts the operator drives `phase` to
  `Ready` with a pod, a ClusterIP Service, and populated endpoints. VM backend is excluded
  (KubeVirt not installable in k3d/CI) and documented as unverified e2e.

## Security notes (inherited + new)

- Per-user namespace with deny-all ingress **except** a guacd carve-out (round 2,
  `_build_ingress_rules`) and locked-down egress (the existing `_build_egress_rules`
  complement-CIDR math).
- `automountServiceAccountToken: false` on desktop pods; optional `securityContext` from the
  user record (users absent from `users.yaml` get none — same as SSH pods).
- Ephemeral root + persistent home PVC limits the data-at-rest surface to one PVC per user.
- VM backend is the answer to "I need nested containers / privileged workloads" — the
  hypervisor contains the blast radius. Do not try to make nested Docker safe in a pod.

### Privileged containers: FUSE and systemd

Two distinct reasons a desktop pod may need `privileged: true`:

**1. FUSE (`fuse: true`)** — some desktop images mount a FUSE filesystem at runtime. In
particular [`gnome-grd`](../desktops/gnome-grd/) (GNOME via `gnome-remote-desktop`): grd's RDP
clipboard channel mounts a FUSE fs and its daemon **aborts** (`g_error`) the moment a client
connects if `/dev/fuse` is absent. The `fuse: true` field on a `DesktopTemplate` grants it.

The least-privilege alternative for FUSE is a **FUSE device plugin** — a DaemonSet (e.g.
[`nextflow-io/k8s-fuse-plugin`](https://github.com/nextflow-io/k8s-fuse-plugin)) that advertises
a `github.com/fuse` resource so the pod gets `/dev/fuse` without elevated privileges. Switching is
a change localized to the `fuse` block in `_build_desktop_pod_spec` (request the resource instead
of setting `privileged`) plus installing the plugin.

**2. systemd as PID 1 (`privileged: true`)** — modern Ubuntu GNOME (26.04 / GNOME 50) is deeply
integrated with systemd. D-Bus service files use `SystemdService=` for activation, user session
registration (`user@UID.service`, `user-runtime-dir@UID.service`) goes through systemd, and logind
session registration requires working user units. Running these components without systemd as PID 1
produces a cascade of failures (`NoSuchUser` from logind, D-Bus-activated apps exit 8, etc.) that
cannot be patched around one by one — it is the intended design of the distro.

The [`gnome-grd`](../desktops/gnome-grd/) image therefore **runs systemd as PID 1**
(`ENTRYPOINT ["/lib/systemd/systemd"]`, `STOPSIGNAL SIGRTMIN+3`). A systemd unit
`gnome-desktop.service` with `PAMName=login` runs the session as the desktop user, giving it a
proper logind session. This requires a privileged container. Set `privileged: true` on the
`DesktopTemplate`; `fuse: true` (also set for gnome-grd) independently adds `/dev/fuse` — both
merge into the same `securityContext.privileged=true` on the pod.

#### Kata Containers: restoring isolation without full VM images

Running a privileged pod means the container can interact with the host kernel. For a multi-tenant
environment this is a significant security boundary reduction. **Kata Containers** restores that
boundary without abandoning the container image workflow:

- Each pod runs inside a lightweight VM (QEMU / Cloud Hypervisor / Firecracker), but the image is
  a standard OCI container image — no VM disk image management, no separate build pipeline.
- `--privileged` inside a Kata VM is contained within that VM; the host kernel is never exposed.
- Startup overhead is modest (~100–200 MB RAM for the guest kernel + agent, seconds faster than
  KubeVirt).
- The only Kubernetes-side change is `runtimeClassName: kata` on the pod (set via the
  `DesktopTemplate` field). The operator wires this into `pod.spec.runtimeClassName`.

In **development / local clusters** (k3d, docker-desktop) run without Kata — `--privileged` is
fine against a dev machine. In **production**, uncomment `runtimeClassName: kata` in the
`DesktopTemplate` values and ensure the cluster has a `kata` RuntimeClass configured.

## Round 2: the display path

Implemented. A browser reaches a desktop with no Guacamole Java web app:

```
browser (guacamole-common-js)  ──WS──▶  portal (Python/aiohttp)
                                            │ guacd handshake, then byte relay
                                            ▼
                                     shared guacd (TCP 4822)
                                            │ dials RDP
                                            ▼
                    per-session ClusterIP Service ──▶ desktop pod (xrdp)
```

- **Display protocol: RDP.** Two desktop images are provided:
  - [`xfce-rdp`](../desktops/xfce-rdp/) — XFCE over xrdp (X11), arm64-native, port 3389.
    Lightweight, no privileged container required. The public `linuxserver/rdesktop` is
    amd64-only; this image is multi-arch. RemoteFX is disabled in `xrdp.ini`
    (`xserverbpp=24`/`max_bpp=24`) so guacd receives lossless bitmaps on the xrdp→guacd leg.
    See [Fidelity: lossless end-to-end](#fidelity-lossless-end-to-end) for why all three levers
    are needed.
  - [`gnome-grd`](../desktops/gnome-grd/) — full GNOME 50 (Wayland) over
    `gnome-remote-desktop` (grd), port 3389. Runs systemd as PID 1 (requires `privileged: true`;
    see [Privileged containers](#privileged-containers-fuse-and-systemd)). grd runs with
    `--headless`, which creates a virtual monitor **per session** via the Mutter
    `RemoteDesktop.Session.CreateVirtualMonitor` D-Bus API rather than capturing a pre-existing
    monitor. This is what enables **true dynamic resize**: grd resizes the virtual monitor to match
    the client's DisplayControl PDU on every window resize, so the desktop always fills the
    browser viewport exactly. (xfce-rdp achieves the same via xrandr; both require
    `resize-method: display-update` in `connectionParams`.)

  Two new template fields drive guacd generically: `protocol` (`vnc`|`rdp`) and
  `connectionParams` (a string map merged into the guacd `connect`). RDP needs a login, so
  well-known image creds + `ignore-cert`/`security` + `color-depth: 24` + `force-lossless: true`
  + `resize-method: display-update` live in `connectionParams` — **not** per-session secrets;
  transport safety is the NetworkPolicy + per-session Service that only guacd can reach.
- **Shared, stateless guacd** Deployment + Service (`whistler-guacd:4822`). One for all
  sessions; it dials each session's per-session Service.
- **Portal** (`whistler/portal/`, a third process from the same image,
  `python -m whistler.portal`):
  - `protocol.py` — pure Guacamole codec (`encode`/`parse_instruction`/`Decoder`/
    `take_complete_instructions`); lengths are Unicode-char counts, parsing is length-driven.
  - `guacd.py` — the server-side handshake (`select`/`args`/`size`…/`connect`/`ready`) like
    guacamole-lite; advertises the **browser client's** protocol version (`VERSION_1_6_0`, matching
    the vendored library — see gotcha 1), not guacd's, and positions params; the browser's
    `Guacamole.Client` only streams after `ready`. The `size` it sends comes from the browser
    viewport (`?w`/`?h` on the connect data), not a fixed default.
  - `app.py` — aiohttp routes (`/`, `/launch`, `/connect/<id>`, `/status/<id>`, `/ws/<id>`,
    `/healthz`, `/static/guacamole-common.min.js`); HTML inline, `guacamole-common-js` **vendored**
    (`whistler/portal/static/`, served by the portal — no CDN); the `/ws` handler does the
    handshake then relays guacd→browser **as whole instructions** (incremental UTF-8 decode +
    `take_complete_instructions`, never splitting an instruction across a WS message — see
    gotchas below).
- **NetworkPolicy carve-out** (`_build_ingress_rules`): round 1's deny-all ingress now allows
  **only** the guacd pod (by namespace + `app: whistler-guacd` label) to reach desktop pods —
  mandatory, or guacd's RDP dial is dropped and nothing renders.
- **Auth is dev-only** this round (`WHISTLER_AUTH_ALLOW_ANY`, identity from header/query); real
  web SSO/OIDC is a follow-up. **No idle-teardown yet** (no active tunnel for N minutes → stop)
  — still the most-forgotten loop; without it preemptible/GPU sessions strand. Track next.
- **Performance**: the codec-heavy work is in guacd (C); the portal is a stateless I/O pump
  that scales by replica count — Python is not the bottleneck.
- **Verification**: unit tests cover the codec + handshake (against a fake guacd) + the ingress
  rule; the `select`/`args`/`VERSION_*` exchange was confirmed against a real guacd 1.6.0
  (multi-arch; 1.5.x was amd64-only). The full e2e test
  ([../tests/integration/test_display.py](../tests/integration/test_display.py)) drives the
  portal→guacd→NetworkPolicy-carve-out→xrdp-pod path against a throwaway k3d cluster (arm64-native
  sample image, so no amd64 requirement).

### Browser rendering: hard-won gotchas

Getting a desktop to actually *render correctly* in the browser took several non-obvious fixes.
guacd and the server-side handshake were never the problem; the issues were all in the
guacd→`guacamole-common-js` contract. Documented so the next person doesn't re-derive them:

1. **Advertise the version of the browser library you actually ship** (`guacd.py`,
   `_CLIENT_PROTOCOL_VERSION = "VERSION_1_6_0"`). guacd's `args` reply leads with a `VERSION_*`
   pseudo-arg; guacd then operates at `min(its version, the version we echo)`. This **must match**
   the `guacamole-common-js` in the browser: advertise higher than the client and guacd emits
   drawing instructions the client can't render (a **black screen** — images decode fine but
   nothing paints). npm only publishes guacamole-common-js up to **1.5.0**, so we **vendor 1.6.0**
   (from the Apache release, `whistler/portal/static/guacamole-common.min.js`, served by the portal)
   and advertise `VERSION_1_6_0` to match it and guacd 1.6.x. (History: we ran 1.5.0-from-CDN +
   `VERSION_1_5_0` first; the 1.6.0 upgrade was an attempt to fix the selection-rectangle drag
   artifact — see [Known limitation](#known-limitation-selection-rectangle--drag-artifacts) — which
   it did **not**, but we kept it for the vendoring/no-CDN win.)
2. **Never split a Guacamole instruction across a WebSocket message** (`app.py` `_relay` +
   `take_complete_instructions`). `guacamole-common-js`'s `WebSocketTunnel.onmessage` parses each
   message **independently from offset 0 with no cross-message buffering** — a message ending
   mid-instruction throws `Incomplete instruction`, or a misread length prefix yields
   `RangeError: Invalid array length` and multi-second parser hangs. A naive 64 KB byte-pump
   splits instructions during heavy redraw, so the relay must forward only complete instructions
   and buffer the partial tail. (This was the big one — it also caused truncated image blobs that
   masqueraded as decode failures.)
3. **The handshake leftover must keep split-multibyte bytes** (`Decoder.pending_bytes`). Bytes can
   arrive glued to `ready`; if a multibyte UTF-8 char is split at that read boundary, the bytes
   buffered inside the incremental decoder aren't in `pending` and get dropped, desyncing the
   stream. Hand the leftover **through** the relay's decoder, never via a standalone strict
   `.decode()`.
4. **Make `#display` its own stacking context** (`connect.html` CSS:
   `position:absolute; z-index:0; isolation:isolate`). `guacamole-common-js` gives its layer
   canvases `z-index:-1`; against any page background that paints them **behind** it — the desktop
   renders but stays invisible. Confirmed via a one-shot canvas geometry/computed-style dump (gated
   behind `?debug`).
5. **Disable `createImageBitmap` in the browser** (`connect.html`,
   `window.createImageBitmap = undefined` before the library loads). Chrome's `createImageBitmap`
   intermittently throws *"source image could not be decoded"* on valid `Blob` input under load,
   and the rejected promise **permanently stalls** guacamole's draw queue (updates freeze). The
   `<img>`-based fallback routes failures through `img.onerror` instead.
6. **Let guac scale mouse coords; never pre-scale or mutate the state** (`connect.html`,
   `client.sendMouseState(state, true)`). When the display is scaled (it is, on hidpi — see
   [Display sizing](#display-sizing-physical-pixels--dynamic-resize--2-toggle)), `Guacamole.Mouse`
   reports coordinates in **CSS/displayed px**, but the remote works in its own resolution. The
   second arg `true` makes guac divide by the live display scale itself (and move the cursor to
   match). Doing it by hand — `state.x /= display.getScale()` — double-scales, because `Guacamole.Mouse`
   reuses **one** state object across events and fires `mousedown` *without recomputing position*:
   the press re-reads the value the previous `mousemove` already mutated. Symptom: the click anchor
   lands at 2× the coordinates while drag-movement tracks correctly (and it's invisible at scale 1,
   i.e. the 50% mode, where the division is a no-op).

Two product-level choices that fell out of this:

- **No forced full-repaint on connect.** RDP delivers a full initial surface per connection and the
  portal opens a fresh guacd→RDP connection on every browser connect, so new sessions *and*
  reconnects get a complete first frame. An earlier `xrefresh`-in-`startwm.sh` workaround was
  removed: it only ran once per X session (never helped reconnects), and the partial first paints
  it chased were really gotcha #2.

### Fidelity: lossless end-to-end

There are **three** independent levers, on two legs, and getting a crisp desktop needs all of them.
The pixels pass through two compressible hops — xrdp→guacd (RDP) and guacd→browser (Guacamole) —
and either can introduce lossy artifacts:

- **xrdp→guacd: disable RemoteFX** (`desktops/xfce-rdp/xrdp.ini`, `xserverbpp=24` + `max_bpp=24`).
  At 32bpp xrdp offers the RemoteFX codec, whose output is lossy; guacd would then re-encode
  already-degraded frames. Forcing a 24-bit session means xrdp can't build the RFX encoder and
  sends plain (lossless) bitmap updates. xrdp 0.9.x has **no per-codec ini toggle** — color depth
  is the documented lever. Verified by log: at 24bpp no `rfxcodec_encode_create`; flip to 32bpp and
  the same image logs it.
- **guacd→browser: `force-lossless: true`** (template `connectionParams`). This is the one that
  bit us. guacd defaults to a lossy heuristic that sends **JPEG** for regions it deems
  "photographic" — visible ringing/mosquito-noise around sharp edges (text, window borders).
  **Advertising only `image/png` in the handshake does NOT prevent this** (the earlier claim here
  was wrong): PNG and JPEG are baseline formats guacd assumes every client decodes, so it emits
  JPEG regardless of what `image` lists. `force-lossless=true` is the actual switch — verified to
  drop guacd's `image/jpeg` frames to zero. (`_IMAGE_MIMETYPES = ("image/png",)` is still set, but
  only to exclude WebP, which decoded unreliably — it does **not** control lossiness.)
- **RDP link color depth: `color-depth: 24`** — full color over the RDP link (guacd often defaults
  to 16-bit, which bands gradients). Separate from the two above; it sets the link depth, not the
  codec or guacd's re-encoding.

Trade-off across all three is bandwidth — fine on a cluster LAN; revisit if WAN access lands.

### Display sizing: physical pixels + dynamic resize + 2× toggle

The remote is sized in **physical device pixels**, not CSS pixels (`connect.html`). On a hidpi or
OS-scaled display (e.g. 4K @ 150%), `window.innerWidth` is in CSS px while the screen has
`innerWidth × devicePixelRatio` real pixels; rendering at CSS px then lets the browser upscale,
which is soft. So the remote renders at `innerWidth × dpr` and the canvas is scaled back down by
`1/dpr` to fill the viewport — **one remote pixel == one device pixel** (no resampling). The
initial size rides the connect data → handshake; window resizes re-target via `client.sendSize`
(debounced), honored by xrdp because the template sets `resize-method: display-update`.

A **100%/50% toggle** (`#q` button) halves the remote resolution on demand (`DIVISOR=2`): far less
to encode/relay/draw on big viewports, upscaled to fill, trading sharpness. `image-rendering:
pixelated` on the canvas keeps that upscale (and any fractional-DPR resampling) nearest-neighbour —
jaggies, not blur.

### Known limitation: selection-rectangle / drag artifacts

Dragging the desktop **selection rectangle** (and, more subtly, window corners during a move)
leaves artifacts in the browser that a **native RDP client does not show**. We chased this hard and
it is **not** fixable from our side: it's a `guacamole-common-js` rendering limitation, not the
desktop image or guacd config. Ruled out by experiment (each verified against the actual guacd
instruction stream / xrdp logs), all of which left the behavior unchanged: RemoteFX on/off, bitmap
/offscreen/glyph caching on/off, the xfwm4 compositor on/off, X `SaveUnders`/`BackingStore` off, and
the 1.5.0→1.6.0 client upgrade. The mechanism: guacd renders xrdp's **save-under** as
`copy 0→-1` (copy the visible layer into an off-screen buffer) and `guacamole-common-js` doesn't
cleanly restore it, so the old outline lingers. A native client implements save/restore correctly,
which is why it's crisp — and is the high-fidelity fallback if a session needs pixel-perfect drag.
Residual banding/edge fuzz some viewers still report after the fidelity fixes above is most likely
the same relay-leg ceiling.

## Round 3: the WebRTC display path

**Why a second path.** Round 2's guacd leg has a hard ceiling: guacd decodes the remote and
**re-rasterizes every frame to PNG/JPEG tiles on a canvas** — so any hardware H.264/H.265 on
the wire is thrown away (this is also why RustDesk can't help: guacd can't speak its protocol,
and even if it could the codec would die at guacd). To get a real codec into the browser's
**decoder** you need **true WebRTC**, with the encoder running *inside the desktop pod* next to
the framebuffer and the browser as a real WebRTC peer. Round 3 adds that as a parallel viewer;
Round 2 stays the default, general-purpose path.

We **adopt [Selkies-GStreamer](https://github.com/selkies-project/selkies-gstreamer)** (Apache-2.0)
as the in-pod engine rather than build one. First image is **software x264** (no GPU); GPU/nvenc
(H.265/AV1) is a later opt-in variant. Note Selkies' prebuilt GStreamer bundle is **amd64-only**
(no arm64 asset at v1.6.2), so the image is pinned to `linux/amd64` and runs under emulation on
arm64 — the originally-planned "multi-arch like the RDP images" doesn't hold for this component.

This is also where Whistler's "run any container as a desktop" idea ends for the WebRTC path:
real VDI needs **purpose-built images** (a display server + Selkies baked in).

```
browser (Whistler page + vendored Selkies client JS)
  │  signaling WS  ─▶  portal /ws-signal/<id>  ──relay──▶  in-pod Selkies signaling (:signalPort)
  │  ICE config    ◀─  portal /ice/<id>   (TURN url + time-limited HMAC creds)
  └═══ media (DTLS/SRTP) ═══▶  coturn ═══▶  in-pod Selkies (GStreamer x264enc)
```

### Viewer selection

A `viewer: guacd | webrtc` field on the desktop `Template` (default `guacd`) picks the backend;
`viewer: guacd` keeps Round 2 byte-for-byte. For `viewer: webrtc` the template carries a
`signalPort` (default 8082) instead of `protocol`/`connectionParams`. To reuse all of Round 1/2's
plumbing, `ensure_session` makes the **signaling port the effective `displayPort`** for the
webrtc viewer — the per-session Service exposes it and `status.displayPort` advertises it exactly
as before; only the portal branches on `viewer` (`status.viewer` is denormalized for that). The
operator writes `status.viewer`; the portal's `connect()` resolves the viewer from the **template**
(always present, unlike status which lags reconcile) to pick the page.

### coturn is mandatory (the real new infra)

WebRTC media can't reach a pod's cluster-internal ICE candidates from a browser outside the
cluster, so **both peers relay through one shared coturn** (`charts/whistler/templates/coturn-*`).
It mirrors the shared-guacd pattern but with two differences that matter:

- **It must be browser-reachable.** `coturn.externalHost` (node IP / LoadBalancer) is *required*
  when enabled; the browser and the in-pod Selkies both use it, and coturn advertises it as its
  relay (`--external-ip`) address.
- **hostNetwork by default.** A wide UDP relay port range can't be mapped sanely through a
  Service/NodePort, so coturn runs with `hostNetwork: true` and the relay ports live on the node
  (the Selkies-recommended pragmatic path). The `k8s WebRTC hairpin` — the in-pod Selkies reaching
  coturn's *external* address from inside the cluster — is the env-specific part that makes a
  one-command dev e2e impossible; it's why coturn is **disabled by default** and not force-enabled
  in skaffold.

Auth uses coturn's **use-auth-secret** (TURN REST) scheme: one shared `static-auth-secret`
(Helm-generated into `<release>-coturn`, stable across upgrades) is given to coturn, injected into
the desktop pod (`SELKIES_TURN_SHARED_SECRET` via `config._selkies_turn_env`), and used by the
portal to **mint a time-limited HMAC credential per browser** at `/ice` — no per-session secrets,
no DB. NetworkPolicy carve-outs: the **portal** joins guacd as an allowed ingress source to desktop
pods (`_build_ingress_rules`), and desktop pods gain egress to coturn (`_build_coturn_egress_rules`).

### Portal

`whistler/portal/webrtc.py` is the parallel of `guacd.py`: pure `mint_turn_credentials` /
`ice_servers` (unit-tested against the coturn HMAC contract) plus an opaque WS↔WS `signal_relay`
(Selkies signaling is self-delimited JSON, so — unlike the guacd relay — there is no
instruction-boundary rule). Routes: `/ice/<id>` (ICE config) and `/ws-signal/<id>` (signaling
relay, reusing `ws()`'s resolve/guard prelude). `connect()` serves the Selkies video page for
webrtc, the Guacamole canvas page for guacd.

### Version lock (the gotcha that will bite, by analogy)

The in-pod Selkies **server** and the Selkies **client JS** the portal serves
(`whistler/portal/static/selkies-core.js`) **must be the same Selkies version** — the exact
analogue of Round 2's guacamole-common-js ↔ guacd match, and almost certainly the same failure
mode (loads, renders nothing). Both pin `SELKIES_VERSION`; the client bundle is sourced from the
same release that ships in the desktop image (`/opt/gst-web`). See
[`desktops/xfce-webrtc/README.md`](../desktops/xfce-webrtc/).

### What's implemented vs. pending

- **Implemented + unit-tested:** the `viewer`/`signalPort` CRD + status plumbing, the pod TURN-env
  injection, the ingress/egress carve-outs, coturn chart objects + shared-secret Secret, the
  portal `webrtc.py` (creds/ICE/relay) + `/ice` + `/ws-signal` + the viewer branch, and the
  `desktops/xfce-webrtc` image + `webrtc-desktop` template.
- **Pending (manual / next):** vendoring the version-locked `selkies-core.js` + the thin
  `startSelkies({videoElement, signalUrl, iceServers, …})` adapter the connect page calls (the
  page reports "client bundle not installed yet" until then); and the full **media e2e** (headed
  browser + reachable coturn), which can't run in k3d/CI. A Playwright-driven check is the intended
  follow-up. Idle-teardown (Round 2's still-missing loop) matters even more here — a WebRTC encoder
  per session is heavier than an idle RDP pod.

### Verification

- **Unit** (`tests/unit/`, no cluster): `test_turn_creds.py` (HMAC cred contract, ICE list,
  signaling relay against a fake in-pod WS), `test_desktop_pod_spec.py` (Selkies TURN-env injected
  only for the webrtc viewer + only when TURN is configured), `test_ingress_rules.py` (portal joins
  the carve-out). All in `make test-local`.
- **Manual e2e:** `helm upgrade ... --set coturn.enabled=true --set coturn.externalHost=<node-ip>`,
  launch `webrtc-desktop`, open `/connect/<id>`: confirm `/ice` returns creds, signaling connects
  through `/ws-signal`, and (once the client JS is vendored) x264 video renders via coturn. Record
  gotchas here as they surface — this path will accrue its own ICE/codec gotchas the way Round 2
  accrued rendering ones.
- **Regression:** existing guacd templates are unaffected (`viewer` defaults to `guacd`); run
  [`tests/integration/test_display.py`](../tests/integration/test_display.py).
