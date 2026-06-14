# VDI: desktop-pod and KubeVirt-VM backends

Status: **implemented (round 1 of 2)**. Display tunnel (Guacamole) is round 2 and not
yet built. KubeVirt VM path is implemented behind the abstraction but **unverified
end-to-end** (no KubeVirt in any cluster we can test against yet).

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
| `instancetype` | string | VM-only; KubeVirt instancetype (supplies cpu/memory) |

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

- Per-user namespace with default-deny ingress and locked-down egress (the existing
  `_build_egress_rules` complement-CIDR math), reused unchanged.
- `automountServiceAccountToken: false` on desktop pods; optional `securityContext` from the
  user record (users absent from `users.yaml` get none — same as SSH pods).
- Ephemeral root + persistent home PVC limits the data-at-rest surface to one PVC per user.
- VM backend is the answer to "I need nested containers / privileged workloads" — the
  hypervisor contains the blast radius. Do not try to make nested Docker safe in a pod.

## What's deferred to round 2 (display path)

Per [../mother-design.md](../mother-design.md): keep `guacd` (protocol engine) +
`guacamole-common-js` (browser renderer); replace the Guacamole Java web app with a thin
WebSocket↔guacd tunnel fed connection params straight from the CR (the per-session Service
address + ephemeral creds). Auth stays in the portal; CRs remain the single source of truth.
Idle-teardown detection (no active tunnel for N minutes → stop) is the most-forgotten loop —
without it, preemptible/vGPU licenses strand.
