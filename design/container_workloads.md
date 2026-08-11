# Container workloads: what containers are for, and the desktop-in-a-pod work

**Status: decision recorded 2026-08-11.** Container sessions are **web
terminal only**. The container *desktop* — a full DE streamed out of a pod —
worked, and is being set aside rather than maintained. This document is where
that work is preserved: what it was, why it was built the way it was, what it
cost, and what would justify bringing it back.

## The decision

A container session is a **throwaway workspace reached through the portal's
web terminal**. No desktop, no SSH (the gateway needs an sshd the images do
not run — see [proxyjump.md](proxyjump.md)). If you want the whole machine —
a real kernel, root, systemd, a desktop, arbitrary mounts — **start a VM**.

The reasoning, in order of weight:

1. **The use case is underdefined.** The honest one-line case for containers
   is "try something fast in a throwaway container". That is real and worth
   keeping. Everything beyond it was built on the assumption that containers
   should grow into machines, which was never argued for and is not what the
   rest of the system wants.
2. **The security model already prefers VMs**, explicitly. A kernel escape on
   a single-user VM buys an attacker nothing — they already have root on that
   machine ([security.md](security.md)). The same escape from a container
   buys the *node*. The code already acts on this: `force_kata_for_privileged`
   coerces privileged and FUSE templates to Kata, which is a VM. So the
   "give me a real machine" container case was *already* being routed to VMs
   by policy. Making that explicit is the design catching up, not a
   regression.
3. **Two desktop stacks is one too many.** The VM desktop (cloud-init +
   baked-in Selkies) and the container desktop (sidecar + shared X socket)
   solve the same problem twice, in two places, with a shared patch that has
   to be kept in sync by hand. The VM one is the one the security model
   wants.

**What is genuinely lost** — recorded because it is real, not to soften the
decision:

- **GPU sharing.** VM GPU passthrough is exclusive: one card, one VM. Pods
  can time-slice or MIG the same GPU across several users. With one card and
  several researchers, containers are the only way to share it. This is the
  strongest argument against the decision and it is not answered here.
- **Start latency and density.** A pod starts in seconds; a VM pays
  cloud-init and the NFS home mount, tens of seconds. For short interactive
  bursts that gap is the whole experience.

Neither is a *desktop* argument, which is why they don't change the decision
— but if either becomes pressing, the answer is a better container story,
not a container desktop.

## The case that would bring GUIs back: single-app containers

The interesting version was never "a desktop in a pod". It is **one
application in a pod, streamed** — a viewer, an annotation tool, a notebook
UI, a domain-specific app — with no desktop environment, no window manager,
no session, no file manager. Just the app on a display.

That is a much better fit for a container than a DE is: it is a single
process with a single purpose, the image can be small and purpose-built,
there is no init system to fight, and the security surface is the app rather
than a whole userland. It also plays to the mechanism below, which never
actually needed a desktop — the workload container only ever had to be an X
client.

Not prioritised. Recorded so the next person understands that setting the
desktop aside is not a judgement on GUIs in containers.

## What was built (the part worth keeping)

The mechanism was genuinely nice and the reason it worked is worth
preserving in full, because a single-app container would use exactly this.

**The workload image knows nothing about displays.** It ships no X server, no
VNC, no encoder, no supervisor. Its entire display contract is two
environment variables:

```
DISPLAY=:0
PULSE_SERVER=unix:/tmp/pulse/native
```

**A native sidecar owns the display.** `desktops/streamer-selkies2` runs
Xvfb + PulseAudio + Selkies (H.264 over WebSocket). It is declared as an
`initContainer` with `restartPolicy: Always` — a *native sidecar*, so
Kubernetes starts it before the workload container and keeps it running for
the pod's life. The two share the X and Pulse sockets over `emptyDir`
volumes:

```yaml
initContainers:
  - name: streamer
    image: ghcr.io/marma/whistler-streamer-selkies2:latest
    restartPolicy: Always            # native sidecar
    env: [{name: SELKIES_PORT, value: "8082"}, ...streamerEnv]
    ports: [{containerPort: 8082, name: display}]
    volumeMounts:
      - {name: x11,   mountPath: /tmp/.X11-unix}
      - {name: pulse, mountPath: /tmp/pulse}
    startupProbe:                    # gates the workload container
      tcpSocket: {port: 8082}
      periodSeconds: 2
      failureThreshold: 60
volumes:
  - {name: x11,   emptyDir: {}}
  - {name: pulse, emptyDir: {}}
```

Four details that are not obvious and cost time to get right:

- **The `startupProbe` on the Selkies port is what makes the contract
  work.** It gates the workload container's start, so the workload's
  entrypoint can assume `DISPLAY` is live and needs no wait loop of its own.
- **MIT-SHM works for free in a pod**, because containers in a pod share an
  IPC namespace by default. The docker-compose equivalent
  (`desktops/compose-sidecar.yaml`) needs explicit `ipc:` wiring for the same
  effect — a real difference between the two harnesses.
- **The display port lives on the sidecar, not on `main`.** The per-session
  Service and the portal proxy are built from it.
- **`streamerEnv` carries what the sidecar cannot infer.** The one that
  matters: `SELKIES_H264_STREAMING_MODE: "true"` is *required* for GL
  compositors like GNOME Shell. Mutter composites once and emits no further
  damage for static windows, so damage-based capture never re-sends those
  regions and the client canvas shows them black until something forces a
  repaint. Streaming mode encodes full frames continuously instead, at the
  cost of constant bandwidth and CPU even when idle. The streamer cannot know
  what the workload runs, so the template has to say.

**The images.** `desktops/xfce-plain` and `desktops/gnome-plain` are workload
images built to that contract — a DE and nothing else. GNOME runs its X11
backend (Wayland is architecturally incompatible with a display-owning
sidecar, since the compositor *is* the display server) on llvmpipe.

## What it cost

- **Two of everything.** A workload image and a streamer image per desktop,
  plus the VM images doing the same job a second way.
- **A hand-synced patch.** The Selkies web client is patched at build time
  (`mac-cmd-chords.patch`) and the patch lives in *both* web-build contexts —
  container and VM — with a note to keep them in sync. `patch` fails the
  build loudly when a `SELKIES_COMMIT` bump makes it stale, which is the
  right failure but still a recurring cost.
- **Software rendering.** GNOME on llvmpipe is usable, not good. Making it
  good means a GPU, and a GPU in a pod for a desktop is a poor use of a card
  that a VM would use better.
- **A steady trickle of display-stack debugging** — streaming modes, keymaps,
  clipboard agents, damage tracking — none of it about what Whistler is for.

## What changed in the code

- `_apply_policy` refuses `mode: desktop` for container and Kata runtimes,
  with a `PolicyError` naming VMs as the path. Fail-closed, like every other
  policy check.
- `_build_pod_spec` no longer builds the streamer sidecar, the X/Pulse
  `emptyDir`s, or the `DISPLAY`/`PULSE_SERVER` injection. Container pods are
  one container.
- Desktop sessions are VM-only; the per-session Service, the portal's desktop
  proxy and the screenshot loop are unchanged and continue to serve them.

Left in place on purpose: the container desktop **images**
(`desktops/streamer-selkies2`, `desktops/xfce-plain`, `desktops/gnome-plain`)
and their compose/Makefile harness. They cost nothing while unbuilt, they are
the reference for the mechanism above, and a single-app container would start
from the streamer image rather than from scratch. `whistler.images.desktop`
in values.yaml is now an allow-list nothing can use — harmless, and the
obvious place to notice this decision.

## What would have to be true to bring it back

- A concrete user asking for a GUI **application** — not a desktop — that
  cannot be a VM, with a reason (density, start latency, or a shared GPU).
- An answer for the GPU: either the app is fine on llvmpipe, or the cluster
  can share a card across pods in a way a VM cannot.
- Acceptance that the container is not a machine: no root, no systemd, no
  arbitrary mounts. If the request needs those, it is a VM.
