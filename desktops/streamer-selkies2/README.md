# streamer-selkies2 — the display-plane sidecar

Xvfb + PulseAudio + **Selkies 2.x** (pixelflux/WebSockets) and **no desktop
environment**. This image is not a catalog entry: it is the streamer sidecar
the operator injects into **every desktop pod**, next to a *display-unaware*
workload image (e.g. [`../xfce-plain`](../xfce-plain/),
[`../gnome-plain`](../gnome-plain/)).

This is stage 1 of the guest-unaware-display direction: workload images know
nothing about streaming. Their entire display contract is two env vars and two
shared sockets:

| Shared | Path | Provided by |
|--------|------|-------------|
| X socket | `/tmp/.X11-unix` | Xvfb here; DE/apps connect from the workload |
| Pulse socket | `/tmp/pulse` (`PULSE_SERVER=unix:/tmp/pulse/native`) | PulseAudio here; workload playback is captured by pcmflux |

In-cluster the operator runs this image as a **native sidecar** (initContainer
with `restartPolicy: Always`) with a TCP startupProbe on `SELKIES_PORT`:
Selkies only opens the port after X is up, so the workload container always
starts against a live display. Swapping the streaming protocol later (WebRTC,
host-side capture, anything) means swapping this image — no workload image
changes. See `KubeConfigManager._build_pod_spec` (desktop branch).

This image is the single home of the streaming stack: Selkies is pinned by
`SELKIES_COMMIT`, and the Python server and web client are both built from
that commit (client/server version lock by construction). The silent-failure
catalog for its packages lives in the Dockerfile comments. It descends from
the embedded `xfce-selkies2`/`gnome-selkies2` images, removed when the sidecar
became the only display path; they remain in git history.

## Env

- `SELKIES_PORT` (default `8082`) — HTTP/WebSocket port (probe target).
- `SELKIES_RESOLUTION` (default `1280x720`) — initial desktop size.
- `SELKIES_H264_STREAMING_MODE` (default `false`) — **must be `true` for GL
  compositors like GNOME Shell** (mutter emits no damage for static content;
  damage-based capture leaves static windows black). Set per template via
  `streamerEnv` — the operator passes any `streamerEnv` entries straight into
  this container, which is how templates tune workload-dependent knobs the
  sidecar can't infer.
- `SELKIES_ENCODER` (default `x264enc`) / `SELKIES_MODE` (default
  `websockets`) / `SELKIES_ENABLE_HTTPS` (default `false`; the 2.x web client
  needs a browser secure context, so any non-`http://localhost` dev access
  needs HTTPS).

Xvfb runs with `+extension GLX` so GL workloads (GNOME's llvmpipe compositing)
get a software GL context; X clients additionally use MIT-SHM, which needs a
shared IPC namespace — pods share it by default, compose needs the `ipc:`
pairing in [`../compose-sidecar.yaml`](../compose-sidecar.yaml).

## Running without a cluster (docker only)

The sidecar pairing degrades gracefully to two plain containers sharing two
named volumes — no Kubernetes involved. The one-command way, from the repo
root:

```bash
make desktop-sidecar-local        # build + run both (XFCE), http://localhost:8082/
make desktop-gnome-sidecar-local  # same, with the GNOME Shell workload
make desktop-sidecar-local-down   # stop either variant, remove the shared volumes
```

(backed by [`../compose-sidecar.yaml`](../compose-sidecar.yaml), whose
healthcheck-gated `depends_on` mirrors the k8s startupProbe ordering; the
usual `SELKIES2_RESOLUTION=… SELKIES2_HTTPS=true` make variables apply).
The manual spelling of the same thing:

```bash
docker build -t whistler-streamer-selkies2:dev .
docker build -t whistler-desktop-xfce-plain:dev ../xfce-plain

docker run -d --name streamer \
  -v whistler-x11:/tmp/.X11-unix -v whistler-pulse:/tmp/pulse \
  -p 8082:8082 whistler-streamer-selkies2:dev

docker run -d --name desktop \
  -v whistler-x11:/tmp/.X11-unix -v whistler-pulse:/tmp/pulse \
  -e DISPLAY=:0 -e PULSE_SERVER=unix:/tmp/pulse/native \
  whistler-desktop-xfce-plain:dev

# → http://localhost:8082 (secure-context rules as usual: localhost is fine)
```

There is no startupProbe in docker, so the workload entrypoint waits up to 30 s
for the display and then fails with an explanatory error instead of
crash-looping silently.

Cleanup: `docker rm -f streamer desktop && docker volume rm whistler-x11 whistler-pulse`.
