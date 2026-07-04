# xfce-webrtc — XFCE over WebRTC (Selkies)

XFCE desktop streamed to the browser over **real WebRTC** via
[Selkies-GStreamer](https://github.com/selkies-project/selkies-gstreamer),
software-encoded with **x264** (no GPU). This is the `viewer: webrtc` path — the
H.264 stream reaches the browser's decoder instead of being re-rasterized to a
canvas like the guacd/RDP images. See `design/vdi.md` → *Round 3* for the
architecture and why a TURN server (coturn) is mandatory.

| | |
|---|---|
| Viewer | `webrtc` |
| Signaling port | `8082` (must match the template's `signalPort`) |
| Encoder | `x264enc` (software, no GPU) |
| Arch | **amd64 only** (Selkies' prebuilt GStreamer is amd64-only; arm64 runs under emulation) |
| Privileged | no |
| Media path | browser ⇄ coturn ⇄ pod (DTLS/SRTP); portal relays only signaling |

## Build

amd64 only — Selkies' prebuilt GStreamer bundle has no arm64 asset (v1.6.2), so the
Dockerfile pins `linux/amd64`. On Apple Silicon it builds/runs under emulation (slow but
fine for a smoke test).

```bash
docker build \
  --build-arg SELKIES_VERSION=1.6.2 \
  -t ghcr.io/marma/whistler-desktop-xfce-webrtc:dev \
  desktops/xfce-webrtc
```

## Local test (no cluster, no portal, no shared coturn)

The image is self-contained — Selkies serves its **own** signaling + HTML5 client on
the signaling port — so you can validate "does this image stream a desktop over WebRTC"
without k3d, the portal, or the chart's coturn. This is the bottom of the test pyramid;
it does **not** exercise the portal/CRD/coturn integration (test that on a cluster).

```bash
make desktop-webrtc-local         # build + run, then open http://localhost:8082/
```

What that does: builds the image and runs it with the **in-container TURN** enabled
(`SELKIES_USE_INTERNAL_TURN=1`) and the TURN listening + relay ports published. Open
<http://localhost:8082/> and you should get XFCE in the browser.

Why the OS matters (WebRTC is same-host-friendly, but Docker Desktop isn't quite
same-host):

- **Linux host** — simplest with real video and *no* TURN:
  ```bash
  docker run --rm -it --network host whistler-desktop-xfce-webrtc:dev
  ```
  `--network host` shares the host's network, so Selkies' ICE **host candidates** are
  directly reachable by the local browser and ICE connects direct. No relay needed.
- **macOS / Windows (Docker Desktop)** — the container runs in a Linux VM, so its host
  candidates aren't reachable from the browser and media won't flow direct. The
  `make` target above enables the internal TURN to bridge that. If you only need to
  confirm the image **boots** (X + XFCE + the Selkies pipeline start), even plain
  `docker run -p 8082:8082 …` is enough — watch the logs; the signaling/UI come up
  without TURN, only the video needs it.

Knobs (env): `SELKIES_USE_INTERNAL_TURN=1` enables it; `INTERNAL_TURN_EXTERNAL_IP`
(default `127.0.0.1`) is the address the browser uses to reach the relay — set it to
your machine's **LAN IP** to test from another device on the network;
`INTERNAL_TURN_PORT` / `INTERNAL_TURN_MIN_PORT` / `INTERNAL_TURN_MAX_PORT` adjust the
ports (publish whatever range you set).

## Version lock (read before bumping Selkies)

The in-pod Selkies **server** and the Selkies **client JS** the portal serves
(`whistler/portal/static/selkies-core.js`) must be the **same Selkies version** —
the exact analogue of the guacamole-common-js ↔ guacd match (and the same failure
mode: it loads but renders nothing). The client bundle ships inside this image at
`/opt/gst-web`. To (re)vendor it for the portal after changing `SELKIES_VERSION`:

```bash
cid=$(docker create ghcr.io/marma/whistler-desktop-xfce-webrtc:dev)
# Copy the client entrypoint the portal page loads (see app.py SELKIES_JS_FILENAME).
docker cp "$cid":/opt/gst-web/. /tmp/gst-web && docker rm "$cid"
# Assemble the single JS the portal serves (selkies-core.js) from the bundle —
# the gst-web app is modular; the portal page (_CONNECT_WEBRTC_HTML) expects a
# startSelkies({videoElement, signalUrl, iceServers, ...}) global. Wiring that
# thin adapter to the bundle's WebRTC client is the remaining integration step.
```

> **Status:** infrastructure (coturn, TURN-cred minting, signaling relay, CRD
> `viewer` field, pod TURN env) is implemented and unit-tested. The version-locked
> client adapter + full media e2e are verified **manually** (a headed browser +
> real TURN can't run in k3d/CI) — see `design/vdi.md` → Round 3 *Verification*.

## Run it via Whistler

1. Enable coturn and point it at a browser-reachable address:
   ```bash
   helm upgrade ... \
     --set coturn.enabled=true \
     --set coturn.externalHost=<node-ip-or-lb>
   ```
2. The `webrtc-desktop` template ships in `charts/whistler/values.yaml`
   (`desktopTemplates`). Launch it from the portal and open `/connect/<id>`.

## TURN

Selkies reads `SELKIES_TURN_HOST` / `SELKIES_TURN_PORT` / `SELKIES_TURN_PROTOCOL`
/ `SELKIES_TURN_SHARED_SECRET`. The operator injects these from the chart's
`coturn` values (`whistler/config.py` → `_selkies_turn_env`); the portal mints a
matching time-limited credential for the browser at `/ice`. Both peers therefore
relay through the one shared coturn.
