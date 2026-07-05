# xfce-selkies2 — XFCE over Selkies 2.x (pixelflux / WebSockets)

Spike evaluating the **Selkies 2.x** stack as the successor to the 1.6.2
GStreamer stack in [`../xfce-webrtc`](../xfce-webrtc/). Same desktop (XFCE on
Xvfb, software encode, no GPU), new streaming engine.

| | |
|---|---|
| Viewer | none yet — standalone only (see *Portal integration*, below) |
| Port | `8082` (web client + WebSocket stream, one port) |
| Transport | **WebSockets** (pixelflux H.264 → browser WebCodecs). No WebRTC, **no coturn/TURN** |
| Encoder | `x264enc` via pixelflux (software; `jpeg` also available) |
| Arch | **multi-arch** — pixelflux/pcmflux ship amd64 + arm64 wheels (unlike 1.x's amd64-only GStreamer bundle) |
| Privileged | no |
| Upstream | `selkies-project/selkies` `main` @ pinned commit (no 2.x tag exists yet) |

## Why this exists (what 2.x changes)

- **Transport**: 2.x defaults to `--mode=websockets` — pixelflux encodes
  H.264/JPEG in-pod, the browser decodes with WebCodecs over a plain WS on the
  web port. The entire TURN story (coturn chart dep, credential minting,
  signaling relay) evaporates for this path. `--mode=webrtc` still exists.
- **No GStreamer**: capture/encode is [`pixelflux`](https://github.com/linuxserver/pixelflux),
  audio is `pcmflux` — PyPI manylinux wheels, MPL-2.0, maintained under the
  **linuxserver** GitHub org (accepted as an upstream *code* dependency; we
  still build our own images — see the VDI strategy note in the project
  memory/docs).
- **Wayland-capable**: pixelflux can act as the Wayland compositor
  (`PIXELFLUX_WAYLAND=true`). Not used here — XFCE is X11 — but it's the
  path for a follow-up Wayland image, which 1.x could never do.
- **Version lock by construction**: the Dockerfile builds the Python server
  *and* the web client (React/Vite `selkies-dashboard` + `selkies-core.js`)
  from the **same pinned commit** (`SELKIES_COMMIT` build arg), instead of the
  hand-maintained client/server version match 1.x needs.

## Build

```bash
docker build -t whistler-desktop-xfce-selkies2:dev desktops/xfce-selkies2
```

Multi-arch works (no platform pin needed):

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t <registry>/whistler-desktop-xfce-selkies2:<tag> --push desktops/xfce-selkies2
```

## Local test (no cluster, no portal — and no TURN, ever)

```bash
make desktop-selkies2-local     # build + run, then open http://localhost:8082/
```

Because everything is TCP on one port, a plain `-p 8082:8082` publish is
sufficient on **every** host OS — the Docker-Desktop-on-macOS caveats that
plague the 1.x image (VM NAT breaking ICE host candidates, internal-TURN
workaround) don't apply. This is the biggest operational win of the spike.

Knobs (env): `SELKIES_RESOLUTION` (default `1280x720`), `SELKIES_ENCODER`
(`x264enc`|`jpeg`), `SELKIES_MODE` (`websockets`|`webrtc`, default
`websockets`), `SELKIES_PORT`, `SELKIES_ENABLE_HTTPS` (see below).

### "Error: This application requires a secure connection (HTTPS)"

The 2.x web client hard-requires a browser **secure context** — not ceremony:
WebCodecs (`window.VideoDecoder`) simply doesn't exist outside one, so there
is nothing to patch out. `http://localhost:8082` **is** a secure context, so
same-machine browsing just works. Browsing from a different machine
(e.g. laptop → dev box) needs one of:

1. **Make it localhost**: `ssh -L 8082:localhost:8082 <dev-box>` and open
   `http://localhost:8082/`. Zero config, no warnings.
2. **Self-signed HTTPS**: `make desktop-selkies2-local SELKIES2_HTTPS=true`
   (or `-e SELKIES_ENABLE_HTTPS=true`), open `https://<host>:8082/` and click
   through the certificate warning. The image bakes in Debian's `ssl-cert`
   snakeoil pair, which selkies' `https_cert`/`https_key` defaults point at.
3. **Chrome flag** (last resort): add `http://<host>:8082` to
   `chrome://flags/#unsafely-treat-insecure-origin-as-secure` and relaunch —
   Chrome then grants that origin a real secure context.

In-cluster this is a non-issue: the portal fronts the pod over the portal's
own HTTPS origin.

## Portal integration (not done — the spike stops at standalone)

This image serves Selkies' own web client, which is enough to evaluate the
stack. Wiring it into Whistler is a *simpler* job than the 1.x `viewer: webrtc`
path, because there is no signaling/media split: everything is HTTP/WS on one
port. The natural shape is a new `DesktopTemplate` viewer type (e.g.
`viewer: selkies`) where the portal reverse-proxies HTTP + WebSocket to the
pod's 8082 — closer to the guacd model operationally (in-cluster relay,
NetworkPolicy stays the boundary) while keeping real H.264 in the browser like
WebRTC. No coturn, no `/ice` credential minting, no vendored client JS in the
portal (the pod serves its own, version-locked by the image build).

## Verification status & spike findings

Verified on a Linux/amd64 host (2026-07-05) with
[`scripts/selkies2_probe.py`](../../scripts/selkies2_probe.py) — a WS protocol
probe speaking the client handshake (`SETTINGS` → `r,WxH` →
`START_VIDEO`/`START_AUDIO`) against the running container (usage in the
script header; verification ladder in
[design/creating_desktops.md](../../design/creating_desktops.md)):

- **Works fully unprivileged** — including `--cap-drop=ALL
  --security-opt=no-new-privileges`: web client HTTP 200, `VIDEO_STARTED` +
  H.264 stripes delivered on screen change, `AUDIO_STARTED` + audio frames,
  full XFCE session. No `/dev/uinput`, no extra caps (gamepads are unix
  sockets + their LD_PRELOAD interposer, not uinput). The streaming layer
  needs nothing the DEs don't already need.
- Pixel changes drive traffic: a static desktop sends (almost) nothing —
  pixelflux's damage detection working as designed.

Gotchas hit while bringing it up (all encoded in the Dockerfile/entrypoint
comments, summarized here for the next stack bump):

- **Python ≥3.12 needs setuptools** at runtime (GPUtil imports `distutils`).
- **CLI flags are dash-separated** (`--web-root`); underscore spellings are
  *silently ignored* (`parse_known_args`).
- **Clipboard is `xclip`** now (not 1.x's `xsel`) — and a missing xclip kills
  the per-client WS loop, presenting as "video never starts".
- **PulseAudio sink must be named `output`** — pcmflux captures the literal
  device `output.monitor` (selkies' `audio_device_name` default).
- **Keyboard needs `xdotool` + a `wtype` shim** — only a–z goes through
  XTEST directly. Space/Enter/arrows/F-keys shell out to `xdotool`, and
  digits/punctuation/unicode to `wtype` (Wayland-only; works in selkies' own
  images because their X11 apps sit under XWayland). Both failures are
  swallowed silently, so the symptom is "only letters type". The image ships
  xdotool plus [`wtype-x11-shim`](wtype-x11-shim) as `/usr/local/bin/wtype`.
  Verified end-to-end: `a 5!⏎` typed over the WS protocol lands verbatim in
  an xfce4-terminal.
- **libva ≥ 2.21 required** (hence Ubuntu 26.04): pixelflux's wheel bundles
  ffmpeg/x264 but links the system libva; on 24.04 (libva 2.20) the capture
  module fails to load, surfacing only as "Legacy screen_capture_module.so
  not found".

## Pinning / bumping

`SELKIES_COMMIT` in the Dockerfile pins `selkies-project/selkies@main` (2.x has
no release tag yet; downstreams — e.g. linuxserver's baseimage-selkies — pin it
the same way). Bumping the commit upgrades server + web client together; there
is nothing else to keep in sync. Watch the
[v2.0.0 roadmap](https://github.com/selkies-project/selkies/issues/227) for a
real tag to pin instead.
