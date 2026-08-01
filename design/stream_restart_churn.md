# Selkies stream restart churn (open investigation)

**Status:** unresolved, not yet investigated. Written 2026-08-01 from evidence collected
while diagnosing two *other* VM desktop bugs (the SQLite/`nobrl` home-mount stall and the
VMI readiness race). Both of those are fixed; this one was set aside deliberately and needs
its own thread.

**Symptom as reported:** "it works intermittently, I'm not sure" — the desktop stream feels
like it comes and goes, distinct from the two fixed bugs.

## What was observed

On `marma/vm-gnome-cuda` (k3s-metal, RTX 4090 passthrough, GNOME 46 + Selkies 2.x, one
browser client attached), the in-guest streamer tears the video pipeline down and rebuilds it
every few seconds. From the guest journal (`whistler-streamer.service`), 14:53–14:55 UTC:

```
14:53:09.879  Received STOP_VIDEO for 'primary'. Stopping stream.
14:53:09.879  Broadcasting primary pipeline reset to all 1 clients: PIPELINE_RESETTING primary
14:53:09.917  Capture loop stopped. X resources released.
14:53:17.154  INFO:webrtc_input:Set clipboard content, length: 13
14:53:17.213  Received START_VIDEO for 'primary'. Starting its stream.
14:53:17.330  NVENC Encoder Initialized successfully.
14:53:19.563  Received STOP_VIDEO for 'primary'. Stopping stream.      <- 2.3s later
14:53:35.648  INFO:webrtc_input:Set clipboard content, length: 13
14:53:35.725  Received START_VIDEO for 'primary'. Starting its stream.
14:53:38.715  Received STOP_VIDEO for 'primary'. Stopping stream.      <- 3.0s later
14:53:43.852  INFO:webrtc_input:Set clipboard content, length: 13
14:53:43.896  Received START_VIDEO for 'primary'. Starting its stream.
14:53:51.632  Received STOP_VIDEO for 'primary'. Stopping stream.      <- 7.7s later
14:54:34.577  INFO:webrtc_input:Set clipboard content, length: 13
14:54:34.629  Received START_VIDEO for 'primary'. Starting its stream.
14:54:38.694  Received STOP_VIDEO for 'primary'. Stopping stream.      <- 4.1s later
14:54:38.991  INFO:webrtc_input:Set clipboard content, length: 13      <- no START follows
```

Each cycle re-initializes NVENC and redoes XShm setup at 3840x1984, and each
`STOP_VIDEO` releases the X capture resources.

## The strongest clue

**`Set clipboard content, length: 13` immediately precedes every single `START_VIDEO`**, by
40–80ms. Five for five. The payload length is identical (13) every time, so this is not a
user copying different things — it looks like a fixed payload replayed as part of a
**client handshake**.

That points at the browser re-establishing the data WebSocket and re-sending its opening
state (clipboard, then `START_VIDEO`), rather than at the streamer deciding to stop on its
own. The streamer only ever logs `Received STOP_VIDEO` — it is being *told* to stop.

The final line at 14:54:38.991 is a clipboard push with **no** `START_VIDEO` after it, i.e.
a handshake that did not complete. That is the state a user would describe as "it stopped
working".

## Hypotheses, most to least favoured

1. **The portal's WebSocket relay is dropping the connection and the browser reconnects.**
   `relay_ws` in [whistler/portal/proxy.py](../whistler/portal/proxy.py) bridges two legs.
   The upstream leg is dialed with `autoping=True`, but the downstream
   `web.WebSocketResponse(max_msg_size=0)` takes the default `heartbeat=None` — so the
   browser-facing leg is never pinged. `_pump` also treats any non-text/binary frame type in
   `_WS_CLOSED_TYPES` as a reason to break and tear both legs down. Worth checking whether
   an idle/ping frame or a transient error is ending the relay.

2. **Traefik (`whistler-portal-proxy`) is timing the WebSocket out.** It sits in front of the
   portal. Its default `respondingTimeouts` are worth reading against the observed 2–8s
   intervals — those are short for a proxy idle timeout, so this ranks below (1), but the
   irregular spacing is not obviously client-driven either.

3. **Client-side backpressure at 4K30.** The streamer logs
   `New frame backpressure task started` / `Frame-based backpressure logic task started` per
   stream, and the settings line shows `Res: 3840x1984 | FPS: 30.0 | Mode: H264 (NVENC)
   FullFrame`. If the client cannot keep up through the proxy, Selkies may be resetting the
   pipeline itself. This would make the churn a symptom of bandwidth, not of a broken relay.

Hypotheses (1) and (3) are distinguishable: (1) means a *new* WebSocket connection each time,
(3) means the *same* connection sending STOP/START.

## Suggested first steps

- Log connection identity in `relay_ws` (a per-connection id at dial and at close, plus the
  close code/reason from both legs). If each `START_VIDEO` arrives on a new relay instance,
  that settles it as (1).
- Check whether `PIPELINE_RESETTING` is broadcast to a client count that changes across
  cycles — `Broadcasting ... to all 1 clients` stayed at 1 throughout, which mildly argues
  the client never fully disconnected, i.e. against (1). This tension needs resolving before
  picking a fix.
- Reproduce with the browser devtools network panel open to see whether the `/websockets`
  request is re-issued per cycle.
- Try a lower resolution / frame rate to see whether the churn interval changes, which would
  implicate (3).

## What this is *not*

- Not the SQLite/`nobrl` home stall (fixed in `whistler/cloudinit.py`) — that was a flat 100s
  block inside app startup, unrelated to the display pipeline.
- Not the VMI readiness race (fixed via a KubeVirt `readinessProbe` plus a Ready-condition
  gate in `_probe_vmi`) — that produced a clean 502 at connect time, not mid-session churn.

## Environment when captured

k3s-metal, KubeVirt v1.8.4, `marma-vm-gnome-cuda` (12 vCPU / 32Gi / 1x RTX 4090 passthrough),
zone `green`, viewer `websockets`, one browser client, `$HOME` over SMB from
`whistler-storage-marma`. Streamer at 3840x1984, H264 NVENC FullFrame, CRF 20.
