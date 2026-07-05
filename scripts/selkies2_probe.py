#!/usr/bin/env python3
"""Protocol probe for Selkies 2.x desktop images (the websockets viewer path).

Verifies a running image end-to-end WITHOUT a browser, by speaking the
selkies-web-core client handshake over the data WebSocket. Two modes:

  stream   Connect, handshake, START_VIDEO/START_AUDIO, wiggle the mouse to
           force pixel changes, and report what arrives. PASS looks like:
           VIDEO_STARTED + AUDIO_STARTED in the text messages, and binary
           messages with first byte 4 (H.264 video stripes — the "big" ones)
           alongside first byte 1 (audio frames).

  keys     Type a, space, 5, !, Enter over the protocol. Exercises all three
           server-side injection paths (XTEST for letters, xdotool for
           space/Enter, the wtype shim for digits/punctuation). Pair it with
           a focused terminal running `cat > /tmp/typed.txt` in the pod to
           confirm what actually lands (see design/creating_desktops.md).

Usage (aiohttp is already in the image's venv — run it inside the container):

  docker cp scripts/selkies2_probe.py <container>:/tmp/probe.py
  docker exec <container> /opt/venv/bin/python /tmp/probe.py stream
  docker exec <container> /opt/venv/bin/python /tmp/probe.py keys

Against a non-default port/host: --url http://host:8082
"""
import argparse
import asyncio
import collections

import aiohttp

HANDSHAKE_SETTINGS = (
    'SETTINGS,{"displayId":"primary","framerate":15,"encoder":"x264enc",'
    '"initialClientWidth":1280,"initialClientHeight":720}'
)


async def handshake(ws):
    """Wait for the server MODE announcement, then register as the primary
    display client the way the real web client does."""
    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT and msg.data.startswith("MODE "):
            await ws.send_str(HANDSHAKE_SETTINGS)
            await ws.send_str("r,1280x720,primary")
            return msg.data
    raise RuntimeError("connection closed before MODE announcement")


async def stream(url: str) -> int:
    counts = collections.Counter()
    buckets = collections.Counter()
    texts = []
    total = 0
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(f"{url}/websockets", max_msg_size=0) as ws:
            mode = await handshake(ws)
            texts.append(mode)
            await ws.send_str("START_VIDEO")
            await ws.send_str("START_AUDIO")

            async def poke():
                await asyncio.sleep(2)
                for i in range(40):
                    await ws.send_str(f"m,{100 + i * 8},{100 + (i % 7) * 30},0,0")
                    await asyncio.sleep(0.05)

            poker = asyncio.create_task(poke())
            try:
                async with asyncio.timeout(12):
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.BINARY:
                            counts["binary"] += 1
                            total += len(msg.data)
                            first = msg.data[0] if msg.data else -1
                            buckets[(first, "big" if len(msg.data) > 1000 else "small")] += 1
                        elif msg.type == aiohttp.WSMsgType.TEXT:
                            counts["text"] += 1
                            if not msg.data.startswith('{"type": "system') and not msg.data.startswith('{"type": "network'):
                                if len(texts) < 14:
                                    texts.append(msg.data[:90])
            except TimeoutError:
                pass
            finally:
                poker.cancel()

    print(f"binary messages: {counts['binary']}, total bytes: {total}")
    for k, v in sorted(buckets.items()):
        print(f"  first_byte={k[0]} {k[1]}: {v}")
    for t in texts:
        print("  text:", t)

    got_video = any(t == "VIDEO_STARTED" for t in texts)
    got_stripes = any(k[0] == 4 and k[1] == "big" for k in buckets)
    ok = got_video and got_stripes
    print("PASS" if ok else "FAIL: no VIDEO_STARTED and/or no large type-4 (H.264) frames")
    return 0 if ok else 1


async def keys(url: str) -> int:
    # a, space, 5, !, Enter — one key per injection path (XTEST / xdotool /
    # wtype shim), see module docstring.
    keysyms = [97, 32, 53, 33, 65293]
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(f"{url}/websockets", max_msg_size=0) as ws:
            await handshake(ws)
            await ws.send_str("START_VIDEO")
            await asyncio.sleep(2)
            for ks in keysyms:
                await ws.send_str(f"kd,{ks}")
                await asyncio.sleep(0.15)
                await ws.send_str(f"ku,{ks}")
                await asyncio.sleep(0.25)
            await asyncio.sleep(1)
    print("keys sent: a SPACE 5 ! ENTER — check the focused app received 'a 5!\\n'")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", choices=["stream", "keys"])
    p.add_argument("--url", default="http://127.0.0.1:8082")
    args = p.parse_args()
    raise SystemExit(asyncio.run({"stream": stream, "keys": keys}[args.mode](args.url)))


if __name__ == "__main__":
    main()
