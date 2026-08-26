"""Periodic desktop screenshots — one current thumbnail per running session.

**The H.264 stream is deliberately not the source.** Selkies only encodes while
a browser is attached (capture starts on ``START_VIDEO`` and stops when the last
client disconnects), so tapping the relay in ``whistler.portal.proxy`` would
only ever see sessions somebody is already watching — the least interesting
ones. The X display, by contrast, exists for the whole life of the session.
Grabbing from there covers every running desktop, watched or not, and costs the
session nothing when nobody is looking.

Grab side, both runtimes, one command: ``xwd -root`` piped through ``gzip``.
``xwd`` (from **x11-apps**) is the one X grabber that needs no image encoder in
the session — it dumps the raw framebuffer at full resolution, and the portal
does the resampling and PNG encode with nothing but the stdlib (``xwd_to_rgb``
/ ``resize_rgb`` / ``sharpen_rgb`` / ``encode_png`` below). That keeps the
streamer sidecar and the VM guests at one extra apt package instead of
imagemagick/ffmpeg, and puts the per-shot CPU on the idle portal rather than in
the user's session — which is also what makes it affordable to scale properly
(box reduce, then a Catmull-Rom cubic, then a small unsharp pass) instead of
throwing pixels away.

  * **pods** — ``kubectl exec -c streamer``: the sidecar owns Xvfb (the
    workload container shares the socket but need not carry the tool).
  * **VMs** — SSH as the session user with the portal's per-user VM access key,
    the same credential the web terminal uses (``kubevirt.relay_ssh``). Xvfb
    runs with ``-ac``, so ``$DISPLAY`` is reachable without an xauth cookie.

Storage is a process-wide dict, newest-only, never written to disk: one PNG per
(user, session), replaced on each pass and dropped when the session goes away.
A portal restart loses them all and the next pass refills — which is the whole
lifetime worth having, since a screenshot that outlives its session is worse
than no screenshot at all. **This assumes a single portal replica**: the store
is per-process, so N replicas each run their own capture loop (N grabs into
every session) and answer /screenshot from their own store. See
``portal.replicaCount`` in values.yaml; the loop warns at startup if it is >1.

## This is monitoring — and it is not the only monitoring here

Say it plainly: a periodic capture of a user's screen is surveillance, and the
stored size is the only thing deciding which kind. The default box is
**960x540**, which puts a 1080p desktop at exactly half scale: window titles,
menu entries and most UI text are readable, editor body text mostly is not.
That is a deliberate step up from the 320px *activity overview* this shipped
with first — at 6x down you could see a session was in use and nothing more —
and it was taken because the overview was, in practice, too coarse to be worth
looking at. Set ``WHISTLER_SCREENSHOT_WIDTH``/``_HEIGHT`` back down to get that
posture, or toward the native resolution to get full readable monitoring.
The setting is the policy, so it is documented as such rather than buried as a
rendering detail. **The stored size is the boundary, not the CSS a kiosk card
displays it at** — anything this module holds is retrievable at full stored
resolution from ``/screenshot/<id>``.

Nor is this the first such capability in the stack, only the first one whistler
actually uses. Selkies is multi-client by design: one capture pipeline
broadcast to every attached websocket (``_broadcast_to_clients``), with a
``controller``/``viewer`` role split where ``viewer`` is read-only spectating
of the live stream. The in-session server runs with no master token and
``--enable-basic-auth=false``, so that role comes straight off the query string
and anything able to reach ``:8082`` can attach. The only access control is the
portal's proxy, which resolves sessions in the requesting user's namespace
(``_resolve_desktop_base``). So the substrate can already put a second pair of
eyes on a live desktop; what is missing is an admin surface for it, not a
mechanism. Worth knowing before anyone concludes that turning screenshots off
makes sessions unobservable.

Only *desktop* sessions have an X display; ssh-mode instances are skipped.

Since groups landed, the loop also skips a session whose owner is not granted
the ``screenshots`` channel in that session's zone — checked before the grab,
so those pixels never reach this process at all.
"""
import asyncio
import contextlib
import logging
import os
import re
import struct
import time
import zlib

from whistler.config import CHANNEL_SCREENSHOTS

logger = logging.getLogger("whistler.portal")

DEFAULT_INTERVAL_SECONDS = 300
# The stored size is the privacy posture, not a rendering detail (see the
# module docstring). 960x540 is a legible half-scale view of a 1080p desktop:
# window titles, menus and most UI text survive it. That is a deliberate step
# up from the 320px activity overview this used to default to, and it is the
# knob to turn back down for a deployment that only wants "in use or not".
DEFAULT_MAX_WIDTH = 960
DEFAULT_MAX_HEIGHT = 540
# Unsharp amount applied after the downscale. Small on purpose: enough to give
# text and window edges their contrast back, not enough to ring.
DEFAULT_SHARPEN = 0.35
DEFAULT_DISPLAY = ":0"
# The streamer sidecar owns Xvfb in a desktop pod (see _build_pod_spec); the
# workload container is "main" and is deliberately left without the grab tool.
STREAMER_CONTAINER = "streamer"

_GRAB_TIMEOUT_SECONDS = 30
_MAX_CONCURRENT_GRABS = 4
# Decompressed ceiling for one dump: 8K RGBA is ~140 MB, a 4K desktop ~35 MB.
# Bounded so a hostile guest can't answer a grab with a zip bomb.
_MAX_XWD_BYTES = 192 * 1024 * 1024
# $DISPLAY goes into a shell command; keep it to the one shape X allows.
_DISPLAY_RE = re.compile(r"^:\d+(\.\d+)?$")

# XWD file format (X11R7 xwd(1)): a 25-field u32 header, the window name, an
# XWDColor table (12 bytes/entry, present even for TrueColor visuals), then the
# raw pixel rows.
_XWD_HEADER_BYTES = 100
_XWD_HEADER_FIELDS = 25
_XWD_FILE_VERSION = 7
_XWD_ZPIXMAP = 2
_XWD_COLOR_ENTRY_BYTES = 12


class ScreenshotError(Exception):
    """One session's grab or decode failed. The capture loop logs it and moves
    on to the next session — a screenshot is never worth failing a pass over."""


# --------------------------------------------------------------------------- #
# Pure decode/encode. No cluster, no I/O — unit-tested in                       #
# tests/unit/test_screenshots.py.                                              #
# --------------------------------------------------------------------------- #

def gunzip(blob: bytes, limit: int = _MAX_XWD_BYTES) -> bytes:
    """Inflate the gzip stream the grab command produces, refusing anything
    that expands past ``limit``."""
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        out = decompressor.decompress(blob, limit)
    except zlib.error as e:
        raise ScreenshotError(f"grab output is not gzip: {e}") from e
    if decompressor.unconsumed_tail:
        raise ScreenshotError(f"xwd dump expands past {limit} bytes")
    return out


def _xwd_header(blob: bytes) -> tuple[int, ...]:
    """The 25 header fields. ``xwd`` normalises them to MSB-first, but detect
    rather than assume: ``file_version`` is the self-describing field."""
    if len(blob) < _XWD_HEADER_BYTES:
        raise ScreenshotError(f"xwd dump truncated ({len(blob)} bytes)")
    for order in (">", "<"):
        fields = struct.unpack(f"{order}{_XWD_HEADER_FIELDS}I", blob[:_XWD_HEADER_BYTES])
        if fields[1] == _XWD_FILE_VERSION:
            return fields
    raise ScreenshotError("not an xwd dump (bad file_version)")


def _mask_channel(mask: int) -> tuple[int, int]:
    """``(shift, width_in_bits)`` for one of a TrueColor visual's colour masks."""
    if not mask:
        raise ScreenshotError("xwd dump has no TrueColor masks")
    shift = (mask & -mask).bit_length() - 1
    return shift, bin(mask >> shift).count("1")


def _scale8(value: int, bits: int) -> int:
    """Widen/narrow a channel of ``bits`` to 8 bits. Identity for the usual
    depth-24 case; the arithmetic only matters for 16bpp (5/6/5) visuals."""
    if bits == 8:
        return value
    if bits > 8:
        return value >> (bits - 8)
    return value * 255 // ((1 << bits) - 1)


def _fast_channel_offsets(bits_per_pixel: int, byte_order: int,
                          masks: tuple[int, int, int]) -> tuple[int, int, int] | None:
    """Byte index of R, G, B inside one pixel — or None if the visual does not
    put each channel in a whole byte of its own.

    This is the shape ``xwd -root`` produces on every desktop we run (depth 24
    in 24 or 32 bits per pixel), and it is what lets the decoder below copy
    channels with slice assignment instead of unpacking two million pixels in
    Python. The generic path still handles anything else."""
    if bits_per_pixel not in (24, 32):
        return None
    stride = bits_per_pixel // 8
    offsets = []
    for mask in masks:
        shift, bits = _mask_channel(mask)
        if bits != 8 or shift % 8 or shift // 8 >= stride:
            return None
        index = shift // 8
        offsets.append(index if byte_order == 0 else stride - 1 - index)
    return tuple(offsets)


def xwd_to_rgb(blob: bytes) -> tuple[int, int, bytes]:
    """Decode an ``xwd`` dump to ``(width, height, packed RGB)`` at the
    display's **full resolution**.

    Decoding used to subsample: taking every n-th pixel on the way out made a
    4K grab cost a thumbnail's worth of work instead of 8M iterations. It also
    made the thumbnails bad in the specific way nearest neighbour is bad —
    at a 6x ratio text becomes speckle, one-pixel window chrome appears and
    disappears between passes, and a scrollbar is there or not depending on
    where the grid landed. Scaling is now its own step over every pixel
    (``resize_rgb``), which is why this function has a fast path for the pixel
    format the sessions actually produce.
    """
    fields = _xwd_header(blob)
    header_size, pixmap_format = fields[0], fields[2]
    width, height = fields[4], fields[5]
    byte_order = fields[7]
    bits_per_pixel, bytes_per_line = fields[11], fields[12]
    red_mask, green_mask, blue_mask = fields[14], fields[15], fields[16]
    ncolors = fields[19]

    if pixmap_format != _XWD_ZPIXMAP:
        raise ScreenshotError(
            f"unsupported xwd pixmap_format {pixmap_format} (need ZPixmap)")
    if bits_per_pixel not in (16, 24, 32):
        raise ScreenshotError(f"unsupported xwd bits_per_pixel {bits_per_pixel}")
    if not width or not height:
        raise ScreenshotError("xwd dump has zero extent")

    offset = header_size + ncolors * _XWD_COLOR_ENTRY_BYTES
    needed = offset + bytes_per_line * height
    if len(blob) < needed:
        raise ScreenshotError(f"xwd dump truncated: {len(blob)} < {needed} bytes")

    endian = "little" if byte_order == 0 else "big"
    stride = bits_per_pixel // 8
    out_stride = width * 3
    rgb = bytearray(out_stride * height)

    fast = _fast_channel_offsets(bits_per_pixel, byte_order,
                                 (red_mask, green_mask, blue_mask))
    if fast:
        r_at, g_at, b_at = fast
        line = bytearray(out_stride)
        for y in range(height):
            base = offset + y * bytes_per_line
            row = blob[base:base + width * stride]
            # Three strided copies per row, all in C: pull each channel out of
            # the pixels and drop it straight into its lane of the RGB line.
            line[0::3] = row[r_at::stride]
            line[1::3] = row[g_at::stride]
            line[2::3] = row[b_at::stride]
            rgb[y * out_stride:(y + 1) * out_stride] = line
        return width, height, bytes(rgb)

    r_shift, r_bits = _mask_channel(red_mask)
    g_shift, g_bits = _mask_channel(green_mask)
    b_shift, b_bits = _mask_channel(blue_mask)
    i = 0
    for y in range(height):
        base = offset + y * bytes_per_line
        for x in range(width):
            start = base + x * stride
            pixel = int.from_bytes(blob[start:start + stride], endian)
            rgb[i] = _scale8((pixel & red_mask) >> r_shift, r_bits)
            rgb[i + 1] = _scale8((pixel & green_mask) >> g_shift, g_bits)
            rgb[i + 2] = _scale8((pixel & blue_mask) >> b_shift, b_bits)
            i += 3
    return width, height, bytes(rgb)


# --------------------------------------------------------------------------- #
# Resize + sharpen                                                             #
#                                                                              #
# A framebuffer scaled to a thumbnail is a resampling problem, and doing it    #
# properly is the difference between a legible small picture and a crunchy     #
# one. Two steps, both stdlib, both integer arithmetic:                        #
#                                                                              #
#   1. an integer box reduce (every source pixel averaged exactly once) down   #
#      to the smallest whole multiple of the target, then                       #
#   2. a Catmull-Rom cubic resample for whatever ratio is left over.            #
#                                                                              #
# Step 1 is what keeps this affordable in pure Python: a full-support cubic    #
# straight from 1920 to 960 needs ~8 taps per output pixel per axis, while the #
# box reduce touches each source pixel once and usually lands *on* the target  #
# (1920x1080 -> 960x540 is exactly 2x, so step 2 is skipped entirely). It is   #
# also the right filter for the job — at large ratios an area average is what  #
# a cubic's widened kernel approximates anyway.                                #
# --------------------------------------------------------------------------- #

# Fixed-point weights: integer accumulation is both faster than float here and
# exactly reversible, so a flat region stays flat instead of drifting a value.
_WEIGHT_SCALE = 1 << 12
_WEIGHT_HALF = _WEIGHT_SCALE // 2
_WEIGHT_BITS = 12
# Catmull-Rom (a = -0.5): the interpolating member of the cubic family, no
# ringing worth the name and a little inherent crispness.
_CUBIC_A = -0.5


def _cubic(t: float) -> float:
    t = abs(t)
    if t < 1.0:
        return ((_CUBIC_A + 2.0) * t - (_CUBIC_A + 3.0)) * t * t + 1.0
    if t < 2.0:
        return ((t - 5.0) * t + 8.0) * t * _CUBIC_A - 4.0 * _CUBIC_A
    return 0.0


def _cubic_taps(src: int, dst: int) -> list[tuple[int, list[int]]]:
    """``(first source index, weights)`` per output pixel for one axis.

    The kernel is widened by the downscale ratio (the standard trick: sample
    the filter in *destination* space) so that every source pixel contributes
    to some output pixel and nothing is simply skipped. Weights are normalised
    to sum to exactly ``_WEIGHT_SCALE``."""
    ratio = src / dst
    scale = max(ratio, 1.0)
    support = 2.0 * scale
    taps = []
    for i in range(dst):
        center = (i + 0.5) * ratio
        start = max(0, int(center - support + 0.5))
        end = min(src, int(center + support + 0.5))
        if end <= start:                      # degenerate at the very edges
            start = min(start, src - 1)
            end = start + 1
        weights = [_cubic((x + 0.5 - center) / scale) for x in range(start, end)]
        total = sum(weights) or 1.0
        ints = [round(w * _WEIGHT_SCALE / total) for w in weights]
        ints[len(ints) // 2] += _WEIGHT_SCALE - sum(ints)
        taps.append((start, ints))
    return taps


def _clamp8(value: int) -> int:
    """Fixed-point accumulator -> byte. A cubic's negative lobes can push a
    high-contrast edge past either end; clip rather than wrap."""
    value = (value + _WEIGHT_HALF) >> _WEIGHT_BITS
    return 0 if value < 0 else (255 if value > 255 else value)


def _box_reduce(width: int, height: int, rgb: bytes,
                kx: int, ky: int) -> tuple[int, int, bytes]:
    """Average every ``kx * ky`` block. The last partial block is cropped
    rather than averaged short — at most ``kx - 1`` columns off the right edge
    of a desktop, which no thumbnail will ever show."""
    out_w, out_h = width // kx, height // ky
    stride = width * 3
    out = bytearray(out_w * out_h * 3)
    count = kx * ky
    half = count // 2
    o = 0
    for oy in range(out_h):
        acc = [0] * (out_w * 3)
        for k in range(ky):
            base = (oy * ky + k) * stride
            row = rgb[base:base + stride]
            i = 0
            for ox in range(out_w):
                p = ox * kx * 3
                for _ in range(kx):
                    acc[i] += row[p]
                    acc[i + 1] += row[p + 1]
                    acc[i + 2] += row[p + 2]
                    p += 3
                i += 3
        for value in acc:
            out[o] = (value + half) // count
            o += 1
    return out_w, out_h, bytes(out)


def _resample_x(width: int, height: int, rgb: bytes,
                out_w: int) -> tuple[int, int, bytes]:
    taps = _cubic_taps(width, out_w)
    stride = width * 3
    out = bytearray(out_w * height * 3)
    o = 0
    for y in range(height):
        base = y * stride
        for start, weights in taps:
            p = base + start * 3
            r = g = b = 0
            for weight in weights:
                r += weight * rgb[p]
                g += weight * rgb[p + 1]
                b += weight * rgb[p + 2]
                p += 3
            out[o] = _clamp8(r)
            out[o + 1] = _clamp8(g)
            out[o + 2] = _clamp8(b)
            o += 3
    return out_w, height, bytes(out)


def _resample_y(width: int, height: int, rgb: bytes,
                out_h: int) -> tuple[int, int, bytes]:
    taps = _cubic_taps(height, out_h)
    stride = width * 3
    out = bytearray(stride * out_h)
    o = 0
    for start, weights in taps:
        # Slice the contributing rows once, then walk them column by column:
        # the whole row is one indexable object per tap instead of a multiply
        # per sample.
        rows = [rgb[(start + k) * stride:(start + k + 1) * stride]
                for k in range(len(weights))]
        pairs = list(zip(weights, rows))
        for c in range(stride):
            acc = 0
            for weight, row in pairs:
                acc += weight * row[c]
            out[o] = _clamp8(acc)
            o += 1
    return width, out_h, bytes(out)


def resize_rgb(width: int, height: int, rgb: bytes,
               max_width: int = DEFAULT_MAX_WIDTH,
               max_height: int = DEFAULT_MAX_HEIGHT) -> tuple[int, int, bytes]:
    """Fit the image inside ``max_width`` x ``max_height``, preserving aspect.

    Never upscales: a 640x480 desktop stays 640x480 rather than being blown up
    to the box. A non-positive bound means "unbounded on that axis"."""
    scale = 1.0
    if max_width > 0:
        scale = min(scale, max_width / width)
    if max_height > 0:
        scale = min(scale, max_height / height)
    if scale >= 1.0:
        return width, height, rgb

    out_w = max(1, round(width * scale))
    out_h = max(1, round(height * scale))
    kx, ky = max(1, width // out_w), max(1, height // out_h)
    if kx > 1 or ky > 1:
        width, height, rgb = _box_reduce(width, height, rgb, kx, ky)
    if width != out_w:
        width, height, rgb = _resample_x(width, height, rgb, out_w)
    if height != out_h:
        width, height, rgb = _resample_y(width, height, rgb, out_h)
    return width, height, rgb


def sharpen_rgb(width: int, height: int, rgb: bytes,
                amount: float = DEFAULT_SHARPEN) -> bytes:
    """A gentle unsharp pass: ``p + amount * (p - neighbours)`` over the
    5-point Laplacian.

    Any honest downscale is a low-pass filter, so the result is softer than the
    screen it came from — window borders and text edges lose the contrast that
    made them readable at a glance. A small amount of sharpening puts that edge
    energy back; a large amount would ring around every glyph, which is why the
    default is deliberately timid. The one-pixel border keeps its original
    values (no neighbours to work with) — invisible, and cheaper than
    reflecting the edges."""
    if amount <= 0 or width < 3 or height < 3:
        return rgb
    weight = int(round(amount * 256))
    center = 256 + 4 * weight
    stride = width * 3
    out = bytearray(rgb)
    for y in range(1, height - 1):
        base = y * stride
        for i in range(base + 3, base + stride - 3):
            value = (center * rgb[i]
                     - weight * (rgb[i - 3] + rgb[i + 3]
                                 + rgb[i - stride] + rgb[i + stride])
                     + 128) >> 8
            out[i] = 0 if value < 0 else (255 if value > 255 else value)
    return bytes(out)


def _png_chunk(tag: bytes, data: bytes) -> bytes:
    return (struct.pack(">I", len(data)) + tag + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))


def encode_png(width: int, height: int, rgb: bytes) -> bytes:
    """Pack RGB into a PNG (8-bit truecolour, filter 0 on every row).

    Deliberately dependency-free: a thumbnail is not worth adding Pillow to an
    image that also carries the SSH server and the operator. Filter 0 costs
    some compression versus an adaptive filter, and zlib does the rest."""
    stride = width * 3
    if len(rgb) != stride * height:
        raise ScreenshotError(
            f"rgb buffer is {len(rgb)} bytes, expected {stride * height}")
    raw = bytearray()
    for y in range(height):
        raw.append(0)
        raw += rgb[y * stride:(y + 1) * stride]
    return (b"\x89PNG\r\n\x1a\n"
            + _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + _png_chunk(b"IDAT", zlib.compress(bytes(raw), 6))
            + _png_chunk(b"IEND", b""))


def render_screenshot(gzipped_xwd: bytes, max_width: int = DEFAULT_MAX_WIDTH,
                      max_height: int = DEFAULT_MAX_HEIGHT,
                      sharpen: float = DEFAULT_SHARPEN) -> bytes:
    """gzipped xwd dump -> resized, sharpened PNG. CPU-bound in pure Python
    (a 1080p grab is a few hundred milliseconds of decode plus the resize), so
    callers run it in an executor and never on the portal's event loop."""
    width, height, rgb = xwd_to_rgb(gunzip(gzipped_xwd))
    width, height, rgb = resize_rgb(width, height, rgb, max_width, max_height)
    rgb = sharpen_rgb(width, height, rgb, sharpen)
    return encode_png(width, height, rgb)


# --------------------------------------------------------------------------- #
# Grab commands + transports                                                   #
# --------------------------------------------------------------------------- #

def grab_script(display: str = DEFAULT_DISPLAY) -> str:
    """The shell one-liner run inside the session. ``-silent`` keeps xwd from
    beeping the display; ``gzip -1`` is worth it because the raw dump is
    width*height*4 bytes (~8 MB at 1080p) and a desktop framebuffer is mostly
    flat colour — cheap CPU for a large cut in bytes over the exec/SSH channel."""
    if not _DISPLAY_RE.match(display):
        raise ScreenshotError(f"refusing suspicious DISPLAY {display!r}")
    return f"DISPLAY={display} xwd -root -silent | gzip -1"


def build_pod_grab_command(pod_name: str, namespace: str,
                           display: str = DEFAULT_DISPLAY,
                           container: str = STREAMER_CONTAINER) -> list[str]:
    """``kubectl exec`` argv for a one-shot grab. No ``-it``: this is a binary
    pipe, and a TTY would mangle it."""
    return ["kubectl", "exec", pod_name, "-n", namespace, "-c", container, "--",
            "sh", "-c", grab_script(display)]


async def capture_pod(pod_name: str, namespace: str,
                      display: str = DEFAULT_DISPLAY) -> bytes:
    cmd = build_pod_grab_command(pod_name, namespace, display)
    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(process.communicate(), _GRAB_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise ScreenshotError(f"grab in {pod_name} timed out")
    if process.returncode != 0:
        raise ScreenshotError(
            f"kubectl exec {pod_name} rc={process.returncode}: "
            f"{err.decode('utf-8', 'replace').strip()[:200]}")
    return out


async def capture_vm(host: str, username: str, private_key_pem: str,
                     display: str = DEFAULT_DISPLAY) -> bytes:
    """Grab over SSH into the guest. Unlike the web terminal there is no retry
    budget: a VM whose sshd isn't up yet simply has no screenshot this pass."""
    import asyncssh  # deferred: only VM sessions need an SSH client

    key = asyncssh.import_private_key(private_key_pem)
    try:
        async with asyncssh.connect(
            host, username=username, client_keys=[key],
            # config=None for the same reason as the relay: the portal's own
            # connections must not be steerable by a ~/.ssh/config in its home.
            known_hosts=None, config=None, connect_timeout=10,
        ) as conn:
            result = await asyncio.wait_for(
                conn.run(grab_script(display), encoding=None, check=False),
                _GRAB_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        raise ScreenshotError(f"grab on {host} timed out")
    except (OSError, asyncssh.Error) as e:
        raise ScreenshotError(f"ssh to {host} failed: {e}") from e
    if result.exit_status != 0:
        stderr = (result.stderr or b"").decode("utf-8", "replace").strip()[:200]
        raise ScreenshotError(f"grab on {host} exited {result.exit_status}: {stderr}")
    return result.stdout


# --------------------------------------------------------------------------- #
# Store + capture loop                                                         #
# --------------------------------------------------------------------------- #

class ScreenshotStore:
    """Newest-only PNG per (user, session), in memory.

    There is no history and no disk: keeping older shots would turn a liveness
    aid into a recording of what a user did, with retention and storage
    questions attached. One frame, overwritten, gone when the session is."""

    def __init__(self):
        self._shots: dict[tuple[str, str], tuple[float, bytes]] = {}

    def put(self, user: str, name: str, png: bytes) -> None:
        self._shots[(user, name)] = (time.time(), png)

    def get(self, user: str, name: str) -> tuple[float, bytes] | None:
        """``(captured_at, png)`` or None if this session has none yet."""
        return self._shots.get((user, name))

    def keep_only(self, keys) -> int:
        """Drop shots whose session no longer exists. Returns the number
        dropped."""
        stale = set(self._shots) - set(keys)
        for key in stale:
            del self._shots[key]
        return len(stale)

    def __len__(self) -> int:
        return len(self._shots)


# Process-wide: the viewer app (aiohttp) runs the capture loop and serves
# /screenshot/<id>, the management app (FastAPI) renders the thumbnails, and
# whistler.portal.__main__ runs both in one event loop.
STORE = ScreenshotStore()


def settings() -> tuple[int, int, int, str]:
    """``(interval_seconds, max_width, max_height, display)`` from the
    environment. A non-positive interval disables capture entirely."""
    def _int(name: str, default: int) -> int:
        try:
            return int(os.environ.get(name, default))
        except ValueError:
            return default

    interval = _int("WHISTLER_SCREENSHOT_INTERVAL", DEFAULT_INTERVAL_SECONDS)
    max_width = _int("WHISTLER_SCREENSHOT_WIDTH", DEFAULT_MAX_WIDTH)
    max_height = _int("WHISTLER_SCREENSHOT_HEIGHT", DEFAULT_MAX_HEIGHT)
    display = os.environ.get("WHISTLER_SCREENSHOT_DISPLAY", DEFAULT_DISPLAY)
    return interval, max(1, max_width), max(1, max_height), display


async def _capture_one(cm, store: ScreenshotStore, session: dict,
                       display: str, max_width: int, max_height: int,
                       semaphore: asyncio.Semaphore) -> bool:
    user, name = session["user"], session["name"]
    loop = asyncio.get_running_loop()

    # The channel grant is checked at *capture*, not at serve time, and that
    # is the point: a session whose owner isn't granted the screenshots
    # channel never has its display read at all, so the image never enters
    # portal memory. Gating only the HTTP route would still have carried the
    # pixels out of the zone (design/security.md, "Access channels").
    channels = await loop.run_in_executor(None, cm.session_channels, user, name)
    if CHANNEL_SCREENSHOTS not in channels:
        logger.debug(f"screenshot {user}/{name}: screenshots channel not granted")
        return False

    async with semaphore:
        try:
            if session.get("runtime") == "vm":
                address, private_key = await asyncio.gather(
                    loop.run_in_executor(None, cm.get_vmi_address, user, name),
                    loop.run_in_executor(None, cm.get_vm_access_private_key, user),
                )
                if not address:
                    raise ScreenshotError("VM has no address yet")
                if not private_key:
                    raise ScreenshotError("no VM access key for this user")
                blob = await capture_vm(address, user, private_key, display)
            else:
                if not session.get("podName"):
                    raise ScreenshotError("session has no pod")
                blob = await capture_pod(session["podName"], session["namespace"], display)
            png = await loop.run_in_executor(
                None, render_screenshot, blob, max_width, max_height)
        except ScreenshotError as e:
            # Expected and frequent (booting guest, session going away
            # mid-pass); debug so a busy cluster doesn't fill the log.
            logger.debug(f"screenshot {user}/{name}: {e}")
            return False
        except Exception as e:
            logger.warning(f"screenshot {user}/{name} failed: {e}")
            return False
    store.put(user, name, png)
    return True


async def capture_all(cm, store: ScreenshotStore, *, display: str,
                      max_width: int, max_height: int) -> int:
    """One pass over every desktop session in the cluster. Returns the number
    of screenshots taken."""
    loop = asyncio.get_running_loop()
    sessions = await loop.run_in_executor(None, cm.list_all_desktop_sessions)
    # Prune against *every* session, not just the Ready ones: a session that is
    # briefly restarting should keep its last screenshot rather than flicker.
    store.keep_only((s["user"], s["name"]) for s in sessions)

    ready = [s for s in sessions if s.get("phase") == "Ready"]
    if not ready:
        return 0
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_GRABS)
    results = await asyncio.gather(*(
        _capture_one(cm, store, s, display, max_width, max_height, semaphore)
        for s in ready
    ))
    return sum(results)


async def run_forever(cm, store: ScreenshotStore, *, interval: int,
                      display: str, max_width: int, max_height: int) -> None:
    while True:
        started = time.monotonic()
        try:
            taken = await capture_all(cm, store, display=display,
                                      max_width=max_width, max_height=max_height)
            logger.info(f"Screenshot pass: {taken} captured, {len(store)} held "
                        f"({time.monotonic() - started:.1f}s)")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Screenshot pass failed: {e}")
        await asyncio.sleep(interval)


def screenshot_ctx(app):
    """aiohttp cleanup_ctx: run the capture loop for the life of the app."""
    interval, max_width, max_height, display = settings()
    if interval <= 0:
        logger.info("Session screenshots disabled (WHISTLER_SCREENSHOT_INTERVAL<=0)")

        async def _noop(_app):
            yield

        return _noop(app)

    async def _ctx(app):
        logger.info(f"Session screenshots: every {interval}s, "
                    f"max {max_width}x{max_height}px, DISPLAY={display}")
        # The store is per-process. With more than one replica each one grabs
        # every session on its own schedule and answers /screenshot only for
        # what it captured, so thumbnails appear and vanish as requests are
        # balanced around — and every session gets grabbed N times.
        try:
            replicas = int(os.environ.get("PORTAL_REPLICAS", "1"))
        except ValueError:
            replicas = 1
        if replicas > 1:
            logger.warning(
                f"Session screenshots are in-memory per portal process, but "
                f"portal.replicaCount is {replicas}: each replica will grab "
                f"every session and serve only its own captures. Run one "
                f"portal replica, or disable screenshots.")
        task = asyncio.create_task(
            run_forever(app["cm"], STORE, interval=interval, display=display,
                        max_width=max_width, max_height=max_height))
        yield
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    return _ctx(app)
