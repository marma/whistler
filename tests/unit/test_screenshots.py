"""Session screenshots: the pure xwd -> PNG path plus the grab command shapes
(whistler.portal.screenshots). No cluster, no X server.

The header layout asserted here is a real ``xwd -root`` dump from Xvfb
(24-bit depth, ZPixmap, 32bpp, LSBFirst byte order, MSB-first header, a
256-entry colormap present despite the TrueColor visual).
"""
import gzip
import struct
import zlib

import pytest

from whistler.config import CHANNELS, CHANNEL_SCREENSHOTS
from whistler.portal import screenshots
from whistler.portal.screenshots import ScreenshotError, ScreenshotStore


# --------------------------------------------------------------------------- #
# Fixture builder: a synthetic xwd dump matching what Xvfb produces            #
# --------------------------------------------------------------------------- #

_WINDOW_NAME = b"root\x00"


def make_xwd(width, height, pixels, *, bits_per_pixel=32, byte_order=0,
             ncolors=256, header_order=">", bytes_per_line=None,
             masks=(0xFF0000, 0x00FF00, 0x0000FF), pixmap_format=2,
             truncate_pixels=False):
    """Build an xwd dump. ``pixels`` is a list of rows of (r, g, b) tuples."""
    stride = bits_per_pixel // 8
    if bytes_per_line is None:
        bytes_per_line = width * stride
    header_size = screenshots._XWD_HEADER_BYTES + len(_WINDOW_NAME)
    red_mask, green_mask, blue_mask = masks

    def shift_of(mask):
        return (mask & -mask).bit_length() - 1

    header = struct.pack(
        f"{header_order}25I",
        header_size, screenshots._XWD_FILE_VERSION, pixmap_format, 24,
        width, height, 0, byte_order, 32, 0, 32, bits_per_pixel,
        bytes_per_line, 4, red_mask, green_mask, blue_mask, 8,
        ncolors, ncolors, width, height, 0, 0, 0,
    )
    body = bytearray()
    for row in pixels:
        raw = bytearray(bytes_per_line)
        for x, (r, g, b) in enumerate(row):
            value = ((r << shift_of(red_mask)) | (g << shift_of(green_mask))
                     | (b << shift_of(blue_mask)))
            raw[x * stride:(x + 1) * stride] = value.to_bytes(
                stride, "little" if byte_order == 0 else "big")
        body += raw
    if truncate_pixels:
        body = body[:-1]
    return (header + _WINDOW_NAME
            + b"\x00" * (ncolors * screenshots._XWD_COLOR_ENTRY_BYTES)
            + bytes(body))


def solid(width, height, color):
    return [[color] * width for _ in range(height)]


def parse_png(png):
    """(width, height, [chunk tags]) from a PNG, verifying every chunk CRC."""
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    tags, offset, size = [], 8, None
    while offset < len(png):
        length = struct.unpack(">I", png[offset:offset + 4])[0]
        tag = png[offset + 4:offset + 8]
        data = png[offset + 8:offset + 8 + length]
        crc = struct.unpack(">I", png[offset + 8 + length:offset + 12 + length])[0]
        assert crc == zlib.crc32(tag + data) & 0xFFFFFFFF, f"bad CRC on {tag}"
        if tag == b"IHDR":
            size = struct.unpack(">II", data[:8])
        tags.append(tag)
        offset += 12 + length
    return size[0], size[1], tags


def png_pixels(png, width, height):
    """Decode our own filter-0 PNG back to rows of (r, g, b)."""
    idat = b""
    offset = 8
    while offset < len(png):
        length = struct.unpack(">I", png[offset:offset + 4])[0]
        if png[offset + 4:offset + 8] == b"IDAT":
            idat += png[offset + 8:offset + 8 + length]
        offset += 12 + length
    raw = zlib.decompress(idat)
    stride = width * 3
    rows = []
    for y in range(height):
        line = raw[y * (stride + 1):(y + 1) * (stride + 1)]
        assert line[0] == 0, "expected filter type 0"
        rows.append([tuple(line[1 + x * 3:4 + x * 3]) for x in range(width)])
    return rows


# --------------------------------------------------------------------------- #
# xwd decoding                                                                 #
# --------------------------------------------------------------------------- #

def test_decodes_colors_and_extent():
    blob = make_xwd(4, 2, [[(255, 0, 0), (0, 255, 0), (0, 0, 255), (18, 52, 86)]] * 2)
    width, height, rgb = screenshots.xwd_to_rgb(blob)
    assert (width, height) == (4, 2)
    assert rgb[:12] == bytes([255, 0, 0, 0, 255, 0, 0, 0, 255, 18, 52, 86])


def test_row_padding_is_respected():
    """bytes_per_line exceeds width*bpp on real servers; rows must not drift."""
    pixels = [[(10, 20, 30)] * 3, [(40, 50, 60)] * 3]
    blob = make_xwd(3, 2, pixels, bytes_per_line=64)
    _, _, rgb = screenshots.xwd_to_rgb(blob)
    assert rgb[:3] == bytes([10, 20, 30])
    assert rgb[9:12] == bytes([40, 50, 60])


def test_msb_first_pixel_byte_order():
    pixels = [[(1, 2, 3), (4, 5, 6)]]
    lsb = screenshots.xwd_to_rgb(make_xwd(2, 1, pixels, byte_order=0))
    msb = screenshots.xwd_to_rgb(make_xwd(2, 1, pixels, byte_order=1))
    assert lsb == msb


def test_byte_swapped_header_is_detected():
    """xwd normalises the header to MSB-first, but we detect via file_version
    rather than assume — a little-endian header must decode identically."""
    pixels = [[(9, 8, 7), (6, 5, 4)]]
    big = screenshots.xwd_to_rgb(make_xwd(2, 1, pixels, header_order=">"))
    little = screenshots.xwd_to_rgb(make_xwd(2, 1, pixels, header_order="<"))
    assert big == little


def test_24bpp_and_16bpp_visuals():
    packed = screenshots.xwd_to_rgb(
        make_xwd(2, 1, [[(255, 128, 0), (0, 0, 0)]], bits_per_pixel=24))
    assert packed[2][:3] == bytes([255, 128, 0])

    # 5/6/5: full-scale channels must widen to full-scale 8-bit, not 248/252.
    _, _, rgb = screenshots.xwd_to_rgb(
        make_xwd(1, 1, [[(31, 63, 31)]], bits_per_pixel=16,
                 masks=(0xF800, 0x07E0, 0x001F)))
    assert rgb == bytes([255, 255, 255])


def test_colormap_offset_is_skipped():
    """The XWDColor table sits between header and pixels even for TrueColor;
    a wrong ncolors offset would read colormap bytes as pixels."""
    pixels = [[(200, 100, 50)]]
    for ncolors in (0, 256):
        _, _, rgb = screenshots.xwd_to_rgb(
            make_xwd(1, 1, pixels, ncolors=ncolors))
        assert rgb == bytes([200, 100, 50])


def test_decoding_keeps_the_full_resolution():
    """Decode no longer subsamples: scaling is resize_rgb's job, over every
    pixel. If this starts shrinking again the thumbnails are back to nearest
    neighbour without anyone editing the resampler."""
    blob = make_xwd(1920, 4, solid(1920, 4, (1, 2, 3)))
    width, height, rgb = screenshots.xwd_to_rgb(blob)
    assert (width, height) == (1920, 4)
    assert len(rgb) == 1920 * 4 * 3


def test_the_fast_and_generic_decode_paths_agree(monkeypatch):
    """A depth-24 dump takes the slice-assignment path; anything else falls
    back to per-pixel unpacking. The two must produce the same bytes."""
    pixels = [[(255, 0, 0), (0, 255, 0), (7, 8, 9)],
              [(0, 0, 255), (18, 52, 86), (255, 255, 255)]]
    for bits_per_pixel, byte_order in ((32, 0), (32, 1), (24, 0), (24, 1)):
        blob = make_xwd(3, 2, pixels, bits_per_pixel=bits_per_pixel,
                        byte_order=byte_order)
        assert screenshots._fast_channel_offsets(
            bits_per_pixel, byte_order, (0xFF0000, 0x00FF00, 0x0000FF))
        fast = screenshots.xwd_to_rgb(blob)
        monkeypatch.setattr(screenshots, "_fast_channel_offsets",
                            lambda *a, **k: None)
        generic = screenshots.xwd_to_rgb(blob)
        monkeypatch.undo()
        assert fast == generic


def test_a_16bpp_visual_has_no_fast_path():
    assert screenshots._fast_channel_offsets(16, 0, (0xF800, 0x07E0, 0x001F)) is None


@pytest.mark.parametrize("blob, message", [
    (b"", "truncated"),
    (b"\x00" * 100, "file_version"),
])
def test_malformed_headers_rejected(blob, message):
    with pytest.raises(ScreenshotError, match=message):
        screenshots.xwd_to_rgb(blob)


def test_truncated_pixel_data_rejected():
    blob = make_xwd(4, 4, solid(4, 4, (0, 0, 0)), truncate_pixels=True)
    with pytest.raises(ScreenshotError, match="truncated"):
        screenshots.xwd_to_rgb(blob)


def test_non_zpixmap_rejected():
    blob = make_xwd(2, 2, solid(2, 2, (0, 0, 0)), pixmap_format=1)
    with pytest.raises(ScreenshotError, match="pixmap_format"):
        screenshots.xwd_to_rgb(blob)


# --------------------------------------------------------------------------- #
# PNG encoding + the end-to-end render                                         #
# --------------------------------------------------------------------------- #

def test_encode_png_roundtrip():
    rows = [[(255, 0, 0), (0, 255, 0)], [(0, 0, 255), (7, 8, 9)]]
    flat = bytes(v for row in rows for px in row for v in px)
    png = screenshots.encode_png(2, 2, flat)
    assert parse_png(png) == (2, 2, [b"IHDR", b"IDAT", b"IEND"])
    assert png_pixels(png, 2, 2) == rows


def test_encode_png_rejects_wrong_buffer_size():
    with pytest.raises(ScreenshotError, match="expected"):
        screenshots.encode_png(4, 4, b"\x00" * 10)


def test_render_screenshot_end_to_end():
    blob = make_xwd(1280, 720, solid(1280, 720, (12, 34, 56)))
    png = screenshots.render_screenshot(gzip.compress(blob))
    width, height, _ = parse_png(png)
    assert (width, height) == (960, 540)
    # Flat in, flat out: the resampler's weights sum to exactly one and the
    # unsharp kernel sums to one, so neither step may tint a solid colour.
    assert png_pixels(png, width, height)[0][0] == (12, 34, 56)


# --------------------------------------------------------------------------- #
# Resize + sharpen                                                             #
# --------------------------------------------------------------------------- #

def flat(width, height, color):
    return bytes(color) * (width * height)


def px(width, rgb, x, y):
    i = (y * width + x) * 3
    return tuple(rgb[i:i + 3])


@pytest.mark.parametrize("src, expected", [
    ((1920, 1080), (960, 540)),      # exact 2x: the box reduce lands on target
    ((3840, 2160), (960, 540)),
    ((1280, 720), (960, 540)),       # 0.75: box reduce cannot help, cubic does
    ((1600, 1200), (720, 540)),      # 4:3 is bounded by the height, not the width
    ((1366, 768), (960, 540)),
    ((640, 480), (640, 480)),        # already inside the box: untouched
])
def test_resize_fits_the_box_and_keeps_the_aspect(src, expected):
    width, height = src
    out_w, out_h, rgb = screenshots.resize_rgb(
        width, height, flat(width, height, (9, 9, 9)), 960, 540)
    assert (out_w, out_h) == expected
    assert len(rgb) == out_w * out_h * 3
    assert set(rgb) == {9}


def test_resize_never_upscales():
    out = screenshots.resize_rgb(320, 200, flat(320, 200, (1, 2, 3)), 960, 540)
    assert out[:2] == (320, 200)


def test_one_pixel_detail_survives_as_grey_rather_than_vanishing():
    """The whole point of dropping nearest neighbour. Alternating black and
    white columns at 2x down must average to grey; a nearest-neighbour resize
    would return an image that is entirely one colour or the other, so a
    scrollbar or a line of text would flicker in and out between passes."""
    width, height = 64, 8
    row = [((255, 255, 255) if x % 2 else (0, 0, 0)) for x in range(width)]
    rgb = bytes(v for _ in range(height) for c in row for v in c)
    out_w, out_h, small = screenshots.resize_rgb(width, height, rgb, 32, 32)
    assert (out_w, out_h) == (32, 4)
    assert all(120 <= v <= 135 for v in small), sorted(set(small))


def test_box_reduce_averages_the_block():
    # 2x2 of 0/10/20/30 in every channel -> 15
    rgb = bytes([0, 0, 0, 10, 10, 10, 20, 20, 20, 30, 30, 30])
    assert screenshots._box_reduce(2, 2, rgb, 2, 2) == (1, 1, bytes([15, 15, 15]))


def test_box_reduce_crops_the_partial_block():
    rgb = flat(5, 3, (4, 4, 4))
    assert screenshots._box_reduce(5, 3, rgb, 2, 2)[:2] == (2, 1)


@pytest.mark.parametrize("src, dst", [(1920, 960), (1280, 960), (10, 3), (3, 3)])
def test_cubic_weights_sum_to_one(src, dst):
    for start, weights in screenshots._cubic_taps(src, dst):
        assert sum(weights) == screenshots._WEIGHT_SCALE
        assert 0 <= start and start + len(weights) <= src


def test_cubic_resample_stays_in_range_on_a_hard_edge():
    """Catmull-Rom undershoots and overshoots at a step edge; the accumulator
    is clamped, so a black/white boundary must not wrap around to the other
    end of the range."""
    width = 32
    row = [(0, 0, 0)] * (width // 2) + [(255, 255, 255)] * (width // 2)
    rgb = bytes(v for c in row for v in c)
    _, _, out = screenshots._resample_x(width, 1, rgb, 24)
    assert min(out) == 0 and max(out) == 255


def test_sharpen_leaves_a_flat_field_alone():
    rgb = flat(8, 8, (60, 120, 180))
    assert screenshots.sharpen_rgb(8, 8, rgb) == rgb


def test_sharpen_raises_edge_contrast():
    width = height = 7
    rows = [[(128, 128, 128)] * width for _ in range(height)]
    for y in range(height):
        for x in range(width // 2, width):
            rows[y][x] = (200, 200, 200)
    rgb = bytes(v for row in rows for c in row for v in c)
    out = screenshots.sharpen_rgb(width, height, rgb)
    dark = px(width, out, width // 2 - 1, height // 2)
    light = px(width, out, width // 2, height // 2)
    assert dark[0] < 128, "the dark side of the edge should be pushed down"
    assert light[0] > 200, "the light side should be pushed up"


def test_sharpen_off_is_a_no_op():
    rgb = flat(8, 8, (1, 2, 3))
    assert screenshots.sharpen_rgb(8, 8, rgb, amount=0) is rgb


def test_render_rejects_non_gzip():
    with pytest.raises(ScreenshotError, match="not gzip"):
        screenshots.render_screenshot(b"plain bytes, no gzip header")


def test_render_refuses_zip_bomb():
    bomb = gzip.compress(b"\x00" * (2 * 1024 * 1024))
    with pytest.raises(ScreenshotError, match="expands past"):
        screenshots.gunzip(bomb, limit=1024)


# --------------------------------------------------------------------------- #
# Grab commands                                                                #
# --------------------------------------------------------------------------- #

def test_pod_grab_command_targets_the_streamer_sidecar():
    cmd = screenshots.build_pod_grab_command("alice-desk", "whistler-alice")
    assert cmd[:9] == ["kubectl", "exec", "alice-desk", "-n", "whistler-alice",
                       "-c", "streamer", "--", "sh"]
    assert "-it" not in cmd, "a TTY would corrupt the binary pipe"
    assert "xwd -root -silent" in cmd[-1] and "gzip" in cmd[-1]


def test_grab_script_carries_the_display():
    assert screenshots.grab_script(":7").startswith("DISPLAY=:7 ")


@pytest.mark.parametrize("display", [":0; rm -rf /", "$(id)", "", "0", ":0 -x"])
def test_grab_script_rejects_shell_injection(display):
    with pytest.raises(ScreenshotError, match="suspicious DISPLAY"):
        screenshots.grab_script(display)


# --------------------------------------------------------------------------- #
# Store                                                                        #
# --------------------------------------------------------------------------- #

def test_store_keeps_only_the_newest_shot():
    store = ScreenshotStore()
    store.put("alice", "desk", b"first")
    store.put("alice", "desk", b"second")
    assert len(store) == 1
    assert store.get("alice", "desk")[1] == b"second"


def test_store_is_scoped_per_user():
    store = ScreenshotStore()
    store.put("alice", "desk", b"a")
    store.put("bob", "desk", b"b")
    assert store.get("bob", "desk")[1] == b"b"
    assert store.get("carol", "desk") is None


def test_store_prunes_vanished_sessions():
    store = ScreenshotStore()
    store.put("alice", "desk", b"a")
    store.put("alice", "gone", b"b")
    assert store.keep_only([("alice", "desk")]) == 1
    assert store.get("alice", "gone") is None
    assert store.get("alice", "desk") is not None


# --------------------------------------------------------------------------- #
# Capture pass: which transport each runtime gets, and what a failure costs    #
# --------------------------------------------------------------------------- #

class FakeCM:
    """Enough ConfigManager for the capture loop."""

    def __init__(self, sessions, channels=None):
        self.sessions = sessions
        # {(user, session): {channels}} — anything unlisted gets the full set,
        # which is what an ungrouped user in an unrestricted zone has.
        self.channels = channels or {}

    def list_all_desktop_sessions(self):
        return self.sessions

    def session_channels(self, user, name):
        return self.channels.get((user, name), set(CHANNELS))

    def get_vmi_address(self, user, name):
        return "10.42.0.9"

    def get_vm_access_private_key(self, user):
        return "FAKE-KEY"


def _pod(name, user="alice", phase="Ready"):
    return {"user": user, "name": name, "namespace": f"whistler-{user}",
            "phase": phase, "runtime": "container", "podName": f"{user}-{name}",
            "vmiName": None}


def _vm(name, user="alice", phase="Ready"):
    return {"user": user, "name": name, "namespace": f"whistler-{user}",
            "phase": phase, "runtime": "vm", "podName": None,
            "vmiName": f"{user}-{name}"}


@pytest.fixture
def grabs(monkeypatch):
    """Record which transport each session was grabbed through, and hand back a
    valid one-pixel dump so the render path runs for real."""
    calls = {"pod": [], "vm": []}
    dump = gzip.compress(make_xwd(1, 1, [[(1, 2, 3)]]))

    async def fake_pod(pod_name, namespace, display=screenshots.DEFAULT_DISPLAY):
        calls["pod"].append((pod_name, namespace, display))
        return dump

    async def fake_vm(host, username, key, display=screenshots.DEFAULT_DISPLAY):
        calls["vm"].append((host, username, key, display))
        return dump

    monkeypatch.setattr(screenshots, "capture_pod", fake_pod)
    monkeypatch.setattr(screenshots, "capture_vm", fake_vm)
    return calls


async def test_pods_are_grabbed_via_exec_and_vms_via_ssh(grabs):
    store = ScreenshotStore()
    cm = FakeCM([_pod("desk"), _vm("box")])
    assert await screenshots.capture_all(cm, store, display=":0", max_width=64, max_height=64) == 2
    assert grabs["pod"] == [("alice-desk", "whistler-alice", ":0")]
    assert grabs["vm"] == [("10.42.0.9", "alice", "FAKE-KEY", ":0")]
    assert store.get("alice", "desk")[1][:8] == b"\x89PNG\r\n\x1a\n"


async def test_only_ready_sessions_are_grabbed(grabs):
    store = ScreenshotStore()
    cm = FakeCM([_pod("up"), _pod("booting", phase="Booting"),
                 _vm("stopped", phase="Stopped")])
    assert await screenshots.capture_all(cm, store, display=":0", max_width=64, max_height=64) == 1
    assert grabs["pod"] == [("alice-up", "whistler-alice", ":0")]


async def test_a_session_without_the_screenshots_channel_is_never_grabbed(grabs):
    """Gated at capture, not at serve: the point is that those pixels never
    enter portal memory at all (design/security.md, "Access channels")."""
    store = ScreenshotStore()
    cm = FakeCM([_pod("watched"), _pod("private")],
                channels={("alice", "private"):
                          {c for c in CHANNELS if c != CHANNEL_SCREENSHOTS}})
    assert await screenshots.capture_all(cm, store, display=":0", max_width=64, max_height=64) == 1
    assert grabs["pod"] == [("alice-watched", "whistler-alice", ":0")]
    assert store.get("alice", "private") is None


async def test_a_failing_session_does_not_sink_the_pass(grabs, monkeypatch):
    async def explode(pod_name, namespace, display=screenshots.DEFAULT_DISPLAY):
        if pod_name == "alice-bad":
            raise ScreenshotError("no X display")
        return gzip.compress(make_xwd(1, 1, [[(4, 5, 6)]]))

    monkeypatch.setattr(screenshots, "capture_pod", explode)
    store = ScreenshotStore()
    cm = FakeCM([_pod("bad"), _pod("good")])
    assert await screenshots.capture_all(cm, store, display=":0", max_width=64, max_height=64) == 1
    assert store.get("alice", "bad") is None
    assert store.get("alice", "good") is not None


async def test_pass_prunes_sessions_that_are_gone_but_keeps_unready_ones(grabs):
    store = ScreenshotStore()
    store.put("alice", "deleted", b"old")
    store.put("alice", "restarting", b"old")
    cm = FakeCM([_pod("restarting", phase="Booting")])
    await screenshots.capture_all(cm, store, display=":0", max_width=64, max_height=64)
    assert store.get("alice", "deleted") is None
    # Still listed, just not Ready — its last screenshot survives the pass.
    assert store.get("alice", "restarting")[1] == b"old"


# --------------------------------------------------------------------------- #
# Settings. The stored size is a privacy posture, not a rendering detail:      #
# whatever is stored is served at full stored resolution, so this test exists  #
# to make changing it a deliberate act.                                        #
# --------------------------------------------------------------------------- #

def test_defaults_are_the_half_scale_posture(monkeypatch):
    for var in ("WHISTLER_SCREENSHOT_INTERVAL", "WHISTLER_SCREENSHOT_WIDTH",
                "WHISTLER_SCREENSHOT_HEIGHT", "WHISTLER_SCREENSHOT_DISPLAY"):
        monkeypatch.delenv(var, raising=False)
    interval, max_width, max_height, display = screenshots.settings()
    assert interval == 300
    # Half of a 1080p desktop: window titles and UI text are readable, which
    # is a monitoring posture and documented as one. Raising it further, or
    # dropping it back to the 320px activity overview, is a policy change.
    assert (max_width, max_height) == (960, 540)
    assert display == ":0"


def test_settings_read_the_environment(monkeypatch):
    monkeypatch.setenv("WHISTLER_SCREENSHOT_INTERVAL", "60")
    monkeypatch.setenv("WHISTLER_SCREENSHOT_WIDTH", "1280")
    monkeypatch.setenv("WHISTLER_SCREENSHOT_HEIGHT", "800")
    monkeypatch.setenv("WHISTLER_SCREENSHOT_DISPLAY", ":3")
    assert screenshots.settings() == (60, 1280, 800, ":3")


@pytest.mark.parametrize("value", ["0", "-1"])
def test_non_positive_interval_disables_capture(monkeypatch, value):
    monkeypatch.setenv("WHISTLER_SCREENSHOT_INTERVAL", value)
    assert screenshots.settings()[0] <= 0


def test_garbage_settings_fall_back_to_defaults(monkeypatch):
    monkeypatch.setenv("WHISTLER_SCREENSHOT_INTERVAL", "soon")
    monkeypatch.setenv("WHISTLER_SCREENSHOT_WIDTH", "big")
    monkeypatch.setenv("WHISTLER_SCREENSHOT_HEIGHT", "tall")
    assert screenshots.settings()[:3] == (300, 960, 540)


def test_the_default_box_halves_a_1080p_desktop():
    """1920x1080 is the size the desktop templates run at, and it is the case
    the pipeline is tuned for: an exact 2x box reduce, no cubic pass at all."""
    width, height, _ = screenshots.resize_rgb(
        1920, 1080, b"\x00" * (1920 * 1080 * 3),
        screenshots.DEFAULT_MAX_WIDTH, screenshots.DEFAULT_MAX_HEIGHT)
    assert (width, height) == (960, 540)
