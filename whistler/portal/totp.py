"""RFC 6238 TOTP, in the standard library.

This exists to make one point concretely: the second factor is not a product
you integrate, it is two RFCs and about forty lines. **Nothing here is tied to
Google Authenticator.** The algorithm is HOTP (RFC 4226) counted in 30-second
steps (TOTP, RFC 6238), and the QR is a plain URI in Google's ``otpauth://``
key-uri-format — a de facto standard every authenticator reads: Google
Authenticator, Microsoft Authenticator, Authy, 1Password, Bitwarden, Aegis,
FreeOTP, KeePassXC, Yubico Authenticator. A user picks their app; the server
never knows which one.

So the only real decisions are storage and replay:

* **Storage.** The shared secret is a bearer credential for the account's second
  factor — 20 random bytes, base32-encoded for humans and QRs. It belongs in a
  Kubernetes Secret in Whistler's own namespace, not on the ``User`` CR
  (design/security.md), and it is written once at enrolment and never shown
  again: an endpoint that re-displays a secret is a second way to enrol.
* **Replay.** A code is valid for its whole 30-second step (plus the skew
  window below), so a correct implementation records the last counter accepted
  per user and refuses anything at or below it. There is nowhere to record that
  yet, which is one of the reasons the kiosk's OTP step is still a mock — see
  ``whistler/portal/kiosk.py``.

``verify`` accepts one step either side of now (``window=1``, ~90s of tolerance)
because phone clocks drift; widening that is the whole knob this has.
"""
import base64
import binascii
import hmac
import secrets
import struct
import time
from urllib.parse import quote

# The defaults every authenticator app assumes when the URI omits them. They are
# spelled out in the provisioning URI anyway: an app that silently disagrees
# with the server about period or digits produces codes that are always wrong,
# with nothing on screen to say why.
DIGITS = 6
PERIOD = 30
ALGORITHM = "sha1"          # not a weakness here: HMAC-SHA1, and it is what
                            # every app supports. SHA256 is optional and often
                            # ignored by the app despite the URI asking for it.
SECRET_BYTES = 20           # RFC 4226 s4 R6: 160 bits, one SHA-1 block


def generate_secret() -> str:
    """A fresh base32 secret, unpadded — the form QRs and manual entry use."""
    return base64.b32encode(secrets.token_bytes(SECRET_BYTES)).decode().rstrip("=")


def decode_secret(secret: str) -> bytes:
    """base32 -> bytes, tolerating what a person types: lowercase, spaces (apps
    group the key in fours), and the padding they will leave off."""
    cleaned = (secret or "").replace(" ", "").replace("-", "").upper()
    # Strip any padding the person copied along before re-deriving it: b32decode
    # wants exactly the right amount, and "already padded" is otherwise a
    # decode failure that looks like a wrong key.
    cleaned = cleaned.rstrip("=")
    cleaned += "=" * (-len(cleaned) % 8)
    try:
        return base64.b32decode(cleaned)
    except (binascii.Error, ValueError):
        return b""


def hotp(key: bytes, counter: int, digits: int = DIGITS,
         algorithm: str = ALGORITHM) -> str:
    """RFC 4226: HMAC of the counter, dynamically truncated to `digits`."""
    mac = hmac.new(key, struct.pack(">Q", counter), algorithm).digest()
    offset = mac[-1] & 0x0F
    code = struct.unpack(">I", mac[offset:offset + 4])[0] & 0x7FFFFFFF
    return str(code % 10 ** digits).zfill(digits)


def counter_at(at: float = None, period: int = PERIOD) -> int:
    return int((time.time() if at is None else at) // period)


def code(key: bytes, at: float = None, digits: int = DIGITS,
         period: int = PERIOD, algorithm: str = ALGORITHM) -> str:
    """The code an app would be showing at `at` (default: now)."""
    return hotp(key, counter_at(at, period), digits, algorithm)


def verify(key: bytes, given: str, at: float = None, digits: int = DIGITS,
           period: int = PERIOD, algorithm: str = ALGORITHM,
           window: int = 1) -> bool:
    """Whether `given` is a live code for `key`.

    Every comparison goes through ``compare_digest`` and every step in the
    window is tried, so the answer's timing says nothing about which step (or
    which leading digits) matched. Returns the boolean only: a caller that
    needs the counter for replay tracking wants ``verify_counter``."""
    return verify_counter(key, given, at, digits, period, algorithm,
                          window) is not None


def verify_counter(key: bytes, given: str, at: float = None,
                   digits: int = DIGITS, period: int = PERIOD,
                   algorithm: str = ALGORITHM, window: int = 1):
    """As ``verify``, but returns the counter that matched, or None.

    This is the shape a real implementation needs: store the returned counter
    and refuse anything <= the stored one, or a code shoulder-surfed inside its
    own 30-second step is replayable."""
    cleaned = (given or "").strip().replace(" ", "")
    # isascii() before isdigit(): "٠١٢٣٤٥" is six digits by str.isdigit and is
    # not what hmac.compare_digest accepts, so without this a login field turns
    # a wrong code into a TypeError out of the handler.
    if len(cleaned) != digits or not (cleaned.isascii() and cleaned.isdigit()):
        return None
    now = counter_at(at, period)
    match = None
    for step in range(-window, window + 1):
        if hmac.compare_digest(hotp(key, now + step, digits, algorithm), cleaned):
            match = now + step
    return match


def provisioning_uri(secret: str, account: str, issuer: str = "Whistler",
                     digits: int = DIGITS, period: int = PERIOD,
                     algorithm: str = ALGORITHM) -> str:
    """The string that goes *inside* the QR code.

    ``otpauth://totp/<issuer>:<account>?secret=...&issuer=...`` — the issuer
    appears twice on purpose: the label prefix is what old apps display, the
    parameter is what current ones read. Everything else is stated rather than
    defaulted, since an app and a server that disagree produce codes that are
    always wrong with nothing on screen to explain it.

    The QR itself is only this text encoded as an image, which is the one piece
    the standard library cannot do — see ``kiosk._mock_qr_svg``."""
    label = quote(f"{issuer}:{account}", safe="")
    params = "&".join([
        f"secret={quote(secret, safe='')}",
        f"issuer={quote(issuer, safe='')}",
        f"algorithm={algorithm.upper()}",
        f"digits={digits}",
        f"period={period}",
    ])
    return f"otpauth://totp/{label}?{params}"
