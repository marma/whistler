"""whistler.portal.totp — checked against the RFCs' own test vectors.

The point of these is narrow and worth stating: the second factor's *algorithm*
is not a thing Whistler invents or approximates. If these vectors pass, any
authenticator app in the world agrees with this code, which is what makes the
kiosk's OTP screen a mock of the *storage and policy* only.
"""
import pytest

from whistler.portal import totp

# RFC 4226 appendix D / RFC 6238 appendix B both use this key.
KEY = b"12345678901234567890"


def test_rfc4226_hotp_vectors():
    """Appendix D, counters 0..9."""
    assert [totp.hotp(KEY, c) for c in range(10)] == [
        "755224", "287082", "359152", "969429", "338314",
        "254676", "287922", "162583", "399871", "520489"]


@pytest.mark.parametrize("at,expected", [
    (59,          "94287082"),
    (1111111109,  "07081804"),
    (1111111111,  "14050471"),
    (1234567890,  "89005924"),
    (2000000000,  "69279037"),
    (20000000000, "65353130"),
])
def test_rfc6238_totp_vectors(at, expected):
    """Appendix B, SHA-1, 8 digits — the same function the 6-digit default uses."""
    assert totp.code(KEY, at=at, digits=8) == expected


def test_a_code_is_accepted_one_step_either_side_of_now():
    """Phone clocks drift; ~90s of tolerance is the standard answer, and the
    step beyond it must still be refused or the window is meaningless."""
    now = 1700000000
    assert totp.verify(KEY, totp.code(KEY, at=now), at=now)
    assert totp.verify(KEY, totp.code(KEY, at=now - 30), at=now)
    assert totp.verify(KEY, totp.code(KEY, at=now + 30), at=now)
    assert not totp.verify(KEY, totp.code(KEY, at=now - 90), at=now)


def test_verify_counter_returns_what_replay_prevention_needs():
    """A code stays valid for its whole step, so the only defence against reuse
    is remembering the counter that was accepted. verify() hides it; this is the
    call a real implementation makes."""
    now = 1700000000
    assert totp.verify_counter(KEY, totp.code(KEY, at=now), at=now) == now // 30
    assert totp.verify_counter(KEY, "000000", at=now) is None


@pytest.mark.parametrize("given", ["", None, "12345", "1234567", "abcdef",
                                   "12 34 5", "٠١٢٣٤٥"])
def test_malformed_input_is_refused_rather_than_raising(given):
    """This field is fed by whatever a browser posts. Note the last case: Arabic
    digits are str.isdigit() True but not what the arithmetic means, and int()
    would happily convert them — the length-and-isdigit check on the *string*
    is what keeps that from becoming an exception in a login handler."""
    assert totp.verify(KEY, given) is False


def test_a_typed_key_is_accepted_the_way_people_type_it():
    """Apps show the key grouped in fours, lowercase happens, and the padding
    gets left off. All three have to decode to the same bytes or manual entry
    fails for reasons nobody can see."""
    secret = totp.generate_secret()
    grouped = " ".join(secret[i:i + 4] for i in range(0, len(secret), 4))
    assert totp.decode_secret(grouped) == totp.decode_secret(secret)
    assert totp.decode_secret(secret.lower()) == totp.decode_secret(secret)
    assert totp.decode_secret(secret + "======") == totp.decode_secret(secret)


def test_garbage_decodes_to_nothing_instead_of_raising():
    assert totp.decode_secret("not base32!") == b""
    assert totp.decode_secret(None) == b""


def test_generate_secret_is_160_bits_of_fresh_randomness():
    a, b = totp.generate_secret(), totp.generate_secret()
    assert a != b
    assert len(totp.decode_secret(a)) == totp.SECRET_BYTES


def test_provisioning_uri_states_every_parameter():
    """An app that assumes a different period or digit count produces codes that
    are always wrong with nothing on screen to explain it, so nothing is left to
    the default — and the issuer appears both in the label and as a parameter,
    because old apps read the first and current ones the second."""
    uri = totp.provisioning_uri("JBSWY3DPEHPK3PXP", "alice")
    assert uri.startswith("otpauth://totp/Whistler%3Aalice?")
    for part in ("secret=JBSWY3DPEHPK3PXP", "issuer=Whistler",
                 "algorithm=SHA1", "digits=6", "period=30"):
        assert part in uri


def test_provisioning_uri_escapes_a_name_that_would_break_it():
    """Usernames are not URL-safe by nature; an unescaped one silently corrupts
    the QR's parameters rather than failing."""
    uri = totp.provisioning_uri("AAAA", "a b&c?d/e")
    assert "a b" not in uri and "&c" not in uri.split("?", 1)[1].split("&")[0]
    assert uri.count("?") == 1
