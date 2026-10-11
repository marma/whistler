"""Passwords (whistler/passwords.py) and the signed cookies that make them
mean something (whistler/portal/login.py sign/unsign)."""
from whistler import passwords
from whistler.portal import login


def test_a_hash_verifies_its_password_and_nothing_else():
    encoded = passwords.hash_password("correct horse")
    assert encoded.startswith("scrypt$")
    assert passwords.verify_password("correct horse", encoded)
    assert not passwords.verify_password("correct hors", encoded)
    assert not passwords.verify_password("correct horse", "garbage")


def test_the_same_password_hashes_differently_each_time():
    assert passwords.hash_password("x" * 12) != passwords.hash_password("x" * 12)


def test_a_temporary_password_means_change_until_it_expires():
    record = passwords.new_record("temp-pass-1234", temporary=True, now=1000)
    assert passwords.check(record, "temp-pass-1234", now=1001) == passwords.CHANGE
    expired = 1000 + passwords.TEMPORARY_LIFETIME
    assert passwords.check(record, "temp-pass-1234", now=expired) == passwords.REFUSED
    assert passwords.status(record, now=1001) == "temporary"
    assert passwords.status(record, now=expired) == "expired"


def test_a_chosen_password_does_not_expire():
    record = passwords.new_record("a-real-password", temporary=False, now=0)
    assert record["expiresAt"] is None
    assert passwords.check(record, "a-real-password", now=10 ** 10) == passwords.OK
    assert passwords.status(record) == "set"


def test_no_record_is_no_sign_in():
    assert passwords.check(None, "anything") == passwords.REFUSED
    assert passwords.status(None) == "none"


def test_an_empty_password_never_matches():
    record = passwords.new_record("", temporary=False)
    assert passwords.check(record, "") == passwords.REFUSED


def test_the_secret_round_trips_the_record():
    record = passwords.new_record("temp-pass-1234", temporary=True, now=1000)
    assert passwords.from_secret_data(passwords.to_secret_data(record)) == record
    assert passwords.from_secret_data(None) is None
    assert passwords.from_secret_data({"hash": "!!not base64"}) is None


def test_temporary_passwords_are_readable_and_distinct():
    a, b = passwords.generate_temporary(), passwords.generate_temporary()
    assert a != b
    assert len(a) == 19 and a.count("-") == 3
    assert not set(a.replace("-", "")) & set("0O1lI")


def test_new_password_rules():
    ok = "a-real-password"
    assert passwords.new_password_problem("alice", ok, ok) is None
    assert "match" in passwords.new_password_problem("alice", ok, ok + "x")
    assert "at least" in passwords.new_password_problem("alice", "short", "short")
    assert "user name" in passwords.new_password_problem(
        "alicealice", "alicealice", "alicealice")
    assert "different" in passwords.new_password_problem("alice", ok, ok, current=ok)


def test_the_throttle_lets_a_few_mistakes_through_then_backs_off():
    t = passwords.Throttle()
    for _ in range(passwords.Throttle.FREE):
        t.failed("alice", now=0)
    assert t.wait("alice", now=0) == 0
    t.failed("alice", now=0)
    assert t.wait("alice", now=0) == 2
    t.failed("alice", now=0)
    assert t.wait("alice", now=0) == 4
    assert t.wait("bob", now=0) == 0
    t.succeeded("alice")
    assert t.wait("alice", now=0) == 0


def test_the_throttle_is_capped():
    t = passwords.Throttle()
    for _ in range(50):
        t.failed("alice", now=0)
    assert t.wait("alice", now=0) == passwords.Throttle.MAX_WAIT


# --- signed cookies --------------------------------------------------------- #

def test_a_signed_name_reads_back(monkeypatch):
    monkeypatch.setenv("WHISTLER_SESSION_KEY", "k" * 32)
    assert login.unsign(login.sign("alice")) == "alice"


def test_a_cookie_is_bound_to_its_purpose(monkeypatch):
    monkeypatch.setenv("WHISTLER_SESSION_KEY", "k" * 32)
    kiosk = login.sign("alice", login.PURPOSE_KIOSK)
    assert login.unsign(kiosk, login.PURPOSE_KIOSK) == "alice"
    assert login.unsign(kiosk, login.PURPOSE_PORTAL) is None
    assert login.unsign(kiosk) is None


def test_editing_the_name_breaks_the_signature(monkeypatch):
    monkeypatch.setenv("WHISTLER_SESSION_KEY", "k" * 32)
    _, issued, mac = login.sign("alice").split(":")
    assert login.unsign(f"admin:{issued}:{mac}") is None
    assert login.unsign("alice") is None
    assert login.unsign("") is None and login.unsign(None) is None


def test_another_key_signs_nothing_here(monkeypatch):
    monkeypatch.setenv("WHISTLER_SESSION_KEY", "k" * 32)
    value = login.sign("alice")
    monkeypatch.setenv("WHISTLER_SESSION_KEY", "j" * 32)
    assert login.unsign(value) is None


def test_cookies_expire(monkeypatch):
    monkeypatch.setenv("WHISTLER_SESSION_KEY", "k" * 32)
    monkeypatch.setenv("WHISTLER_SESSION_MAX_AGE", "3600")
    value = login.sign("alice", now=0)
    assert login.unsign(value, now=3600) == "alice"
    assert login.unsign(value, now=3601) is None
    change = login.sign("alice", login.PURPOSE_CHANGE, now=0)
    assert login.unsign(change, login.PURPOSE_CHANGE, now=901) is None
