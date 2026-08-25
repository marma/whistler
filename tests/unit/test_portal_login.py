"""The portal's sign-in screen (whistler.portal.login + management's handlers).

The form is the kiosk's, so what is worth testing here is what the *portal*
does differently: no second factor, its own marker cookie, and a ?next= that
cannot be talked into pointing off-site. The dev gate is the same one the rest
of the portal runs on — any password while WHISTLER_AUTH_ALLOW_ANY is set, and
nothing at all without it — so these tests also pin the "no credential store"
answer in both directions.
"""
import pytest
from starlette.requests import Request

from whistler.portal import login
from whistler.portal.login import USER_COOKIE
from whistler.portal.management import (LOGIN_PATH, LoginRequired,
                                        PORTAL_COOKIE, _get_identity,
                                        _login_required, _next_url, _safe_next,
                                        login_form, login_submit, logout,
                                        require_user)


@pytest.fixture
def dev(monkeypatch):
    monkeypatch.setenv("WHISTLER_AUTH_ALLOW_ANY", "true")


@pytest.fixture
def no_dev(monkeypatch):
    monkeypatch.delenv("WHISTLER_AUTH_ALLOW_ANY", raising=False)


def _request(path="/", query="", cookies=None, headers=None, method="GET"):
    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    if cookies:
        raw.append((b"cookie",
                    "; ".join(f"{k}={v}" for k, v in cookies.items()).encode()))
    return Request({"type": "http", "http_version": "1.1", "method": method,
                    "scheme": "http", "server": ("portal", 80), "root_path": "",
                    "path": path, "raw_path": path.encode(),
                    "query_string": query.encode(), "headers": raw})


def _cookies(response):
    out = {}
    for header in response.headers.getlist("set-cookie"):
        name, _, rest = header.partition("=")
        out[name] = rest.split(";")[0]
    return out


# --------------------------------------------------------------------------- #
# The form                                                                     #
# --------------------------------------------------------------------------- #

def test_the_form_posts_to_login_and_asks_for_both_fields(dev):
    body = login.render_login(action=LOGIN_PATH)
    assert "action=/login" in body
    assert "name=user" in body and "name=password" in body
    # The portal's form is the kiosk's, minus the factor behind it: no code
    # field here, and nothing to type one into on the way to the dashboard.
    assert "name=code" not in body


def test_the_form_says_whether_a_password_is_being_checked(dev, monkeypatch):
    assert "any password is accepted" in login.render_login(action=LOGIN_PATH)
    monkeypatch.delenv("WHISTLER_AUTH_ALLOW_ANY")
    # Not silence: a form that cannot let anyone in has to say so, rather than
    # implying the password it is asking for means something.
    assert "No credential store" in login.render_login(action=LOGIN_PATH)


def test_next_is_round_tripped_as_an_escaped_hidden_field(dev):
    body = login.render_login(action=LOGIN_PATH,
                              hidden={"next": '/homes?a="b'})
    assert 'name="next"' in body and "&quot;b" in body


def test_the_dashboard_default_carries_no_hidden_field(dev):
    assert "hidden" not in login.render_login(action=LOGIN_PATH,
                                             hidden={"next": ""})


# --------------------------------------------------------------------------- #
# Who a request is                                                             #
# --------------------------------------------------------------------------- #

def test_no_cookies_means_no_identity(dev):
    """The old answer was "user" for anybody at all, which on a portal with a
    login screen would mean nobody is ever logged out."""
    assert _get_identity(_request()) is None
    with pytest.raises(LoginRequired):
        require_user(_request())


def test_the_identity_cookie_alone_is_not_a_portal_sign_in(dev):
    """A kiosk sign-in sets the shared identity cookie. It must not also hand
    someone the management UI — that is what a kiosk is for."""
    assert _get_identity(_request(cookies={USER_COOKIE: "alice",
                                           "whistler_kiosk": "1"})) is None
    assert _get_identity(_request(cookies={USER_COOKIE: "alice",
                                           PORTAL_COOKIE: "1"})) == "alice"


def test_the_marker_without_a_name_is_not_an_identity(dev):
    assert _get_identity(_request(cookies={PORTAL_COOKIE: "1"})) is None


def test_the_dev_shortcut_still_skips_the_form(dev):
    assert _get_identity(_request(query="user=alice")) == "alice"
    assert _get_identity(_request(headers={"X-Whistler-User": "bob"})) == "bob"
    # The SSH username convention: only the first segment is the user.
    assert _get_identity(_request(query="user=alice-box")) == "alice"


def test_the_dev_shortcut_is_gated_on_dev_mode(no_dev):
    assert _get_identity(_request(query="user=alice")) is None


def test_a_cookie_beats_no_query(dev):
    """?user= wins when present — every internal link carries it — but its
    absence must not log a signed-in browser out."""
    request = _request(cookies={USER_COOKIE: "alice", PORTAL_COOKIE: "1"},
                       query="user=bob")
    assert _get_identity(request) == "bob"


# --------------------------------------------------------------------------- #
# Signing in                                                                   #
# --------------------------------------------------------------------------- #

async def test_signing_in_sets_both_cookies_and_lands_on_the_dashboard(dev):
    response = await login_submit(_request(method="POST"), user="alice",
                                  password="anything")
    assert response.status_code == 303
    assert response.headers["location"] == "/?user=alice"
    cookies = _cookies(response)
    assert cookies[USER_COOKIE] == "alice" and cookies[PORTAL_COOKIE] == "1"


async def test_the_marker_cookie_is_httponly_and_the_identity_one_is_not(dev):
    """The identity cookie is read by the viewer app's own page JS paths and is
    the same one the kiosk sets; the marker is nobody's business but the
    server's."""
    response = await login_submit(_request(method="POST"), user="alice",
                                  password="x")
    headers = {h.split("=")[0]: h for h in response.headers.getlist("set-cookie")}
    assert "httponly" in headers[PORTAL_COOKIE].lower()
    assert "httponly" not in headers[USER_COOKIE].lower()


async def test_a_nameless_sign_in_is_refused(dev):
    response = await login_submit(_request(method="POST"), user="   ",
                                  password="x")
    assert response.status_code == 401
    assert not _cookies(response)


async def test_outside_dev_mode_nothing_is_accepted(no_dev):
    response = await login_submit(_request(method="POST"), user="alice",
                                  password="hunter2")
    assert response.status_code == 401
    assert not _cookies(response)
    assert login.verify_credentials("alice", "hunter2") is False


async def test_signing_in_returns_to_where_the_browser_was_going(dev):
    response = await login_submit(_request(method="POST"), user="alice",
                                  password="x", next="/instances/box")
    assert response.headers["location"] == "/instances/box?user=alice"


async def test_an_already_signed_in_browser_skips_the_form(dev):
    request = _request(query="next=/homes",
                       cookies={USER_COOKIE: "alice", PORTAL_COOKIE: "1"})
    response = await login_form(request)
    assert response.status_code == 303
    assert response.headers["location"] == "/homes?user=alice"


async def test_the_form_is_served_no_store(dev):
    response = await login_form(_request())
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"


async def test_signing_out_drops_both_cookies(dev):
    response = await logout(_request(method="POST"))
    assert response.headers["location"] == LOGIN_PATH
    for header in response.headers.getlist("set-cookie"):
        assert 'expires=Thu, 01 Jan 1970' in header or "Max-Age=0" in header
    assert set(_cookies(response)) == {USER_COOKIE, PORTAL_COOKIE}


# --------------------------------------------------------------------------- #
# ?next= is not an open redirect                                               #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("hostile", [
    "//evil.example",                 # protocol-relative
    "https://evil.example/",          # absolute
    "http:/evil.example",             # scheme with one slash
    "/\r\nSet-Cookie: x=1",           # header injection
    "\\\\evil.example",               # UNC-ish
    "evil.example",                   # no leading slash at all
    "/login?next=/login",             # a loop back to the form
    "/x" * 400,                       # absurdly long
    None,
])
def test_next_only_ever_stays_on_this_app(hostile):
    assert _safe_next(hostile) == "/"


def test_an_ordinary_path_survives():
    assert _safe_next("/admin/users?q=a") == "/admin/users?q=a"


def test_a_stale_user_in_next_loses_to_the_signed_in_name():
    """`query_params.get` returns the first value, so appending beside a stale
    ?user= would sign the browser in as whoever made the link."""
    assert _next_url("/homes?user=bob&x=1", "alice") == "/homes?x=1&user=alice"


# --------------------------------------------------------------------------- #
# What an unauthenticated request gets                                         #
# --------------------------------------------------------------------------- #

async def test_a_navigation_is_sent_to_the_form_with_its_destination(dev):
    request = _request(path="/instances/box", query="tab=1",
                       headers={"Sec-Fetch-Mode": "navigate"})
    response = await _login_required(request, LoginRequired())
    assert response.status_code == 303
    assert response.headers["location"] == \
        "/login?next=%2Finstances%2Fbox%3Ftab%3D1"


async def test_an_html_request_without_sec_fetch_mode_still_gets_the_form(dev):
    response = await _login_required(
        _request(path="/", headers={"Accept": "text/html,*/*"}),
        LoginRequired())
    assert response.status_code == 303
    assert response.headers["location"] == LOGIN_PATH


async def test_a_poll_gets_a_status_it_can_act_on(dev):
    """An htmx status poll or a fetch redirected to an HTML page produces a
    parse error at the far end, not a login."""
    response = await _login_required(
        _request(path="/instances/box/status-badge",
                 headers={"Accept": "*/*", "HX-Request": "true"}),
        LoginRequired())
    assert response.status_code == 401
