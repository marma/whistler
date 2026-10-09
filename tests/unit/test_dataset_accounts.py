"""Per-user dataset keys, the pure half (whistler/dataset_accounts.py)."""
import datetime
import json

from whistler import dataset_accounts as da


def test_sigv4_matches_awss_worked_example():
    # "Example: GET Object" from the AWS S3 SigV4 documentation
    # (sig-v4-header-based-auth). If this drifts, nothing we sign is valid.
    headers = da.sign_v4(
        "GET", "https://examplebucket.s3.amazonaws.com/test.txt",
        "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        headers={"Range": "bytes=0-9"},
        now=datetime.datetime(2013, 5, 24, tzinfo=datetime.timezone.utc))
    assert headers["Authorization"] == (
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/"
        "s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;"
        "x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba"
        "48dd91039c6036bdb41")


def test_account_names_cannot_collide():
    # A username is a DNS label (no dots), so <mode>.<user> is unambiguous.
    assert da.account_name("alice", "ro") == "ro.alice"
    assert da.parse_account_name("rw.anna-lisa") == ("anna-lisa", "rw")
    assert da.parse_account_name("root") is None
    assert da.parse_account_name("xx.alice") is None


def test_desired_accounts_one_per_user_and_mode():
    assert da.desired_accounts({"ro": ["alice", "bob"], "rw": ["alice"]}) == {
        "ro.alice": ("alice", "ro"), "ro.bob": ("bob", "ro"),
        "rw.alice": ("alice", "rw")}


def test_users_json_is_versitygws_format_and_stable():
    rendered = da.render_users_json({"rw.b": "s2", "ro.a": "s1"})
    data = json.loads(rendered)
    assert data["accessAccounts"]["ro.a"] == {
        "access": "ro.a", "secret": "s1", "role": "user",
        "userID": 0, "groupID": 0, "projectID": 0}
    # Same keys, same bytes: the Secret is only rewritten on a real change.
    assert rendered == da.render_users_json({"ro.a": "s1", "rw.b": "s2"})


def test_policy_grants_each_mode_only_its_actions():
    policy = da.build_policy("data", ["ro.alice", "rw.bob", "rw.carol"])
    by_principal = {tuple(s["Principal"]["AWS"]): set(s["Action"])
                    for s in policy["Statement"]}
    assert by_principal[("ro.alice",)] == set(da.READ_ACTIONS)
    assert "s3:PutObject" in by_principal[("rw.bob", "rw.carol")]
    for actions in by_principal.values():
        # Never the bucket's own policy or ACL: with those an rw user could
        # make the bucket anonymous-readable ("*" is anonymous in VersityGW).
        assert "s3:*" not in actions
        assert not {a for a in actions if "Policy" in a or "Acl" in a}
    for s in policy["Statement"]:
        assert s["Principal"] != "*"
        assert s["Resource"] == ["arn:aws:s3:::data", "arn:aws:s3:::data/*"]


def test_no_accounts_no_policy():
    assert da.build_policy("data", []) is None
    assert da.build_policy("data", ["root", "zz.x"]) is None


def test_list_users_parses_versitygws_answer():
    body = (b'<?xml version="1.0" encoding="UTF-8"?>\n<ListUserAccountsResult>'
            b'<Accounts><Access>rw.bob</Access><Secret>s3cret</Secret>'
            b'<Role>user</Role><UserID>0</UserID><GroupID>0</GroupID>'
            b'<ProjectID>0</ProjectID></Accounts></ListUserAccountsResult>')
    assert da.parse_list_users(body) == {"rw.bob": "s3cret"}
    assert da.parse_list_users(b"<ListUserAccountsResult/>") == {}


def test_account_xml_escapes():
    xml = da.account_xml("ro.a", "s<&>")
    assert b"<Secret>s&lt;&amp;&gt;</Secret>" in xml
    assert b"<Role>user</Role>" in xml
