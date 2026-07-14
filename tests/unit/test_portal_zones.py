"""Zone form parsing/serialization (whistler.portal.management).

The admin zone editor speaks flat text (textareas); these helpers translate to
and from the Zone CR's egress/dns structure. Bad input must 400, never be
silently dropped — a typo'd CIDR would otherwise widen a zone.
"""
import pytest
from fastapi import HTTPException

from whistler.portal.management import (
    _build_zone_data,
    _format_allow_cidrs,
    _parse_allow_cidrs,
    _parse_block_cidrs,
    _parse_dns_servers,
)


def test_allow_cidrs_parse_ports_and_bare_cidrs():
    entries = _parse_allow_cidrs("203.0.113.0/24 443/tcp 53/udp\n0.0.0.0/0\n")
    assert entries == [
        {"cidr": "203.0.113.0/24",
         "ports": [{"port": 443, "protocol": "TCP"}, {"port": 53, "protocol": "UDP"}]},
        {"cidr": "0.0.0.0/0"},
    ]


def test_allow_cidrs_round_trip():
    text = "203.0.113.0/24 443/tcp 53/udp\n0.0.0.0/0"
    assert _format_allow_cidrs(_parse_allow_cidrs(text)) == text


@pytest.mark.parametrize("bad", [
    "not-a-cidr",
    "203.0.113.0/24 99999/tcp",
    "203.0.113.0/24 443/icmp",
    "203.0.113.0/24 443",
])
def test_allow_cidrs_reject_bad_input(bad):
    with pytest.raises(HTTPException) as exc:
        _parse_allow_cidrs(bad)
    assert exc.value.status_code == 400


def test_block_cidrs_parse_and_reject():
    assert _parse_block_cidrs("10.0.0.0/8\n\n192.168.0.0/16") == \
        ["10.0.0.0/8", "192.168.0.0/16"]
    with pytest.raises(HTTPException):
        _parse_block_cidrs("10.0.0.0/8\nnope")


def test_dns_servers_parse_and_reject():
    assert _parse_dns_servers("10.0.0.53, 10.0.0.54 10.0.0.55") == \
        ["10.0.0.53", "10.0.0.54", "10.0.0.55"]
    with pytest.raises(HTTPException):
        _parse_dns_servers("10.0.0.53, example.com")


def test_build_zone_data_assembles_the_cr_spec_shape():
    data = _build_zone_data(
        name=" green ", description=" internet only ",
        allow_cidrs="", block_cidrs="10.0.0.0/8",
        dns_cluster_only="on", dns_servers="",
    )
    assert data == {
        "name": "green",
        "description": "internet only",
        "egress": {"blockCIDRs": ["10.0.0.0/8"]},
        "dns": {"clusterOnly": True},
    }


def test_build_zone_data_empty_form_yields_bare_zone():
    # save_zone replaces the spec, so an emptied form must clear every field.
    assert _build_zone_data("z", None, "", "", None, "") == {"name": "z"}
