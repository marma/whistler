"""Guacamole wire protocol codec (whistler.portal.protocol)."""
import pytest

from whistler.portal.protocol import Decoder, ProtocolError, encode, parse_instruction


def test_encode_basic():
    assert encode("select", "rdp") == b"6.select,3.rdp;"


def test_encode_empty_element():
    assert encode("image") == b"5.image;"
    assert encode("connect", "", "x") == b"7.connect,0.,1.x;"


def test_encode_length_is_char_count_not_bytes():
    # "résumé" is 6 characters but 8 UTF-8 bytes; the length prefix must be 6.
    out = encode("résumé")
    assert out == "6.résumé;".encode("utf-8")
    assert out.split(b".", 1)[0] == b"6"


def test_parse_instruction_roundtrip():
    assert parse_instruction("5.ready,1.x;") == ["ready", "x"]


def test_parse_value_containing_delimiters():
    # A value may contain '.', ',' and ';' — parsing is length-driven.
    tricky = "a.b,c;d"          # 7 chars
    inst = encode("arg", tricky)
    assert parse_instruction(inst.decode()) == ["arg", tricky]


def test_parse_instruction_rejects_incomplete():
    with pytest.raises(ProtocolError):
        parse_instruction("6.select,3.rdp")   # no terminating ;


def test_decoder_splits_instructions():
    dec = Decoder()
    assert dec.feed(b"6.select,3.rdp;4.size,4.1024,3.768,2.96;") == [
        ["select", "rdp"],
        ["size", "1024", "768", "96"],
    ]


def test_decoder_handles_instruction_split_across_chunks():
    dec = Decoder()
    assert dec.feed(b"6.sel") == []
    assert dec.feed(b"ect,3.rdp;") == [["select", "rdp"]]


def test_decoder_handles_multibyte_char_split_across_chunks():
    payload = encode("arg", "é")          # 'é' = 2 bytes
    mid = len(payload) // 2
    dec = Decoder()
    first = dec.feed(payload[:mid])
    second = dec.feed(payload[mid:])
    assert first == []
    assert second == [["arg", "é"]]


def test_decoder_pending_holds_partial_tail():
    dec = Decoder()
    dec.feed(b"5.ready,1.x;4.syn")
    assert dec.pending == "4.syn"
