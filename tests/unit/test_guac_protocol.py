"""Guacamole wire protocol codec (whistler.portal.protocol)."""
import pytest

from whistler.portal.protocol import (
    Decoder,
    ProtocolError,
    encode,
    parse_instruction,
    take_complete_instructions,
)


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


def test_take_complete_instructions_holds_partial_tail():
    # The relay must forward only whole instructions per WS message (the browser
    # tunnel doesn't buffer across messages), holding any partial tail.
    complete, remainder = take_complete_instructions("5.ready,1.x;4.syn")
    assert complete == "5.ready,1.x;"
    assert remainder == "4.syn"


def test_take_complete_instructions_is_verbatim_and_delimiter_safe():
    # The prefix is returned unmodified, and values containing '.'/','/';' are
    # not mis-split (length-driven, not delimiter-split).
    full = encode("a", "x;y,z.q").decode("utf-8")  # "1.a,7.x;y,z.q;"
    complete, remainder = take_complete_instructions(full + "3.ab")
    assert complete == full
    assert remainder == "3.ab"


def test_take_complete_instructions_no_complete_yet():
    complete, remainder = take_complete_instructions("10.partialdat")
    assert complete == ""
    assert remainder == "10.partialdat"


def test_decoder_pending_bytes_includes_split_multibyte_tail():
    # A multibyte char split at the end of a read must not be lost: `pending`
    # (decoded chars) drops the buffered lead byte, but `pending_bytes` keeps it,
    # so handing it to a fresh decoder reconstructs the stream byte-for-byte.
    payload = encode("name", "café") + encode("sync", "9")
    cut = payload.index(b"\xa9")          # split inside 'é' (0xc3 0xa9)
    dec = Decoder()
    assert dec.feed(payload[:cut]) == []  # nothing complete yet
    relay = Decoder()
    assert relay.feed(dec.pending_bytes + payload[cut:]) == [
        ["name", "café"], ["sync", "9"],
    ]
