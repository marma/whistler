"""The Guacamole wire protocol — pure encode/decode, no I/O.

An instruction is a comma-separated list of elements terminated by a semicolon;
each element is ``LENGTH.VALUE`` where LENGTH is the number of **Unicode
characters** in VALUE (not bytes). Values may themselves contain ``.``, ``,``
and ``;``, so parsing must be length-driven, never delimiter-split.

    encode("select", "rdp")        -> b"6.select,3.rdp;"
    parse_instruction("5.ready,1.x;") -> ["ready", "x"]

This module is deliberately dependency-free so it always runs in the unit suite.
"""
import codecs
from typing import List, Tuple


class ProtocolError(ValueError):
    """Malformed Guacamole instruction."""


def encode(*elements: str) -> bytes:
    """Encode one instruction. Lengths are Unicode char counts; the whole
    instruction is UTF-8 encoded only at the end."""
    body = ",".join(f"{len(e)}.{e}" for e in elements) + ";"
    return body.encode("utf-8")


def _parse_buffer(buf: str) -> Tuple[List[List[str]], str]:
    """Consume as many complete instructions as ``buf`` holds.

    Returns ``(instructions, remainder)`` where remainder is the unparsed tail
    (an incomplete instruction kept for the next chunk)."""
    instructions: List[List[str]] = []
    pos = 0
    n = len(buf)

    while pos < n:
        start = pos
        elements: List[str] = []
        complete = False

        while True:
            dot = buf.find(".", pos)
            if dot == -1:
                break  # incomplete: length field not fully arrived
            try:
                length = int(buf[pos:dot])
            except ValueError:
                raise ProtocolError(f"invalid element length: {buf[pos:dot]!r}")

            val_start = dot + 1
            val_end = val_start + length
            if val_end >= n:
                break  # incomplete: value (or its separator) not fully arrived

            value = buf[val_start:val_end]
            sep = buf[val_end]
            elements.append(value)
            pos = val_end + 1

            if sep == ";":
                instructions.append(elements)
                complete = True
                break
            if sep != ",":
                raise ProtocolError(f"invalid element separator: {sep!r}")

        if not complete:
            pos = start  # rewind the partial instruction
            break

    return instructions, buf[pos:]


def take_complete_instructions(buf: str) -> Tuple[str, str]:
    """Split ``buf`` into ``(prefix, remainder)`` where ``prefix`` holds only
    whole instructions (returned **verbatim**, original length fields intact) and
    ``remainder`` is a trailing partial instruction, if any.

    guacamole-common-js's WebSocketTunnel parses each WS message on its own and
    does NOT buffer a partial instruction across messages — a message ending
    mid-instruction makes it raise "Incomplete instruction." So the relay must
    forward only the complete-instruction prefix per message and hold the rest."""
    _instructions, remainder = _parse_buffer(buf)
    return buf[:len(buf) - len(remainder)], remainder


def parse_instruction(text: str) -> List[str]:
    """Parse exactly one complete instruction (trailing ``;`` required)."""
    instructions, remainder = _parse_buffer(text)
    if len(instructions) != 1 or remainder:
        raise ProtocolError(f"expected exactly one complete instruction: {text!r}")
    return instructions[0]


class Decoder:
    """Incremental decoder: feed raw bytes, get back complete instructions.

    Handles instructions split across reads and multibyte UTF-8 characters split
    across reads (via an incremental UTF-8 decoder)."""

    def __init__(self) -> None:
        self._utf8 = codecs.getincrementaldecoder("utf-8")()
        self._buf = ""

    def feed(self, data: bytes) -> List[List[str]]:
        self._buf += self._utf8.decode(data)
        instructions, self._buf = _parse_buffer(self._buf)
        return instructions

    @property
    def pending(self) -> str:
        """Decoded-but-incomplete tail not yet forming a full instruction."""
        return self._buf

    @property
    def pending_bytes(self) -> bytes:
        """The full remaining byte tail: the decoded-but-unparsed chars **plus**
        any incomplete multibyte sequence still buffered inside the UTF-8 decoder.

        The handshake uses this to hand the bytes that arrived glued to ``ready``
        to the relay's fresh decoder. ``pending`` alone would drop a multibyte
        character split across the read that delivered ``ready``, orphaning its
        continuation bytes and desyncing the stream (a rare image-corruption
        / decode-error source)."""
        return self._buf.encode("utf-8") + self._utf8.getstate()[0]
