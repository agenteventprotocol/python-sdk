"""RFC 6455 client-side pure logic, shared by both WebSocket flavors.

One definition, two consumers: :mod:`aep_sdk.aio` and :mod:`aep_sdk.control`
both speak the same wire, so the pure (no-I/O) pieces — the handshake key /
accept computation, the upgrade request bytes, and masked frame *encoding* —
live here and only here. Frame *decoding* deliberately stays in each flavor:
it is interleaved with that flavor's reads (``StreamReader.readexactly`` vs
a blocking ``makefile("rb")``) and the header math is small, so sharing it
would trade two short read loops for an awkward I/O-agnostic state machine.

Minimal on purpose — text/ping/pong/close, no fragmentation (the relay sends
whole frames well under the 64 KiB event ceiling, AEP-0003).
"""
from __future__ import annotations

import base64
import hashlib
import os

WS_ACCEPT_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"  # RFC 6455 §1.3 constant


def handshake_key() -> str:
    """A fresh ``sec-websocket-key`` value (base64 of 16 random bytes)."""
    return base64.b64encode(os.urandom(16)).decode("ascii")


def expected_accept(key: str) -> str:
    """The ``sec-websocket-accept`` value a truthful server must echo for
    ``key`` (RFC 6455 §4.2.2: base64 of SHA-1 over key + GUID)."""
    return base64.b64encode(
        hashlib.sha1((key + WS_ACCEPT_GUID).encode("ascii")).digest()).decode("ascii")


def upgrade_request(host: str, port: int, path: str, key: str) -> bytes:
    """The HTTP/1.1 upgrade request for ``path``, ready to write to the wire."""
    return (f"GET {path} HTTP/1.1\r\n"
            f"host: {host}:{port}\r\n"
            "upgrade: websocket\r\n"
            "connection: Upgrade\r\n"
            f"sec-websocket-key: {key}\r\n"
            "sec-websocket-version: 13\r\n\r\n").encode("latin-1")


def encode_text_frame(payload: bytes) -> bytes:
    """One masked FIN text frame (client side MUST mask, RFC 6455 §5.3)."""
    mask = os.urandom(4)
    length = len(payload)
    if length < 126:
        header = bytes([0x81, 0x80 | length])
    elif length < 65_536:
        header = bytes([0x81, 0x80 | 126]) + length.to_bytes(2, "big")
    else:
        header = bytes([0x81, 0x80 | 127]) + length.to_bytes(8, "big")
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return header + mask + masked


def encode_control_frame(opcode: int, payload: bytes = b"") -> bytes:
    """One masked control frame (ping/pong/close; payload always < 126)."""
    mask = os.urandom(4)
    header = bytes([0x80 | opcode, 0x80 | len(payload)])
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return header + mask + masked
