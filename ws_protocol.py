"""
WebSocket protocol primitives (RFC 6455), implemented from scratch.

Shared by the server (ws_server.py) and the test/CLI client (ws_client.py).

Key points of the spec implemented here:
  - Handshake: Sec-WebSocket-Accept = base64(SHA1(key + magic GUID))
  - Frame layout: FIN/opcode byte, MASK bit + 7/16/64-bit payload length,
    optional 4-byte masking key, payload
  - Client->server frames MUST be masked; server->client frames MUST NOT be
  - Control frames (close/ping/pong) are <= 125 bytes and never fragmented
  - Messages may be fragmented across several frames (opcode 0x0 = continuation)
  - TCP is a byte stream: one recv() may contain half a frame or three frames,
    so frames are parsed from a buffer, never assumed to align with recv()
"""

import base64
import hashlib
import os
import socket
import struct
from typing import Optional

WS_MAGIC_STRING = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONTINUATION = 0x0
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA
CONTROL_OPCODES = {OP_CLOSE, OP_PING, OP_PONG}
DATA_OPCODES = {OP_CONTINUATION, OP_TEXT, OP_BINARY}

# Close status codes (RFC 6455 section 7.4.1)
CLOSE_NORMAL = 1000
CLOSE_GOING_AWAY = 1001
CLOSE_PROTOCOL_ERROR = 1002
CLOSE_INVALID_DATA = 1007
CLOSE_TOO_BIG = 1009

MAX_MESSAGE_BYTES = 1 << 20  # 1 MiB: protects the server from memory exhaustion


class ProtocolError(Exception):
    def __init__(self, message: str, close_code: int = CLOSE_PROTOCOL_ERROR):
        super().__init__(message)
        self.close_code = close_code


class ConnectionClosed(Exception):
    pass


def compute_accept_key(client_key: str) -> str:
    """RFC 6455 handshake: base64(SHA1(key + magic string))."""
    sha1 = hashlib.sha1((client_key + WS_MAGIC_STRING).encode("ascii")).digest()
    return base64.b64encode(sha1).decode("ascii")


def encode_frame(opcode: int, payload: bytes = b"", *, mask: bool = False, fin: bool = True) -> bytes:
    """Build a single frame. Servers send unmasked frames; clients must mask."""
    header = bytearray()
    header.append((0x80 if fin else 0x00) | opcode)
    mask_bit = 0x80 if mask else 0x00
    length = len(payload)
    if length <= 125:
        header.append(mask_bit | length)
    elif length <= 0xFFFF:
        header.append(mask_bit | 126)
        header += struct.pack("!H", length)
    else:
        header.append(mask_bit | 127)
        header += struct.pack("!Q", length)

    if mask:
        key = os.urandom(4)
        header += key
        payload = apply_mask(payload, key)
    return bytes(header) + payload


def encode_text(message: str, *, mask: bool = False) -> bytes:
    return encode_frame(OP_TEXT, message.encode("utf-8"), mask=mask)


def encode_close(code: int = CLOSE_NORMAL, reason: str = "", *, mask: bool = False) -> bytes:
    return encode_frame(OP_CLOSE, struct.pack("!H", code) + reason.encode("utf-8")[:123], mask=mask)


def apply_mask(data: bytes, key: bytes) -> bytes:
    """XOR-mask data with a 4-byte key. It's symmetric: the same call unmasks.

    Uses a single big-integer XOR instead of a per-byte Python loop, which is
    roughly 50x faster on large payloads.
    """
    if not data:
        return data
    n = len(data)
    repeated = (key * (n // 4 + 1))[:n]
    return (int.from_bytes(data, "big") ^ int.from_bytes(repeated, "big")).to_bytes(n, "big")


class FrameReader:
    """Reads complete frames from a socket, handling partial and coalesced reads."""

    def __init__(self, sock: socket.socket, *, expect_masked: Optional[bool],
                 max_message_bytes: int = MAX_MESSAGE_BYTES, initial: bytes = b""):
        self.sock = sock
        self.expect_masked = expect_masked  # True for server side, False for client side
        self.max_message_bytes = max_message_bytes
        self._buf = bytearray(initial)

    def _read_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionClosed("peer closed the TCP connection")
            self._buf += chunk
        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

    def read_frame(self) -> tuple[bool, int, bytes]:
        """Returns (fin, opcode, unmasked_payload) for one frame."""
        b0, b1 = self._read_exact(2)
        fin = bool(b0 & 0x80)
        if b0 & 0x70:
            raise ProtocolError("RSV bits set without a negotiated extension")
        opcode = b0 & 0x0F
        if opcode not in CONTROL_OPCODES | DATA_OPCODES:
            raise ProtocolError(f"unknown opcode {opcode:#x}")

        masked = bool(b1 & 0x80)
        if self.expect_masked is not None and masked != self.expect_masked:
            raise ProtocolError("client frames must be masked" if self.expect_masked
                                else "server frames must not be masked")

        length = b1 & 0x7F
        if length == 126:
            (length,) = struct.unpack("!H", self._read_exact(2))
        elif length == 127:
            (length,) = struct.unpack("!Q", self._read_exact(8))

        if opcode in CONTROL_OPCODES and (length > 125 or not fin):
            raise ProtocolError("control frames must be <= 125 bytes and not fragmented")
        if length > self.max_message_bytes:
            raise ProtocolError("frame too large", CLOSE_TOO_BIG)

        key = self._read_exact(4) if masked else None
        payload = self._read_exact(length)
        if key:
            payload = apply_mask(payload, key)
        return fin, opcode, payload

    def read_message(self, on_control=None) -> tuple[int, bytes]:
        """Read one complete (possibly fragmented) data message.

        Control frames that arrive between fragments are passed to
        `on_control(opcode, payload)`. A close frame is returned immediately
        as (OP_CLOSE, payload).
        """
        message_opcode = None
        parts: list[bytes] = []
        size = 0
        while True:
            fin, opcode, payload = self.read_frame()

            if opcode in CONTROL_OPCODES:
                if opcode == OP_CLOSE:
                    return OP_CLOSE, payload
                if on_control:
                    on_control(opcode, payload)
                    continue
                return opcode, payload

            if opcode == OP_CONTINUATION:
                if message_opcode is None:
                    raise ProtocolError("continuation frame without a starting frame")
            else:
                if message_opcode is not None:
                    raise ProtocolError("new data frame while a fragmented message is in progress")
                message_opcode = opcode

            parts.append(payload)
            size += len(payload)
            if size > self.max_message_bytes:
                raise ProtocolError("message too large", CLOSE_TOO_BIG)
            if fin:
                data = b"".join(parts)
                if message_opcode == OP_TEXT:
                    try:
                        data.decode("utf-8")
                    except UnicodeDecodeError:
                        raise ProtocolError("text message is not valid UTF-8", CLOSE_INVALID_DATA)
                return message_opcode, data


VALID_CLOSE_CODES = set(range(1000, 1004)) | set(range(1007, 1015)) | set(range(3000, 5000))


def parse_close_payload(payload: bytes) -> tuple[Optional[int], str]:
    """Validate a close frame body (RFC 6455 section 5.5.1 / 7.4).

    Empty means no status code. Otherwise the body is a 2-byte code plus an
    optional UTF-8 reason. Codes such as 1005/1006/1015 are reserved for local
    use and must never appear on the wire."""
    if not payload:
        return None, ""
    if len(payload) == 1:
        raise ProtocolError("close frame payload of 1 byte")
    code = int.from_bytes(payload[:2], "big")
    if code not in VALID_CLOSE_CODES:
        raise ProtocolError(f"invalid close code {code}")
    try:
        reason = payload[2:].decode("utf-8")
    except UnicodeDecodeError:
        raise ProtocolError("close reason is not valid UTF-8", CLOSE_INVALID_DATA)
    return code, reason


def read_http_head(sock: socket.socket, limit: int = 16384) -> tuple[bytes, bytes]:
    """Read until the blank line ending HTTP headers.
    Returns (head, leftover_bytes_already_received)."""
    buf = bytearray()
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionClosed("connection closed during HTTP handshake")
        buf += chunk
        if len(buf) > limit:
            raise ProtocolError("HTTP header too large")
    head, _, rest = bytes(buf).partition(b"\r\n\r\n")
    return head, rest


def parse_http_request(head: bytes) -> tuple[str, str, dict]:
    lines = head.decode("latin-1").split("\r\n")
    # Works for request lines ("GET /ws HTTP/1.1") and status lines
    # ("HTTP/1.1 101 Switching Protocols") alike.
    parts = lines[0].split(" ", 2)
    if len(parts) != 3:
        raise ProtocolError("malformed HTTP start line")
    method, path, _version = parts
    headers = {}
    for line in lines[1:]:
        name, sep, value = line.partition(":")
        if sep:
            headers[name.strip().lower()] = value.strip()
    return method, path, headers
