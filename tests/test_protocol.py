"""Unit tests for the from-scratch RFC 6455 primitives (no network needed)."""
import os
import socket
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import ws_protocol as wsp  # noqa: E402


_open_sockets = []


def tearDownModule():
    for s in _open_sockets:
        s.close()


def reader_for(data: bytes, expect_masked=True, chunk_size=None, **kw):
    """A FrameReader fed from a socketpair, optionally delivering the bytes in
    tiny chunks to simulate TCP splitting a frame across many recv() calls."""
    a, b = socket.socketpair()
    if chunk_size:
        for i in range(0, len(data), chunk_size):
            a.sendall(data[i:i + chunk_size])
    else:
        a.sendall(data)
    a.shutdown(socket.SHUT_WR)
    r = wsp.FrameReader(b, expect_masked=expect_masked, **kw)
    _open_sockets.extend((a, b))  # closed in tearDownModule
    return r


class TestHandshake(unittest.TestCase):
    def test_accept_key_matches_rfc_6455_example(self):
        # The worked example from RFC 6455 section 1.3
        self.assertEqual(wsp.compute_accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
                         "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")

    def test_parse_request_and_status_lines(self):
        m, p, h = wsp.parse_http_request(b"GET /ws HTTP/1.1\r\nUpgrade: websocket\r\nX-Two:  a:b ")
        self.assertEqual((m, p), ("GET", "/ws"))
        self.assertEqual(h, {"upgrade": "websocket", "x-two": "a:b"})
        version, code, headers = wsp.parse_http_request(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket")
        self.assertEqual((version, code, headers), ("HTTP/1.1", "101", {"upgrade": "websocket"}))

    def test_malformed_start_line(self):
        with self.assertRaises(wsp.ProtocolError):
            wsp.parse_http_request(b"garbage")


class TestFrameCodec(unittest.TestCase):
    def test_length_encoding_boundaries(self):
        # 7-bit, 16-bit and 64-bit length forms, at and around each boundary
        for n, header_len in ((0, 2), (125, 2), (126, 4), (65535, 4), (65536, 10)):
            with self.subTest(n=n):
                frame = wsp.encode_frame(wsp.OP_BINARY, b"x" * n)
                self.assertEqual(len(frame), header_len + n)

    def test_roundtrip_masked_all_sizes(self):
        for n in (0, 1, 125, 126, 1000, 65535, 65536, 200_000):
            with self.subTest(n=n):
                payload = os.urandom(n)
                r = reader_for(wsp.encode_frame(wsp.OP_BINARY, payload, mask=True))
                fin, op, data = r.read_frame()
                self.assertEqual((fin, op, data), (True, wsp.OP_BINARY, payload))

    def test_masking_is_symmetric_and_actually_masks(self):
        key = b"\x01\x02\x03\x04"
        data = b"hello websocket"
        masked = wsp.apply_mask(data, key)
        self.assertNotEqual(masked, data)
        self.assertEqual(wsp.apply_mask(masked, key), data)

    def test_frame_split_across_many_recv_calls(self):
        """The v1 server assumed one recv() == one frame. TCP gives no such guarantee."""
        frame = wsp.encode_text("split me into pieces " * 20, mask=True)
        r = reader_for(frame, chunk_size=3)
        self.assertEqual(r.read_message(), (wsp.OP_TEXT, ("split me into pieces " * 20).encode()))

    def test_multiple_frames_in_one_recv(self):
        data = b"".join(wsp.encode_text(f"m{i}", mask=True) for i in range(5))
        r = reader_for(data)
        self.assertEqual([r.read_message()[1] for _ in range(5)], [f"m{i}".encode() for i in range(5)])

    def test_fragmented_message_with_interleaved_ping(self):
        data = (wsp.encode_frame(wsp.OP_TEXT, b"Hel", mask=True, fin=False)
                + wsp.encode_frame(wsp.OP_PING, b"p", mask=True)
                + wsp.encode_frame(wsp.OP_CONTINUATION, b"lo", mask=True, fin=False)
                + wsp.encode_frame(wsp.OP_CONTINUATION, b"!", mask=True))
        seen = []
        r = reader_for(data)
        self.assertEqual(r.read_message(on_control=lambda op, p: seen.append((op, p))),
                         (wsp.OP_TEXT, b"Hello!"))
        self.assertEqual(seen, [(wsp.OP_PING, b"p")])


class TestProtocolViolations(unittest.TestCase):
    def assertProtocolError(self, data, code=wsp.CLOSE_PROTOCOL_ERROR, **kw):
        with self.assertRaises(wsp.ProtocolError) as ctx:
            reader_for(data, **kw).read_message()
        self.assertEqual(ctx.exception.close_code, code)

    def test_unmasked_client_frame_rejected(self):
        self.assertProtocolError(wsp.encode_text("hi", mask=False))

    def test_masked_server_frame_rejected_by_client(self):
        self.assertProtocolError(wsp.encode_text("hi", mask=True), expect_masked=False)

    def test_oversized_control_frame_rejected(self):
        self.assertProtocolError(wsp.encode_frame(wsp.OP_PING, b"x" * 126, mask=True))

    def test_fragmented_control_frame_rejected(self):
        self.assertProtocolError(wsp.encode_frame(wsp.OP_PING, b"x", mask=True, fin=False))

    def test_unknown_opcode_rejected(self):
        self.assertProtocolError(wsp.encode_frame(0x3, b"", mask=True))

    def test_rsv_bits_rejected(self):
        frame = bytearray(wsp.encode_text("x", mask=True))
        frame[0] |= 0x40
        self.assertProtocolError(bytes(frame))

    def test_orphan_continuation_rejected(self):
        self.assertProtocolError(wsp.encode_frame(wsp.OP_CONTINUATION, b"x", mask=True))

    def test_invalid_utf8_rejected_with_1007(self):
        self.assertProtocolError(wsp.encode_frame(wsp.OP_TEXT, b"\xff\xfe", mask=True),
                                 code=wsp.CLOSE_INVALID_DATA)

    def test_message_size_limit_with_1009(self):
        self.assertProtocolError(wsp.encode_frame(wsp.OP_BINARY, b"x" * 2000, mask=True),
                                 code=wsp.CLOSE_TOO_BIG, max_message_bytes=1000)

    def test_close_payload_validation(self):
        self.assertEqual(wsp.parse_close_payload(b""), (None, ""))
        self.assertEqual(wsp.parse_close_payload(b"\x03\xe8bye"), (1000, "bye"))
        self.assertEqual(wsp.parse_close_payload((4001).to_bytes(2, "big")), (4001, ""))
        for bad, code in ((b"\x03", wsp.CLOSE_PROTOCOL_ERROR),                  # 1-byte body
                          ((1005).to_bytes(2, "big"), wsp.CLOSE_PROTOCOL_ERROR),  # reserved
                          ((999).to_bytes(2, "big"), wsp.CLOSE_PROTOCOL_ERROR),
                          (b"\x03\xe8\xff", wsp.CLOSE_INVALID_DATA)):          # bad UTF-8
            with self.subTest(bad=bad), self.assertRaises(wsp.ProtocolError) as ctx:
                wsp.parse_close_payload(bad)
            self.assertEqual(ctx.exception.close_code, code)

    def test_peer_disconnect_raises_connection_closed(self):
        with self.assertRaises(wsp.ConnectionClosed):
            reader_for(wsp.encode_text("partial", mask=True)[:4]).read_message()


if __name__ == "__main__":
    unittest.main()
