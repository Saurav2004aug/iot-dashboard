"""
Minimal WebSocket client built on raw sockets, the counterpart to ws_server.py.

Used by the test suite, and as a CLI to watch the live stream:
    python ws_client.py              # print 5 ticks from ws://127.0.0.1:9001/ws
    python ws_client.py --count 20
"""

import argparse
import base64
import json
import os
import socket

import ws_protocol as wsp


class WebSocketClient:
    def __init__(self, host: str, port: int, path: str = "/ws", timeout: float = 5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        self.sock.sendall((
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii"))

        head, leftover = wsp.read_http_head(self.sock)
        status_line = head.split(b"\r\n", 1)[0].decode()
        if " 101 " not in status_line:
            raise ConnectionError(f"handshake failed: {status_line}")
        _, _, headers = wsp.parse_http_request(head)
        if headers.get("sec-websocket-accept") != wsp.compute_accept_key(key):
            raise ConnectionError("server returned a wrong Sec-WebSocket-Accept")
        self.handshake_response = head.decode()
        self.reader = wsp.FrameReader(self.sock, expect_masked=False, initial=leftover)

    def send_text(self, text: str):
        self.sock.sendall(wsp.encode_text(text, mask=True))

    def send_json(self, obj):
        self.send_text(json.dumps(obj))

    def send_raw(self, data: bytes):
        self.sock.sendall(data)

    def recv_frame(self):
        return self.reader.read_frame()

    def recv_json(self, msg_type: str | None = None, max_skip: int = 50) -> dict:
        """Return the next JSON text message, optionally skipping until one of `msg_type`."""
        for _ in range(max_skip):
            opcode, payload = self.reader.read_message(on_control=self._on_control)
            if opcode == wsp.OP_CLOSE:
                raise wsp.ConnectionClosed(f"server sent close: {payload!r}")
            msg = json.loads(payload)
            if msg_type is None or msg.get("type") == msg_type:
                return msg
        raise TimeoutError(f"no {msg_type!r} message received")

    def _on_control(self, opcode, payload):
        if opcode == wsp.OP_PING:  # answer server heartbeats
            self.sock.sendall(wsp.encode_frame(wsp.OP_PONG, payload, mask=True))

    def close(self, code: int = wsp.CLOSE_NORMAL):
        try:
            self.sock.sendall(wsp.encode_close(code, mask=True))
        finally:
            self.sock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--count", type=int, default=5)
    args = parser.parse_args()

    client = WebSocketClient(args.host, args.port)
    print(client.handshake_response, "\n")
    hello = client.recv_json("hello")
    print(f"Connected. Devices: {', '.join(hello['devices'])}\n")
    for _ in range(args.count):
        tick = client.recv_json("tick")
        for r in tick["readings"]:
            flag = "  ⚠ WARNING" if r["status"] == "warning" else ""
            print(f"{r['device_id']}: {r['temperature_c']:6.2f}°C  {r['humidity_pct']:5.1f}%  "
                  f"{r['vibration_g']:.3f} g{flag}")
        print()
    client.close()


if __name__ == "__main__":
    main()
