"""
Integration tests: a real SensorServer on a random free port with a fast
tick, exercised by the raw-socket client. No websocket library on either side.
"""
import json
import os
import random
import socket
import sys
import time
import unittest
import urllib.request

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import ws_protocol as wsp  # noqa: E402
from ws_client import WebSocketClient  # noqa: E402
from ws_server import SensorServer, SimulatedDevice  # noqa: E402


class ServerTestCase(unittest.TestCase):
    def setUp(self):
        self.server = SensorServer(port=0, tick_seconds=0.05, ping_interval=60, seed=1)
        self.port = self.server.start()
        self.clients = []

    def tearDown(self):
        for c in self.clients:
            try:
                c.sock.close()
            except OSError:
                pass
        self.server.stop()

    def connect(self) -> WebSocketClient:
        c = WebSocketClient("127.0.0.1", self.port)
        self.clients.append(c)
        return c

    def wait_for(self, predicate, timeout=2.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.01)
        return False


class TestStreaming(ServerTestCase):
    def test_handshake_hello_history_and_ticks(self):
        c = self.connect()
        self.assertIn("101 Switching Protocols", c.handshake_response)
        hello = c.recv_json()
        self.assertEqual(hello["type"], "hello")
        self.assertEqual(hello["devices"], ["esp32-01", "esp32-02", "esp32-03"])
        self.assertEqual(c.recv_json()["type"], "history")
        for _ in range(3):
            tick = c.recv_json("tick")
            self.assertEqual(len(tick["readings"]), 3)
            for r in tick["readings"]:
                self.assertEqual(set(r), {"device_id", "temperature_c", "humidity_pct",
                                          "vibration_g", "status"})

    def test_late_joiner_receives_history(self):
        first = self.connect()
        for _ in range(5):
            first.recv_json("tick")
        late = self.connect()
        late.recv_json("hello")
        self.assertGreaterEqual(len(late.recv_json("history")["ticks"]), 5)

    def test_broadcast_reaches_every_client(self):
        clients = [self.connect() for _ in range(5)]
        self.assertTrue(self.wait_for(lambda: len(self.server.clients) == 5))
        stamps = [c.recv_json("tick")["timestamp"] for c in clients]
        self.assertTrue(all(isinstance(s, float) for s in stamps))


class TestControlFrames(ServerTestCase):
    def test_ping_gets_pong_with_same_payload(self):
        c = self.connect()
        c.send_raw(wsp.encode_frame(wsp.OP_PING, b"are-you-there", mask=True))
        for _ in range(50):
            fin, op, payload = c.recv_frame()
            if op == wsp.OP_PONG:
                self.assertEqual(payload, b"are-you-there")
                return
        self.fail("no pong received")

    def test_close_handshake_echoes_code_and_removes_client(self):
        c = self.connect()
        self.assertTrue(self.wait_for(lambda: len(self.server.clients) == 1))
        c.send_raw(wsp.encode_close(wsp.CLOSE_NORMAL, mask=True))
        for _ in range(100):
            fin, op, payload = c.recv_frame()
            if op == wsp.OP_CLOSE:
                self.assertEqual(int.from_bytes(payload[:2], "big"), wsp.CLOSE_NORMAL)
                break
        else:
            self.fail("server did not answer the close frame")
        self.assertTrue(self.wait_for(lambda: len(self.server.clients) == 0))

    def test_invalid_close_code_answered_with_1002(self):
        c = self.connect()
        c.send_raw(wsp.encode_frame(wsp.OP_CLOSE, (1005).to_bytes(2, "big"), mask=True))
        for _ in range(100):
            fin, op, payload = c.recv_frame()
            if op == wsp.OP_CLOSE:
                self.assertEqual(int.from_bytes(payload[:2], "big"), wsp.CLOSE_PROTOCOL_ERROR)
                return
        self.fail("no close frame")

    def test_unmasked_frame_closes_with_1002(self):
        c = self.connect()
        c.send_raw(wsp.encode_text("not masked", mask=False))
        for _ in range(100):
            fin, op, payload = c.recv_frame()
            if op == wsp.OP_CLOSE:
                self.assertEqual(int.from_bytes(payload[:2], "big"), wsp.CLOSE_PROTOCOL_ERROR)
                return
        self.fail("no close frame")

    def test_abrupt_disconnect_is_cleaned_up(self):
        c = self.connect()
        self.assertTrue(self.wait_for(lambda: len(self.server.clients) == 1))
        c.sock.close()
        self.assertTrue(self.wait_for(lambda: len(self.server.clients) == 0))


class TestCommands(ServerTestCase):
    def test_set_threshold_is_broadcast_to_all_clients(self):
        a, b = self.connect(), self.connect()
        self.assertTrue(self.wait_for(lambda: len(self.server.clients) == 2))
        a.send_json({"type": "set_threshold", "value": 0.8})
        self.assertEqual(a.recv_json("config")["vibration_threshold"], 0.8)
        self.assertEqual(b.recv_json("config")["vibration_threshold"], 0.8)

    def test_threshold_changes_status_classification(self):
        c = self.connect()
        c.send_json({"type": "set_threshold", "value": 0.1})
        c.recv_json("config")
        tick = c.recv_json("tick")
        for r in tick["readings"]:
            self.assertEqual(r["status"], "warning" if r["vibration_g"] > 0.1 else "normal")

    def test_invalid_commands_return_errors_without_disconnecting(self):
        c = self.connect()
        for bad in ("not json", json.dumps({"type": "set_threshold", "value": 99}),
                    json.dumps({"type": "reboot"})):
            c.send_text(bad)
            self.assertEqual(c.recv_json("error")["type"], "error")
        self.assertEqual(c.recv_json("tick")["type"], "tick")  # still connected

    def test_fragmented_command_split_across_tcp_packets(self):
        c = self.connect()
        body = json.dumps({"type": "set_threshold", "value": 2.5}).encode()
        frames = (wsp.encode_frame(wsp.OP_TEXT, body[:10], mask=True, fin=False)
                  + wsp.encode_frame(wsp.OP_CONTINUATION, body[10:], mask=True))
        for i in range(0, len(frames), 2):  # 2 bytes per TCP send
            c.send_raw(frames[i:i + 2])
            time.sleep(0.001)
        self.assertEqual(c.recv_json("config")["vibration_threshold"], 2.5)


class TestHttp(ServerTestCase):
    def get(self, path):
        return urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=3)

    def test_dashboard_served_on_same_port(self):
        r = self.get("/")
        self.assertEqual(r.status, 200)
        self.assertIn(b"<canvas", r.read())

    def test_health_endpoint(self):
        body = json.loads(self.get("/health").read())
        self.assertEqual(body["status"], "ok")
        self.assertIn("clients", body)

    def test_unknown_path_404(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.get("/nope")
        self.assertEqual(ctx.exception.code, 404)

    def test_upgrade_on_wrong_path_is_404(self):
        with self.assertRaises(ConnectionError) as ctx:
            WebSocketClient("127.0.0.1", self.port, path="/not-ws")
        self.assertIn("404", str(ctx.exception))

    def test_new_client_sees_current_threshold(self):
        a = self.connect()
        a.send_json({"type": "set_threshold", "value": 3.3})
        a.recv_json("config")
        b = self.connect()
        self.assertEqual(b.recv_json("hello")["vibration_threshold"], 3.3)

    def test_bad_websocket_version_rejected_with_400(self):
        s = socket.create_connection(("127.0.0.1", self.port), timeout=3)
        s.sendall(b"GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                  b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 8\r\n\r\n")
        response = s.recv(4096)
        s.close()
        self.assertTrue(response.startswith(b"HTTP/1.1 400"))
        self.assertIn(b"Sec-WebSocket-Version: 13", response)


class TestSimulation(unittest.TestCase):
    def test_values_stay_bounded_over_a_long_run(self):
        """v1 used an upward-biased random walk, so vibration drifted up forever
        and the dashboard ended up permanently in 'warning'. The mean-reverting
        model must stay in a realistic range."""
        d = SimulatedDevice("x", 25.0, random.Random(7))
        readings = [d.step() for _ in range(20_000)]
        temps = [r["temperature_c"] for r in readings]
        vibs = [r["vibration_g"] for r in readings]
        self.assertTrue(18 < min(temps) and max(temps) < 32)
        self.assertLess(max(vibs), 3.0)
        self.assertLess(sum(vibs) / len(vibs), 0.6)
        self.assertTrue(any(v > 1.2 for v in vibs), "fault spikes should occasionally occur")


if __name__ == "__main__":
    unittest.main()
