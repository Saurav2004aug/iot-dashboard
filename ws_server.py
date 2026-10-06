"""
Real-Time IoT Sensor Dashboard: WebSocket server built on raw TCP sockets
---------------------------------------------------------------------------
Implements the WebSocket protocol (RFC 6455) from scratch on Python's
`socket` module (see ws_protocol.py). No websocket library is used.

One port serves both:
  GET /             -> the dashboard page (plain HTTP)
  GET /health       -> JSON health/metrics
  GET /ws (Upgrade) -> the WebSocket sensor stream

It simulates a fleet of ESP32-style devices streaming temperature, humidity
and vibration. It broadcasts readings as JSON to every connected client, and
accepts commands back from the browser (e.g. changing the alert threshold).

Concurrency design:
  - one accept thread
  - per client: a reader thread and a writer thread with a bounded outbound
    queue. A slow client that falls behind is disconnected instead of
    stalling the broadcast to everyone else (backpressure).
  - one simulator/broadcast thread, which also sends heartbeat pings and
    drops clients that stop answering.

Run:
    python ws_server.py            # then open http://127.0.0.1:9001/
"""

import json
import os
import queue
import random
import socket
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field

import ws_protocol as wsp

HERE = os.path.dirname(os.path.abspath(__file__))
HISTORY_TICKS = 120          # readings kept for newly connected clients
OUTBOUND_QUEUE_SIZE = 64     # frames buffered per client before it's considered too slow


# ---------------------------------------------------------------------------
# Sensor simulation
# ---------------------------------------------------------------------------

@dataclass
class SimulatedDevice:
    """Mean-reverting (Ornstein-Uhlenbeck) random walk, so values drift
    realistically but never run away, plus occasional vibration spikes that
    look like a machine fault."""
    device_id: str
    base_temp: float
    rng: random.Random
    temperature: float = field(init=False)
    humidity: float = field(init=False)
    vibration: float = field(init=False)
    spike_ticks: int = 0

    def __post_init__(self):
        self.temperature = self.base_temp
        self.humidity = 45.0
        self.vibration = 0.3

    @staticmethod
    def _revert(value, mean, strength, noise, rng):
        return value + strength * (mean - value) + rng.gauss(0, noise)

    def step(self) -> dict:
        r = self.rng
        self.temperature = self._revert(self.temperature, self.base_temp, 0.05, 0.15, r)
        self.humidity = min(100.0, max(0.0, self._revert(self.humidity, 45.0, 0.05, 0.6, r)))

        if self.spike_ticks == 0 and r.random() < 0.02:
            self.spike_ticks = r.randint(3, 8)          # start a fault event
        target = 1.6 if self.spike_ticks else 0.3
        if self.spike_ticks:
            self.spike_ticks -= 1
        self.vibration = max(0.0, self._revert(self.vibration, target, 0.35, 0.04, r))

        return {
            "device_id": self.device_id,
            "temperature_c": round(self.temperature, 2),
            "humidity_pct": round(self.humidity, 1),
            "vibration_g": round(self.vibration, 3),
        }


# ---------------------------------------------------------------------------
# Client connection
# ---------------------------------------------------------------------------

class Client:
    def __init__(self, sock: socket.socket, addr):
        self.sock = sock
        self.addr = addr
        self.outbound: "queue.Queue[bytes | None]" = queue.Queue(OUTBOUND_QUEUE_SIZE)
        self.last_seen = time.monotonic()
        self.closed = threading.Event()

    def send(self, frame: bytes) -> bool:
        """Queue a frame without blocking. False means the client is too slow."""
        if self.closed.is_set():
            return False
        try:
            self.outbound.put_nowait(frame)
            return True
        except queue.Full:
            return False

    def writer_loop(self):
        try:
            while True:
                frame = self.outbound.get()
                if frame is None:
                    break
                self.sock.sendall(frame)
        except OSError:
            pass
        finally:
            self.closed.set()
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()

    def close(self, code: int = wsp.CLOSE_NORMAL, reason: str = ""):
        """Send a close frame (best effort), then stop the writer."""
        if self.closed.is_set():
            return
        try:
            self.outbound.put_nowait(wsp.encode_close(code, reason))
        except queue.Full:
            pass
        try:
            self.outbound.put_nowait(None)
        except queue.Full:
            self.closed.set()
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

class SensorServer:
    def __init__(self, host="127.0.0.1", port=9001, tick_seconds=1.0,
                 ping_interval=20.0, device_count=3, seed=None):
        self.host = host
        self.port = port
        self.tick_seconds = tick_seconds
        self.ping_interval = ping_interval
        self.vibration_threshold = 1.2
        rng = random.Random(seed)
        self.devices = [SimulatedDevice(f"esp32-{i + 1:02d}", 22.0 + 3 * i, rng)
                        for i in range(device_count)]
        self.history: deque = deque(maxlen=HISTORY_TICKS)
        self.clients: set[Client] = set()
        self.lock = threading.Lock()
        self.started_at = time.time()
        self.messages_sent = 0
        self._stop = threading.Event()
        self._sock: socket.socket | None = None

    # ---- lifecycle ----

    def start(self) -> int:
        """Bind and start background threads. Returns the bound port (useful with port=0)."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(64)
        self.port = self._sock.getsockname()[1]
        threading.Thread(target=self._accept_loop, daemon=True, name="accept").start()
        threading.Thread(target=self._simulate_loop, daemon=True, name="simulator").start()
        return self.port

    def stop(self):
        self._stop.set()
        with self.lock:
            clients = list(self.clients)
        for c in clients:
            c.close(wsp.CLOSE_GOING_AWAY, "server shutting down")
        if self._sock:
            self._sock.close()

    # ---- networking ----

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, addr = self._sock.accept()
            except OSError:
                break
            threading.Thread(target=self._handle_connection, args=(conn, addr), daemon=True).start()

    def _handle_connection(self, conn: socket.socket, addr):
        conn.settimeout(10)  # handshake must finish promptly
        try:
            head, leftover = wsp.read_http_head(conn)
            method, path, headers = wsp.parse_http_request(head)
        except (wsp.ProtocolError, wsp.ConnectionClosed, OSError, UnicodeDecodeError):
            conn.close()
            return

        is_upgrade = headers.get("upgrade", "").lower() == "websocket"
        if not is_upgrade:
            self._serve_http(conn, method, path)
            return

        if path.split("?")[0] != "/ws":
            self._http_response(conn, 404, "text/plain", b"WebSocket endpoint is /ws")
            return

        error = None
        if method != "GET":
            error = "WebSocket handshake must use GET"
        elif "upgrade" not in headers.get("connection", "").lower():
            error = "Missing 'Connection: Upgrade'"
        elif headers.get("sec-websocket-version") != "13":
            error = "Unsupported Sec-WebSocket-Version (need 13)"
        elif not headers.get("sec-websocket-key"):
            error = "Missing Sec-WebSocket-Key"
        if error:
            self._http_response(conn, 400, "text/plain", error.encode(),
                                extra="Sec-WebSocket-Version: 13\r\n")
            return

        accept = wsp.compute_accept_key(headers["sec-websocket-key"])
        conn.sendall((
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        ).encode("ascii"))
        conn.settimeout(None)
        self._run_client(Client(conn, addr), leftover)

    def _run_client(self, client: Client, leftover: bytes):
        threading.Thread(target=client.writer_loop, daemon=True).start()

        # Build hello, queue hello + history, and join the broadcast set all
        # under one lock. The client then always sees hello -> history -> ticks
        # with no gap or duplicate tick, and can't miss a threshold change
        # (set_threshold takes the same lock).
        with self.lock:
            client.send(wsp.encode_text(json.dumps({
                "type": "hello",
                "vibration_threshold": self.vibration_threshold,
                "tick_seconds": self.tick_seconds,
                "devices": [d.device_id for d in self.devices],
            })))
            client.send(wsp.encode_text(json.dumps({"type": "history", "ticks": list(self.history)})))
            self.clients.add(client)
            total = len(self.clients)
        print(f"[+] {client.addr} connected ({total} client(s))")

        reader = wsp.FrameReader(client.sock, expect_masked=True, initial=leftover)

        def on_control(opcode, payload):
            client.last_seen = time.monotonic()
            if opcode == wsp.OP_PING:
                client.send(wsp.encode_frame(wsp.OP_PONG, payload))

        close_code = wsp.CLOSE_NORMAL
        try:
            while not client.closed.is_set():
                opcode, payload = reader.read_message(on_control=on_control)
                client.last_seen = time.monotonic()
                if opcode == wsp.OP_CLOSE:
                    # Validate, then echo the peer's code to complete the close
                    # handshake. An invalid close frame raises ProtocolError -> 1002/1007.
                    code, _reason = wsp.parse_close_payload(payload)
                    close_code = code or wsp.CLOSE_NORMAL
                    break
                if opcode == wsp.OP_TEXT:
                    self._handle_command(client, payload.decode("utf-8"))
        except wsp.ProtocolError as e:
            close_code = e.close_code
            print(f"[!] {client.addr} protocol error: {e}")
        except (wsp.ConnectionClosed, OSError):
            close_code = None
        finally:
            with self.lock:
                self.clients.discard(client)
                total = len(self.clients)
            if close_code is not None:
                client.close(close_code)
            else:
                client.close()
            print(f"[-] {client.addr} disconnected ({total} client(s))")

    def _handle_command(self, client: Client, text: str):
        try:
            msg = json.loads(text)
        except json.JSONDecodeError:
            client.send(wsp.encode_text(json.dumps({"type": "error", "error": "invalid JSON"})))
            return

        if msg.get("type") == "set_threshold":
            value = msg.get("value")
            if not isinstance(value, (int, float)) or not 0.1 <= value <= 5:
                client.send(wsp.encode_text(json.dumps(
                    {"type": "error", "error": "threshold must be a number in [0.1, 5]"})))
                return
            with self.lock:
                self.vibration_threshold = float(value)
            self.broadcast({"type": "config", "vibration_threshold": float(value)})
        else:
            client.send(wsp.encode_text(json.dumps(
                {"type": "error", "error": f"unknown command {msg.get('type')!r}"})))

    def broadcast(self, message: dict, clients: list | None = None):
        frame = wsp.encode_text(json.dumps(message))
        if clients is None:
            with self.lock:
                clients = list(self.clients)  # snapshot; don't hold the lock while sending
        for c in clients:
            if c.send(frame):
                self.messages_sent += 1
            else:
                print(f"[!] {c.addr} too slow, disconnecting")
                with self.lock:
                    self.clients.discard(c)
                c.close(wsp.CLOSE_GOING_AWAY, "client too slow")

    # ---- simulation + heartbeat ----

    def _simulate_loop(self):
        next_tick = time.monotonic()
        last_ping = time.monotonic()
        while not self._stop.is_set():
            next_tick += self.tick_seconds
            readings = [d.step() for d in self.devices]
            for r in readings:
                r["status"] = "warning" if r["vibration_g"] > self.vibration_threshold else "normal"
            tick = {"timestamp": time.time(), "readings": readings}
            with self.lock:  # same lock as client registration -> no gaps/duplicates
                self.history.append(tick)
                recipients = list(self.clients)
            self.broadcast({"type": "tick", **tick}, recipients)

            now = time.monotonic()
            if now - last_ping >= self.ping_interval:
                last_ping = now
                self._heartbeat(now)

            self._stop.wait(max(0.0, next_tick - time.monotonic()))

    def _heartbeat(self, now: float):
        with self.lock:
            clients = list(self.clients)
        for c in clients:
            if now - c.last_seen > 2 * self.ping_interval:
                print(f"[!] {c.addr} missed heartbeats, disconnecting")
                with self.lock:
                    self.clients.discard(c)
                c.close(wsp.CLOSE_GOING_AWAY, "heartbeat timeout")
            else:
                c.send(wsp.encode_frame(wsp.OP_PING, b"hb"))

    # ---- plain HTTP ----

    def _serve_http(self, conn, method, path):
        path = path.split("?")[0]
        if method != "GET":
            self._http_response(conn, 405, "text/plain", b"Method Not Allowed")
        elif path in ("/", "/dashboard.html"):
            with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                self._http_response(conn, 200, "text/html; charset=utf-8", f.read())
        elif path == "/health":
            with self.lock:
                n = len(self.clients)
            body = json.dumps({
                "status": "ok",
                "clients": n,
                "uptime_seconds": round(time.time() - self.started_at, 1),
                "messages_sent": self.messages_sent,
            }).encode()
            self._http_response(conn, 200, "application/json", body)
        else:
            self._http_response(conn, 404, "text/plain", b"Not Found")

    @staticmethod
    def _http_response(conn, status, content_type, body: bytes, extra: str = ""):
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed"}[status]
        try:
            conn.sendall((
                f"HTTP/1.1 {status} {reason}\r\n"
                f"Content-Type: {content_type}\r\n"
                f"Content-Length: {len(body)}\r\n"
                f"{extra}"
                "Connection: close\r\n\r\n"
            ).encode("ascii") + body)
        except OSError:
            pass
        finally:
            conn.close()


def main():
    sys.stdout.reconfigure(line_buffering=True)  # logs show up immediately
    server = SensorServer(
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", 9001)),
    )
    port = server.start()
    print(f"Dashboard:  http://{server.host}:{port}/")
    print(f"WebSocket:  ws://{server.host}:{port}/ws")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()


if __name__ == "__main__":
    main()
