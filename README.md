# Real-Time IoT Dashboard

[![CI](https://github.com/YOUR_GITHUB_USERNAME/iot-dashboard/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_GITHUB_USERNAME/iot-dashboard/actions)

A real-time IoT monitoring dashboard built around a custom WebSocket server implemented directly on Python TCP sockets.

The project focuses on **network protocols, WebSocket framing, concurrent clients and real-time sensor data delivery**.

## Highlights

- WebSocket server implemented from raw TCP sockets
- RFC 6455-style framing
- Masking and unmasking
- Fragmentation handling
- Ping/pong support
- Multiple concurrent clients
- Per-client outbound queues
- Simulated sensor/device streams
- Browser dashboard with live updates
- Automated protocol and server tests

## Architecture

```text
Simulated IoT devices
        |
        v
Python WebSocket server
        |
   +----+----+
   |         |
Client A   Client B ...
   |         |
   +----+----+
        |
        v
Browser dashboard
```

A per-client queue prevents one slow consumer from blocking updates to other connected clients.

## Tech Stack

- Python
- Raw TCP sockets
- WebSocket protocol
- JavaScript
- HTML/CSS
- unittest
- asyncio/threading depending on server path

## Run locally

Install dependencies:

```bash
pip install -r requirements.txt
```

Start the server:

```bash
python ws_server.py
```

Open the dashboard:

```text
dashboard.html
```

The repository includes demo screenshots/GIFs under `docs/`.

## Testing

```bash
python -m unittest discover -s tests -v
```

The tests cover WebSocket protocol behavior and server functionality.

## Why build WebSockets from scratch?

Using Socket.IO or a high-level WebSocket framework would hide important protocol details. This project intentionally implements the protocol layer to demonstrate understanding of:

- TCP streams
- framing
- masking
- fragmentation
- control frames
- connection lifecycle
- backpressure
- concurrent clients

## Deployment note

For public deployment, the server needs a hosting environment that supports long-lived WebSocket connections. The production deployment should use a platform/configuration with WebSocket support rather than treating it as a normal short-lived HTTP API.
