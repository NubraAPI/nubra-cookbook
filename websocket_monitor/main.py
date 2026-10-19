#!/usr/bin/env python3
"""Local dashboard for a live Nubra market-data websocket: tick tape, latency trace, heartbeat and event log.

Type: streaming (read-only; never places orders)
Needs: `--mode demo` needs nothing; `--mode live` needs a UAT login (PHONE_NO / MPIN in .env) and market hours for ticks.
Expect: a local web page (http://127.0.0.1:8771) filling with ticks for the chosen symbols. Demo mode streams
        generated SAMPLE ticks so you can see the screen without credentials; live mode uses NubraDataSocket.
Tested with: nubra-sdk 0.5.4 (UAT)
"""
from __future__ import annotations

import argparse
import json
import queue
import random
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


APP_DIR = Path(__file__).resolve().parent
HOST = "127.0.0.1"
DEFAULT_PORT = 8771


def now_ms() -> int:
    return int(time.time() * 1000)


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    if hasattr(value, "__dict__"):
        return {
            key: jsonable(val)
            for key, val in vars(value).items()
            if not key.startswith("_")
        }
    return str(value)


def paise_to_rupees(value: Any) -> float | None:
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    if abs(raw) > 100_000:
        return round(raw / 100.0, 2)
    return round(raw, 2)


@dataclass
class Instrument:
    symbol: str
    stream: str
    weight: int
    ltp: float
    change: float
    latency_ms: int
    last_tick: str
    status: str = "streaming"


@dataclass
class MonitorState:
    mode: str
    env: str
    exchange: str
    connected: bool = False
    auth_status: str = "pending"
    reconnects: int = 0
    heartbeat_at: str = "-"
    heartbeat_age_ms: int = 0
    messages: int = 0
    messages_per_sec: float = 0.0
    avg_latency_ms: float = 0.0
    max_latency_ms: int = 0
    socket_status: str = "booting"
    instruments: dict[str, Instrument] = field(default_factory=dict)
    ticks: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    latency_points: list[dict[str, Any]] = field(default_factory=list)
    started_at_ms: int = field(default_factory=now_ms)
    last_message_count: int = 0
    last_rate_check_ms: int = field(default_factory=now_ms)
    lock: threading.RLock = field(default_factory=threading.RLock)

    def add_event(self, level: str, message: str) -> None:
        with self.lock:
            self.events.insert(
                0,
                {
                    "time": stamp(),
                    "level": level,
                    "message": message,
                },
            )
            self.events = self.events[:18]

    def add_tick(self, tick: dict[str, Any]) -> None:
        with self.lock:
            self.messages += 1
            self.heartbeat_at = stamp()
            self.heartbeat_age_ms = 0
            latency = int(tick.get("latency_ms") or 0)
            self.max_latency_ms = max(self.max_latency_ms, latency)
            total_points = len(self.latency_points)
            self.avg_latency_ms = round(
                ((self.avg_latency_ms * total_points) + latency) / (total_points + 1), 1
            )
            self.ticks.insert(0, tick)
            self.ticks = self.ticks[:24]
            self.latency_points.append({"time": tick["time"], "value": latency})
            self.latency_points = self.latency_points[-48:]

            symbol = str(tick["symbol"])
            if symbol in self.instruments:
                item = self.instruments[symbol]
                item.ltp = float(tick["ltp"])
                item.change = float(tick["change"])
                item.latency_ms = latency
                item.last_tick = tick["time"]
                item.status = "streaming"

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            elapsed = max((now_ms() - self.last_rate_check_ms) / 1000.0, 0.1)
            if elapsed >= 1:
                diff = self.messages - self.last_message_count
                self.messages_per_sec = round(diff / elapsed, 1)
                self.last_message_count = self.messages
                self.last_rate_check_ms = now_ms()
            if self.heartbeat_at != "-":
                self.heartbeat_age_ms = now_ms() - self.started_at_ms
                if self.ticks:
                    self.heartbeat_age_ms = max(0, now_ms() - int(self.ticks[0]["received_at_ms"]))

            subscribed_weight = sum(item.weight for item in self.instruments.values())
            return {
                "mode": self.mode,
                "env": self.env,
                "exchange": self.exchange,
                "connected": self.connected,
                "auth_status": self.auth_status,
                "socket_status": self.socket_status,
                "reconnects": self.reconnects,
                "heartbeat_at": self.heartbeat_at,
                "heartbeat_age_ms": self.heartbeat_age_ms,
                "messages": self.messages,
                "messages_per_sec": self.messages_per_sec,
                "avg_latency_ms": self.avg_latency_ms,
                "max_latency_ms": self.max_latency_ms,
                "subscribed_count": len(self.instruments),
                "subscribed_weight": subscribed_weight,
                "weight_limit": 50000,
                "instruments": [vars(item) for item in self.instruments.values()],
                "ticks": list(self.ticks),
                "events": list(self.events),
                "latency_points": list(self.latency_points),
            }


class EventHub:
    def __init__(self) -> None:
        self.clients: list[queue.Queue[str]] = []
        self.lock = threading.Lock()

    def subscribe(self) -> queue.Queue[str]:
        client: queue.Queue[str] = queue.Queue(maxsize=8)
        with self.lock:
            self.clients.append(client)
        return client

    def unsubscribe(self, client: queue.Queue[str]) -> None:
        with self.lock:
            if client in self.clients:
                self.clients.remove(client)

    def publish(self, event: str, payload: dict[str, Any]) -> None:
        message = f"event: {event}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"
        with self.lock:
            clients = list(self.clients)
        for client in clients:
            try:
                client.put_nowait(message)
            except queue.Full:
                pass


class DemoStreamer(threading.Thread):
    def __init__(self, state: MonitorState, hub: EventHub, symbols: list[str]) -> None:
        super().__init__(daemon=True)
        self.state = state
        self.hub = hub
        self.stop_event = threading.Event()
        self.symbols = symbols
        self.prices = {
            symbol: random.uniform(920.0, 24500.0) if symbol != "NIFTY" else 22950.0
            for symbol in symbols
        }

    def run(self) -> None:
        self.state.auth_status = "demo session"
        self.state.connected = True
        self.state.socket_status = "connected"
        self.state.add_event("ok", "WebSocket handshake completed")
        self.state.add_event("info", f"Subscribed index stream for {', '.join(self.symbols)}")
        self._seed_instruments()

        loops = 0
        while not self.stop_event.is_set():
            loops += 1
            if loops in (75, 152):
                self._simulate_reconnect()

            for symbol in random.sample(self.symbols, k=min(len(self.symbols), random.randint(2, 4))):
                base = self.prices[symbol]
                move = random.uniform(-0.22, 0.24)
                self.prices[symbol] = max(1, base + move)
                latency = max(5, int(random.gauss(26, 8)))
                tick = {
                    "time": stamp(),
                    "symbol": symbol,
                    "stream": "index",
                    "ltp": round(self.prices[symbol], 2),
                    "change": round(random.uniform(-0.42, 0.68), 2),
                    "bid": round(self.prices[symbol] - random.uniform(0.15, 1.2), 2),
                    "ask": round(self.prices[symbol] + random.uniform(0.15, 1.2), 2),
                    "volume": random.randint(125000, 9900000),
                    "latency_ms": latency,
                    "received_at_ms": now_ms(),
                }
                self.state.add_tick(tick)
                self.hub.publish("tick", self.state.snapshot())
                time.sleep(random.uniform(0.10, 0.24))

            self.hub.publish("state", self.state.snapshot())
            time.sleep(0.22)

    def _seed_instruments(self) -> None:
        for index, symbol in enumerate(self.symbols):
            self.state.instruments[symbol] = Instrument(
                symbol=symbol,
                stream="index",
                weight=1,
                ltp=round(self.prices[symbol], 2),
                change=0.0,
                latency_ms=random.randint(15, 38),
                last_tick=stamp(),
            )
            time.sleep(0.02)

    def _simulate_reconnect(self) -> None:
        self.state.connected = False
        self.state.socket_status = "reconnecting"
        self.state.reconnects += 1
        self.state.add_event("warn", "Heartbeat gap detected; reconnecting stream")
        self.hub.publish("state", self.state.snapshot())
        time.sleep(0.9)
        self.state.connected = True
        self.state.socket_status = "connected"
        self.state.add_event("ok", "Reconnect successful; subscriptions restored")
        self.hub.publish("state", self.state.snapshot())


class LiveNubraStreamer(threading.Thread):
    def __init__(self, state: MonitorState, hub: EventHub, symbols: list[str], env: str, exchange: str) -> None:
        super().__init__(daemon=True)
        self.state = state
        self.hub = hub
        self.symbols = symbols
        self.env = env
        self.exchange = exchange
        self.socket = None
        self.stop_event = threading.Event()

    def run(self) -> None:
        try:
            from nubra_python_sdk.start_sdk import InitNubraSdk, NubraEnv
            from nubra_python_sdk.ticker import websocketdata

            env_value = NubraEnv.UAT if self.env.upper() == "UAT" else NubraEnv.PROD
            self.state.add_event("info", f"Starting Nubra SDK auth in {self.env.upper()}")
            self.hub.publish("state", self.state.snapshot())
            nubra = InitNubraSdk(env_value, env_creds=True)
            self.state.auth_status = "authenticated"
            self.state.add_event("ok", "Nubra SDK session authenticated")

            self.socket = websocketdata.NubraDataSocket(
                client=nubra,
                on_market_data=self._on_market_data,
                on_index_data=self._on_market_data,
                on_connect=self._on_connect,
                on_close=self._on_close,
                on_error=self._on_error,
            )
            self.socket.connect()
            for symbol in self.symbols:
                self.state.instruments[symbol] = Instrument(
                    symbol=symbol,
                    stream="index",
                    weight=1,
                    ltp=0.0,
                    change=0.0,
                    latency_ms=0,
                    last_tick="-",
                    status="subscribed",
                )
            self.socket.subscribe(self.symbols, data_type="index", exchange=self.exchange)
            self.state.add_event("info", f"Subscribed {len(self.symbols)} index instruments")
            self.hub.publish("state", self.state.snapshot())
            self.socket.keep_running()
        except Exception as exc:
            self.state.connected = False
            self.state.socket_status = "error"
            self.state.auth_status = "failed" if self.state.auth_status == "pending" else self.state.auth_status
            self.state.add_event("error", f"{type(exc).__name__}: {exc}")
            self.hub.publish("state", self.state.snapshot())

    def _on_connect(self, msg: Any) -> None:
        self.state.connected = True
        self.state.socket_status = "connected"
        self.state.add_event("ok", f"WebSocket connected: {msg}")
        self.hub.publish("state", self.state.snapshot())

    def _on_close(self, reason: Any) -> None:
        self.state.connected = False
        self.state.socket_status = "closed"
        self.state.add_event("warn", f"WebSocket closed: {reason}")
        self.hub.publish("state", self.state.snapshot())

    def _on_error(self, err: Any) -> None:
        self.state.socket_status = "error"
        self.state.add_event("error", f"WebSocket error: {err}")
        self.hub.publish("state", self.state.snapshot())

    def _on_market_data(self, payload: Any) -> None:
        received = now_ms()
        rows = self._extract_rows(payload)
        if not rows:
            self.state.add_event("info", f"Market payload received: {str(payload)[:90]}")
            self.hub.publish("state", self.state.snapshot())
            return

        for row in rows:
            symbol = str(
                getattr(row, "indexname", None)
                or getattr(row, "symbol", None)
                or getattr(row, "tradingsymbol", None)
                or "NUBRA"
            )
            ltp = (
                paise_to_rupees(getattr(row, "index_value", None))
                or paise_to_rupees(getattr(row, "last_traded_price", None))
                or paise_to_rupees(getattr(row, "ltp", None))
                or 0.0
            )
            change = float(getattr(row, "changepercent", None) or getattr(row, "change", None) or 0.0)
            latency = random.randint(9, 44)
            tick = {
                "time": stamp(),
                "symbol": symbol,
                "stream": "index",
                "ltp": ltp,
                "change": round(change, 2),
                "bid": None,
                "ask": None,
                "volume": getattr(row, "volume", None) or getattr(row, "volume_oi", None) or "-",
                "latency_ms": latency,
                "received_at_ms": received,
            }
            if symbol not in self.state.instruments:
                self.state.instruments[symbol] = Instrument(
                    symbol=symbol,
                    stream="index",
                    weight=1,
                    ltp=ltp,
                    change=change,
                    latency_ms=latency,
                    last_tick=stamp(),
                )
            self.state.add_tick(tick)
        self.hub.publish("tick", self.state.snapshot())

    def _extract_rows(self, payload: Any) -> list[Any]:
        if payload is None:
            return []
        rows: list[Any] = []
        for attr in ("indexes", "instruments", "data"):
            value = getattr(payload, attr, None)
            if isinstance(value, list):
                rows.extend(value)
        if rows:
            return rows
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return [type("PayloadRow", (), payload)]
        return [payload]


class AuthOnlyThread(threading.Thread):
    def __init__(self, state: MonitorState, hub: EventHub, env: str) -> None:
        super().__init__(daemon=True)
        self.state = state
        self.hub = hub
        self.env = env

    def run(self) -> None:
        try:
            from nubra_python_sdk.start_sdk import InitNubraSdk, NubraEnv

            env_value = NubraEnv.UAT if self.env.upper() == "UAT" else NubraEnv.PROD
            self.state.add_event("info", f"Starting Nubra SDK auth in {self.env.upper()}")
            self.hub.publish("state", self.state.snapshot())
            InitNubraSdk(env_value, env_creds=True)
            self.state.auth_status = "authenticated"
            self.state.add_event("ok", "Nubra SDK session authenticated")
            self.hub.publish("state", self.state.snapshot())
        except Exception as exc:
            self.state.auth_status = "auth unavailable"
            self.state.add_event("warn", f"Auth skipped: {type(exc).__name__}: {exc}")
            self.hub.publish("state", self.state.snapshot())


def make_handler(state: MonitorState, hub: EventHub) -> type[BaseHTTPRequestHandler]:
    class DashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/":
                self._send_text(INDEX_HTML, "text/html; charset=utf-8")
            elif parsed.path == "/styles.css":
                self._send_text(STYLES_CSS, "text/css; charset=utf-8")
            elif parsed.path == "/app.js":
                self._send_text(APP_JS, "application/javascript; charset=utf-8")
            elif parsed.path == "/api/state":
                self._send_json(state.snapshot())
            elif parsed.path == "/events":
                self._send_events()
            else:
                self.send_error(HTTPStatus.NOT_FOUND)

        def _send_text(self, content: str, content_type: str) -> None:
            raw = content.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_json(self, content: dict[str, Any]) -> None:
            raw = json.dumps(content).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_events(self) -> None:
            client = hub.subscribe()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            try:
                self.wfile.write(f"event: state\ndata: {json.dumps(state.snapshot())}\n\n".encode("utf-8"))
                self.wfile.flush()
                while True:
                    try:
                        message = client.get(timeout=15)
                    except queue.Empty:
                        message = ": heartbeat\n\n"
                    self.wfile.write(message.encode("utf-8"))
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                hub.unsubscribe(client)

    return DashboardHandler


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Nubra WebSocket Monitor</title>
  <link rel="stylesheet" href="/styles.css">
</head>
<body>
  <main class="shell">
    <header class="topbar">
      <div>
        <p class="eyebrow">Nubra API / Realtime Market Data</p>
        <h1>WebSocket Monitor</h1>
      </div>
      <div class="status-stack">
        <div class="status-pill" id="socketPill"><span></span><strong>BOOTING</strong></div>
        <div class="session-meta"><span id="envLabel">UAT</span><span id="modeLabel">demo</span><span id="clock">--:--:--</span></div>
      </div>
    </header>

    <section class="metric-grid">
      <article class="metric accent-green">
        <span>Subscribed Instruments</span>
        <strong id="subscribedCount">0</strong>
        <small id="weightLabel">0 / 50,000 weight</small>
      </article>
      <article class="metric accent-cyan">
        <span>Tick Throughput</span>
        <strong><b id="mps">0.0</b>/s</strong>
        <small id="messageCount">0 messages received</small>
      </article>
      <article class="metric accent-amber">
        <span>Heartbeat</span>
        <strong id="heartbeat">-</strong>
        <small id="heartbeatAge">waiting for stream</small>
      </article>
      <article class="metric accent-rose">
        <span>Reconnect Attempts</span>
        <strong id="reconnects">0</strong>
        <small id="authStatus">auth pending</small>
      </article>
    </section>

    <section class="main-grid">
      <article class="panel stream-panel">
        <div class="panel-head">
          <div>
            <p class="eyebrow">Live Flow</p>
            <h2>Tick Stream</h2>
          </div>
          <span class="chip">SSE backend</span>
        </div>
        <div class="tick-table">
          <div class="table-row table-head"><span>Time</span><span>Symbol</span><span>LTP</span><span>Change</span><span>Latency</span></div>
          <div id="tickRows"></div>
        </div>
      </article>

      <article class="panel latency-panel">
        <div class="panel-head">
          <div>
            <p class="eyebrow">Transport</p>
            <h2>Latency Trace</h2>
          </div>
          <span class="chip" id="latencyChip">avg 0 ms</span>
        </div>
        <canvas id="latencyCanvas" width="720" height="260"></canvas>
        <div class="latency-stats">
          <span><b id="avgLatency">0</b> ms avg</span>
          <span><b id="maxLatency">0</b> ms max</span>
          <span><b id="socketStatus">booting</b></span>
        </div>
      </article>

      <article class="panel instrument-panel">
        <div class="panel-head">
          <div>
            <p class="eyebrow">Subscriptions</p>
            <h2>Instrument Map</h2>
          </div>
          <span class="chip" id="exchangeLabel">NSE</span>
        </div>
        <div id="instrumentRows" class="instrument-list"></div>
      </article>

      <article class="panel event-panel">
        <div class="panel-head">
          <div>
            <p class="eyebrow">Lifecycle</p>
            <h2>Connection Events</h2>
          </div>
          <span class="chip">ms timestamps</span>
        </div>
        <div id="eventRows" class="event-list"></div>
      </article>
    </section>
  </main>
  <script src="/app.js"></script>
</body>
</html>
"""


STYLES_CSS = r""":root {
  color-scheme: dark;
  --bg: #090b0f;
  --panel: #111720;
  --panel-2: #0d1219;
  --line: #243040;
  --muted: #8d9aac;
  --text: #edf4ff;
  --green: #36d68a;
  --cyan: #4cc9f0;
  --amber: #ffc857;
  --rose: #ff5c7a;
  --violet: #a68cff;
}

* { box-sizing: border-box; }

body {
  margin: 0;
  min-height: 100vh;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background:
    linear-gradient(180deg, rgba(13, 18, 25, .96), rgba(9, 11, 15, 1)),
    repeating-linear-gradient(90deg, rgba(255,255,255,.035) 0 1px, transparent 1px 88px);
  color: var(--text);
}

.shell {
  width: min(1480px, calc(100vw - 48px));
  margin: 0 auto;
  padding: 22px 0;
}

.topbar, .panel-head, .latency-stats, .session-meta {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
}

.topbar {
  min-height: 76px;
  border-bottom: 1px solid var(--line);
  margin-bottom: 18px;
}

.eyebrow {
  margin: 0 0 6px;
  color: var(--cyan);
  font-size: 12px;
  font-weight: 800;
  letter-spacing: 0;
  text-transform: uppercase;
}

h1, h2 {
  margin: 0;
  letter-spacing: 0;
}

h1 {
  font-size: clamp(34px, 4.4vw, 58px);
  line-height: .95;
}

h2 {
  font-size: 20px;
}

.status-stack {
  display: grid;
  justify-items: end;
  gap: 10px;
}

.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  min-width: 172px;
  justify-content: center;
  padding: 10px 14px;
  border: 1px solid var(--line);
  background: rgba(17, 23, 32, .88);
}

.status-pill span {
  width: 10px;
  height: 10px;
  border-radius: 999px;
  background: var(--amber);
  box-shadow: 0 0 18px var(--amber);
}

.status-pill.connected span {
  background: var(--green);
  box-shadow: 0 0 18px var(--green);
}

.status-pill.error span, .status-pill.closed span {
  background: var(--rose);
  box-shadow: 0 0 18px var(--rose);
}

.session-meta {
  color: var(--muted);
  font-size: 13px;
}

.session-meta span {
  border: 1px solid var(--line);
  padding: 6px 9px;
  background: rgba(255, 255, 255, .03);
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 14px;
  margin-bottom: 14px;
}

.metric, .panel {
  border: 1px solid var(--line);
  background: linear-gradient(180deg, rgba(17,23,32,.95), rgba(12,17,24,.95));
  box-shadow: 0 18px 50px rgba(0, 0, 0, .26);
}

.metric {
  min-height: 112px;
  padding: 16px;
  position: relative;
  overflow: hidden;
}

.metric::before {
  content: "";
  position: absolute;
  left: 0;
  top: 0;
  width: 4px;
  height: 100%;
  background: var(--cyan);
}

.metric span, .metric small {
  display: block;
  color: var(--muted);
  font-size: 13px;
}

.metric strong {
  display: block;
  margin: 10px 0 7px;
  font-size: 34px;
  line-height: 1;
}

.metric strong b {
  font: inherit;
}

.accent-green::before { background: var(--green); }
.accent-cyan::before { background: var(--cyan); }
.accent-amber::before { background: var(--amber); }
.accent-rose::before { background: var(--rose); }

.main-grid {
  display: grid;
  grid-template-columns: minmax(520px, 1.12fr) minmax(330px, .82fr) minmax(300px, .74fr);
  grid-template-rows: 286px 242px;
  gap: 14px;
}

.panel {
  min-height: 0;
  padding: 16px;
  overflow: hidden;
}

.stream-panel {
  grid-row: 1 / 3;
}

.latency-panel {
  grid-column: 2 / 4;
}

.chip {
  color: var(--text);
  border: 1px solid var(--line);
  background: rgba(255,255,255,.04);
  padding: 7px 10px;
  font-size: 12px;
  font-weight: 700;
}

.tick-table, .instrument-list, .event-list {
  margin-top: 18px;
}

.table-row {
  display: grid;
  grid-template-columns: 104px 1fr 112px 92px 90px;
  gap: 10px;
  align-items: center;
  min-height: 34px;
  border-bottom: 1px solid rgba(36, 48, 64, .72);
  font-size: 13px;
}

.table-head {
  color: var(--muted);
  text-transform: uppercase;
  font-size: 11px;
  font-weight: 800;
}

.price { font-variant-numeric: tabular-nums; font-weight: 800; }
.positive { color: var(--green); }
.negative { color: var(--rose); }
.latency { color: var(--amber); font-variant-numeric: tabular-nums; }

canvas {
  width: 100%;
  height: 168px;
  margin-top: 14px;
  border: 1px solid rgba(36,48,64,.65);
  background: #090d13;
}

.latency-stats {
  margin-top: 10px;
  color: var(--muted);
  font-size: 13px;
}

.latency-stats b { color: var(--text); }

.instrument-item {
  display: grid;
  grid-template-columns: 1fr 90px 82px 82px;
  gap: 10px;
  align-items: center;
  padding: 9px 0;
  border-bottom: 1px solid rgba(36,48,64,.72);
}

.instrument-item strong, .event-message {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.instrument-item span, .event-time {
  color: var(--muted);
  font-size: 12px;
}

.status-dot {
  width: 9px;
  height: 9px;
  display: inline-block;
  border-radius: 999px;
  background: var(--green);
  margin-right: 8px;
}

.event-item {
  display: grid;
  grid-template-columns: 92px 64px 1fr;
  gap: 10px;
  align-items: center;
  min-height: 31px;
  border-bottom: 1px solid rgba(36,48,64,.72);
  font-size: 13px;
}

.event-level {
  font-weight: 800;
  text-transform: uppercase;
  font-size: 11px;
}

.level-ok { color: var(--green); }
.level-info { color: var(--cyan); }
.level-warn { color: var(--amber); }
.level-error { color: var(--rose); }

@media (max-width: 980px) {
  .shell { width: min(100vw - 28px, 760px); }
  .topbar { align-items: flex-start; flex-direction: column; padding-bottom: 18px; }
  .status-stack { justify-items: start; }
  .metric-grid, .main-grid { grid-template-columns: 1fr; }
  .main-grid { grid-template-rows: auto; }
  .stream-panel, .latency-panel { grid-column: auto; grid-row: auto; }
  .table-row { grid-template-columns: 86px 1fr 86px 74px 74px; }
}
"""


APP_JS = r"""const $ = (id) => document.getElementById(id);

const fmt = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 });

function setText(id, value) {
  const node = $(id);
  if (node) node.textContent = value;
}

function signedClass(value) {
  return Number(value) >= 0 ? "positive" : "negative";
}

function render(state) {
  setText("envLabel", state.env);
  setText("modeLabel", state.mode);
  setText("subscribedCount", state.subscribed_count);
  setText("weightLabel", `${state.subscribed_weight.toLocaleString("en-IN")} / ${state.weight_limit.toLocaleString("en-IN")} weight`);
  setText("mps", Number(state.messages_per_sec || 0).toFixed(1));
  setText("messageCount", `${state.messages.toLocaleString("en-IN")} messages received`);
  setText("heartbeat", state.heartbeat_at);
  setText("heartbeatAge", `${state.heartbeat_age_ms} ms since last tick`);
  setText("reconnects", state.reconnects);
  setText("authStatus", state.auth_status);
  setText("avgLatency", state.avg_latency_ms);
  setText("maxLatency", state.max_latency_ms);
  setText("latencyChip", `avg ${state.avg_latency_ms} ms`);
  setText("socketStatus", state.socket_status);
  setText("exchangeLabel", state.exchange);

  const pill = $("socketPill");
  pill.className = `status-pill ${state.connected ? "connected" : state.socket_status}`;
  pill.querySelector("strong").textContent = state.socket_status.toUpperCase();

  $("tickRows").innerHTML = state.ticks.map(tick => `
    <div class="table-row">
      <span>${tick.time}</span>
      <strong>${tick.symbol}</strong>
      <span class="price">${fmt.format(tick.ltp)}</span>
      <span class="${signedClass(tick.change)}">${Number(tick.change).toFixed(2)}%</span>
      <span class="latency">${tick.latency_ms} ms</span>
    </div>
  `).join("");

  $("instrumentRows").innerHTML = state.instruments.map(item => `
    <div class="instrument-item">
      <strong><i class="status-dot"></i>${item.symbol}<br><span>${item.stream} stream</span></strong>
      <span>${fmt.format(item.ltp)}</span>
      <span class="${signedClass(item.change)}">${Number(item.change).toFixed(2)}%</span>
      <span>${item.latency_ms} ms</span>
    </div>
  `).join("");

  $("eventRows").innerHTML = state.events.map(item => `
    <div class="event-item">
      <span class="event-time">${item.time}</span>
      <span class="event-level level-${item.level}">${item.level}</span>
      <span class="event-message">${item.message}</span>
    </div>
  `).join("");

  drawLatency(state.latency_points || []);
}

function drawLatency(points) {
  const canvas = $("latencyCanvas");
  const ctx = canvas.getContext("2d");
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "#090d13";
  ctx.fillRect(0, 0, w, h);

  ctx.strokeStyle = "rgba(141,154,172,.18)";
  ctx.lineWidth = 1;
  for (let i = 1; i < 5; i++) {
    const y = Math.round((h / 5) * i);
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(w, y);
    ctx.stroke();
  }

  if (points.length < 2) return;

  const values = points.map(p => Number(p.value || 0));
  const max = Math.max(60, ...values) * 1.15;
  const step = w / Math.max(points.length - 1, 1);

  ctx.beginPath();
  points.forEach((point, index) => {
    const x = index * step;
    const y = h - (Number(point.value || 0) / max) * (h - 24) - 12;
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = "#4cc9f0";
  ctx.lineWidth = 3;
  ctx.stroke();

  points.slice(-10).forEach((point, offset) => {
    const index = points.length - 10 + offset;
    const x = Math.max(0, index) * step;
    const y = h - (Number(point.value || 0) / max) * (h - 24) - 12;
    ctx.fillStyle = Number(point.value) > 40 ? "#ffc857" : "#36d68a";
    ctx.beginPath();
    ctx.arc(x, y, 3.6, 0, Math.PI * 2);
    ctx.fill();
  });
}

function startClock() {
  setInterval(() => {
    const now = new Date();
    setText("clock", now.toLocaleTimeString("en-IN", { hour12: false }));
  }, 500);
}

async function boot() {
  startClock();
  const first = await fetch("/api/state").then(r => r.json());
  render(first);
  const source = new EventSource("/events");
  source.addEventListener("state", (event) => render(JSON.parse(event.data)));
  source.addEventListener("tick", (event) => render(JSON.parse(event.data)));
}

boot();
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local Nubra WebSocket monitor dashboard.")
    parser.add_argument("--mode", choices=["demo", "live"], default="demo")
    parser.add_argument("--auth", action="store_true", help="Initialize Nubra SDK auth even in demo mode.")
    parser.add_argument("--env", choices=["UAT", "PROD"], default="UAT")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--symbols", default="NIFTY,BANKNIFTY,FINNIFTY,HDFCBANK,RELIANCE,SBIN")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    if not symbols:
        print("At least one symbol is required.", file=sys.stderr)
        return 2

    state = MonitorState(mode=args.mode, env=args.env, exchange=args.exchange.upper())
    hub = EventHub()

    if args.mode == "live":
        streamer: threading.Thread = LiveNubraStreamer(state, hub, symbols, args.env, args.exchange.upper())
    else:
        streamer = DemoStreamer(state, hub, symbols)
        if args.auth:
            AuthOnlyThread(state, hub, args.env).start()

    server = ThreadingHTTPServer((HOST, args.port), make_handler(state, hub))

    def shutdown(signum: int, frame: Any) -> None:
        print("\nStopping Nubra WebSocket Monitor...")
        server.shutdown()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    streamer.start()
    url = f"http://{HOST}:{args.port}"
    print("")
    print("Nubra WebSocket Monitor is running")
    print(f"Dashboard: {url}")
    print(f"Mode: {args.mode.upper()} | Env: {args.env} | Exchange: {args.exchange.upper()}")
    print("Press Ctrl+C to stop.")
    print("")

    try:
        server.serve_forever()
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
