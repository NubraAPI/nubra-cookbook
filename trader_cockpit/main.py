#!/usr/bin/env python3
"""Local option chain dashboard: live spot, OI wall ladder, IV, PCR, tick tape and API status.

Type: streaming (market data only; never places or modifies orders)
Needs: UAT login (PHONE_NO / MPIN in .env); market hours for ticks. `--env prod` is opt-in.
Expect: a local web page (http://127.0.0.1:8772) for one underlying (default NIFTY). There is no demo data: if login,
        the option-chain snapshot or the websocket subscription fails, the page shows the error.
Tested with: nubra-sdk 0.5.4 (UAT)
"""
from __future__ import annotations

import argparse
import json
import math
import queue
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse


HOST = "127.0.0.1"
DEFAULT_PORT = 8772
DISCONNECT_ERRORS = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError)


class QuietThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request: Any, client_address: Any) -> None:
        exc_type, exc, _traceback = sys.exc_info()
        if exc_type and issubclass(exc_type, DISCONNECT_ERRORS):
            return
        super().handle_error(request, client_address)


def now_ms() -> int:
    return int(time.time() * 1000)


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def fmt_expiry() -> str:
    return "-"


def paise_to_rupees(value: Any) -> float | None:
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    return round(raw / 100.0, 2)


def normalize_iv(value: Any) -> float:
    parsed = as_float(value)
    if 0 < parsed < 1:
        return parsed * 100
    return parsed


def human_oi(value: float) -> str:
    value = float(value or 0)
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= 10_000_000:
        return f"{sign}{value / 10_000_000:.2f}Cr"
    if value >= 100_000:
        return f"{sign}{value / 100_000:.2f}L"
    if value >= 1_000:
        return f"{sign}{value / 1_000:.1f}K"
    return f"{sign}{value:.0f}"


def as_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def as_float(value: Any) -> float:
    try:
        parsed = float(value or 0)
        if math.isnan(parsed) or math.isinf(parsed):
            return 0.0
        return parsed
    except (TypeError, ValueError):
        return 0.0


@dataclass
class StrikeRow:
    strike: int
    ce_oi: int
    pe_oi: int
    ce_chg: int
    pe_chg: int
    ce_ltp: float
    pe_ltp: float
    ce_iv: float
    pe_iv: float
    ce_delta: float
    pe_delta: float

    def as_dict(self) -> dict[str, Any]:
        max_oi = max(self.ce_oi, self.pe_oi, 1)
        return {
            "strike": self.strike,
            "ce_oi": self.ce_oi,
            "pe_oi": self.pe_oi,
            "ce_oi_text": human_oi(self.ce_oi),
            "pe_oi_text": human_oi(self.pe_oi),
            "ce_chg": self.ce_chg,
            "pe_chg": self.pe_chg,
            "ce_chg_text": human_oi(self.ce_chg),
            "pe_chg_text": human_oi(self.pe_chg),
            "ce_ltp": round(self.ce_ltp, 2),
            "pe_ltp": round(self.pe_ltp, 2),
            "ce_iv": round(self.ce_iv, 2),
            "pe_iv": round(self.pe_iv, 2),
            "ce_delta": round(self.ce_delta, 2),
            "pe_delta": round(self.pe_delta, 2),
            "ce_bar": round(self.ce_oi / max_oi * 100, 1),
            "pe_bar": round(self.pe_oi / max_oi * 100, 1),
        }


@dataclass
class TraderState:
    mode: str
    env: str
    symbol: str
    exchange: str
    expiry: str = field(default_factory=fmt_expiry)
    connected: bool = False
    socket_status: str = "booting"
    auth_status: str = "pending"
    spot: float | None = None
    spot_change: float | None = None
    atm: int | None = None
    pcr: float | None = None
    avg_iv: float | None = None
    max_pain: int | None = None
    support: int | None = None
    resistance: int | None = None
    messages: int = 0
    mps: float = 0.0
    latency_ms: int = 24
    heartbeat: str = "-"
    reconnects: int = 0
    rows: list[StrikeRow] = field(default_factory=list)
    tape: list[dict[str, Any]] = field(default_factory=list)
    walls: list[dict[str, Any]] = field(default_factory=list)
    flow: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    last_rate_ms: int = field(default_factory=now_ms)
    last_messages: int = 0
    lock: threading.RLock = field(default_factory=threading.RLock)

    def event(self, level: str, text: str) -> None:
        with self.lock:
            self.events.insert(0, {"time": ts(), "level": level, "text": text})
            self.events = self.events[:7]

    def tick(self, item: dict[str, Any]) -> None:
        with self.lock:
            self.messages += 1
            self.heartbeat = ts()
            self.tape.insert(0, item)
            self.tape = self.tape[:12]

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            elapsed = max((now_ms() - self.last_rate_ms) / 1000.0, 0.1)
            if elapsed >= 1:
                self.mps = round((self.messages - self.last_messages) / elapsed, 1)
                self.last_messages = self.messages
                self.last_rate_ms = now_ms()
            return {
                "mode": self.mode,
                "env": self.env,
                "symbol": self.symbol,
                "exchange": self.exchange,
                "expiry": self.expiry,
                "connected": self.connected,
                "socket_status": self.socket_status,
                "auth_status": self.auth_status,
                "spot": round(self.spot, 2) if self.spot is not None else None,
                "spot_change": round(self.spot_change, 2) if self.spot_change is not None else None,
                "atm": self.atm,
                "pcr": round(self.pcr, 2) if self.pcr is not None else None,
                "avg_iv": round(self.avg_iv, 1) if self.avg_iv is not None else None,
                "max_pain": self.max_pain,
                "support": self.support,
                "resistance": self.resistance,
                "messages": self.messages,
                "mps": self.mps,
                "latency_ms": self.latency_ms,
                "heartbeat": self.heartbeat,
                "reconnects": self.reconnects,
                "rows": [row.as_dict() for row in self.rows],
                "walls": list(self.walls),
                "flow": list(self.flow),
                "tape": list(self.tape),
                "events": list(self.events),
            }


class Hub:
    def __init__(self) -> None:
        self.clients: list[queue.Queue[str]] = []
        self.lock = threading.Lock()

    def subscribe(self) -> queue.Queue[str]:
        client: queue.Queue[str] = queue.Queue(maxsize=4)
        with self.lock:
            self.clients.append(client)
        return client

    def unsubscribe(self, client: queue.Queue[str]) -> None:
        with self.lock:
            if client in self.clients:
                self.clients.remove(client)

    def publish(self, state: TraderState) -> None:
        payload = json.dumps(state.snapshot(), separators=(",", ":"))
        message = f"event: state\ndata: {payload}\n\n"
        with self.lock:
            clients = list(self.clients)
        for client in clients:
            try:
                client.put_nowait(message)
            except queue.Full:
                pass

def recompute_levels(state: TraderState) -> None:
    rows = state.rows
    if not rows:
        return
    if state.spot is not None:
        state.atm = round(state.spot / 50) * 50
    state.resistance = max(rows, key=lambda row: row.ce_oi).strike
    state.support = max(rows, key=lambda row: row.pe_oi).strike
    state.max_pain = min(rows, key=lambda row: abs(row.ce_oi - row.pe_oi)).strike
    total_ce = sum(row.ce_oi for row in rows)
    total_pe = sum(row.pe_oi for row in rows)
    state.pcr = total_pe / total_ce if total_ce else 0
    state.avg_iv = sum((row.ce_iv + row.pe_iv) / 2 for row in rows) / len(rows)
    ranked = []
    for row in rows:
        ranked.append({"strike": row.strike, "side": "CE", "oi": row.ce_oi, "text": human_oi(row.ce_oi)})
        ranked.append({"strike": row.strike, "side": "PE", "oi": row.pe_oi, "text": human_oi(row.pe_oi)})
    state.walls = sorted(ranked, key=lambda item: item["oi"], reverse=True)[:7]
    state.flow = [
        {"label": "Call writers", "value": sum(max(0, row.ce_chg) for row in rows), "side": "CE"},
        {"label": "Put writers", "value": sum(max(0, row.pe_chg) for row in rows), "side": "PE"},
        {"label": "Call unwinding", "value": abs(sum(min(0, row.ce_chg) for row in rows)), "side": "CE"},
        {"label": "Put unwinding", "value": abs(sum(min(0, row.pe_chg) for row in rows)), "side": "PE"},
    ]

class LiveFeed(threading.Thread):
    def __init__(self, state: TraderState, hub: Hub, env: str) -> None:
        super().__init__(daemon=True)
        self.state = state
        self.hub = hub
        self.env = env
        self.socket = None

    def run(self) -> None:
        try:
            from nubra_python_sdk.marketdata.market_data import MarketData
            from nubra_python_sdk.start_sdk import InitNubraSdk, NubraEnv
            from nubra_python_sdk.ticker import websocketdata

            env_value = NubraEnv.PROD if self.env.upper() == "PROD" else NubraEnv.UAT
            self.state.event("info", f"starting Nubra SDK auth in {self.env.upper()}")
            self.hub.publish(self.state)
            client = InitNubraSdk(env_value, env_creds=True)
            self.state.auth_status = "authenticated"
            self.state.event("ok", "Nubra SDK authenticated")
            self.hub.publish(self.state)
            market = MarketData(client)
            try:
                chain_response = market.option_chain(self.state.symbol, None, exchange=self.state.exchange)
                self._seed_from_chain(chain_response)
            except Exception as exc:
                raise RuntimeError(f"option chain snapshot failed: {type(exc).__name__}: {exc}") from exc
            if not self.state.rows:
                raise RuntimeError("option chain snapshot returned no paired CE/PE rows")
            if self.state.expiry == "-":
                raise RuntimeError("option chain snapshot did not return an expiry; cannot subscribe to option stream")
            recompute_levels(self.state)
            self.state.event("ok", f"option chain snapshot loaded: {len(self.state.rows)} strikes")
            self.hub.publish(self.state)
            self.socket = websocketdata.NubraDataSocket(
                client=client,
                on_connect=self._on_connect,
                on_close=self._on_close,
                on_error=self._on_error,
                on_index_data=self._on_index,
                on_option_data=self._on_option,
            )
            self.socket.connect()
            self.socket.subscribe([self.state.symbol], data_type="index", exchange=self.state.exchange)
            self.socket.subscribe([f"{self.state.symbol}:{self.state.expiry}"], data_type="option", exchange=self.state.exchange)
            self.state.event("info", f"{self.state.symbol} index + option-chain streams subscribed")
            self.socket.keep_running()
        except Exception as exc:
            self.state.socket_status = "error"
            self.state.connected = False
            self.state.event("error", f"{type(exc).__name__}: {exc}")
            self.hub.publish(self.state)

    def _seed_from_chain(self, response: Any) -> None:
        chain = getattr(response, "chain", response)
        self.state.expiry = str(getattr(chain, "expiry", None) or self.state.expiry)
        self.state.spot = paise_to_rupees(getattr(chain, "current_price", None)) or self.state.spot
        by_strike: dict[int, dict[str, Any]] = {}
        for side, rows in (("CE", getattr(chain, "ce", []) or []), ("PE", getattr(chain, "pe", []) or [])):
            for item in rows:
                strike = int((paise_to_rupees(getattr(item, "strike_price", None)) or 0))
                if not strike:
                    continue
                target = by_strike.setdefault(strike, {})
                target[side] = item
        seeded: list[StrikeRow] = []
        for strike, pair in sorted(by_strike.items()):
            ce = pair.get("CE")
            pe = pair.get("PE")
            if not ce or not pe:
                continue
            seeded.append(
                StrikeRow(
                    strike=strike,
                    ce_oi=int(getattr(ce, "open_interest", None) or 0),
                    pe_oi=int(getattr(pe, "open_interest", None) or 0),
                    ce_chg=int((getattr(ce, "open_interest", 0) or 0) - (getattr(ce, "previous_open_interest", 0) or 0)),
                    pe_chg=int((getattr(pe, "open_interest", 0) or 0) - (getattr(pe, "previous_open_interest", 0) or 0)),
                    ce_ltp=paise_to_rupees(getattr(ce, "last_traded_price", None)) or 0,
                    pe_ltp=paise_to_rupees(getattr(pe, "last_traded_price", None)) or 0,
                    ce_iv=normalize_iv(getattr(ce, "iv", None)),
                    pe_iv=normalize_iv(getattr(pe, "iv", None)),
                    ce_delta=float(getattr(ce, "delta", None) or 0),
                    pe_delta=float(getattr(pe, "delta", None) or 0),
                )
            )
        if seeded:
            if self.state.spot is not None:
                atm = round(self.state.spot / 50) * 50
                self.state.rows = [row for row in seeded if abs(row.strike - atm) <= 400] or seeded[:16]
            else:
                self.state.rows = seeded[:16]

    def _on_connect(self, msg: Any) -> None:
        self.state.connected = True
        self.state.socket_status = "connected"
        self.state.event("ok", "Nubra market-data websocket connected")
        self.hub.publish(self.state)

    def _on_close(self, reason: Any) -> None:
        self.state.connected = False
        self.state.socket_status = "closed"
        self.state.event("warn", f"websocket closed: {reason}")
        self.hub.publish(self.state)

    def _on_error(self, err: Any) -> None:
        self.state.socket_status = "error"
        self.state.event("error", f"websocket error: {err}")
        self.hub.publish(self.state)

    def _on_index(self, payload: Any) -> None:
        received_at = now_ms()
        entries = []
        for attr in ("indexes", "instruments"):
            value = getattr(payload, attr, None)
            if isinstance(value, list):
                entries.extend(value)
        if not entries:
            entries = [payload]
        for item in entries:
            self.state.spot = paise_to_rupees(getattr(item, "index_value", None)) or self.state.spot
            change = getattr(item, "changepercent", None)
            if change is not None:
                self.state.spot_change = as_float(change)
        recompute_levels(self.state)
        self.state.latency_ms = max(0, now_ms() - received_at)
        self.state.tick({"time": ts(), "symbol": self.state.symbol, "ltp": round(self.state.spot, 2) if self.state.spot is not None else "-", "oi": "-", "iv": "-", "latency": self.state.latency_ms})
        self.hub.publish(self.state)

    def _on_option(self, payload: Any) -> None:
        received_at = now_ms()
        chain = getattr(payload, "chain", payload)
        self._update_side(getattr(chain, "ce", []) or [], "CE")
        self._update_side(getattr(chain, "pe", []) or [], "PE")
        recompute_levels(self.state)
        self.state.latency_ms = max(0, now_ms() - received_at)
        self.state.tick({"time": ts(), "symbol": "OPTION CHAIN", "ltp": "update", "oi": "live", "iv": "live", "latency": self.state.latency_ms})
        self.hub.publish(self.state)

    def _update_side(self, items: list[Any], side: str) -> None:
        if not items:
            return
        rows_by_strike = {row.strike: row for row in self.state.rows}
        for item in items:
            strike_value = paise_to_rupees(getattr(item, "strike_price", None))
            if strike_value is None:
                continue
            row = rows_by_strike.get(int(strike_value))
            if row is None:
                continue
            oi = as_int(getattr(item, "open_interest", None))
            previous_oi = as_int(getattr(item, "previous_open_interest", None))
            ltp = paise_to_rupees(getattr(item, "last_traded_price", None)) or 0.0
            iv = normalize_iv(getattr(item, "iv", None))
            delta = as_float(getattr(item, "delta", None))
            if side == "CE":
                row.ce_oi = oi
                row.ce_chg = oi - previous_oi
                row.ce_ltp = ltp
                row.ce_iv = iv
                row.ce_delta = delta
            else:
                row.pe_oi = oi
                row.pe_chg = oi - previous_oi
                row.pe_ltp = ltp
                row.pe_iv = iv
                row.pe_delta = delta


def make_handler(state: TraderState, hub: Hub) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/":
                self._text(HTML, "text/html; charset=utf-8")
            elif path == "/styles.css":
                self._text(CSS, "text/css; charset=utf-8")
            elif path == "/app.js":
                self._text(JS, "application/javascript; charset=utf-8")
            elif path == "/api/state":
                self._json(state.snapshot())
            elif path == "/events":
                self._events()
            else:
                self.send_error(HTTPStatus.NOT_FOUND)

        def _text(self, text: str, content_type: str) -> None:
            raw = text.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _json(self, payload: dict[str, Any]) -> None:
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _events(self) -> None:
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
            except DISCONNECT_ERRORS:
                pass
            finally:
                hub.unsubscribe(client)

    return Handler


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Nubra Options Cockpit</title>
  <link rel="stylesheet" href="/styles.css">
</head>
<body>
  <main class="shell">
    <header class="topbar">
      <div>
        <p class="eyebrow">Nubra APIs / Live Options Workspace</p>
        <h1><span id="symbol">NIFTY</span> Options Cockpit</h1>
      </div>
      <div class="status">
        <div id="socket" class="socket"><i></i><b>CONNECTING</b></div>
        <span id="env">UAT</span><span id="mode">LIVE</span><span id="expiry">EXP</span><span id="clock">--:--:--</span>
      </div>
    </header>

    <section class="metrics">
      <article><span>Spot</span><strong id="spot">-</strong><small id="spotChange">-</small></article>
      <article><span>PCR</span><strong id="pcr">-</strong><small>total OI ratio</small></article>
      <article><span>Max Pain</span><strong id="maxPain">-</strong><small>nearest equilibrium</small></article>
      <article><span>Support Wall</span><strong id="support">-</strong><small>largest put OI</small></article>
      <article><span>Resistance Wall</span><strong id="resistance">-</strong><small>largest call OI</small></article>
      <article><span>Avg IV</span><strong id="avgIv">-</strong><small>chain weighted</small></article>
    </section>

    <section class="layout">
      <article class="chain panel">
        <div class="panel-head"><div><p class="eyebrow">Option Chain</p><h2>OI Wall Ladder</h2></div><b id="atm">ATM</b></div>
        <div class="chain-head row"><span>Call OI</span><span>CE LTP</span><span>Strike</span><span>PE LTP</span><span>Put OI</span></div>
        <div id="chainRows"></div>
      </article>

      <section class="right">
        <article class="panel map">
          <div class="panel-head"><div><p class="eyebrow">Market Map</p><h2>Walls & Flow</h2></div><b id="heartbeat">-</b></div>
          <div id="walls" class="walls"></div>
          <div id="flow" class="flow"></div>
        </article>

        <article class="panel tape">
          <div class="panel-head"><div><p class="eyebrow">Live Feed</p><h2>Tick Tape</h2></div><b id="latency">- ms</b></div>
          <div id="tape"></div>
        </article>

        <article class="panel events">
          <div class="panel-head"><div><p class="eyebrow">System</p><h2>API Status</h2></div><b id="mps">0/s</b></div>
          <div id="events"></div>
        </article>
      </section>
    </section>
  </main>
  <script src="/app.js"></script>
</body>
</html>
"""


CSS = r""":root {
  color-scheme: dark;
  --bg: #080a0d;
  --panel: #11161d;
  --panel2: #0d1117;
  --line: #27313d;
  --muted: #8a96a8;
  --text: #f3f7ff;
  --green: #2bd782;
  --red: #ff5870;
  --cyan: #39c7e8;
  --amber: #ffd166;
  --blue: #7aa2ff;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  min-height: 100vh;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: linear-gradient(180deg, #0a0e13 0%, #06080b 100%);
  color: var(--text);
}
.shell { width: min(1510px, calc(100vw - 42px)); margin: 0 auto; padding: 14px 0; }
.topbar { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--line); padding-bottom: 12px; }
.eyebrow { margin: 0 0 5px; color: var(--cyan); font-size: 12px; font-weight: 900; text-transform: uppercase; letter-spacing: 0; }
h1, h2 { margin: 0; letter-spacing: 0; }
h1 { font-size: 44px; line-height: 1; }
h2 { font-size: 19px; }
.status { display: flex; align-items: center; gap: 8px; color: var(--muted); }
.status span, .socket { border: 1px solid var(--line); background: rgba(255,255,255,.035); padding: 8px 10px; font-size: 12px; }
.socket { display: flex; align-items: center; gap: 8px; color: var(--text); min-width: 138px; justify-content: center; }
.socket i { width: 9px; height: 9px; border-radius: 99px; background: var(--amber); box-shadow: 0 0 16px var(--amber); }
.socket.connected i { background: var(--green); box-shadow: 0 0 16px var(--green); }
.socket.error i, .socket.closed i { background: var(--red); box-shadow: 0 0 16px var(--red); }
.metrics { display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; margin: 10px 0; }
.metrics article, .panel { border: 1px solid var(--line); background: linear-gradient(180deg, rgba(17,22,29,.97), rgba(10,14,19,.97)); box-shadow: 0 16px 44px rgba(0,0,0,.25); }
.metrics article { min-height: 86px; padding: 11px 13px; border-left: 4px solid var(--cyan); }
.metrics article:nth-child(4) { border-left-color: var(--green); }
.metrics article:nth-child(5) { border-left-color: var(--red); }
.metrics span, .metrics small { display: block; color: var(--muted); font-size: 12px; }
.metrics strong { display: block; font-size: 25px; margin: 7px 0 4px; line-height: 1; }
.layout { display: grid; grid-template-columns: 1.16fr .84fr; gap: 10px; }
.panel { padding: 12px; overflow: hidden; }
.chain { height: 462px; }
.right { display: grid; grid-template-columns: 1fr 1fr; grid-template-rows: 226px 226px; gap: 10px; }
.map { grid-column: 1 / 3; }
.panel-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 8px; }
.panel-head b { border: 1px solid var(--line); padding: 7px 9px; font-size: 12px; color: var(--text); background: rgba(255,255,255,.035); }
.row { display: grid; grid-template-columns: 1.25fr .7fr .62fr .7fr 1.25fr; gap: 10px; align-items: center; min-height: 25px; border-bottom: 1px solid rgba(39,49,61,.82); font-size: 12px; }
.chain-head { color: var(--muted); text-transform: uppercase; font-size: 11px; font-weight: 900; }
.strike { text-align: center; font-weight: 900; color: var(--amber); }
.ltp { font-weight: 800; font-variant-numeric: tabular-nums; }
.oi-cell { position: relative; min-height: 22px; display: flex; flex-direction: column; justify-content: center; overflow: hidden; }
.oi-cell em { position: absolute; inset: 3px auto 3px 0; width: var(--bar); background: rgba(255,88,112,.18); border-right: 2px solid rgba(255,88,112,.8); }
.oi-cell.put em { right: 0; left: auto; background: rgba(43,215,130,.18); border-right: 0; border-left: 2px solid rgba(43,215,130,.85); }
.oi-cell strong, .oi-cell small { position: relative; z-index: 1; }
.oi-cell small { color: var(--muted); font-size: 11px; }
.positive { color: var(--green); }
.negative { color: var(--red); }
.walls { display: grid; grid-template-columns: repeat(7, 1fr); gap: 7px; margin-bottom: 10px; }
.wall { min-height: 70px; border: 1px solid var(--line); padding: 8px; background: rgba(255,255,255,.03); }
.wall b { display: block; font-size: 16px; }
.wall span { color: var(--muted); font-size: 12px; }
.wall.ce { border-top: 3px solid var(--red); }
.wall.pe { border-top: 3px solid var(--green); }
.flow { display: grid; grid-template-columns: repeat(4, 1fr); gap: 7px; }
.flow-card { border: 1px solid var(--line); padding: 8px; background: rgba(255,255,255,.025); }
.flow-card span { color: var(--muted); font-size: 12px; }
.flow-card strong { display: block; margin-top: 5px; font-size: 18px; }
.tape-row, .event-row { display: grid; grid-template-columns: 86px 1fr 70px 62px; gap: 8px; min-height: 28px; align-items: center; border-bottom: 1px solid rgba(39,49,61,.75); font-size: 12px; }
.tape-row strong { overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
.event-row { grid-template-columns: 86px 58px 1fr; }
.event-row span, .tape-row span { color: var(--muted); }
.level-ok { color: var(--green) !important; font-weight: 900; }
.level-info { color: var(--cyan) !important; font-weight: 900; }
.level-warn { color: var(--amber) !important; font-weight: 900; }
.level-error { color: var(--red) !important; font-weight: 900; }
.empty-state { border: 1px solid var(--line); color: var(--muted); padding: 24px; margin-top: 12px; font-size: 14px; background: rgba(255,255,255,.025); }
@media (max-width: 1050px) {
  .topbar { align-items: flex-start; flex-direction: column; }
  .metrics { grid-template-columns: repeat(3, 1fr); }
  .layout { grid-template-columns: 1fr; }
  .right { grid-template-columns: 1fr 1fr; grid-template-rows: 226px 226px; }
  .map { grid-column: auto; }
  .walls { grid-template-columns: repeat(4, 1fr); }
  .flow { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 700px) {
  .shell { width: min(100vw - 24px, 680px); }
  h1 { font-size: 36px; }
  .metrics { grid-template-columns: repeat(2, 1fr); }
  .right { grid-template-columns: 1fr; grid-template-rows: 260px 210px 210px; }
  .map { grid-column: auto; }
  .walls { grid-template-columns: repeat(2, 1fr); }
}
"""


JS = r"""const $ = (id) => document.getElementById(id);
const nf = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 });
function set(id, value) { const n = $(id); if (n) n.textContent = value; }
function signed(v) { return Number(v) >= 0 ? "positive" : "negative"; }
function dash(v) { return v === null || v === undefined || v === "" ? "-" : v; }
function num(v) { return v === null || v === undefined ? "-" : nf.format(v); }
function pct(v) { return v === null || v === undefined ? "-" : `${Number(v).toFixed(2)}% today`; }
function rowClass(row, state) { return row.strike === state.atm ? " row atm-row" : "row"; }
function render(state) {
  set("symbol", state.symbol);
  set("env", state.env);
  set("mode", state.mode.toUpperCase());
  set("expiry", state.expiry);
  set("spot", num(state.spot));
  set("spotChange", pct(state.spot_change));
  $("spotChange").className = signed(state.spot_change);
  set("pcr", state.pcr === null || state.pcr === undefined ? "-" : Number(state.pcr).toFixed(2));
  set("maxPain", dash(state.max_pain));
  set("support", dash(state.support));
  set("resistance", dash(state.resistance));
  set("avgIv", state.avg_iv === null || state.avg_iv === undefined ? "-" : `${Number(state.avg_iv).toFixed(1)}%`);
  set("atm", state.atm === null || state.atm === undefined ? "ATM -" : `ATM ${state.atm}`);
  set("heartbeat", state.heartbeat);
  set("latency", `${state.latency_ms} ms`);
  set("mps", `${Number(state.mps || 0).toFixed(1)}/s`);
  const socket = $("socket");
  socket.className = `socket ${state.connected ? "connected" : state.socket_status}`;
  socket.querySelector("b").textContent = state.socket_status.toUpperCase();

  $("chainRows").innerHTML = state.rows.length ? state.rows.map(row => `
    <div class="${rowClass(row, state)}">
      <div class="oi-cell"><em style="--bar:${row.ce_bar}%"></em><strong>${row.ce_oi_text}</strong><small class="${signed(row.ce_chg)}">${row.ce_chg_text}</small></div>
      <span class="ltp">${nf.format(row.ce_ltp)} <small>IV ${row.ce_iv}</small></span>
      <span class="strike">${row.strike}</span>
      <span class="ltp">${nf.format(row.pe_ltp)} <small>IV ${row.pe_iv}</small></span>
      <div class="oi-cell put"><em style="--bar:${row.pe_bar}%"></em><strong>${row.pe_oi_text}</strong><small class="${signed(row.pe_chg)}">${row.pe_chg_text}</small></div>
    </div>`).join("") : `<div class="empty-state">Waiting for real Nubra option-chain data. No simulated values are shown.</div>`;

  $("walls").innerHTML = state.walls.map(w => `
    <div class="wall ${w.side.toLowerCase()}"><span>${w.side} wall</span><b>${w.strike}</b><strong>${w.text}</strong></div>
  `).join("");

  $("flow").innerHTML = state.flow.map(f => `
    <div class="flow-card"><span>${f.label}</span><strong class="${f.side === "PE" ? "positive" : "negative"}">${human(f.value)}</strong></div>
  `).join("");

  $("tape").innerHTML = state.tape.map(t => `
    <div class="tape-row"><span>${t.time}</span><strong>${t.symbol}</strong><b>${t.ltp}</b><span>${t.latency} ms</span></div>
  `).join("");

  $("events").innerHTML = state.events.map(e => `
    <div class="event-row"><span>${e.time}</span><span class="level-${e.level}">${e.level}</span><strong>${e.text}</strong></div>
  `).join("");
}
function human(v) {
  v = Number(v || 0);
  if (v >= 10000000) return `${(v / 10000000).toFixed(2)}Cr`;
  if (v >= 100000) return `${(v / 100000).toFixed(2)}L`;
  if (v >= 1000) return `${(v / 1000).toFixed(1)}K`;
  return v.toFixed(0);
}
async function boot() {
  setInterval(() => set("clock", new Date().toLocaleTimeString("en-IN", { hour12: false })), 500);
  render(await fetch("/api/state").then(r => r.json()));
  const source = new EventSource("/events");
  source.addEventListener("state", e => render(JSON.parse(e.data)));
}
boot();
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Nubra trader-facing options cockpit.")
    parser.add_argument("--mode", choices=["live"], default="live")
    parser.add_argument("--env", choices=["UAT", "PROD"], default="UAT")
    parser.add_argument("--symbol", default="NIFTY")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = TraderState(mode=args.mode, env=args.env, symbol=args.symbol.upper(), exchange=args.exchange.upper())
    hub = Hub()
    feed: threading.Thread = LiveFeed(state, hub, args.env)
    server = QuietThreadingHTTPServer((HOST, args.port), make_handler(state, hub))

    def shutdown(signum: int, frame: Any) -> None:
        print("\nStopping Nubra Options Cockpit...")
        server.shutdown()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    feed.start()
    print("")
    print("Nubra Options Cockpit is running")
    print(f"Dashboard: http://{HOST}:{args.port}")
    print(f"Mode: {args.mode.upper()} | Env: {args.env} | Symbol: {args.symbol.upper()} | Exchange: {args.exchange.upper()}")
    print("Market-data only. No orders are placed.")
    print("")
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
