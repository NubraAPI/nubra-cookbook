#!/usr/bin/env python3
"""Local dashboard for order states and realtime order updates: blotter, event timeline and broker messages.

Type: streaming (read-only by default; UAT only)
Needs: UAT login (PHONE_NO / MPIN in .env). Order placement happens only if you pass --place-test-order or
       --nifty-options-algo, and only on UAT (PROD is refused).
Expect: a local web page (http://127.0.0.1:8770) listing current-day UAT orders and order/trade updates. There is no
        demo data: if login, order polling or the order-update socket fails, the page shows the error.
Tested with: nubra-sdk 0.5.4 (UAT)
"""
from __future__ import annotations

import argparse
import json
import queue
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


HOST = "127.0.0.1"
DEFAULT_PORT = 8770
DISCONNECT_ERRORS = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError)
RUN_LOCK_PATH = Path(__file__).resolve().parent / ".uat_order_run.lock"


def now_ms() -> int:
    return int(time.time() * 1000)


def stamp_ms() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def rupees(value: Any) -> str:
    try:
        return f"{float(value) / 100.0:,.2f}"
    except (TypeError, ValueError):
        return "-"


def get_attr(value: Any, key: str, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def as_plain(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): as_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [as_plain(v) for v in value]
    if hasattr(value, "model_dump"):
        try:
            return as_plain(value.model_dump())
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        return {k: as_plain(v) for k, v in vars(value).items() if not k.startswith("_")}
    return str(value)


def clean_status(value: Any) -> str:
    raw = str(value or "UNKNOWN")
    raw = raw.replace("ORDER_STATUS_", "").replace("EXECUTION_STATUS_", "")
    raw = raw.replace("ORDER_", "").replace("_", " ")
    return raw.strip().upper() or "UNKNOWN"


def clean_type(value: Any) -> str:
    raw = str(value or "-")
    return raw.replace("ORDER_TYPE_", "").replace("ORDER_SIDE_", "").replace("ORDER_DELIVERY_TYPE_", "").replace("_", " ")


def parse_sdk_time(value: Any) -> str:
    if value in (None, "", 0, "0"):
        return "-"
    try:
        raw = int(value)
        if raw > 10_000_000_000_000_000:
            dt = datetime.fromtimestamp(raw / 1_000_000_000, tz=timezone.utc)
        elif raw > 10_000_000_000:
            dt = datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(raw, tz=timezone.utc)
        return dt.astimezone().strftime("%H:%M:%S.%f")[:-3]
    except Exception:
        return str(value)


class QuietThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request: Any, client_address: Any) -> None:
        exc_type, _exc, _tb = sys.exc_info()
        if exc_type and issubclass(exc_type, DISCONNECT_ERRORS):
            return
        super().handle_error(request, client_address)


@dataclass
class OrderRow:
    order_id: str
    exchange_order_id: str
    symbol: str
    side: str
    qty: str
    filled_qty: str
    price: str
    avg_price: str
    status: str
    update_msg: str
    tag: str
    updated_at: str


@dataclass
class TimelineEvent:
    time: str
    event: str
    order_id: str
    status: str
    detail: str
    latency_ms: int | None = None


@dataclass
class ConsoleState:
    env: str
    account_label: str = "UAT sandbox"
    mode_label: str = "Order monitor"
    entry_rule: str = "-"
    selected_contract: str = "-"
    active_tag: str = "-"
    socket_status: str = "booting"
    auth_status: str = "pending"
    connected: bool = False
    last_update: str = "-"
    orders: list[OrderRow] = field(default_factory=list)
    timeline: list[TimelineEvent] = field(default_factory=list)
    events: list[TimelineEvent] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    placed_count: int = 0
    open_count: int = 0
    filled_count: int = 0
    rejected_count: int = 0
    cancelled_count: int = 0
    modified_count: int = 0
    ws_events: int = 0
    poll_count: int = 0
    last_poll_ms: int = 0
    lock: threading.RLock = field(default_factory=threading.RLock)

    def add_event(self, event: str, order_id: Any = "-", status: Any = "-", detail: Any = "", latency_ms: int | None = None) -> None:
        item = TimelineEvent(
            time=stamp_ms(),
            event=event,
            order_id=str(order_id or "-"),
            status=clean_status(status),
            detail=str(detail or ""),
            latency_ms=latency_ms,
        )
        with self.lock:
            self.events.insert(0, item)
            self.events = self.events[:16]
            self.timeline.insert(0, item)
            self.timeline = self.timeline[:16]
            self.last_update = item.time

    def add_error(self, text: str) -> None:
        with self.lock:
            self.errors.insert(0, f"{stamp_ms()} | {text}")
            self.errors = self.errors[:6]
            self.last_update = stamp_ms()

    def set_orders(self, rows: list[OrderRow]) -> None:
        with self.lock:
            self.orders = rows[:30]
            statuses = [row.status.upper() for row in rows]
            self.placed_count = len(rows)
            self.open_count = sum(any(key in status for key in ("OPEN", "PENDING", "PLACED", "ACK", "TRIGGER")) for status in statuses)
            self.filled_count = sum(any(key in status for key in ("FILL", "EXECUTED", "COMPLETE")) for status in statuses)
            self.rejected_count = sum("REJECT" in status for status in statuses)
            self.cancelled_count = sum("CANCEL" in status for status in statuses)
            self.modified_count = sum("MODIF" in status for status in statuses)
            self.last_update = stamp_ms()

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "env": self.env,
                "account_label": self.account_label,
                "mode_label": self.mode_label,
                "entry_rule": self.entry_rule,
                "selected_contract": self.selected_contract,
                "active_tag": self.active_tag,
                "socket_status": self.socket_status,
                "auth_status": self.auth_status,
                "connected": self.connected,
                "last_update": self.last_update,
                "orders": [vars(row) for row in self.orders],
                "timeline": [vars(item) for item in self.timeline],
                "events": [vars(item) for item in self.events],
                "errors": list(self.errors),
                "placed_count": self.placed_count,
                "open_count": self.open_count,
                "filled_count": self.filled_count,
                "rejected_count": self.rejected_count,
                "cancelled_count": self.cancelled_count,
                "modified_count": self.modified_count,
                "ws_events": self.ws_events,
                "poll_count": self.poll_count,
                "last_poll_ms": self.last_poll_ms,
            }


class Hub:
    def __init__(self) -> None:
        self.clients: list[queue.Queue[str]] = []
        self.lock = threading.Lock()

    def subscribe(self) -> queue.Queue[str]:
        client: queue.Queue[str] = queue.Queue(maxsize=6)
        with self.lock:
            self.clients.append(client)
        return client

    def unsubscribe(self, client: queue.Queue[str]) -> None:
        with self.lock:
            if client in self.clients:
                self.clients.remove(client)

    def publish(self, state: ConsoleState) -> None:
        payload = json.dumps(state.snapshot(), separators=(",", ":"))
        message = f"event: state\ndata: {payload}\n\n"
        with self.lock:
            clients = list(self.clients)
        for client in clients:
            try:
                client.put_nowait(message)
            except queue.Full:
                pass


class OrderLifecycleEngine(threading.Thread):
    def __init__(self, state: ConsoleState, hub: Hub, args: argparse.Namespace) -> None:
        super().__init__(daemon=True)
        self.state = state
        self.hub = hub
        self.args = args
        self.stop_event = threading.Event()
        self.trader = None
        self.order_workflow_started = threading.Event()

    def run(self) -> None:
        try:
            from nubra_python_sdk.marketdata.market_data import MarketData
            from nubra_python_sdk.refdata.instruments import InstrumentData
            from nubra_python_sdk.start_sdk import InitNubraSdk, NubraEnv
            from nubra_python_sdk.ticker import orderupdate
            from nubra_python_sdk.trading.trading_data import NubraTrader

            self.state.add_event("auth_start", detail="InitNubraSdk UAT env_creds=True")
            self.hub.publish(self.state)
            client = InitNubraSdk(NubraEnv.UAT, env_creds=True)
            self.state.auth_status = "authenticated"
            self.state.add_event("authenticated", detail="UAT session ready")
            self.hub.publish(self.state)

            self.trader = NubraTrader(client, version="V2")
            market = MarketData(client)
            instruments = InstrumentData(client)

            socket = orderupdate.OrderUpdate(
                client=client,
                on_order_update=self.on_order_update,
                on_trade_update=self.on_trade_update,
                on_connect=self.on_socket_connect,
                on_close=self.on_socket_close,
                on_error=self.on_socket_error,
            )
            threading.Thread(target=self._run_socket, args=(socket,), daemon=True).start()

            if self.args.nifty_options_algo:
                threading.Thread(target=self.run_nifty_options_algo, args=(market, instruments), daemon=True).start()
            elif self.args.place_test_order:
                threading.Thread(target=self.place_test_order, args=(instruments, market), daemon=True).start()

            while not self.stop_event.is_set():
                start = now_ms()
                try:
                    self.poll_orders()
                    self.state.last_poll_ms = max(0, now_ms() - start)
                    self.state.poll_count += 1
                except Exception as exc:
                    self.state.add_error(f"orders() poll failed: {type(exc).__name__}: {exc}")
                self.hub.publish(self.state)
                time.sleep(self.args.poll_seconds)
        except Exception as exc:
            self.state.socket_status = "error"
            self.state.connected = False
            self.state.add_error(f"{type(exc).__name__}: {exc}")
            self.hub.publish(self.state)

    def _run_socket(self, socket: Any) -> None:
        try:
            socket.connect("V2")
            socket.keep_running()
        except Exception as exc:
            self.state.socket_status = "error"
            self.state.connected = False
            self.state.add_error(f"OrderUpdate socket failed: {type(exc).__name__}: {exc}")
            self.hub.publish(self.state)

    def poll_orders(self) -> None:
        if self.trader is None:
            return
        result = self.trader.orders()
        root = get_attr(result, "root", result)
        if root is None:
            root = []
        rows = [self.row_from_order(order) for order in list(root)]
        self.state.set_orders(rows)

    def row_from_order(self, order: Any) -> OrderRow:
        ref_data = get_attr(order, "ref_data")
        symbol = (
            get_attr(order, "display_name")
            or get_attr(ref_data, "trading_symbol")
            or get_attr(ref_data, "symbol")
            or get_attr(order, "ref_id")
            or "-"
        )
        return OrderRow(
            order_id=str(get_attr(order, "order_id", "-")),
            exchange_order_id=str(get_attr(order, "exchange_order_id", "-") or "-"),
            symbol=str(symbol),
            side=clean_type(get_attr(order, "order_side", "-")),
            qty=str(get_attr(order, "order_qty", "-")),
            filled_qty=str(get_attr(order, "filled_qty", "-")),
            price=rupees(get_attr(order, "order_price")),
            avg_price=rupees(get_attr(order, "avg_filled_price")),
            status=clean_status(get_attr(order, "order_status", "-")),
            update_msg=str(get_attr(order, "update_msg", "") or ""),
            tag=str(get_attr(order, "tag", "") or ""),
            updated_at=parse_sdk_time(get_attr(order, "last_modified", None)),
        )

    def on_socket_connect(self, msg: Any) -> None:
        self.state.connected = True
        self.state.socket_status = "connected"
        self.state.add_event("order_ws_connected", detail=msg)
        self.hub.publish(self.state)

    def on_socket_close(self, reason: Any) -> None:
        self.state.connected = False
        self.state.socket_status = "closed"
        self.state.add_event("order_ws_closed", detail=reason)
        self.hub.publish(self.state)

    def on_socket_error(self, err: Any) -> None:
        self.state.socket_status = "error"
        self.state.add_error(f"OrderUpdate socket error: {err}")
        self.hub.publish(self.state)

    def on_order_update(self, msg: Any) -> None:
        started = now_ms()
        self.state.ws_events += 1
        order_id = get_attr(msg, "order_id", "-")
        status = get_attr(msg, "execution_status", None) or get_attr(msg, "response_type", None)
        detail = get_attr(msg, "update_msg", None) or get_attr(msg, "request_type", None) or "order update"
        self.state.add_event("websocket_order_update", order_id=order_id, status=status, detail=detail, latency_ms=max(0, now_ms() - started))
        self.hub.publish(self.state)

    def on_trade_update(self, msg: Any) -> None:
        started = now_ms()
        self.state.ws_events += 1
        params = get_attr(msg, "order_params")
        order_id = get_attr(msg, "order_id", "-")
        qty = get_attr(params, "trade_qty", None) or get_attr(params, "filled_qty", None)
        price = get_attr(params, "trade_price", None) or get_attr(params, "avg_fill_price", None)
        self.state.add_event("websocket_trade_update", order_id=order_id, status="FILL", detail=f"qty={qty} price={rupees(price)}", latency_ms=max(0, now_ms() - started))
        self.hub.publish(self.state)

    def place_test_order(self, instruments: Any, market: Any) -> None:
        try:
            tag = f"nifty_exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            qty = int(self.args.qty or 1)
            self.state.add_event("resolve_instrument", detail=f"{self.args.symbol} {self.args.exchange}")
            instrument = instruments.get_instrument_by_symbol(self.args.symbol, exchange=self.args.exchange)
            ref_id = get_attr(instrument, "ref_id")
            quote = market.quote(ref_id=ref_id, levels=1)
            ltp = get_attr(get_attr(quote, "orderBook"), "last_traded_price")
            if ltp is None:
                raise RuntimeError("quote did not return orderBook.last_traded_price")
            order_price = int(float(ltp) * (1 - self.args.passive_offset_pct / 100.0))
            payload = {
                "ref_id": ref_id,
                "order_type": "ORDER_TYPE_REGULAR",
                "order_qty": qty,
                "order_side": "ORDER_SIDE_BUY",
                "order_delivery_type": "ORDER_DELIVERY_TYPE_IDAY",
                "validity_type": "DAY",
                "price_type": "LIMIT",
                "order_price": order_price,
                "exchange": self.args.exchange,
                "tag": tag,
            }
            self.state.add_event("order_sent", detail=f"{self.args.symbol} BUY {qty} LIMIT {rupees(order_price)} tag={tag}")
            self.hub.publish(self.state)
            response = self.trader.create_order(payload)
            order_id = get_attr(response, "order_id", "-")
            self.state.add_event("broker_ack", order_id=order_id, status=get_attr(response, "order_status", "ACK"), detail=get_attr(response, "update_msg", "create_order returned"))
            self.hub.publish(self.state)
            if self.args.cancel_after > 0 and order_id not in ("-", None):
                time.sleep(self.args.cancel_after)
                self.state.add_event("cancel_sent", order_id=order_id, detail=f"cancel_after={self.args.cancel_after}s")
                self.hub.publish(self.state)
                cancel_res = self.trader.cancel_orders_v2(order_ids=[int(order_id)])
                self.state.add_event("cancel_response", order_id=order_id, detail=str(as_plain(cancel_res))[:180])
                self.hub.publish(self.state)
        except Exception as exc:
            self.state.add_error(f"test order workflow failed: {type(exc).__name__}: {exc}")
            self.hub.publish(self.state)

    def run_nifty_options_algo(self, market: Any, instruments: Any) -> None:
        try:
            if self.order_workflow_started.is_set():
                self.state.add_error("order workflow already started in this process; refusing duplicate placement")
                self.hub.publish(self.state)
                return
            self.order_workflow_started.set()
            if not self.acquire_run_lock():
                self.state.add_error(f"order workflow lock exists at {RUN_LOCK_PATH}; refusing duplicate placement")
                self.hub.publish(self.state)
                return
            self.state.mode_label = "NIFTY options execution"
            tag = f"nifty_exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.state.active_tag = tag
            self.state.add_event("strategy_start", detail=f"resolve {self.args.underlying} option chain in UAT")
            self.hub.publish(self.state)

            contract = self.resolve_nifty_option_contract(market, instruments)
            ref_id = contract["ref_id"]
            lot_size = int(contract["lot_size"] or 1)
            qty = int(self.args.qty or lot_size)
            quote = market.quote(ref_id=ref_id, levels=1)
            ltp = get_attr(get_attr(quote, "orderBook"), "last_traded_price")
            if ltp is None:
                raise RuntimeError(f"quote did not return orderBook.last_traded_price for ref_id={ref_id}")

            order_price = max(1, int(float(ltp) * (1 - self.args.passive_offset_pct / 100.0)))
            self.state.entry_rule = f"{contract['side']} BUY limit entry"
            self.state.selected_contract = contract["label"]
            self.state.add_event(
                "order_prepared",
                status="PLACED",
                detail=f"{contract['label']} ltp={rupees(ltp)} limit={rupees(order_price)} qty={qty}",
            )
            self.hub.publish(self.state)

            payload = {
                "ref_id": ref_id,
                "order_type": "ORDER_TYPE_REGULAR",
                "order_qty": qty,
                "order_side": "ORDER_SIDE_BUY",
                "order_delivery_type": "ORDER_DELIVERY_TYPE_IDAY",
                "validity_type": "DAY",
                "price_type": "LIMIT",
                "order_price": order_price,
                "exchange": self.args.exchange,
                "tag": tag,
            }
            self.state.add_event("order_sent", status="PENDING", detail=f"tag={tag} ref_id={ref_id}")
            self.hub.publish(self.state)
            response = self.trader.create_order(payload)
            order_id = get_attr(response, "order_id", "-")
            self.state.add_event(
                "broker_ack",
                order_id=order_id,
                status=get_attr(response, "order_status", "ACKNOWLEDGED"),
                detail=get_attr(response, "update_msg", "create_order returned"),
            )
            self.hub.publish(self.state)

            if self.args.modify_after > 0 and order_id not in ("-", None):
                time.sleep(self.args.modify_after)
                modified_price = max(1, order_price - int(self.args.modify_ticks))
                modify_req = {
                    "order_price": str(modified_price),
                    "order_qty": str(qty),
                    "exchange": self.args.exchange,
                    "order_type": "ORDER_TYPE_REGULAR",
                }
                self.state.add_event("modify_sent", order_id=order_id, status="MODIFIED", detail=f"new_limit={rupees(modified_price)}")
                self.hub.publish(self.state)
                modify_res = self.trader.modify_order_v2(order_id=int(order_id), request=modify_req)
                self.state.add_event("modify_response", order_id=order_id, status="MODIFIED", detail=str(as_plain(modify_res))[:180])
                self.hub.publish(self.state)

            if self.args.cancel_after > 0 and order_id not in ("-", None):
                time.sleep(self.args.cancel_after)
                self.state.add_event("cancel_sent", order_id=order_id, status="CANCELLED", detail=f"cancel_after={self.args.cancel_after}s")
                self.hub.publish(self.state)
                cancel_res = self.trader.cancel_orders_v2(order_ids=[int(order_id)])
                self.state.add_event("cancel_response", order_id=order_id, status="CANCELLED", detail=str(as_plain(cancel_res))[:180])
                self.hub.publish(self.state)
        except Exception as exc:
            self.state.add_error(f"NIFTY options execution failed: {type(exc).__name__}: {exc}")
            self.hub.publish(self.state)
        finally:
            self.release_run_lock()

    def acquire_run_lock(self) -> bool:
        try:
            fd = RUN_LOCK_PATH.open("x", encoding="utf-8")
        except FileExistsError:
            return False
        with fd:
            fd.write(f"pid={sys.argv}\nstarted_at={datetime.now().isoformat()}\n")
        return True

    def release_run_lock(self) -> None:
        try:
            RUN_LOCK_PATH.unlink()
        except FileNotFoundError:
            pass

    def resolve_nifty_option_contract(self, market: Any, instruments: Any) -> dict[str, Any]:
        chain_wrapper = market.option_chain(self.args.underlying, exchange=self.args.exchange)
        chain = get_attr(chain_wrapper, "chain")
        if chain is None:
            raise RuntimeError("option_chain returned no chain")
        expiry = get_attr(chain, "expiry") or ((get_attr(chain, "all_expiries") or [None])[0])
        atm_strike = get_attr(chain, "at_the_money_strike")
        if expiry is None or atm_strike is None:
            raise RuntimeError("option_chain missing expiry or ATM strike")

        side = self.args.option_side.upper()
        if side == "AUTO":
            current_price = get_attr(chain, "current_price") or 0
            side = "CE" if float(current_price or 0) >= float(atm_strike or 0) else "PE"
        rows = list(get_attr(chain, side.lower()) or [])
        selected = next((item for item in rows if get_attr(item, "strike_price") == atm_strike), None)
        if selected is None:
            pattern = [
                {
                    "exchange": self.args.exchange,
                    "asset": self.args.underlying,
                    "derivative_type": "OPT",
                    "expiry": expiry,
                    "strike_price": str(atm_strike),
                    "option_type": side,
                    "asset_type": "INDEX_FO",
                }
            ]
            matches = instruments.get_instruments_by_pattern(pattern) or []
            selected = matches[0] if matches else None
        if selected is None:
            raise RuntimeError(f"failed to resolve {self.args.underlying} {expiry} {atm_strike} {side}")
        ref_id = get_attr(selected, "ref_id")
        if not ref_id:
            raise RuntimeError("selected option contract has no ref_id")
        lot_size = get_attr(selected, "lot_size") or 1
        strike_text = rupees(atm_strike) if float(atm_strike or 0) > 100000 else str(atm_strike)
        return {
            "ref_id": ref_id,
            "side": side,
            "expiry": expiry,
            "strike": atm_strike,
            "lot_size": lot_size,
            "label": f"{self.args.underlying} {expiry} {strike_text} {side}",
        }


def make_handler(state: ConsoleState, hub: Hub) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/":
                self._send_text(HTML, "text/html; charset=utf-8")
            elif path == "/styles.css":
                self._send_text(CSS, "text/css; charset=utf-8")
            elif path == "/app.js":
                self._send_text(JS, "application/javascript; charset=utf-8")
            elif path == "/api/state":
                self._send_json(state.snapshot())
            elif path == "/events":
                self._events()
            else:
                self.send_error(HTTPStatus.NOT_FOUND)

        def _send_text(self, text: str, content_type: str) -> None:
            raw = text.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_json(self, payload: dict[str, Any]) -> None:
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
  <title>Nubra UAT Order Lifecycle Console</title>
  <link rel="stylesheet" href="/styles.css">
</head>
<body>
  <main class="shell">
    <header class="topbar">
      <div>
        <p class="eyebrow">Nubra UAT / Options Execution</p>
        <h1>Order Lifecycle Console</h1>
      </div>
      <div class="status-rail">
        <span id="socketBadge" class="badge">BOOTING</span>
        <span id="envBadge">UAT</span>
        <span id="authBadge">AUTH</span>
        <span id="clock">--:--:--</span>
      </div>
    </header>

    <section class="metrics">
      <article><span>Total Orders</span><strong id="placedCount">0</strong><small>current UAT day</small></article>
      <article><span>Open/Pending</span><strong id="openCount">0</strong><small>live order book</small></article>
      <article><span>Filled/Executed</span><strong id="filledCount">0</strong><small>trade updates</small></article>
      <article><span>Rejected</span><strong id="rejectedCount">0</strong><small>broker responses</small></article>
      <article><span>Cancelled</span><strong id="cancelledCount">0</strong><small>cancel flow</small></article>
      <article><span>WS Events</span><strong id="wsEvents">0</strong><small id="pollLabel">poll - ms</small></article>
    </section>

    <section class="algo-strip">
      <div><span>Mode</span><strong id="modeLabel">Order monitor</strong></div>
      <div><span>Entry Rule</span><strong id="entryRule">-</strong></div>
      <div><span>Selected Contract</span><strong id="selectedContract">-</strong></div>
      <div><span>Strategy Run</span><strong id="activeTag">-</strong></div>
    </section>

    <section class="layout">
      <article class="panel timeline">
        <div class="panel-head"><div><p class="eyebrow">Lifecycle</p><h2>Event Timeline</h2></div><b id="lastUpdate">-</b></div>
        <div id="timeline"></div>
      </article>

      <article class="panel orders">
        <div class="panel-head"><div><p class="eyebrow">Execution Blotter</p><h2>Current-Day Orders</h2></div><b>UAT only</b></div>
        <div class="table-head order-row"><span>Time</span><span>Order</span><span>Instrument</span><span>Side</span><span>Qty</span><span>Price</span><span>Status</span></div>
        <div id="orders"></div>
      </article>

      <article class="panel log">
        <div class="panel-head"><div><p class="eyebrow">Execution Log</p><h2>ms-Level Updates</h2></div><b>OrderUpdate V2</b></div>
        <div id="events"></div>
      </article>

      <article class="panel errors">
        <div class="panel-head"><div><p class="eyebrow">Broker Responses</p><h2>UAT Messages</h2></div><b>Live only</b></div>
        <div id="errors"></div>
      </article>
    </section>
  </main>
  <script src="/app.js"></script>
</body>
</html>
"""


CSS = r""":root {
  color-scheme: light;
  --bg: #f7f3ed;
  --panel: #fffdf8;
  --line: #dfd5c5;
  --line-strong: #c8b9a5;
  --text: #1e2930;
  --muted: #6b7280;
  --blue: #2563eb;
  --cyan: #0f766e;
  --green: #166534;
  --amber: #a16207;
  --red: #b91c1c;
  --violet: #7c3aed;
}
* { box-sizing: border-box; }
body { margin: 0; min-height: 100vh; background: var(--bg); color: var(--text); font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
.shell { width: min(1500px, calc(100vw - 42px)); margin: 0 auto; padding: 22px 0; }
.topbar { display: flex; justify-content: space-between; gap: 20px; align-items: center; padding: 4px 0 16px; border-bottom: 1px solid var(--line-strong); }
.eyebrow { margin: 0 0 5px; color: var(--blue); text-transform: uppercase; font-size: 12px; font-weight: 900; letter-spacing: 0; }
h1, h2 { margin: 0; letter-spacing: 0; }
h1 { font-size: 46px; line-height: 1; }
h2 { font-size: 20px; }
.status-rail { display: flex; gap: 8px; align-items: center; }
.status-rail span, .panel-head b { border: 1px solid var(--line); background: #f8fafc; color: #334155; padding: 8px 11px; font-size: 12px; font-weight: 800; }
.badge.connected { color: var(--green); border-color: rgba(21,128,61,.35); }
.badge.error { color: var(--red); border-color: rgba(190,18,60,.35); }
.metrics { display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; margin: 14px 0; }
.metrics article, .panel { background: var(--panel); border: 1px solid var(--line); box-shadow: 0 12px 30px rgba(92,64,30,.08); }
.metrics article { min-height: 96px; padding: 13px 15px; border-top: 4px solid var(--blue); }
.metrics article:nth-child(2) { border-top-color: var(--amber); }
.metrics article:nth-child(3) { border-top-color: var(--green); }
.metrics article:nth-child(4) { border-top-color: var(--red); }
.metrics article:nth-child(5) { border-top-color: var(--violet); }
.metrics span, .metrics small { display: block; color: var(--muted); font-size: 12px; }
.metrics strong { display: block; margin: 8px 0 5px; font-size: 30px; line-height: 1; }
.algo-strip { display: grid; grid-template-columns: .8fr .9fr 1.3fr 1.25fr; gap: 12px; margin: 0 0 14px; }
.algo-strip div { background: #f8fafc; border: 1px solid var(--line); padding: 10px 12px; min-height: 66px; }
.algo-strip span { display: block; color: var(--muted); font-size: 11px; text-transform: uppercase; font-weight: 900; }
.algo-strip strong { display: block; margin-top: 7px; font-size: 14px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
.layout { display: grid; grid-template-columns: .92fr 1.38fr; grid-template-rows: 296px 286px; gap: 12px; }
.panel { padding: 14px 16px; overflow: hidden; }
.orders { grid-row: 1 / 3; grid-column: 2; }
.panel-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; gap: 12px; }
.timeline-item { display: grid; grid-template-columns: 92px 1fr 112px; gap: 10px; padding: 9px 0; border-bottom: 1px solid var(--line); align-items: center; }
.timeline-item time, .event-row time { color: var(--muted); font-size: 12px; font-variant-numeric: tabular-nums; }
.timeline-main strong { display: block; font-size: 13px; }
.timeline-main span { display: block; color: var(--muted); font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.pill { display: inline-flex; justify-content: center; align-items: center; min-height: 26px; padding: 5px 8px; font-size: 11px; font-weight: 900; color: #fff; background: var(--blue); text-transform: uppercase; }
.pill.open, .pill.pending, .pill.placed, .pill.acknowledged { background: var(--amber); }
.pill.filled, .pill.executed, .pill.complete { background: var(--green); }
.pill.rejected { background: var(--red); }
.pill.cancelled, .pill.canceled { background: var(--violet); }
.order-row { display: grid; grid-template-columns: 80px 94px 1fr 70px 70px 86px 118px; gap: 8px; min-height: 34px; align-items: center; border-bottom: 1px solid var(--line); font-size: 12px; }
.table-head { color: var(--muted); text-transform: uppercase; font-weight: 900; font-size: 11px; }
.order-row strong { overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
.event-row { display: grid; grid-template-columns: 92px 1fr 72px; gap: 10px; padding: 8px 0; border-bottom: 1px solid var(--line); font-size: 12px; }
.event-row strong { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.empty { color: var(--muted); border: 1px dashed var(--line-strong); background: #f8fafc; padding: 18px; margin-top: 10px; font-size: 13px; }
.error-line { color: var(--red); font-size: 12px; border-bottom: 1px solid var(--line); padding: 8px 0; }
@media (max-width: 1100px) {
  .topbar { align-items: flex-start; flex-direction: column; }
  .metrics { grid-template-columns: repeat(3, 1fr); }
  .algo-strip { grid-template-columns: 1fr 1fr; }
  .layout { grid-template-columns: 1fr; grid-template-rows: auto; }
  .orders { grid-column: auto; grid-row: auto; }
}
"""


JS = r"""const $ = (id) => document.getElementById(id);
function set(id, value) { const n = $(id); if (n) n.textContent = value; }
function klass(status) {
  return String(status || "unknown").toLowerCase().replaceAll(" ", "-");
}
function render(state) {
  set("envBadge", state.env);
  set("authBadge", state.auth_status.toUpperCase());
  set("lastUpdate", state.last_update);
  set("modeLabel", state.mode_label);
  set("entryRule", state.entry_rule);
  set("selectedContract", state.selected_contract);
  set("activeTag", state.active_tag);
  set("placedCount", state.placed_count);
  set("openCount", state.open_count);
  set("filledCount", state.filled_count);
  set("rejectedCount", state.rejected_count);
  set("cancelledCount", state.cancelled_count);
  set("wsEvents", state.ws_events);
  set("pollLabel", `${state.poll_count} polls / ${state.last_poll_ms} ms`);
  const socket = $("socketBadge");
  socket.textContent = state.socket_status.toUpperCase();
  socket.className = `badge ${state.connected ? "connected" : state.socket_status}`;

  $("timeline").innerHTML = state.timeline.length ? state.timeline.map(item => `
    <div class="timeline-item">
      <time>${item.time}</time>
      <div class="timeline-main"><strong>${item.event}</strong><span>order ${item.order_id} / ${item.detail}</span></div>
      <span class="pill ${klass(item.status)}">${item.status}</span>
    </div>
  `).join("") : `<div class="empty">Waiting for UAT order lifecycle events from the strategy run.</div>`;

  $("orders").innerHTML = state.orders.length ? state.orders.map(row => `
    <div class="order-row">
      <span>${row.updated_at}</span>
      <strong>${row.order_id}</strong>
      <strong>${row.symbol}</strong>
      <span>${row.side}</span>
      <span>${row.filled_qty}/${row.qty}</span>
      <span>${row.price}</span>
      <span class="pill ${klass(row.status)}">${row.status}</span>
    </div>
  `).join("") : `<div class="empty">No current-day UAT orders returned yet.</div>`;

  $("events").innerHTML = state.events.length ? state.events.map(item => `
    <div class="event-row"><time>${item.time}</time><strong>${item.event} / ${item.detail}</strong><span>${item.latency_ms ?? "-"} ms</span></div>
  `).join("") : `<div class="empty">Realtime OrderUpdate events will appear here.</div>`;

  $("errors").innerHTML = state.errors.length ? state.errors.map(line => `<div class="error-line">${line}</div>`).join("") : `<div class="empty">No broker or API errors reported.</div>`;
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
    parser = argparse.ArgumentParser(description="Run a real Nubra UAT order lifecycle console.")
    parser.add_argument("--env", choices=["UAT"], default="UAT", help="Only UAT is allowed in this example.")
    parser.add_argument("--exchange", default="NSE")
    parser.add_argument("--symbol", default="HDFCBANK")
    parser.add_argument("--qty", type=int, default=0, help="Quantity. For the NIFTY option order, 0 means use contract lot size.")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--place-test-order", action="store_true", help="Intentionally place one real UAT test limit order.")
    parser.add_argument("--nifty-options-algo", action="store_true", help="Resolve NIFTY option chain and place one real UAT tagged option order.")
    parser.add_argument("--underlying", default="NIFTY")
    parser.add_argument("--option-side", choices=["AUTO", "CE", "PE"], default="AUTO")
    parser.add_argument("--modify-after", type=float, default=0.0, help="Modify the option order after N seconds.")
    parser.add_argument("--modify-ticks", type=int, default=500, help="Native price units to subtract during modify step.")
    parser.add_argument("--cancel-after", type=float, default=0.0, help="Cancel the test order after N seconds when --place-test-order is used.")
    parser.add_argument("--passive-offset-pct", type=float, default=4.0, help="Buy limit below LTP for a passive UAT order.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = ConsoleState(env=args.env)
    hub = Hub()
    engine = OrderLifecycleEngine(state, hub, args)
    server = QuietThreadingHTTPServer((HOST, args.port), make_handler(state, hub))
    shutting_down = threading.Event()

    def shutdown(signum: int, frame: Any) -> None:
        if shutting_down.is_set():
            return
        shutting_down.set()
        print("\nStopping Nubra Order Lifecycle Console...")
        engine.stop_event.set()
        engine.release_run_lock()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    engine.start()
    print("")
    print("Nubra Order Lifecycle Console is running")
    print(f"Dashboard: http://{HOST}:{args.port}")
    print(f"Environment: UAT | Symbol: {args.symbol.upper()} | Exchange: {args.exchange.upper()}")
    print("Real UAT order data only. No demo rows are rendered.")
    if args.nifty_options_algo:
        print(f"NIFTY options execution is ENABLED for UAT. Option side: {args.option_side}.")
    if args.place_test_order:
        print("Test order workflow is ENABLED for UAT.")
    print("")
    try:
        server.serve_forever()
    finally:
        engine.stop_event.set()
        engine.release_run_lock()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
