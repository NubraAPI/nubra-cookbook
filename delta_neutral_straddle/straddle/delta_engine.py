import logging
import threading
from dataclasses import dataclass, field

from nubra_python_sdk.ticker import websocketdata

log = logging.getLogger("delta_neutral_straddle")


@dataclass
class Leg:
    ref_id: int
    option_type: str
    strike_price: int
    lot_size: int
    signed_qty: int  # traded quantity in units, negative for short legs
    role: str  # "core" (the original straddle leg) or "hedge"


@dataclass
class PositionBook:
    legs: dict[int, Leg] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, leg: Leg) -> None:
        with self._lock:
            if leg.ref_id in self.legs:
                self.legs[leg.ref_id].signed_qty += leg.signed_qty
            else:
                self.legs[leg.ref_id] = leg

    def ref_ids(self) -> list[int]:
        with self._lock:
            return list(self.legs.keys())

    def snapshot(self) -> list[Leg]:
        with self._lock:
            return [Leg(**vars(leg)) for leg in self.legs.values()]


class GreeksFeed:
    """Wraps the Greeks websocket and keeps a live delta cache keyed by ref_id."""

    def __init__(self, nubra_client):
        self._cache: dict[int, float] = {}
        self._lock = threading.Lock()
        self._subscribed: set[int] = set()
        self._socket = websocketdata.NubraDataSocket(
            client=nubra_client,
            on_greeks_data=self._on_greeks_data,
            on_connect=lambda msg: log.info("Greeks socket: %s", msg),
            on_close=lambda reason: log.info("Greeks socket closed: %s", reason),
            on_error=lambda err: log.warning("Greeks socket error: %s", err),
        )

    def _on_greeks_data(self, msg) -> None:
        if getattr(msg, "delta", None) is None:
            return
        with self._lock:
            self._cache[int(msg.ref_id)] = float(msg.delta)

    def start(self) -> None:
        self._socket.connect()

    def stop(self) -> None:
        self._socket.close()

    def subscribe(self, ref_ids: list[int]) -> None:
        new_ids = [str(r) for r in ref_ids if r not in self._subscribed]
        if not new_ids:
            return
        self._socket.subscribe(new_ids, data_type="greeks")
        self._subscribed.update(ref_ids)

    def delta_for(self, ref_id: int) -> float | None:
        with self._lock:
            return self._cache.get(ref_id)

    def net_delta_lots(self, book: PositionBook, lot_size: int) -> float | None:
        """Net delta across all open legs, in lot-equivalent units."""
        total = 0.0
        for leg in book.snapshot():
            delta = self.delta_for(leg.ref_id)
            if delta is None:
                return None  # wait until every open leg has a live delta tick
            total += leg.signed_qty * delta
        return total / lot_size
