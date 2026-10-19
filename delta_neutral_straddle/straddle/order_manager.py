import logging

from .delta_engine import PositionBook
from .instruments import OptionLeg

log = logging.getLogger("delta_neutral_straddle")


class OrderGate:
    """Single place where orders leave the program. Dry run by default: payloads are logged, not sent."""

    def __init__(self, trader, place_orders: bool):
        self.trader = trader
        self.place_orders = place_orders

    def send(self, payload):
        if not self.place_orders:
            log.warning("DRY RUN - order not sent (use --place-orders to send): %s", payload)
            return None
        return self.trader.create_order(payload)


def short_straddle_order(tag: str, ce: OptionLeg, pe: OptionLeg, lots: int) -> dict:
    """One multi-leg strategy order: sell `lots` lots of the ATM CE and ATM PE."""
    return {
        "isMultiLeg": True,
        "qty": ce.lot_size * lots,
        "side": "BUY",  # strategy-wrapper side; leg direction comes from the sign of unitQty
        "deliveryType": "CNC",
        "priceType": "MARKET",
        "validityType": "IOC",  # the exchange rejects MARKET orders with DAY validity
        "executionMode": "ENTRY",
        "legs": [
            {"refId": ce.ref_id, "unitQty": -lots},
            {"refId": pe.ref_id, "unitQty": -lots},
        ],
        "stratTags": [tag],
    }


def hedge_leg_order(tag: str, leg: OptionLeg, lots: int, side: str) -> dict:
    """One independent single-leg order used to nudge net delta back toward zero."""
    return {
        "refId": leg.ref_id,
        "qty": leg.lot_size * lots,
        "side": side,  # "BUY" or "SELL"
        "deliveryType": "CNC",
        "priceType": "MARKET",
        "validityType": "IOC",
        "isMultiLeg": False,
        "executionMode": "ENTRY",
        "stratTags": [tag],
    }


def square_off_orders(tag: str, book: PositionBook) -> list[dict]:
    """Opposite orders for this example's own legs only (it never touches other positions)."""
    orders = []
    for leg in book.snapshot():
        if leg.signed_qty == 0:
            continue
        orders.append({
            "refId": leg.ref_id,
            "qty": abs(leg.signed_qty),
            "side": "BUY" if leg.signed_qty < 0 else "SELL",
            "deliveryType": "CNC",
            "priceType": "MARKET",
            "validityType": "IOC",
            "isMultiLeg": False,
            "executionMode": "ENTRY",
            "stratTags": [tag],
        })
    return orders
