"""Offline tests for the straddle's position book, delta maths and order payloads (no SDK login needed)."""
import math
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from straddle.delta_engine import Leg, PositionBook  # noqa: E402
from straddle.instruments import OptionLeg, find_hedge_candidate  # noqa: E402
from straddle.order_manager import OrderGate, hedge_leg_order, short_straddle_order, square_off_orders  # noqa: E402


def make_leg(ref_id, kind, strike, delta):
    return OptionLeg(ref_id=ref_id, option_type=kind, strike_price=strike, lot_size=75, delta=delta, ltp=10000.0)


class StraddleTests(unittest.TestCase):
    def test_straddle_payload_sells_both_legs_with_ioc(self):
        order = short_straddle_order("tag", make_leg(1, "CE", 2400000, 0.5), make_leg(2, "PE", 2400000, -0.5), 2)
        self.assertTrue(order["isMultiLeg"])
        self.assertEqual(order["qty"], 150)
        self.assertEqual([leg["unitQty"] for leg in order["legs"]], [-2, -2])
        self.assertEqual(order["validityType"], "IOC")
        self.assertEqual(order["stratTags"], ["tag"])

    def test_book_merges_quantity_per_ref_id(self):
        book = PositionBook()
        book.add(Leg(1, "CE", 2400000, 75, -75, "core"))
        book.add(Leg(1, "CE", 2400000, 75, 150, "hedge"))
        self.assertEqual(book.snapshot()[0].signed_qty, 75)

    def test_square_off_only_covers_own_legs_and_reverses_side(self):
        book = PositionBook()
        book.add(Leg(1, "CE", 2400000, 75, -75, "core"))
        book.add(Leg(2, "PE", 2400000, 75, 150, "hedge"))
        book.add(Leg(3, "PE", 2390000, 75, 0, "hedge"))
        orders = {o["refId"]: o for o in square_off_orders("tag", book)}
        self.assertEqual(set(orders), {1, 2})  # flat legs are skipped
        self.assertEqual((orders[1]["side"], orders[1]["qty"]), ("BUY", 75))
        self.assertEqual((orders[2]["side"], orders[2]["qty"]), ("SELL", 150))

    def test_hedge_lot_sizing_matches_runner_formula(self):
        hedge = make_leg(5, "PE", 2390000, -0.35)
        net_delta_lots = 0.8
        lots = max(1, math.ceil(abs(net_delta_lots) / max(abs(hedge.delta), 0.01)))
        self.assertEqual(lots, 3)
        self.assertEqual(hedge_leg_order("tag", hedge, lots, "BUY")["qty"], 225)

    def test_hedge_candidate_prefers_nearest_strike_above_delta_floor(self):
        rows = [
            SimpleNamespace(ref_id=1, strike_price=2400000, lot_size=75, delta=0.55, last_traded_price=9000),
            SimpleNamespace(ref_id=2, strike_price=2410000, lot_size=75, delta=0.15, last_traded_price=3000),
            SimpleNamespace(ref_id=3, strike_price=2430000, lot_size=75, delta=0.40, last_traded_price=6000),
        ]
        chain = SimpleNamespace(ce=rows, pe=[], at_the_money_strike=2410000)
        # Strike 2410000 is nearest ATM but its |delta| 0.15 is below the 0.20 floor, so the next nearest wins.
        self.assertEqual(find_hedge_candidate(chain, "CE").ref_id, 1)

    def test_gate_never_sends_in_dry_run(self):
        sent = []
        gate = OrderGate(SimpleNamespace(create_order=lambda p: sent.append(p)), place_orders=False)
        self.assertIsNone(gate.send({"x": 1}))
        self.assertEqual(sent, [])
        OrderGate(SimpleNamespace(create_order=lambda p: sent.append(p) or "ok"), place_orders=True).send({"x": 1})
        self.assertEqual(sent, [{"x": 1}])


if __name__ == "__main__":
    unittest.main()
