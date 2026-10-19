import logging
import math
import time as time_module
from datetime import datetime

from nubra_python_sdk.marketdata.market_data import MarketData
from nubra_python_sdk.start_sdk import InitNubraSdk, NubraEnv
from nubra_python_sdk.trading.trading_data import NubraTrader

from .config import Settings
from .delta_engine import GreeksFeed, Leg, PositionBook
from .instruments import find_hedge_candidate, nearest_weekly_expiry, resolve_atm_legs
from .order_manager import OrderGate, hedge_leg_order, short_straddle_order, square_off_orders

log = logging.getLogger("delta_neutral_straddle")


class DeltaNeutralStraddle:
    """Short ATM straddle that adds long options when net delta drifts outside a band."""

    def __init__(self, settings: Settings):
        self.settings = settings
        env = NubraEnv.PROD if settings.env.upper() == "PROD" else NubraEnv.UAT
        self.nubra = InitNubraSdk(env, totp_login=settings.totp_login, env_creds=True)
        self.market_data = MarketData(self.nubra)
        self.gate = OrderGate(NubraTrader(self.nubra), settings.place_orders)

        self.book = PositionBook()
        self.feed = GreeksFeed(self.nubra)

        self.lot_size: int | None = None
        self.hedges_today = 0
        self.last_rebalance_ts = 0.0

    def enter(self) -> None:
        s = self.settings
        chain, atm_ce, atm_pe = resolve_atm_legs(self.market_data, s.symbol, s.exchange)
        expiry = chain.expiry or nearest_weekly_expiry(chain.all_expiries)
        log.info(
            "Entering short straddle: %s expiry=%s strike=%.2f CE ref_id=%s PE ref_id=%s",
            s.symbol, expiry, atm_ce.strike_price / 100, atm_ce.ref_id, atm_pe.ref_id,
        )

        result = self.gate.send(short_straddle_order(s.strat_tag, atm_ce, atm_pe, s.lots))
        if result is not None:
            log.info("Entry order response: %s", result)

        self.lot_size = atm_ce.lot_size
        self.book.add(Leg(atm_ce.ref_id, "CE", atm_ce.strike_price, atm_ce.lot_size, -s.lots * atm_ce.lot_size, "core"))
        self.book.add(Leg(atm_pe.ref_id, "PE", atm_pe.strike_price, atm_pe.lot_size, -s.lots * atm_pe.lot_size, "core"))

        self.feed.start()
        self.feed.subscribe(self.book.ref_ids())

    def _past_square_off_time(self) -> bool:
        return datetime.now().time() >= self.settings.square_off_time

    def maybe_rebalance(self, net_delta_lots: float | None = None) -> bool:
        """Add a hedge if net delta is outside the band. Returns True when a hedge was sent or logged."""
        s = self.settings
        now = time_module.monotonic()
        if now - self.last_rebalance_ts < s.rebalance_cooldown_seconds:
            return False
        if self.hedges_today >= s.max_hedges_per_day:
            return False

        if net_delta_lots is None:
            net_delta_lots = self.feed.net_delta_lots(self.book, self.lot_size)
        if net_delta_lots is None:
            return False  # still waiting for a live delta tick on every open leg
        if abs(net_delta_lots) <= s.delta_band_lots:
            return False

        chain, _, _ = resolve_atm_legs(self.market_data, s.symbol, s.exchange)
        if net_delta_lots > 0:
            hedge = find_hedge_candidate(chain, "PE")  # delta too positive: buy a put to pull it down
        else:
            hedge = find_hedge_candidate(chain, "CE")  # delta too negative: buy a call to pull it up
        side = "BUY"

        hedge_lots = max(1, math.ceil(abs(net_delta_lots) / max(abs(hedge.delta), 0.01)))
        log.info(
            "Rebalancing: net_delta=%.3f lots -> %s %d lot(s) of %.2f %s (delta=%.3f)",
            net_delta_lots, side, hedge_lots, hedge.strike_price / 100, hedge.option_type, hedge.delta,
        )

        result = self.gate.send(hedge_leg_order(s.strat_tag, hedge, hedge_lots, side))
        if result is not None:
            log.info("Hedge order response: %s", result)
        signed_qty = hedge_lots * hedge.lot_size * (1 if side == "BUY" else -1)
        self.book.add(Leg(hedge.ref_id, hedge.option_type, hedge.strike_price, hedge.lot_size, signed_qty, "hedge"))
        self.feed.subscribe([hedge.ref_id])

        self.last_rebalance_ts = now
        self.hedges_today += 1
        return True

    def square_off(self) -> None:
        orders = square_off_orders(self.settings.strat_tag, self.book)
        if not orders:
            return
        log.info("Squaring off %d leg(s) opened by this example", len(orders))
        result = self.gate.send(orders)
        if result is not None:
            log.info("Square-off response: %s", result)

    def run(self) -> None:
        s = self.settings
        log.info(
            "%s | env=%s | delta band=%.2f lots, cooldown=%ss, square-off=%s",
            "ORDERS ENABLED" if s.place_orders else "DRY RUN (no orders will be sent)",
            s.env, s.delta_band_lots, s.rebalance_cooldown_seconds, s.square_off_time,
        )
        self.enter()
        started = time_module.monotonic()
        try:
            while True:
                if self._past_square_off_time():
                    log.info("Square-off time reached.")
                    return
                if s.max_seconds and time_module.monotonic() - started >= s.max_seconds:
                    log.info("Stopping after --max-seconds=%s.", s.max_seconds)
                    return
                self.maybe_rebalance()
                time_module.sleep(2)
        except KeyboardInterrupt:
            log.info("Interrupted.")
        finally:
            self.feed.stop()
            self.square_off()
