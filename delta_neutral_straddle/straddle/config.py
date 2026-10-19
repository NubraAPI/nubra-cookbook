from dataclasses import dataclass
from datetime import time


@dataclass(frozen=True)
class Settings:
    env: str = "UAT"              # "UAT" (sandbox) or "PROD"
    totp_login: bool = False
    place_orders: bool = False    # False = dry run: orders are logged, never sent

    symbol: str = "NIFTY"
    exchange: str = "NSE"
    lots: int = 1

    # Net portfolio delta is tracked in "lot-equivalent" units (net delta / lot size).
    # Crossing this band triggers a hedge adjustment.
    delta_band_lots: float = 0.30

    # Minimum time between two hedge adjustments, so one noisy tick cannot trigger repeated orders.
    rebalance_cooldown_seconds: int = 20
    max_hedges_per_day: int = 12

    square_off_time: time = time(15, 20)
    max_seconds: float = 0.0      # 0 = run until the square-off time

    strat_tag: str = "delta-neutral-straddle"
