"""Delta-neutral short ATM straddle: enter, track live net delta and add hedge legs when it drifts.

Type: mutating (dry run by default; sends orders only with --place-orders)
Needs: UAT login (PHONE_NO / MPIN in .env); market hours for live Greeks; `pip install -r requirements.txt`
Expect: log lines for the straddle entry, then a "Rebalancing" line each time net delta leaves the band.
        Without --place-orders every order is only logged as "DRY RUN". The run ends at the square-off time,
        after --max-seconds, or on Ctrl+C, and (with --place-orders) closes only the legs this example opened.
Tested with: nubra-sdk 0.5.4 (UAT)
"""
import argparse
import logging
import sys
from datetime import datetime

from straddle.config import Settings
from straddle.runner import DeltaNeutralStraddle


def parse_time(value: str):
    return datetime.strptime(value, "%H:%M").time()


def parse_args(argv):
    p = argparse.ArgumentParser(description="Delta-neutral short ATM straddle example (educational; not investment advice).")
    p.add_argument("--env", choices=["uat", "prod"], default="uat", help="Default: uat (sandbox).")
    p.add_argument("--place-orders", action="store_true", help="Send orders. Without it every order is only logged.")
    p.add_argument("--allow-prod", action="store_true", help="Required together with --place-orders on --env prod.")
    p.add_argument("--totp-login", action="store_true", help="Use the SDK's TOTP login flow.")
    p.add_argument("--symbol", default="NIFTY")
    p.add_argument("--exchange", default="NSE")
    p.add_argument("--lots", type=int, default=1)
    p.add_argument("--delta-band-lots", type=float, default=0.30, help="Hedge when |net delta| exceeds this many lots.")
    p.add_argument("--cooldown-seconds", type=int, default=20, help="Minimum seconds between hedge adjustments.")
    p.add_argument("--max-hedges", type=int, default=12, help="Maximum hedge adjustments per run.")
    p.add_argument("--square-off-time", type=parse_time, default=parse_time("15:20"), help="HH:MM local time.")
    p.add_argument("--max-seconds", type=float, default=0.0, help="Stop after this many seconds (0 = until square-off time).")
    args = p.parse_args(argv)
    if args.lots < 1:
        p.error("--lots must be at least 1")
    if args.env == "prod" and args.place_orders and not args.allow_prod:
        p.error("--place-orders on --env prod also requires --allow-prod")
    return args


def main(argv=None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings(
        env=args.env.upper(),
        totp_login=args.totp_login,
        place_orders=args.place_orders,
        symbol=args.symbol,
        exchange=args.exchange,
        lots=args.lots,
        delta_band_lots=args.delta_band_lots,
        rebalance_cooldown_seconds=args.cooldown_seconds,
        max_hedges_per_day=args.max_hedges,
        square_off_time=args.square_off_time,
        max_seconds=args.max_seconds,
    )
    DeltaNeutralStraddle(settings).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
