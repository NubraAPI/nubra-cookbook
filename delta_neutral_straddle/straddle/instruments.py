from dataclasses import dataclass
from datetime import date, datetime


@dataclass
class OptionLeg:
    ref_id: int
    option_type: str  # "CE" or "PE"
    strike_price: int  # paise
    lot_size: int
    delta: float
    ltp: float  # paise


def nearest_weekly_expiry(all_expiries: list[str], today: date | None = None) -> str:
    """Pick the earliest expiry that has not already passed."""
    today = today or date.today()
    upcoming = sorted(e for e in all_expiries if datetime.strptime(e, "%Y%m%d").date() >= today)
    if not upcoming:
        raise ValueError("No upcoming expiry found in option chain")
    return upcoming[0]


def _leg(raw, option_type: str) -> OptionLeg:
    return OptionLeg(
        ref_id=int(raw.ref_id),
        option_type=option_type,
        strike_price=int(raw.strike_price),
        lot_size=int(raw.lot_size),
        delta=float(raw.delta or 0.0),
        ltp=float(raw.last_traded_price or 0.0),
    )


def resolve_atm_legs(market_data, symbol: str, exchange: str, expiry: str | None = None):
    """Fetch the option chain and return the chain plus the ATM CE and PE legs."""
    result = market_data.option_chain(symbol, expiry=expiry, exchange=exchange)
    if result is None:
        raise RuntimeError(f"No option chain returned for {symbol} on {exchange}")
    chain = result.chain
    atm_strike = chain.at_the_money_strike

    atm_ce = next((o for o in chain.ce if o.strike_price == atm_strike), None)
    atm_pe = next((o for o in chain.pe if o.strike_price == atm_strike), None)
    if atm_ce is None or atm_pe is None:
        raise RuntimeError(f"ATM strike {atm_strike} has no CE/PE row in the chain")
    return chain, _leg(atm_ce, "CE"), _leg(atm_pe, "PE")


def find_hedge_candidate(chain, option_type: str, min_abs_delta: float = 0.20) -> OptionLeg:
    """Pick a strike on the requested side (CE or PE) to use as a hedge leg.

    Prefers the strike closest to ATM whose |delta| is still above min_abs_delta, so one lot of
    the hedge has a meaningful effect on net delta.
    """
    side = chain.ce if option_type == "CE" else chain.pe
    candidates = [o for o in side if o.delta is not None and abs(o.delta) >= min_abs_delta]
    pool = candidates or side
    closest = min(pool, key=lambda o: abs(o.strike_price - chain.at_the_money_strike))
    return _leg(closest, option_type)
