# Delta-Neutral Short Straddle in Python with the Nubra SDK (ATM options + delta hedge adjustments)

> **For educational and demonstration purposes only. Not investment advice.** This example shows how to use the Nubra Python SDK; it is not a recommendation to buy, sell or hold any instrument. Short options carry large risk. Run it on UAT (sandbox) first.

## What it demonstrates

- Resolving the ATM strike from a live option chain and building a **multi-leg strategy order** (sell 1 ATM call + 1 ATM put) with `NubraTrader.create_order`.
- Streaming **Greeks** over the websocket (`NubraDataSocket`, `data_type="greeks"`) and keeping a live delta cache per `ref_id`.
- Tracking **net delta** across all open legs in lot-equivalent units, and adding a long call or put when it leaves a band.
- A single order gate, so dry run vs real orders is decided in one place.

## How it works

1. **Entry:** one multi-leg order sells `--lots` lots each of the ATM call and put for the nearest expiry (MARKET, `IOC`).
2. **Track:** every open leg's delta comes from the Greeks stream. Net delta in lots = sum(signed quantity x delta) / lot size.
3. **Hedge:** if `|net delta|` exceeds `--delta-band-lots`, and the cooldown and daily limit allow it, the example buys a put (when net delta is too positive) or a call (when too negative). It picks the strike nearest ATM with `|delta| >= 0.20` and sizes the lots as `ceil(|net delta| / |hedge delta|)`.
4. **Exit:** at `--square-off-time`, after `--max-seconds`, or on Ctrl+C it closes **only the legs this run opened**. It never calls the account-wide `exit_all_positions`.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env     # then fill in PHONE_NO and MPIN (UAT account)
```

`.env` is git-ignored. The SDK reads it from the current folder.

## Run

```bash
python main.py --help
python main.py                                   # dry run on UAT: logs every order, sends nothing
python main.py --max-seconds 120                 # dry run for two minutes
python main.py --place-orders                    # sends orders to UAT
python main.py --delta-band-lots 0.5 --lots 2    # tune the band and size
```

### Orders are off by default

Without `--place-orders` every order is logged as `DRY RUN` and the example keeps a paper position so you can watch the hedge decisions. `--env prod` is opt-in, and `--place-orders` on PROD also needs `--allow-prod`.

## Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--env uat\|prod` | `uat` | Environment. |
| `--place-orders` | off | Send orders instead of logging them. |
| `--allow-prod` | off | Required with `--place-orders` on prod. |
| `--symbol`, `--exchange` | `NIFTY`, `NSE` | Underlying. |
| `--lots` | `1` | Lots per leg. |
| `--delta-band-lots` | `0.30` | Hedge when `|net delta|` is above this many lots. |
| `--cooldown-seconds` | `20` | Minimum time between hedges. |
| `--max-hedges` | `12` | Hedge adjustments per run. |
| `--square-off-time` | `15:20` | Local time (HH:MM) to stop and close. |
| `--max-seconds` | `0` | Stop after N seconds (0 = until square-off time). |
| `--totp-login` | off | Use the SDK's TOTP login. |

## Files

| File | Purpose |
| --- | --- |
| [main.py](main.py) | Entry point and flags |
| [straddle/runner.py](straddle/runner.py) | Entry, hedge loop and square-off |
| [straddle/delta_engine.py](straddle/delta_engine.py) | Position book and Greeks feed |
| [straddle/instruments.py](straddle/instruments.py) | ATM and hedge strike selection from the option chain |
| [straddle/order_manager.py](straddle/order_manager.py) | Order payloads and the dry-run gate |
| [straddle/config.py](straddle/config.py) | Settings |
| [tests/test_straddle.py](tests/test_straddle.py) | Offline tests for the sizing and square-off logic |

## Notes

- Market orders use `IOC` validity; the exchange rejects MARKET orders with `DAY` validity.
- Hedges here are long options, which decay with time; this is a teaching example, not an optimised hedging scheme. For futures-based and gamma-aware hedging see the sibling projects [delta_hedging](../delta_hedging) and [gamma_hedging](../gamma_hedging).
- Greeks need market hours. Outside them the example waits for ticks and logs no hedges.
