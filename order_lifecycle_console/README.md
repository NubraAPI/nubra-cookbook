# Order Lifecycle Console in Python: Track Order Status and Realtime Order Updates with the Nubra SDK

> **For educational and demonstration purposes only. Not investment advice.** This example shows how to use the Nubra Python SDK; it is not a recommendation to buy, sell or hold any instrument. Run it on UAT (sandbox) first.

## What it demonstrates

- Polling the day's **orders** and listening to the **order-update websocket** (`OrderUpdate`) at the same time.
- Following one order through its states (placed, open, modified, executed, cancelled, rejected) on a timeline, an execution blotter and a millisecond-level update log.
- Optionally generating UAT orders to watch, with a modify and a cancel step.

There is no demo data. If login, order polling or the order-update socket fails, the page shows the error instead of fake rows. On UAT the order-update connection can fail because UAT returns an empty order-service URL; that is an environment limit.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env     # fill in PHONE_NO and MPIN (UAT account)
```

## Run

```bash
python main.py --env UAT          # view real UAT orders and updates; places nothing
```

Open http://127.0.0.1:8770.

### Optional: place UAT test orders (off by default)

These flags place **real UAT orders** and are the only way this example sends an order. `--env` accepts only `UAT`; PROD is refused.

```bash
# One passive limit order on a stock, cancelled after 4 seconds
python main.py --env UAT --place-test-order --symbol HDFCBANK --exchange NSE --cancel-after 4

# One NIFTY option order (ATM call or put from the live chain), modified after 3 s and cancelled after 5 s
python main.py --env UAT --nifty-options-algo --underlying NIFTY --option-side AUTO --modify-after 3 --cancel-after 5
```

The generated order tag starts with `nifty_exec_`. Orders need funds in the UAT account.

## Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--env UAT` | `UAT` | Only UAT is allowed. |
| `--place-test-order` | off | Place one passive limit order on `--symbol`. |
| `--nifty-options-algo` | off | Resolve the NIFTY option chain and place one tagged option order. |
| `--symbol`, `--exchange` | `HDFCBANK`, `NSE` | Instrument for `--place-test-order`. |
| `--underlying`, `--option-side` | `NIFTY`, `AUTO` | Underlying and `AUTO`/`CE`/`PE` for the option order. |
| `--qty` | `0` | Quantity; 0 uses the contract lot size for options. |
| `--passive-offset-pct` | `4.0` | How far below LTP a passive buy limit is placed. |
| `--modify-after`, `--modify-ticks` | `0`, `500` | Modify the order after N seconds, by this many price units. |
| `--cancel-after` | `0` | Cancel the order after N seconds. |
| `--poll-seconds` | `1.0` | Order polling interval. |
| `--port` | `8770` | Local port. |

## Files

| File | Purpose |
| --- | --- |
| [main.py](main.py) | Entry point: SDK login, order polling, order-update socket, optional UAT order helper and the dashboard page |

`.uat_order_run.lock` is created next to `main.py` while an order run is active so two runs do not overlap; it is git-ignored.
