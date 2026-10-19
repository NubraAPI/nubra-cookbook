# Options Cockpit in Python: Live Option Chain, OI Walls, PCR and IV Dashboard with the Nubra SDK

> **For educational and demonstration purposes only. Not investment advice.** This example shows how to use the Nubra Python SDK; it is not a recommendation to buy, sell or hold any instrument. Run it on UAT (sandbox) first.

## What it demonstrates

- Taking an **option chain** snapshot with `MarketData.option_chain` and subscribing to live ticks over the websocket (`NubraDataSocket`).
- A local dashboard (standard-library HTTP server) with the live spot, a strike ladder showing **open interest (OI) walls**, IV, **PCR**, a tick tape and API/system status.
- Showing the real error on screen when login, the snapshot or the subscription fails.

It is market data only: it never places or modifies orders. There is no demo data.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env     # fill in PHONE_NO and MPIN (UAT account)
```

## Run

```bash
python main.py                                          # UAT, NIFTY on NSE
python main.py --symbol BANKNIFTY --exchange NSE
python main.py --env PROD --symbol NIFTY --exchange NSE # live market data, still read-only; opt-in
```

Open http://127.0.0.1:8772. Live ticks need market hours.

## Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--env UAT`/`PROD` | `UAT` | Environment. PROD is opt-in. |
| `--symbol` | `NIFTY` | Underlying. |
| `--exchange` | `NSE` | Exchange. |
| `--port` | `8772` | Local port. |

## Files

| File | Purpose |
| --- | --- |
| [main.py](main.py) | Entry point: SDK login, option chain snapshot, websocket subscription, HTTP server and the dashboard page |

## Related

For single-file option chain analysis (max pain, PCR, Greeks) see the [Nubra Python SDK examples](https://github.com/NubraAPI/nubra-python-sdk/tree/main/examples/market_data/option_chain).
