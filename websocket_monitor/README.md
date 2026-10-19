# Nubra WebSocket Monitor in Python: Live Market Data Stream Dashboard (ticks, latency, heartbeat)

> **For educational and demonstration purposes only. Not investment advice.** This example shows how to use the Nubra Python SDK; it is not a recommendation to buy, sell or hold any instrument. Run it on UAT (sandbox) first.

## What it demonstrates

- Connecting to the Nubra **market-data websocket** with `NubraDataSocket` and subscribing to index/stock ticks (`data_type="index"`).
- Serving a small **local dashboard** (standard-library HTTP server, no front-end build) with a tick tape, latency trace, heartbeat, instrument map and connection event log.
- Handling connect, close and error callbacks so you can see what a stream is doing.

It is read-only: it never places or modifies orders.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env     # only needed for --mode live or --auth; fill in PHONE_NO and MPIN (UAT account)
```

## Run

```bash
python main.py --mode demo                                   # no credentials: streams generated SAMPLE ticks
python main.py --mode demo --auth                            # demo ticks, but log in through the SDK first
python main.py --mode live --env UAT --symbols NIFTY,HDFCBANK --exchange NSE
```

Open the URL printed in the terminal, by default http://127.0.0.1:8771. Live mode needs market hours for ticks. Let it run for 20-30 seconds so the tape, latency chart and event log fill up.

**Demo mode shows generated sample data, not market prices.** The page labels it `demo`; use `--mode live` for real ticks.

## Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--mode demo`/`live` | `demo` | Sample ticks or the real websocket. |
| `--env UAT`/`PROD` | `UAT` | Environment for `--auth` and `--mode live`. |
| `--auth` | off | Run the SDK login flow even in demo mode. |
| `--exchange` | `NSE` | Exchange for the symbols. |
| `--symbols` | `NIFTY,BANKNIFTY,FINNIFTY,HDFCBANK,RELIANCE,SBIN` | Comma-separated symbols to subscribe to. |
| `--port` | `8771` | Local port. |

## Files

| File | Purpose |
| --- | --- |
| [main.py](main.py) | Entry point: SDK connection, demo streamer, HTTP server and the dashboard page |
