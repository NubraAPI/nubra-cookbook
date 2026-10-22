# Nubra Python SDK Cookbook: Runnable Examples for Trading and Market Data
The official Nubra Python SDK cookbook provides runnable examples and practical recipes for authentication, UAT sandbox testing, order execution, live market data, WebSockets, option chains, historical data and portfolio management.

> **For educational and demonstration purposes only. Not investment advice.** These projects show how to use the [Nubra Python SDK](https://github.com/NubraAPI/nubra-python-sdk); they are not a recommendation to buy, sell or hold any instrument. Trading in the securities market is subject to market risk. Run everything on UAT (sandbox) first.

Complete, runnable Python projects for Indian options traders and students who want to see the Nubra API used end to end: option chain and Greeks data, live websocket streams, historical data, hedging logic and dashboards. Each folder is a self-contained starter template with its own `README.md`, `requirements.txt` and `.env.example`.

Looking for short, single-purpose API snippets (login, option chain, max pain, PCR, Greeks, orders)? Those live in the SDK repo's [examples](https://github.com/NubraAPI/nubra-python-sdk/tree/main/examples) folder. This repo is for the larger projects, including ones that can place orders.

## Projects

| Project | What it does | Entry point |
| --- | --- | --- |
| [delta_hedging](delta_hedging) | Delta hedging a long future + short ATM calls position on NSE stock options: replay on historical bars, live Greeks dashboard, liquidity scan and an account-wide loss watchdog | [main.py](delta_hedging/main.py) |
| [gamma_hedging](gamma_hedging) | Gamma-aware dynamic delta hedging for SENSEX option sellers, with a terminal and a browser dashboard | [main.py](gamma_hedging/main.py) |
| [delta_neutral_backtest](delta_neutral_backtest) | Backtest a short ATM straddle with and without a futures delta hedge on historical 1-minute option and index data | [run_backtest.py](delta_neutral_backtest/run_backtest.py) |
| [open_interest_dashboard](open_interest_dashboard) | NIFTY 50 constituent pressure, option chain open interest (OI) change and gamma-aware contract ranking on a live terminal and web dashboard | [main.py](open_interest_dashboard/main.py) |
| [delta_neutral_straddle](delta_neutral_straddle) | Delta-neutral short ATM straddle: enter from the live option chain, track net delta from the Greeks stream and add hedge legs when it drifts | [main.py](delta_neutral_straddle/main.py) |
| [websocket_monitor](websocket_monitor) | Local dashboard for a live market-data websocket: tick tape, latency trace, heartbeat and event log (demo mode needs no login) | [main.py](websocket_monitor/main.py) |
| [order_lifecycle_console](order_lifecycle_console) | Local dashboard that tracks order states and realtime order updates, with optional UAT test orders | [main.py](order_lifecycle_console/main.py) |
| [trader_cockpit](trader_cockpit) | Local option chain dashboard: live spot, OI wall ladder, IV, PCR and tick tape (market data only) | [main.py](trader_cockpit/main.py) |

## Setup

1. Python 3.9+ and a Nubra UAT account. Install a project's dependencies: `pip install -r requirements.txt` inside its folder.
2. Copy the project's `.env.example` to `.env` and fill in `PHONE_NO` and `MPIN`. Never commit `.env`; it is git-ignored.
3. Follow the project's own README to run it.

## Conventions

- **UAT by default.** `--env prod` is opt-in.
- **Orders are off by default.** Anything that can place, modify or cancel an order prints a dry run unless you pass `--place-orders`; on PROD it also needs `--allow-prod`.
- Prices are integer paise in SDK payloads and printed as rupees.
- Backtests are historical simulations and do not indicate future results.

## Verification status

The first four projects were run against the UAT sandbox on 2026-10-19: the backtest on historical data, and the dashboards, scanner, watchdog and hedgers live in dry-run mode with real option chains, Greeks and websocket streams. `delta_neutral_straddle`, `websocket_monitor`, `order_lifecycle_console` and `trader_cockpit` were checked on UAT after market hours: login, option chain and order-list requests, the margin API for the straddle's order payloads, and each local server. Live ticks and Greeks were not exercised for those four.

Not verified live: actual order placement, modification and cancellation (`--place-orders`), because the UAT test account had no funds; those payloads were checked against the installed SDK only. UAT returns no previous open interest, so the open interest dashboard's OI-change score stays at zero there; use PROD data to see it move.

## Related

- [Nubra Python SDK](https://github.com/NubraAPI/nubra-python-sdk): install with `pip install nubra-sdk`; API how-to examples, guide and response schemas.
- Single-file option chain analysis in the SDK repo: [max pain, PCR and OI analysis](https://github.com/NubraAPI/nubra-python-sdk/blob/main/examples/market_data/option_chain/max_pain_pcr_oi_analysis.py), [option Greeks near ATM](https://github.com/NubraAPI/nubra-python-sdk/blob/main/examples/market_data/option_chain/option_greeks_atm.py) and an [option strategy payoff calculator](https://github.com/NubraAPI/nubra-python-sdk/blob/main/examples/market_data/option_chain/option_strategy_payoff_calculator.py).

## Contributing and security

Open an issue or pull request. Never include credentials, session files or account identifiers in either; report vulnerabilities privately.

## License

MIT, see [LICENSE](LICENSE).
