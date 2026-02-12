# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Taut-Arb-Backtest is a cryptocurrency arbitrage backtesting system that compares Polymarket prediction market prices against model-derived probabilities using Deribit derivatives data. It targets four assets: BTC, ETH, SOL, XRP.

The full technical specification lives in `backtest_guide.md` — read it before implementing any component.

## Architecture

The system has four logical layers:

1. **Data Collection** — Async collectors pulling from 6 APIs (no auth required for any):
   - Polymarket CLOB API (market discovery + price history, cursor-based pagination)
   - Polymarket Gamma API (active markets, offset-based pagination)
   - Goldsky GraphQL subgraph (price backfill for April 2025+, needed because CLOB returns empty for ~30-50% of settled markets)
   - Deribit History API (`history.deribit.com`) — options trades with IV, futures trades, OHLCV candles
   - Deribit Main API (`www.deribit.com`) — funding rates only (not on history API), needs 30-day windowed requests due to ~744 record pagination limit

2. **Storage** — SQLite database (`backtest_data.db`) with 9 tables: `polymarket_markets`, `polymarket_price_history`, `deribit_option_trades`, `deribit_futures_history`, `deribit_funding_history`, `deribit_ohlcv`, `options_snapshots`, `backtest_predictions`, plus indexes. Full DDL in guide Section 8.

3. **Analysis Engine** — Market classification (European Digital vs Barrier One-Touch via regex patterns), IV surface reconstruction from per-trade options data, forward price interpolation from dated futures, Rogers-Satchell realized volatility from 1-hour OHLCV candles, funding rate drift estimation.

4. **Backtest Prediction** — Compares model probabilities vs market probabilities, calculates edge and PnL.

## Data Collection Order (dependencies matter)

1. Polymarket markets (foundation)
2. Polymarket price histories (CLOB first, then Goldsky backfill for gaps)
3. Deribit options trades (day-by-day)
4. Deribit futures trades
5. Deribit funding rates (30-day chunks)
6. Deribit OHLCV candles (1-hour perpetual)

## Key Implementation Constraints

- **IV normalization**: Deribit returns IV as percentage (e.g., 85.5 = 85.5%). Normalize to decimal (0.855) for calculations. Reject IV > 5.0 (500%).
- **Polymarket timestamps**: OHLCV endpoint returns milliseconds; price-history returns seconds. Handle both.
- **Options snapshots**: Require minimum 50 trades within a 24-hour window. Strike prices must be within 3x spot price.
- **Futures filtering**: Exclude perpetuals (`BTC-PERPETUAL`, etc.) from forward curve interpolation — only use dated futures.
- **Funding rates**: Exactly 3 entries per day (8-hour periods). Typical range [-0.001, 0.001].
- **Deribit instrument naming**: Inverse instruments for BTC/ETH, linear for SOL/XRP. Different API path parameter (`kind=any` covers both).

## Current Status

The repository is in specification phase — `backtest_guide.md` contains the complete blueprint. Source code (Python collectors, analysis engine, backtester) needs to be implemented from this spec.
