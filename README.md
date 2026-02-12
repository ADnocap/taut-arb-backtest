# Taut-Arb-Backtest

Backtesting framework for crypto prediction market arbitrage — compares Polymarket prices against model-derived probabilities from Deribit derivatives data.

## Overview

Polymarket lists thousands of prediction markets on crypto price events ("Will BTC be above $100k on Jan 1?"). Each market trades at a price between 0 and 1 that reflects the crowd's implied probability. This system independently prices those same events using options, futures, and funding rate data from Deribit, then identifies mispricings and simulates trading them.

**Assets:** BTC, ETH, SOL, XRP
**Coverage:** April 2025 – February 2026
**Markets:** 30,180 settled prediction markets with 1.1M price observations

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  1. COLLECTION         Async collectors (6 APIs, no auth)   │
│     Polymarket CLOB ─── market discovery + price history    │
│     Polymarket Gamma ── active markets                      │
│     Goldsky GraphQL ─── price backfill for gaps             │
│     Deribit History ─── options trades, futures, OHLCV      │
│     Deribit Main ────── funding rates                       │
├─────────────────────────────────────────────────────────────┤
│  2. STORAGE            SQLite (9 raw tables → 6 snapshot    │
│                        tables in sample DB)                 │
├─────────────────────────────────────────────────────────────┤
│  3. ANALYSIS           IV surface reconstruction            │
│                        Forward curve interpolation           │
│                        Rogers-Satchell realized volatility   │
│                        Funding rate drift estimation         │
├─────────────────────────────────────────────────────────────┤
│  4. BACKTEST           Model probability vs market price    │
│                        Edge calculation + PnL simulation    │
└─────────────────────────────────────────────────────────────┘
```

## Data Sources

| Source | Endpoint | Data |
|--------|----------|------|
| Polymarket CLOB | `clob.polymarket.com` | Market discovery, price history (cursor pagination) |
| Polymarket Gamma | `gamma-api.polymarket.com` | Active markets (offset pagination) |
| Goldsky GraphQL | `api.goldsky.com` | Price backfill for ~30-50% of settled markets |
| Deribit History | `history.deribit.com` | Options trades with IV, dated futures, 1h OHLCV |
| Deribit Main | `www.deribit.com` | 8-hour perpetual funding rates |

No API keys required — all endpoints are public.

## Dataset

Pre-collected databases covering April 2025 – February 2026 are available as a [GitHub Release](https://github.com/ADnocap/taut-arb-backtest/releases/tag/v1.0-data):

| File | Contents | Raw Size | Download |
|------|----------|----------|----------|
| `backtest_data.db.gz` | Raw trade-level data (9 tables, ~12M rows) | 2.4 GB | [Download](https://github.com/ADnocap/taut-arb-backtest/releases/download/v1.0-data/backtest_data.db.gz) |
| `backtest_sample.db.gz` | Hourly snapshots for backtesting (6 tables) | 1.5 GB | [Download](https://github.com/ADnocap/taut-arb-backtest/releases/download/v1.0-data/backtest_sample.db.gz) |

To decompress:

```bash
gunzip backtest_data.db.gz                  # → backtest_data.db (project root)
gunzip backtest_sample.db.gz                # → move to sample/backtest_sample.db
```

## Sample Data

The `sample/` directory contains a pre-built SQLite database with hourly snapshots and diagnostic charts. See [`sample/DATA_GUIDE.md`](sample/DATA_GUIDE.md) for full schema documentation and query examples.

### Asset Prices (OHLCV)

![Asset Prices](sample/asset_prices.png)

### 30,180 Markets by Direction Type

![Markets by Direction](sample/markets_by_direction.png)

### IV Smile — BTC Calls

![IV Smile Example](sample/iv_smile_example.png)

### Funding Rates

![Funding Rates](sample/funding_rates.png)

## Quick Start

```bash
pip install -r requirements.txt

# Collect Polymarket markets for BTC
python collect.py --assets BTC --step 1

# Collect price histories
python collect.py --assets BTC --step 2

# Collect Deribit options, futures, funding, OHLCV (steps 3–6)
python collect.py --assets BTC --step 3
python collect.py --assets BTC --step 4
python collect.py --assets BTC --step 5
python collect.py --assets BTC --step 6

# Validate collected data
python validate.py

# Build sample database with charts
python build_sample.py
```

Collection is idempotent — safe to re-run. Each step resumes from the last saved timestamp.

## Project Structure

```
config.py                       Asset configs, API URLs, constants
database.py                     SQLite schema, batch inserts, resume helpers
classifier.py                   Market classification regex
collect.py                      CLI orchestrator (--assets, --step)
validate.py                     Data quality report (Rich tables)
build_sample.py                 Build sample DB + charts from raw data
collectors/
  base.py                       BaseCollector with retry/backoff/semaphore
  polymarket_markets.py         CLOB + Gamma market discovery
  polymarket_prices.py          CLOB price history + Goldsky backfill
  deribit_options.py            Options trades (day-by-day, IV normalization)
  deribit_futures.py            Dated futures (perpetuals filtered)
  deribit_funding.py            Funding rates (30-day chunks)
  deribit_ohlcv.py              1h OHLCV candles (30-day chunks)
sample/
  DATA_GUIDE.md                 Full schema docs + query examples
  *.png                         Diagnostic charts
backtest_guide.md               Complete technical specification
```

## Status

- Data collection pipeline: **complete** (all 6 APIs, 4 assets)
- Sample database builder: **complete** (hourly snapshots + charts)
- Analysis engine: **in progress**
- Backtester: **planned**
