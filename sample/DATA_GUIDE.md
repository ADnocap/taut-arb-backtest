# Sample Database Guide

`backtest_sample.db` is a self-contained SQLite database built from raw data collected across 6 APIs. It contains nine tables — including DVOL volatility index data and volatility-of-volatility (VoV) — everything needed to backtest crypto prediction market arbitrage strategies for BTC, ETH, SOL, and XRP.

**Size:** ~1.5 GB | **Coverage:** 2025-04-04 to 2026-02-12 | **Format:** SQLite 3 (WAL mode)

## How It Was Created

The sample database is built by `build_sample.py` from a raw collection database (`backtest_data.db`). The raw database is populated by async collectors that pull from:

| Source | What it provides |
|--------|-----------------|
| Polymarket CLOB API | Market discovery + price history (cursor-based pagination) |
| Polymarket Gamma API | Active markets (offset-based pagination) |
| Goldsky GraphQL subgraph | Price backfill for markets where CLOB returns empty (~30-50% of settled markets) |
| Deribit History API (`history.deribit.com`) | Options trades with IV, futures trades, OHLCV candles |
| Deribit Main API (`www.deribit.com`) | Funding rates, DVOL volatility index |

The build script transforms raw trade-level data into hourly snapshots using a 24-hour sliding window. For each hour, it takes the most recent trade per instrument within the past 24 hours and emits that as the snapshot. This compresses millions of individual trades into a regular hourly grid suitable for backtesting.

To rebuild the sample database:

```bash
py -3.11 build_sample.py            # full rebuild with charts
py -3.11 build_sample.py --no-charts # skip chart generation
```

This drops and recreates `sample/backtest_sample.db` from scratch (~2 minutes).

---

## Schema Overview

Nine tables, organized in four layers:

```
Prediction Markets          Derivatives Snapshots         Market Data
==================          =====================         ===========
markets ──< market_prices   options_snapshots              funding_rates
                            futures_snapshots              ohlcv

Volatility
==========
dvol_official    (Deribit DVOL index, BTC/ETH)
dvol_computed    (VIX-style DVOL from options, BTC/ETH/SOL)
vov              (volatility-of-volatility, daily)
```

---

## Table Reference

### 1. `markets` — 30,180 rows

Each row is a settled Polymarket prediction market about a crypto price event. Markets are classified into five direction types by regex pattern matching on the question text.

| Column | Type | Description |
|--------|------|-------------|
| `condition_id` | TEXT PK | Polymarket unique market identifier |
| `asset` | TEXT | `BTC`, `ETH`, `SOL`, or `XRP` |
| `threshold` | REAL | Strike price extracted from question (e.g., 100000.0) |
| `upper_threshold` | REAL | Upper bound for `between` markets, NULL otherwise |
| `direction` | TEXT | Market type (see below) |
| `settlement_date` | TEXT | ISO 8601 settlement datetime |
| `outcome` | INTEGER | 1 = YES won, 0 = NO won |
| `yes_token_id` | TEXT | CLOB token ID for the YES outcome |
| `no_token_id` | TEXT | CLOB token ID for the NO outcome |
| `question` | TEXT | Original market question |

**Direction types:**

| Direction | Count | Description | Example |
|-----------|-------|-------------|---------|
| `above` | 15,737 | European digital — price above threshold at settlement | "Will BTC be above $100k on Jan 1?" |
| `between` | 10,801 | European digital — price between two thresholds | "Will ETH be between $3k and $4k?" |
| `below` | 1,433 | European digital — price below threshold at settlement | "Will SOL be below $200?" |
| `reach` | 1,109 | Barrier one-touch — price touches threshold before settlement | "Will BTC reach $120k?" |
| `dip` | 1,100 | Barrier one-touch — price dips to threshold before settlement | "Will ETH dip to $2000?" |

**Rows per asset:** BTC: 8,009 | ETH: 7,962 | SOL: 7,270 | XRP: 6,939

---

### 2. `market_prices` — 1,053,736 rows

Time series of YES/NO prices for each market. Sourced from Polymarket CLOB price history (primary) with Goldsky GraphQL backfill for gaps.

| Column | Type | Description |
|--------|------|-------------|
| `condition_id` | TEXT FK | References `markets.condition_id` |
| `timestamp` | INTEGER | Unix timestamp in **seconds** |
| `yes_price` | REAL | YES token price, 0.0 to 1.0 (nullable) |
| `no_price` | REAL | Independent NO token price, 0.0 to 1.0 (nullable) |
| `volume` | REAL | Trade volume in the bucket |
| `trade_count` | INTEGER | Number of trades in the bucket |
| `source` | TEXT | Data source: `clob` or `goldsky` |

**Unique constraint:** `(condition_id, timestamp)`
**Index:** `idx_mp` on `(condition_id, timestamp)`

**Rows per asset:** BTC: 444,517 | ETH: 296,619 | SOL: 178,616 | XRP: 133,984
**Date range:** 2025-04-04 to 2026-02-11

**Important:** Timestamps are in **seconds** (not milliseconds), unlike the other tables.

---

### 3. `options_snapshots` — 7,233,951 rows

Hourly snapshots of Deribit options, built from individual trades via a 24-hour sliding window. Each row represents one instrument's state at one snapshot hour — the most recent trade for that instrument within the prior 24 hours.

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_hour` | INTEGER | Hour-floored Unix timestamp in **milliseconds** |
| `asset` | TEXT | `BTC`, `ETH`, `SOL`, or `XRP` |
| `instrument_name` | TEXT | Deribit instrument (e.g., `BTC-8APR25-77000-P`) |
| `strike` | REAL | Strike price in USD |
| `expiry_date` | TEXT | ISO 8601 expiry datetime |
| `expiry_str` | TEXT | Compact expiry `YYMMDD` (e.g., `250408`) |
| `option_type` | TEXT | `C` (call) or `P` (put) |
| `mark_iv` | REAL | Implied volatility as **annualized decimal** (e.g., 0.8371 = 83.71%) |
| `bid` | REAL | Always NULL (not available from trade data) |
| `ask` | REAL | Always NULL (not available from trade data) |
| `mark_price` | REAL | Option price in **USD** (converted from BTC/ETH for inverse instruments) |
| `underlying_price` | REAL | Spot index price in USD at time of trade |

**Unique constraint:** `(snapshot_hour, asset, instrument_name)`
**Index:** `idx_opts` on `(asset, snapshot_hour)`

**Rows per asset:** BTC: 3,006,589 | ETH: 3,114,667 | SOL: 1,053,145 | XRP: 59,550
**Distinct hours:** BTC: 7,328 | ETH: 7,464 | SOL: 7,441 | XRP: 6,191
**Date range:** 2025-04-07 to 2026-02-11

**Sliding window details:** A snapshot is emitted for an hour only if the 24-hour window contains enough distinct instruments (BTC/ETH/SOL: 50+, XRP: 5+ due to thin trading). XRP options are far less liquid — max ~28 instruments in any 24h window vs 400+ for BTC/ETH.

**IV normalization:** Raw Deribit IV is a percentage (85.5 = 85.5%). Values are normalized to decimal (0.855). Trades with IV > 5.0 (500%) or IV <= 0 are rejected.

**Mark price conversion:** For inverse instruments (BTC, ETH), `mark_price` is converted from the native denomination (BTC or ETH) to USD by multiplying by `underlying_price`. SOL and XRP are linear (USDC-settled) so no conversion needed.

---

### 4. `futures_snapshots` — 118,320 rows

Hourly snapshots of dated futures (BTC, ETH) or synthetic spot (SOL, XRP).

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_hour` | INTEGER | Hour-floored Unix timestamp in **milliseconds** |
| `asset` | TEXT | `BTC`, `ETH`, `SOL`, or `XRP` |
| `instrument_name` | TEXT | Deribit futures name (e.g., `BTC-25APR25`) or `SPOT` |
| `expiry_date` | TEXT | ISO 8601 expiry datetime (NULL for SPOT) |
| `expiry_str` | TEXT | Compact expiry `YYMMDD` (NULL for SPOT) |
| `mark_price` | REAL | Futures price in USD |
| `delivery_price` | REAL | Delivery/settlement price (if available) |
| `underlying_price` | REAL | Spot index price in USD |

**Unique constraint:** `(snapshot_hour, asset, instrument_name)`
**Index:** `idx_fut` on `(asset, snapshot_hour)`

**Rows per asset:** BTC: 51,652 | ETH: 51,736 | SOL: 7,466 | XRP: 7,466
**Date range:** 2025-04-07 to 2026-02-12

**BTC/ETH** have real dated futures from Deribit trade data (sliding window, same 24h mechanism as options). Perpetuals (`BTC-PERPETUAL`, `ETH-PERPETUAL`) are excluded — only dated futures used for forward curve interpolation.

**SOL/XRP** do not have dated futures on Deribit. Instead, synthetic `SPOT` rows are generated from OHLCV close prices — one row per hour.

---

### 5. `funding_rates` — 29,860 rows

8-hour perpetual funding rates from Deribit.

| Column | Type | Description |
|--------|------|-------------|
| `asset` | TEXT | `BTC`, `ETH`, `SOL`, or `XRP` |
| `timestamp` | INTEGER | Unix timestamp in **milliseconds** |
| `interest_8h` | REAL | 8-hour funding rate (typically in [-0.001, 0.001]) |

**Unique constraint:** `(asset, timestamp)`
**Index:** `idx_fund` on `(asset, timestamp)`

**Rows per asset:** 7,465 each (exactly 3 per day, 8-hour periods)
**Date range:** 2025-04-07 to 2026-02-12

Positive `interest_8h` means longs pay shorts; negative means shorts pay longs. Annualized funding = `interest_8h * 3 * 365`.

---

### 6. `ohlcv` — 29,864 rows

1-hour OHLCV candles for each asset's perpetual contract on Deribit.

| Column | Type | Description |
|--------|------|-------------|
| `asset` | TEXT | `BTC`, `ETH`, `SOL`, or `XRP` |
| `timestamp` | INTEGER | Hour-start Unix timestamp in **milliseconds** |
| `open` | REAL | Open price (USD) |
| `high` | REAL | High price (USD) |
| `low` | REAL | Low price (USD) |
| `close` | REAL | Close price (USD) |
| `volume` | REAL | Volume in native asset units |

**Unique constraint:** `(asset, timestamp)`
**Index:** `idx_ohlcv` on `(asset, timestamp)`

**Rows per asset:** 7,466 each
**Date range:** 2025-04-07 to 2026-02-12

Used for Rogers-Satchell realized volatility calculation and general price reference.

---

### 7. `dvol_official` — 15,014 rows

Hourly candles of Deribit's official DVOL volatility index. Available for BTC and ETH only (SOL/XRP not published by Deribit).

| Column | Type | Description |
|--------|------|-------------|
| `asset` | TEXT | `BTC` or `ETH` |
| `timestamp` | INTEGER | Hour-start Unix timestamp in **milliseconds** |
| `open` | REAL | DVOL open, annualized decimal (e.g., 0.58 = 58%) |
| `high` | REAL | DVOL high |
| `low` | REAL | DVOL low |
| `close` | REAL | DVOL close |

**Unique constraint:** `(asset, timestamp)`
**Index:** `idx_dvol_off` on `(asset, timestamp)`

**Rows per asset:** BTC: 7,302 | ETH: 7,712
**Date range:** 2025-04-07 to 2026-02-12

---

### 8. `dvol_computed` — 21,530 rows

VIX-style DVOL computed from options snapshots using the Carr-Madan variance swap replication method (Black-76 pricing). Covers BTC, ETH, and SOL; XRP is too sparse to compute reliably.

| Column | Type | Description |
|--------|------|-------------|
| `asset` | TEXT | `BTC`, `ETH`, or `SOL` |
| `snapshot_hour` | INTEGER | Hour-floored Unix timestamp in **milliseconds** |
| `dvol` | REAL | Computed DVOL, annualized decimal (e.g., 0.55 = 55%) |
| `quality` | TEXT | Computation quality indicator |
| `near_expiry` | TEXT | Near-term expiry used in interpolation |
| `far_expiry` | TEXT | Far-term expiry used in interpolation |
| `n_near_strikes` | INTEGER | Number of OTM strikes in near-term expiry |
| `n_far_strikes` | INTEGER | Number of OTM strikes in far-term expiry |

**Unique constraint:** `(asset, snapshot_hour)`
**Index:** `idx_dvol_comp` on `(asset, snapshot_hour)`

**Rows per asset:** BTC: 7,302 | ETH: 7,412 | SOL: 6,816
**Date range:** 2025-04-07 to 2026-02-12

**Validation:** Computed DVOL correlates well with official Deribit DVOL — BTC r=0.9154, ETH r=0.9539. See `dvol_comparison_btc.png` and `dvol_comparison_eth.png`.

**Requirements:** Each computation requires 3+ OTM strikes per side, 2 expiries bracketing the 30-day target tenor.

---

### 9. `vov` — 925 rows

Daily volatility-of-volatility (VoV) derived from DVOL. Used to scale model parameters based on the current vol regime.

| Column | Type | Description |
|--------|------|-------------|
| `asset` | TEXT | `BTC`, `ETH`, or `SOL` |
| `timestamp` | INTEGER | Day-start Unix timestamp in **milliseconds** |
| `dvol_daily` | REAL | Daily DVOL value (last computed hourly value of the day) |
| `log_return` | REAL | Log-return of daily DVOL |
| `vov` | REAL | 30-day rolling std of daily DVOL log-returns, annualized (x sqrt(365)) |
| `f_vov` | REAL | VoV scaling factor: min((VoV_t / VoV_bar)^0.75, 2.0) |

**Unique constraint:** `(asset, timestamp)`
**Index:** `idx_vov` on `(asset, timestamp)`

**Rows per asset:** BTC: 306 | ETH: 311 | SOL: 308
**Date range:** 2025-05-07 to 2026-02-12 (30-day lookback required)

**f_vov** defaults to 1.0 when VoV is unavailable (e.g., XRP, or the first 30 days).

---

## Timestamp Conventions

| Table | Timestamp column | Unit | Example |
|-------|-----------------|------|---------|
| `market_prices` | `timestamp` | **Seconds** | `1743724800` = 2025-04-04T00:00:00Z |
| `options_snapshots` | `snapshot_hour` | **Milliseconds** | `1743984000000` = 2025-04-07T00:00:00Z |
| `futures_snapshots` | `snapshot_hour` | **Milliseconds** | `1743984000000` |
| `funding_rates` | `timestamp` | **Milliseconds** | `1743987600000` |
| `ohlcv` | `timestamp` | **Milliseconds** | `1743984000000` |
| `dvol_official` | `timestamp` | **Milliseconds** | `1743984000000` |
| `dvol_computed` | `snapshot_hour` | **Milliseconds** | `1743984000000` |
| `vov` | `timestamp` | **Milliseconds** | `1743984000000` |

To convert millisecond timestamps to human-readable dates in SQL:

```sql
datetime(snapshot_hour / 1000, 'unixepoch')   -- for ms timestamps
datetime(timestamp, 'unixepoch')               -- for market_prices (seconds)
```

---

## Query Examples

### Basic Counts

```sql
-- Row counts per table
SELECT 'markets' as tbl, COUNT(*) FROM markets
UNION ALL SELECT 'market_prices', COUNT(*) FROM market_prices
UNION ALL SELECT 'options_snapshots', COUNT(*) FROM options_snapshots
UNION ALL SELECT 'futures_snapshots', COUNT(*) FROM futures_snapshots
UNION ALL SELECT 'funding_rates', COUNT(*) FROM funding_rates
UNION ALL SELECT 'ohlcv', COUNT(*) FROM ohlcv
UNION ALL SELECT 'dvol_official', COUNT(*) FROM dvol_official
UNION ALL SELECT 'dvol_computed', COUNT(*) FROM dvol_computed
UNION ALL SELECT 'vov', COUNT(*) FROM vov;
```

### Market Exploration

```sql
-- Markets by asset and direction
SELECT asset, direction, COUNT(*) as n
FROM markets
GROUP BY asset, direction
ORDER BY asset, n DESC;

-- Win rate per direction
SELECT direction,
       COUNT(*) as total,
       SUM(outcome) as yes_wins,
       ROUND(100.0 * SUM(outcome) / COUNT(*), 1) as yes_pct
FROM markets
GROUP BY direction;

-- Find BTC markets with threshold near $100k
SELECT question, threshold, direction, outcome,
       date(settlement_date) as settles
FROM markets
WHERE asset = 'BTC' AND threshold BETWEEN 95000 AND 105000
ORDER BY settlement_date;
```

### Price Trajectories

```sql
-- Price history for a specific market
SELECT datetime(timestamp, 'unixepoch') as dt,
       yes_price, no_price
FROM market_prices
WHERE condition_id = '0x...'
ORDER BY timestamp;

-- Average YES price in the last 24h before settlement
SELECT m.condition_id, m.question, m.outcome,
       AVG(mp.yes_price) as avg_final_price
FROM markets m
JOIN market_prices mp ON m.condition_id = mp.condition_id
WHERE mp.timestamp > CAST(strftime('%s', m.settlement_date) AS INTEGER) - 86400
GROUP BY m.condition_id;
```

### Options Surface at a Point in Time

```sql
-- IV smile for BTC calls at a specific hour
SELECT strike, mark_iv, instrument_name
FROM options_snapshots
WHERE asset = 'BTC'
  AND snapshot_hour = 1743984000000
  AND option_type = 'C'
  AND expiry_str = '250425'
ORDER BY strike;

-- All distinct expiries available at a given hour
SELECT expiry_str, expiry_date, COUNT(*) as instruments
FROM options_snapshots
WHERE asset = 'ETH' AND snapshot_hour = 1743984000000
GROUP BY expiry_str
ORDER BY expiry_date;

-- ATM implied volatility over time (nearest strike to spot)
SELECT o.snapshot_hour,
       datetime(o.snapshot_hour / 1000, 'unixepoch') as dt,
       o.mark_iv,
       o.strike,
       o.underlying_price
FROM options_snapshots o
INNER JOIN (
    SELECT snapshot_hour, MIN(ABS(strike - underlying_price)) as min_dist
    FROM options_snapshots
    WHERE asset = 'BTC' AND option_type = 'C' AND expiry_str = '250627'
    GROUP BY snapshot_hour
) best ON o.snapshot_hour = best.snapshot_hour
    AND ABS(o.strike - o.underlying_price) = best.min_dist
WHERE o.asset = 'BTC' AND o.option_type = 'C' AND o.expiry_str = '250627'
ORDER BY o.snapshot_hour;
```

### Forward Curve

```sql
-- BTC forward curve at a given hour
SELECT instrument_name, expiry_str, expiry_date,
       mark_price, underlying_price,
       mark_price - underlying_price as basis
FROM futures_snapshots
WHERE asset = 'BTC' AND snapshot_hour = 1743984000000
ORDER BY expiry_date;

-- SOL spot price over time (synthetic from OHLCV)
SELECT datetime(snapshot_hour / 1000, 'unixepoch') as dt,
       mark_price as spot
FROM futures_snapshots
WHERE asset = 'SOL'
ORDER BY snapshot_hour;
```

### Funding & Volatility

```sql
-- Cumulative funding for BTC over a date range
SELECT datetime(timestamp / 1000, 'unixepoch') as dt,
       interest_8h,
       SUM(interest_8h) OVER (ORDER BY timestamp) as cumulative
FROM funding_rates
WHERE asset = 'BTC'
ORDER BY timestamp;

-- Rogers-Satchell hourly realized vol components
SELECT datetime(timestamp / 1000, 'unixepoch') as dt,
       (LN(high / close) * LN(high / open)
        + LN(low / close) * LN(low / open)) as rs_component
FROM ohlcv
WHERE asset = 'ETH'
ORDER BY timestamp;

-- 24h rolling realized volatility (annualized)
SELECT timestamp,
       datetime(timestamp / 1000, 'unixepoch') as dt,
       SQRT(
           AVG(LN(high/close) * LN(high/open) + LN(low/close) * LN(low/open))
           OVER (ORDER BY timestamp ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)
           * 8760
       ) as rv_annualized
FROM ohlcv
WHERE asset = 'BTC'
ORDER BY timestamp;
```

### DVOL & Volatility-of-Volatility

```sql
-- Compare official vs computed DVOL for BTC
SELECT o.timestamp,
       datetime(o.timestamp / 1000, 'unixepoch') as dt,
       o.close as official, c.dvol as computed
FROM dvol_official o
JOIN dvol_computed c ON c.asset = o.asset AND c.snapshot_hour = o.timestamp
WHERE o.asset = 'BTC'
ORDER BY o.timestamp;

-- Daily VoV and scaling factor over time
SELECT datetime(timestamp / 1000, 'unixepoch') as dt,
       asset, dvol_daily, vov, f_vov
FROM vov
WHERE asset = 'BTC'
ORDER BY timestamp;

-- Average f_vov by asset (how much vol regime differs from baseline)
SELECT asset,
       ROUND(AVG(f_vov), 3) as avg_f_vov,
       ROUND(MIN(f_vov), 3) as min_f_vov,
       ROUND(MAX(f_vov), 3) as max_f_vov
FROM vov
GROUP BY asset;
```

### Joining Markets with Derivatives

```sql
-- For each BTC "above" market, get the ATM IV at the nearest snapshot hour
-- before settlement
SELECT m.question, m.threshold, m.outcome, m.settlement_date,
       o.mark_iv as atm_iv, o.underlying_price as spot
FROM markets m
JOIN options_snapshots o ON o.asset = m.asset
    AND o.snapshot_hour = (
        SELECT MAX(snapshot_hour) FROM options_snapshots
        WHERE asset = m.asset
          AND snapshot_hour <= CAST(strftime('%s', m.settlement_date) AS INTEGER) * 1000
    )
WHERE m.asset = 'BTC' AND m.direction = 'above'
  AND o.option_type = 'C'
  AND ABS(o.strike - o.underlying_price) = (
      SELECT MIN(ABS(strike - underlying_price))
      FROM options_snapshots
      WHERE asset = m.asset AND snapshot_hour = o.snapshot_hour AND option_type = 'C'
  )
LIMIT 20;
```

---

## Connection Examples

### Python

```python
import sqlite3

conn = sqlite3.connect("sample/backtest_sample.db")
conn.row_factory = sqlite3.Row

# Query options surface
rows = conn.execute("""
    SELECT strike, mark_iv, option_type
    FROM options_snapshots
    WHERE asset = 'BTC' AND snapshot_hour = 1743984000000
    ORDER BY strike
""").fetchall()

for r in rows:
    print(f"{r['option_type']} K={r['strike']:,.0f}  IV={r['mark_iv']:.4f}")

conn.close()
```

### pandas

```python
import pandas as pd
import sqlite3

conn = sqlite3.connect("sample/backtest_sample.db")

# Load OHLCV as DataFrame
df = pd.read_sql_query("""
    SELECT asset, timestamp, open, high, low, close, volume
    FROM ohlcv ORDER BY asset, timestamp
""", conn)
df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

# Load market prices
prices = pd.read_sql_query("""
    SELECT mp.condition_id, mp.timestamp, mp.yes_price, mp.no_price,
           mp.volume, mp.source, m.asset, m.direction
    FROM market_prices mp
    JOIN markets m ON mp.condition_id = m.condition_id
""", conn)
prices["datetime"] = pd.to_datetime(prices["timestamp"], unit="s", utc=True)

conn.close()
```

---

## Charts

The build script generates 14 diagnostic charts in `sample/`:

| File | Description |
|------|-------------|
| `markets_by_asset.png` | Bar chart of market counts per asset |
| `markets_by_direction.png` | Horizontal bar of market counts by direction type |
| `outcome_distribution.png` | Stacked YES/NO outcome counts per asset |
| `price_coverage.png` | Markets with price data over time |
| `options_instruments_per_hour.png` | Instruments per hourly snapshot by asset |
| `iv_distribution.png` | Histogram of all mark_iv values with mean/median |
| `iv_smile_example.png` | IV vs strike for one well-populated BTC snapshot |
| `asset_prices.png` | 2x2 grid of asset price histories from OHLCV |
| `funding_rates.png` | Funding rate time series for all 4 assets |
| `market_price_examples.png` | 6 example market price trajectories (YES and NO lines) |
| `data_summary.png` | Table image of row counts and date ranges |
| `dvol_comparison_btc.png` | Official vs computed DVOL for BTC (r=0.9154) |
| `dvol_comparison_eth.png` | Official vs computed DVOL for ETH (r=0.9539) |
| `vov_timeseries.png` | VoV and f_vov scaling factor over time |
