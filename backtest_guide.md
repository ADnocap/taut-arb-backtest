# Backtest Dataset Building Guide

Comprehensive reference for collecting and storing all data needed to run the Taut-Arb backtester. Covers Polymarket market discovery, historical price data, and Deribit options/futures/funding/OHLCV collection across all four supported assets (BTC, ETH, SOL, XRP).

---

## 1. Data Requirements Matrix

| Data Type                        | Source API                         | Granularity                   | Auth Required | Retention                    | Notes                                  |
| -------------------------------- | ---------------------------------- | ----------------------------- | ------------- | ---------------------------- | -------------------------------------- |
| **Polymarket Markets**           | CLOB `clob.polymarket.com/markets` | Per-market                    | No            | Indefinite                   | Cursor-based pagination                |
| **Polymarket Prices**            | CLOB `/prices-history`             | Configurable (30-min default) | No            | Unreliable for older markets | Returns empty for many settled markets |
| **Polymarket Prices (backfill)** | Goldsky subgraph (GraphQL)         | Per-trade → 30-min buckets    | No            | April 2025+ only             | `orderFilledEvents`                    |
| **Deribit Options Trades**       | `history.deribit.com`              | Per-trade (with IV)           | No            | Full history                 | Inverse (BTC/ETH) + Linear (SOL/XRP)   |
| **Deribit Futures Trades**       | `history.deribit.com`              | Per-trade                     | No            | Full history                 | For forward price interpolation        |
| **Deribit Funding Rates**        | `www.deribit.com` (main API)       | 8-hour periods                | No            | Full history                 | NOT on history API                     |
| **Deribit OHLCV**                | `history.deribit.com`              | 1-hour candles                | No            | Full history                 | For Rogers-Satchell RV                 |

---

## 2. Polymarket Market Discovery & Classification

### 2.1 CLOB API (Primary — Closed Markets)

```
GET https://clob.polymarket.com/markets
```

**Pagination**: Cursor-based. Send `next_cursor` from previous response.

```python
params = {"limit": 100}
if cursor:
    params["next_cursor"] = cursor

# Response format:
# {"data": [...], "next_cursor": "abc123"}
```

All markets are returned (active + closed). Filter with `market_data.get("closed") == True`.

### 2.2 Gamma API (Active Markets)

```
GET https://gamma-api.polymarket.com/markets
```

**Pagination**: Offset-based.

```python
params = {"active": "true", "closed": "false", "limit": 100, "offset": 0}
```

### 2.3 Market Type Classification

The production system (`polymarket_client.py`) classifies markets into two categories:

#### European Digital (above/below/between)

Requires `"price"` keyword in question text. Patterns from production:

**ABOVE patterns** (production lines 534-548):

```python
above_patterns = [
    r'(?:be\s+)?(?:at\s+or\s+)?above\s*\$?([\d,]+(?:\.\d+)?)',
    r'(?:over|hit|reach|exceed|surpass)\s*\$?([\d,]+(?:\.\d+)?)',
    r'(?:greater|more|higher)\s+than\s*\$?([\d,]+(?:\.\d+)?)',
    r'\$?([\d,]+(?:\.\d+)?)\s*(?:or\s+)?(?:higher|more|above)',
    r'at\s+least\s*\$?([\d,]+(?:\.\d+)?)',
    r'(?:price|trading|trade)\s+above\s*\$?([\d,]+(?:\.\d+)?)',
]
```

**BELOW patterns** (production lines 551-565):

```python
below_patterns = [
    r'(?:be\s+)?(?:at\s+or\s+)?below\s*\$?([\d,]+(?:\.\d+)?)',
    r'(?:under|drop\s+to|fall\s+to|fall\s+below|drop\s+below)\s*\$?([\d,]+(?:\.\d+)?)',
    r'(?:less|lower)\s+than\s*\$?([\d,]+(?:\.\d+)?)',
    r'\$?([\d,]+(?:\.\d+)?)\s*(?:or\s+)?(?:lower|less|below)',
    r'at\s+most\s*\$?([\d,]+(?:\.\d+)?)',
    r'(?:price|trading|trade)\s+below\s*\$?([\d,]+(?:\.\d+)?)',
]
```

**BETWEEN patterns** (production lines 568-581):

```python
between_patterns = [
    r'between\s*\$?([\d,]+(?:\.\d+)?)\s*(?:and|to|-)\s*\$?([\d,]+(?:\.\d+)?)',
    r'be\s+between\s*\$?([\d,]+(?:\.\d+)?)\s*(?:and|to|-)\s*\$?([\d,]+(?:\.\d+)?)',
    r'\$?([\d,]+(?:\.\d+)?)\s*[-–]\s*\$?([\d,]+(?:\.\d+)?)',
    r'(?:in\s+the\s+)?range\s+(?:of\s+)?\$?([\d,]+(?:\.\d+)?)\s*(?:to|-)\s*\$?([\d,]+(?:\.\d+)?)',
]
```

#### Barrier One-Touch (reach/dip)

Only triggers when `"price"` is **absent** from the question text (lines 490-522). This avoids misclassifying European digitals that use overlapping keywords like "reach".

```python
# Up-barrier patterns
reach_patterns = ["reach", "hit $", "touch $"]

# Down-barrier patterns
dip_patterns = ["dip to", "drop to", "fall to", "dip below"]
```

### 2.4 Asset Detection

Word-boundary regex matching from `config/constants.py`:

```python
ASSET_KEYWORDS = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth", "ether"],
    "SOL": ["solana", "sol"],
    "XRP": ["ripple", "xrp"],
}
```

Pre-filter: question must contain `"price"` OR a barrier keyword (`reach`, `dip to`, `drop to`, `fall to`, `hit $`).

**Exclusion topics** — skip if question contains any of:

```python
ASSET_DETECT_EXCLUDE_TOPICS = [
    "volatility index", "gas price", "floor price", "dominance",
    "market cap", "nft", "token price", "total supply",
    "trading volume", "hash rate", "hashrate",
]
```

### 2.5 Threshold Extraction

Number parsing with comma/K/M suffix handling:

```python
def _parse_number(s: str) -> Optional[float]:
    s = s.replace(",", "").strip()
    multiplier = 1
    if s.endswith("k") or s.endswith("K"):
        multiplier = 1000; s = s[:-1]
    elif s.endswith("m") or s.endswith("M"):
        multiplier = 1000000; s = s[:-1]
    return float(s) * multiplier
```

**Plausible range validation per asset** (from `constants.py`):

| Asset | Min USD | Max USD |
| ----- | ------- | ------- |
| BTC   | 1,000   | 500,000 |
| ETH   | 100     | 50,000  |
| SOL   | 1       | 5,000   |
| XRP   | 0.1     | 100     |

The backtest collector uses a simpler heuristic: reject thresholds < 100 USD for above/below/between markets.

### 2.6 Settlement Date Parsing

**Field priority list** (production `_parse_settlement_date`, line 602):

```python
date_fields = [
    "endDate", "end_date_iso", "endDateIso",
    "resolutionDate", "resolution_date",
    "closeTime", "close_time"
]
```

Each field is parsed as:

- Numeric (unix timestamp): divide by 1000 if > 1e12 (ms → s)
- String numeric: same conversion
- ISO string: `datetime.fromisoformat(value.replace("Z", "+00:00"))`

**Fallback**: parse date from question text (patterns like "on Mar 29", "on April 5, 2024").

### 2.7 Outcome Determination

**CLOB API**: `winner` field on token objects. `token.get("winner") == True` on the Yes token means `outcome = True`.

```python
for token in tokens:
    if token.get("outcome") == "Yes" and token.get("winner") is True:
        outcome = True
    elif token.get("outcome") == "No" and token.get("winner") is True:
        outcome = False
```

**Gamma API**: `resolvedTo` field — `"Yes"` or `"No"` string.

---

## 3. Polymarket Historical Price Data

### 3.1 Primary: CLOB `/prices-history`

```
GET https://clob.polymarket.com/prices-history
```

| Param      | Type   | Description                                          |
| ---------- | ------ | ---------------------------------------------------- |
| `market`   | string | **Token ID** (not condition_id) — the `yes_token_id` |
| `startTs`  | int    | Start timestamp (unix seconds)                       |
| `endTs`    | int    | End timestamp (unix seconds)                         |
| `fidelity` | int    | Resolution in minutes (default: 30)                  |

Response: `{"history": [{"t": 1706000000, "p": 0.45}, ...]}`

**Recommended config**: 30-min fidelity, 7-day window before settlement.

```python
end_time = settlement_date
start_time = settlement_date - timedelta(days=7)
start_ts = int(start_time.timestamp())
end_ts = int(end_time.timestamp())
```

### 3.2 Known Problems

- **Returns empty for many older settled markets.** The CLOB API does not reliably serve price history for markets that settled months ago. Coverage is unpredictable.
- **No error signal.** Empty responses (`{"history": []}`) are indistinguishable from genuinely zero-trade markets.
- Typical coverage: 50-70% of markets get some price data; the rest return empty.

### 3.3 Fallback: Goldsky Subgraph

```
POST https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn
```

No authentication required. Queries `orderFilledEvents` via GraphQL.

**Critical limitation**: Data only exists from **April 7, 2025** onwards (timestamp >= 1744013119). Markets settled before this date have no subgraph data.

**GraphQL query** (with token ID filtering):

```graphql
{
  orderFilledEvents(
    first: 1000
    orderBy: timestamp
    orderDirection: asc
    where: {
      or: [
        { timestamp_gt: "START_TS", makerAssetId_in: ["TOKEN_ID_1", ...] },
        { timestamp_gt: "START_TS", takerAssetId_in: ["TOKEN_ID_1", ...] }
      ]
    }
  ) {
    id
    timestamp
    makerAmountFilled
    takerAmountFilled
    makerAssetId
    takerAssetId
  }
}
```

**Price calculation** from fill events:

```python
USDC_ASSET_ID = "0"

# If maker sells tokens for USDC:
if maker_asset == token_id and taker_asset == USDC_ASSET_ID:
    price = (taker_amount / 1e6) / (maker_amount / 1e6)

# If taker buys tokens with USDC:
if taker_asset == token_id and maker_asset == USDC_ASSET_ID:
    price = (maker_amount / 1e6) / (taker_amount / 1e6)
```

Prices are bucketed into 30-minute intervals (`(ts // 1800) * 1800`), keeping the last price per interval.

**Pagination**: Use `id_gt` when all results in a page share the same timestamp (sticky mode), otherwise advance `timestamp_gt`.

### 3.4 Storage Format

```python
# In polymarket_price_history table:
{"condition_id": "...", "timestamp": unix_seconds, "yes_price": 0.45}
```

---

## 4. Deribit Options Chain Reconstruction

### 4.1 API Endpoint

```
GET https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time
```

Free, no authentication, every option trade ever made with IV at execution time.

### 4.2 Inverse vs Linear Contracts

| Asset   | Currency Param | Instrument Format        | Mark Price → USD           |
| ------- | -------------- | ------------------------ | -------------------------- |
| **BTC** | `"BTC"`        | `BTC-25SEP20-6000-C`     | `mark_price * index_price` |
| **ETH** | `"ETH"`        | `ETH-25SEP20-3000-P`     | `mark_price * index_price` |
| **SOL** | `"USDC"`       | `SOL_USDC-25SEP20-150-C` | `mark_price` (already USD) |
| **XRP** | `"USDC"`       | `XRP_USDC-25SEP20-1-P`   | `mark_price` (already USD) |

SOL and XRP options are fetched via `currency=USDC` (they share the linear USDC namespace on Deribit). The instrument name prefix distinguishes them: `SOL_USDC-*` vs `XRP_USDC-*`.

### 4.3 Request Parameters

```python
params = {
    "currency": "BTC",       # or "ETH" or "USDC" (for SOL + XRP)
    "kind": "option",
    "start_timestamp": ms,   # milliseconds
    "end_timestamp": ms,
    "count": 10000,           # max per page
    "sorting": "asc",
}
# For pagination:
if start_seq is not None:
    params["start_seq"] = start_seq
```

### 4.4 Pagination

- Each page returns up to 10,000 trades
- Check `result.has_more` for more pages
- Use `trades[-1].trade_seq + 1` as `start_seq` for the next page
- Safety limit: 20 pages per window (200K trades)

### 4.5 IV Normalization

Deribit returns IV as percentage (e.g., 74.74 = 74.74%). Normalize to decimal:

```python
iv = trade["iv"]
if iv > 5:
    iv = iv / 100  # 74.74 -> 0.7474
```

### 4.6 Snapshot Reconstruction

To reconstruct a point-in-time IV surface:

1. Collect all option trades in a window (default: 24 hours before target time)
2. Group by instrument name
3. Keep only the most recent trade per instrument
4. Skip trades with `iv <= 0` or `iv is None`
5. Convert mark_price: inverse = `mark_price * index_price`, linear = `mark_price`

```python
# Instrument name parsing: BTC-25SEP20-6000-P
parts = name.split("-")
# parts[0] = asset, parts[1] = expiry (DDMMMYY), parts[2] = strike, parts[3] = C/P
# Expiry always 08:00 UTC
```

### 4.7 Day-by-Day Download Strategy

For bulk collection, process one day at a time with incremental saves:

```python
# Process in batches of 10 concurrent days
await download_trades_for_date_range(
    asset="BTC",
    start_date=start,
    end_date=end,
    on_day_save=save_callback,  # saves immediately, frees memory
    batch_size=10,
)
```

---

## 5. Deribit Futures Data

### 5.1 API

Same history API as options, with `kind=future`:

```python
params = {
    "currency": "BTC",   # or "ETH" or "USDC" (SOL/XRP)
    "kind": "future",
    "start_timestamp": ms,
    "end_timestamp": ms,
    "count": 10000,
    "sorting": "asc",
}
```

### 5.2 Instrument Parsing

```
BTC-25SEP20      → asset=BTC, expiry=2020-09-25 08:00 UTC
SOL_USDC-31JAN25 → asset=SOL, expiry=2025-01-31 08:00 UTC
BTC-PERPETUAL     → skip (perpetual, not dated future)
```

**Filter out perpetuals.** Only dated futures are needed for forward price interpolation.

### 5.3 Purpose

Forward price interpolation: Deribit settles at 08:00 UTC, Polymarket at ~17:00 UTC. The system interpolates the forward price at the Polymarket settlement time using the two closest Deribit futures:

```
B_poly = B1 + (B2 - B1) * (T_poly - T1) / (T2 - T1)
F_poly = S + B_poly
```

where B = F - S (basis).

### 5.4 Stored Fields

```python
{
    "timestamp": ms,
    "asset": "BTC",
    "instrument_name": "BTC-25SEP20",
    "expiry_date": ms,         # expiry as unix ms
    "mark_price": 0.05,        # in BTC for inverse, USD for linear
    "delivery_price": 0.05,
    "index_price": 50000.0,    # spot USD
}
```

---

## 6. Deribit Funding Rate History

### 6.1 API (Main API — NOT History API)

```
GET https://www.deribit.com/api/v2/public/get_funding_rate_history
```

**This is NOT available on `history.deribit.com`.** Must use the main API at `www.deribit.com`.

### 6.2 Instrument Names

| Asset | Perpetual Instrument |
| ----- | -------------------- |
| BTC   | `BTC-PERPETUAL`      |
| ETH   | `ETH-PERPETUAL`      |
| SOL   | `SOL_USDC-PERPETUAL` |
| XRP   | `XRP_USDC-PERPETUAL` |

### 6.3 Request Parameters

```python
params = {
    "instrument_name": "BTC-PERPETUAL",
    "start_timestamp": ms,   # milliseconds
    "end_timestamp": ms,
}
```

### 6.4 Response

Returns `interest_8h` per 8-hour funding period. API returns ~744 records max per request.

**Chunk into 30-day requests** to handle longer date ranges:

```python
async def fetch_funding_rate_history_chunked(asset, start, end, chunk_days=30):
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        rates = await fetch_funding_rate_history(asset, current, chunk_end)
        all_rates.extend(rates)
        current = chunk_end
```

### 6.5 Usage in Backtest

The d2-space drift calculation uses funding rate:

```python
mu = -1.0 * (funding_8h - F_0) * ANNUALIZATION_FACTOR
# where F_0 = 0.0, ANNUALIZATION_FACTOR = 1095.75 (365.25 * 3 periods/day)
```

---

## 7. Deribit OHLCV Candles

### 7.1 API

```
GET https://history.deribit.com/api/v2/public/get_tradingview_chart_data
```

Also works on the main API (`www.deribit.com`).

### 7.2 Request Parameters

```python
params = {
    "instrument_name": "BTC-PERPETUAL",    # or "SOL_USDC-PERPETUAL"
    "start_timestamp": ms,
    "end_timestamp": ms,
    "resolution": "60",   # minutes — "60" = 1-hour candles
}
```

### 7.3 Response Format

Returns parallel arrays, not array-of-objects:

```python
result = {
    "ticks": [1706000000000, ...],
    "open": [42000.0, ...],
    "high": [42100.0, ...],
    "low": [41900.0, ...],
    "close": [42050.0, ...],
    "volume": [123.5, ...],
}
```

Reassemble into candle dicts:

```python
for i, ts in enumerate(ticks):
    candle = {
        "timestamp": ts,
        "asset": asset,
        "open": opens[i], "high": highs[i],
        "low": lows[i], "close": closes[i],
        "volume": volumes[i],
        "resolution": "1h",
    }
```

### 7.4 Required Lookback

- **Minimum**: 7 days (168 1-hour candles) for Rogers-Satchell realized volatility
- The backtest fetches 169 candles (7d + 1h buffer) and slices:
  - Last 25 candles → 24h RV
  - Full 169 candles → 7d RV

### 7.5 Chunking

For large date ranges, chunk into 30-day requests (same pattern as funding rates).

---

## 8. SQLite Schema

### 8.1 Complete DDL

```sql
-- Polymarket markets (metadata + outcomes)
CREATE TABLE IF NOT EXISTS polymarket_markets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    question TEXT,
    asset TEXT NOT NULL,
    threshold REAL NOT NULL,
    direction TEXT NOT NULL,
    upper_threshold REAL,
    settlement_date TEXT,
    yes_price REAL,
    no_price REAL,
    yes_token_id TEXT,
    no_token_id TEXT,
    volume REAL,
    outcome INTEGER,           -- 1=YES won, 0=NO won, NULL=unsettled
    UNIQUE(timestamp, condition_id)
);

-- Polymarket price history
CREATE TABLE IF NOT EXISTS polymarket_price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,   -- unix seconds
    yes_price REAL NOT NULL,
    UNIQUE(condition_id, timestamp)
);

-- Deribit option trades (for IV reconstruction)
CREATE TABLE IF NOT EXISTS deribit_option_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,        -- unix milliseconds
    instrument_name TEXT NOT NULL,
    asset TEXT NOT NULL,
    strike REAL NOT NULL,
    expiry TEXT NOT NULL,              -- ISO format
    option_type TEXT NOT NULL,          -- "C" or "P"
    iv REAL NOT NULL,                   -- decimal (0.7474, not 74.74%)
    mark_price REAL NOT NULL,           -- BTC units for inverse, USD for linear
    index_price REAL NOT NULL,          -- spot USD
    trade_price REAL NOT NULL,
    amount REAL,
    UNIQUE(timestamp, instrument_name, trade_price)
);

-- Deribit futures (for forward price interpolation)
CREATE TABLE IF NOT EXISTS deribit_futures_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,        -- unix milliseconds
    asset TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    expiry_date INTEGER,               -- unix milliseconds
    mark_price REAL,
    delivery_price REAL,
    index_price REAL,
    UNIQUE(asset, instrument_name, timestamp)
);

-- Deribit funding rates (for d2 drift)
CREATE TABLE IF NOT EXISTS deribit_funding_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,        -- unix milliseconds
    asset TEXT NOT NULL,
    funding_8h REAL NOT NULL,
    UNIQUE(asset, timestamp)
);

-- Deribit OHLCV candles (for realized volatility)
CREATE TABLE IF NOT EXISTS deribit_ohlcv (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,        -- unix milliseconds
    asset TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL,
    resolution TEXT DEFAULT '1h',
    UNIQUE(asset, timestamp, resolution)
);

-- Precomputed options snapshots (optional, for fast replay)
CREATE TABLE IF NOT EXISTS options_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    asset TEXT NOT NULL,
    spot_price REAL NOT NULL,
    options_json TEXT NOT NULL,
    UNIQUE(timestamp, asset)
);

-- Backtest predictions (output, not input)
CREATE TABLE IF NOT EXISTS backtest_predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    analysis_timestamp TEXT NOT NULL,
    asset TEXT NOT NULL,
    threshold REAL NOT NULL,
    direction TEXT NOT NULL,
    model_prob REAL NOT NULL,
    market_prob REAL NOT NULL,
    edge_percent REAL NOT NULL,
    recommended_side TEXT NOT NULL,
    confidence REAL,
    iv_used REAL,
    spot_at_analysis REAL,
    settlement_date TEXT,
    actual_outcome INTEGER,
    pnl REAL,
    UNIQUE(condition_id, analysis_timestamp)
);
```

### 8.2 Indexes

```sql
CREATE INDEX IF NOT EXISTS idx_options_timestamp ON options_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_options_asset ON options_snapshots(asset);
CREATE INDEX IF NOT EXISTS idx_polymarket_timestamp ON polymarket_markets(timestamp);
CREATE INDEX IF NOT EXISTS idx_polymarket_settlement ON polymarket_markets(settlement_date);
CREATE INDEX IF NOT EXISTS idx_deribit_trades_ts ON deribit_option_trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_deribit_trades_asset ON deribit_option_trades(asset, timestamp);
CREATE INDEX IF NOT EXISTS idx_price_history_cond ON polymarket_price_history(condition_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_futures_asset_ts ON deribit_futures_history(asset, timestamp);
CREATE INDEX IF NOT EXISTS idx_funding_asset_ts ON deribit_funding_history(asset, timestamp);
CREATE INDEX IF NOT EXISTS idx_ohlcv_asset_ts ON deribit_ohlcv(asset, timestamp);
CREATE INDEX IF NOT EXISTS idx_predictions_cond ON backtest_predictions(condition_id);
```

### 8.3 Gap Detection Queries

```sql
-- Find date range of existing option trades per asset
SELECT asset,
       MIN(timestamp) as earliest_ms,
       MAX(timestamp) as latest_ms,
       COUNT(*) as total_trades,
       datetime(MIN(timestamp)/1000, 'unixepoch') as earliest_date,
       datetime(MAX(timestamp)/1000, 'unixepoch') as latest_date
FROM deribit_option_trades
GROUP BY asset;

-- Find markets missing price history
SELECT condition_id, asset, threshold, direction, settlement_date
FROM polymarket_markets
WHERE condition_id NOT IN (
    SELECT DISTINCT condition_id FROM polymarket_price_history
)
AND yes_token_id IS NOT NULL AND yes_token_id != ''
AND settlement_date IS NOT NULL
ORDER BY settlement_date DESC;

-- Price history coverage rate
SELECT
    pm.asset,
    COUNT(DISTINCT pm.condition_id) as total_markets,
    COUNT(DISTINCT ph.condition_id) as markets_with_prices,
    ROUND(100.0 * COUNT(DISTINCT ph.condition_id) / COUNT(DISTINCT pm.condition_id), 1) as coverage_pct
FROM polymarket_markets pm
LEFT JOIN (SELECT DISTINCT condition_id FROM polymarket_price_history) ph
    ON pm.condition_id = ph.condition_id
GROUP BY pm.asset;

-- Daily trade count for gap detection (option trades)
SELECT asset,
       date(timestamp/1000, 'unixepoch') as trade_date,
       COUNT(*) as trades
FROM deribit_option_trades
GROUP BY asset, trade_date
ORDER BY asset, trade_date;

-- Funding rate gaps (should have 3 entries per day)
SELECT asset,
       date(timestamp/1000, 'unixepoch') as funding_date,
       COUNT(*) as periods
FROM deribit_funding_history
GROUP BY asset, funding_date
HAVING periods < 3
ORDER BY asset, funding_date;
```

---

## 9. Data Quality & Validation

### 9.1 Options Data Quality

| Check                   | Threshold                    | Action                                             |
| ----------------------- | ---------------------------- | -------------------------------------------------- |
| Min trades per snapshot | 50+                          | Reject snapshot if fewer (insufficient IV surface) |
| IV range                | 0 < IV < 5.0 (500%)          | Skip individual trades outside range               |
| IV normalization        | If IV > 5, divide by 100     | Auto-applied during collection                     |
| Stale trade detection   | Trades > 24h old in snapshot | Default window is 24h; tighten if needed           |
| Strike sanity           | Within 3x spot price         | Skip far OTM with no liquidity                     |

### 9.2 Price Data Quality

| Check           | Threshold                    | Action                                  |
| --------------- | ---------------------------- | --------------------------------------- |
| Price range     | 0 < price < 1.0              | Reject prices outside [0, 1]            |
| Min data points | 10+ per market               | `has_price_history(cid, min_points=10)` |
| Staleness       | No data for > 4 hours        | Flag for review                         |
| Continuity      | Price jumps > 0.30 in 30 min | Flag as suspicious                      |

### 9.3 Funding Rate Quality

- Should have exactly 3 entries per day (8-hour periods)
- Values typically in range [-0.001, 0.001] (but can spike during extreme conditions)
- Missing periods: interpolate from neighbors or use last known value

### 9.4 OHLCV Quality

- Should have 24 candles per day (1-hour resolution)
- `high >= max(open, close)` and `low <= min(open, close)` — reject otherwise
- Volume > 0 for all candles (perpetuals are always liquid)

### 9.5 Coverage Statistics

```sql
-- Overall data inventory
SELECT 'option_trades' as table_name, COUNT(*) as rows FROM deribit_option_trades
UNION ALL SELECT 'futures', COUNT(*) FROM deribit_futures_history
UNION ALL SELECT 'funding', COUNT(*) FROM deribit_funding_history
UNION ALL SELECT 'ohlcv', COUNT(*) FROM deribit_ohlcv
UNION ALL SELECT 'poly_markets', COUNT(*) FROM polymarket_markets
UNION ALL SELECT 'poly_prices', COUNT(*) FROM polymarket_price_history
UNION ALL SELECT 'predictions', COUNT(*) FROM backtest_predictions;
```

### 9.6 Known Issues by Data Source

| Source                 | Issue                                       | Impact                                            |
| ---------------------- | ------------------------------------------- | ------------------------------------------------- |
| CLOB `/prices-history` | Returns empty for many settled markets      | 30-50% of markets may have no price data          |
| Goldsky subgraph       | Only April 2025+                            | Markets before April 2025 have no backfill source |
| Deribit history API    | SOL/XRP options are newer (limited history) | Fewer trades for SOL/XRP before 2024              |
| Deribit funding API    | ~744 record limit per request               | Must chunk into 30-day windows                    |
| OHLCV API              | Timestamps in milliseconds                  | Be careful mixing with Polymarket (seconds)       |

---

## 10. Collection Execution Order

### Step 1: Polymarket Markets

Collect closed markets with outcomes. This is the foundation — everything else is keyed off these markets.

```bash
# Using the collector CLI
python -m crypto_v3.backtest.data_collectors.polymarket_collector --collect --days 365
```

Or programmatically:

```python
store = BacktestDataStore("backtest_data.db")
collector = PolymarketHistoricalCollector(store)
markets = await collector.collect_markets_with_price_history(
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime.now(timezone.utc),
    assets=["BTC", "ETH", "SOL", "XRP"],
)
```

**Expected volume**: ~2,000-5,000 crypto markets (depends on date range).

**Rate limit**: CLOB API has no documented rate limit but use 100/page with brief pauses.

### Step 2: Polymarket Price Histories

Fetch from CLOB API first, then backfill gaps with Goldsky subgraph.

```python
# CLOB prices (already included in collect_markets_with_price_history above)
# Then backfill remaining gaps:
from crypto_v3.backtest.data_collectors.subgraph_price_backfill import SubgraphPriceBackfill
backfill = SubgraphPriceBackfill(store)
await backfill.backfill_all_missing(assets=["BTC", "ETH", "SOL", "XRP"])
```

**Expected volume**: ~50K-200K price points across all markets.

**Rate limit**: Goldsky has soft rate limits; the collector uses 20 concurrent requests with exponential backoff on 429s.

### Step 3: Deribit Options Trades

Fetch per asset, day-by-day with gap detection. The collector automatically skips date ranges already in the database.

```python
from crypto_v3.backtest.data_collectors.deribit_history_collector import DeribitHistoryCollector
collector = DeribitHistoryCollector(max_concurrent=10)

for currency in ["BTC", "ETH", "USDC"]:  # USDC covers SOL + XRP
    await collector.download_all_historical_data(
        asset=currency,
        start_date=start,
        end_date=end,
        store=store,
        include_options=True,
        include_futures=False,
        include_funding=False,
        include_ohlcv=False,
    )
```

**Note on USDC currency**: When `currency="USDC"`, the API returns both SOL and XRP options. Parse the instrument name prefix to distinguish them: `SOL_USDC-*` vs `XRP_USDC-*`.

**Expected volume**: ~500K-2M trades per asset per year for BTC; less for altcoins.

**Rate limit**: History API is generous (no auth required), but use 10 concurrent connections max.

### Step 4: Deribit Futures Trades

Same API with `kind=future`. Smaller volume than options.

```python
await collector.download_all_historical_data(
    asset="BTC",
    start_date=start,
    end_date=end,
    store=store,
    include_options=False,
    include_futures=True,
    include_funding=False,
    include_ohlcv=False,
)
```

**Expected volume**: ~10K-50K trades per asset per year.

### Step 5: Deribit Funding Rates

Uses main API (`www.deribit.com`), chunked into 30-day windows.

```python
await collector.download_all_historical_data(
    asset="BTC",
    start_date=start,
    end_date=end,
    store=store,
    include_options=False,
    include_futures=False,
    include_funding=True,
    include_ohlcv=False,
)
```

**Remember**: For SOL/XRP, the instrument is `SOL_USDC-PERPETUAL` / `XRP_USDC-PERPETUAL`. The collector currently uses `{asset}-PERPETUAL` which works for BTC/ETH but needs the `_USDC` suffix for SOL/XRP.

**Expected volume**: ~1,095 records per asset per year (3 per day).

### Step 6: Deribit OHLCV Candles

1-hour candles from perpetual instruments.

```python
await collector.download_all_historical_data(
    asset="BTC",
    start_date=start,
    end_date=end,
    store=store,
    include_options=False,
    include_futures=False,
    include_funding=False,
    include_ohlcv=True,
)
```

**Expected volume**: ~8,760 candles per asset per year (24 per day).
