"""SQLite database layer — schema, batch inserts, resume helpers."""

import aiosqlite
from config import DB_PATH

DDL = """
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
    outcome INTEGER,
    UNIQUE(timestamp, condition_id)
);

-- Polymarket price history
CREATE TABLE IF NOT EXISTS polymarket_price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    yes_price REAL NOT NULL,
    UNIQUE(condition_id, timestamp)
);

-- Deribit option trades (for IV reconstruction)
CREATE TABLE IF NOT EXISTS deribit_option_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    instrument_name TEXT NOT NULL,
    asset TEXT NOT NULL,
    strike REAL NOT NULL,
    expiry TEXT NOT NULL,
    option_type TEXT NOT NULL,
    iv REAL NOT NULL,
    mark_price REAL NOT NULL,
    index_price REAL NOT NULL,
    trade_price REAL NOT NULL,
    amount REAL,
    UNIQUE(timestamp, instrument_name, trade_price)
);

-- Deribit futures (for forward price interpolation)
CREATE TABLE IF NOT EXISTS deribit_futures_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    asset TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    expiry_date INTEGER,
    mark_price REAL,
    delivery_price REAL,
    index_price REAL,
    UNIQUE(asset, instrument_name, timestamp)
);

-- Deribit funding rates (for d2 drift)
CREATE TABLE IF NOT EXISTS deribit_funding_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    asset TEXT NOT NULL,
    funding_8h REAL NOT NULL,
    UNIQUE(asset, timestamp)
);

-- Deribit OHLCV candles (for realized volatility)
CREATE TABLE IF NOT EXISTS deribit_ohlcv (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    asset TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL,
    resolution TEXT DEFAULT '1h',
    UNIQUE(asset, timestamp, resolution)
);

-- Precomputed options snapshots
CREATE TABLE IF NOT EXISTS options_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    asset TEXT NOT NULL,
    spot_price REAL NOT NULL,
    options_json TEXT NOT NULL,
    UNIQUE(timestamp, asset)
);

-- Backtest predictions (output)
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
"""

INDEXES = """
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
"""


class Database:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._db: aiosqlite.Connection | None = None

    async def connect(self):
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._init_schema()

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _init_schema(self):
        await self._db.executescript(DDL)
        await self._db.executescript(INDEXES)
        await self._db.commit()

    # ---- Batch inserts ----

    async def insert_markets(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO polymarket_markets
               (timestamp, condition_id, question, asset, threshold, direction,
                upper_threshold, settlement_date, yes_price, no_price,
                yes_token_id, no_token_id, volume, outcome)
               VALUES (:timestamp, :condition_id, :question, :asset,
                       :threshold, :direction, :upper_threshold,
                       :settlement_date, :yes_price, :no_price,
                       :yes_token_id, :no_token_id, :volume, :outcome)""",
            rows,
        )
        await self._db.commit()

    async def insert_price_history(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO polymarket_price_history
               (condition_id, timestamp, yes_price)
               VALUES (:condition_id, :timestamp, :yes_price)""",
            rows,
        )
        await self._db.commit()

    async def insert_option_trades(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO deribit_option_trades
               (timestamp, instrument_name, asset, strike, expiry,
                option_type, iv, mark_price, index_price, trade_price, amount)
               VALUES (:timestamp, :instrument_name, :asset, :strike,
                       :expiry, :option_type, :iv, :mark_price,
                       :index_price, :trade_price, :amount)""",
            rows,
        )
        await self._db.commit()

    async def insert_futures(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO deribit_futures_history
               (timestamp, asset, instrument_name, expiry_date,
                mark_price, delivery_price, index_price)
               VALUES (:timestamp, :asset, :instrument_name, :expiry_date,
                       :mark_price, :delivery_price, :index_price)""",
            rows,
        )
        await self._db.commit()

    async def insert_funding(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO deribit_funding_history
               (timestamp, asset, funding_8h)
               VALUES (:timestamp, :asset, :funding_8h)""",
            rows,
        )
        await self._db.commit()

    async def insert_ohlcv(self, rows: list[dict]):
        if not rows:
            return
        await self._db.executemany(
            """INSERT OR IGNORE INTO deribit_ohlcv
               (timestamp, asset, open, high, low, close, volume, resolution)
               VALUES (:timestamp, :asset, :open, :high, :low, :close,
                       :volume, :resolution)""",
            rows,
        )
        await self._db.commit()

    # ---- Resume helpers ----

    async def get_latest_option_trade_timestamp(self, asset: str) -> int | None:
        cur = await self._db.execute(
            "SELECT MAX(timestamp) FROM deribit_option_trades WHERE asset=?",
            (asset,),
        )
        row = await cur.fetchone()
        return row[0] if row and row[0] else None

    async def get_latest_futures_timestamp(self, asset: str) -> int | None:
        cur = await self._db.execute(
            "SELECT MAX(timestamp) FROM deribit_futures_history WHERE asset=?",
            (asset,),
        )
        row = await cur.fetchone()
        return row[0] if row and row[0] else None

    async def get_latest_funding_timestamp(self, asset: str) -> int | None:
        cur = await self._db.execute(
            "SELECT MAX(timestamp) FROM deribit_funding_history WHERE asset=?",
            (asset,),
        )
        row = await cur.fetchone()
        return row[0] if row and row[0] else None

    async def get_latest_ohlcv_timestamp(self, asset: str) -> int | None:
        cur = await self._db.execute(
            "SELECT MAX(timestamp) FROM deribit_ohlcv WHERE asset=?",
            (asset,),
        )
        row = await cur.fetchone()
        return row[0] if row and row[0] else None

    # ---- Coverage queries ----

    async def get_markets_missing_prices(self, asset: str) -> list[dict]:
        cur = await self._db.execute(
            """SELECT condition_id, asset, threshold, direction,
                      settlement_date, yes_token_id, no_token_id
               FROM polymarket_markets
               WHERE asset = ?
                 AND condition_id NOT IN (
                     SELECT DISTINCT condition_id FROM polymarket_price_history
                 )
                 AND yes_token_id IS NOT NULL AND yes_token_id != ''
                 AND settlement_date IS NOT NULL
               ORDER BY settlement_date DESC""",
            (asset,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_all_markets(self, asset: str) -> list[dict]:
        cur = await self._db.execute(
            """SELECT condition_id, asset, threshold, direction,
                      settlement_date, yes_token_id, no_token_id, outcome
               FROM polymarket_markets
               WHERE asset = ?
               ORDER BY settlement_date DESC""",
            (asset,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_market_count(self, asset: str | None = None) -> int:
        if asset:
            cur = await self._db.execute(
                "SELECT COUNT(*) FROM polymarket_markets WHERE asset=?", (asset,)
            )
        else:
            cur = await self._db.execute("SELECT COUNT(*) FROM polymarket_markets")
        row = await cur.fetchone()
        return row[0]

    async def get_table_counts(self) -> dict[str, int]:
        tables = [
            "polymarket_markets", "polymarket_price_history",
            "deribit_option_trades", "deribit_futures_history",
            "deribit_funding_history", "deribit_ohlcv",
            "options_snapshots", "backtest_predictions",
        ]
        counts = {}
        for t in tables:
            cur = await self._db.execute(f"SELECT COUNT(*) FROM {t}")
            row = await cur.fetchone()
            counts[t] = row[0]
        return counts

    async def get_price_coverage(self, asset: str) -> dict:
        cur = await self._db.execute(
            """SELECT
                   COUNT(DISTINCT pm.condition_id) as total_markets,
                   COUNT(DISTINCT ph.condition_id) as markets_with_prices
               FROM polymarket_markets pm
               LEFT JOIN (SELECT DISTINCT condition_id FROM polymarket_price_history) ph
                   ON pm.condition_id = ph.condition_id
               WHERE pm.asset = ?""",
            (asset,),
        )
        row = await cur.fetchone()
        total = row[0] or 0
        with_prices = row[1] or 0
        return {
            "total": total,
            "with_prices": with_prices,
            "coverage_pct": round(100 * with_prices / total, 1) if total else 0,
        }

    async def get_deribit_date_range(self, table: str, asset: str) -> dict | None:
        cur = await self._db.execute(
            f"""SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts,
                       COUNT(*) as cnt
                FROM {table} WHERE asset=?""",
            (asset,),
        )
        row = await cur.fetchone()
        if not row or not row[0]:
            return None
        return {"min_ts": row[0], "max_ts": row[1], "count": row[2]}
