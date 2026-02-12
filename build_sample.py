"""Build sample/backtest_sample.db from raw backtest_data.db.

Synchronous script — pure SQLite I/O, no async needed.
Generates ~11 PNG charts in sample/ after building the dataset.
"""

import argparse
import os
import sqlite3
import time
from pathlib import Path

from rich.console import Console
from rich.table import Table

from config import ASSETS, DB_PATH

console = Console()

SAMPLE_DIR = Path("sample")
SAMPLE_DB = SAMPLE_DIR / "backtest_sample.db"
BATCH_SIZE = 10_000

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SAMPLE_DDL = """\
CREATE TABLE IF NOT EXISTS markets (
    condition_id TEXT PRIMARY KEY,
    asset TEXT NOT NULL,
    threshold REAL NOT NULL,
    upper_threshold REAL,
    direction TEXT NOT NULL,
    settlement_date TEXT NOT NULL,
    outcome INTEGER NOT NULL,
    yes_token_id TEXT,
    no_token_id TEXT,
    question TEXT
);

CREATE TABLE IF NOT EXISTS market_prices (
    condition_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    yes_price REAL NOT NULL,
    no_price REAL NOT NULL,
    UNIQUE(condition_id, timestamp),
    FOREIGN KEY (condition_id) REFERENCES markets(condition_id)
);
CREATE INDEX IF NOT EXISTS idx_mp ON market_prices(condition_id, timestamp);

CREATE TABLE IF NOT EXISTS options_snapshots (
    snapshot_hour INTEGER NOT NULL,
    asset TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    strike REAL NOT NULL,
    expiry_date TEXT NOT NULL,
    expiry_str TEXT NOT NULL,
    option_type TEXT NOT NULL,
    mark_iv REAL NOT NULL,
    bid REAL,
    ask REAL,
    mark_price REAL,
    underlying_price REAL,
    UNIQUE(snapshot_hour, asset, instrument_name)
);
CREATE INDEX IF NOT EXISTS idx_opts ON options_snapshots(asset, snapshot_hour);

CREATE TABLE IF NOT EXISTS futures_snapshots (
    snapshot_hour INTEGER NOT NULL,
    asset TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    expiry_date TEXT,
    expiry_str TEXT,
    mark_price REAL NOT NULL,
    delivery_price REAL,
    underlying_price REAL,
    UNIQUE(snapshot_hour, asset, instrument_name)
);
CREATE INDEX IF NOT EXISTS idx_fut ON futures_snapshots(asset, snapshot_hour);

CREATE TABLE IF NOT EXISTS funding_rates (
    asset TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    interest_8h REAL NOT NULL,
    UNIQUE(asset, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_fund ON funding_rates(asset, timestamp);

CREATE TABLE IF NOT EXISTS ohlcv (
    asset TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL,
    UNIQUE(asset, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_ohlcv ON ohlcv(asset, timestamp);
"""

DIRECTION_MAP = {
    "up_barrier": "reach",
    "down_barrier": "dip",
}

HOUR_MS = 3_600_000
DAY_MS = 86_400_000

# Minimum instruments in 24h window to emit an options snapshot hour.
# XRP options are thinly traded (max ~28 instruments in any 24h window).
OPTIONS_MIN_INSTRUMENTS = {"BTC": 50, "ETH": 50, "SOL": 50, "XRP": 5}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def floor_hour(ts_ms: int) -> int:
    """Round a millisecond timestamp down to the hour."""
    return (ts_ms // HOUR_MS) * HOUR_MS


def expiry_iso_to_str(iso: str) -> str:
    """'2025-09-25T08:00:00+00:00' → '250925'."""
    # Take YYYY-MM-DD portion
    date_part = iso[:10]  # '2025-09-25'
    y, m, d = date_part.split("-")
    return f"{y[2:]}{m}{d}"


def ms_to_iso(ts_ms: int) -> str:
    """Unix ms → ISO 8601 string (UTC, 08:00 for Deribit expiry convention)."""
    import datetime as _dt
    dt = _dt.datetime.fromtimestamp(ts_ms / 1000, tz=_dt.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def create_sample_db() -> sqlite3.Connection:
    """Create (or recreate) the sample database with empty tables."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    if SAMPLE_DB.exists():
        SAMPLE_DB.unlink()
    conn = sqlite3.connect(str(SAMPLE_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SAMPLE_DDL)
    return conn


def open_source_db() -> sqlite3.Connection:
    """Open the raw database read-only."""
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Step 1: Markets
# ---------------------------------------------------------------------------

def build_markets(src: sqlite3.Connection, dst: sqlite3.Connection) -> int:
    console.print("\n[bold cyan]Step 1:[/] Building markets table...")
    t0 = time.perf_counter()

    rows = src.execute("""
        SELECT condition_id, asset, threshold, direction, upper_threshold,
               settlement_date, yes_token_id, no_token_id, outcome, question
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY condition_id ORDER BY id DESC) as rn
            FROM polymarket_markets
            WHERE outcome IS NOT NULL AND settlement_date IS NOT NULL
        )
        WHERE rn = 1
    """).fetchall()

    batch = []
    for r in rows:
        direction = DIRECTION_MAP.get(r["direction"], r["direction"])
        batch.append((
            r["condition_id"], r["asset"], r["threshold"],
            r["upper_threshold"], direction, r["settlement_date"],
            r["outcome"], r["yes_token_id"], r["no_token_id"],
            r["question"],
        ))

    dst.executemany(
        "INSERT OR IGNORE INTO markets VALUES (?,?,?,?,?,?,?,?,?,?)",
        batch,
    )
    dst.commit()
    elapsed = time.perf_counter() - t0
    console.print(f"  Inserted [green]{len(batch)}[/] markets in {elapsed:.1f}s")
    return len(batch)


# ---------------------------------------------------------------------------
# Step 2: Market prices
# ---------------------------------------------------------------------------

def build_prices(src: sqlite3.Connection, dst: sqlite3.Connection) -> int:
    console.print("\n[bold cyan]Step 2:[/] Building market_prices table...")
    t0 = time.perf_counter()

    # Get condition_ids from destination markets table
    cids = {row[0] for row in dst.execute("SELECT condition_id FROM markets")}

    total = 0
    batch = []

    # Stream from source
    cursor = src.execute(
        "SELECT condition_id, timestamp, yes_price FROM polymarket_price_history ORDER BY condition_id"
    )
    for row in cursor:
        cid = row["condition_id"]
        if cid not in cids:
            continue
        yes_p = row["yes_price"]
        no_p = round(1.0 - yes_p, 4)
        batch.append((cid, row["timestamp"], yes_p, no_p))
        if len(batch) >= BATCH_SIZE:
            dst.executemany(
                "INSERT OR IGNORE INTO market_prices VALUES (?,?,?,?)",
                batch,
            )
            total += len(batch)
            batch.clear()

    if batch:
        dst.executemany(
            "INSERT OR IGNORE INTO market_prices VALUES (?,?,?,?)",
            batch,
        )
        total += len(batch)

    dst.commit()
    elapsed = time.perf_counter() - t0
    console.print(f"  Inserted [green]{total:,}[/] price rows in {elapsed:.1f}s")
    return total


# ---------------------------------------------------------------------------
# Step 3: Options snapshots (sliding window)
# ---------------------------------------------------------------------------

def build_options(src: sqlite3.Connection, dst: sqlite3.Connection) -> int:
    console.print("\n[bold cyan]Step 3:[/] Building options_snapshots table (sliding window)...")
    t0 = time.perf_counter()
    grand_total = 0

    for asset_name, cfg in ASSETS.items():
        console.print(f"  Processing [yellow]{asset_name}[/] options...")
        at0 = time.perf_counter()

        cursor = src.execute(
            """SELECT timestamp, instrument_name, strike, expiry, option_type,
                      iv, mark_price, index_price
               FROM deribit_option_trades
               WHERE asset = ?
               ORDER BY timestamp""",
            (asset_name,),
        )

        min_instruments = OPTIONS_MIN_INSTRUMENTS[asset_name]
        window = {}       # instrument_name → dict of trade fields
        win_times = {}    # instrument_name → timestamp
        current_hour = None
        batch = []
        asset_total = 0
        hours_emitted = 0

        for trade in cursor:
            ts = trade["timestamp"]

            if current_hour is None:
                current_hour = floor_hour(ts)

            # Emit snapshots for hours before this trade
            trade_hour = floor_hour(ts)
            while current_hour < trade_hour:
                if len(window) >= min_instruments:
                    for inst, t in window.items():
                        mark_iv = t["iv"]
                        # mark_price USD conversion for inverse
                        mp = t["mark_price"]
                        if cfg.is_inverse and t["index_price"]:
                            mp = mp * t["index_price"]

                        expiry_str = expiry_iso_to_str(t["expiry"])
                        batch.append((
                            current_hour, asset_name, inst,
                            t["strike"], t["expiry"], expiry_str,
                            t["option_type"], mark_iv,
                            None, None,  # bid, ask
                            mp, t["index_price"],
                        ))

                    if len(batch) >= BATCH_SIZE:
                        dst.executemany(
                            "INSERT OR IGNORE INTO options_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                            batch,
                        )
                        asset_total += len(batch)
                        batch.clear()

                    hours_emitted += 1
                    if hours_emitted % 1000 == 0:
                        console.print(f"    {asset_name}: {hours_emitted:,} hours emitted, {asset_total:,} rows so far")

                current_hour += HOUR_MS
                # Evict stale entries (>24h old)
                evict_before = current_hour - DAY_MS
                stale = [k for k, v in win_times.items() if v < evict_before]
                for k in stale:
                    del window[k]
                    del win_times[k]

            # Update window with this trade
            inst_name = trade["instrument_name"]
            window[inst_name] = {
                "strike": trade["strike"],
                "expiry": trade["expiry"],
                "option_type": trade["option_type"],
                "iv": trade["iv"],
                "mark_price": trade["mark_price"],
                "index_price": trade["index_price"],
            }
            win_times[inst_name] = ts

        # Emit remaining hours after last trade
        if current_hour is not None:
            # One final emission for current_hour
            if len(window) >= min_instruments:
                for inst, t in window.items():
                    mp = t["mark_price"]
                    if cfg.is_inverse and t["index_price"]:
                        mp = mp * t["index_price"]
                    expiry_str = expiry_iso_to_str(t["expiry"])
                    batch.append((
                        current_hour, asset_name, inst,
                        t["strike"], t["expiry"], expiry_str,
                        t["option_type"], t["iv"],
                        None, None, mp, t["index_price"],
                    ))

        if batch:
            dst.executemany(
                "INSERT OR IGNORE INTO options_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                batch,
            )
            asset_total += len(batch)
            batch.clear()

        dst.commit()
        grand_total += asset_total
        elapsed = time.perf_counter() - at0
        console.print(f"    {asset_name}: [green]{asset_total:,}[/] rows, {hours_emitted:,} hours in {elapsed:.1f}s")

    elapsed = time.perf_counter() - t0
    console.print(f"  Total options_snapshots: [green]{grand_total:,}[/] rows in {elapsed:.1f}s")
    return grand_total


# ---------------------------------------------------------------------------
# Step 4: Futures snapshots
# ---------------------------------------------------------------------------

def build_futures(src: sqlite3.Connection, dst: sqlite3.Connection) -> int:
    console.print("\n[bold cyan]Step 4:[/] Building futures_snapshots table...")
    t0 = time.perf_counter()
    grand_total = 0

    # --- BTC/ETH: sliding window over deribit_futures_history ---
    for asset_name in ("BTC", "ETH"):
        cfg = ASSETS[asset_name]
        console.print(f"  Processing [yellow]{asset_name}[/] dated futures...")
        at0 = time.perf_counter()

        cursor = src.execute(
            """SELECT timestamp, instrument_name, expiry_date, mark_price,
                      delivery_price, index_price
               FROM deribit_futures_history
               WHERE asset = ?
               ORDER BY timestamp""",
            (asset_name,),
        )

        window = {}
        win_times = {}
        current_hour = None
        batch = []
        asset_total = 0

        for trade in cursor:
            ts = trade["timestamp"]
            if current_hour is None:
                current_hour = floor_hour(ts)

            trade_hour = floor_hour(ts)
            while current_hour < trade_hour:
                # Emit all instruments in window (no minimum threshold)
                for inst, t in window.items():
                    exp_ms = t["expiry_date"]
                    exp_iso = ms_to_iso(exp_ms) if exp_ms else None
                    exp_str = expiry_iso_to_str(exp_iso) if exp_iso else None

                    mp = t["mark_price"]
                    if cfg.is_inverse and t["index_price"]:
                        mp = mp * t["index_price"]

                    batch.append((
                        current_hour, asset_name, inst,
                        exp_iso, exp_str, mp,
                        t["delivery_price"], t["index_price"],
                    ))

                if len(batch) >= BATCH_SIZE:
                    dst.executemany(
                        "INSERT OR IGNORE INTO futures_snapshots VALUES (?,?,?,?,?,?,?,?)",
                        batch,
                    )
                    asset_total += len(batch)
                    batch.clear()

                current_hour += HOUR_MS
                evict_before = current_hour - DAY_MS
                stale = [k for k, v in win_times.items() if v < evict_before]
                for k in stale:
                    del window[k]
                    del win_times[k]

            inst_name = trade["instrument_name"]
            window[inst_name] = {
                "expiry_date": trade["expiry_date"],
                "mark_price": trade["mark_price"],
                "delivery_price": trade["delivery_price"],
                "index_price": trade["index_price"],
            }
            win_times[inst_name] = ts

        # Final emission
        if current_hour is not None and window:
            for inst, t in window.items():
                exp_ms = t["expiry_date"]
                exp_iso = ms_to_iso(exp_ms) if exp_ms else None
                exp_str = expiry_iso_to_str(exp_iso) if exp_iso else None
                mp = t["mark_price"]
                if cfg.is_inverse and t["index_price"]:
                    mp = mp * t["index_price"]
                batch.append((
                    current_hour, asset_name, inst,
                    exp_iso, exp_str, mp,
                    t["delivery_price"], t["index_price"],
                ))

        if batch:
            dst.executemany(
                "INSERT OR IGNORE INTO futures_snapshots VALUES (?,?,?,?,?,?,?,?)",
                batch,
            )
            asset_total += len(batch)
            batch.clear()

        dst.commit()
        grand_total += asset_total
        elapsed = time.perf_counter() - at0
        console.print(f"    {asset_name}: [green]{asset_total:,}[/] rows in {elapsed:.1f}s")

    # --- SOL/XRP: synthetic SPOT rows from OHLCV ---
    for asset_name in ("SOL", "XRP"):
        console.print(f"  Processing [yellow]{asset_name}[/] SPOT from OHLCV...")
        at0 = time.perf_counter()

        cursor = src.execute(
            "SELECT timestamp, close FROM deribit_ohlcv WHERE asset = ? ORDER BY timestamp",
            (asset_name,),
        )

        batch = []
        asset_total = 0
        for row in cursor:
            batch.append((
                row["timestamp"], asset_name, "SPOT",
                None, None, row["close"],
                None, row["close"],
            ))
            if len(batch) >= BATCH_SIZE:
                dst.executemany(
                    "INSERT OR IGNORE INTO futures_snapshots VALUES (?,?,?,?,?,?,?,?)",
                    batch,
                )
                asset_total += len(batch)
                batch.clear()

        if batch:
            dst.executemany(
                "INSERT OR IGNORE INTO futures_snapshots VALUES (?,?,?,?,?,?,?,?)",
                batch,
            )
            asset_total += len(batch)
            batch.clear()

        dst.commit()
        grand_total += asset_total
        elapsed = time.perf_counter() - at0
        console.print(f"    {asset_name}: [green]{asset_total:,}[/] rows in {elapsed:.1f}s")

    elapsed = time.perf_counter() - t0
    console.print(f"  Total futures_snapshots: [green]{grand_total:,}[/] rows in {elapsed:.1f}s")
    return grand_total


# ---------------------------------------------------------------------------
# Step 5: Funding rates
# ---------------------------------------------------------------------------

def build_funding(src: sqlite3.Connection, dst: sqlite3.Connection) -> int:
    console.print("\n[bold cyan]Step 5:[/] Building funding_rates table...")
    t0 = time.perf_counter()

    cursor = src.execute(
        "SELECT asset, timestamp, funding_8h FROM deribit_funding_history ORDER BY asset, timestamp"
    )

    batch = []
    total = 0
    for row in cursor:
        batch.append((row["asset"], row["timestamp"], row["funding_8h"]))
        if len(batch) >= BATCH_SIZE:
            dst.executemany(
                "INSERT OR IGNORE INTO funding_rates VALUES (?,?,?)",
                batch,
            )
            total += len(batch)
            batch.clear()

    if batch:
        dst.executemany(
            "INSERT OR IGNORE INTO funding_rates VALUES (?,?,?)",
            batch,
        )
        total += len(batch)

    dst.commit()
    elapsed = time.perf_counter() - t0
    console.print(f"  Inserted [green]{total:,}[/] funding rows in {elapsed:.1f}s")
    return total


# ---------------------------------------------------------------------------
# Step 6: OHLCV
# ---------------------------------------------------------------------------

def build_ohlcv(src: sqlite3.Connection, dst: sqlite3.Connection) -> int:
    console.print("\n[bold cyan]Step 6:[/] Building ohlcv table...")
    t0 = time.perf_counter()

    cursor = src.execute(
        "SELECT asset, timestamp, open, high, low, close, volume FROM deribit_ohlcv ORDER BY asset, timestamp"
    )

    batch = []
    total = 0
    for row in cursor:
        batch.append((
            row["asset"], row["timestamp"],
            row["open"], row["high"], row["low"], row["close"], row["volume"],
        ))
        if len(batch) >= BATCH_SIZE:
            dst.executemany(
                "INSERT OR IGNORE INTO ohlcv VALUES (?,?,?,?,?,?,?)",
                batch,
            )
            total += len(batch)
            batch.clear()

    if batch:
        dst.executemany(
            "INSERT OR IGNORE INTO ohlcv VALUES (?,?,?,?,?,?,?)",
            batch,
        )
        total += len(batch)

    dst.commit()
    elapsed = time.perf_counter() - t0
    console.print(f"  Inserted [green]{total:,}[/] OHLCV rows in {elapsed:.1f}s")
    return total


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(dst: sqlite3.Connection):
    table = Table(title="Sample Database Summary")
    table.add_column("Table", style="cyan")
    table.add_column("Rows", justify="right", style="green")

    for tbl in ("markets", "market_prices", "options_snapshots",
                "futures_snapshots", "funding_rates", "ohlcv"):
        count = dst.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        table.add_row(tbl, f"{count:,}")

    console.print("\n")
    console.print(table)

    # DB file size
    size_mb = SAMPLE_DB.stat().st_size / (1024 * 1024)
    console.print(f"\nDatabase size: [bold]{size_mb:.1f} MB[/]")


# ---------------------------------------------------------------------------
# Step 7: Visualization
# ---------------------------------------------------------------------------

def build_charts(dst: sqlite3.Connection):
    console.print("\n[bold cyan]Step 7:[/] Generating charts...")

    import datetime as _dt
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    chart_dir = SAMPLE_DIR
    charts_made = 0

    # Helper: save and close
    def save(fig, name):
        nonlocal charts_made
        path = chart_dir / name
        fig.savefig(str(path), dpi=120, bbox_inches="tight")
        plt.close(fig)
        charts_made += 1
        console.print(f"    Saved {name}")

    # ---- 1. Markets by asset ----
    rows = dst.execute("SELECT asset, COUNT(*) FROM markets GROUP BY asset ORDER BY asset").fetchall()
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar([r[0] for r in rows], [r[1] for r in rows], color=["#f4a261", "#2a9d8f", "#e76f51", "#264653"])
    ax.set_title("Markets by Asset")
    ax.set_ylabel("Count")
    save(fig, "markets_by_asset.png")

    # ---- 2. Markets by direction ----
    rows = dst.execute("SELECT direction, COUNT(*) FROM markets GROUP BY direction ORDER BY COUNT(*) DESC").fetchall()
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.barh([r[0] for r in rows], [r[1] for r in rows], color="#2a9d8f")
    ax.set_title("Markets by Direction")
    ax.set_xlabel("Count")
    save(fig, "markets_by_direction.png")

    # ---- 3. Outcome distribution ----
    rows = dst.execute(
        "SELECT asset, outcome, COUNT(*) FROM markets GROUP BY asset, outcome ORDER BY asset, outcome"
    ).fetchall()
    assets_seen = []
    yes_counts = {}
    no_counts = {}
    for asset, outcome, cnt in rows:
        if asset not in assets_seen:
            assets_seen.append(asset)
        if outcome == 1:
            yes_counts[asset] = cnt
        else:
            no_counts[asset] = cnt

    fig, ax = plt.subplots(figsize=(6, 4))
    x = range(len(assets_seen))
    yes_vals = [yes_counts.get(a, 0) for a in assets_seen]
    no_vals = [no_counts.get(a, 0) for a in assets_seen]
    ax.bar(x, yes_vals, label="YES won", color="#2a9d8f")
    ax.bar(x, no_vals, bottom=yes_vals, label="NO won", color="#e76f51")
    ax.set_xticks(x)
    ax.set_xticklabels(assets_seen)
    ax.set_title("Outcome Distribution")
    ax.set_ylabel("Count")
    ax.legend()
    save(fig, "outcome_distribution.png")

    # ---- 4. Price coverage over time ----
    rows = dst.execute("""
        SELECT (timestamp / 2592000) * 2592000 as month_bucket, COUNT(DISTINCT condition_id)
        FROM market_prices
        GROUP BY month_bucket ORDER BY month_bucket
    """).fetchall()
    if rows:
        dates = [_dt.datetime.fromtimestamp(r[0], tz=_dt.timezone.utc) for r in rows]
        counts = [r[1] for r in rows]
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(dates, counts, color="#264653", linewidth=1.5)
        ax.set_title("Markets with Price Data Over Time")
        ax.set_ylabel("Distinct markets with prices")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        fig.autofmt_xdate()
        save(fig, "price_coverage.png")

    # ---- 5. Options instruments per hour ----
    rows = dst.execute("""
        SELECT asset, snapshot_hour, COUNT(*) as n
        FROM options_snapshots
        GROUP BY asset, snapshot_hour
        ORDER BY asset, snapshot_hour
    """).fetchall()
    if rows:
        by_asset = {}
        for asset, hour, n in rows:
            by_asset.setdefault(asset, ([], []))
            by_asset[asset][0].append(_dt.datetime.fromtimestamp(hour / 1000, tz=_dt.timezone.utc))
            by_asset[asset][1].append(n)

        fig, ax = plt.subplots(figsize=(12, 5))
        for asset, (times, counts) in by_asset.items():
            ax.plot(times, counts, label=asset, linewidth=0.5, alpha=0.8)
        ax.set_title("Options Instruments per Hourly Snapshot")
        ax.set_ylabel("Instruments")
        ax.legend()
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        fig.autofmt_xdate()
        save(fig, "options_instruments_per_hour.png")

    # ---- 6. IV distribution ----
    ivs = [r[0] for r in dst.execute("SELECT mark_iv FROM options_snapshots WHERE mark_iv > 0 AND mark_iv < 5").fetchall()]
    if ivs:
        import statistics
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.hist(ivs, bins=100, color="#2a9d8f", edgecolor="none", alpha=0.8)
        med = statistics.median(ivs)
        mean = statistics.mean(ivs)
        ax.axvline(med, color="red", linestyle="--", label=f"Median: {med:.3f}")
        ax.axvline(mean, color="orange", linestyle="--", label=f"Mean: {mean:.3f}")
        ax.set_title("IV Distribution (Annualized Decimal)")
        ax.set_xlabel("Mark IV")
        ax.set_ylabel("Count")
        ax.legend()
        save(fig, "iv_distribution.png")

    # ---- 7. IV smile example ----
    # Pick a well-populated snapshot for BTC
    best = dst.execute("""
        SELECT snapshot_hour, COUNT(*) as n
        FROM options_snapshots
        WHERE asset = 'BTC'
        GROUP BY snapshot_hour
        ORDER BY n DESC LIMIT 1
    """).fetchone()
    if best:
        sh = best[0]
        # Pick most common expiry at that hour
        exp = dst.execute("""
            SELECT expiry_str, COUNT(*) as n
            FROM options_snapshots
            WHERE asset='BTC' AND snapshot_hour=? AND option_type='C'
            GROUP BY expiry_str ORDER BY n DESC LIMIT 1
        """, (sh,)).fetchone()
        if exp:
            smile_rows = dst.execute("""
                SELECT strike, mark_iv FROM options_snapshots
                WHERE asset='BTC' AND snapshot_hour=? AND expiry_str=? AND option_type='C'
                ORDER BY strike
            """, (sh, exp[0])).fetchall()
            if len(smile_rows) >= 3:
                fig, ax = plt.subplots(figsize=(8, 4))
                ax.plot([r[0] for r in smile_rows], [r[1] for r in smile_rows], "o-", color="#264653")
                snap_dt = _dt.datetime.fromtimestamp(sh / 1000, tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
                ax.set_title(f"IV Smile — BTC Calls, Expiry {exp[0]}, Snap {snap_dt}")
                ax.set_xlabel("Strike ($)")
                ax.set_ylabel("Mark IV")
                save(fig, "iv_smile_example.png")

    # ---- 8. Asset prices ----
    fig, axes = plt.subplots(2, 2, figsize=(14, 8), sharex=False)
    for idx, asset in enumerate(["BTC", "ETH", "SOL", "XRP"]):
        ax = axes[idx // 2][idx % 2]
        rows = dst.execute(
            "SELECT timestamp, close FROM ohlcv WHERE asset=? ORDER BY timestamp",
            (asset,),
        ).fetchall()
        if rows:
            times = [_dt.datetime.fromtimestamp(r[0] / 1000, tz=_dt.timezone.utc) for r in rows]
            prices = [r[1] for r in rows]
            ax.plot(times, prices, linewidth=0.7, color="#264653")
        ax.set_title(f"{asset} Price")
        ax.set_ylabel("USD")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    fig.suptitle("Asset Prices (OHLCV Close)", fontsize=14)
    fig.tight_layout()
    save(fig, "asset_prices.png")

    # ---- 9. Funding rates ----
    fig, ax = plt.subplots(figsize=(12, 5))
    for asset in ["BTC", "ETH", "SOL", "XRP"]:
        rows = dst.execute(
            "SELECT timestamp, interest_8h FROM funding_rates WHERE asset=? ORDER BY timestamp",
            (asset,),
        ).fetchall()
        if rows:
            times = [_dt.datetime.fromtimestamp(r[0] / 1000, tz=_dt.timezone.utc) for r in rows]
            rates = [r[1] for r in rows]
            ax.plot(times, rates, label=asset, linewidth=0.5, alpha=0.8)
    ax.set_title("Funding Rates Over Time")
    ax.set_ylabel("Funding Rate")
    ax.legend()
    ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    save(fig, "funding_rates.png")

    # ---- 10. Market price examples ----
    # Pick 6 markets with the most price points
    examples = dst.execute("""
        SELECT m.condition_id, m.question, m.asset, COUNT(*) as n
        FROM markets m
        JOIN market_prices mp ON m.condition_id = mp.condition_id
        GROUP BY m.condition_id
        ORDER BY n DESC LIMIT 6
    """).fetchall()
    if examples:
        fig, axes = plt.subplots(2, 3, figsize=(16, 8))
        for idx, (cid, question, asset, _) in enumerate(examples):
            ax = axes[idx // 3][idx % 3]
            rows = dst.execute(
                "SELECT timestamp, yes_price FROM market_prices WHERE condition_id=? ORDER BY timestamp",
                (cid,),
            ).fetchall()
            times = [_dt.datetime.fromtimestamp(r[0], tz=_dt.timezone.utc) for r in rows]
            prices = [r[1] for r in rows]
            ax.plot(times, prices, linewidth=0.8, color="#e76f51")
            # Truncate question for title
            short_q = (question[:50] + "...") if question and len(question) > 50 else (question or "")
            ax.set_title(f"{asset}: {short_q}", fontsize=8)
            ax.set_ylim(-0.05, 1.05)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        fig.suptitle("Example Market Price Trajectories", fontsize=14)
        fig.tight_layout()
        save(fig, "market_price_examples.png")

    # ---- 11. Data summary table ----
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.axis("off")
    summary_data = []
    for tbl in ("markets", "market_prices", "options_snapshots",
                "futures_snapshots", "funding_rates", "ohlcv"):
        cnt = dst.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]

        # Date range
        if tbl == "markets":
            r = dst.execute("SELECT MIN(settlement_date), MAX(settlement_date) FROM markets").fetchone()
            date_range = f"{r[0][:10]} to {r[1][:10]}" if r[0] else "N/A"
        elif tbl == "market_prices":
            r = dst.execute("SELECT MIN(timestamp), MAX(timestamp) FROM market_prices").fetchone()
            if r[0]:
                d0 = _dt.datetime.fromtimestamp(r[0], tz=_dt.timezone.utc).strftime("%Y-%m-%d")
                d1 = _dt.datetime.fromtimestamp(r[1], tz=_dt.timezone.utc).strftime("%Y-%m-%d")
                date_range = f"{d0} to {d1}"
            else:
                date_range = "N/A"
        elif tbl in ("options_snapshots", "futures_snapshots"):
            r = dst.execute(f"SELECT MIN(snapshot_hour), MAX(snapshot_hour) FROM {tbl}").fetchone()
            if r[0]:
                d0 = _dt.datetime.fromtimestamp(r[0] / 1000, tz=_dt.timezone.utc).strftime("%Y-%m-%d")
                d1 = _dt.datetime.fromtimestamp(r[1] / 1000, tz=_dt.timezone.utc).strftime("%Y-%m-%d")
                date_range = f"{d0} to {d1}"
            else:
                date_range = "N/A"
        elif tbl in ("funding_rates", "ohlcv"):
            r = dst.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {tbl}").fetchone()
            if r[0]:
                d0 = _dt.datetime.fromtimestamp(r[0] / 1000, tz=_dt.timezone.utc).strftime("%Y-%m-%d")
                d1 = _dt.datetime.fromtimestamp(r[1] / 1000, tz=_dt.timezone.utc).strftime("%Y-%m-%d")
                date_range = f"{d0} to {d1}"
            else:
                date_range = "N/A"
        else:
            date_range = "N/A"

        summary_data.append([tbl, f"{cnt:,}", date_range])

    table = ax.table(
        cellText=summary_data,
        colLabels=["Table", "Rows", "Date Range"],
        loc="center",
        cellLoc="left",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)
    ax.set_title("Data Summary", fontsize=14, pad=20)
    save(fig, "data_summary.png")

    console.print(f"  Generated [green]{charts_made}[/] charts in sample/")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build backtest sample database")
    parser.add_argument("--no-charts", action="store_true", help="Skip chart generation")
    args = parser.parse_args()

    console.print("[bold]Building sample database...[/]")
    console.print(f"  Source: {DB_PATH}")
    console.print(f"  Destination: {SAMPLE_DB}")

    t0 = time.perf_counter()

    src = open_source_db()
    dst = create_sample_db()

    try:
        build_markets(src, dst)
        build_prices(src, dst)
        build_options(src, dst)
        build_futures(src, dst)
        build_funding(src, dst)
        build_ohlcv(src, dst)
        print_summary(dst)

        if not args.no_charts:
            build_charts(dst)
    finally:
        src.close()
        dst.close()

    elapsed = time.perf_counter() - t0
    console.print(f"\n[bold green]Done![/] Total time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
