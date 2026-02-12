"""Data quality report — Rich-formatted console output."""

import asyncio
import logging
import sys
from datetime import datetime, timezone

from rich.console import Console
from rich.table import Table

from database import Database

console = Console()
log = logging.getLogger(__name__)


async def print_report(db: Database):
    """Print comprehensive data quality report."""

    # 1. Table row counts
    counts = await db.get_table_counts()
    t = Table(title="Table Row Counts")
    t.add_column("Table", style="cyan")
    t.add_column("Rows", justify="right", style="green")
    for table, count in counts.items():
        t.add_row(table, f"{count:,}")
    console.print(t)

    # 2. Price coverage per asset
    t = Table(title="Price History Coverage")
    t.add_column("Asset", style="cyan")
    t.add_column("Total Markets", justify="right")
    t.add_column("With Prices", justify="right", style="green")
    t.add_column("Coverage %", justify="right", style="bold")

    for asset in ["BTC", "ETH", "SOL", "XRP"]:
        cov = await db.get_price_coverage(asset)
        if cov["total"] > 0:
            t.add_row(
                asset,
                str(cov["total"]),
                str(cov["with_prices"]),
                f"{cov['coverage_pct']:.1f}%",
            )
    console.print(t)

    # 2b. Price history source breakdown
    t = Table(title="Price History Source Breakdown")
    t.add_column("Asset", style="cyan")
    t.add_column("Goldsky Rows", justify="right", style="green")
    t.add_column("CLOB Rows", justify="right", style="yellow")
    t.add_column("Avg Volume (USDC)", justify="right")
    t.add_column("Avg Trades/Bucket", justify="right")
    t.add_column("Avg Spread", justify="right")

    for asset in ["BTC", "ETH", "SOL", "XRP"]:
        # Goldsky rows
        cur = await db._db.execute(
            """SELECT COUNT(*) FROM polymarket_price_history ph
               JOIN polymarket_markets pm ON ph.condition_id = pm.condition_id
               WHERE pm.asset = ? AND ph.source = 'goldsky'""",
            (asset,),
        )
        goldsky_rows = (await cur.fetchone())[0]

        # CLOB rows
        cur = await db._db.execute(
            """SELECT COUNT(*) FROM polymarket_price_history ph
               JOIN polymarket_markets pm ON ph.condition_id = pm.condition_id
               WHERE pm.asset = ? AND ph.source = 'clob'""",
            (asset,),
        )
        clob_rows = (await cur.fetchone())[0]

        if goldsky_rows + clob_rows == 0:
            continue

        # Avg volume (where volume is not null)
        cur = await db._db.execute(
            """SELECT AVG(ph.volume) FROM polymarket_price_history ph
               JOIN polymarket_markets pm ON ph.condition_id = pm.condition_id
               WHERE pm.asset = ? AND ph.volume IS NOT NULL""",
            (asset,),
        )
        avg_vol = (await cur.fetchone())[0]

        # Avg trade count
        cur = await db._db.execute(
            """SELECT AVG(ph.trade_count) FROM polymarket_price_history ph
               JOIN polymarket_markets pm ON ph.condition_id = pm.condition_id
               WHERE pm.asset = ? AND ph.trade_count IS NOT NULL""",
            (asset,),
        )
        avg_trades = (await cur.fetchone())[0]

        # Avg spread (yes_price + no_price - 1.0) where both non-null
        cur = await db._db.execute(
            """SELECT AVG(ph.yes_price + ph.no_price - 1.0)
               FROM polymarket_price_history ph
               JOIN polymarket_markets pm ON ph.condition_id = pm.condition_id
               WHERE pm.asset = ? AND ph.yes_price IS NOT NULL AND ph.no_price IS NOT NULL""",
            (asset,),
        )
        avg_spread = (await cur.fetchone())[0]

        t.add_row(
            asset,
            f"{goldsky_rows:,}",
            f"{clob_rows:,}",
            f"${avg_vol:,.2f}" if avg_vol else "N/A",
            f"{avg_trades:.1f}" if avg_trades else "N/A",
            f"{avg_spread:.4f}" if avg_spread is not None else "N/A",
        )
    console.print(t)

    # 3. Deribit data ranges
    deribit_tables = [
        ("deribit_option_trades", "Options"),
        ("deribit_futures_history", "Futures"),
        ("deribit_funding_history", "Funding"),
        ("deribit_ohlcv", "OHLCV"),
    ]

    t = Table(title="Deribit Data Ranges")
    t.add_column("Data Type", style="cyan")
    t.add_column("Asset")
    t.add_column("From")
    t.add_column("To")
    t.add_column("Count", justify="right", style="green")

    for table, label in deribit_tables:
        for asset in ["BTC", "ETH", "SOL", "XRP"]:
            info = await db.get_deribit_date_range(table, asset)
            if info:
                from_dt = datetime.fromtimestamp(
                    info["min_ts"] / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d")
                to_dt = datetime.fromtimestamp(
                    info["max_ts"] / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d")
                t.add_row(label, asset, from_dt, to_dt, f"{info['count']:,}")
    console.print(t)

    # 4. Warnings
    warnings = []

    # Check for perpetuals in futures
    cur = await db._db.execute(
        "SELECT COUNT(*) FROM deribit_futures_history WHERE instrument_name LIKE '%PERPETUAL%'"
    )
    row = await cur.fetchone()
    if row[0] > 0:
        warnings.append(f"PERPETUAL in futures table: {row[0]} rows (should be 0)")

    # Check IV distribution
    cur = await db._db.execute(
        "SELECT MIN(iv), MAX(iv), AVG(iv), COUNT(*) FROM deribit_option_trades"
    )
    row = await cur.fetchone()
    if row[3] > 0:
        console.print(
            f"\n[bold]IV Stats:[/] min={row[0]:.4f}, max={row[1]:.4f}, "
            f"avg={row[2]:.4f}, count={row[3]:,}"
        )
        if row[1] > 5.0:
            warnings.append(f"IV values > 5.0 detected (max={row[1]:.2f})")

    # Check funding rate completeness
    cur = await db._db.execute(
        """SELECT asset, date(timestamp/1000, 'unixepoch') as d, COUNT(*) as c
           FROM deribit_funding_history
           GROUP BY asset, d
           HAVING c < 3
           LIMIT 10"""
    )
    incomplete_days = await cur.fetchall()
    if incomplete_days:
        warnings.append(
            f"Funding rate incomplete days: {len(incomplete_days)} "
            f"(expected 3/day, first: {dict(incomplete_days[0])})"
        )

    # Check OHLCV completeness
    cur = await db._db.execute(
        """SELECT asset, date(timestamp/1000, 'unixepoch') as d, COUNT(*) as c
           FROM deribit_ohlcv
           GROUP BY asset, d
           HAVING c < 24
           LIMIT 10"""
    )
    incomplete_ohlcv = await cur.fetchall()
    if incomplete_ohlcv:
        warnings.append(
            f"OHLCV incomplete days: {len(incomplete_ohlcv)} (expected 24/day)"
        )

    if warnings:
        console.print("\n[bold red]Warnings:")
        for w in warnings:
            console.print(f"  [yellow]- {w}")
    else:
        console.print("\n[bold green]No warnings!")


async def main():
    async with Database() as db:
        await print_report(db)


if __name__ == "__main__":
    asyncio.run(main())
