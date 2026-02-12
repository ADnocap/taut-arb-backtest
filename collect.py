"""Main orchestrator CLI — runs data collection pipeline in dependency order."""

import argparse
import asyncio
import logging
import sys

from rich.console import Console
from rich.logging import RichHandler

from collectors.deribit_funding import DeribitFundingCollector
from collectors.deribit_futures import DeribitFuturesCollector
from collectors.deribit_ohlcv import DeribitOHLCVCollector
from collectors.deribit_options import DeribitOptionsCollector
from collectors.polymarket_markets import PolymarketMarketsCollector
from collectors.polymarket_prices import PolymarketPricesCollector
from database import Database

console = Console()

STEPS = {
    1: "Polymarket markets",
    2: "Polymarket price histories",
    3: "Deribit options trades",
    4: "Deribit futures trades",
    5: "Deribit funding rates",
    6: "Deribit OHLCV candles",
}


async def run_pipeline(assets: list[str], step: int | None = None):
    async with Database() as db:
        steps_to_run = [step] if step else list(STEPS.keys())

        for s in steps_to_run:
            console.rule(f"[bold]Step {s}: {STEPS[s]}")

            if s == 1:
                async with PolymarketMarketsCollector() as collector:
                    count = await collector.collect(db, assets)
                console.print(f"  Saved {count} markets")

            elif s == 2:
                async with PolymarketPricesCollector() as collector:
                    for asset in assets:
                        stats = await collector.collect(db, asset)
                        console.print(f"  {asset}: CLOB={stats['clob_success']}, "
                                      f"Goldsky={stats['goldsky_backfilled']}, "
                                      f"empty={stats['no_data']}")

            elif s == 3:
                async with DeribitOptionsCollector() as collector:
                    for asset in assets:
                        count = await collector.collect(db, asset)
                        console.print(f"  {asset}: {count} option trades")

            elif s == 4:
                async with DeribitFuturesCollector() as collector:
                    for asset in assets:
                        count = await collector.collect(db, asset)
                        console.print(f"  {asset}: {count} futures trades")

            elif s == 5:
                async with DeribitFundingCollector() as collector:
                    for asset in assets:
                        count = await collector.collect(db, asset)
                        console.print(f"  {asset}: {count} funding records")

            elif s == 6:
                async with DeribitOHLCVCollector() as collector:
                    for asset in assets:
                        count = await collector.collect(db, asset)
                        console.print(f"  {asset}: {count} OHLCV candles")

        # Run validation report
        console.rule("[bold]Validation Report")
        from validate import print_report
        await print_report(db)


def main():
    parser = argparse.ArgumentParser(description="Crypto arbitrage data collection pipeline")
    parser.add_argument(
        "--assets",
        nargs="+",
        default=["BTC"],
        choices=["BTC", "ETH", "SOL", "XRP"],
        help="Assets to collect (default: BTC)",
    )
    parser.add_argument(
        "--step",
        type=int,
        choices=list(STEPS.keys()),
        help="Run a single step (for debugging)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    console.print(f"[bold green]Starting pipeline for assets: {args.assets}")
    asyncio.run(run_pipeline(args.assets, args.step))
    console.print("[bold green]Done!")


if __name__ == "__main__":
    main()
