"""Polymarket price history: CLOB primary + Goldsky subgraph backfill."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from collectors.base import BaseCollector
from config import (
    CLOB_BASE_URL,
    GOLDSKY_EARLIEST_TIMESTAMP,
    GOLDSKY_PAGE_SIZE,
    GOLDSKY_SEMAPHORE,
    GOLDSKY_URL,
    POLYMARKET_SEMAPHORE,
    PRICE_FIDELITY_MINUTES,
    PRICE_LOOKBACK_DAYS,
    USDC_ASSET_ID,
)
from database import Database

log = logging.getLogger(__name__)


class PolymarketPricesCollector(BaseCollector):
    """Two-phase price collection: CLOB then Goldsky backfill."""

    def __init__(self):
        super().__init__(concurrency=POLYMARKET_SEMAPHORE)

    async def collect(self, db: Database, asset: str) -> dict:
        """Collect prices for all markets of an asset.

        Returns stats dict with clob_success, clob_empty, goldsky_backfilled, no_data.
        """
        markets = await db.get_all_markets(asset)
        if not markets:
            log.warning("No markets found for %s", asset)
            return {"clob_success": 0, "clob_empty": 0, "goldsky_backfilled": 0, "no_data": 0}

        log.info("Phase 1: CLOB price history for %d %s markets", len(markets), asset)
        clob_results = await self._phase_clob(db, markets)

        # Phase 2: Goldsky backfill for markets with empty CLOB
        missing = await db.get_markets_missing_prices(asset)
        eligible = [
            m for m in missing
            if self._is_goldsky_eligible(m)
        ]
        log.info(
            "Phase 2: Goldsky backfill for %d/%d missing markets (eligible)",
            len(eligible), len(missing),
        )
        goldsky_count = 0
        if eligible:
            goldsky_count = await self._phase_goldsky(db, eligible)

        stats = {
            "clob_success": clob_results["success"],
            "clob_empty": clob_results["empty"],
            "goldsky_backfilled": goldsky_count,
            "no_data": len(missing) - goldsky_count,
        }
        log.info("Price collection stats: %s", stats)
        return stats

    # ---- Phase 1: CLOB ----

    async def _phase_clob(self, db: Database, markets: list[dict]) -> dict:
        sem = asyncio.Semaphore(5)
        success = 0
        empty = 0
        total = len(markets)

        async def fetch_one(mkt):
            nonlocal success, empty
            token_id = mkt.get("yes_token_id")
            if not token_id:
                return

            settlement = mkt.get("settlement_date")
            if settlement:
                try:
                    end_dt = datetime.fromisoformat(settlement)
                except (ValueError, TypeError):
                    end_dt = datetime.now(timezone.utc)
            else:
                end_dt = datetime.now(timezone.utc)

            start_dt = end_dt - timedelta(days=PRICE_LOOKBACK_DAYS)
            start_ts = int(start_dt.timestamp())
            end_ts = int(end_dt.timestamp())

            params = {
                "market": token_id,
                "startTs": start_ts,
                "endTs": end_ts,
                "fidelity": PRICE_FIDELITY_MINUTES,
            }

            async with sem:
                data = await self._get(
                    f"{CLOB_BASE_URL}/prices-history", params=params
                )

            if not data:
                empty += 1
                done = success + empty
                if done % 200 == 0:
                    log.info("CLOB progress: %d/%d done (%d success, %d empty)", done, total, success, empty)
                return

            history = data.get("history", [])
            if not history:
                empty += 1
                done = success + empty
                if done % 200 == 0:
                    log.info("CLOB progress: %d/%d done (%d success, %d empty)", done, total, success, empty)
                return

            rows = []
            for pt in history:
                ts = pt.get("t")
                price = pt.get("p")
                if ts is not None and price is not None:
                    rows.append({
                        "condition_id": mkt["condition_id"],
                        "timestamp": int(ts),
                        "yes_price": float(price),
                    })

            if rows:
                await db.insert_price_history(rows)
                success += 1
            else:
                empty += 1

            done = success + empty
            if done % 200 == 0:
                log.info("CLOB progress: %d/%d done (%d success, %d empty)", done, total, success, empty)

        tasks = [fetch_one(m) for m in markets]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            log.warning("CLOB phase: %d tasks raised exceptions", len(errors))
            for exc in errors[:3]:
                log.warning("  %s: %s", type(exc).__name__, exc)
        log.info("CLOB phase done: %d success, %d empty, %d errors", success, empty, len(errors))
        return {"success": success, "empty": empty}

    # ---- Phase 2: Goldsky ----

    def _is_goldsky_eligible(self, market: dict) -> bool:
        """Check if market settlement is after Goldsky data availability."""
        sd = market.get("settlement_date")
        if not sd:
            return False
        try:
            dt = datetime.fromisoformat(sd)
            return dt.timestamp() >= GOLDSKY_EARLIEST_TIMESTAMP
        except (ValueError, TypeError):
            return False

    async def _phase_goldsky(self, db: Database, markets: list[dict]) -> int:
        """Backfill prices via Goldsky subgraph. Returns count of markets backfilled."""
        sem = asyncio.Semaphore(GOLDSKY_SEMAPHORE)
        backfilled = 0
        done = 0
        total = len(markets)

        async def fetch_one(mkt):
            nonlocal backfilled, done
            yes_token = mkt.get("yes_token_id", "")
            no_token = mkt.get("no_token_id", "")
            token_ids = [t for t in [yes_token, no_token] if t]
            if not token_ids:
                done += 1
                if done % 50 == 0:
                    log.info("Goldsky progress: %d/%d done (%d backfilled)", done, total, backfilled)
                return

            settlement = mkt.get("settlement_date")
            try:
                end_dt = datetime.fromisoformat(settlement)
            except (ValueError, TypeError):
                done += 1
                if done % 50 == 0:
                    log.info("Goldsky progress: %d/%d done (%d backfilled)", done, total, backfilled)
                return

            start_dt = end_dt - timedelta(days=PRICE_LOOKBACK_DAYS)
            start_ts = max(int(start_dt.timestamp()), GOLDSKY_EARLIEST_TIMESTAMP)

            fills = await self._query_goldsky_fills(sem, token_ids, start_ts)
            if not fills:
                done += 1
                if done % 50 == 0:
                    log.info("Goldsky progress: %d/%d done (%d backfilled)", done, total, backfilled)
                return

            # Calculate prices and bucket into 30-min intervals
            buckets: dict[int, float] = {}
            for fill in fills:
                price = self._calc_fill_price(fill, yes_token)
                if price is None:
                    continue
                ts = int(fill["timestamp"])
                bucket = (ts // 1800) * 1800
                buckets[bucket] = price  # last price wins

            rows = [
                {
                    "condition_id": mkt["condition_id"],
                    "timestamp": ts,
                    "yes_price": price,
                }
                for ts, price in sorted(buckets.items())
            ]

            if rows:
                await db.insert_price_history(rows)
                backfilled += 1

            done += 1
            if done % 50 == 0:
                log.info("Goldsky progress: %d/%d done (%d backfilled)", done, total, backfilled)

        tasks = [fetch_one(m) for m in markets]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            log.warning("Goldsky phase: %d tasks raised exceptions", len(errors))
            for exc in errors[:3]:
                log.warning("  %s: %s", type(exc).__name__, exc)
        log.info("Goldsky phase done: %d backfilled, %d errors", backfilled, len(errors))
        return backfilled

    async def _query_goldsky_fills(
        self,
        sem: asyncio.Semaphore,
        token_ids: list[str],
        start_ts: int,
    ) -> list[dict]:
        """Paginate through Goldsky orderFilledEvents."""
        all_fills = []
        cursor_ts = str(start_ts)
        cursor_id = None
        sticky_mode = False

        for _ in range(200):  # safety limit
            if sticky_mode and cursor_id:
                where_clause = self._build_goldsky_where_id(token_ids, cursor_id)
            else:
                where_clause = self._build_goldsky_where_ts(token_ids, cursor_ts)

            query = f"""{{
  orderFilledEvents(
    first: {GOLDSKY_PAGE_SIZE}
    orderBy: timestamp
    orderDirection: asc
    where: {where_clause}
  ) {{
    id
    timestamp
    makerAmountFilled
    takerAmountFilled
    makerAssetId
    takerAssetId
  }}
}}"""

            async with sem:
                resp = await self._post(
                    GOLDSKY_URL,
                    json={"query": query},
                    headers={"Content-Type": "application/json"},
                )

            if not resp:
                break

            data = resp.get("data", {})
            fills = data.get("orderFilledEvents", [])
            if not fills:
                break

            all_fills.extend(fills)

            if len(fills) < GOLDSKY_PAGE_SIZE:
                break

            # Check if all timestamps are the same (sticky mode)
            timestamps = {f["timestamp"] for f in fills}
            if len(timestamps) == 1:
                sticky_mode = True
                cursor_id = fills[-1]["id"]
            else:
                sticky_mode = False
                cursor_ts = fills[-1]["timestamp"]
                cursor_id = None

        return all_fills

    def _build_goldsky_where_ts(self, token_ids: list[str], ts: str) -> str:
        ids_str = ", ".join(f'"{t}"' for t in token_ids)
        return (
            f'{{ or: ['
            f'{{ timestamp_gt: "{ts}", makerAssetId_in: [{ids_str}] }}, '
            f'{{ timestamp_gt: "{ts}", takerAssetId_in: [{ids_str}] }}'
            f'] }}'
        )

    def _build_goldsky_where_id(self, token_ids: list[str], id_cursor: str) -> str:
        ids_str = ", ".join(f'"{t}"' for t in token_ids)
        return (
            f'{{ or: ['
            f'{{ id_gt: "{id_cursor}", makerAssetId_in: [{ids_str}] }}, '
            f'{{ id_gt: "{id_cursor}", takerAssetId_in: [{ids_str}] }}'
            f'] }}'
        )

    def _calc_fill_price(self, fill: dict, yes_token_id: str) -> float | None:
        """Calculate yes_price from a Goldsky fill event."""
        maker_asset = fill.get("makerAssetId", "")
        taker_asset = fill.get("takerAssetId", "")

        try:
            maker_amount = int(fill.get("makerAmountFilled", 0))
            taker_amount = int(fill.get("takerAmountFilled", 0))
        except (ValueError, TypeError):
            return None

        if maker_amount <= 0 or taker_amount <= 0:
            return None

        price = None

        # Maker sells tokens for USDC
        if maker_asset == yes_token_id and taker_asset == USDC_ASSET_ID:
            price = (taker_amount / 1e6) / (maker_amount / 1e6)
        # Taker buys tokens with USDC
        elif taker_asset == yes_token_id and maker_asset == USDC_ASSET_ID:
            price = (maker_amount / 1e6) / (taker_amount / 1e6)

        if price is not None and 0 < price < 1.0:
            return round(price, 6)
        return None
