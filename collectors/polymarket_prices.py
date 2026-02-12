"""Polymarket price history: Goldsky primary + CLOB fallback."""

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
    """Two-phase price collection: Goldsky primary, CLOB fallback."""

    def __init__(self):
        super().__init__(concurrency=POLYMARKET_SEMAPHORE)

    async def collect(self, db: Database, asset: str) -> dict:
        """Collect prices for all markets of an asset.

        Returns stats dict with goldsky_success, clob_fallback, no_data.
        """
        markets = await db.get_all_markets(asset)
        if not markets:
            log.warning("No markets found for %s", asset)
            return {"goldsky_success": 0, "clob_fallback": 0, "no_data": 0}

        # Phase 1: Goldsky for eligible markets (real trades with volume)
        eligible = [m for m in markets if self._is_goldsky_eligible(m)]
        log.info(
            "Phase 1: Goldsky for %d/%d %s markets (eligible)",
            len(eligible), len(markets), asset,
        )
        goldsky_count = 0
        if eligible:
            goldsky_count = await self._phase_goldsky(db, eligible)

        # Phase 2: CLOB fallback for markets still missing prices
        missing = await db.get_markets_missing_prices(asset)
        log.info(
            "Phase 2: CLOB fallback for %d missing %s markets",
            len(missing), asset,
        )
        clob_count = 0
        if missing:
            clob_results = await self._phase_clob(db, missing)
            clob_count = clob_results["success"]

        # Recount missing after both phases
        still_missing = await db.get_markets_missing_prices(asset)
        stats = {
            "goldsky_success": goldsky_count,
            "clob_fallback": clob_count,
            "no_data": len(still_missing),
        }
        log.info("Price collection stats: %s", stats)
        return stats

    # ---- Phase 1: Goldsky (primary) ----

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

            # Dual-token VWAP bucketing into 30-min intervals
            buckets: dict[int, dict] = {}
            for fill in fills:
                result = self._calc_fill_price_and_volume(fill, yes_token, no_token)
                if result is None:
                    continue
                side, price, usdc_vol = result
                ts = int(fill["timestamp"])
                bucket = (ts // 1800) * 1800

                if bucket not in buckets:
                    buckets[bucket] = {
                        "yes_pv": 0.0, "yes_vol": 0.0, "yes_n": 0,
                        "no_pv": 0.0, "no_vol": 0.0, "no_n": 0,
                    }

                b = buckets[bucket]
                if side == "yes":
                    b["yes_pv"] += price * usdc_vol
                    b["yes_vol"] += usdc_vol
                    b["yes_n"] += 1
                else:
                    b["no_pv"] += price * usdc_vol
                    b["no_vol"] += usdc_vol
                    b["no_n"] += 1

            rows = []
            for ts in sorted(buckets):
                b = buckets[ts]
                yes_price = round(b["yes_pv"] / b["yes_vol"], 6) if b["yes_vol"] > 0 else None
                no_price = round(b["no_pv"] / b["no_vol"], 6) if b["no_vol"] > 0 else None
                volume = round(b["yes_vol"] + b["no_vol"], 2)
                trade_count = b["yes_n"] + b["no_n"]
                rows.append({
                    "condition_id": mkt["condition_id"],
                    "timestamp": ts,
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "volume": volume if volume > 0 else None,
                    "trade_count": trade_count if trade_count > 0 else None,
                    "source": "goldsky",
                })

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

    def _calc_fill_price_and_volume(
        self, fill: dict, yes_token_id: str, no_token_id: str
    ) -> tuple[str, float, float] | None:
        """Calculate price and USDC volume from a Goldsky fill event.

        Returns ("yes"|"no", price, usdc_volume) or None if unparseable.
        """
        maker_asset = fill.get("makerAssetId", "")
        taker_asset = fill.get("takerAssetId", "")

        try:
            maker_amount = int(fill.get("makerAmountFilled", 0))
            taker_amount = int(fill.get("takerAmountFilled", 0))
        except (ValueError, TypeError):
            return None

        if maker_amount <= 0 or taker_amount <= 0:
            return None

        # Identify which token and which side (maker sells token / taker buys token)
        for token_id, side in [(yes_token_id, "yes"), (no_token_id, "no")]:
            if not token_id:
                continue

            # Maker sells tokens for USDC
            if maker_asset == token_id and taker_asset == USDC_ASSET_ID:
                price = (taker_amount / 1e6) / (maker_amount / 1e6)
                usdc_vol = taker_amount / 1e6
                if 0 < price < 1.0:
                    return (side, round(price, 6), round(usdc_vol, 2))

            # Taker buys tokens with USDC
            if taker_asset == token_id and maker_asset == USDC_ASSET_ID:
                price = (maker_amount / 1e6) / (taker_amount / 1e6)
                usdc_vol = maker_amount / 1e6
                if 0 < price < 1.0:
                    return (side, round(price, 6), round(usdc_vol, 2))

        return None

    # ---- Phase 2: CLOB (fallback) ----

    async def _phase_clob(self, db: Database, markets: list[dict]) -> dict:
        sem = asyncio.Semaphore(5)
        success = 0
        empty = 0
        total = len(markets)

        async def fetch_one(mkt):
            nonlocal success, empty
            yes_token = mkt.get("yes_token_id")
            no_token = mkt.get("no_token_id")
            if not yes_token and not no_token:
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

            # Query both YES and NO tokens
            yes_history = {}
            no_history = {}

            for token_id, target in [(yes_token, yes_history), (no_token, no_history)]:
                if not token_id:
                    continue
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
                if data:
                    for pt in data.get("history", []):
                        ts = pt.get("t")
                        price = pt.get("p")
                        if ts is not None and price is not None:
                            target[int(ts)] = float(price)

            if not yes_history and not no_history:
                empty += 1
                done = success + empty
                if done % 200 == 0:
                    log.info("CLOB progress: %d/%d done (%d success, %d empty)", done, total, success, empty)
                return

            # Merge YES and NO by timestamp
            all_ts = sorted(set(yes_history) | set(no_history))
            rows = []
            for ts in all_ts:
                rows.append({
                    "condition_id": mkt["condition_id"],
                    "timestamp": ts,
                    "yes_price": yes_history.get(ts),
                    "no_price": no_history.get(ts),
                    "volume": None,
                    "trade_count": None,
                    "source": "clob",
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
