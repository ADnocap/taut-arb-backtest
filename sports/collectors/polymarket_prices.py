"""Polymarket sports price history — Goldsky primary + CLOB fallback (15-min)."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sports.collectors.base import BaseCollector
from sports.config import (
    CLOB_BASE_URL,
    GOLDSKY_EARLIEST_TIMESTAMP,
    GOLDSKY_PAGE_SIZE,
    GOLDSKY_RATE_BURST,
    GOLDSKY_RATE_LIMIT,
    GOLDSKY_RATE_WINDOW,
    GOLDSKY_URL,
    POLYMARKET_SEMAPHORE,
    SPORTS_GOLDSKY_SEMAPHORE,
    SPORTS_PRICE_FIDELITY,
    SPORTS_PRICE_LOOKBACK_DAYS,
    USDC_ASSET_ID,
)
from sports.database import SportsDatabase

log = logging.getLogger(__name__)

# 15-min bucket in seconds
BUCKET_SECS = SPORTS_PRICE_FIDELITY * 60


class _RateLimiter:
    """Token-bucket rate limiter for Goldsky requests."""

    def __init__(self, rate: float, window: float, burst: int):
        self._interval = window / rate  # seconds per token
        self._max_tokens = burst
        self._tokens = float(burst)
        self._lock = asyncio.Lock()
        self._last_refill: float | None = None

    async def acquire(self):
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            if self._last_refill is None:
                self._last_refill = now
            else:
                elapsed = now - self._last_refill
                self._tokens = min(
                    self._max_tokens, self._tokens + elapsed / self._interval
                )
                self._last_refill = now

            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) * self._interval
                self._last_refill += wait
                self._tokens = 0.0
                await asyncio.sleep(wait)
            else:
                self._tokens -= 1.0


class SportsPricesCollector(BaseCollector):
    """Two-phase price collection for sports markets: Goldsky + CLOB."""

    def __init__(self):
        super().__init__(concurrency=POLYMARKET_SEMAPHORE)

    async def collect(
        self, db: SportsDatabase, sport: str | None = None,
        *, progress=None, task_id=None,
    ) -> dict:
        """Collect prices for sports markets. Returns stats."""
        markets = await db.get_markets_missing_prices(sport)
        if not markets:
            log.info("No markets missing prices (sport=%s)", sport)
            return {"goldsky_success": 0, "clob_fallback": 0, "no_data": 0}

        # Phase 1: Goldsky for eligible markets
        eligible = [m for m in markets if self._is_goldsky_eligible(m)]
        log.info(
            "Phase 1: Goldsky for %d/%d markets (sport=%s)",
            len(eligible), len(markets), sport,
        )
        goldsky_count = 0
        if eligible:
            goldsky_count = await self._phase_goldsky(
                db, eligible, progress=progress, task_id=task_id,
            )

        # Sync progress bar between phases
        still_missing = await db.get_markets_missing_prices(sport)
        if progress is not None and task_id is not None:
            progress.update(task_id, completed=len(markets) - len(still_missing))

        # Phase 2: CLOB fallback for remaining
        log.info(
            "Phase 2: CLOB fallback for %d markets (sport=%s)",
            len(still_missing), sport,
        )
        clob_count = 0
        if still_missing:
            clob_count = await self._phase_clob(
                db, still_missing, progress=progress, task_id=task_id,
            )

        if progress is not None and task_id is not None:
            progress.update(task_id, completed=len(markets))

        final_missing = await db.get_markets_missing_prices(sport)
        stats = {
            "goldsky_success": goldsky_count,
            "clob_fallback": clob_count,
            "no_data": len(final_missing),
        }
        log.info("Price stats (sport=%s): %s", sport, stats)
        return stats

    def _is_goldsky_eligible(self, market: dict) -> bool:
        gst = market.get("game_start_time")
        if gst and int(gst) >= GOLDSKY_EARLIEST_TIMESTAMP:
            return True
        gd = market.get("game_date")
        if gd:
            try:
                dt = datetime.strptime(gd, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                return dt.timestamp() >= GOLDSKY_EARLIEST_TIMESTAMP
            except ValueError:
                pass
        return False

    def _get_time_window(self, market: dict) -> tuple[int, int]:
        """Return (start_ts, end_ts) for price lookback."""
        end_ts = None
        gst = market.get("game_start_time")
        if gst:
            end_ts = int(gst)
        if not end_ts:
            gd = market.get("game_date")
            if gd:
                try:
                    dt = datetime.strptime(gd, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    end_ts = int(dt.timestamp()) + 86400  # end of day
                except ValueError:
                    pass
        if not end_ts:
            end_ts = int(datetime.now(timezone.utc).timestamp())

        start_ts = end_ts - (SPORTS_PRICE_LOOKBACK_DAYS * 86400)
        return start_ts, end_ts

    # ---- Phase 1: Goldsky ----

    async def _phase_goldsky(
        self, db: SportsDatabase, markets: list[dict],
        *, progress=None, task_id=None,
    ) -> int:
        backfilled = 0
        done = 0
        total = len(markets)
        queue: asyncio.Queue = asyncio.Queue()
        for m in markets:
            queue.put_nowait(m)
        limiter = _RateLimiter(GOLDSKY_RATE_LIMIT, GOLDSKY_RATE_WINDOW, GOLDSKY_RATE_BURST)

        async def worker():
            nonlocal backfilled, done
            while not queue.empty():
                try:
                    mkt = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                token_a = mkt.get("token_a_id", "")
                token_b = mkt.get("token_b_id", "")
                token_ids = [t for t in [token_a, token_b] if t]
                if not token_ids:
                    done += 1
                    if progress is not None and task_id is not None:
                        progress.update(task_id, advance=1)
                    continue

                start_ts, _ = self._get_time_window(mkt)
                start_ts = max(start_ts, GOLDSKY_EARLIEST_TIMESTAMP)

                fills = await self._query_goldsky_fills(token_ids, start_ts, limiter)
                if not fills:
                    done += 1
                    if progress is not None and task_id is not None:
                        progress.update(task_id, advance=1)
                    continue

                # VWAP bucketing into 15-min intervals
                buckets: dict[int, dict] = {}
                for fill in fills:
                    result = self._calc_fill_price(fill, token_a, token_b)
                    if not result:
                        continue
                    side, price, usdc_vol = result
                    ts = int(fill["timestamp"])
                    bucket = (ts // BUCKET_SECS) * BUCKET_SECS

                    if bucket not in buckets:
                        buckets[bucket] = {
                            "a_pv": 0.0, "a_vol": 0.0,
                            "b_pv": 0.0, "b_vol": 0.0,
                        }

                    b = buckets[bucket]
                    if side == "a":
                        b["a_pv"] += price * usdc_vol
                        b["a_vol"] += usdc_vol
                    else:
                        b["b_pv"] += price * usdc_vol
                        b["b_vol"] += usdc_vol

                rows = []
                for ts in sorted(buckets):
                    b = buckets[ts]
                    a_price = round(b["a_pv"] / b["a_vol"], 6) if b["a_vol"] > 0 else None
                    b_price = round(b["b_pv"] / b["b_vol"], 6) if b["b_vol"] > 0 else None
                    rows.append({
                        "condition_id": mkt["condition_id"],
                        "timestamp": ts,
                        "team_a_price": a_price,
                        "team_b_price": b_price,
                        "source": "goldsky",
                    })

                if rows:
                    await db.insert_sports_prices(rows)
                    backfilled += 1

                done += 1
                if progress is not None and task_id is not None:
                    progress.update(task_id, advance=1)
                if done % 50 == 0:
                    log.info("Goldsky progress: %d/%d (%d backfilled)", done, total, backfilled)

        workers = [asyncio.create_task(worker()) for _ in range(SPORTS_GOLDSKY_SEMAPHORE)]
        results = await asyncio.gather(*workers, return_exceptions=True)
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            log.warning("Goldsky: %d exceptions", len(errors))
            for exc in errors[:3]:
                log.warning("  %s: %s", type(exc).__name__, exc)
        return backfilled

    async def _query_goldsky_fills(
        self, token_ids: list[str], start_ts: int, limiter: _RateLimiter,
    ) -> list[dict]:
        all_fills = []
        cursor_ts = str(start_ts)
        cursor_id = None
        sticky = False

        for _ in range(200):
            if sticky and cursor_id:
                where = self._where_id(token_ids, cursor_id)
            else:
                where = self._where_ts(token_ids, cursor_ts)

            query = f"""{{
  orderFilledEvents(
    first: {GOLDSKY_PAGE_SIZE}
    orderBy: timestamp
    orderDirection: asc
    where: {where}
  ) {{
    id
    timestamp
    makerAmountFilled
    takerAmountFilled
    makerAssetId
    takerAssetId
  }}
}}"""

            await limiter.acquire()
            resp = await self._post(
                GOLDSKY_URL,
                json={"query": query},
                headers={"Content-Type": "application/json"},
            )

            if not resp:
                break

            fills = resp.get("data", {}).get("orderFilledEvents", [])
            if not fills:
                break

            all_fills.extend(fills)
            if len(fills) < GOLDSKY_PAGE_SIZE:
                break

            timestamps = {f["timestamp"] for f in fills}
            if len(timestamps) == 1:
                sticky = True
                cursor_id = fills[-1]["id"]
            else:
                sticky = False
                cursor_ts = fills[-1]["timestamp"]
                cursor_id = None

        return all_fills

    def _where_ts(self, token_ids: list[str], ts: str) -> str:
        ids = ", ".join(f'"{t}"' for t in token_ids)
        return (
            f'{{ or: ['
            f'{{ timestamp_gt: "{ts}", makerAssetId_in: [{ids}] }}, '
            f'{{ timestamp_gt: "{ts}", takerAssetId_in: [{ids}] }}'
            f'] }}'
        )

    def _where_id(self, token_ids: list[str], id_cursor: str) -> str:
        ids = ", ".join(f'"{t}"' for t in token_ids)
        return (
            f'{{ or: ['
            f'{{ id_gt: "{id_cursor}", makerAssetId_in: [{ids}] }}, '
            f'{{ id_gt: "{id_cursor}", takerAssetId_in: [{ids}] }}'
            f'] }}'
        )

    def _calc_fill_price(
        self, fill: dict, token_a_id: str, token_b_id: str
    ) -> tuple[str, float, float] | None:
        """Returns ("a"|"b", price, usdc_volume) or None."""
        maker_asset = fill.get("makerAssetId", "")
        taker_asset = fill.get("takerAssetId", "")
        try:
            maker_amount = int(fill.get("makerAmountFilled", 0))
            taker_amount = int(fill.get("takerAmountFilled", 0))
        except (ValueError, TypeError):
            return None

        if maker_amount <= 0 or taker_amount <= 0:
            return None

        for token_id, side in [(token_a_id, "a"), (token_b_id, "b")]:
            if not token_id:
                continue
            if maker_asset == token_id and taker_asset == USDC_ASSET_ID:
                price = (taker_amount / 1e6) / (maker_amount / 1e6)
                usdc_vol = taker_amount / 1e6
                if 0 < price < 1.0:
                    return (side, round(price, 6), round(usdc_vol, 2))
            if taker_asset == token_id and maker_asset == USDC_ASSET_ID:
                price = (maker_amount / 1e6) / (taker_amount / 1e6)
                usdc_vol = maker_amount / 1e6
                if 0 < price < 1.0:
                    return (side, round(price, 6), round(usdc_vol, 2))

        return None

    # ---- Phase 2: CLOB fallback ----

    async def _phase_clob(
        self, db: SportsDatabase, markets: list[dict],
        *, progress=None, task_id=None,
    ) -> int:
        sem = asyncio.Semaphore(POLYMARKET_SEMAPHORE)
        success = 0
        done = 0
        total = len(markets)

        async def fetch_one(mkt):
            nonlocal success, done
            token_a = mkt.get("token_a_id")
            token_b = mkt.get("token_b_id")
            if not token_a and not token_b:
                done += 1
                return

            start_ts, end_ts = self._get_time_window(mkt)

            a_history = {}
            b_history = {}

            for token_id, target in [(token_a, a_history), (token_b, b_history)]:
                if not token_id:
                    continue
                params = {
                    "market": token_id,
                    "startTs": start_ts,
                    "endTs": end_ts,
                    "fidelity": SPORTS_PRICE_FIDELITY,
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

            if not a_history and not b_history:
                done += 1
                return

            all_ts = sorted(set(a_history) | set(b_history))
            rows = []
            for ts in all_ts:
                rows.append({
                    "condition_id": mkt["condition_id"],
                    "timestamp": ts,
                    "team_a_price": a_history.get(ts),
                    "team_b_price": b_history.get(ts),
                    "source": "clob",
                })

            if rows:
                await db.insert_sports_prices(rows)
                success += 1

            done += 1
            if progress is not None and task_id is not None:
                progress.update(task_id, advance=1)
            if done % 100 == 0:
                log.info("CLOB progress: %d/%d (%d success)", done, total, success)

        tasks = [fetch_one(m) for m in markets]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            log.warning("CLOB: %d exceptions", len(errors))
            for exc in errors[:3]:
                log.warning("  %s: %s", type(exc).__name__, exc)
        return success
