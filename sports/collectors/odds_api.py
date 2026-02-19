"""The Odds API historical odds collector — targeted by Polymarket market dates."""

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv

from sports.collectors.base import BaseCollector
from sports.config import (
    ODDS_API_BASE,
    ODDS_CREDITS_PER_CALL,
    ODDS_CREDIT_BUDGET,
    SPORTS,
    SPORTS_PRICE_LOOKBACK_DAYS,
)
from sports.database import SportsDatabase

log = logging.getLogger(__name__)

# Query every 30 min for full coverage of the 15-min price grid
QUERY_OFFSETS_MINUTES = list(range(0, 1440, 30))  # [0, 30, 60, ..., 1410]

# Load API key from .env
load_dotenv()
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")


class OddsApiCollector(BaseCollector):
    """Collect historical odds from The Odds API, targeted by Polymarket dates."""

    def __init__(self):
        super().__init__(concurrency=20)
        self._credits_used = 0

    async def collect(
        self, db: SportsDatabase, sports: list[str],
        *, progress=None, task_id=None,
    ) -> dict:
        """Collect odds for all sports. Returns per-sport stats."""
        if not ODDS_API_KEY:
            log.error("ODDS_API_KEY not found in .env — skipping odds collection")
            return {}

        # Load running credit total from DB
        self._credits_used = await db.get_total_credits_used()
        log.info("Starting credits used so far: %d / %d", self._credits_used, ODDS_CREDIT_BUDGET)

        # Pre-compute all query_dates so we can set total for progress bar
        sport_queries: list[tuple[str, list[tuple[str, str]]]] = []
        total_queries = 0
        for sport_name in sports:
            cfg = SPORTS.get(sport_name)
            if not cfg:
                continue
            game_dates = await db.get_unique_game_dates_for_sport(sport_name)
            if not game_dates:
                log.info("%s: no markets found, skipping odds", sport_name)
                sport_queries.append((sport_name, []))
                continue
            query_dates = self._build_query_dates(game_dates, cfg.odds_api_sport_keys)
            log.info(
                "%s: %d market dates -> %d API date queries across %d sport keys",
                sport_name, len(game_dates), len(query_dates), len(cfg.odds_api_sport_keys),
            )
            sport_queries.append((sport_name, query_dates))
            total_queries += len(query_dates)

        if progress is not None and task_id is not None:
            progress.update(task_id, total=total_queries)

        stats = {}
        for sport_name, query_dates in sport_queries:
            if not query_dates:
                stats[sport_name] = 0
                continue
            count = await self._collect_sport(
                db, sport_name, query_dates,
                progress=progress, task_id=task_id,
            )
            stats[sport_name] = count

        return stats

    def _build_query_dates(
        self, game_dates: list[str], sport_keys: list[str]
    ) -> list[tuple[str, str]]:
        """Build (sport_key, iso_timestamp) pairs for API queries.

        For each game_date, query from game_date - lookback to game_date,
        every 30 minutes (48x/day) for full coverage of the 15-min price grid.
        Deduplicate across overlapping windows.
        """
        all_timestamps: set[str] = set()
        for gd in game_dates:
            try:
                dt = datetime.strptime(gd, "%Y-%m-%d")
            except ValueError:
                continue
            for d in range(SPORTS_PRICE_LOOKBACK_DAYS + 1):
                query_dt = dt - timedelta(days=d)
                for m in QUERY_OFFSETS_MINUTES:
                    all_timestamps.add(
                        query_dt.replace(hour=m // 60, minute=m % 60).strftime("%Y-%m-%dT%H:%M:%SZ")
                    )

        # Cross product: each sport_key x each timestamp
        pairs = []
        for sk in sport_keys:
            for ts in sorted(all_timestamps):
                pairs.append((sk, ts))
        return pairs

    async def _collect_sport(
        self,
        db: SportsDatabase,
        sport_name: str,
        query_dates: list[tuple[str, str]],
        *,
        progress=None,
        task_id=None,
    ) -> int:
        """Collect odds for a sport using concurrent workers. Returns total snapshot rows inserted."""
        done = await db.get_completed_odds_queries(sport_name)
        pending = [(sk, ds) for sk, ds in query_dates if (sk, ds) not in done]
        skipped = len(query_dates) - len(pending)

        if skipped:
            log.info("%s: skipped %d already-fetched queries (resume)", sport_name, skipped)
            if progress is not None and task_id is not None:
                progress.update(task_id, advance=skipped)

        total_rows = 0
        lock = asyncio.Lock()
        budget_exhausted = False

        async def _worker(sport_key: str, date_str: str):
            nonlocal total_rows, budget_exhausted

            async with lock:
                if budget_exhausted or self._credits_used + ODDS_CREDITS_PER_CALL > ODDS_CREDIT_BUDGET:
                    budget_exhausted = True
                    log.warning(
                        "Credit budget exhausted (%d/%d), stopping",
                        self._credits_used, ODDS_CREDIT_BUDGET,
                    )
                    return

            rows = await self._fetch_historical_odds(sport_name, sport_key, date_str)

            ts_now = int(time.time())
            async with lock:
                if rows:
                    await db.insert_odds_snapshots(rows)
                    total_rows += len(rows)
                self._credits_used += ODDS_CREDITS_PER_CALL

            await db.log_credits(
                sport_name, ODDS_CREDITS_PER_CALL, ts_now,
                sport_key=sport_key, date_str=date_str,
            )

            if progress is not None and task_id is not None:
                progress.update(task_id, advance=1)

        tasks = [asyncio.create_task(_worker(sk, ds)) for sk, ds in pending]
        await asyncio.gather(*tasks)

        log.info(
            "%s: finished with %d snapshot rows, %d total credits",
            sport_name, total_rows, self._credits_used,
        )
        return total_rows

    async def _fetch_historical_odds(
        self, sport_name: str, sport_key: str, date_str: str
    ) -> list[dict]:
        """Fetch one historical odds snapshot for a sport+timestamp.

        date_str is a full ISO timestamp like '2025-06-01T06:00:00Z'.
        Returns list of odds_snapshots rows.
        """
        params = {
            "apiKey": ODDS_API_KEY,
            "date": date_str,
            "regions": "eu",
            "bookmakers": "pinnacle",
            "markets": "h2h",
            "oddsFormat": "decimal",
        }

        try:
            data = await self._get(
                f"{ODDS_API_BASE}/historical/sports/{sport_key}/odds",
                params=params,
            )
        except Exception as exc:
            log.warning(
                "Odds API error for %s on %s: %s", sport_key, date_str, exc
            )
            return []

        if not data:
            return []

        # The response wraps events in a "data" key
        events = data.get("data", [])
        if not events:
            # Some endpoints return the list directly
            if isinstance(data, list):
                events = data
            else:
                return []

        snapshot_ts_str = data.get("timestamp")
        if snapshot_ts_str:
            try:
                snapshot_ts = int(
                    datetime.fromisoformat(
                        snapshot_ts_str.replace("Z", "+00:00")
                    ).timestamp()
                )
            except (ValueError, TypeError):
                snapshot_ts = int(
                    datetime.fromisoformat(
                        date_str.replace("Z", "+00:00")
                    ).timestamp()
                )
        else:
            snapshot_ts = int(
                datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).timestamp()
            )

        rows = []
        for event in events:
            event_id = event.get("id", "")
            home_team = event.get("home_team", "")
            away_team = event.get("away_team", "")
            commence_time_str = event.get("commence_time", "")

            commence_ts = None
            if commence_time_str:
                try:
                    commence_ts = int(
                        datetime.fromisoformat(
                            commence_time_str.replace("Z", "+00:00")
                        ).timestamp()
                    )
                except (ValueError, TypeError):
                    pass

            bookmakers = event.get("bookmakers", [])
            for bm in bookmakers:
                bm_key = bm.get("key", "")
                markets = bm.get("markets", [])
                for mkt in markets:
                    if mkt.get("key") != "h2h":
                        continue

                    outcomes = mkt.get("outcomes", [])
                    home_odds = None
                    away_odds = None
                    draw_odds = None

                    for oc in outcomes:
                        name = oc.get("name", "")
                        price = oc.get("price")
                        if price is None:
                            continue
                        if name == home_team:
                            home_odds = float(price)
                        elif name == away_team:
                            away_odds = float(price)
                        elif name.lower() == "draw":
                            draw_odds = float(price)

                    if home_odds is not None and away_odds is not None:
                        rows.append({
                            "odds_event_id": event_id,
                            "sport": sport_name,
                            "home_team": home_team,
                            "away_team": away_team,
                            "commence_time": commence_ts,
                            "snapshot_ts": snapshot_ts,
                            "home_odds": home_odds,
                            "away_odds": away_odds,
                            "draw_odds": draw_odds,
                            "bookmaker": bm_key,
                        })

        return rows
