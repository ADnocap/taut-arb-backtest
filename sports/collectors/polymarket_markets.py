"""Polymarket sports market discovery — Gamma events API for moneyline markets."""

import json
import logging
import re
from datetime import datetime, timezone

import aiohttp

from sports.collectors.base import BaseCollector
from sports.config import (
    GAMMA_BASE_URL,
    POLYMARKET_SEMAPHORE,
    DATE_PATTERNS,
    SPREAD_TOTAL_KEYWORDS,
    SPORTS,
    TEAM_ALIASES,
)
from sports.database import SportsDatabase

log = logging.getLogger(__name__)

# Build a lookup of known team names per sport (alias keys + canonical values, lowercased).
# Used to reject markets whose outcome names don't match any known team.
_KNOWN_TEAMS: dict[str, set[str]] = {}
for _sport, _aliases in TEAM_ALIASES.items():
    _names: set[str] = set()
    for _alias, _canonical in _aliases.items():
        _names.add(_alias.lower())
        _names.add(_canonical.lower())
    _KNOWN_TEAMS[_sport] = _names


class SportsMarketsCollector(BaseCollector):
    """Discover closed binary moneyline sports markets on Polymarket via Gamma events."""

    def __init__(self):
        super().__init__(concurrency=POLYMARKET_SEMAPHORE)

    async def collect(
        self, db: SportsDatabase, sports: list[str],
        *, progress=None, task_id=None,
    ) -> int:
        """Discover markets for given sports. Returns total count saved."""
        total_saved = 0

        for sport_name in sports:
            cfg = SPORTS.get(sport_name)
            if not cfg:
                log.warning("Unknown sport: %s", sport_name)
                continue

            markets = await self._collect_sport(
                sport_name, cfg.gamma_series_ids,
                progress=progress, task_id=task_id,
            )
            log.info("%s: found %d moneyline markets", sport_name, len(markets))

            if markets:
                await db.insert_sports_markets(markets)
                total_saved += len(markets)

        return total_saved

    async def _collect_sport(
        self, sport: str, series_ids: list[int],
        *, progress=None, task_id=None,
    ) -> list[dict]:
        """Paginate Gamma events for each series_id, filter to binary moneyline."""
        seen_cids: set[str] = set()
        markets: list[dict] = []

        for series_id in series_ids:
            sid_markets = await self._paginate_gamma_events(
                sport, series_id, seen_cids,
                progress=progress, task_id=task_id,
            )
            markets.extend(sid_markets)

        return markets

    async def _paginate_gamma_events(
        self, sport: str, series_id: int, seen_cids: set[str],
        *, progress=None, task_id=None,
    ) -> list[dict]:
        """Paginate through Gamma /events API for a single series_id."""
        markets: list[dict] = []
        offset = 0
        page_limit = 100
        page = 0

        while True:
            params = {
                "series_id": series_id,
                "closed": "true",
                "limit": page_limit,
                "offset": offset,
            }

            try:
                data = await self._get(f"{GAMMA_BASE_URL}/events", params=params)
            except aiohttp.ClientResponseError as exc:
                if page > 0:
                    log.warning(
                        "Gamma %s series=%d stopped at page %d (%s), got %d markets",
                        sport, series_id, page, exc, len(markets),
                    )
                    break
                raise
            if not data or not isinstance(data, list) or len(data) == 0:
                break

            page_new = 0
            for event in data:
                event_markets = event.get("markets", [])
                for mkt in event_markets:
                    cid = mkt.get("conditionId") or mkt.get("condition_id", "")
                    if not cid or cid in seen_cids:
                        continue

                    parsed = self._parse_gamma_market(mkt, event, sport)
                    if parsed:
                        seen_cids.add(cid)
                        markets.append(parsed)
                        page_new += 1

            if progress is not None and task_id is not None and page_new:
                progress.update(task_id, advance=page_new)

            if len(data) < page_limit:
                break
            offset += page_limit
            page += 1

            if page % 10 == 0:
                log.info(
                    "Gamma %s series=%d page %d, %d markets so far",
                    sport, series_id, page, len(markets),
                )

        return markets

    def _parse_gamma_market(self, mkt: dict, event: dict, sport: str) -> dict | None:
        """Parse and filter a Gamma market from an event response. Returns dict or None."""
        # --- Primary filter: sportsMarketType ---
        smt = mkt.get("sportsMarketType", "")
        has_smt = bool(smt)

        if has_smt:
            if smt != "moneyline":
                return None
        # else: fallback filtering below for old events without sportsMarketType

        # --- Parse outcomes and token IDs ---
        raw_outcomes = mkt.get("outcomes")
        if isinstance(raw_outcomes, str):
            try:
                outcomes = json.loads(raw_outcomes)
            except (json.JSONDecodeError, TypeError):
                return None
        elif isinstance(raw_outcomes, list):
            outcomes = raw_outcomes
        else:
            return None

        raw_token_ids = mkt.get("clobTokenIds")
        if isinstance(raw_token_ids, str):
            try:
                token_ids = json.loads(raw_token_ids)
            except (json.JSONDecodeError, TypeError):
                return None
        elif isinstance(raw_token_ids, list):
            token_ids = raw_token_ids
        else:
            return None

        # Must be binary
        if len(outcomes) != 2 or len(token_ids) != 2:
            return None

        team_a = str(outcomes[0]).strip()
        team_b = str(outcomes[1]).strip()

        # --- Fallback filtering when sportsMarketType is absent ---
        if not has_smt:
            # Reject Yes/No markets
            if team_a.lower() in ("yes", "no") or team_b.lower() in ("yes", "no"):
                return None

            # Reject spread/total/prop keywords
            question = mkt.get("question", "")
            q_lower = question.lower()
            for kw in SPREAD_TOTAL_KEYWORDS:
                if kw in q_lower:
                    return None

            # Team validation (skip for sports with empty alias dicts like Tennis)
            known = _KNOWN_TEAMS.get(sport)
            if known:
                if team_a.lower() not in known or team_b.lower() not in known:
                    return None

        # --- Parse game_start_time ---
        game_start_time = None
        gst = mkt.get("gameStartTime") or event.get("startDate")
        if gst:
            try:
                s = str(gst).replace(" ", "T")
                # Normalize timezone: "+00" -> "+00:00", "Z" -> "+00:00"
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                elif re.search(r"[+-]\d{2}$", s):
                    s += ":00"
                dt = datetime.fromisoformat(s)
                game_start_time = int(dt.timestamp())
            except (ValueError, TypeError):
                pass

        # Parse game_date from question text as fallback
        question = mkt.get("question", "")
        game_date = self._extract_date(question)

        # If no date in question text, derive from game_start_time
        if game_date is None and game_start_time is not None:
            game_date = datetime.fromtimestamp(game_start_time, tz=timezone.utc).strftime("%Y-%m-%d")

        # --- Determine winner from outcomePrices ---
        outcome = None
        winner = None
        raw_prices = mkt.get("outcomePrices")
        if raw_prices:
            if isinstance(raw_prices, str):
                try:
                    prices = json.loads(raw_prices)
                except (json.JSONDecodeError, TypeError):
                    prices = []
            elif isinstance(raw_prices, list):
                prices = raw_prices
            else:
                prices = []

            if len(prices) == 2:
                try:
                    p0 = float(prices[0])
                    p1 = float(prices[1])
                    if p0 >= 0.95:
                        winner = team_a
                        outcome = team_a
                    elif p1 >= 0.95:
                        winner = team_b
                        outcome = team_b
                except (ValueError, TypeError):
                    pass

        condition_id = mkt.get("conditionId") or mkt.get("condition_id", "")

        return {
            "condition_id": condition_id,
            "sport": sport,
            "question": question,
            "team_a": team_a,
            "team_b": team_b,
            "game_date": game_date,
            "game_start_time": game_start_time,
            "token_a_id": str(token_ids[0]),
            "token_b_id": str(token_ids[1]),
            "winner": winner,
            "outcome": outcome,
        }

    def _extract_date(self, text: str) -> str | None:
        """Try to extract a game date from question text."""
        for pat in DATE_PATTERNS:
            m = pat.search(text)
            if m:
                raw = m.group(1)
                try:
                    # Try ISO format first
                    dt = datetime.strptime(raw, "%Y-%m-%d")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
                try:
                    # "January 15, 2025" / "Jan 15, 2025"
                    for fmt in ["%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"]:
                        try:
                            dt = datetime.strptime(raw.replace(",", "").strip(), fmt)
                            return dt.strftime("%Y-%m-%d")
                        except ValueError:
                            continue
                except Exception:
                    pass
                try:
                    # "1/15/2025"
                    dt = datetime.strptime(raw, "%m/%d/%Y")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
        return None
