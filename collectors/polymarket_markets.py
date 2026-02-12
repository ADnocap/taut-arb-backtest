"""Polymarket market discovery via CLOB + Gamma APIs."""

import logging
from datetime import datetime, timezone

import aiohttp

from classifier import classify_market, determine_outcome, parse_settlement_date
from collectors.base import BaseCollector
from config import (
    CLOB_BASE_URL,
    CLOB_PAGE_LIMIT,
    GAMMA_BASE_URL,
    GAMMA_PAGE_LIMIT,
    DEFAULT_COLLECTION_START,
    POLYMARKET_SEMAPHORE,
)
from database import Database

log = logging.getLogger(__name__)


class PolymarketMarketsCollector(BaseCollector):
    """Collect closed crypto markets from Polymarket CLOB + Gamma APIs."""

    def __init__(self):
        super().__init__(concurrency=POLYMARKET_SEMAPHORE)

    async def collect(
        self,
        db: Database,
        assets: list[str],
        start_date: datetime | None = None,
    ) -> int:
        """Discover and classify markets. Returns count of markets saved."""
        if start_date is None:
            start_date = DEFAULT_COLLECTION_START

        log.info("Collecting Polymarket markets for assets=%s", assets)

        # Phase 1: CLOB closed markets
        clob_markets = await self._collect_clob(assets)
        log.info("CLOB: found %d classified markets", len(clob_markets))

        # Phase 2: Gamma supplementary data (for resolvedTo)
        clob_cids = {m["condition_id"] for m in clob_markets}
        gamma_data = await self._collect_gamma_lookup(clob_cids)
        log.info("Gamma: fetched %d market records", len(gamma_data))

        # Merge Gamma outcome info
        for mkt in clob_markets:
            cid = mkt["condition_id"]
            if cid in gamma_data:
                if mkt["outcome"] is None:
                    resolved = gamma_data[cid]
                    if resolved == "Yes":
                        mkt["outcome"] = 1
                    elif resolved == "No":
                        mkt["outcome"] = 0

        # Filter by start_date
        start_ts = start_date.isoformat()
        filtered = []
        for mkt in clob_markets:
            sd = mkt.get("settlement_date")
            if sd and sd >= start_ts:
                filtered.append(mkt)
            elif not sd:
                filtered.append(mkt)

        log.info("After date filter: %d markets", len(filtered))
        await db.insert_markets(filtered)
        return len(filtered)

    async def _collect_clob(self, assets: list[str]) -> list[dict]:
        """Paginate through CLOB API, classify each market."""
        markets = []
        cursor = None
        seen_cursors = set()
        page = 0

        while True:
            params = {"limit": CLOB_PAGE_LIMIT}
            if cursor:
                params["next_cursor"] = cursor

            try:
                data = await self._get(f"{CLOB_BASE_URL}/markets", params=params)
            except aiohttp.ClientResponseError as exc:
                if page > 0:
                    log.warning(
                        "CLOB pagination stopped at page %d (%s), "
                        "returning %d markets collected so far",
                        page, exc, len(markets),
                    )
                    break
                raise
            if not data:
                break

            items = data.get("data", [])
            if not items:
                break

            for item in items:
                # Only closed markets
                if not item.get("closed"):
                    continue

                question = item.get("question", "")
                classification = classify_market(question, target_assets=assets)
                if not classification:
                    continue

                # Extract token IDs
                tokens = item.get("tokens", [])
                yes_token_id = None
                no_token_id = None
                for tok in tokens:
                    if tok.get("outcome") == "Yes":
                        yes_token_id = tok.get("token_id")
                    elif tok.get("outcome") == "No":
                        no_token_id = tok.get("token_id")

                outcome = determine_outcome(item)
                settlement = parse_settlement_date(item)

                # Current prices
                yes_price = None
                no_price = None
                for tok in tokens:
                    if tok.get("outcome") == "Yes":
                        yes_price = tok.get("price")
                    elif tok.get("outcome") == "No":
                        no_price = tok.get("price")

                markets.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "condition_id": item.get("condition_id", ""),
                    "question": question,
                    "asset": classification["asset"],
                    "threshold": classification["threshold"],
                    "direction": classification["direction"],
                    "upper_threshold": classification.get("upper_threshold"),
                    "settlement_date": settlement,
                    "yes_price": float(yes_price) if yes_price else None,
                    "no_price": float(no_price) if no_price else None,
                    "yes_token_id": yes_token_id,
                    "no_token_id": no_token_id,
                    "volume": item.get("volume"),
                    "outcome": outcome,
                })

            new_cursor = data.get("next_cursor")
            if not new_cursor or new_cursor == "LTE=" or new_cursor in seen_cursors:
                break
            seen_cursors.add(new_cursor)
            cursor = new_cursor
            page += 1

            if page % 10 == 0:
                log.info("CLOB page %d, %d markets so far", page, len(markets))

        return markets

    async def _collect_gamma_lookup(self, target_ids: set[str]) -> dict[str, str | None]:
        """Fetch Gamma API data for outcome resolution.

        Only stores records whose condition_id is in *target_ids*,
        and only keeps the resolvedTo value (not the full JSON object).
        Returns {condition_id: resolvedTo_string_or_None}.
        """
        lookup: dict[str, str | None] = {}
        offset = 0
        page = 0

        while True:
            params = {
                "closed": "true",
                "limit": GAMMA_PAGE_LIMIT,
                "offset": offset,
            }
            data = await self._get(f"{GAMMA_BASE_URL}/markets", params=params)
            if not data or not isinstance(data, list) or len(data) == 0:
                break

            for item in data:
                cid = item.get("conditionId") or item.get("condition_id")
                if cid and cid in target_ids:
                    lookup[cid] = item.get("resolvedTo")

            page += 1
            if page % 50 == 0:
                log.info(
                    "Gamma page %d (offset %d), matched %d/%d target markets",
                    page, offset, len(lookup), len(target_ids),
                )

            # Early exit: all target markets found
            if len(lookup) >= len(target_ids):
                log.info(
                    "Gamma: all %d target markets matched at page %d, stopping early",
                    len(target_ids), page,
                )
                break

            if len(data) < GAMMA_PAGE_LIMIT:
                break
            offset += GAMMA_PAGE_LIMIT

        return lookup
