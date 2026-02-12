"""Deribit options trade collector with IV normalization."""

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

from collectors.base import BaseCollector
from config import (
    ASSETS,
    DERIBIT_HISTORY_URL,
    DERIBIT_MAX_PAGES_PER_DAY,
    DERIBIT_SEMAPHORE,
    DERIBIT_TRADE_COUNT,
    DEFAULT_COLLECTION_START,
)
from database import Database

log = logging.getLogger(__name__)

# Month abbreviation map for instrument name parsing
_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Instrument pattern: BTC-25SEP20-6000-C or SOL_USDC-25SEP20-150-C
_INSTRUMENT_RE = re.compile(
    r"^(\w+)-(\d{1,2})([A-Z]{3})(\d{2,4})-(\d+(?:\.\d+)?)-([CP])$"
)


def _parse_instrument(name: str) -> dict | None:
    """Parse option instrument name into components."""
    m = _INSTRUMENT_RE.match(name)
    if not m:
        return None
    prefix, day, mon_str, year_str, strike_str, opt_type = m.groups()

    month = _MONTH_MAP.get(mon_str)
    if not month:
        return None

    year = int(year_str)
    if year < 100:
        year += 2000

    try:
        expiry = datetime(year, month, int(day), 8, 0, 0, tzinfo=timezone.utc)
    except ValueError:
        return None

    # Determine asset from prefix
    asset = prefix.split("_")[0]  # "SOL_USDC" -> "SOL"

    return {
        "asset": asset,
        "expiry": expiry.isoformat(),
        "strike": float(strike_str),
        "option_type": opt_type,
    }


def _normalize_iv(iv_raw) -> float | None:
    """Normalize IV: Deribit returns percentage (74.74 = 74.74%). Convert to decimal."""
    if iv_raw is None:
        return None
    iv = float(iv_raw)
    if iv <= 0:
        return None
    if iv > 5.0:
        iv = iv / 100.0
    if iv > 5.0 or iv <= 0:
        return None
    return round(iv, 6)


class DeribitOptionsCollector(BaseCollector):
    """Day-by-day options trade collection with IV normalization."""

    def __init__(self):
        super().__init__(concurrency=DERIBIT_SEMAPHORE)

    async def collect(
        self,
        db: Database,
        asset: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> int:
        """Collect option trades for an asset. Returns total trades saved."""
        cfg = ASSETS[asset]
        currency = cfg.deribit_currency

        if start_date is None:
            start_date = DEFAULT_COLLECTION_START
        if end_date is None:
            end_date = datetime.now(timezone.utc)

        # Resume from last saved trade
        latest_ts = await db.get_latest_option_trade_timestamp(asset)
        if latest_ts:
            resume_dt = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("Resuming %s options from %s", asset, start_date.date())

        # Build list of days
        days = []
        d = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        while d < end_date:
            days.append(d)
            d += timedelta(days=1)

        log.info("Collecting %s options: %d days (%s to %s)",
                 asset, len(days), days[0].date() if days else "?",
                 days[-1].date() if days else "?")

        total_saved = 0
        # Process in batches of 10 concurrent days
        batch_size = DERIBIT_SEMAPHORE
        for i in range(0, len(days), batch_size):
            batch = days[i : i + batch_size]
            results = await asyncio.gather(
                *[self._collect_day(db, asset, currency, day) for day in batch],
                return_exceptions=True,
            )
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    log.error("Error on %s: %s", batch[j].date(), result)
                else:
                    total_saved += result

            log.info("Options progress: %d/%d days, %d trades saved",
                     min(i + batch_size, len(days)), len(days), total_saved)

        log.info("Options done: %d trades saved for %s", total_saved, asset)
        return total_saved

    async def _collect_day(
        self, db: Database, asset: str, currency: str, day: datetime
    ) -> int:
        """Collect all option trades for a single day."""
        start_ms = int(day.timestamp() * 1000)
        end_ms = int((day + timedelta(days=1)).timestamp() * 1000)

        all_trades = []
        start_seq = None

        for page in range(DERIBIT_MAX_PAGES_PER_DAY):
            params = {
                "currency": currency,
                "kind": "option",
                "start_timestamp": start_ms,
                "end_timestamp": end_ms,
                "count": DERIBIT_TRADE_COUNT,
                "sorting": "asc",
            }
            if start_seq is not None:
                params["start_seq"] = start_seq

            resp = await self._get(
                f"{DERIBIT_HISTORY_URL}/get_last_trades_by_currency_and_time",
                params=params,
            )
            if not resp:
                break

            result = resp.get("result", {})
            trades = result.get("trades", [])
            if not trades:
                break

            for trade in trades:
                parsed = self._parse_trade(trade, asset)
                if parsed:
                    all_trades.append(parsed)

            has_more = result.get("has_more", False)
            if not has_more or len(trades) < DERIBIT_TRADE_COUNT:
                break

            start_seq = trades[-1].get("trade_seq", 0) + 1

        if all_trades:
            await db.insert_option_trades(all_trades)
        return len(all_trades)

    def _parse_trade(self, trade: dict, target_asset: str) -> dict | None:
        """Parse and validate a single option trade."""
        name = trade.get("instrument_name", "")
        parsed = _parse_instrument(name)
        if not parsed:
            return None

        # For USDC currency, filter by target asset prefix
        if parsed["asset"] != target_asset:
            return None

        iv = _normalize_iv(trade.get("iv"))
        if iv is None:
            return None

        index_price = trade.get("index_price")
        if not index_price or index_price <= 0:
            return None

        # Strike sanity check: within 3x spot
        if parsed["strike"] > index_price * 3 or parsed["strike"] < index_price / 3:
            return None

        return {
            "timestamp": trade.get("timestamp"),
            "instrument_name": name,
            "asset": parsed["asset"],
            "strike": parsed["strike"],
            "expiry": parsed["expiry"],
            "option_type": parsed["option_type"],
            "iv": iv,
            "mark_price": trade.get("mark_price", 0),
            "index_price": index_price,
            "trade_price": trade.get("price", 0),
            "amount": trade.get("amount"),
        }
