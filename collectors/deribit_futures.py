"""Deribit dated futures trade collector (perpetuals excluded)."""

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

_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Dated future: BTC-25SEP20 or SOL_USDC-31JAN25
_FUTURE_RE = re.compile(r"^(\w+)-(\d{1,2})([A-Z]{3})(\d{2,4})$")


def _parse_future_expiry(name: str) -> tuple[str, int] | None:
    """Parse dated future instrument. Returns (asset, expiry_ms) or None.

    Returns None for perpetuals.
    """
    if "PERPETUAL" in name.upper():
        return None

    m = _FUTURE_RE.match(name)
    if not m:
        return None

    prefix, day, mon_str, year_str = m.groups()
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

    asset = prefix.split("_")[0]
    return (asset, int(expiry.timestamp() * 1000))


class DeribitFuturesCollector(BaseCollector):
    """Collect dated futures trades (perpetuals filtered out)."""

    def __init__(self):
        super().__init__(concurrency=DERIBIT_SEMAPHORE)

    async def collect(
        self,
        db: Database,
        asset: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> int:
        cfg = ASSETS[asset]
        currency = cfg.deribit_currency

        if start_date is None:
            start_date = DEFAULT_COLLECTION_START
        if end_date is None:
            end_date = datetime.now(timezone.utc)

        # Resume
        latest_ts = await db.get_latest_futures_timestamp(asset)
        if latest_ts:
            resume_dt = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("Resuming %s futures from %s", asset, start_date.date())

        days = []
        d = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        while d < end_date:
            days.append(d)
            d += timedelta(days=1)

        log.info("Collecting %s futures: %d days", asset, len(days))

        total_saved = 0
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

            log.info("Futures progress: %d/%d days, %d trades saved",
                     min(i + batch_size, len(days)), len(days), total_saved)

        log.info("Futures done: %d trades saved for %s", total_saved, asset)
        return total_saved

    async def _collect_day(
        self, db: Database, asset: str, currency: str, day: datetime
    ) -> int:
        start_ms = int(day.timestamp() * 1000)
        end_ms = int((day + timedelta(days=1)).timestamp() * 1000)

        all_trades = []
        start_seq = None

        for page in range(DERIBIT_MAX_PAGES_PER_DAY):
            params = {
                "currency": currency,
                "kind": "future",
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
            await db.insert_futures(all_trades)
        return len(all_trades)

    def _parse_trade(self, trade: dict, target_asset: str) -> dict | None:
        name = trade.get("instrument_name", "")
        parsed = _parse_future_expiry(name)
        if not parsed:
            return None

        trade_asset, expiry_ms = parsed
        if trade_asset != target_asset:
            return None

        return {
            "timestamp": trade.get("timestamp"),
            "asset": trade_asset,
            "instrument_name": name,
            "expiry_date": expiry_ms,
            "mark_price": trade.get("mark_price"),
            "delivery_price": trade.get("delivery_price"),
            "index_price": trade.get("index_price"),
        }
