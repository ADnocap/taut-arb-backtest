"""Deribit funding rate collector (main API, 30-day chunks)."""

import logging
from datetime import datetime, timedelta, timezone

from collectors.base import BaseCollector
from config import ASSETS, DERIBIT_CHUNK_DAYS, DERIBIT_MAIN_URL, DEFAULT_COLLECTION_START
from database import Database

log = logging.getLogger(__name__)


class DeribitFundingCollector(BaseCollector):
    """Collect 8-hour funding rates from Deribit main API."""

    def __init__(self):
        super().__init__(concurrency=5)

    async def collect(
        self,
        db: Database,
        asset: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> int:
        cfg = ASSETS[asset]
        instrument = cfg.perpetual_name

        if start_date is None:
            start_date = DEFAULT_COLLECTION_START
        if end_date is None:
            end_date = datetime.now(timezone.utc)

        # Resume
        latest_ts = await db.get_latest_funding_timestamp(asset)
        if latest_ts:
            resume_dt = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("Resuming %s funding from %s", asset, start_date.date())

        log.info(
            "Collecting %s funding rates: %s to %s",
            asset, start_date.date(), end_date.date(),
        )

        total_saved = 0
        current = start_date
        chunk_num = 0

        while current < end_date:
            chunk_end = min(current + timedelta(days=DERIBIT_CHUNK_DAYS), end_date)
            start_ms = int(current.timestamp() * 1000)
            end_ms = int(chunk_end.timestamp() * 1000)
            chunk_num += 1

            params = {
                "instrument_name": instrument,
                "start_timestamp": start_ms,
                "end_timestamp": end_ms,
            }

            resp = await self._get(
                f"{DERIBIT_MAIN_URL}/get_funding_rate_history",
                params=params,
            )

            rows = []
            if resp:
                result = resp.get("result", [])
                for entry in result:
                    ts = entry.get("timestamp")
                    interest = entry.get("interest_8h")
                    if ts is not None and interest is not None:
                        rows.append({
                            "timestamp": ts,
                            "asset": asset,
                            "funding_8h": interest,
                        })

                if rows:
                    await db.insert_funding(rows)
                    total_saved += len(rows)

            log.info(
                "Funding %s: chunk %d — %d records (ending %s, total %d)",
                asset, chunk_num, len(rows), chunk_end.date(), total_saved,
            )

            current = chunk_end

        log.info("Funding done: %d records saved for %s", total_saved, asset)
        return total_saved
