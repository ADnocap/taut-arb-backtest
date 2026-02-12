"""Deribit OHLCV 1-hour candle collector (30-day chunks)."""

import logging
from datetime import datetime, timedelta, timezone

from collectors.base import BaseCollector
from config import ASSETS, DERIBIT_CHUNK_DAYS, DERIBIT_HISTORY_URL, DEFAULT_COLLECTION_START
from database import Database

log = logging.getLogger(__name__)


class DeribitOHLCVCollector(BaseCollector):
    """Collect 1-hour OHLCV candles from perpetual instruments."""

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
        latest_ts = await db.get_latest_ohlcv_timestamp(asset)
        if latest_ts:
            resume_dt = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("Resuming %s OHLCV from %s", asset, start_date.date())

        log.info(
            "Collecting %s OHLCV: %s to %s",
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
                "resolution": "60",
            }

            resp = await self._get(
                f"{DERIBIT_HISTORY_URL}/get_tradingview_chart_data",
                params=params,
            )

            rows = []
            if resp:
                result = resp.get("result", {})
                rows = self._parse_candles(result, asset)
                if rows:
                    await db.insert_ohlcv(rows)
                    total_saved += len(rows)

            log.info(
                "OHLCV %s: chunk %d — %d candles (ending %s, total %d)",
                asset, chunk_num, len(rows), chunk_end.date(), total_saved,
            )

            current = chunk_end

        log.info("OHLCV done: %d candles saved for %s", total_saved, asset)
        return total_saved

    def _parse_candles(self, result: dict, asset: str) -> list[dict]:
        """Parse parallel-array response into candle dicts."""
        ticks = result.get("ticks", [])
        opens = result.get("open", [])
        highs = result.get("high", [])
        lows = result.get("low", [])
        closes = result.get("close", [])
        volumes = result.get("volume", [])

        if not ticks:
            return []

        rows = []
        for i, ts in enumerate(ticks):
            if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes):
                break

            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            v = volumes[i] if i < len(volumes) else None

            # Validate OHLC relationship
            if h < max(o, c) or l > min(o, c):
                continue

            rows.append({
                "timestamp": ts,
                "asset": asset,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
                "resolution": "1h",
            })

        return rows
