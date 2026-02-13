"""Deribit DVOL (volatility index) collector — hourly candles, 30-day chunks."""

import logging
from datetime import datetime, timedelta, timezone

from collectors.base import BaseCollector
from config import ASSETS, DERIBIT_CHUNK_DAYS, DERIBIT_MAIN_URL, DEFAULT_COLLECTION_START
from database import Database

log = logging.getLogger(__name__)


class DeribitDVOLCollector(BaseCollector):
    """Collect hourly DVOL candles from Deribit volatility index endpoint."""

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
        currency = cfg.deribit_currency

        # SOL/XRP use USDC currency — try anyway but expect empty
        if currency == "USDC":
            # Deribit DVOL only supports BTC/ETH natively.
            # Try with the asset name directly (SOL, XRP) — graceful no-op.
            currency = asset

        if start_date is None:
            start_date = DEFAULT_COLLECTION_START
        if end_date is None:
            end_date = datetime.now(timezone.utc)

        # Resume
        latest_ts = await db.get_latest_dvol_timestamp(asset)
        if latest_ts:
            resume_dt = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
            if resume_dt > start_date:
                start_date = resume_dt
                log.info("Resuming %s DVOL from %s", asset, start_date.date())

        log.info(
            "Collecting %s DVOL: %s to %s",
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
                "currency": currency,
                "start_timestamp": start_ms,
                "end_timestamp": end_ms,
                "resolution": "3600",
            }

            resp = await self._get(
                f"{DERIBIT_MAIN_URL}/get_volatility_index_data",
                params=params,
            )

            rows = []
            if resp:
                result = resp.get("result", {})
                data = result.get("data", [])
                continuation = result.get("continuation")

                rows = self._parse_candles(data, asset)
                if rows:
                    await db.insert_dvol_candles(rows)
                    total_saved += len(rows)

                # Paginate via continuation
                while continuation and data:
                    params["end_timestamp"] = continuation
                    resp = await self._get(
                        f"{DERIBIT_MAIN_URL}/get_volatility_index_data",
                        params=params,
                    )
                    if not resp:
                        break
                    result = resp.get("result", {})
                    data = result.get("data", [])
                    continuation = result.get("continuation")
                    page_rows = self._parse_candles(data, asset)
                    if page_rows:
                        await db.insert_dvol_candles(page_rows)
                        total_saved += len(page_rows)
                        rows.extend(page_rows)

            if not rows and chunk_num == 1 and asset in ("SOL", "XRP"):
                log.info("DVOL %s: no data available (expected for %s)", asset, asset)
                return 0

            log.info(
                "DVOL %s: chunk %d — %d candles (ending %s, total %d)",
                asset, chunk_num, len(rows), chunk_end.date(), total_saved,
            )

            current = chunk_end

        log.info("DVOL done: %d candles saved for %s", total_saved, asset)
        return total_saved

    def _parse_candles(self, data: list, asset: str) -> list[dict]:
        """Parse [[ts_ms, open, high, low, close], ...] into candle dicts."""
        if not data:
            return []

        rows = []
        for entry in data:
            if not isinstance(entry, (list, tuple)) or len(entry) < 5:
                continue

            ts, o, h, l, c = entry[0], entry[1], entry[2], entry[3], entry[4]

            # Auto-detect percentage vs decimal.
            # DVOL values: if > 5.0 they're percentages (e.g. 55 = 55%),
            # normalize to decimal (0.55).
            o = self._normalize_dvol(o)
            h = self._normalize_dvol(h)
            l = self._normalize_dvol(l)
            c = self._normalize_dvol(c)

            if o is None or h is None or l is None or c is None:
                continue

            rows.append({
                "timestamp": ts,
                "asset": asset,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
            })

        return rows

    @staticmethod
    def _normalize_dvol(val: float | None) -> float | None:
        """Normalize DVOL to decimal. Values > 5.0 are treated as percentages."""
        if val is None or val <= 0:
            return None
        if val > 5.0:
            val = val / 100.0
        if val > 5.0 or val <= 0:
            return None
        return val
