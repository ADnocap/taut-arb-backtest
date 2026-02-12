"""Base HTTP client with retry, backoff, and concurrency control."""

import asyncio
import logging

import aiohttp

from config import MAX_RETRIES, REQUEST_TIMEOUT, RETRY_DELAYS

log = logging.getLogger(__name__)


class BaseCollector:
    """Async HTTP client with exponential backoff and semaphore."""

    def __init__(self, concurrency: int = 10):
        self._sem = asyncio.Semaphore(concurrency)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()
            self._session = None

    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
        headers: dict | None = None,
    ) -> dict | list | None:
        """HTTP request with retry + exponential backoff."""
        last_exc = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                async with self._sem:
                    async with self._session.request(
                        method, url, params=params, json=json, headers=headers
                    ) as resp:
                        if resp.status == 429:
                            retry_after = resp.headers.get("Retry-After")
                            backoff = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                            wait = (
                                max(float(retry_after), backoff)
                                if retry_after
                                else backoff
                            )
                            log.warning("429 rate limited, waiting %.1fs", wait)
                            await asyncio.sleep(wait)
                            continue
                        if resp.status in (502, 503, 504):
                            wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                            log.warning(
                                "%d from %s, retry in %.1fs", resp.status, url, wait
                            )
                            await asyncio.sleep(wait)
                            continue
                        resp.raise_for_status()
                        return await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                if attempt < MAX_RETRIES:
                    wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                    log.warning(
                        "%s on attempt %d, retry in %.1fs",
                        type(exc).__name__,
                        attempt + 1,
                        wait,
                    )
                    await asyncio.sleep(wait)
        log.error("All retries exhausted for %s", url)
        if last_exc:
            raise last_exc
        return None

    async def _get(self, url: str, params: dict | None = None) -> dict | list | None:
        return await self._request("GET", url, params=params)

    async def _post(
        self, url: str, json: dict | None = None, headers: dict | None = None
    ) -> dict | list | None:
        return await self._request("POST", url, json=json, headers=headers)
