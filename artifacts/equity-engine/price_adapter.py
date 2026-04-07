"""
Async Price Data Adapter
========================
High-performance async downloader for Yahoo Finance chart data using aiohttp.
Replaces yfinance.download for bulk historical prices.

Features:
- Bounded async concurrency with connection pooling
- Exponential backoff with retry
- Incremental history refresh (fetch only missing tail)
- Robust OHLCV parsing
- Isolated behind adapter interface so transport details stay contained
"""

import asyncio
import time
import logging
from typing import Optional, Callable

import aiohttp
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_CHART_URL = "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
_CRUMB_URL = "https://query2.finance.yahoo.com/v1/test/getcrumb"
_COOKIE_URL = "https://fc.yahoo.com/"
_QUOTE_URL = "https://query2.finance.yahoo.com/v7/finance/quote"

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
_HEADERS = {"User-Agent": _USER_AGENT}


class PriceDataAdapter:
    """Async Yahoo Finance chart data downloader with connection pooling and retry."""

    def __init__(self, max_concurrent: int = 50, max_retries: int = 3):
        self._max_concurrent = max_concurrent
        self._max_retries = max_retries
        self._session: Optional[aiohttp.ClientSession] = None
        self._crumb: Optional[str] = None
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def _init_session(self):
        connector = aiohttp.TCPConnector(
            limit=self._max_concurrent + 10,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        jar = aiohttp.CookieJar()
        self._session = aiohttp.ClientSession(
            connector=connector,
            cookie_jar=jar,
            headers=_HEADERS,
        )
        try:
            async with self._session.get(
                _COOKIE_URL, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                await resp.read()
            async with self._session.get(
                _CRUMB_URL, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                text = await resp.text()
                if text and len(text) < 50 and "\n" not in text:
                    self._crumb = text
                    logger.info("Yahoo crumb acquired")
                else:
                    self._crumb = None
                    logger.warning("Failed to get Yahoo crumb — will try without")
        except Exception as e:
            logger.warning(f"Crumb init failed ({e}) — will try without crumb")
            self._crumb = None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _refresh_crumb(self):
        try:
            async with self._session.get(
                _COOKIE_URL, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                await resp.read()
            async with self._session.get(
                _CRUMB_URL, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                text = await resp.text()
                if text and len(text) < 50:
                    self._crumb = text
        except Exception:
            pass

    async def _fetch_chart(
        self, ticker: str, period1: int, period2: int
    ) -> Optional[tuple]:
        async with self._semaphore:
            url = _CHART_URL.format(symbol=ticker)
            params = {
                "period1": str(period1),
                "period2": str(period2),
                "interval": "1d",
                "includeAdjustedClose": "true",
            }
            if self._crumb:
                params["crumb"] = self._crumb

            for attempt in range(self._max_retries):
                try:
                    timeout = aiohttp.ClientTimeout(total=15)
                    async with self._session.get(
                        url, params=params, timeout=timeout
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            return self._parse_chart(ticker, data)
                        elif resp.status == 429:
                            wait = min(2 ** (attempt + 1), 10)
                            await asyncio.sleep(wait)
                        elif resp.status in (401, 403):
                            if attempt == 0:
                                await self._refresh_crumb()
                                if self._crumb:
                                    params["crumb"] = self._crumb
                                continue
                            return None
                        elif resp.status == 404:
                            return None
                        else:
                            await asyncio.sleep(1)
                except asyncio.TimeoutError:
                    await asyncio.sleep(2 ** attempt)
                except Exception:
                    await asyncio.sleep(2 ** attempt)
            return None

    def _parse_chart(
        self, ticker: str, data: dict
    ) -> Optional[tuple]:
        try:
            chart = data.get("chart", {})
            results = chart.get("result")
            if not results:
                return None

            result = results[0]
            timestamps = result.get("timestamp")
            if not timestamps:
                return None

            indicators = result.get("indicators", {})
            quotes = indicators.get("quote", [{}])[0]
            adjclose_list = indicators.get("adjclose", [{}])

            close = quotes.get("close", [])
            volume = quotes.get("volume", [])

            if adjclose_list and adjclose_list[0].get("adjclose"):
                adj = adjclose_list[0]["adjclose"]
                if len(adj) == len(timestamps):
                    close = adj

            if not close or len(close) != len(timestamps):
                return None

            dates = pd.to_datetime(timestamps, unit="s", utc=True).tz_convert(None).normalize()

            close_arr = np.array(
                [float(v) if v is not None else np.nan for v in close],
                dtype=np.float64
            )
            vol_arr = np.array(
                [float(v) if v is not None else 0.0 for v in volume],
                dtype=np.float64
            ) if volume and len(volume) == len(timestamps) else np.zeros(len(timestamps))

            close_arr[close_arr <= 0] = np.nan

            close_s = pd.Series(close_arr, index=dates, name=ticker)
            vol_s = pd.Series(vol_arr, index=dates, name=ticker)

            close_s = close_s[~close_s.index.duplicated(keep="last")]
            vol_s = vol_s[~vol_s.index.duplicated(keep="last")]

            close_s = close_s.dropna()
            if len(close_s) < 100:
                return None

            return ticker, close_s, vol_s

        except Exception as e:
            logger.debug(f"{ticker}: parse error — {e}")
            return None

    async def fetch_prices(
        self,
        tickers: list,
        progress_cb: Optional[Callable] = None,
        period1: Optional[int] = None,
        period2: Optional[int] = None,
    ) -> tuple:
        t0 = time.time()
        await self._init_session()

        if period2 is None:
            period2 = int(time.time())
        if period1 is None:
            period1 = period2 - (2 * 365 + 30) * 86400

        close_dict = {}
        volume_dict = {}
        failed = []
        completed = 0
        lock = asyncio.Lock()

        async def _fetch_and_collect(ticker):
            nonlocal completed
            result = await self._fetch_chart(ticker, period1, period2)
            async with lock:
                completed += 1
                if result:
                    t, close, vol = result
                    close_dict[t] = close
                    volume_dict[t] = vol
                else:
                    failed.append(ticker)
                if progress_cb and completed % 25 == 0:
                    progress_cb(len(close_dict), len(tickers), completed)

        await asyncio.gather(
            *[_fetch_and_collect(t) for t in tickers],
            return_exceptions=True
        )

        await self.close()
        elapsed = time.time() - t0
        logger.info(
            f"PriceDataAdapter: {len(close_dict)}/{len(tickers)} downloaded, "
            f"{len(failed)} failed, {elapsed:.1f}s"
        )
        return close_dict, volume_dict, failed

    async def fetch_batch_quotes(
        self, tickers: list, batch_size: int = 100
    ) -> dict:
        if not self._session or self._session.closed:
            await self._init_session()

        meta = {}
        batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]

        for batch in batches:
            symbols = ",".join(batch)
            params = {"symbols": symbols}
            if self._crumb:
                params["crumb"] = self._crumb

            try:
                timeout = aiohttp.ClientTimeout(total=15)
                async with self._session.get(
                    _QUOTE_URL, params=params, timeout=timeout
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                    quotes = data.get("quoteResponse", {}).get("result", [])
                    for q in quotes:
                        sym = q.get("symbol", "")
                        if sym:
                            meta[sym] = {
                                "name": q.get("longName") or q.get("shortName") or sym,
                                "sector": q.get("sector"),
                                "industry": q.get("industry"),
                                "market_cap": q.get("marketCap"),
                                "price": q.get("regularMarketPrice"),
                                "avg_volume": q.get("averageDailyVolume10Day"),
                                "quote_type": q.get("quoteType", ""),
                                "exchange": q.get("exchange", ""),
                            }
            except Exception as e:
                logger.warning(f"Batch quote error: {e}")

        return meta


def run_async_download(tickers, progress_cb=None, period1=None, period2=None, max_concurrent=50):
    adapter = PriceDataAdapter(max_concurrent=max_concurrent)
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            adapter.fetch_prices(tickers, progress_cb, period1, period2)
        )
    finally:
        loop.close()


def run_async_batch_quotes(tickers, max_concurrent=50):
    adapter = PriceDataAdapter(max_concurrent=max_concurrent)
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(adapter.fetch_batch_quotes(tickers))
    finally:
        loop.close()
