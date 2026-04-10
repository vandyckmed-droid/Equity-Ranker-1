"""
quality_loader.py — Quality data fetch/persist layer.

Fetches Operating Income, EBIT, Net Income, and Total Assets from yfinance
income_stmt + balance_sheet, sequentially, for every ticker in the universe.

Cache: diskcache key "quality_raw_v1", TTL = 7 days
  - Incremental save every 100 tickers (never lose progress mid-run)
  - Never-drop-good-data: if a refresh run fails, the last good snapshot is kept
  - On restart, serves instantly from cache if not expired

Usage:
  from quality_loader import get_or_build_quality_raw
  raw = get_or_build_quality_raw(tickers, cache)  # returns immediately if cached
"""

import logging
import time
from typing import Optional, Tuple

import numpy as np
import yfinance as yf

logger = logging.getLogger(__name__)

QUALITY_RAW_CACHE_KEY = "quality_raw_v1"
QUALITY_RAW_CACHE_TTL = 7 * 24 * 3600   # 7 days

# ── Income-statement field aliases (tried in order, most→least preferred) ─────
_OPERATING_INCOME_KEYS = [
    "Operating Income",
    "Total Operating Income As Reported",
    "EBIT",                                     # fallback within op-income slot
]
_EBIT_KEYS = [
    "EBIT",
    "Operating Income",
    "Normalized Income",
]
_NET_INCOME_KEYS = [
    "Net Income",
    "Net Income Common Stockholders",
    "Net Income Including Noncontrolling Interests",
    "Net Income Continuous Operations",
    "Net Income From Continuing Operation Net Minority Interest",
]

# ── Balance-sheet field aliases ───────────────────────────────────────────────
_TOTAL_ASSETS_KEYS = [
    "Total Assets",
]


# ── Low-level yfinance extraction helpers ─────────────────────────────────────

def _first_val(df, keys: list) -> Tuple[Optional[float], Optional[str]]:
    """
    Search df.index for the first matching key, return (value, key_found).
    Value is the most-recent non-null entry in that row.
    Returns (None, None) if nothing found.
    """
    if df is None or df.empty:
        return None, None
    for key in keys:
        if key in df.index:
            for v in df.loc[key]:
                if v is not None and not (isinstance(v, float) and np.isnan(v)):
                    return float(v), key
    return None, None


def _two_most_recent(df, keys: list) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Return (current_val, prior_val, key_found) — the two most recent values
    from the first matching key row.
    """
    if df is None or df.empty:
        return None, None, None
    for key in keys:
        if key in df.index:
            row = df.loc[key].dropna()
            vals = [float(v) for v in row.values[:2]]
            if len(vals) >= 2:
                return vals[0], vals[1], key
            elif len(vals) == 1:
                return vals[0], None, key
    return None, None, None


def _period_date(df) -> Optional[str]:
    """Return the most recent column date as ISO string."""
    try:
        if df is not None and not df.empty and len(df.columns) > 0:
            return str(df.columns[0].date())
    except Exception:
        pass
    return None


# ── Per-ticker fetch ──────────────────────────────────────────────────────────

def _fetch_one(ticker: str) -> dict:
    """
    Fetch quality raw data for a single ticker.
    Returns a structured dict; never raises.
    """
    rec = {
        "ticker":               ticker,
        "operating_income":     None,
        "operating_income_key": None,
        "ebit":                 None,
        "ebit_key":             None,
        "net_income":           None,
        "net_income_key":       None,
        "total_assets_current": None,
        "total_assets_prior":   None,
        "total_assets_key":     None,
        "period_date":          None,
        "error":                None,
    }
    try:
        t   = yf.Ticker(ticker)
        fin = t.income_stmt          # rows=concepts, cols=dates (most-recent first)
        bs  = t.balance_sheet

        if fin is not None and not fin.empty:
            rec["operating_income"], rec["operating_income_key"] = _first_val(fin, _OPERATING_INCOME_KEYS)
            rec["ebit"],             rec["ebit_key"]             = _first_val(fin, _EBIT_KEYS)
            rec["net_income"],       rec["net_income_key"]       = _first_val(fin, _NET_INCOME_KEYS)
            rec["period_date"]                                   = _period_date(fin)

        if bs is not None and not bs.empty:
            cur, prior, key = _two_most_recent(bs, _TOTAL_ASSETS_KEYS)
            rec["total_assets_current"] = cur
            rec["total_assets_prior"]   = prior
            rec["total_assets_key"]     = key

    except Exception as e:
        rec["error"] = str(e)[:300]

    return rec


# ── Bulk build (called from background thread) ────────────────────────────────

def build_quality_raw(tickers: list, cache, progress_cb=None) -> dict:
    """
    Sequentially fetch quality financials for every ticker.
    - Never uses ThreadPoolExecutor (sequential-only rule).
    - Resumable: skips tickers already present in the cache (no error).
    - Saves incrementally every 100 tickers so progress is never lost.
    - On completion, atomically sets the cache key with the full result.

    Args:
        tickers:      list of ticker strings
        cache:        diskcache.Cache instance
        progress_cb:  optional callable(done: int, total: int, failed: int)

    Returns:
        dict  {ticker: raw_record}
    """
    # Load existing snapshot — any ticker already fetched (with no error) is skipped
    existing: dict = cache.get(QUALITY_RAW_CACHE_KEY) or {}
    results: dict  = dict(existing)

    # Only fetch tickers not yet in cache or that previously errored
    to_fetch = [
        t for t in tickers
        if t not in existing or existing[t].get("error") is not None
    ]
    skipped = len(tickers) - len(to_fetch)

    logger.info(
        f"quality_loader: {len(to_fetch)} to fetch, {skipped} already cached "
        f"(total universe {len(tickers)})"
    )

    n      = len(to_fetch)
    failed = 0
    t0     = time.time()

    for i, ticker in enumerate(to_fetch):
        try:
            rec = _fetch_one(ticker)
            results[ticker] = rec
            if rec.get("error"):
                failed += 1
        except Exception as e:
            results[ticker] = {
                "ticker": ticker,
                "operating_income": None, "operating_income_key": None,
                "ebit": None,             "ebit_key": None,
                "net_income": None,       "net_income_key": None,
                "total_assets_current": None, "total_assets_prior": None,
                "total_assets_key": None,
                "period_date": None,
                "error": str(e)[:300],
            }
            failed += 1

        # Incremental persist every 100 new fetches
        if n > 0 and (i + 1) % 100 == 0:
            cache.set(QUALITY_RAW_CACHE_KEY, results, expire=QUALITY_RAW_CACHE_TTL)
            elapsed = time.time() - t0
            rate    = (i + 1) / elapsed if elapsed > 0 else 0
            eta     = (n - i - 1) / rate if rate > 0 else 0
            logger.info(
                f"quality_loader: {i+1}/{n} new  failed={failed}  "
                f"elapsed={elapsed:.0f}s  eta={eta:.0f}s"
            )
            if progress_cb:
                progress_cb(i + 1, n, failed)

    # Final persist
    cache.set(QUALITY_RAW_CACHE_KEY, results, expire=QUALITY_RAW_CACHE_TTL)
    elapsed = time.time() - t0
    logger.info(
        f"quality_loader: DONE — {len(results)} total, {n} fetched, "
        f"{failed} errors, {elapsed:.1f}s"
    )
    return results


# ── Public entry point ────────────────────────────────────────────────────────

def get_or_build_quality_raw(tickers: list, cache, force_refresh: bool = False) -> dict:
    """
    Return the quality raw dataset.
    - If a valid cache entry exists and force_refresh=False: returns immediately.
    - Otherwise: triggers build_quality_raw() synchronously.

    In normal usage, this is called from a background daemon thread, so the
    synchronous fetch is fine — it doesn't block the HTTP server.
    """
    if not force_refresh:
        cached = cache.get(QUALITY_RAW_CACHE_KEY)
        if cached and isinstance(cached, dict) and len(cached) > 0:
            logger.info(
                f"quality_loader: cache hit — {len(cached)} tickers "
                f"(skipping refresh)"
            )
            return cached

    return build_quality_raw(tickers, cache)
