"""
Equity Ranking Engine
=====================
Fetches real market data from Yahoo Finance, computes momentum and quality
factors using a 3-sleeve alpha model, and provides portfolio risk metrics.

Universe construction
---------------------
At cold start, the live universe is built dynamically from the NASDAQ screener API:
  1. NASDAQ API  : fetches NYSE- and NASDAQ-listed stocks (exchange=NYSE, exchange=NASDAQ)
                   with a $500M market-cap pre-filter to bound download volume.
  2. Format filter: non-empty symbol, <= 5 chars, alphabetic only.
  3. ADR/foreign merge: curated ADR/foreign names from _FALLBACK_TICKERS are always
                   included (TSM, ASML, ARM, RIO, BHP, VALE, etc.).
  4. classify_ticker(): excludes ETFs, funds, OTC, partnerships, SPACs at meta-fetch time.
  5. Price/ADV/cap filters: price >= $5, 63-day median ADV >= $10M, market cap >= $1B.
Typical result: ~2,000 tickers fetched → 900–1,300 survivors after all filters.
On any NASDAQ API failure the engine falls back to _FALLBACK_TICKERS (offline list).
The universe list is cached for 24 h under UNIVERSE_CACHE_KEY (separate from price cache).

Universe policy
---------------
US-retail-tradable, US-listed operating company equities.
  INCLUDED : common stocks, ADRs / ADSs, US-listed foreign companies (e.g. TSM, ASML, ARM)
  EXCLUDED : ETFs, mutual funds, CEFs, BDCs, SPACs, shells, preferreds, warrants,
             LP / MLP / partnership structures (non-standard K-1 tax), OTC / Pink Sheet names

Liquidity filter
----------------
63-trading-day median daily dollar volume (Close × Volume) computed from
downloaded price history. Does NOT rely on Yahoo Finance averageDailyVolume metadata.

Quality sleeve
--------------
Three buckets, each winsorized then z-scored cross-sectionally:
  Profitability : mean(z_roe, z_roa)         — available inputs averaged
  Margin        : mean(z_gross_margin, z_op_margin) — available inputs averaged
  Leverage      : -z(de_ratio)               — lower debt is better
Minimum coverage: profitability bucket must exist AND at least one of margin/leverage.
No imputation for missing descriptors; missing quality → alpha renormalized to S+T only.
"""

import os
import json
import time
import threading
import logging
import concurrent.futures
from datetime import datetime
from typing import Optional

import requests

import numpy as np
import pandas as pd
import yfinance as yf
from scipy import stats
from sklearn.cluster import AgglomerativeClustering
import diskcache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR          = "/tmp/equity_cache"
PRICE_CACHE_TTL    = 8  * 3600   # 8 hours  — price + dollar-volume history
META_CACHE_TTL     = 24 * 3600   # 24 hours — metadata (more stable than prices)
UNIVERSE_CACHE_TTL = 24 * 3600   # 24 hours — dynamic universe ticker list
PRICE_CACHE_KEY    = "price_data_v5"
META_CACHE_KEY     = "meta_data_v2"
UNIVERSE_CACHE_KEY = "universe_v1"
NASDAQ_META_KEY    = "nasdaq_meta_v1"   # NASDAQ screener metadata fallback

cache = diskcache.Cache(CACHE_DIR)

_status = {
    "status": "loading",
    "message": "Initializing...",
    "progress": 0,
    "total": 0,
    "loaded": 0,
    "cached_at": None,
}
_status_lock = threading.Lock()

# ─── Offline fallback universe ────────────────────────────────────────────────
# _FALLBACK_TICKERS is used ONLY when the NASDAQ screener API is unreachable.
# It is NOT the live universe — at runtime the live universe is built by
# _fetch_universe() from the NASDAQ API and cached for 24 h.
# Curated ADR/foreign names in this list are always merged into the live
# universe regardless (TSM, ASML, ARM, RIO, BHP, VALE, etc.).
_FALLBACK_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG", "META", "TSLA", "AVGO", "JPM",
    "LLY", "V", "UNH", "XOM", "MA", "COST", "JNJ", "PG", "HD", "WMT",
    "MRK", "ABBV", "CVX", "BAC", "KO", "NFLX", "PEP", "CRM", "TMO", "ACN",
    "MCD", "AMD", "PM", "LIN", "CSCO", "GE", "ABT", "NOW", "DIS", "WFC",
    "CAT", "TXN", "NEE", "INTU", "IBM", "CMCSA", "ISRG", "SPGI", "GS", "AMGN",
    "QCOM", "RTX", "BKNG", "VZ", "HON", "LOW", "AMAT", "AXP", "PFE", "SCHW",
    "T", "UNP", "SYK", "BX", "MS", "UBER", "C", "DE", "MU", "BA",
    "ADI", "GILD", "ADP", "BLK", "ETN", "REGN", "PLD", "CB", "CI", "VRTX",
    "LRCX", "TJX", "MDLZ", "SO", "MMC", "EOG", "FI", "SHW", "BMY", "ICE",
    "CL", "PANW", "EQIX", "WM", "CME", "PH", "ZTS", "AON", "CARR", "KLAC",
    "MCO", "DUK", "SLB", "COF", "MAR", "TDG", "APH", "GEV", "APO", "MPC",
    "PSX", "WELL", "TT", "CTAS", "GD", "HCA", "OXY", "PCAR", "SNPS", "ECL",
    "CDNS", "CMG", "NOC", "MSI", "NKE", "FTNT", "ORLY", "CEG", "AFL", "AIG",
    "PSA", "WDAY", "TMUS", "WMB", "EW", "HES", "COP", "AZO", "FANG",
    "PCG", "KMB", "ALL", "STZ", "RCL", "CBRE", "ITW", "D", "PPG", "DOW",
    "LHX", "PAYX", "HSY", "EXC", "GWW", "CCI", "FICO", "PEG", "HAL", "BIIB",
    "FAST", "ACGL", "VRSK", "BDX", "KEYS", "HLT", "KHC", "RSG", "CTSH", "NEM",
    "A", "OKE", "XEL", "DAL", "UAL", "F", "GM", "SBUX",
    "EBAY", "PYPL", "SQ", "SNOW", "PLTR", "CRWD", "DDOG", "NET", "ZS", "OKTA",
    "HUBS", "BILL", "NTNX", "PTC", "MANH", "PAYC", "PCTY", "WK", "CDAY",
    "CLX", "CHD", "EL", "ULTA", "ELF", "COTY",
    "TAP", "MNST", "CELH",
    "CAG", "SJM", "CPB", "MKC", "HRL", "TSN",
    "WH", "CHH", "H", "IHG",
    "EXP", "MLM", "VMC", "IP", "AVY", "PKG",
    "DLR", "AMT", "IRM", "ARE", "BXP", "EQR", "AVB",
    "ESS", "MAA", "UDR", "NNN", "O", "VICI", "GLPI", "STAG",
    "AJG", "WTW", "MKL", "RE",
    "OC", "SWK", "MAS", "TREX",
    "LII", "ROK", "EMR", "TRMB",
    "DXCM", "PODD", "ALGN", "HOLX", "BIO", "XRAY",
    "MCK", "CAH", "COR", "HSIC",
    "HUM", "MOH", "CNC", "ELV",
    "IQV", "ICLR", "MEDP",
    "RMD",
    "AMP", "PFG", "VOYA", "BEN", "IVZ", "AMG",
    "NTRS", "STT", "TROW", "KEY", "CMA", "ZION",
    "FITB", "HBAN", "RF", "CFG", "ALLY", "SYF",
    "DFS", "GPN", "FIS", "FLT", "WEX",
    "MTB", "EWBC", "WAL",
    "GL", "MET", "PRU",
    "AEP", "FE", "ES", "AEE", "CMS", "LNT", "NI", "PNW", "EVRG", "ATO",
    "AWK",
    "LEN", "DHI", "PHM", "NVR", "MDC", "MTH",
    "BLD", "DOOR", "BECN",
    "CW", "HEICO", "HEI",
    "AXON",
    # US-listed foreign operating companies — eligible via ADR / primary listing
    "TSM", "ASML", "ARM", "LSCC", "ENTG", "AMBA",
    "ALGM", "CRUS", "DIOD",
    "ALNY", "BMRN", "EXEL",
    "SGEN", "RGEN", "RARE", "ARWR", "SRPT",
    "ADM", "BG", "CF", "MOS", "NTR", "LW",
    "ZBH", "COO",
    "TFX", "NUVA", "GMED",
    "FMC", "IFF",
    "AXTA", "RPM", "FUL", "CBT",
    "LEA", "ALV", "BWA", "GT",
    "APTV", "GNTX",
    "LULU", "DECK", "SKX", "COLM",
    "RL", "PVH", "HBI", "GIL",
    "GPC", "AAP", "MNRO",
    "PII", "HOG", "BC",
    "NXST", "TGNA", "AMCX", "PARA",
    "FOXA", "FOX", "WBD", "LUMN",
    "SIRI",
    "ARES", "KKR", "ARCC",
    "WES", "AM", "KMI", "MPLX", "ET", "TRGP",
    "EPD",
    "LBRT", "PTEN",
    "CIVI", "SM", "MTDR", "CPE",
    "VALE", "RIO", "BHP", "FCX", "NUE", "CLF",
    "STLD", "CMC", "RS",
    "AA",
    "CCJ",
    "WM", "RSG", "CLH",
    "CIEN", "VIAV", "INFN", "CALX",
    "MARA", "RIOT",
    "STWD", "BXMT",
    "TTD",
    "ONON",
    "ZI", "BRZE", "ALTR", "CFLT",
    "NTAP", "HPQ", "HPE", "DELL", "SMCI", "PSTG",
    "ANET", "JNPR",
    "INTC", "ON", "MRVL", "SWKS", "MPWR", "QRVO",
    "GPK", "SEE", "BERY",
    "FDS", "MSCI", "NDAQ", "CBOE", "MKTX",
    "EVR", "PJT", "LAZ", "JEF", "IBKR", "RJF", "LPL", "LPLA", "VIRT",
    "GDDY", "AKAM",
    "ODFL", "JBHT", "CHRW", "XPO", "SAIA",
    "FDX", "UPS",
    "GXO",
    "PAG", "AN", "LAD",
    "WERN", "MRTN",
    "WBS", "PNFP", "BOKF", "WAFD",
    "MHO", "GRBK",
    "TRU", "EXPN",
    "POOL", "SNA",
    "BSX", "MDT", "STE",
    "CVS", "WBA",
    "HOOD", "COIN",
    "APP", "RBLX",
    "IAG",
    "ACM", "PWR", "MTZ",
    "PNR", "IR", "TDY", "LDOS", "BAH", "SAIC",
    "IEX",
    "ROST", "BURL", "FIVE", "OLLI", "DG", "DLTR", "KR", "SFM",
    "ZM", "DOCU", "TWLO",
    "BWXT",
    "LNG", "RUN",
    "SRCL",
    "GENI", "ACHR",
    "ASTS",
    "SMAR", "DOMO",
    "PUBM", "DV",
    "BBAI",
]

# Deduplicate
_FALLBACK_TICKERS = sorted(list(set(
    t for t in _FALLBACK_TICKERS
    if t and len(t) <= 5 and t.replace(".", "").isalpha()
)))

# ─── Dynamic universe fetch ───────────────────────────────────────────────────

_NASDAQ_API = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange={exchange}"
_NASDAQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
_NASDAQ_TIMEOUT = 15

# ADR / foreign names that must always be in the universe regardless of API coverage.
_ADR_ANCHORS = {
    "TSM", "ASML", "ARM", "RIO", "BHP", "VALE", "NVO", "SAP", "TM",
    "HMC", "SONY", "SNY", "AZN", "GSK", "BTI", "SHOP", "SAN",
}


def _fetch_universe() -> list:
    """
    Build a dynamic ticker universe from the NASDAQ screener API.

    Steps
    -----
    1. Query NYSE and NASDAQ exchanges via the NASDAQ screener API.
    2. Pre-filter to market cap >= $2B (string-formatted in API response).
    3. Validate ticker format: non-empty, <= 5 chars, alphabetic only.
    4. Merge with _ADR_ANCHORS to ensure key ADR / foreign names are always present.
    5. Cache the combined sorted list in diskcache under UNIVERSE_CACHE_KEY (24h TTL).
    6. On any exception, log a warning and return _FALLBACK_TICKERS.

    Returns
    -------
    list of str — deduplicated, sorted ticker symbols
    """
    cached = cache.get(UNIVERSE_CACHE_KEY)
    if cached and isinstance(cached, list) and len(cached) > 0:
        logger.info(f"Universe from cache: {len(cached)} tickers")
        return cached

    try:
        tickers: set   = set()
        nasdaq_meta: dict = {}   # {ticker: {market_cap, sector, name, exchange, is_etf}}

        for exchange in ("NYSE", "NASDAQ"):
            url = _NASDAQ_API.format(exchange=exchange)
            resp = requests.get(url, headers=_NASDAQ_HEADERS, timeout=_NASDAQ_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            # Strict shape validation — raise on malformed response to force fallback
            table = data.get("data", {}).get("table", {})
            if not isinstance(table, dict):
                raise ValueError(f"NASDAQ API ({exchange}): unexpected 'table' shape: {type(table)}")
            rows = table.get("rows")
            if not rows or not isinstance(rows, list):
                raise ValueError(f"NASDAQ API ({exchange}): empty or missing 'rows' in response")

            exchange_count = 0
            for row in rows:
                symbol = (row.get("symbol") or "").strip().upper()
                if not symbol or len(symbol) > 5 or not symbol.isalpha():
                    continue
                # Pre-filter by market cap >= $2B
                raw_cap = row.get("marketCap") or row.get("market_cap") or ""
                try:
                    cap = float(str(raw_cap).replace(",", ""))
                except (ValueError, TypeError):
                    cap = 0.0
                if cap < 2_000_000_000:
                    continue
                tickers.add(symbol)
                exchange_count += 1
                # Store NASDAQ-sourced metadata as fallback for yfinance failures
                is_etf = str(row.get("etf") or "").lower() in ("true", "1", "yes", "y")
                nasdaq_meta[symbol] = {
                    "market_cap": cap,
                    "sector":     row.get("sector") or None,
                    "name":       row.get("name") or symbol,
                    "exchange":   exchange,
                    "is_etf":     is_etf,
                }
            logger.info(f"NASDAQ API ({exchange}): {exchange_count} tickers after $2B filter")

        # Merge ADR anchors from fallback list (always include)
        for t in _FALLBACK_TICKERS:
            if t in _ADR_ANCHORS:
                tickers.add(t)

        universe = sorted(tickers)

        if len(universe) < 500:
            logger.warning(
                f"Universe size abnormally low ({len(universe)} tickers) — "
                "possible partial API failure; falling back to _FALLBACK_TICKERS"
            )
            return _FALLBACK_TICKERS

        logger.info(f"Universe fetched from NASDAQ API: {len(universe)} tickers")
        cache.set(UNIVERSE_CACHE_KEY, universe, expire=UNIVERSE_CACHE_TTL)
        # Cache NASDAQ metadata separately (24h) — used as fallback when yfinance fails
        cache.set(NASDAQ_META_KEY, nasdaq_meta, expire=UNIVERSE_CACHE_TTL)
        logger.info(f"NASDAQ metadata cached for {len(nasdaq_meta)} tickers")
        return universe

    except Exception as e:
        logger.warning(f"NASDAQ universe fetch failed ({e}); falling back to _FALLBACK_TICKERS")
        return _FALLBACK_TICKERS


# ─── Globals ──────────────────────────────────────────────────────────────────
_price_data:    Optional[pd.DataFrame] = None   # Close prices: index=dates, cols=tickers
_dollar_volume: Optional[pd.DataFrame] = None   # Close×Volume: same shape as _price_data
_meta_data:     Optional[dict]         = None   # Per-ticker metadata dict
_rankings_cache = None                          # Cached ranked DataFrame (last params)
_last_params:   Optional[str]          = None
_audit_printed  = False                         # Guard: fire audit summary only once


# ─── Utilities ────────────────────────────────────────────────────────────────

def update_status(status: str, message: str, progress: float = 0,
                  total: int = 0, loaded: int = 0):
    with _status_lock:
        _status.update({"status": status, "message": message,
                        "progress": progress, "total": total, "loaded": loaded})
        if status == "ready":
            _status["cached_at"] = datetime.now().isoformat()


def get_status() -> dict:
    with _status_lock:
        return dict(_status)


def winsorize(series: pd.Series, p: float = 2.0) -> pd.Series:
    lo = np.percentile(series.dropna(), p)
    hi = np.percentile(series.dropna(), 100 - p)
    return series.clip(lo, hi)


def zscore(series: pd.Series) -> pd.Series:
    mu, sigma = series.mean(), series.std()
    if sigma < 1e-10:
        return series * 0
    return (series - mu) / sigma


def std_cs(series: pd.Series, winsor_p: float = 2.0) -> pd.Series:
    """Cross-sectional: winsorize then z-score. Returns NaN where input is NaN."""
    # Always cast to float first so None → NaN and negation/arithmetic work safely
    s = pd.to_numeric(series, errors="coerce")
    valid = s.notna()
    if valid.sum() < 10:
        return pd.Series(np.nan, index=s.index, dtype=float)
    out = s.copy()
    out[valid] = zscore(winsorize(s[valid], winsor_p))
    return out


def ols_tstat(log_prices: pd.Series) -> float:
    """OLS t-stat of log price regressed on time index."""
    n = len(log_prices)
    if n < 10:
        return np.nan
    x = np.arange(n, dtype=float)
    x -= x.mean()
    y = log_prices.values
    y = np.where(np.isfinite(y), y, np.nan)
    mask = ~np.isnan(y)
    if mask.sum() < 10:
        return np.nan
    slope, _, _, _, std_err = stats.linregress(x[mask], y[mask])
    return slope / std_err if std_err >= 1e-10 else np.nan


# ─── Universe structure classification ────────────────────────────────────────

# Universe policy:
#   INCLUDE: EQUITY / ADR quoteType on major US exchanges
#   EXCLUDE: funds, non-equity instruments, OTC/Pink Sheet, LP/MLP/partnership,
#            SPAC / blank-check / acquisition vehicles
#   NOTE: foreign issuer domicile is NOT a reason for exclusion.
#         TSM (Taiwan), ASML (Netherlands), ARM (UK), etc. are INCLUDED.

_FUND_TYPES       = {"ETF", "MUTUALFUND", "CEF"}
_EQUITY_TYPES     = {"EQUITY", "ADR", ""}       # allow blank — some legit stocks return ""
_OTC_EXCHANGES    = {"OTC", "PINK", "OTCBB", "OTCMKTS", "OTCQB", "OTCQX", "OTHER OTC"}
_PARTNERSHIP_MARK = {" LP", " L.P.", " LP.", "LIMITED PARTNERSHIP", " MLP",
                     " PARTNERS L", " PARTNERS,"}
_SPAC_MARKERS     = {"ACQUISITION CORP", "BLANK CHECK", " ACQ CORP",
                     "SPECIAL PURPOSE", " SPAC "}


def classify_ticker(ticker: str, meta: dict) -> Optional[str]:
    """
    Return an exclusion reason string if the ticker should be excluded from
    the universe, or None if it passes structure classification.

    Possible reasons
    ----------------
    excluded_fund              — ETF, mutual fund, CEF
    excluded_non_equity        — quoteType not EQUITY or ADR (e.g. index, warrant, note)
    excluded_otc               — OTC / Pink Sheet exchange
    excluded_partnership       — LP / MLP / partnership structure
    excluded_spac              — SPAC / blank-check / acquisition vehicle
    """
    quote_type = (meta.get("quote_type") or "").upper()
    exchange   = (meta.get("exchange")   or "").upper()
    long_name  = (meta.get("name")       or "").upper()

    if quote_type in _FUND_TYPES:
        return "excluded_fund"

    if quote_type and quote_type not in _EQUITY_TYPES:
        return "excluded_non_equity"

    if exchange in _OTC_EXCHANGES:
        return "excluded_otc"

    if any(m in long_name for m in _PARTNERSHIP_MARK):
        return "excluded_partnership"

    if any(m in long_name for m in _SPAC_MARKERS):
        return "excluded_spac"

    return None


# ─── Data loading ─────────────────────────────────────────────────────────────

def load_data_batch(tickers: list, batch_size: int = 100) -> tuple:
    """
    Download 2-year adjusted Close + Volume for all tickers in sequential batches.

    Uses sequential (non-parallel) batching because yfinance internally creates
    threads per download; parallel wrappers exhaust the system thread limit.
    Batch size 100 (up from 50) halves round-trips for the larger universe.

    Returns
    -------
    all_close       : dict  ticker → pd.Series of Close prices
    all_dollar_vol  : dict  ticker → pd.Series of daily dollar volume (Close × Volume)
    failed          : list  tickers that could not be loaded
    """
    all_close: dict      = {}
    all_dollar_vol: dict = {}
    failed: list         = []

    batches       = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    total_batches = len(batches)

    update_status("loading", f"Downloading price data (0/{len(tickers)} stocks)...",
                  progress=0, total=len(tickers), loaded=0)

    for bi, batch in enumerate(batches):
        try:
            raw = yf.download(
                batch, period="2y", auto_adjust=True,
                progress=False, timeout=60,
            )
            if raw.empty:
                failed.extend(batch)
                continue

            if isinstance(raw.columns, pd.MultiIndex):
                lvl0 = raw.columns.get_level_values(0)
                close  = raw["Close"]  if "Close"  in lvl0 else pd.DataFrame()
                volume = raw["Volume"] if "Volume" in lvl0 else pd.DataFrame()
            else:
                close  = raw[["Close"]]  if "Close"  in raw.columns else pd.DataFrame()
                volume = raw[["Volume"]] if "Volume" in raw.columns else pd.DataFrame()

            for ticker in batch:
                try:
                    if ticker not in close.columns:
                        failed.append(ticker)
                        continue
                    col = close[ticker].dropna()
                    if len(col) < 252:
                        failed.append(ticker)
                        continue
                    all_close[ticker] = col
                    if ticker in volume.columns:
                        vol_col = volume[ticker].reindex(col.index).fillna(0)
                        all_dollar_vol[ticker] = col * vol_col
                except Exception:
                    failed.append(ticker)

        except Exception as e:
            logger.error(f"Batch {bi} download error: {e}")
            failed.extend(batch)

        loaded   = len(all_close)
        progress = (bi + 1) / total_batches
        update_status("loading",
                      f"Downloading price data ({loaded}/{len(tickers)} stocks)...",
                      progress=progress, total=len(tickers), loaded=loaded)

    return all_close, all_dollar_vol, failed


def _fetch_one_meta(ticker: str) -> tuple:
    """Fetch Yahoo Finance info for a single ticker. Returns (ticker, meta_dict, ok)."""
    try:
        info = yf.Ticker(ticker).info
        return ticker, {
            "name":         info.get("longName") or info.get("shortName") or ticker,
            "sector":       info.get("sector"),
            "industry":     info.get("industry"),
            "market_cap":   info.get("marketCap"),
            "price":        info.get("currentPrice") or info.get("regularMarketPrice"),
            "avg_volume":   info.get("averageDailyVolume10Day"),
            "roe":          info.get("returnOnEquity"),
            "roa":          info.get("returnOnAssets"),
            "gross_margin": info.get("grossMargins"),
            "op_margin":    info.get("operatingMargins"),
            "de_ratio":     info.get("debtToEquity"),
            # Universe classification fields
            "quote_type":   info.get("quoteType", ""),
            "exchange":     info.get("exchange", ""),
        }, True
    except Exception:
        return ticker, {
            "name": ticker, "sector": None, "industry": None,
            "market_cap": None, "price": None, "avg_volume": None,
            "roe": None, "roa": None, "gross_margin": None,
            "op_margin": None, "de_ratio": None,
            "quote_type": "", "exchange": "",
        }, False


def load_meta_with_info(tickers: list) -> dict:
    """
    Load metadata for all tickers using bounded concurrency (10 workers).

    Fetches: sector, quality descriptors (ROE, ROA, margins, D/E),
             quoteType and exchange for universe structure classification.

    Failures per ticker are silently caught; failed tickers receive null fields.
    """
    meta: dict   = {}
    failed_count = 0

    update_status("loading", f"Loading metadata for {len(tickers)} stocks...",
                  progress=0.75, total=len(tickers), loaded=0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_map = {executor.submit(_fetch_one_meta, t): t for t in tickers}
        done = 0
        for future in concurrent.futures.as_completed(future_map):
            ticker, result, ok = future.result()
            meta[ticker] = result
            if not ok:
                failed_count += 1
            done += 1
            if done % 50 == 0:
                progress = 0.75 + (done / len(tickers)) * 0.20
                update_status("loading",
                              f"Loading metadata ({done}/{len(tickers)})...",
                              progress=progress)

    if failed_count:
        logger.warning(f"Metadata fetch: {failed_count}/{len(tickers)} tickers had errors")

    # Backfill missing fields from NASDAQ screener data (reliable even after yfinance 401s)
    nasdaq_meta = cache.get(NASDAQ_META_KEY) or {}
    backfilled = 0
    for ticker, m in meta.items():
        nd = nasdaq_meta.get(ticker)
        if not nd:
            continue
        if m.get("market_cap") is None:
            m["market_cap"] = nd["market_cap"]
            backfilled += 1
        if not m.get("sector"):
            m["sector"] = nd.get("sector")
        if not m.get("name") or m.get("name") == ticker:
            m["name"] = nd.get("name", ticker)
        # Infer ETF status from NASDAQ etf flag when quoteType is blank (yfinance failed)
        if not m.get("quote_type") and nd.get("is_etf"):
            m["quote_type"] = "ETF"
        # Ensure exchange is set for classify_ticker
        if not m.get("exchange"):
            m["exchange"] = nd.get("exchange", "")

    if backfilled:
        logger.info(f"Backfilled market_cap from NASDAQ screener for {backfilled} tickers")
    logger.info(f"Metadata loaded for {len(meta)} tickers")
    return meta


# ─── Factor computation ───────────────────────────────────────────────────────

def compute_factors(prices: pd.DataFrame,
                    meta: dict,
                    dollar_volume: Optional[pd.DataFrame] = None,
                    vol_floor: float = 0.05,
                    winsor_p: float = 2.0) -> pd.DataFrame:
    """
    Compute per-stock factor scores and quality buckets.

    Quality sleeve construction
    ---------------------------
    Each descriptor is winsorized then z-scored cross-sectionally before
    bucket construction. No imputation — missing values stay NaN.

    Profitability bucket : mean(z_roe, z_roa)              [uses available inputs]
    Margin bucket        : mean(z_gross_margin, z_op_margin)[uses available inputs]
    Leverage bucket      : -z(de_ratio)                    [lower debt = better]

    Quality score is the mean of available buckets, but REQUIRES:
      profitability bucket must be non-NaN
      AND at least one of margin or leverage must be non-NaN.
    Otherwise quality = NaN (no imputation).

    Liquidity
    ---------
    ADV is the 63-trading-day median of daily dollar volume (Close × Volume)
    computed from downloaded history. Falls back to metadata estimate only
    if dollar volume history is unavailable for that ticker.
    """
    log_prices  = np.log(prices)
    log_returns = log_prices.diff()

    rows = []
    for ticker in prices.columns:
        lp = log_prices[ticker].dropna()
        lr = log_returns[ticker].dropna()
        n  = len(lp)
        if n < 252:
            continue

        p_now = float(lp.iloc[-1])

        # ── Momentum ──────────────────────────────────────────────────────────
        r1  = float(lp.iloc[-1] - lp.iloc[-22])  if n >= 22  else np.nan
        r6  = float(lp.iloc[-1] - lp.iloc[-127]) if n >= 127 else np.nan
        r12 = float(lp.iloc[-1] - lp.iloc[-253]) if n >= 253 else np.nan
        m6  = r6  - r1 if np.isfinite(r6)  and np.isfinite(r1) else np.nan
        m12 = r12 - r1 if np.isfinite(r12) and np.isfinite(r1) else np.nan

        # ── Volatility ────────────────────────────────────────────────────────
        sigma6  = float(lr.iloc[-126:].std() * np.sqrt(252)) if n >= 126 else np.nan
        sigma12 = float(lr.std()             * np.sqrt(252))
        s6  = m6  / max(sigma6,  vol_floor) if np.isfinite(m6)  and np.isfinite(sigma6)  else np.nan
        s12 = m12 / max(sigma12, vol_floor) if np.isfinite(m12) else np.nan

        # ── OLS t-stats (always computed — required for T sleeve) ─────────────
        tstat6  = ols_tstat(lp.iloc[-126:]) if n >= 126 else np.nan
        tstat12 = ols_tstat(lp)

        # ── Liquidity: 63-day median daily dollar volume ──────────────────────
        # Primary: computed from downloaded Close × Volume history
        adv = np.nan
        if dollar_volume is not None and ticker in dollar_volume.columns:
            dv = dollar_volume[ticker].dropna()
            if len(dv) >= 63:
                adv = float(dv.iloc[-63:].median())
            elif len(dv) >= 21:
                adv = float(dv.iloc[-21:].median())
        # Fallback: metadata estimate (less reliable — 10-day window from Yahoo)
        if not np.isfinite(adv) or adv == 0:
            avg_vol_shares = meta.get(ticker, {}).get("avg_volume")
            price_last     = np.exp(p_now)
            if avg_vol_shares:
                adv = price_last * avg_vol_shares

        # ── Quality raw descriptors from metadata ─────────────────────────────
        m            = meta.get(ticker, {})
        roe          = m.get("roe")
        roa          = m.get("roa")
        gross_margin = m.get("gross_margin")
        op_margin    = m.get("op_margin")
        de_ratio     = m.get("de_ratio")
        market_cap   = m.get("market_cap")

        rows.append({
            "ticker":   ticker,
            "name":     m.get("name", ticker),
            "sector":   m.get("sector"),
            "industry": m.get("industry"),
            "price":    np.exp(p_now),
            "market_cap": market_cap,
            "adv":      adv,
            "r1": r1, "m6": m6, "m12": m12,
            "sigma6": sigma6, "sigma12": sigma12,
            "s6": s6, "s12": s12,
            "tstat6": tstat6, "tstat12": tstat12,
            # Raw quality descriptors (unstandardized)
            "_roe": roe, "_roa": roa,
            "_gross_margin": gross_margin, "_op_margin": op_margin,
            "_de_ratio": de_ratio,
            # Universe classification
            "structure_exclusion": classify_ticker(ticker, m),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # ── Quality bucket construction (cross-sectional) ─────────────────────────
    #
    # Step 1: standardize each raw descriptor independently
    z_roe   = std_cs(df["_roe"],          winsor_p)
    z_roa   = std_cs(df["_roa"],          winsor_p)
    z_gross = std_cs(df["_gross_margin"], winsor_p)
    z_op    = std_cs(df["_op_margin"],    winsor_p)
    z_de    = std_cs(df["_de_ratio"],     winsor_p)

    # Step 2: build buckets — mean of available z-scores within each bucket
    prof_frame   = pd.DataFrame({"z_roe": z_roe,   "z_roa": z_roa})
    margin_frame = pd.DataFrame({"z_gross": z_gross, "z_op": z_op})

    df["_prof_bucket"]     = prof_frame.mean(axis=1, skipna=True)   # NaN iff both inputs NaN
    df["_margin_bucket"]   = margin_frame.mean(axis=1, skipna=True) # NaN iff both inputs NaN
    df["_leverage_bucket"] = -z_de                                   # NaN iff de_ratio missing

    # Step 3: audit flags
    df["has_profitability_bucket"] = df["_prof_bucket"].notna()
    df["has_margin_bucket"]        = df["_margin_bucket"].notna()
    df["has_leverage_bucket"]      = df["_leverage_bucket"].notna()
    df["quality_bucket_count"] = (
        df["has_profitability_bucket"].astype(int) +
        df["has_margin_bucket"].astype(int) +
        df["has_leverage_bucket"].astype(int)
    )

    # Step 4: quality score — mean of available buckets, with minimum coverage rule
    # Rule: profitability bucket must exist AND at least one of margin or leverage
    quality_ok = (
        df["has_profitability_bucket"] &
        (df["has_margin_bucket"] | df["has_leverage_bucket"])
    )
    qual_frame = pd.DataFrame({
        "prof":   df["_prof_bucket"],
        "margin": df["_margin_bucket"],
        "lev":    df["_leverage_bucket"],
    })
    df["quality"] = qual_frame.mean(axis=1, skipna=True)
    df.loc[~quality_ok, "quality"] = np.nan   # enforce minimum coverage

    # Step 5: missing-data reason (only set for stocks without a quality score)
    def _quality_reason(row) -> Optional[str]:
        if pd.notna(row["quality"]):
            return None
        if not row["has_profitability_bucket"] and not row["has_margin_bucket"]:
            return "missing_profitability_and_margin"
        if not row["has_profitability_bucket"] and not row["has_leverage_bucket"]:
            return "missing_profitability_and_leverage"
        if not row["has_profitability_bucket"]:
            return "missing_profitability"
        return "insufficient_quality_inputs"

    df["quality_missing_reason"] = df.apply(_quality_reason, axis=1)

    return df


# ─── Ranking and alpha construction ──────────────────────────────────────────

def compute_rankings(df: pd.DataFrame,
                     vol_adjust: bool = True,
                     use_quality: bool = True,
                     use_tstats: bool = False,
                     w6: float = 0.4,
                     w12: float = 0.4,
                     w_quality: float = 0.2,
                     winsor_p: float = 2.0,
                     vol_floor: float = 0.05,
                     cluster_n: int = 100,
                     cluster_k: int = 10,
                     cluster_lookback: int = 252,
                     prices: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Cross-sectionally standardize factors and compute sleeve-based alpha scores.

    Sleeve formula
    --------------
    S = 0.5*z(s6) + 0.5*z(s12)          return-strength (Sharpe-adjusted momentum)
    T = 0.5*z(tstat6) + 0.5*z(tstat12)  trend-quality (OLS t-stat)
    Q = z(quality)                        quality composite

    alpha = (wS*S + wT*T + wQ*Q) / (wS+wT+wQ)   [when quality available]
    alpha = (wS*S + wT*T) / (wS+wT)              [when quality is NaN — renormalized]

    Per-stock renormalization prevents the missing-Q imputation (zero-fill) from
    artificially pulling a stock's alpha toward 0. alpha_formula tracks which
    formula was used per stock.
    """
    if df.empty:
        return df

    d = df.copy()

    def std_factor(series):
        v = series[series.notna()]
        if len(v) < 10:
            return series * 0
        out = series.copy()
        out[out.notna()] = zscore(winsorize(v, winsor_p))
        return out

    # ── Atomic z-scores ───────────────────────────────────────────────────────
    d["zS6"]  = std_factor(d["s6"])
    d["zS12"] = std_factor(d["s12"])
    d["zT6"]  = std_factor(d["tstat6"])
    d["zT12"] = std_factor(d["tstat12"])

    # zQ: standardize quality, preserving NaN for stocks without a quality score
    d["zQ"] = std_factor(d["quality"]) if use_quality else pd.Series(0.0, index=d.index)

    # Backward-compat aliases
    d["zM6"]      = d["zS6"]
    d["zM12"]     = d["zS12"]
    d["zQuality"] = d["zQ"]

    # ── Sleeve construction: 50/50 within each horizon pair ───────────────────
    d["sSleeve"] = 0.5 * d["zS6"].fillna(0) + 0.5 * d["zS12"].fillna(0)
    d["tSleeve"] = 0.5 * d["zT6"].fillna(0) + 0.5 * d["zT12"].fillna(0)
    d["qSleeve"] = d["zQ"].fillna(0)

    # ── Per-stock alpha with quality renormalization ───────────────────────────
    #
    # Stocks without a valid quality score are NOT penalized by forcing Q=0.
    # Instead the alpha formula is renormalized over available sleeves only:
    #   has_quality  → alpha = (wS*S + wT*T + wQ*Q) / (wS+wT+wQ)
    #   !has_quality → alpha = (wS*S + wT*T) / (wS+wT)
    has_quality = d["quality"].notna() & use_quality
    d["quality_missing"] = ~has_quality

    wS = w6
    wT = w12
    wQ = w_quality if use_quality else 0.0

    total_w_full = wS + wT + wQ if (wS + wT + wQ) > 0 else 1.0
    total_w_no_q = wS + wT      if (wS + wT)      > 0 else 1.0

    S = d["sSleeve"]
    T = d["tSleeve"]
    Q = d["qSleeve"]   # already 0-filled; has_quality mask ensures correct branch

    alpha_with_q    = (wS * S + wT * T + wQ * Q) / total_w_full
    alpha_without_q = (wS * S + wT * T)           / total_w_no_q

    d["alpha"]        = np.where(has_quality, alpha_with_q, alpha_without_q)
    d["alpha_formula"] = np.where(has_quality, "S+T+Q", "S+T")

    # ── Rank ──────────────────────────────────────────────────────────────────
    d = d.sort_values("alpha", ascending=False)
    d["rank"]       = np.arange(1, len(d) + 1)
    d["percentile"] = 100.0 * (1 - (d["rank"] - 1) / len(d))

    # ── Correlation clustering (top-N by alpha) ───────────────────────────────
    d["cluster"] = None
    if prices is not None and len(d) > 0:
        top_n        = d.head(cluster_n)["ticker"].tolist()
        valid_tickers = [t for t in top_n if t in prices.columns]
        if len(valid_tickers) >= cluster_k:
            try:
                sub  = prices[valid_tickers].tail(cluster_lookback)
                lr   = sub.pct_change().dropna()
                if lr.shape[0] > 30:
                    corr  = lr.corr().fillna(0).clip(-1, 1)
                    lr_z  = (lr - lr.mean()) / lr.std().clip(lower=1e-8)
                    feat  = lr_z.T.fillna(0).values
                    k     = min(cluster_k, len(valid_tickers))
                    clust = AgglomerativeClustering(
                        n_clusters=k,
                        metric="euclidean", linkage="ward"
                    )
                    labels    = clust.fit_predict(feat)
                    label_map = dict(zip(valid_tickers, labels.tolist()))
                    d["cluster"] = d["ticker"].map(label_map)

                    # Clustering audit log
                    sizes = {}
                    within_corrs = {}
                    for ci in range(k):
                        members = [t for t, lbl in label_map.items() if lbl == ci]
                        sizes[ci] = len(members)
                        if len(members) >= 2:
                            sub_corr = corr.loc[members, members]
                            mask = np.ones(sub_corr.shape, dtype=bool)
                            np.fill_diagonal(mask, False)
                            within_corrs[ci] = round(float(sub_corr.values[mask].mean()), 2)
                        else:
                            within_corrs[ci] = float("nan")
                    all_members_list = list(label_map.keys())
                    all_labels_arr   = np.array([label_map[t] for t in all_members_list])
                    cross_vals = []
                    n_m = len(all_members_list)
                    for i in range(n_m):
                        for j in range(i + 1, n_m):
                            if all_labels_arr[i] != all_labels_arr[j]:
                                cross_vals.append(corr.loc[all_members_list[i], all_members_list[j]])
                    avg_cross = round(float(np.mean(cross_vals)), 2) if cross_vals else float("nan")
                    logger.info(
                        "Clustering audit (k=%d, n=%d stocks):\n"
                        "  Cluster sizes: %s\n"
                        "  Avg within-cluster corr: %s\n"
                        "  Avg cross-cluster corr: %s",
                        k, len(valid_tickers), sizes, within_corrs, avg_cross
                    )
            except Exception as e:
                logger.error(f"Clustering error: {e}")

    return d


# ─── Universe filtering ───────────────────────────────────────────────────────

def apply_universe_filters(df: pd.DataFrame,
                           min_price:      float = 5.0,
                           min_adv:        float = 1e7,
                           min_market_cap: float = 1e9) -> pd.DataFrame:
    """
    Apply universe inclusion filters and log summary of exclusion reasons.

    Filters applied (in order)
    --------------------------
    1. Structure exclusion  — quoteType / exchange / name classification
    2. Price >= $5
    3. 63-day median ADV >= $10M   (computed from downloaded history)
    4. Market cap >= $1B

    Stocks failing any filter are dropped. Exclusion counts are logged per reason.
    """
    n_in = len(df)
    mask = pd.Series(True, index=df.index)

    # ── 1. Structure exclusion ────────────────────────────────────────────────
    if "structure_exclusion" in df.columns:
        struct_fail = df["structure_exclusion"].notna()
        if struct_fail.any():
            for reason in df.loc[struct_fail, "structure_exclusion"].unique():
                n = (df["structure_exclusion"] == reason).sum()
                logger.info(f"Universe exclusion [{reason}]: {n} tickers")
        mask &= ~struct_fail

    # ── 2. Price ──────────────────────────────────────────────────────────────
    if "price" in df.columns:
        price_fail = df["price"].fillna(0) < min_price
        if price_fail.any():
            logger.info(f"Universe exclusion [excluded_price]: {price_fail.sum()} tickers")
        mask &= ~price_fail

    # ── 3. Liquidity (ADV) ────────────────────────────────────────────────────
    if "adv" in df.columns:
        adv_fail = df["adv"].fillna(0) < min_adv
        if adv_fail.any():
            logger.info(f"Universe exclusion [excluded_liquidity]: {adv_fail.sum()} tickers")
        mask &= ~adv_fail

    # ── 4. Market cap ─────────────────────────────────────────────────────────
    if "market_cap" in df.columns:
        cap_fail = df["market_cap"].fillna(0) < min_market_cap
        if cap_fail.any():
            logger.info(f"Universe exclusion [excluded_market_cap]: {cap_fail.sum()} tickers")
        mask &= ~cap_fail

    result = df[mask].copy()
    logger.info(f"Universe filters: {n_in} candidates → {len(result)} qualifying stocks")
    return result


# ─── Public data accessors ────────────────────────────────────────────────────

def get_price_data() -> Optional[pd.DataFrame]:
    return _price_data


def get_meta_data() -> Optional[dict]:
    return _meta_data


def _print_audit_summary(price_data: pd.DataFrame, meta_data: dict,
                          factors_pre: pd.DataFrame, factors_post: pd.DataFrame) -> None:
    """
    Log a one-shot universe drop funnel + null-field audit to INFO.

    Called once per engine lifetime (guarded by _audit_printed). Captures:
    - Drop counts at each filter stage (structure, price, ADV, market-cap)
    - Null field counts across factors_pre
    - Quality and alpha formula breakdowns in factors_post
    """
    lines: list = ["", "═" * 60, "  UNIVERSE AUDIT SUMMARY", "═" * 60]

    # ── Drop funnel ────────────────────────────────────────────────────────────
    downloaded       = len(price_data.columns)
    pre_total        = len(factors_pre)
    dropped_history  = downloaded - pre_total   # had <252 days — skipped by compute_factors

    lines.append(f"  Downloaded tickers      : {downloaded:>6}")
    lines.append(f"  dropped_history_lt_252  : {dropped_history:>6}")
    lines.append(f"  compute_factors output  : {pre_total:>6}")
    lines.append("")

    # structure_exclusion breakdown
    if "structure_exclusion" in factors_pre.columns:
        se = factors_pre["structure_exclusion"]
        reasons = se[se.notna()].value_counts()
        for reason, cnt in reasons.items():
            lines.append(f"  {reason:<30}: {cnt:>6}")
        struct_pass = se.isna().sum()
        lines.append(f"  structure_pass          : {struct_pass:>6}")
    lines.append("")

    # Price / ADV / market-cap filters
    post_total = len(factors_post)
    struct_excl_n = (factors_pre["structure_exclusion"].notna().sum()
                     if "structure_exclusion" in factors_pre.columns else 0)
    price_adv_cap_dropped = pre_total - struct_excl_n - post_total
    lines.append(f"  dropped_price+adv+mcap  : {price_adv_cap_dropped:>6}")
    lines.append(f"  final_survivors         : {post_total:>6}")
    lines.append("")

    # ── Null field counts (factors_pre) ───────────────────────────────────────
    lines.append("  NULL FIELD COUNTS (factors_pre):")
    for col, label in [("market_cap", "missing_market_cap"),
                        ("sector",     "missing_sector"),
                        ("adv",        "missing_adv")]:
        if col in factors_pre.columns:
            n = factors_pre[col].isna().sum()
            lines.append(f"    {label:<30}: {n:>6}")

    # Meta field nulls (from _meta_data directly)
    if meta_data:
        miss_exchange   = sum(1 for m in meta_data.values() if not m.get("exchange"))
        miss_quote_type = sum(1 for m in meta_data.values() if not m.get("quote_type"))
        lines.append(f"    {'missing_exchange':<30}: {miss_exchange:>6}")
        lines.append(f"    {'missing_quote_type':<30}: {miss_quote_type:>6}")
    lines.append("")

    # ── Quality breakdown (factors_post) ──────────────────────────────────────
    lines.append("  QUALITY & ALPHA (factors_post):")
    if "quality" in factors_post.columns:
        miss_qual = factors_post["quality"].isna().sum()
        lines.append(f"    {'missing_quality':<30}: {miss_qual:>6}")
    if "alpha_formula" in factors_post.columns:
        st_only = (factors_post["alpha_formula"] == "S+T").sum()
        lines.append(f"    {'alpha_st_only (no Q)':<30}: {st_only:>6}")
    lines.append("═" * 60)

    for line in lines:
        logger.info(line)


def get_ranked_data(params: dict) -> Optional[pd.DataFrame]:
    global _rankings_cache, _last_params, _price_data, _meta_data, _dollar_volume, _audit_printed

    if _price_data is None:
        return None

    cache_key = json.dumps(params, sort_keys=True)
    if _rankings_cache is not None and _last_params == cache_key:
        return _rankings_cache

    factors = compute_factors(
        _price_data, _meta_data,
        dollar_volume=_dollar_volume,
        vol_floor=params.get("vol_floor", 0.05),
        winsor_p=params.get("winsor_p", 2.0),
    )

    if factors.empty:
        return factors

    factors_pre = factors.copy()   # snapshot before filtering (for audit)
    factors = apply_universe_filters(factors)

    if not _audit_printed:
        try:
            _print_audit_summary(_price_data, _meta_data, factors_pre, factors)
        except Exception as e:
            logger.warning(f"Audit summary failed: {e}")
        _audit_printed = True

    ranked = compute_rankings(
        factors,
        vol_adjust=params.get("vol_adjust", True),
        use_quality=params.get("use_quality", True),
        use_tstats=params.get("use_tstats", False),
        w6=params.get("w6", 0.4),
        w12=params.get("w12", 0.4),
        w_quality=params.get("w_quality", 0.2),
        winsor_p=params.get("winsor_p", 2.0),
        vol_floor=params.get("vol_floor", 0.05),
        cluster_n=params.get("cluster_n", 100),
        cluster_k=params.get("cluster_k", 10),
        cluster_lookback=params.get("cluster_lookback", 252),
        prices=_price_data,
    )

    _rankings_cache = ranked
    _last_params    = cache_key
    return ranked


# ─── Portfolio risk ───────────────────────────────────────────────────────────

def compute_portfolio_risk(tickers: list, weights: list, lookback: int = 252) -> dict:
    """Compute portfolio volatility and average pairwise correlation."""
    global _price_data

    if _price_data is None:
        return {"error": "Data not loaded"}

    valid        = [(t, w) for t, w in zip(tickers, weights) if t in _price_data.columns]
    if not valid:
        return {"error": "No valid tickers in price data"}

    tickers_v  = [x[0] for x in valid]
    weights_v  = np.array([x[1] for x in valid])
    weights_v /= weights_v.sum()

    prices_sub  = _price_data[tickers_v].tail(lookback)
    log_returns = np.log(prices_sub / prices_sub.shift(1)).dropna()

    vols     = log_returns.std() * np.sqrt(252)
    cov      = log_returns.cov() * 252
    port_vol = float(np.sqrt(weights_v @ cov.values @ weights_v))

    corr = log_returns.corr()
    n    = len(tickers_v)
    if n > 1:
        mask     = np.ones((n, n), dtype=bool)
        np.fill_diagonal(mask, False)
        avg_corr = float(corr.values[mask].mean())
    else:
        avg_corr = 1.0

    return {
        "port_vol": port_vol,
        "avg_corr": avg_corr,
        "vols":     {t: float(vols[t]) for t in tickers_v},
        "weights":  {t: float(w)       for t, w in zip(tickers_v, weights_v)},
    }


# ─── Initial data load ────────────────────────────────────────────────────────

def initial_data_load():
    """
    Background thread: load price history, dollar volume, and metadata.

    Warm start (disk cache present)
    --------------------------------
    1. Restore _price_data + _dollar_volume from PRICE_CACHE_KEY (8h TTL)
    2. Restore _meta_data from META_CACHE_KEY (24h TTL)
       — if meta cache is missing, fetch and re-cache meta only (prices preserved)
    3. Return immediately; engine is ready

    Cold start (no cache)
    ----------------------
    1. Download 2y of Close + Volume for _fetch_universe() tickers in batches of 50
    2. Build price DataFrame and dollar-volume DataFrame
    3. Fetch metadata concurrently (10 workers)
    4. Write price+dollar-volume to PRICE_CACHE_KEY (8h)
    5. Write metadata to META_CACHE_KEY (24h)
    """
    global _price_data, _meta_data, _dollar_volume

    # Always call _fetch_universe early so nasdaq_meta_v1 is populated
    # (fast when universe_v1 is cached; triggers NASDAQ API fetch when not)
    _fetch_universe()

    # ── Try price + dollar-volume cache ──────────────────────────────────────
    price_cached = cache.get(PRICE_CACHE_KEY)
    if price_cached:
        logger.info("Restoring price data from disk cache")
        update_status("loading", "Restoring from cache...", progress=0.5)
        try:
            _price_data, _dollar_volume = price_cached
            n = len(_price_data.columns)

            # Try metadata cache (separate TTL)
            meta_cached = cache.get(META_CACHE_KEY)
            if meta_cached and isinstance(meta_cached, dict) and len(meta_cached) > 0:
                coverage = sum(1 for t in _price_data.columns if t in meta_cached) / n
                if coverage >= 0.8:
                    _meta_data = meta_cached
                    logger.info(f"Metadata from cache ({coverage:.0%} coverage)")
                else:
                    logger.info(f"Metadata cache coverage low ({coverage:.0%}), re-fetching")
                    _meta_data = load_meta_with_info(list(_price_data.columns))
                    cache.set(META_CACHE_KEY, _meta_data, expire=META_CACHE_TTL)
            else:
                logger.info("Metadata cache miss, fetching...")
                update_status("loading", "Loading metadata (cache miss)...", progress=0.75)
                _meta_data = load_meta_with_info(list(_price_data.columns))
                cache.set(META_CACHE_KEY, _meta_data, expire=META_CACHE_TTL)

            update_status("ready",
                          f"Ready. {n} stocks loaded from cache.",
                          progress=1.0, total=n, loaded=n)
            return
        except Exception as e:
            logger.error(f"Cache restore failed: {e}; falling back to full download")

    # ── Cold start: full download ─────────────────────────────────────────────
    tickers = _fetch_universe()
    logger.info(f"Cold start: downloading data for ~{len(tickers)} tickers")
    update_status("loading", f"Starting download for {len(tickers)} stocks...",
                  progress=0, total=len(tickers), loaded=0)

    all_close, all_dollar_vol, failed = load_data_batch(tickers, batch_size=100)

    if not all_close:
        update_status("error", "Failed to load any price data. Check network connectivity.")
        return

    logger.info(f"Downloaded {len(all_close)} tickers; {len(failed)} failed")

    # Build price DataFrame
    prices = pd.concat([s.rename(t) for t, s in all_close.items()], axis=1)
    prices = prices.sort_index().dropna(how="all").ffill(limit=5)
    valid_cols = [c for c in prices.columns if prices[c].count() >= 252]
    prices = prices[valid_cols]

    # Build dollar-volume DataFrame (aligned to valid price tickers)
    dv_series = {t: s for t, s in all_dollar_vol.items() if t in valid_cols}
    if dv_series:
        dollar_volume = pd.concat(
            [s.rename(t) for t, s in dv_series.items()], axis=1
        ).sort_index().ffill(limit=5).reindex(columns=valid_cols)
    else:
        dollar_volume = pd.DataFrame(index=prices.index, columns=prices.columns)

    logger.info(f"Price matrix: {prices.shape}; dollar-volume: {dollar_volume.shape}")

    # Load metadata (concurrent, 24h cache)
    update_status("loading",
                  f"Loading sector and quality data for {len(valid_cols)} stocks...",
                  progress=0.75, total=len(valid_cols), loaded=len(valid_cols))
    meta = load_meta_with_info(valid_cols)

    _price_data    = prices
    _dollar_volume = dollar_volume
    _meta_data     = meta

    # Persist to disk
    try:
        cache.set(PRICE_CACHE_KEY, (_price_data, _dollar_volume), expire=PRICE_CACHE_TTL)
        cache.set(META_CACHE_KEY,  _meta_data,                    expire=META_CACHE_TTL)
    except Exception as e:
        logger.error(f"Cache write failed: {e}")

    update_status("ready",
                  f"Ready. {len(valid_cols)} stocks loaded.",
                  progress=1.0, total=len(valid_cols), loaded=len(valid_cols))
    logger.info("Data load complete")


def start_background_load():
    """Start data loading in a daemon background thread."""
    t = threading.Thread(target=initial_data_load, daemon=True)
    t.start()
