"""
Equity Ranking Engine — Performance Edition
=============================================
Fetches real market data from Yahoo Finance, computes momentum and quality
factors using a 3-sleeve alpha model, and provides portfolio risk metrics.

Architecture
------------
- Async price downloader via PriceDataAdapter (aiohttp, 50 concurrent connections)
- Vectorized factor computation (numpy matrix ops, no per-ticker loops)
- Three-layer cache: factors → rankings → clustering
- Two-stage startup: Stage 1 (prices+essential meta) → usable; Stage 2 (quality enrichment) → background
- Per-phase timing instrumentation

Universe construction
---------------------
At cold start, the live universe is built dynamically from the NASDAQ screener API:
  1. NASDAQ API  : fetches NYSE- and NASDAQ-listed stocks with $2B market-cap pre-filter
  2. Format filter: non-empty symbol, <= 5 chars, alphabetic only.
  3. ADR/foreign merge: curated ADR/foreign names always included.
  4. classify_ticker(): excludes ETFs, funds, OTC, partnerships, SPACs at meta-fetch time.
  5. Price/ADV/cap filters: price >= $5, 63-day median ADV >= $10M, market cap >= $1B.

Quality sleeve
--------------
Three buckets, each winsorized then z-scored cross-sectionally:
  Profitability : mean(z_roe, z_roa)
  Margin        : mean(z_gross_margin, z_op_margin)
  Leverage      : -z(de_ratio)
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
PRICE_CACHE_TTL    = 8  * 3600
META_CACHE_TTL     = 24 * 3600
UNIVERSE_CACHE_TTL = 24 * 3600
QUALITY_CACHE_TTL  = 24 * 3600
PRICE_CACHE_KEY    = "price_data_v5"
META_CACHE_KEY     = "meta_data_v2"
UNIVERSE_CACHE_KEY = "universe_v1"
NASDAQ_META_KEY    = "nasdaq_meta_v1"
QUALITY_CACHE_KEY  = "quality_data_v1"

cache = diskcache.Cache(CACHE_DIR)

_status = {
    "status": "loading",
    "message": "Initializing...",
    "progress": 0,
    "total": 0,
    "loaded": 0,
    "cached_at": None,
    "enrichment": "pending",
    "quality_coverage": "",
    "timings": {},
}
_status_lock = threading.Lock()

# ─── Offline fallback universe ────────────────────────────────────────────────
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

_FALLBACK_TICKERS = sorted(list(set(
    t for t in _FALLBACK_TICKERS
    if t and len(t) <= 5 and t.replace(".", "").isalpha()
)))

# ─── Dynamic universe fetch ───────────────────────────────────────────────────

_NASDAQ_API = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange={exchange}&download=true"
_NASDAQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
_NASDAQ_TIMEOUT = 15

_ADR_ANCHORS = {
    "TSM", "ASML", "ARM", "RIO", "BHP", "VALE", "NVO", "SAP", "TM",
    "HMC", "SONY", "SNY", "AZN", "GSK", "BTI", "SHOP", "SAN",
}


def _fetch_universe() -> list:
    cached = cache.get(UNIVERSE_CACHE_KEY)
    if cached and isinstance(cached, list) and len(cached) > 0:
        logger.info(f"Universe from cache: {len(cached)} tickers")
        return cached

    try:
        tickers: set   = set()
        nasdaq_meta: dict = {}

        for exchange in ("NYSE", "NASDAQ"):
            url = _NASDAQ_API.format(exchange=exchange)
            resp = requests.get(url, headers=_NASDAQ_HEADERS, timeout=_NASDAQ_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            ddata = data.get("data", {})
            rows = ddata.get("rows") or ddata.get("table", {}).get("rows")
            if not rows or not isinstance(rows, list):
                raise ValueError(f"NASDAQ API ({exchange}): empty or missing 'rows' in response")

            exchange_count = 0
            for row in rows:
                symbol = (row.get("symbol") or "").strip().upper()
                if not symbol or len(symbol) > 5 or not symbol.isalpha():
                    continue
                raw_cap = row.get("marketCap") or row.get("market_cap") or ""
                try:
                    cap = float(str(raw_cap).replace(",", ""))
                except (ValueError, TypeError):
                    cap = 0.0
                if cap < 2_000_000_000:
                    continue
                tickers.add(symbol)
                exchange_count += 1
                is_etf = str(row.get("etf") or "").lower() in ("true", "1", "yes", "y")
                nasdaq_meta[symbol] = {
                    "market_cap": cap,
                    "sector":     row.get("sector") or None,
                    "industry":   row.get("industry") or None,
                    "name":       row.get("name") or symbol,
                    "exchange":   exchange,
                    "is_etf":     is_etf,
                }
            logger.info(f"NASDAQ API ({exchange}): {exchange_count} tickers after $2B filter")

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
        cache.set(NASDAQ_META_KEY, nasdaq_meta, expire=UNIVERSE_CACHE_TTL)
        logger.info(f"NASDAQ metadata cached for {len(nasdaq_meta)} tickers")
        return universe

    except Exception as e:
        logger.warning(f"NASDAQ universe fetch failed ({e}); falling back to _FALLBACK_TICKERS")
        return _FALLBACK_TICKERS


# ─── Globals ──────────────────────────────────────────────────────────────────
_price_data:    Optional[pd.DataFrame] = None
_dollar_volume: Optional[pd.DataFrame] = None
_meta_data:     Optional[dict]         = None
_audit_printed  = False

_quality_epoch  = 0
_factor_cache   = None
_factor_key     = None
_ranking_cache  = None
_ranking_key    = None
_cluster_cache  = None
_cluster_key    = None
_cache_lock     = threading.Lock()

_timings: dict  = {}


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
        d = dict(_status)
        d["timings"] = dict(_timings)
        return d


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
    s = pd.to_numeric(series, errors="coerce")
    valid = s.notna()
    if valid.sum() < 10:
        return pd.Series(np.nan, index=s.index, dtype=float)
    out = s.copy()
    out[valid] = zscore(winsorize(s[valid], winsor_p))
    return out


# ─── Universe structure classification ────────────────────────────────────────

_FUND_TYPES       = {"ETF", "MUTUALFUND", "CEF"}
_EQUITY_TYPES     = {"EQUITY", "ADR", ""}
_OTC_EXCHANGES    = {"OTC", "PINK", "OTCBB", "OTCMKTS", "OTCQB", "OTCQX", "OTHER OTC"}
_PARTNERSHIP_MARK = {" LP", " L.P.", " LP.", "LIMITED PARTNERSHIP", " MLP",
                     " PARTNERS L", " PARTNERS,"}
_SPAC_MARKERS     = {"ACQUISITION CORP", "BLANK CHECK", " ACQ CORP",
                     "SPECIAL PURPOSE", " SPAC "}


def classify_ticker(ticker: str, meta: dict) -> Optional[str]:
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


# ─── Vectorized OLS t-stat ───────────────────────────────────────────────────

def _batch_ols_tstat(log_prices_window: pd.DataFrame) -> pd.Series:
    """
    Vectorized OLS t-stat computation across all tickers simultaneously.
    Replaces ~1,800 sequential scipy.stats.linregress calls with a single
    matrix multiplication. Exact for tickers with full data; negligible
    approximation for the rare tickers with NaN gaps (forward-filled input).
    """
    Y = log_prices_window.values.copy()
    T, N = Y.shape
    if T < 10:
        return pd.Series(np.nan, index=log_prices_window.columns)

    valid_count = np.isfinite(Y).sum(axis=0)
    too_few = valid_count < 10

    col_means = np.nanmean(Y, axis=0)
    nan_mask = ~np.isfinite(Y)
    if nan_mask.any():
        col_idx = np.where(nan_mask)[1]
        Y[nan_mask] = col_means[col_idx]

    x = np.arange(T, dtype=np.float64)
    x -= x.mean()
    x_sq_sum = (x ** 2).sum()

    y_means = Y.mean(axis=0)
    slopes = (x @ Y) / x_sq_sum

    predicted = np.outer(x, slopes) + y_means
    residuals = Y - predicted
    sse = (residuals ** 2).sum(axis=0)

    denom = np.maximum((valid_count - 2) * x_sq_sum, 1e-20)
    std_errs = np.sqrt(sse / denom)

    tstats = np.where(std_errs > 1e-10, slopes / std_errs, np.nan)
    tstats[too_few] = np.nan

    return pd.Series(tstats, index=log_prices_window.columns)


# ─── Vectorized factor computation ──────────────────────────────────────────

def compute_factors_vectorized(prices: pd.DataFrame,
                               meta: dict,
                               dollar_volume: Optional[pd.DataFrame] = None,
                               vol_floor: float = 0.05,
                               winsor_p: float = 2.0) -> pd.DataFrame:
    """
    Compute per-stock factor scores and quality buckets using vectorized ops.
    Replaces the old per-ticker Python loop with numpy matrix operations.
    Preserves exact same factor definitions and quality bucket construction.
    """
    t0 = time.time()

    n_days, n_tickers = prices.shape
    if n_tickers == 0:
        return pd.DataFrame()

    log_prices  = np.log(prices.clip(lower=1e-8))
    log_returns = log_prices.diff()

    tickers = prices.columns.tolist()

    valid_counts = prices.notna().sum(axis=0)
    has_252 = valid_counts >= 252
    has_253 = valid_counts >= 253
    has_127 = valid_counts >= 127
    has_126 = valid_counts >= 126
    has_22  = valid_counts >= 22

    lp_last = log_prices.iloc[-1]

    r1  = np.where(has_22,  lp_last - log_prices.iloc[-22],  np.nan)
    r6  = np.where(has_127, lp_last - log_prices.iloc[-127], np.nan)
    r12 = np.where(has_253, lp_last - log_prices.iloc[-253], np.nan)

    r1_s  = pd.Series(r1,  index=tickers)
    r6_s  = pd.Series(r6,  index=tickers)
    r12_s = pd.Series(r12, index=tickers)

    m6  = r6_s  - r1_s
    m12 = r12_s - r1_s

    sigma6  = log_returns.iloc[-126:].std() * np.sqrt(252)
    sigma12 = log_returns.std() * np.sqrt(252)
    sigma6  = sigma6.where(has_126, np.nan)

    s6  = m6  / sigma6.clip(lower=vol_floor)
    s12 = m12 / sigma12.clip(lower=vol_floor)

    t_ols = time.time()
    tstat12 = _batch_ols_tstat(log_prices)
    tstat6  = _batch_ols_tstat(log_prices.iloc[-126:])
    tstat6  = tstat6.where(has_126, np.nan)
    t_ols_elapsed = time.time() - t_ols

    price_last = np.exp(lp_last)

    if dollar_volume is not None:
        dv_63 = dollar_volume.iloc[-63:]
        adv = dv_63.median()
        short_mask = dv_63.notna().sum() < 21
        if short_mask.any():
            dv_21 = dollar_volume.iloc[-21:]
            adv_short = dv_21.median()
            adv = adv.where(~short_mask, adv_short)
    else:
        adv = pd.Series(np.nan, index=tickers)

    meta_names     = []
    meta_sectors   = []
    meta_industries = []
    meta_mcaps     = []
    meta_roes      = []
    meta_roas      = []
    meta_gm        = []
    meta_om        = []
    meta_de        = []
    struct_excl    = []

    for t in tickers:
        m = meta.get(t, {})
        meta_names.append(m.get("name", t))
        meta_sectors.append(m.get("sector"))
        meta_industries.append(m.get("industry"))
        meta_mcaps.append(m.get("market_cap"))
        meta_roes.append(m.get("roe"))
        meta_roas.append(m.get("roa"))
        meta_gm.append(m.get("gross_margin"))
        meta_om.append(m.get("op_margin"))
        meta_de.append(m.get("de_ratio"))
        struct_excl.append(classify_ticker(t, m))

        if not np.isfinite(adv.get(t, np.nan)) or adv.get(t, 0) == 0:
            avg_vol = m.get("avg_volume")
            if avg_vol and np.isfinite(price_last.get(t, 0)):
                adv[t] = price_last[t] * avg_vol

    df = pd.DataFrame({
        "ticker":    tickers,
        "name":      meta_names,
        "sector":    meta_sectors,
        "industry":  meta_industries,
        "price":     price_last.values,
        "market_cap": meta_mcaps,
        "adv":       adv.values,
        "r1":        r1,
        "m6":        m6.values,
        "m12":       m12.values,
        "sigma6":    sigma6.values,
        "sigma12":   sigma12.values,
        "s6":        s6.values,
        "s12":       s12.values,
        "tstat6":    tstat6.values,
        "tstat12":   tstat12.values,
        "_roe":      meta_roes,
        "_roa":      meta_roas,
        "_gross_margin": meta_gm,
        "_op_margin":    meta_om,
        "_de_ratio":     meta_de,
        "structure_exclusion": struct_excl,
    })

    min_hist = has_252.values
    df = df[min_hist].reset_index(drop=True)

    if df.empty:
        return df

    z_roe   = std_cs(df["_roe"],          winsor_p)
    z_roa   = std_cs(df["_roa"],          winsor_p)
    z_gross = std_cs(df["_gross_margin"], winsor_p)
    z_op    = std_cs(df["_op_margin"],    winsor_p)
    z_de    = std_cs(df["_de_ratio"],     winsor_p)

    prof_frame   = pd.DataFrame({"z_roe": z_roe,   "z_roa": z_roa})
    margin_frame = pd.DataFrame({"z_gross": z_gross, "z_op": z_op})

    df["_prof_bucket"]     = prof_frame.mean(axis=1, skipna=True)
    df["_margin_bucket"]   = margin_frame.mean(axis=1, skipna=True)
    df["_leverage_bucket"] = -z_de

    df["has_profitability_bucket"] = df["_prof_bucket"].notna()
    df["has_margin_bucket"]        = df["_margin_bucket"].notna()
    df["has_leverage_bucket"]      = df["_leverage_bucket"].notna()
    df["quality_bucket_count"] = (
        df["has_profitability_bucket"].astype(int) +
        df["has_margin_bucket"].astype(int) +
        df["has_leverage_bucket"].astype(int)
    )

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
    df.loc[~quality_ok, "quality"] = np.nan

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

    elapsed = time.time() - t0
    _timings["compute_factors"] = round(elapsed, 3)
    _timings["ols_tstat_vectorized"] = round(t_ols_elapsed, 3)
    logger.info(
        f"compute_factors_vectorized: {elapsed:.3f}s for {len(df)} stocks "
        f"(OLS: {t_ols_elapsed:.3f}s)"
    )

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
                     vol_floor: float = 0.05) -> pd.DataFrame:
    """
    Cross-sectionally standardize factors and compute sleeve-based alpha scores.
    Clustering is handled separately by compute_clustering().
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

    d["zS6"]  = std_factor(d["s6"])
    d["zS12"] = std_factor(d["s12"])
    d["zT6"]  = std_factor(d["tstat6"])
    d["zT12"] = std_factor(d["tstat12"])

    d["zQ"] = std_factor(d["quality"]) if use_quality else pd.Series(0.0, index=d.index)

    d["zM6"]      = d["zS6"]
    d["zM12"]     = d["zS12"]
    d["zQuality"] = d["zQ"]

    d["sSleeve"] = 0.5 * d["zS6"].fillna(0) + 0.5 * d["zS12"].fillna(0)
    d["tSleeve"] = 0.5 * d["zT6"].fillna(0) + 0.5 * d["zT12"].fillna(0)
    d["qSleeve"] = d["zQ"].fillna(0)

    has_quality = d["quality"].notna() & use_quality
    d["quality_missing"] = ~has_quality

    wS = w6
    wT = w12
    wQ = w_quality if use_quality else 0.0

    total_w_full = wS + wT + wQ if (wS + wT + wQ) > 0 else 1.0
    total_w_no_q = wS + wT      if (wS + wT)      > 0 else 1.0

    S = d["sSleeve"]
    T = d["tSleeve"]
    Q = d["qSleeve"]

    alpha_with_q    = (wS * S + wT * T + wQ * Q) / total_w_full
    alpha_without_q = (wS * S + wT * T)           / total_w_no_q

    d["alpha"]        = np.where(has_quality, alpha_with_q, alpha_without_q)
    d["alpha_formula"] = np.where(has_quality, "S+T+Q", "S+T")

    d = d.sort_values("alpha", ascending=False)
    d["rank"]       = np.arange(1, len(d) + 1)
    d["percentile"] = 100.0 * (1 - (d["rank"] - 1) / len(d))

    d["cluster"] = None

    return d


def compute_clustering(d: pd.DataFrame,
                       prices: pd.DataFrame,
                       cluster_n: int = 100,
                       cluster_k: int = 10,
                       cluster_lookback: int = 252) -> pd.DataFrame:
    """Run Ward clustering on top-N alpha stocks. Separated from rankings for caching."""
    t0 = time.time()
    result = d.copy()

    if prices is None or len(result) == 0:
        return result

    top_n        = result.head(cluster_n)["ticker"].tolist()
    valid_tickers = [t for t in top_n if t in prices.columns]

    if len(valid_tickers) < cluster_k:
        _timings["clustering"] = round(time.time() - t0, 3)
        return result

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
            result["cluster"] = result["ticker"].map(label_map)

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

    _timings["clustering"] = round(time.time() - t0, 3)
    return result


# ─── Universe filtering ───────────────────────────────────────────────────────

def apply_universe_filters(df: pd.DataFrame,
                           min_price:      float = 5.0,
                           min_adv:        float = 1e7,
                           min_market_cap: float = 1e9) -> pd.DataFrame:
    n_in = len(df)
    mask = pd.Series(True, index=df.index)

    if "structure_exclusion" in df.columns:
        struct_fail = df["structure_exclusion"].notna()
        if struct_fail.any():
            for reason in df.loc[struct_fail, "structure_exclusion"].unique():
                n = (df["structure_exclusion"] == reason).sum()
                logger.info(f"Universe exclusion [{reason}]: {n} tickers")
        mask &= ~struct_fail

    if "price" in df.columns:
        price_fail = df["price"].fillna(0) < min_price
        if price_fail.any():
            logger.info(f"Universe exclusion [excluded_price]: {price_fail.sum()} tickers")
        mask &= ~price_fail

    if "adv" in df.columns:
        adv_fail = df["adv"].fillna(0) < min_adv
        if adv_fail.any():
            logger.info(f"Universe exclusion [excluded_liquidity]: {adv_fail.sum()} tickers")
        mask &= ~adv_fail

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
    lines: list = ["", "═" * 60, "  UNIVERSE AUDIT SUMMARY", "═" * 60]

    downloaded       = len(price_data.columns)
    pre_total        = len(factors_pre)
    dropped_history  = downloaded - pre_total

    lines.append(f"  Downloaded tickers      : {downloaded:>6}")
    lines.append(f"  dropped_history_lt_252  : {dropped_history:>6}")
    lines.append(f"  compute_factors output  : {pre_total:>6}")
    lines.append("")

    if "structure_exclusion" in factors_pre.columns:
        se = factors_pre["structure_exclusion"]
        reasons = se[se.notna()].value_counts()
        for reason, cnt in reasons.items():
            lines.append(f"  {reason:<30}: {cnt:>6}")
        struct_pass = se.isna().sum()
        lines.append(f"  structure_pass          : {struct_pass:>6}")
    lines.append("")

    post_total = len(factors_post)
    struct_excl_n = (factors_pre["structure_exclusion"].notna().sum()
                     if "structure_exclusion" in factors_pre.columns else 0)
    price_adv_cap_dropped = pre_total - struct_excl_n - post_total
    lines.append(f"  dropped_price+adv+mcap  : {price_adv_cap_dropped:>6}")
    lines.append(f"  final_survivors         : {post_total:>6}")
    lines.append("")

    lines.append("  NULL FIELD COUNTS (factors_pre):")
    for col, label in [("market_cap", "missing_market_cap"),
                        ("sector",     "missing_sector"),
                        ("adv",        "missing_adv")]:
        if col in factors_pre.columns:
            n = factors_pre[col].isna().sum()
            lines.append(f"    {label:<30}: {n:>6}")

    if meta_data:
        miss_exchange   = sum(1 for m in meta_data.values() if not m.get("exchange"))
        miss_quote_type = sum(1 for m in meta_data.values() if not m.get("quote_type"))
        lines.append(f"    {'missing_exchange':<30}: {miss_exchange:>6}")
        lines.append(f"    {'missing_quote_type':<30}: {miss_quote_type:>6}")
    lines.append("")

    lines.append("  QUALITY & ALPHA (factors_post):")
    if "quality" in factors_post.columns:
        miss_qual = factors_post["quality"].isna().sum()
        lines.append(f"    {'missing_quality':<30}: {miss_qual:>6}")
    if "alpha_formula" in factors_post.columns:
        st_only = (factors_post["alpha_formula"] == "S+T").sum()
        lines.append(f"    {'alpha_st_only (no Q)':<30}: {st_only:>6}")

    lines.append("")
    lines.append("  TIMINGS:")
    for k, v in _timings.items():
        lines.append(f"    {k:<30}: {v:>8.3f}s")

    enrich = _status.get("enrichment", "unknown")
    qcov = _status.get("quality_coverage", "")
    lines.append("")
    lines.append(f"  ENRICHMENT STATUS: {enrich}")
    if qcov:
        lines.append(f"  QUALITY COVERAGE:  {qcov}")
    lines.append("═" * 60)

    for line in lines:
        logger.info(line)


# ─── Three-layer cached ranking pipeline ─────────────────────────────────────

def get_ranked_data(params: dict) -> Optional[pd.DataFrame]:
    global _factor_cache, _factor_key
    global _ranking_cache, _ranking_key
    global _cluster_cache, _cluster_key
    global _price_data, _meta_data, _dollar_volume, _audit_printed

    if _price_data is None:
        return None

    t0 = time.time()
    vol_floor = params.get("vol_floor", 0.05)
    winsor_p  = params.get("winsor_p", 2.0)
    w6        = params.get("w6", 0.4)
    w12       = params.get("w12", 0.4)
    w_quality = params.get("w_quality", 0.2)
    use_quality = params.get("use_quality", True)
    use_tstats  = params.get("use_tstats", False)
    vol_adjust  = params.get("vol_adjust", True)
    cluster_n   = params.get("cluster_n", 100)
    cluster_k   = params.get("cluster_k", 10)
    cluster_lookback = params.get("cluster_lookback", 252)

    fk = json.dumps({"vol_floor": vol_floor, "winsor_p": winsor_p,
                      "qe": _quality_epoch}, sort_keys=True)
    rk = json.dumps({"fk": fk, "w6": w6, "w12": w12, "wq": w_quality,
                      "uq": use_quality, "ut": use_tstats, "va": vol_adjust,
                      "wp": winsor_p}, sort_keys=True)
    ck = json.dumps({"rk": rk, "cn": cluster_n, "ck": cluster_k,
                      "cl": cluster_lookback}, sort_keys=True)

    cache_info = []

    with _cache_lock:
        if _factor_cache is not None and _factor_key == fk:
            factors_filtered = _factor_cache
            cache_info.append("factors:HIT")
        else:
            factors_raw = compute_factors_vectorized(
                _price_data, _meta_data,
                dollar_volume=_dollar_volume,
                vol_floor=vol_floor,
                winsor_p=winsor_p,
            )
            if factors_raw.empty:
                return factors_raw

            factors_pre = factors_raw.copy()
            factors_filtered = apply_universe_filters(factors_raw)

            if not _audit_printed:
                try:
                    _print_audit_summary(_price_data, _meta_data, factors_pre, factors_filtered)
                except Exception as e:
                    logger.warning(f"Audit summary failed: {e}")
                _audit_printed = True

            _factor_cache = factors_filtered
            _factor_key = fk
            _ranking_cache = None
            _ranking_key = None
            _cluster_cache = None
            _cluster_key = None
            cache_info.append("factors:MISS")

        if _ranking_cache is not None and _ranking_key == rk:
            ranked = _ranking_cache
            cache_info.append("rankings:HIT")
        else:
            ranked = compute_rankings(
                factors_filtered,
                vol_adjust=vol_adjust,
                use_quality=use_quality,
                use_tstats=use_tstats,
                w6=w6, w12=w12, w_quality=w_quality,
                winsor_p=winsor_p, vol_floor=vol_floor,
            )
            _ranking_cache = ranked
            _ranking_key = rk
            _cluster_cache = None
            _cluster_key = None
            cache_info.append("rankings:MISS")

        if _cluster_cache is not None and _cluster_key == ck:
            result = _cluster_cache
            cache_info.append("clustering:HIT")
        else:
            result = compute_clustering(
                ranked, _price_data,
                cluster_n=cluster_n,
                cluster_k=cluster_k,
                cluster_lookback=cluster_lookback,
            )
            _cluster_cache = result
            _cluster_key = ck
            cache_info.append("clustering:MISS")

    elapsed = time.time() - t0
    _timings["get_ranked_data"] = round(elapsed, 3)
    logger.info(f"get_ranked_data: {elapsed:.3f}s [{', '.join(cache_info)}]")
    return result


# ─── Portfolio risk ───────────────────────────────────────────────────────────

def compute_portfolio_risk(tickers: list, weights: list, lookback: int = 252) -> dict:
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


# ─── yfinance fallback for price download ─────────────────────────────────────

def _yf_load_data_batch(tickers: list, batch_size: int = 100) -> tuple:
    """Fallback: sequential yfinance.download batches (slow but reliable)."""
    all_close: dict      = {}
    all_dollar_vol: dict = {}
    failed: list         = []

    batches       = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    total_batches = len(batches)

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
            logger.error(f"yf batch {bi} download error: {e}")
            failed.extend(batch)

        loaded   = len(all_close)
        progress = (bi + 1) / total_batches
        update_status("loading",
                      f"Downloading (yf fallback) ({loaded}/{len(tickers)})...",
                      progress=progress, total=len(tickers), loaded=loaded)

    return all_close, all_dollar_vol, failed


# ─── Quality metadata enrichment ──────────────────────────────────────────────

def _fetch_one_quality(ticker: str) -> tuple:
    try:
        info = yf.Ticker(ticker).info
        return ticker, {
            "roe":          info.get("returnOnEquity"),
            "roa":          info.get("returnOnAssets"),
            "gross_margin": info.get("grossMargins"),
            "op_margin":    info.get("operatingMargins"),
            "de_ratio":     info.get("debtToEquity"),
        }, True
    except Exception:
        return ticker, {}, False


def _background_quality_enrichment(tickers: list):
    """
    Stage 2: Load quality fundamental data in background.
    This runs AFTER the engine is already marked 'ready' with S+T rankings.
    When complete, it invalidates the factor cache so next ranking request
    includes quality data in the Q sleeve.
    """
    global _meta_data, _quality_epoch, _factor_cache, _factor_key, _audit_printed

    t0 = time.time()

    with _status_lock:
        _status["enrichment"] = "loading"
        _status["quality_coverage"] = f"0/{len(tickers)}"

    quality_cached = cache.get(QUALITY_CACHE_KEY)
    if quality_cached and isinstance(quality_cached, dict):
        cached_count = sum(1 for t in tickers if t in quality_cached)
        coverage = cached_count / len(tickers) if tickers else 0
        if coverage >= 0.7:
            logger.info(f"Quality data from cache: {cached_count}/{len(tickers)} ({coverage:.0%})")
            for t in tickers:
                if t in quality_cached and t in _meta_data:
                    for k, v in quality_cached[t].items():
                        if v is not None and _meta_data[t].get(k) is None:
                            _meta_data[t][k] = v
            with _cache_lock:
                _quality_epoch += 1
                _factor_cache = None
                _factor_key = None
                _audit_printed = False
            with _status_lock:
                _status["enrichment"] = "complete"
                _status["quality_coverage"] = f"{cached_count}/{len(tickers)}"
            _timings["quality_enrichment"] = round(time.time() - t0, 3)
            return

    quality_store = {}
    done = 0
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_map = {executor.submit(_fetch_one_quality, t): t for t in tickers}
        for future in concurrent.futures.as_completed(future_map):
            ticker, result, ok = future.result()
            done += 1
            if ok and any(v is not None for v in result.values()):
                quality_store[ticker] = result
                if ticker in _meta_data:
                    for k, v in result.items():
                        if v is not None:
                            _meta_data[ticker][k] = v
            else:
                failed += 1

            if done % 100 == 0:
                with _status_lock:
                    _status["quality_coverage"] = f"{len(quality_store)}/{len(tickers)}"
                logger.info(f"Quality enrichment: {done}/{len(tickers)} done, {len(quality_store)} with data")

    try:
        cache.set(QUALITY_CACHE_KEY, quality_store, expire=QUALITY_CACHE_TTL)
    except Exception as e:
        logger.warning(f"Quality cache write failed: {e}")

    with _cache_lock:
        _quality_epoch += 1
        _factor_cache = None
        _factor_key = None
        _audit_printed = False

    elapsed = time.time() - t0
    _timings["quality_enrichment"] = round(elapsed, 3)

    with _status_lock:
        _status["enrichment"] = "complete"
        _status["quality_coverage"] = f"{len(quality_store)}/{len(tickers)}"

    logger.info(
        f"Quality enrichment complete: {len(quality_store)}/{len(tickers)} enriched, "
        f"{failed} failed, {elapsed:.1f}s"
    )


# ─── Essential metadata (NASDAQ screener + batch quotes) ─────────────────────

def _build_essential_meta(tickers: list) -> dict:
    """
    Build essential metadata from NASDAQ screener data (already cached from
    universe fetch) plus batch quote API. No per-ticker .info calls needed.
    This is fast (~2-5s) and sufficient for Stage 1.
    """
    t0 = time.time()
    meta = {}

    nasdaq_meta = cache.get(NASDAQ_META_KEY) or {}

    for t in tickers:
        nd = nasdaq_meta.get(t, {})
        meta[t] = {
            "name":       nd.get("name", t),
            "sector":     nd.get("sector"),
            "industry":   nd.get("industry"),
            "market_cap": nd.get("market_cap"),
            "price":      None,
            "avg_volume":  None,
            "roe":        None,
            "roa":        None,
            "gross_margin": None,
            "op_margin":  None,
            "de_ratio":   None,
            "quote_type": "ETF" if nd.get("is_etf") else "",
            "exchange":   nd.get("exchange", ""),
        }

    try:
        from price_adapter import run_async_batch_quotes
        batch_meta = run_async_batch_quotes(tickers)
        merged = 0
        for t, bm in batch_meta.items():
            if t in meta:
                if bm.get("name") and bm["name"] != t:
                    meta[t]["name"] = bm["name"]
                if bm.get("sector"):
                    meta[t]["sector"] = bm["sector"]
                if bm.get("industry"):
                    meta[t]["industry"] = bm["industry"]
                if bm.get("market_cap"):
                    meta[t]["market_cap"] = bm["market_cap"]
                if bm.get("avg_volume"):
                    meta[t]["avg_volume"] = bm["avg_volume"]
                if bm.get("quote_type"):
                    meta[t]["quote_type"] = bm["quote_type"]
                if bm.get("exchange"):
                    meta[t]["exchange"] = bm["exchange"]
                merged += 1
        logger.info(f"Batch quotes merged: {merged}/{len(tickers)}")
    except Exception as e:
        logger.warning(f"Batch quote fetch failed ({e}); using NASDAQ-only metadata")

    elapsed = time.time() - t0
    _timings["essential_metadata"] = round(elapsed, 3)
    logger.info(f"Essential metadata built for {len(meta)} tickers in {elapsed:.1f}s")
    return meta


def _load_full_meta_yf(tickers: list) -> dict:
    """Legacy full metadata loader using yfinance .info (slow, ~5-10 min)."""
    meta: dict   = {}
    failed_count = 0

    update_status("loading", f"Loading metadata for {len(tickers)} stocks...",
                  progress=0.75, total=len(tickers), loaded=0)

    def _fetch_one(ticker):
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_map = {executor.submit(_fetch_one, t): t for t in tickers}
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
        if not m.get("quote_type") and nd.get("is_etf"):
            m["quote_type"] = "ETF"
        if not m.get("exchange"):
            m["exchange"] = nd.get("exchange", "")

    if backfilled:
        logger.info(f"Backfilled market_cap from NASDAQ screener for {backfilled} tickers")
    return meta


# ─── Initial data load (two-stage) ──────────────────────────────────────────

def initial_data_load():
    """
    Two-stage background data load:
      Stage 1: Universe + prices + essential metadata → engine usable (status=ready)
      Stage 2: Quality enrichment in background → factor cache invalidated on completion
    """
    global _price_data, _meta_data, _dollar_volume, _quality_epoch

    t_start = time.time()

    _fetch_universe()

    # ── Try price + dollar-volume cache ──────────────────────────────────────
    price_cached = cache.get(PRICE_CACHE_KEY)
    if price_cached:
        logger.info("Restoring price data from disk cache")
        update_status("loading", "Restoring from cache...", progress=0.5)
        try:
            _price_data, _dollar_volume = price_cached
            n = len(_price_data.columns)
            _timings["price_restore"] = round(time.time() - t_start, 3)

            meta_cached = cache.get(META_CACHE_KEY)
            if meta_cached and isinstance(meta_cached, dict) and len(meta_cached) > 0:
                coverage = sum(1 for t in _price_data.columns if t in meta_cached) / n
                if coverage >= 0.8:
                    _meta_data = meta_cached
                    logger.info(f"Metadata from cache ({coverage:.0%} coverage)")
                else:
                    logger.info(f"Metadata cache coverage low ({coverage:.0%}), rebuilding essential")
                    _meta_data = _build_essential_meta(list(_price_data.columns))
                    cache.set(META_CACHE_KEY, _meta_data, expire=META_CACHE_TTL)
            else:
                logger.info("Metadata cache miss, building essential metadata...")
                update_status("loading", "Building metadata...", progress=0.75)
                _meta_data = _build_essential_meta(list(_price_data.columns))
                cache.set(META_CACHE_KEY, _meta_data, expire=META_CACHE_TTL)

            _timings["total_stage1"] = round(time.time() - t_start, 3)

            update_status("ready",
                          f"Ready. {n} stocks loaded from cache.",
                          progress=1.0, total=n, loaded=n)

            _start_quality_enrichment(list(_price_data.columns))
            return
        except Exception as e:
            logger.error(f"Cache restore failed: {e}; falling back to full download")

    # ── Cold start: async price download ─────────────────────────────────────
    tickers = _fetch_universe()
    logger.info(f"Cold start: downloading data for ~{len(tickers)} tickers")
    update_status("loading", f"Starting download for {len(tickers)} stocks...",
                  progress=0, total=len(tickers), loaded=0)

    t_dl = time.time()
    all_close = None
    all_dollar_vol = None
    failed = []

    def progress_cb(loaded, total, completed):
        update_status("loading",
                      f"Downloading price data ({loaded}/{total})...",
                      progress=completed / total, total=total, loaded=loaded)

    try:
        from price_adapter import run_async_download
        all_close, all_dollar_vol_raw, failed = run_async_download(
            tickers, progress_cb=progress_cb, max_concurrent=50
        )
        all_dollar_vol = {}
        for t in all_close:
            if t in all_dollar_vol_raw:
                vol_s = all_dollar_vol_raw[t].reindex(all_close[t].index).fillna(0)
                all_dollar_vol[t] = all_close[t] * vol_s
            else:
                all_dollar_vol[t] = pd.Series(0.0, index=all_close[t].index)

        success_rate = len(all_close) / len(tickers) if tickers else 0
        logger.info(f"Async download: {len(all_close)}/{len(tickers)} ({success_rate:.0%})")

        if success_rate < 0.3:
            logger.warning("Async download success rate too low, falling back to yfinance")
            all_close = None
    except Exception as e:
        logger.warning(f"Async download failed ({e}), falling back to yfinance")
        all_close = None

    if all_close is None:
        logger.info("Using yfinance fallback for price download")
        all_close, all_dollar_vol, failed = _yf_load_data_batch(tickers, batch_size=100)

    _timings["price_download"] = round(time.time() - t_dl, 3)

    if not all_close:
        update_status("error", "Failed to load any price data. Check network connectivity.")
        return

    logger.info(f"Downloaded {len(all_close)} tickers; {len(failed)} failed")

    t_build = time.time()
    prices = pd.concat([s.rename(t) for t, s in all_close.items()], axis=1)
    prices = prices.sort_index().dropna(how="all").ffill(limit=5)
    valid_cols = [c for c in prices.columns if prices[c].count() >= 252]
    prices = prices[valid_cols]

    dv_series = {t: s for t, s in all_dollar_vol.items() if t in valid_cols}
    if dv_series:
        dollar_volume = pd.concat(
            [s.rename(t) for t, s in dv_series.items()], axis=1
        ).sort_index().ffill(limit=5).reindex(columns=valid_cols)
    else:
        dollar_volume = pd.DataFrame(index=prices.index, columns=prices.columns)

    _timings["matrix_build"] = round(time.time() - t_build, 3)
    logger.info(f"Price matrix: {prices.shape}; dollar-volume: {dollar_volume.shape}")

    t_meta = time.time()
    meta = _build_essential_meta(valid_cols)
    _timings["essential_metadata"] = round(time.time() - t_meta, 3)

    _price_data    = prices
    _dollar_volume = dollar_volume
    _meta_data     = meta

    try:
        cache.set(PRICE_CACHE_KEY, (_price_data, _dollar_volume), expire=PRICE_CACHE_TTL)
        cache.set(META_CACHE_KEY,  _meta_data,                    expire=META_CACHE_TTL)
    except Exception as e:
        logger.error(f"Cache write failed: {e}")

    _timings["total_stage1"] = round(time.time() - t_start, 3)

    update_status("ready",
                  f"Ready. {len(valid_cols)} stocks loaded.",
                  progress=1.0, total=len(valid_cols), loaded=len(valid_cols))
    logger.info(f"Stage 1 complete in {_timings['total_stage1']:.1f}s — engine usable")

    _start_quality_enrichment(valid_cols)


def _start_quality_enrichment(tickers: list):
    """Start Stage 2 quality enrichment in a separate daemon thread."""
    t = threading.Thread(target=_background_quality_enrichment, args=(tickers,), daemon=True)
    t.start()
    logger.info(f"Stage 2 quality enrichment started for {len(tickers)} tickers")


def start_background_load():
    """Start data loading in a daemon background thread."""
    t = threading.Thread(target=initial_data_load, daemon=True)
    t.start()
