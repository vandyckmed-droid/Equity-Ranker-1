"""
Equity Ranking Engine — Performance Edition
=============================================
Fetches real market data from Yahoo Finance, computes momentum factors
using a 2-sleeve alpha model (S + T), and provides portfolio risk metrics.

Architecture
------------
- Async price downloader via PriceDataAdapter (aiohttp, 50 concurrent connections)
- Vectorized factor computation (numpy matrix ops, no per-ticker loops)
- Three-layer cache: factors → rankings → clustering
- Single-stage startup: prices + essential meta → engine usable
- Per-phase timing instrumentation

Universe construction
---------------------
At cold start, the live universe is built dynamically from the NASDAQ screener API:
  1. NASDAQ API  : fetches NYSE- and NASDAQ-listed stocks with $2B market-cap pre-filter
  2. Format filter: non-empty symbol, <= 5 chars, alphabetic only.
  3. ADR/foreign merge: curated ADR/foreign names always included.
  4. classify_ticker(): excludes ETFs, funds, OTC, partnerships, SPACs at meta-fetch time.
  5. Price/ADV/cap filters: price >= $5, 63-day median ADV >= $10M, market cap >= $1B.

Alpha model (5-composite model)
--------------------------------------
  M  = ¼(zM6 + zM12 + zT6 + zT12)
  RM = 0.4·zR6 + 0.6·zR12
  α  = (wM·M + wRM·RM + wR·(−zM1) + wLV·zLowVol + wQ·zOPA) / Σw

Signal conventions (pure: one idea → one transformation chain):
  Each signal: raw metric → winsorize(2%/98%) → cross-sectional z-score

  m6 / m12   : Σ log_ret — windows [t-126:t-22] / [t-252:t-22]  (cumulative return, no vol division)
  m1         : Σ log_ret[t-21:t]  (1-month return; enters α with − sign as reversal)
  res6/res12 : sum(ε, window) — OLS with intercept on market+peer; betas-only residual
  tstat6/12  : OLS t-stat of slope on ln(P) ~ t  over 126/252 days
  lowvol     : −Z(sigma_60), sigma_60 = std(log_ret[-60:]) × √252  (60-day realized vol)
  OPA        : Operating profitability on assets (cross-sectional z-score)
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

from quality_loader  import get_or_build_quality_raw
from quality_formula import compute_opa_all
from quality_audit   import build_coverage_report, build_per_ticker_audit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR                  = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
PRICE_CACHE_TTL            = 8  * 3600
META_CACHE_TTL             = 48 * 3600
UNIVERSE_CACHE_TTL         = 24 * 3600
BENCHMARK_PRICES_CACHE_TTL = 8  * 3600
PRICE_CACHE_KEY            = "price_data_v5"
META_CACHE_KEY             = "meta_data_v2"
UNIVERSE_CACHE_KEY         = "universe_v1"
NASDAQ_META_KEY            = "nasdaq_meta_v1"
SECTOR_MAP_CACHE_KEY       = "sector_map_v1"
BENCHMARK_PRICES_CACHE_KEY = "benchmark_prices_v1"

# ─── GICS sector → sector ETF (static, deterministic) ────────────────────────
MARKET_BENCHMARK: str = "VTI"

GICS_SECTOR_ETF: dict = {
    "Technology":             "XLK",
    "Financials":             "XLF",
    "Industrials":            "XLI",
    "Health Care":            "XLV",
    "Consumer Discretionary": "XLY",
    "Consumer Staples":       "XLP",
    "Energy":                 "XLE",
    "Utilities":              "XLU",
    "Materials":              "XLB",
    "Real Estate":            "XLRE",
    "Communication Services": "XLC",
}

# Normalize raw sector strings (NASDAQ API / yfinance) → GICS sector key
_SECTOR_NORM: dict = {
    "technology":                   "Technology",
    "tech":                         "Technology",
    "semiconductors":               "Technology",
    "software":                     "Technology",
    "information technology":       "Technology",
    "financials":                   "Financials",
    "finance":                      "Financials",
    "financial services":           "Financials",
    "financial":                    "Financials",
    "banks":                        "Financials",
    "banking":                      "Financials",
    "insurance":                    "Financials",
    "capital markets":              "Financials",
    "asset management":             "Financials",
    "industrials":                  "Industrials",
    "industrial":                   "Industrials",
    "aerospace & defense":          "Industrials",
    "aerospace and defense":        "Industrials",
    "defense":                      "Industrials",
    "transportation":               "Industrials",
    "machinery":                    "Industrials",
    "construction":                 "Industrials",
    "health care":                  "Health Care",
    "healthcare":                   "Health Care",
    "health":                       "Health Care",
    "biotechnology":                "Health Care",
    "pharmaceuticals":              "Health Care",
    "medical devices":              "Health Care",
    "medical":                      "Health Care",
    "life sciences":                "Health Care",
    "consumer discretionary":       "Consumer Discretionary",
    "consumer cyclical":            "Consumer Discretionary",
    "cyclical":                     "Consumer Discretionary",
    "retail":                       "Consumer Discretionary",
    "auto":                         "Consumer Discretionary",
    "automotive":                   "Consumer Discretionary",
    "leisure":                      "Consumer Discretionary",
    "hotels":                       "Consumer Discretionary",
    "restaurants":                  "Consumer Discretionary",
    "apparel":                      "Consumer Discretionary",
    "consumer staples":             "Consumer Staples",
    "consumer defensive":           "Consumer Staples",
    "defensive":                    "Consumer Staples",
    "staples":                      "Consumer Staples",
    "food":                         "Consumer Staples",
    "beverages":                    "Consumer Staples",
    "tobacco":                      "Consumer Staples",
    "household products":           "Consumer Staples",
    "personal products":            "Consumer Staples",
    "energy":                       "Energy",
    "oil & gas":                    "Energy",
    "oil and gas":                  "Energy",
    "oil":                          "Energy",
    "gas":                          "Energy",
    "utilities":                    "Utilities",
    "utility":                      "Utilities",
    "electric utilities":           "Utilities",
    "water utilities":              "Utilities",
    "regulated utilities":          "Utilities",
    "materials":                    "Materials",
    "basic materials":              "Materials",
    "chemicals":                    "Materials",
    "mining":                       "Materials",
    "metals":                       "Materials",
    "metals & mining":              "Materials",
    "metals and mining":            "Materials",
    "paper":                        "Materials",
    "containers":                   "Materials",
    "packaging":                    "Materials",
    "real estate":                  "Real Estate",
    "reits":                        "Real Estate",
    "reit":                         "Real Estate",
    "real estate investment trusts": "Real Estate",
    "communication services":       "Communication Services",
    "communication":                "Communication Services",
    "communications":               "Communication Services",
    "telecommunications":           "Communication Services",
    "telecom":                      "Communication Services",
    "media":                        "Communication Services",
    "entertainment":                "Communication Services",
    "internet":                     "Communication Services",
    "interactive media":            "Communication Services",
}

# Per-ticker overrides for stocks with None / 'Miscellaneous' sector metadata.
# These are applied before the generic normalizer so they always win.
_TICKER_SECTOR_OVERRIDE: dict = {
    "AMPX":  "Energy",                  # Amprius Technologies — battery cells
    "ATKR":  "Industrials",             # Atkore — electrical conduit/products
    "BELFA": "Technology",              # Bel Fuse A shares — electronic components
    "BELFB": "Technology",              # Bel Fuse B shares — electronic components
    "CAE":   "Industrials",             # CAE — flight simulation & training
    "DBD":   "Technology",              # Diebold Nixdorf — banking technology
    "DLB":   "Communication Services",  # Dolby Laboratories — audio/media tech
    "FERG":  "Industrials",             # Ferguson — industrial distribution
    "FLNC":  "Energy",                  # Fluence Energy — grid-scale storage
    "GEF":   "Materials",               # Greif — industrial packaging
    "GEV":   "Industrials",             # GE Vernova — power/energy equipment
    "IDCC":  "Technology",              # InterDigital — wireless IP licensing
    "KSPI":  "Financials",              # Kaspi.kz — Kazakhstan fintech/banking
    "NATL":  "Financials",              # National Western Financial — insurance
    "NMFCZ": "Financials",              # New Mountain Finance — BDC
    "NOVT":  "Technology",              # Novanta — medical/industrial motion tech
    "OGC":   "Materials",               # OceanaGold — gold mining
    "QS":    "Technology",              # QuantumScape — solid-state battery R&D
    "RUN":   "Utilities",               # Sunrun — residential solar energy
    "TPGXL": "Financials",              # TPG — alternative asset manager
    # Genuinely ambiguous → remain market-only
    # "AAUC", "ARIS" — left unmapped intentionally
}

os.makedirs(CACHE_DIR, exist_ok=True)
cache = diskcache.Cache(CACHE_DIR)

_status = {
    "status": "loading",
    "message": "Initializing...",
    "progress": 0,
    "total": 0,
    "loaded": 0,
    "cached_at": None,
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

_factor_cache   = None
_factor_key     = None
_factor_audit   = None
_ranking_cache  = None
_ranking_key    = None
_cluster_cache  = None
_cluster_key    = None
_cache_lock     = threading.Lock()

_timings: dict  = {}

# Sector / benchmark data layer (constant-time reads, populated after stage 1)
_sector_map:       Optional[dict]         = None  # {ticker: {gics_sector, sector_etf, benchmark}}
_benchmark_prices: Optional[pd.DataFrame] = None  # daily adj-close: VTI + XL* ETFs
_sector_map_lock   = threading.Lock()
_benchmark_lock    = threading.Lock()

# Residual momentum audit (populated each time factors are recomputed)
_residual_audit: dict = {"status": "not_computed"}

# Quality data layer (populated by background thread after stage-1 ready)
_quality_opa:  Optional[dict] = None   # {ticker: formula_result}
_quality_lock  = threading.Lock()


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
    has_126 = valid_counts >= 126
    has_22  = valid_counts >= 22

    lp_last = log_prices.iloc[-1]          # last log-price row (used for price_last below)

    # ── Pure cumulative log-return momentum signals (no vol division) ──────────
    # m6  = sum(log_ret[t-126:t-22])   — 6-1 momentum
    # m12 = sum(log_ret[t-252:t-22])   — 12-1 momentum
    # m1  = sum(log_ret[t-21:t])       — 1-month reversal input
    m6  = log_returns.iloc[-126:-21].sum()
    m12 = log_returns.iloc[-252:-21].sum()
    m1  = log_returns.iloc[-21:].sum()

    # Require minimum history; set to NaN otherwise (→ z-score 0 after cross-section)
    m6  = m6.where(has_126, np.nan)
    m12 = m12.where(has_252, np.nan)
    m1  = m1.where(has_22, np.nan)

    # ── 60-day realized volatility for zLowVol signal ─────────────────────────
    # sigma_60 = std(log_ret[-60:]) × √252  (simple rolling std, no EWMA, no floor)
    sigma_60 = log_returns.iloc[-60:].std() * np.sqrt(252)
    sigma_60 = sigma_60.where(has_22, np.nan)

    # ── 63-day realized volatility for RAM signals ─────────────────────────────
    # sigma_63 = std(log_ret[-63:]) × √252  (3-month vol, common denominator for all RAM)
    # RAM_w = MOM_w / max(sigma_63, vol_floor_ram)  — rewards consistent risk-adjusted drift
    _VOL_FLOOR_RAM = 0.15  # 15% annualized floor to avoid dividing by near-zero vol
    sigma_63 = log_returns.iloc[-63:].std() * np.sqrt(252)
    sigma_63 = sigma_63.where(has_22, np.nan)
    vol_denom_ram = sigma_63.clip(lower=_VOL_FLOOR_RAM)
    ram6  = (m6  / vol_denom_ram).where(has_126, np.nan)
    ram12 = (m12 / vol_denom_ram).where(has_252, np.nan)
    ram1  = (m1  / vol_denom_ram).where(has_22,  np.nan)

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

    meta_names      = []
    meta_sectors    = []
    meta_industries = []
    meta_mcaps      = []
    struct_excl     = []

    for t in tickers:
        m = meta.get(t, {})
        meta_names.append(m.get("name", t))
        meta_sectors.append(m.get("sector"))
        meta_industries.append(m.get("industry"))
        meta_mcaps.append(m.get("market_cap"))
        struct_excl.append(classify_ticker(t, m))

        if not np.isfinite(adv.get(t, np.nan)) or adv.get(t, 0) == 0:
            avg_vol = m.get("avg_volume")
            if avg_vol and np.isfinite(price_last.get(t, 0)):
                adv[t] = price_last[t] * avg_vol

    # ── Residual momentum signals (vectorized, reuses log_returns) ───────────
    # meta passed for industry peer selection (min-size rule)
    res_df = _compute_residual_signals(log_returns, tickers, meta=meta)

    df = pd.DataFrame({
        "ticker":               tickers,
        "name":                 meta_names,
        "sector":               meta_sectors,
        "industry":             meta_industries,
        "price":                price_last.values,
        "market_cap":           meta_mcaps,
        "adv":                  adv.values,
        "m1":                   m1.values,
        "m6":                   m6.values,
        "m12":                  m12.values,
        "ram1":                 ram1.values,
        "ram6":                 ram6.values,
        "ram12":                ram12.values,
        "tstat6":               tstat6.values,
        "tstat12":              tstat12.values,
        "sigma_60":             sigma_60.values,
        "res6":                 res_df["res6"].values,
        "res12":                res_df["res12"].values,
        "reg_type":             res_df["reg_type"].values,
        "structure_exclusion":  struct_excl,
    })

    min_hist = has_252.values
    df = df[min_hist].reset_index(drop=True)

    if df.empty:
        return df

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
                     use_tstats: bool = False,
                     w_s6: float = 1, w_s12: float = 1,
                     w_res6: float = 2, w_res12: float = 3,
                     w_t6: float = 1, w_t12: float = 1,
                     w_s1: float = 1,
                     w_invvol: float = 2,
                     w_opa: float = 2,
                     winsor_p: float = 2.0,
                     vol_floor: float = 0.10,
                     opa_dict: dict = None) -> pd.DataFrame:
    """
    Cross-sectionally z-score all 9 signals (winsorize → z-score), compute flat weighted alpha.

    Signal definitions (pure, one idea per signal):
      zM6     = Z(winsorize(m6))   where m6  = Σ log_ret[t-126:t-22]  (6-1 momentum)
      zM12    = Z(winsorize(m12))  where m12 = Σ log_ret[t-252:t-22]  (12-1 momentum)
      zM1     = Z(winsorize(m1))   where m1  = Σ log_ret[t-21:t]      (1m reversal input)
      zT6     = Z(winsorize(tstat6))   — OLS t-stat, 6m window
      zT12    = Z(winsorize(tstat12))  — OLS t-stat, 12m window
      zR6     = Z(winsorize(res6))     — cumulative residual, 6m
      zR12    = Z(winsorize(res12))    — cumulative residual, 12m
      zLowVol = −Z(winsorize(sigma_60)) where sigma_60 = std(log_ret[-60:]) × √252
      zOPA    = Z(winsorize(OPA))

    alpha = (wM·M + wRM·RM + wR·(−zM1) + wLV·zLowVol + wQ·zOPA) / Σw
      where M  = ¼(zM6 + zM12 + zT6 + zT12)
            RM = 0.4·zR6 + 0.6·zR12

    Note: −zM1 applies the reversal (zM1 is positive 1-month return; we fade it).
    Clustering handled separately by compute_clustering().
    """
    if df.empty:
        return df

    d = df.copy()

    def std_factor(series):
        s = pd.to_numeric(series, errors="coerce")
        v = s[s.notna()]
        if len(v) < 10:
            return pd.Series(0.0, index=s.index)
        out = pd.Series(np.nan, index=s.index)
        out[s.notna()] = zscore(winsorize(v, winsor_p))
        return out

    # ── Individual z-scores (winsorize → cross-sectional z-score) ────────────
    d["zM6"]  = std_factor(d["m6"])  if "m6"  in d.columns else pd.Series(0.0, index=d.index)
    d["zM12"] = std_factor(d["m12"]) if "m12" in d.columns else pd.Series(0.0, index=d.index)
    d["zT6"]  = std_factor(d["tstat6"])
    d["zT12"] = std_factor(d["tstat12"])
    d["zM1"]  = std_factor(d["m1"])  if "m1"  in d.columns else pd.Series(0.0, index=d.index)
    # lowvol = −Z(sigma_60): 60-day realized std × √252, negated so low-vol → high score
    d["zLowVol"] = -std_factor(d["sigma_60"]) if "sigma_60" in d.columns else pd.Series(0.0, index=d.index)
    # RAM = MOM / max(σ63, 0.15): vol-adjusted momentum signals
    d["zRam6"]  = std_factor(d["ram6"])  if "ram6"  in d.columns else pd.Series(0.0, index=d.index)
    d["zRam12"] = std_factor(d["ram12"]) if "ram12" in d.columns else pd.Series(0.0, index=d.index)
    d["zRam1"]  = std_factor(d["ram1"])  if "ram1"  in d.columns else pd.Series(0.0, index=d.index)

    has_residuals = ("res6" in d.columns and "res12" in d.columns
                     and d["res6"].abs().sum() > 0)
    if has_residuals:
        d["zR6"]  = std_factor(d["res6"])
        d["zR12"] = std_factor(d["res12"])
        alpha_formula = "flat-9"
    else:
        d["zR6"]  = pd.Series(0.0, index=d.index)
        d["zR12"] = pd.Series(0.0, index=d.index)
        alpha_formula = "flat-7"

    # ── OPA z-score (universe-level, same winsorize+zscore as other signals) ──
    if opa_dict:
        opa_vals = d["ticker"].map(
            lambda t: opa_dict[t].get("opa") if (t in opa_dict and opa_dict[t].get("available")) else np.nan
        )
        d["zOPA"] = std_factor(opa_vals)
    else:
        d["zOPA"] = pd.Series(0.0, index=d.index)

    # ── Display sleeves (detail panel only — do not affect alpha) ─────────────
    if has_residuals:
        d["M6_blend"]  = 0.7 * d["zM6"].fillna(0) + 0.3 * d["zR6"].fillna(0)
        d["M12_blend"] = 0.7 * d["zM12"].fillna(0) + 0.3 * d["zR12"].fillna(0)
        d["sSleeve"]   = 0.5 * d["M6_blend"] + 0.5 * d["M12_blend"]
    else:
        d["M6_blend"]  = d["zM6"].fillna(0)
        d["M12_blend"] = d["zM12"].fillna(0)
        d["sSleeve"]   = 0.5 * d["zM6"].fillna(0) + 0.5 * d["zM12"].fillna(0)

    d["tSleeve"]   = 0.5 * d["zT6"].fillna(0) + 0.5 * d["zT12"].fillna(0)
    d["revSleeve"] = -d["zM1"].fillna(0)   # negative for display (reversal direction)

    # ── Flat alpha ────────────────────────────────────────────────────────────
    total_w = (w_s6 + w_s12 + w_res6 + w_res12 + w_t6 + w_t12 + w_s1 + w_invvol + w_opa) or 1.0
    d["alpha"] = (
          w_s6    * d["zM6"].fillna(0)
        + w_s12   * d["zM12"].fillna(0)
        + w_res6  * d["zR6"].fillna(0)
        + w_res12 * d["zR12"].fillna(0)
        + w_t6    * d["zT6"].fillna(0)
        + w_t12   * d["zT12"].fillna(0)
        - w_s1    * d["zM1"].fillna(0)      # reversal: penalise last-month winners
        + w_invvol * d["zLowVol"].fillna(0)  # lowvol = −Z(sigma_60)
        + w_opa   * d["zOPA"].fillna(0)
    ) / total_w

    d["alpha_formula"] = alpha_formula

    d = d.sort_values("alpha", ascending=False)
    d["rank"]       = np.arange(1, len(d) + 1)
    d["percentile"] = 100.0 * (1 - (d["rank"] - 1) / len(d))
    d["cluster"]    = None

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


# ─── Residual momentum computation ────────────────────────────────────────────

def _compute_residual_signals(
    stock_log_ret: pd.DataFrame,  # (T_full, N) — all log returns, already computed
    tickers: list,
    w6:  int = 126,
    w12: int = 252,
    meta: dict = None,            # {ticker: {"industry": ..., ...}} for peer selection
    min_industry_size: int = 10,  # minimum group size to use industry peer
) -> pd.DataFrame:
    """
    Vectorized residual momentum using OLS with intercept + dynamic peer selection.

    Peer selection (minimum group size rule):
      ≥ min_industry_size stocks in same industry → equal-weight industry return
      < min_industry_size (or no industry)        → sector ETF return
      no sector ETF                               → market-only (VTI)

    Regression model (with intercept, for consistent beta estimation):
      r_i = α + β_m·r_market + β_p·r_peer + ε̃

    Residuals are then computed WITHOUT the intercept term, so that the
    stock's average alpha is retained in the signal (not differenced away):
      ε = r_i − β_m·r_market − β_p·r_peer

    This avoids the sum-to-zero constraint that would kill cumulative signals
    while still deriving betas from an unbiased (intercept) regression.

    Signal windows (skip last 21 days — true 1-month reversal skip):
      res6  = sum(ε, t-126:t-22)  (~104 observations)
      res12 = sum(ε, t-252:t-22)  (~230 observations)

    Cross-sectionally z-scored in compute_rankings.
    Neutral (0.0) on failure — universe coverage never decreases.

    Returns DataFrame indexed by tickers:
      columns: res6, res12, reg_type
    """
    global _residual_audit

    sm = get_sector_map()
    bp = get_benchmark_prices()

    _no_residuals = pd.DataFrame({
        "res6":     np.zeros(len(tickers), dtype=np.float64),
        "res12":    np.zeros(len(tickers), dtype=np.float64),
        "reg_type": ["none"] * len(tickers),
    }, index=tickers)

    if bp is None or bp.empty:
        _residual_audit = {"status": "skipped", "reason": "benchmark_prices not ready"}
        return _no_residuals

    t0 = time.time()

    # ── Align stock and benchmark returns on common trading-day index ──────────
    bench_log_ret = np.log(bp.clip(lower=1e-8)).diff()
    common_idx    = stock_log_ret.index.intersection(bench_log_ret.index)

    if len(common_idx) < 30:
        _residual_audit = {"status": "skipped", "reason": f"only {len(common_idx)} common dates"}
        return _no_residuals

    idx_w12 = common_idx[-w12:]
    T12     = len(idx_w12)

    S_mat = (stock_log_ret
             .reindex(index=idx_w12, columns=tickers)
             .fillna(0.0)
             .values)                  # (T12, N)
    B_df  = (bench_log_ret
             .reindex(index=idx_w12)
             .fillna(0.0))             # (T12, K)

    vti = B_df["VTI"].values if "VTI" in B_df.columns else np.zeros(T12)

    # ── Build industry groups from meta ───────────────────────────────────────
    from collections import defaultdict

    ticker_industry  = [((meta or {}).get(t) or {}).get("industry") for t in tickers]
    ticker_sector_etf = [(sm or {}).get(t, {}).get("sector_etf") if sm else None for t in tickers]

    industry_to_idxs: dict = defaultdict(list)
    for i, ind in enumerate(ticker_industry):
        if ind:
            industry_to_idxs[ind].append(i)

    # ── Compute equal-weight industry returns (only for qualifying groups) ─────
    industry_returns: dict = {}   # industry → (T12,) array
    for ind, idxs in industry_to_idxs.items():
        if len(idxs) >= min_industry_size:
            industry_returns[ind] = S_mat[:, idxs].mean(axis=1)

    # ── Assign each ticker to its peer group ──────────────────────────────────
    peer_group: list = []   # (peer_type, peer_key) per ticker position
    for i in range(len(tickers)):
        ind = ticker_industry[i]
        etf = ticker_sector_etf[i]
        if ind and ind in industry_returns:
            peer_group.append(("industry", ind))
        elif etf and etf in B_df.columns:
            peer_group.append(("sector", etf))
        else:
            peer_group.append(("market_only", None))

    # Collapse tickers into peer groups for vectorized OLS
    group_map: dict = defaultdict(list)   # (peer_type, peer_key) → [ticker positions]
    for i, pg in enumerate(peer_group):
        group_map[pg].append(i)

    # ── Vectorized OLS + cumulative residuals helper ───────────────────────────
    ones_col = np.ones((T12, 1))

    def _batch_resid_cumulative(
        Y: np.ndarray,       # (W, group_N)  — stock returns
        X_factors: np.ndarray,  # (W, K)     — market + peer returns, NO intercept col
    ) -> tuple:
        """
        Step 1 — Estimate betas using OLS WITH intercept:
          r = α·1 + X_factors·β + ε̃
        Step 2 — Compute residuals WITHOUT removing intercept:
          ε = r − X_factors·β  (= α̂ + ε̃, so the stock's alpha is retained)

        Subtracting β·factors but NOT α ensures cumulative residuals carry
        the stock-specific alpha component and are not constrained to sum to zero.

        Returns: (r6, r12) — summed residuals over skip-adjusted windows.
        Falls back to 0.0 on any failure.
        """
        W, N = Y.shape
        if W < 30 or N == 0:
            return np.zeros(N), np.zeros(N)
        try:
            X_int = np.column_stack([ones_col[:W], X_factors[:W]])  # (W, K+1) with intercept
            beta_int, _, _, _ = np.linalg.lstsq(X_int, Y, rcond=None)  # (K+1, N)
            beta_slopes = beta_int[1:]                                    # (K, N) — slopes only
            resid = Y - X_factors[:W] @ beta_slopes                       # (W, N) — alpha retained
            r6  = resid[-126:-21].sum(axis=0)   # 6m skip-adjusted window
            r12 = resid[:-21].sum(axis=0)       # 12m skip-adjusted window
            r6  = np.where(np.isfinite(r6),  r6,  0.0)
            r12 = np.where(np.isfinite(r12), r12, 0.0)
            return r6, r12
        except Exception:
            return np.zeros(N), np.zeros(N)

    # ── Process each peer group ────────────────────────────────────────────────
    res6_arr  = np.zeros(len(tickers), dtype=np.float64)
    res12_arr = np.zeros(len(tickers), dtype=np.float64)
    reg_types = ["none"] * len(tickers)

    n_industry = 0; n_sector = 0; n_market = 0

    for (ptype, pkey), idxs in group_map.items():
        Y = S_mat[:, idxs]           # (T12, group_N)

        if ptype == "industry":
            peer_ret   = industry_returns[pkey].reshape(-1, 1)       # (T12, 1)
            X_factors  = np.column_stack([vti, peer_ret])             # (T12, 2)
            rtype      = f"industry:{pkey[:24]}"
            n_industry += len(idxs)
        elif ptype == "sector":
            sector_ret = B_df[pkey].values.reshape(-1, 1)            # (T12, 1)
            X_factors  = np.column_stack([vti, sector_ret])          # (T12, 2)
            rtype      = f"sector:{pkey}"
            n_sector  += len(idxs)
        else:
            X_factors  = vti.reshape(-1, 1)                          # (T12, 1)
            rtype      = "market_only"
            n_market  += len(idxs)

        r6, r12 = _batch_resid_cumulative(Y, X_factors)
        for li, gi in enumerate(idxs):
            res6_arr[gi]  = r6[li]
            res12_arr[gi] = r12[li]
            reg_types[gi] = rtype

    elapsed = time.time() - t0
    _timings["residual_signals"] = round(elapsed, 3)

    n_ind_groups = sum(1 for ind, idxs in industry_to_idxs.items() if len(idxs) >= min_industry_size)

    _residual_audit = {
        "status":                    "ok",
        "elapsed_s":                 round(elapsed, 3),
        "total_stocks":              len(tickers),
        "industry_regressions":      n_industry,
        "sector_regressions":        n_sector,
        "market_only_regressions":   n_market,
        "qualifying_industry_groups": n_ind_groups,
        "window_6m_days":            min(126, T12) - 21,
        "window_12m_days":           T12 - 21,
        "method":                    "sum(ε), OLS+intercept+peer, betas-only residual, skip-21d",
    }

    logger.info(
        f"residual_signals: {elapsed:.3f}s | N={len(tickers)} | "
        f"industry={n_industry}({n_ind_groups} groups) "
        f"sector={n_sector} market={n_market}"
    )

    return pd.DataFrame({
        "res6":     res6_arr,
        "res12":    res12_arr,
        "reg_type": reg_types,
    }, index=tickers)


# ─── Universe filtering ───────────────────────────────────────────────────────

def apply_universe_filters(df: pd.DataFrame,
                           min_price:       float = 5.0,
                           min_adv:         float = 1e7,
                           min_market_cap:  float = 0.0,
                           exclude_sectors: list  = None) -> tuple:
    """
    Apply base universe filters. Pass 0 for min_price / min_adv / min_market_cap
    to disable that filter entirely (skip it rather than applying a 0 floor).
    """
    n_in = len(df)
    mask = pd.Series(True, index=df.index)
    audit_exclusions = {}

    if "structure_exclusion" in df.columns:
        struct_fail = df["structure_exclusion"].notna()
        if struct_fail.any():
            for reason in df.loc[struct_fail, "structure_exclusion"].unique():
                n = int((df["structure_exclusion"] == reason).sum())
                audit_exclusions[reason] = n
                logger.info(f"Universe exclusion [{reason}]: {n} tickers")
        mask &= ~struct_fail

    if min_price > 0 and "price" in df.columns:
        price_fail = df["price"].fillna(0) < min_price
        if price_fail.any():
            audit_exclusions["price_below_floor"] = int(price_fail.sum())
            logger.info(f"Universe exclusion [price_below_floor]: {price_fail.sum()} tickers")
        mask &= ~price_fail

    if min_adv > 0 and "adv" in df.columns:
        adv_fail = df["adv"].fillna(0) < min_adv
        if adv_fail.any():
            audit_exclusions["liquidity_below_floor"] = int(adv_fail.sum())
            logger.info(f"Universe exclusion [liquidity_below_floor]: {adv_fail.sum()} tickers")
        mask &= ~adv_fail

    if min_market_cap > 0 and "market_cap" in df.columns:
        cap_fail = df["market_cap"].fillna(0) < min_market_cap
        if cap_fail.any():
            audit_exclusions["market_cap_below_floor"] = int(cap_fail.sum())
            logger.info(f"Universe exclusion [market_cap_below_floor]: {cap_fail.sum()} tickers")
        mask &= ~cap_fail

    if exclude_sectors and "sector" in df.columns:
        excl_set = {s.strip() for s in exclude_sectors}
        sector_hit = df["sector"].isin(excl_set) & mask
        if sector_hit.any():
            for s in excl_set:
                n = int(((df["sector"] == s) & mask).sum())
                if n > 0:
                    audit_exclusions[f"sector_{s.lower().replace(' ','_')}"] = n
            logger.info(f"Universe exclusion [sector]: {sector_hit.sum()} tickers ({', '.join(excl_set)})")
        mask &= ~df["sector"].isin(excl_set)

    result = df[mask].copy()
    logger.info(f"Universe filters: {n_in} candidates → {len(result)} qualifying stocks")

    sector_counts = {}
    if "sector" in result.columns:
        for s, cnt in result["sector"].value_counts().items():
            if s:
                sector_counts[s] = int(cnt)

    active_filters = ["history>=252d", "common_stock_only"]
    if min_price > 0:
        active_filters.append(f"price>=${min_price}")
    if min_adv > 0:
        active_filters.append(f"adv>=${min_adv/1e6:.0f}M")
    if min_market_cap > 0:
        active_filters.append(f"mcap>=${min_market_cap/1e9:.0f}B")
    if exclude_sectors:
        active_filters.append(f"exclude_sectors:{','.join(exclude_sectors)}")

    audit = {
        "preFilterCount":  n_in,
        "postFilterCount": len(result),
        "exclusions":      audit_exclusions,
        "sectorBreakdown": sector_counts,
        "activeFilters":   active_filters,
    }

    return result, audit


# ─── Public data accessors ────────────────────────────────────────────────────

def get_price_data() -> Optional[pd.DataFrame]:
    return _price_data


def get_meta_data() -> Optional[dict]:
    return _meta_data


def get_residual_audit() -> dict:
    """Return the most recent residual momentum computation audit."""
    return dict(_residual_audit)


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

    lines.append("  TIMINGS:")
    for k, v in _timings.items():
        lines.append(f"    {k:<30}: {v:>8.3f}s")

    lines.append("═" * 60)

    for line in lines:
        logger.info(line)


# ─── Three-layer cached ranking pipeline ─────────────────────────────────────

def get_ranked_data(params: dict) -> Optional[pd.DataFrame]:
    global _factor_cache, _factor_key, _factor_audit
    global _ranking_cache, _ranking_key
    global _cluster_cache, _cluster_key
    global _price_data, _meta_data, _dollar_volume, _audit_printed

    if _price_data is None:
        return None, {}

    t0 = time.time()
    vol_floor        = params.get("vol_floor", 0.10)
    winsor_p         = params.get("winsor_p", 2.0)
    use_tstats       = params.get("use_tstats", False)
    vol_adjust       = params.get("vol_adjust", True)
    cluster_n        = params.get("cluster_n", 100)
    cluster_k        = params.get("cluster_k", 10)
    cluster_lookback = params.get("cluster_lookback", 252)
    exclude_sectors  = params.get("exclude_sectors", [])
    min_price        = float(params.get("min_price", 5.0))
    min_adv          = float(params.get("min_adv", 1e7))
    min_market_cap   = float(params.get("min_market_cap", 0.0))

    # Default weights (fixed server-side; clients own user-facing weight UI)
    W_S6 = 1; W_S12 = 1; W_RES6 = 2; W_RES12 = 3
    W_T6 = 1; W_T12 = 1; W_S1 = 1; W_INVVOL = 2; W_OPA = 2

    has_opa = bool(get_quality_opa())

    # has_bench in factor key: if benchmark prices arrive after first computation,
    # the key changes → factors are recomputed with residuals on the next request.
    # Filter params are also in fk so changing them busts the factor cache correctly.
    fk = json.dumps({"vol_floor": vol_floor, "winsor_p": winsor_p,
                      "xs":        sorted(exclude_sectors) if exclude_sectors else [],
                      "has_bench": bool(get_benchmark_prices() is not None),
                      "min_price": min_price,
                      "min_adv":   min_adv,
                      "min_mcap":  min_market_cap},
                     sort_keys=True)
    rk = json.dumps({"fk": fk, "ut": use_tstats, "va": vol_adjust,
                      "wp": winsor_p, "has_opa": has_opa}, sort_keys=True)
    ck = json.dumps({"rk": rk, "cn": cluster_n, "ck": cluster_k,
                      "cl": cluster_lookback}, sort_keys=True)

    cache_info = []
    audit = {}

    with _cache_lock:
        if _factor_cache is not None and _factor_key == fk:
            factors_filtered = _factor_cache
            audit = _factor_audit or {}
            cache_info.append("factors:HIT")
        else:
            factors_raw = compute_factors_vectorized(
                _price_data, _meta_data,
                dollar_volume=_dollar_volume,
                vol_floor=vol_floor,
                winsor_p=winsor_p,
            )
            if factors_raw.empty:
                return factors_raw, {}

            factors_pre = factors_raw.copy()
            factors_filtered, audit = apply_universe_filters(
                factors_raw,
                min_price=min_price,
                min_adv=min_adv,
                min_market_cap=min_market_cap,
                exclude_sectors=exclude_sectors,
            )

            if not _audit_printed:
                try:
                    _print_audit_summary(_price_data, _meta_data, factors_pre, factors_filtered)
                except Exception as e:
                    logger.warning(f"Audit summary failed: {e}")
                _audit_printed = True

            _factor_cache = factors_filtered
            _factor_key = fk
            _factor_audit = audit
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
                use_tstats=use_tstats,
                w_s6=W_S6, w_s12=W_S12,
                w_res6=W_RES6, w_res12=W_RES12,
                w_t6=W_T6, w_t12=W_T12,
                w_s1=W_S1, w_invvol=W_INVVOL, w_opa=W_OPA,
                winsor_p=winsor_p, vol_floor=vol_floor,
                opa_dict=get_quality_opa(),
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
    return result, audit


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
            "avg_volume": None,
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


# ─── Sector + benchmark data layer ───────────────────────────────────────────

def _normalize_sector(raw: Optional[str]) -> Optional[str]:
    """Map a raw sector string to a GICS sector key. Returns None if unmappable."""
    if not raw:
        return None
    key = raw.strip().lower()
    # Exact match
    if key in _SECTOR_NORM:
        return _SECTOR_NORM[key]
    # Substring match (raw contains a known keyword)
    for pattern, gics in _SECTOR_NORM.items():
        if pattern in key:
            return gics
    # Already a valid GICS name
    for gics in GICS_SECTOR_ETF:
        if gics.lower() == key:
            return gics
    return None


def build_sector_map(meta: dict) -> dict:
    """
    Build a deterministic sector map from existing metadata.
    Every ticker gets assigned:
      - gics_sector : canonical GICS sector name (or None → fallback)
      - sector_etf  : corresponding XL* ETF (or None → market-only)
      - benchmark   : always MARKET_BENCHMARK (VTI)
      - raw_sector  : original string from meta

    100% coverage guaranteed — no stock is dropped.
    Runs synchronously and instantly (pure dict lookup, no I/O).
    """
    result: dict = {}
    mapped   = 0
    fallback = 0

    for ticker, m in meta.items():
        raw_sector = m.get("sector")
        # Per-ticker override wins over generic normalizer
        gics = (_TICKER_SECTOR_OVERRIDE.get(ticker)
                or _normalize_sector(raw_sector))
        if gics and gics in GICS_SECTOR_ETF:
            result[ticker] = {
                "gics_sector": gics,
                "sector_etf":  GICS_SECTOR_ETF[gics],
                "benchmark":   MARKET_BENCHMARK,
                "raw_sector":  raw_sector,
            }
            mapped += 1
        else:
            result[ticker] = {
                "gics_sector": None,
                "sector_etf":  None,
                "benchmark":   MARKET_BENCHMARK,
                "raw_sector":  raw_sector,
            }
            fallback += 1

    total = len(result)
    logger.info(
        f"Sector map: {total} stocks | {mapped} sector-mapped "
        f"| {fallback} market-only fallback "
        f"| coverage {100*mapped/total:.1f}%" if total else "Sector map: 0 stocks"
    )
    return result


def _fetch_benchmark_prices_bg() -> None:
    """
    Background daemon: download daily adj-close for VTI + all 11 XL sector ETFs.
    Atomic update pattern: fetch → validate → swap into _benchmark_prices.
    Never replaces a good dataset with a partial/failed result.
    Reuses the existing diskcache for persistence.
    """
    global _benchmark_prices

    # ── Check cache first ────────────────────────────────────────────────────
    cached = cache.get(BENCHMARK_PRICES_CACHE_KEY)
    required_syms = set([MARKET_BENCHMARK, "SGOV"] + list(GICS_SECTOR_ETF.values()))
    if (cached is not None
            and isinstance(cached, pd.DataFrame)
            and not cached.empty
            and required_syms.issubset(set(cached.columns))):
        with _benchmark_lock:
            _benchmark_prices = cached
        logger.info(f"Benchmark prices from cache: {cached.shape}")
        return

    symbols = sorted(required_syms)
    logger.info(f"Fetching benchmark/sector-ETF prices: {symbols}")

    try:
        raw = yf.download(
            symbols, period="2y", auto_adjust=True,
            progress=False, timeout=90,
        )
        if raw.empty:
            logger.error("Benchmark download: empty result")
            return

        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"]
        else:
            close = raw.rename(columns={"Close": symbols[0]}) if len(symbols) == 1 else raw

        # Validate coverage — warn but don't abort on partial failure
        valid_cols = [c for c in close.columns if close[c].count() >= 252]
        missing    = required_syms - set(valid_cols)
        if missing:
            logger.warning(f"Benchmark download: insufficient data for {missing}")
        if not valid_cols:
            logger.error("Benchmark download: no valid columns — aborting")
            return

        close = close[valid_cols].sort_index().ffill(limit=5)

        # Atomic swap — only replace if new data is at least as complete
        with _benchmark_lock:
            existing = _benchmark_prices
            if existing is not None and len(existing.columns) > len(close.columns):
                logger.warning(
                    "Benchmark refresh produced fewer columns than existing data "
                    "— keeping existing"
                )
                return
            _benchmark_prices = close

        cache.set(BENCHMARK_PRICES_CACHE_KEY, close, expire=BENCHMARK_PRICES_CACHE_TTL)
        logger.info(
            f"Benchmark prices loaded: {close.shape} "
            f"| cols: {sorted(close.columns.tolist())}"
        )

    except Exception as e:
        logger.error(f"Benchmark price fetch failed: {e}")
        # Never touch _benchmark_prices on failure — keep last-known-good


def get_sector_map() -> Optional[dict]:
    """Constant-time read of the sector map (populated after stage 1)."""
    return _sector_map


def get_benchmark_prices() -> Optional[pd.DataFrame]:
    """Constant-time read of benchmark + sector ETF prices."""
    with _benchmark_lock:
        return _benchmark_prices


def get_sector_coverage_stats() -> dict:
    """Return sector mapping coverage stats for API / logging."""
    sm = _sector_map
    if sm is None:
        return {"status": "not_built"}

    total    = len(sm)
    mapped   = sum(1 for v in sm.values() if v.get("sector_etf") is not None)
    fallback = total - mapped

    etf_counts: dict = {}
    gics_counts: dict = {}
    for v in sm.values():
        etf  = v.get("sector_etf") or "_market_only"
        gics = v.get("gics_sector") or "_unmapped"
        etf_counts[etf]   = etf_counts.get(etf, 0)   + 1
        gics_counts[gics] = gics_counts.get(gics, 0) + 1

    bp = _benchmark_prices
    return {
        "total_stocks":            total,
        "sector_mapped":           mapped,
        "market_only_fallback":    fallback,
        "coverage_pct":            round(100 * mapped / total, 1) if total else 0.0,
        "market_benchmark":        MARKET_BENCHMARK,
        "sector_etfs":             sorted(GICS_SECTOR_ETF.values()),
        "etf_stock_counts":        etf_counts,
        "gics_stock_counts":       gics_counts,
        "benchmark_prices_ok":     (bp is not None and not bp.empty),
        "benchmark_symbols_loaded": sorted(bp.columns.tolist()) if bp is not None else [],
    }


# ─── Initial data load ───────────────────────────────────────────────────────

def _launch_benchmark_bg() -> None:
    """Start benchmark price fetch in a daemon thread (non-blocking)."""
    t = threading.Thread(target=_fetch_benchmark_prices_bg, daemon=True,
                         name="benchmark-fetch")
    t.start()


# ─── Quality data background layer ───────────────────────────────────────────

def _fetch_quality_bg() -> None:
    """
    Background daemon: fetch/restore quality financials for all universe tickers.

    Flow:
      1. Pre-load any existing diskcache snapshot into _quality_opa immediately
         (makes the quality endpoints respond in milliseconds on restart — never
         leaves a coverage gap between restarts).
      2. If the cache is fresh (not expired) for all tickers: done.
      3. If stale or missing: fetch sequentially via quality_loader
         (may take ~10–15 min on a cold run; incremental saves every 100 tickers
         preserve progress so a crash can resume mid-batch).
      4. Apply formula hierarchy and update _quality_opa with final results.

    Never blocks the HTTP server — runs in a dedicated daemon thread.
    Sequential yfinance calls only (no ThreadPoolExecutor).
    """
    global _quality_opa
    try:
        tickers = list(_price_data.columns) if _price_data is not None else []
        if not tickers:
            logger.warning("quality_bg: no tickers available, skipping")
            return

        # ── Step 1: pre-load from cache (instant, never-drop-good-data) ──────
        from quality_loader import QUALITY_RAW_CACHE_KEY
        existing_raw = cache.get(QUALITY_RAW_CACHE_KEY) or {}
        if existing_raw:
            initial_opa = compute_opa_all(existing_raw)
            with _quality_lock:
                _quality_opa = initial_opa
            logger.info(
                f"quality_bg: pre-loaded {len(existing_raw)} cached records "
                f"({len(initial_opa)} OPA values) — endpoints now serving"
            )

        # ── Step 2: full fetch (builds/refreshes cache; may be fast if cached) ─
        logger.info(f"quality_bg: starting fetch for {len(tickers)} tickers")
        t0 = time.time()

        raw = get_or_build_quality_raw(tickers, cache)
        opa = compute_opa_all(raw)

        with _quality_lock:
            _quality_opa = opa

        elapsed = time.time() - t0
        report  = build_coverage_report(opa, tickers)
        logger.info(
            f"quality_bg: DONE in {elapsed:.1f}s — "
            f"available={report['available']}/{report['universe_count']} "
            f"({report['available_pct']}%)  "
            f"formulas={report['formula_breakdown']}"
        )
    except Exception as e:
        logger.error(f"quality_bg: unhandled error: {e}", exc_info=True)


def _launch_quality_bg() -> None:
    """Start quality data fetch in a daemon thread (non-blocking)."""
    t = threading.Thread(target=_fetch_quality_bg, daemon=True, name="quality-fetch")
    t.start()


def get_quality_opa() -> Optional[dict]:
    """Return the current quality OPA results dict, or None if not yet computed."""
    with _quality_lock:
        return _quality_opa


def initial_data_load():
    """Load universe, prices, and essential metadata. Engine becomes usable when complete."""
    global _price_data, _meta_data, _dollar_volume, _sector_map

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

            # ── Sector map (instant, deterministic) ──────────────────────────
            with _sector_map_lock:
                _sector_map = build_sector_map(_meta_data)
            _launch_benchmark_bg()
            _launch_quality_bg()

            update_status("ready",
                          f"Ready. {n} stocks loaded from cache.",
                          progress=1.0, total=n, loaded=n)
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

    # ── Sector map (instant, deterministic) ───────────────────────────────────
    with _sector_map_lock:
        _sector_map = build_sector_map(_meta_data)
    _launch_benchmark_bg()
    _launch_quality_bg()

    _timings["total_stage1"] = round(time.time() - t_start, 3)

    update_status("ready",
                  f"Ready. {len(valid_cols)} stocks loaded.",
                  progress=1.0, total=len(valid_cols), loaded=len(valid_cols))
    logger.info(f"Stage 1 complete in {_timings['total_stage1']:.1f}s — engine usable")


def start_background_load():
    """Start data loading in a daemon background thread."""
    t = threading.Thread(target=initial_data_load, daemon=True)
    t.start()
