"""
FastAPI server for the equity data engine.
Runs on a separate port, proxied by Express.
"""

import os
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
import numpy as np
import pandas as pd

import engine
from quality_audit import build_coverage_report, build_per_ticker_audit, export_audit_json

app = FastAPI(title="Equity Engine")

app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Tag system ────────────────────────────────────────────────────────────────
# Tags are applied AFTER all ranking, z-score, and quality computations.
# They are purely decorative signals — they never affect rankings or formulas.
#
# To define a tag, add an entry to TAG_DEFINITIONS:
#   "tag_key": {
#       "label":       "Human-readable name",
#       "shortLabel":  "HQ",   # 1–3 chars, shown in mobile badge
#       "description": "Tooltip text shown to the user",
#       "color":       "emerald",  # emerald | amber | sky | rose | violet | slate
#       "_rule":       lambda s: bool(condition_on_stock_dict),  # stripped before JSON
#   }
#
# Rules read ONLY final computed fields (alpha, zQ, rank, percentile, etc.).
# They MUST NOT modify any field or trigger recomputation.
TAG_DEFINITIONS: dict = {
    # ── Audit: profitability formula is not the primary (op-income) formula ──
    "fallback_profitability": {
        "label":       "Fallback Formula",
        "shortLabel":  "FB",
        "description": "Profitability uses EBIT or net-income proxy — primary op-income data unavailable",
        "color":       "amber",
        # Primary formula key is "op_income/avg_assets".
        # Trigger when: formula is set (not missing) AND it is NOT the primary.
        "_rule": lambda s: (
            s.get("qualityFormula") is not None
            and s.get("qualityFormula") != "op_income/avg_assets"
        ),
    },

    # ── Audit: no valid profitability input at all ────────────────────────────
    "quality_missing": {
        "label":       "Q Missing",
        "shortLabel":  "Q?",
        "description": "No valid profitability data — quality signal unavailable for this stock",
        "color":       "slate",
        "_rule": lambda s: bool(s.get("qualityMissing")),
    },

    # ── Signal: high profitability by OPA z-score ────────────────────────────
    "high_profitability": {
        "label":       "High Profitability",
        "shortLabel":  "HP",
        "description": "OPA z-score ≥ +0.75 — top ~23% of universe by profitability (display only)",
        "color":       "emerald",
        # zQ is None when quality is missing; treat None as 0 via safe guard.
        "_rule": lambda s: (s.get("zQ") is not None) and (s["zQ"] >= 0.75),
    },

    # ── Signal: low profitability by OPA z-score ─────────────────────────────
    "low_profitability": {
        "label":       "Low Profitability",
        "shortLabel":  "LP",
        "description": "OPA z-score ≤ −0.75 — bottom ~23% of universe by profitability (display only)",
        "color":       "rose",
        "_rule": lambda s: (s.get("zQ") is not None) and (s["zQ"] <= -0.75),
    },
}


def _apply_tags(stock: dict) -> list:
    """
    Assign tags to a single stock by reading its final computed values only.
    Called AFTER all rankings, z-scores, and quality computations are complete.
    NEVER modifies any value. NEVER affects ranking math.
    """
    tags = []
    for key, defn in TAG_DEFINITIONS.items():
        rule = defn.get("_rule")
        if callable(rule):
            try:
                if rule(stock):
                    tags.append(key)
            except Exception:
                pass  # malformed rule — silently skip, never crash rankings
    return tags


# Start background data load on startup
@app.on_event("startup")
def startup():
    engine.start_background_load()


@app.get("/status")
def get_status():
    status = engine.get_status()
    return {
        "status": status["status"],
        "message": status["message"],
        "progress": status["progress"],
        "total": status["total"],
        "loaded": status["loaded"],
        "cached_at": status.get("cached_at"),
        "timings": status.get("timings", {}),
    }


@app.get("/sector-map")
def get_sector_map(full: bool = False):
    """
    Return sector mapping coverage stats.
    ?full=true also returns the per-ticker mapping dict.
    """
    stats = engine.get_sector_coverage_stats()
    if not full:
        return stats
    sm = engine.get_sector_map()
    return {**stats, "map": sm or {}}


@app.get("/residual-audit")
def get_residual_audit():
    """Return the most recent residual momentum regression audit."""
    return engine.get_residual_audit()


@app.get("/quality-coverage")
def quality_coverage():
    """
    Return quality data coverage summary.

    Fields:
      status:            "ready" | "loading" | "not_started"
      universe_count:    total tickers in price universe
      available:         count with OPA computed
      available_pct:     percentage of universe with quality data
      unavailable:       count without quality data
      formula_breakdown: {formula: count}  (op_income, ebit, net_income paths)
      reason_breakdown:  top reasons for unavailability
      using_avg_assets:  count using average of current + prior year assets
      using_cur_assets:  count using current year assets only (prior not available)
    """
    opa = engine.get_quality_opa()
    if opa is None:
        return {"status": "loading", "message": "Quality data fetch in progress"}
    price_data = engine.get_price_data()
    universe   = list(price_data.columns) if price_data is not None else []
    report     = build_coverage_report(opa, universe)
    report["status"] = "ready"
    return report


@app.get("/quality-audit")
def quality_audit_endpoint(
    ticker: Optional[str] = Query(None, description="Filter to a single ticker"),
    formula: Optional[str] = Query(None, description="Filter by formula used"),
    available: Optional[bool] = Query(None, description="Filter by availability"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    export_json: bool = Query(False, description="Return full JSON export"),
):
    """
    Per-ticker quality audit records.

    Each record contains: ticker, available, formula, opa, numerator, denominator,
    using_avg_assets, assets_current, assets_prior, period_date, source_fields, reason.

    Query params:
      ticker:      single ticker lookup
      formula:     filter by formula path (op_income/avg_assets, ebit/*, net_income/*, unavailable)
      available:   true = only available, false = only unavailable
      limit/offset: pagination (max 5000)
      export_json: return the full unfiltered list as a JSON attachment
    """
    opa = engine.get_quality_opa()
    if opa is None:
        return {"status": "loading", "message": "Quality data fetch in progress", "records": []}

    price_data = engine.get_price_data()
    universe   = list(price_data.columns) if price_data is not None else list(opa.keys())

    records = build_per_ticker_audit(opa, universe)

    if export_json:
        from fastapi.responses import Response
        body = export_audit_json(records)
        return Response(
            content=body,
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=quality_audit.json"},
        )

    # Filters
    if ticker:
        records = [r for r in records if r["ticker"].upper() == ticker.upper()]
    if formula is not None:
        records = [r for r in records if formula in (r["formula"] or "")]
    if available is not None:
        records = [r for r in records if r["available"] == available]

    total    = len(records)
    page     = records[offset: offset + limit]

    return {
        "status":  "ready",
        "total":   total,
        "offset":  offset,
        "limit":   limit,
        "records": page,
    }


@app.get("/rankings")
def get_rankings(
    vol_adjust: bool = Query(True),
    use_tstats: bool = Query(False),
    vol_floor: float = Query(0.10),
    winsor_p: float = Query(2.0),
    cluster_n: int = Query(100),
    cluster_k: int = Query(10),
    cluster_lookback: int = Query(252),
    exclude_sectors: Optional[str] = Query(None),
):
    status = engine.get_status()
    if status["status"] == "loading":
        return {"status": "loading", "message": status["message"],
                "progress": status["progress"], "total": status["total"],
                "loaded": status["loaded"]}

    if status["status"] == "error":
        raise HTTPException(status_code=500, detail=status["message"])

    sectors_list = [s.strip() for s in exclude_sectors.split(",") if s.strip()] if exclude_sectors else []

    params = {
        "vol_adjust": vol_adjust,
        "use_tstats": use_tstats,
        "vol_floor": vol_floor,
        "winsor_p": winsor_p,
        "cluster_n": cluster_n,
        "cluster_k": cluster_k,
        "cluster_lookback": cluster_lookback,
        "exclude_sectors": sectors_list,
    }

    result = engine.get_ranked_data(params)
    if result is None:
        return {"stocks": [], "total": 0, "cluster_count": 0, "cached_at": None}
    df, audit = result
    if df is None or df.empty:
        return {"stocks": [], "total": 0, "cluster_count": 0, "cached_at": None, "audit": audit}

    def safe(v):
        if v is None:
            return None
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return None
        if hasattr(v, "item"):
            return v.item()
        return v

    def safe_str(v, default=None):
        if v is None:
            return default
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return default
        return str(v)

    def safe_bool(v, default=False):
        if v is None:
            return default
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return default
        return bool(v)

    _sector_override = getattr(engine, "_TICKER_SECTOR_OVERRIDE", {})
    _unmapped_sector_labels = {"Miscellaneous", "Other", "N/A"}

    # ── Quality OPA join (serialization layer only — cache untouched) ──────────
    quality_opa = engine.get_quality_opa() or {}

    def _compute_zq(opa_values: list) -> dict:
        """Winsorize + z-score a list of (ticker, opa) pairs, return {ticker: zQ}."""
        if len(opa_values) < 2:
            return {}
        tickers_in, vals = zip(*opa_values)
        arr = np.array(vals, dtype=float)
        p2, p98 = np.nanpercentile(arr, [2, 98])
        clipped = np.clip(arr, p2, p98)
        mu = np.nanmean(clipped)
        sd = np.nanstd(clipped)
        if sd < 1e-12:
            return {t: 0.0 for t in tickers_in}
        z = (clipped - mu) / sd
        return {t: float(z[i]) for i, t in enumerate(tickers_in)}

    def _build_zq_map(df_rows: pd.DataFrame) -> dict:
        """
        Build {ticker: zQ} using industry → sector → universe peer hierarchy.
        Min peer group N ≥ 10 with valid OPA. Falls back to next level if < 10.
        Missing OPA → zQ = None (NOT 0).
        """
        _MIN_PEERS = 10

        records = []
        for _, row in df_rows.iterrows():
            t = row["ticker"]
            ind = row.get("industry")
            sec = row.get("sector")
            rec = quality_opa.get(t)
            opa_val = rec.get("opa") if rec and rec.get("available") else None
            records.append((t, ind, sec, opa_val))

        # Industry-level groups
        from collections import defaultdict
        by_industry: dict = defaultdict(list)
        by_sector: dict = defaultdict(list)
        universe_valid: list = []
        for t, ind, sec, opa_val in records:
            if opa_val is not None:
                if ind:
                    by_industry[ind].append((t, opa_val))
                if sec:
                    by_sector[sec].append((t, opa_val))
                universe_valid.append((t, opa_val))

        # Precompute z-scores at each level
        zq_industry = {}
        for grp, pairs in by_industry.items():
            if len(pairs) >= _MIN_PEERS:
                zq_industry.update(_compute_zq(pairs))

        zq_sector = {}
        for grp, pairs in by_sector.items():
            if len(pairs) >= _MIN_PEERS:
                zq_sector.update(_compute_zq(pairs))

        zq_universe = {}
        if len(universe_valid) >= _MIN_PEERS:
            zq_universe = _compute_zq(universe_valid)

        result = {}
        for t, ind, sec, opa_val in records:
            if opa_val is None:
                result[t] = None
            elif t in zq_industry:
                result[t] = zq_industry[t]
            elif t in zq_sector:
                result[t] = zq_sector[t]
            elif t in zq_universe:
                result[t] = zq_universe[t]
            else:
                result[t] = None
        return result

    zq_map = _build_zq_map(df)

    stocks = []
    for _, row in df.iterrows():
        _ticker = row["ticker"]
        _raw_sector = safe(row.get("sector"))
        # Apply override when raw sector is missing or a non-GICS catch-all label.
        # If no override exists either, return None so the frontend treats the ticker
        # as unmapped (excluded from sector-deficit math, never forms a fake bucket).
        if not _raw_sector or _raw_sector in _unmapped_sector_labels:
            _sector = _sector_override.get(_ticker)  # None when no override → unmapped
        else:
            _sector = _raw_sector

        _q_rec = quality_opa.get(_ticker)
        _q_available = bool(_q_rec and _q_rec.get("available"))
        _q_missing = not _q_available
        stocks.append({
            "ticker":    _ticker,
            "name":      row.get("name") or _ticker,
            "sector":    _sector,
            "industry":  safe(row.get("industry")),
            "price":     safe(row.get("price")),
            "marketCap": safe(row.get("market_cap")),
            "adv":       safe(row.get("adv")),
            "r1":        safe(row.get("r1")),
            "m6":        safe(row.get("m6")),
            "m12":       safe(row.get("m12")),
            "sigma1":    safe(row.get("sigma1")),
            "sigma6":    safe(row.get("sigma6")),
            "sigma12":   safe(row.get("sigma12")),
            "s1":        safe(row.get("s1")),
            "s6":        safe(row.get("s6")),
            "s12":       safe(row.get("s12")),
            "tstat6":    safe(row.get("tstat6")),
            "tstat12":   safe(row.get("tstat12")),
            "zS6":       safe(row.get("zS6")),
            "zS12":      safe(row.get("zS12")),
            "zT6":       safe(row.get("zT6")),
            "zT12":      safe(row.get("zT12")),
            "zR6":       safe(row.get("zR6")),
            "zR12":      safe(row.get("zR12")),
            "zS1":       safe(row.get("zS1")),
            "zInvVol":   safe(row.get("zInvVol")),
            "zOPA":      safe(row.get("zOPA")),
            "sigmaEwma": safe(row.get("sigma_ewma")),
            "res6":      safe(row.get("res6")),
            "res12":     safe(row.get("res12")),
            "S6blend":   safe(row.get("S6_blend")),
            "S12blend":  safe(row.get("S12_blend")),
            "regType":   safe_str(row.get("reg_type"), "none"),
            "sSleeve":      safe(row.get("sSleeve")),
            "tSleeve":      safe(row.get("tSleeve")),
            "revSleeve":    safe(row.get("revSleeve")),
            "alpha":        safe(row.get("alpha")),
            "rank":         safe(row.get("rank")),
            "percentile":   safe(row.get("percentile")),
            "cluster":      safe(row.get("cluster")),
            "alphaFormula": safe_str(row.get("alpha_formula"), "S+T"),
            # ── Quality / Profitability (display-only, does not affect alpha) ──
            "quality":              safe(_q_rec.get("opa") if _q_rec else None),
            "zQ":                   safe(zq_map.get(_ticker)),
            "qualityMissing":       _q_missing,
            "qualityMissingReason": safe_str(_q_rec.get("reason") if _q_rec else "not_computed"),
            "qualityFormula":       safe_str(_q_rec.get("formula") if _q_rec else None),
        })

    cluster_vals = [s["cluster"] for s in stocks if s["cluster"] is not None]
    cluster_count = len(set(cluster_vals))

    # ── Quality coverage stats for audit ──────────────────────────────────────
    _q_total = len(stocks)
    _q_primary_count        = sum(1 for s in stocks if s.get("qualityFormula") == "op_income/avg_assets")
    _q_ebit_count           = sum(1 for s in stocks if s.get("qualityFormula") == "ebit/avg_assets")
    _q_net_income_count     = sum(1 for s in stocks if s.get("qualityFormula") == "net_income/avg_assets")
    _q_missing_count        = sum(1 for s in stocks if s.get("qualityMissing", True))
    _q_available_count      = _q_primary_count + _q_ebit_count + _q_net_income_count

    def _pct(n: int) -> float:
        return round(n / _q_total * 100, 1) if _q_total > 0 else 0.0

    audit["qualityCoverage"]               = f"{_q_available_count}/{_q_total}"
    audit["qualityPct"]                    = _pct(_q_available_count)
    audit["qualityPrimaryCount"]           = _q_primary_count
    audit["qualityPrimaryPct"]             = _pct(_q_primary_count)
    audit["qualityEbitFallbackCount"]      = _q_ebit_count
    audit["qualityEbitFallbackPct"]        = _pct(_q_ebit_count)
    audit["qualityNetIncomeFallbackCount"] = _q_net_income_count
    audit["qualityNetIncomeFallbackPct"]   = _pct(_q_net_income_count)
    audit["qualityMissingCount"]           = _q_missing_count
    audit["qualityMissingPct"]             = _pct(_q_missing_count)

    # ── Tag system (post-calculation, display-only) ──────────────────────────
    # Applied AFTER all rankings, z-scores, alpha, and quality computations.
    # Tags NEVER modify any computed value. NEVER affect rankings or z-scores.
    # Add entries to TAG_DEFINITIONS to activate tags.
    for s in stocks:
        s["tags"] = _apply_tags(s)

    # Strip internal _rule callables before serializing tagDefinitions to JSON
    tag_defs_public = {
        k: {kk: vv for kk, vv in v.items() if not kk.startswith("_")}
        for k, v in TAG_DEFINITIONS.items()
    }

    return {
        "stocks": stocks,
        "total": len(stocks),
        "cluster_count": cluster_count,
        "cached_at": status.get("cached_at"),
        "audit": audit,
        "tagDefinitions": tag_defs_public,
    }


class UniverseFiltersBody(BaseModel):
    min_price: float = 5.0
    min_adv: float = 1e7
    min_market_cap: float = 1e9
    vol_adjust: bool = True
    use_tstats: bool = False
    w6: float = 0.5
    w12: float = 0.5
    vol_floor: float = 0.05
    winsor_p: float = 2.0
    cluster_n: int = 100
    cluster_k: int = 10
    cluster_lookback: int = 252


@app.post("/universe-filters")
def universe_filters(body: UniverseFiltersBody):
    params = body.dict()
    result = engine.get_ranked_data(params)
    if result is None:
        return {"stocks": [], "total": 0, "cluster_count": 0, "cached_at": None}
    df, _audit = result
    if df is None or df.empty:
        return {"stocks": [], "total": 0, "cluster_count": 0, "cached_at": None}

    filtered, filter_audit = engine.apply_universe_filters(
        df,
        min_price=body.min_price,
        min_adv=body.min_adv,
        min_market_cap=body.min_market_cap,
    )

    def safe(v):
        if v is None:
            return None
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return None
        if hasattr(v, "item"):
            return v.item()
        return v

    def safe_str(v, default=None):
        if v is None:
            return default
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return default
        return str(v)

    def safe_bool(v, default=False):
        if v is None:
            return default
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return default
        return bool(v)

    stocks = []
    for _, row in filtered.iterrows():
        stocks.append({
            "ticker":       row["ticker"],
            "name":         row.get("name") or row["ticker"],
            "sector":       safe(row.get("sector")),
            "industry":     safe(row.get("industry")),
            "price":        safe(row.get("price")),
            "marketCap":    safe(row.get("market_cap")),
            "adv":          safe(row.get("adv")),
            "r1":           safe(row.get("r1")),
            "m6":           safe(row.get("m6")),
            "m12":          safe(row.get("m12")),
            "sigma6":       safe(row.get("sigma6")),
            "sigma12":      safe(row.get("sigma12")),
            "s6":           safe(row.get("s6")),
            "s12":          safe(row.get("s12")),
            "tstat6":       safe(row.get("tstat6")),
            "tstat12":      safe(row.get("tstat12")),
            "zS6":          safe(row.get("zS6")),
            "zS12":         safe(row.get("zS12")),
            "zT6":          safe(row.get("zT6")),
            "zT12":         safe(row.get("zT12")),
            "sSleeve":      safe(row.get("sSleeve")),
            "tSleeve":      safe(row.get("tSleeve")),
            "revSleeve":    safe(row.get("revSleeve")),
            "alpha":        safe(row.get("alpha")),
            "rank":         safe(row.get("rank")),
            "percentile":   safe(row.get("percentile")),
            "cluster":      safe(row.get("cluster")),
            "alphaFormula": safe_str(row.get("alpha_formula"), "S+T"),
        })

    cluster_count = len(set(s["cluster"] for s in stocks if s["cluster"] is not None))
    return {
        "stocks": stocks,
        "total": len(stocks),
        "cluster_count": cluster_count,
        "cached_at": engine.get_status().get("cached_at"),
    }


VOL_TARGET = 0.15  # 15% annualized portfolio vol target


class PortfolioHolding(BaseModel):
    ticker: str
    weight: float


class PortfolioRiskRequest(BaseModel):
    holdings: List[PortfolioHolding]
    lookback: int = 252
    weighting_method: str = "equal"  # "equal", "inverse_vol", "min_var"
    cluster_n: int = 100
    cluster_k: int = 10
    cluster_lookback: int = 252


class CorrSeedRequest(BaseModel):
    tickers: List[str]       # candidates in alpha-rank order (best first)
    n: int = 20              # max basket size
    max_corr: float = 0.70   # pairwise correlation ceiling
    lookback: int = 252      # trading days used for correlation


MIN_VAR_MAX_WEIGHT = 0.40   # single-name concentration cap
MIN_VAR_RIDGE_BASE = 1e-4  # ridge coefficient (relative to trace/n)


def _compute_min_var_weights(
    log_rets,  # pd.DataFrame of daily log returns (rows=days, cols=tickers)
) -> tuple[Optional[np.ndarray], str, bool]:
    """
    Institutional-grade long-only minimum-variance optimiser.

    Design:
    - sklearn LedoitWolf fitted directly on raw daily returns (correct oracle shrinkage)
    - Diagonal ridge regularisation for numerical stability
    - Condition-number check with automatic ridge escalation
    - Long-only, fully-invested, per-name cap (40%)
    - Multi-start SLSQP: equal weights + 3 Dirichlet random starts
    - Returns (weights, cov_model_label, concentration_capped)
      or (None, label, False) on total failure
    """
    import pandas as pd
    from scipy.optimize import minimize

    returns = log_rets.values  # shape (T, n)
    n = returns.shape[1]

    # ── Covariance estimation ────────────────────────────────────────────────
    try:
        from sklearn.covariance import LedoitWolf
        lw = LedoitWolf(store_precision=False, assume_centered=False)
        lw.fit(returns)
        # Annualise: LedoitWolf gives daily cov, multiply by 252
        cov_ann = lw.covariance_ * 252
        rho = float(lw.shrinkage_)
        cov_model = f"lw(ρ={rho:.2f})"
    except Exception:
        # Fallback: plain sample covariance
        cov_ann = (log_rets.cov().values) * 252
        cov_model = "sample"

    # ── Ridge regularisation ─────────────────────────────────────────────────
    trace = float(np.trace(cov_ann))
    ridge_lambda = MIN_VAR_RIDGE_BASE * (trace / n)
    cov_reg = cov_ann + ridge_lambda * np.eye(n)

    # Escalate ridge if still ill-conditioned
    try:
        cond = np.linalg.cond(cov_reg)
        if cond > 1e6:
            ridge_lambda *= 10.0
            cov_reg = cov_ann + ridge_lambda * np.eye(n)
            cov_model += "+ridge²"
        else:
            cov_model += "+ridge"
    except np.linalg.LinAlgError:
        cov_model += "+ridge"

    # ── Optimisation ─────────────────────────────────────────────────────────
    max_w = min(MIN_VAR_MAX_WEIGHT, 1.0)
    bounds = [(0.0, max_w)] * n
    constraints = [{"type": "eq", "fun": lambda w: w.sum() - 1.0}]

    def portfolio_variance(w: np.ndarray) -> float:
        return float(w @ cov_reg @ w)

    def try_optimize(x0: np.ndarray) -> Optional[np.ndarray]:
        try:
            res = minimize(
                portfolio_variance,
                x0=x0,
                method="SLSQP",
                bounds=bounds,
                constraints=constraints,
                options={"maxiter": 1000, "ftol": 1e-12},
            )
            if res.success and np.all(np.isfinite(res.x)):
                w = np.maximum(res.x, 0.0)
                s = w.sum()
                if s > 1e-8:
                    return w / s
        except Exception:
            pass
        return None

    # Multi-start: equal weight + 3 Dirichlet seeds
    rng = np.random.default_rng(42)
    starts = [np.ones(n) / n]
    for _ in range(3):
        raw = rng.dirichlet(np.ones(n))
        raw = np.minimum(raw, max_w)
        raw /= raw.sum()
        starts.append(raw)

    best: Optional[np.ndarray] = None
    best_var = np.inf
    for x0 in starts:
        candidate = try_optimize(x0)
        if candidate is not None:
            v = portfolio_variance(candidate)
            if v < best_var:
                best_var = v
                best = candidate

    if best is None:
        return None, cov_model, False

    concentration_capped = bool(np.any(best >= max_w - 1e-4))
    return best, cov_model, concentration_capped


# ─── Additional weighting helpers ─────────────────────────────────────────────

ERC_MAX_WEIGHT = 0.15   # risk-parity per-name cap
MV_MAX_WEIGHT  = 0.15   # mean-variance per-name cap
EWMA_LAMBDA    = 0.94   # EWMA decay factor


def _build_ewma_cov(
    log_rets,                  # pd.DataFrame: rows=days, cols=tickers
    lam: float = EWMA_LAMBDA,
    vol_floor: float = 0.05,
) -> tuple:
    """
    EWMA covariance matrix (annualized) with diagonal ridge.
    Returns (cov_reg, diag_vols, label).
    """
    returns = log_rets.values          # (T, n)
    T, n = returns.shape
    if T < 5:
        raise ValueError("Insufficient return history for EWMA")
    decay = np.array([(1 - lam) * (lam ** k) for k in range(T - 1, -1, -1)])
    decay /= decay.sum()
    mu = (decay[:, None] * returns).sum(axis=0)
    c  = returns - mu
    cov_daily = (decay[:, None] * c).T @ c
    cov_ann   = cov_daily * 252
    trace  = float(np.trace(cov_ann))
    ridge  = max(1e-4 * trace / max(n, 1), 1e-6)
    cov_reg    = cov_ann + ridge * np.eye(n)
    diag_vols  = np.maximum(np.sqrt(np.diag(cov_reg)), vol_floor)
    return cov_reg, diag_vols, f"ewma(λ={lam})+ridge"


def _compute_risk_parity_weights(
    cov_ann,
    vol_floor: float = 0.05,
    max_w: float = ERC_MAX_WEIGHT,
) -> np.ndarray:
    """
    Capped Equal Risk Contribution (ERC) via SLSQP.

    Objective (all constraints inside the solver — no post-hoc clipping):
        min_w  Σ_i (RC_i − RC_mean)²
        where  RC_i = w_i · (Σw)_i   (proportional risk contribution)
    Constraints:
        Σ w_i = 1      (fully invested)
        0 ≤ w_i ≤ max_w   (long-only + per-name cap)

    Analytical gradient (derived via chain rule):
        ∂f/∂w_k = 2 · [ v_k · (Σw)_k + (Σ(v * w))_k ]
        where  v_i = RC_i − RC_mean

    Multi-start (equal weights + 3 Dirichlet seeds) for robustness.

    Fallback: Spinu L-BFGS-B (positivity-only bounds) followed by a
    *defensive* post-hoc clip.  This path is only reached if all SLSQP
    starts fail (extremely rare for n ≤ 40 with a PD covariance).
    """
    from scipy.optimize import minimize

    n    = cov_ann.shape[0]
    vols = np.maximum(np.sqrt(np.diag(cov_ann)), vol_floor)

    def erc_obj(w: np.ndarray) -> float:
        Sw = cov_ann @ w
        rc = w * Sw
        mu = rc.mean()
        return float(np.dot(rc - mu, rc - mu))

    def erc_grad(w: np.ndarray) -> np.ndarray:
        Sw  = cov_ann @ w
        rc  = w * Sw
        v   = rc - rc.mean()                  # deviations from mean RC
        return 2.0 * (v * Sw + cov_ann @ (v * w))

    bounds      = [(0.0, max_w)] * n
    constraints = [{"type": "eq",
                    "fun": lambda w: w.sum() - 1.0,
                    "jac": lambda w: np.ones(n)}]

    rng    = np.random.default_rng(42)
    starts = [np.ones(n) / n]
    for _ in range(3):
        raw  = rng.dirichlet(np.ones(n))
        raw  = np.minimum(raw, max_w)
        raw /= raw.sum()
        starts.append(raw)

    best: Optional[np.ndarray] = None
    best_val = np.inf

    for x0 in starts:
        try:
            res = minimize(
                erc_obj, x0=x0, jac=erc_grad, method="SLSQP",
                bounds=bounds, constraints=constraints,
                options={"maxiter": 2000, "ftol": 1e-12},
            )
            if res.success and np.all(np.isfinite(res.x)):
                w = np.maximum(res.x, 0.0)   # defensive numerical cleanup only
                s = w.sum()
                if s > 1e-8:
                    w /= s
                    v = erc_obj(w)
                    if v < best_val:
                        best_val, best = v, w
        except Exception:
            pass

    if best is not None:
        return best

    # ── Fallback: Spinu L-BFGS-B (only reached if all SLSQP starts fail) ────
    b  = np.ones(n) / n
    x0 = 1.0 / vols
    x0 /= x0.sum()

    def spinu_obj(x: np.ndarray) -> float:
        return 0.5 * float(x @ cov_ann @ x) - float(b @ np.log(np.maximum(x, 1e-16)))

    def spinu_grad(x: np.ndarray) -> np.ndarray:
        return cov_ann @ x - b / np.maximum(x, 1e-16)

    try:
        res = minimize(
            spinu_obj, x0=x0, jac=spinu_grad, method="L-BFGS-B",
            bounds=[(1e-8, None)] * n,
            options={"maxiter": 2000, "ftol": 1e-14, "gtol": 1e-9},
        )
        x = res.x if (res.success and np.all(np.isfinite(res.x)) and np.all(res.x > 0)) else x0
    except Exception:
        x = x0

    best = x / x.sum()
    # Defensive post-hoc clip (fallback path only)
    for _ in range(50):
        prev = best.copy()
        best = np.clip(best, 0.0, max_w)
        best /= best.sum()
        if np.max(np.abs(best - prev)) < 1e-9:
            break
    return best


def _compute_mean_variance_weights(
    cov_ann,
    signals_arr,
    max_w: float = MV_MAX_WEIGHT,
    risk_aversion: float = 1.0,
) -> Optional[np.ndarray]:
    """
    Constrained Mean-Variance: maximise  μ'w − (γ/2) w'Σw
    subject to  sum(w) = 1,  0 ≤ w_i ≤ max_w.
    Signals normalised to unit std; multi-start SLSQP for robustness.
    Distinct from Min-Var: uses expected return signal, not zero-signal.
    """
    from scipy.optimize import minimize

    n   = cov_ann.shape[0]
    sig = signals_arr.astype(float).copy()
    std = float(np.std(sig))
    if std > 1e-8:
        sig /= std

    def neg_utility(w: np.ndarray) -> float:
        return float(0.5 * risk_aversion * (w @ cov_ann @ w) - sig @ w)

    bounds      = [(0.0, max_w)] * n
    constraints = [{"type": "eq", "fun": lambda w: w.sum() - 1.0}]
    rng         = np.random.default_rng(42)
    starts      = [np.ones(n) / n]
    for _ in range(3):
        raw = rng.dirichlet(np.ones(n))
        raw = np.minimum(raw, max_w)
        raw /= raw.sum()
        starts.append(raw)

    best: Optional[np.ndarray] = None
    best_val = np.inf
    for x0 in starts:
        try:
            res = minimize(
                neg_utility, x0=x0, method="SLSQP",
                bounds=bounds, constraints=constraints,
                options={"maxiter": 1000, "ftol": 1e-12},
            )
            if res.success and np.all(np.isfinite(res.x)):
                w = np.maximum(res.x, 0.0)
                s = w.sum()
                if s > 1e-8:
                    w /= s
                    v = neg_utility(w)
                    if v < best_val:
                        best_val, best = v, w
        except Exception:
            pass
    if best is None:
        return None
    # Iterative projection: clip-normalise until all base weights ≤ max_w
    for _ in range(50):
        prev = best.copy()
        best = np.clip(best, 0.0, max_w)
        best /= best.sum()
        if np.max(np.abs(best - prev)) < 1e-9:
            break
    return best


class ReversalRequest(BaseModel):
    tickers: List[str]


@app.post("/portfolio-reversal")
def portfolio_reversal(body: ReversalRequest):
    import engine as eng
    import math

    if not body.tickers:
        raise HTTPException(status_code=400, detail="No tickers provided")

    status = eng.get_status()
    if status["status"] != "ready":
        raise HTTPException(status_code=503, detail="Data not ready")

    price_data = eng.get_price_data()
    if price_data is None:
        raise HTTPException(status_code=503, detail="Price data not available")

    sector_map = eng.get_sector_map() or {}

    # Last 22 rows for 21-day log return
    tail = price_data.tail(22)
    if len(tail) < 22:
        raise HTTPException(status_code=400, detail="Insufficient price history (need ≥22 rows)")

    # Compute 21-day log return for all universe tickers with valid data
    universe_r21: dict[str, float] = {}
    for col in price_data.columns:
        p_start = tail[col].iloc[0]
        p_end   = tail[col].iloc[-1]
        if pd.notna(p_start) and pd.notna(p_end) and p_start > 0 and p_end > 0:
            universe_r21[col] = math.log(p_end / p_start)

    # Group universe tickers by gics_sector and compute mean r21 per sector
    sector_r21_lists: dict[str, list[float]] = {}
    for ticker, r21 in universe_r21.items():
        info = sector_map.get(ticker) or {}
        sector = info.get("gics_sector") if isinstance(info, dict) else None
        if sector:
            sector_r21_lists.setdefault(sector, []).append(r21)

    sector_means: dict[str, float] = {
        s: sum(vals) / len(vals)
        for s, vals in sector_r21_lists.items()
        if len(vals) >= 3
    }

    # Process portfolio holdings
    skipped: list[str] = []
    valid_items: list[dict] = []

    for ticker in body.tickers:
        if ticker not in universe_r21:
            skipped.append(ticker)
            continue
        r21 = universe_r21[ticker]
        info = sector_map.get(ticker) or {}
        sector = info.get("gics_sector") if isinstance(info, dict) else None
        if sector and sector in sector_means:
            r21_res = r21 - sector_means[sector]
        else:
            r21_res = r21  # fallback: no neutralization
        valid_items.append({"ticker": ticker, "r21": r21, "r21Res": r21_res})

    if len(valid_items) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 holdings with valid price history to compute Z-scores")

    # Z-score r21_res across portfolio holdings only
    res_values = [x["r21Res"] for x in valid_items]
    mean_res = sum(res_values) / len(res_values)
    var_res = sum((v - mean_res) ** 2 for v in res_values) / max(len(res_values) - 1, 1)
    std_res = math.sqrt(var_res) if var_res > 1e-10 else 1.0

    for item in valid_items:
        z = (item["r21Res"] - mean_res) / std_res
        item["zScore"] = z
        item["reversalScore"] = -z  # higher = more dipped

    # Rank by reversal score (1 = most dipped)
    valid_items.sort(key=lambda x: x["reversalScore"], reverse=True)
    n = len(valid_items)
    for i, item in enumerate(valid_items):
        item["rank"] = i + 1
        item["pct"] = round((i + 1) / n, 4)

    return {"items": valid_items, "skipped": skipped}


@app.post("/portfolio-risk")
def portfolio_risk(body: PortfolioRiskRequest):
    import engine as eng
    import logging
    logger = logging.getLogger("server")

    if not body.holdings:
        raise HTTPException(status_code=400, detail="No holdings provided")

    status = eng.get_status()
    if status["status"] != "ready":
        raise HTTPException(status_code=503, detail="Data not ready")

    price_data = eng.get_price_data()
    if price_data is None:
        raise HTTPException(status_code=503, detail="Price data not available")

    all_tickers = [h.ticker for h in body.holdings]

    # Filter to tickers with price data
    valid_pairs = [(t, i) for i, t in enumerate(all_tickers) if t in price_data.columns]
    if not valid_pairs:
        raise HTTPException(status_code=400, detail="No valid tickers found in price data")

    tickers_v = [t for t, _ in valid_pairs]

    # Build log returns and sample covariance (shared baseline)
    prices_sub = price_data[tickers_v].tail(body.lookback)
    log_rets   = np.log(prices_sub / prices_sub.shift(1)).dropna()
    cov_ann    = log_rets.cov().values * 252
    vols_ann   = np.maximum(np.sqrt(np.diag(cov_ann)), 0.05)

    # Pre-fetch ranked data — needed for signal-based methods + cluster labels
    _rp = {"vol_adjust": True, "use_tstats": False,
           "w6": 0.5, "w12": 0.5, "vol_floor": 0.05,
           "winsor_p": 2.0,
           "cluster_n": body.cluster_n, "cluster_k": body.cluster_k,
           "cluster_lookback": body.cluster_lookback}
    _ranked = eng.get_ranked_data(_rp)
    cluster_map: dict = {}
    signal_map:  dict = {}
    if _ranked is not None:
        _rdf, _ = _ranked
        if _rdf is not None and not _rdf.empty:
            cluster_map = dict(zip(_rdf["ticker"], _rdf["cluster"]))
            signal_map  = dict(zip(_rdf["ticker"], _rdf["alpha"].fillna(0.0)))

    n             = len(tickers_v)
    fallback      = None
    actual_method = body.weighting_method
    cov_model     = None
    cov_overlay   = cov_ann   # covariance used for vol-target + diagnostics (may be overridden)
    names_capped: list[str] = []

    # ── Step 1: Base weights (long-only, sum = 1) ────────────────────────────
    if body.weighting_method == "equal":
        base_w = np.ones(n) / n

    elif body.weighting_method == "inverse_vol":
        inv_v  = 1.0 / vols_ann
        base_w = inv_v / inv_v.sum()

    elif body.weighting_method == "signal_vol":
        # raw_i = max(alpha_i, 0) / sigma_i  then normalise
        sigs = np.array([max(float(signal_map.get(t, 0.0)), 0.0) for t in tickers_v])
        pos  = sigs[sigs > 0]
        if len(pos) > 1:
            sigs = np.minimum(sigs, np.percentile(pos, 99))  # winsorise
        raw   = sigs / vols_ann
        total = float(raw.sum())
        if total < 1e-8:
            logger.warning("Signal/Vol: all signals ≤ 0 — falling back to Inverse Vol")
            inv_v  = 1.0 / vols_ann
            base_w = inv_v / inv_v.sum()
            actual_method = "inverse_vol"
            fallback = "Signal/Vol: all signals ≤ 0 — fell back to Inverse Vol"
        else:
            base_w = raw / total

    elif body.weighting_method == "risk_parity":
        try:
            cov_ewma, _, cov_model = _build_ewma_cov(log_rets)
            base_w    = _compute_risk_parity_weights(cov_ewma)
            cov_overlay = cov_ewma   # use same cov for vol-target (consistency)
        except Exception as e:
            logger.warning(f"Risk Parity failed ({e}) — falling back to Inverse Vol")
            inv_v  = 1.0 / vols_ann
            base_w = inv_v / inv_v.sum()
            actual_method = "inverse_vol"
            cov_model = "n/a"
            fallback = "Risk Parity failed. Fell back to Inverse Vol."

    elif body.weighting_method == "min_var":
        min_var_w, cov_model, concentration_capped = _compute_min_var_weights(log_rets)
        if min_var_w is not None:
            base_w = min_var_w
            if concentration_capped:
                fallback = f"Per-name 40% cap active (≥1 name at 40%). Cov: {cov_model}."
        else:
            logger.warning("Min Var failed — falling back to Inverse Vol")
            inv_v  = 1.0 / vols_ann
            base_w = inv_v / inv_v.sum()
            actual_method = "inverse_vol"
            cov_model = "n/a"
            fallback = "Min Var failed (all starts failed). Fell back to Inverse Vol."

    elif body.weighting_method == "mean_variance":
        try:
            cov_ewma, _, cov_model = _build_ewma_cov(log_rets)
            sigs  = np.array([float(signal_map.get(t, 0.0)) for t in tickers_v])
            mv_w  = _compute_mean_variance_weights(cov_ewma, sigs)
            if mv_w is not None:
                base_w = mv_w
                if np.any(mv_w >= MV_MAX_WEIGHT - 1e-4):
                    fallback = f"Per-name 15% cap active. Cov: {cov_model}."
            else:
                logger.warning("Mean-Variance failed — falling back to Inverse Vol")
                inv_v  = 1.0 / vols_ann
                base_w = inv_v / inv_v.sum()
                actual_method = "inverse_vol"
                cov_model = "n/a"
                fallback = "Mean-Variance failed. Fell back to Inverse Vol."
        except Exception as e:
            logger.warning(f"Mean-Variance failed ({e}) — falling back to Inverse Vol")
            inv_v  = 1.0 / vols_ann
            base_w = inv_v / inv_v.sum()
            actual_method = "inverse_vol"
            cov_model = "n/a"
            fallback = "Mean-Variance failed. Fell back to Inverse Vol."

    else:
        base_w = np.ones(n) / n
        actual_method = "equal"
        fallback = f"Unknown method '{body.weighting_method}'. Used Equal weights."

    # ── Universal max-position cap (applies to every weighting method) ─────────
    # Iteratively clip any weight that exceeds MAX_POSITION and redistribute
    # the excess pro-rata to uncapped names.  Tracks which names were genuinely
    # over the limit before clipping so names_capped is accurate.
    MAX_POSITION = 0.15
    genuinely_over: set[int] = set()
    for _ in range(n + 1):
        over_idx = np.where(base_w > MAX_POSITION + 1e-8)[0]
        if len(over_idx) == 0:
            break
        genuinely_over.update(over_idx.tolist())
        excess = float((base_w[over_idx] - MAX_POSITION).sum())
        base_w[over_idx] = MAX_POSITION
        free_idx = np.array([i for i in range(n) if i not in genuinely_over])
        if len(free_idx) == 0:
            break
        base_w[free_idx] += excess / len(free_idx)

    # Also count names sitting exactly at the cap (e.g. ERC solver pinned them there)
    AT_CAP_TOL = 1e-6
    names_capped = [tickers_v[i] for i in range(n) if base_w[i] >= MAX_POSITION - AT_CAP_TOL]
    if names_capped and not fallback:
        fallback = f"Max-pos {MAX_POSITION*100:.0f}% cap active: {', '.join(names_capped)}"

    # ── Step 2: Pre-scale portfolio vol using the same cov as optimisation ────
    pre_var = float(base_w @ cov_overlay @ base_w)
    pre_vol = float(np.sqrt(max(pre_var, 1e-12)))

    # ── Step 3: Vol-target overlay — capped at 1.0 (no leverage) ─────────────
    # multiplier ≤ 1 → equity sleeve at multiplier, residual in cash/SGOV
    # multiplier = 1 → fully invested in equity (basket vol ≤ target)
    multiplier    = min(VOL_TARGET / pre_vol, 1.0)
    final_w       = base_w * multiplier
    risky_sleeve  = float(final_w.sum())   # = multiplier (base sums to 1)
    sgov_weight   = max(0.0, round(1.0 - risky_sleeve, 6))
    gross_exposure = risky_sleeve          # for backward compat

    # ── Step 4: Portfolio vol after overlay ───────────────────────────────────
    final_port_var = float(final_w @ cov_overlay @ final_w)
    final_port_vol = float(np.sqrt(max(final_port_var, 1e-12)))

    # ── Step 5: Diagnostics ───────────────────────────────────────────────────
    # Diversification ratio: weighted avg vol / portfolio vol (base weights)
    diag_vols_overlay = np.maximum(np.sqrt(np.diag(cov_overlay)), 0.05)
    wtd_avg_vol       = float(base_w @ diag_vols_overlay)
    div_ratio         = round(wtd_avg_vol / pre_vol, 4) if pre_vol > 1e-8 else 1.0

    # Effective number of positions (Herfindahl)
    effective_n = round(1.0 / float(np.sum(base_w ** 2)), 2)

    # Per-name risk contributions (fraction of total portfolio variance; sums to 1)
    port_var_base = float(base_w @ cov_overlay @ base_w)
    mrc           = cov_overlay @ base_w              # marginal risk contributions
    rc_frac       = base_w * mrc / max(port_var_base, 1e-12)   # sums ≈ 1

    if n > 1:
        corr     = log_rets.corr().values
        mask     = ~np.eye(n, dtype=bool)
        avg_corr = float(corr[mask].mean())
    else:
        avg_corr = 1.0

    holdings_out = []
    cluster_weights: dict[int, float] = {}
    for i, t in enumerate(tickers_v):
        fw  = float(final_w[i])
        bw  = float(base_w[i])
        vol = float(vols_ann[i])
        raw_cluster = cluster_map.get(t)
        try:
            cluster = int(raw_cluster) if raw_cluster is not None and not (isinstance(raw_cluster, float) and np.isnan(raw_cluster)) else None
        except (ValueError, TypeError):
            cluster = None
        holdings_out.append({
            "ticker":      t,
            "weight":      fw,
            "baseWeight":  round(bw, 6),
            "vol":         vol,
            "riskContrib": round(float(rc_frac[i]), 6),
            "cluster":     cluster,
        })
        if cluster is not None:
            cluster_weights[cluster] = cluster_weights.get(cluster, 0.0) + fw

    cluster_dist = [
        {"cluster": k, "count": sum(1 for h in holdings_out if h["cluster"] == k),
         "weight": float(v)}
        for k, v in sorted(cluster_weights.items())
    ]

    largest_weight = float(max(final_w)) if n > 0 else 0.0

    return {
        "portfolioVol":       round(final_port_vol, 6),
        "basePortVol":        round(pre_vol, 6),
        "volTargetMultiplier": round(multiplier, 6),
        "grossExposure":      round(risky_sleeve, 6),
        "riskySleeve":        round(risky_sleeve, 6),
        "sgovWeight":         sgov_weight,
        "diversificationRatio": div_ratio,
        "effectiveN":         effective_n,
        "namesCapped":        names_capped,
        "method":             actual_method,
        "covModel":           cov_model,
        "fallback":           fallback,
        "volLookback":        body.lookback,
        "covLookback":        body.lookback,
        "avgCorrelation":     round(avg_corr, 6),
        "holdings":           holdings_out,
        "clusterDistribution": cluster_dist,
        "largestWeight":      round(largest_weight, 6),
        "numHoldings":        len(holdings_out),
    }


class PortfolioHistoryRequest(BaseModel):
    holdings: List[PortfolioHolding]
    lookback: int = 252
    sgov_weight: float = 0.0  # cash/SGOV weight hint from vol-target overlay


@app.post("/portfolio-history")
def portfolio_history(body: PortfolioHistoryRequest):
    """
    Compute a weighted log-return equity curve for the current basket.

    Formula: r_p,t = Σ_i w_i * ln(P_i,t / P_i,t-1)  +  w_cash * ln(SGOV_t / SGOV_t-1)

    Weights:
      - Equity weights (body.holdings[].weight) represent the vol-target-scaled risky sleeve.
        They are used AS-IS — not renormalized to 1 — so they reflect the true equity exposure.
      - cash_weight = max(0, 1 - sum(equity_w_used)) — derived defensively from actual weights,
        not echoed from body.sgov_weight (which is an input hint).
      - SGOV prices used when available; falls back to zero cash return, logged.

    Alignment:
      - Date index is intersected across all equity tickers + SGOV (if available).
      - Any date with ANY NaN log-return is dropped.
      - ≥252 shared valid daily return observations required; HTTP 400 otherwise.
    """
    import logging as _logging
    _log = _logging.getLogger("server")

    if not body.holdings:
        raise HTTPException(status_code=400, detail="No holdings provided")

    status = engine.get_status()
    if status["status"] != "ready":
        raise HTTPException(status_code=503, detail="Data not ready")

    price_data = engine.get_price_data()
    if price_data is None:
        raise HTTPException(status_code=503, detail="Price data not available")

    # ── Filter to valid tickers (preserve caller's weight magnitudes) ─────────
    valid = [(h.ticker, h.weight) for h in body.holdings if h.ticker in price_data.columns]
    if not valid:
        raise HTTPException(status_code=400, detail="No valid tickers in price data")

    tickers  = [t for t, _ in valid]
    equity_w = np.array([w for _, w in valid], dtype=float)

    # invested / cash diagnostic — derived from actual weights used
    invested_weight = float(equity_w.sum())
    cash_weight     = max(0.0, 1.0 - invested_weight)

    # ── Pull price window: extra row needed for log-diff ─────────────────────
    prices = price_data[tickers].tail(body.lookback + 1)

    # ── SGOV / cash setup ─────────────────────────────────────────────────────
    bench = engine.get_benchmark_prices()
    sgov_series: Optional[pd.Series] = None
    cash_method = "zero"

    if bench is not None and "SGOV" in bench.columns:
        sgov_raw = bench["SGOV"].tail(body.lookback + 1)
        # Align equity price index with SGOV
        shared_idx = prices.index.intersection(sgov_raw.index)
        if len(shared_idx) > 1:
            prices      = prices.loc[shared_idx]
            sgov_series = sgov_raw.loc[shared_idx]
            cash_method = "sgov"
        else:
            _log.warning(
                "portfolio-history: SGOV present in benchmark but index overlap with equity "
                "prices is insufficient (%d shared dates) — using zero cash return",
                len(shared_idx),
            )
    else:
        _log.warning("portfolio-history: SGOV not in benchmark prices — using zero cash return")

    # ── Log returns: ln(P_t / P_t-1), drop seed row ──────────────────────────
    equity_log_df = np.log(prices / prices.shift(1)).iloc[1:]   # shape (T, N)

    # ── Align equity + SGOV log-returns; drop any date with a NaN ────────────
    if sgov_series is not None:
        sgov_log = np.log(sgov_series / sgov_series.shift(1)).iloc[1:]
        combined  = pd.concat([equity_log_df, sgov_log.rename("_sgov")], axis=1).dropna()
        eq_log    = combined[tickers].values                # (M, N)
        sv_log    = combined["_sgov"].values                # (M,)
    else:
        combined  = equity_log_df.dropna()
        eq_log    = combined[tickers].values                # (M, N)
        sv_log    = np.zeros(len(combined))                 # (M,)

    # ── Enforce ≥200 shared valid observations ────────────────────────────────
    n_obs = len(combined)
    if n_obs < 200:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Insufficient shared price history: {n_obs} valid observations after "
                f"alignment and log-return calculation — require ≥200."
            ),
        )

    # ── Portfolio daily log-return series ─────────────────────────────────────
    # r_p,t = Σ_i w_i * r_i,t  +  w_cash * r_sgov,t
    port_rets = (eq_log * equity_w).sum(axis=1) + sv_log * cash_weight

    # ── Cumulative NAV from log-returns: NAV_t = 100 × exp(Σ r_s, s≤t) ──────
    nav = 100.0 * np.exp(np.cumsum(port_rets))
    nav = np.insert(nav, 0, 100.0)   # prepend day-0 base at 100

    # ── Drawdown series ───────────────────────────────────────────────────────
    running_peak = np.maximum.accumulate(nav)
    drawdown_pct = (nav / running_peak - 1.0) * 100.0

    # ── Date labels: [base_date] + return_dates  (length = n_obs + 1) ────────
    base_date_str = prices.index[0].strftime("%Y-%m-%d")
    date_strs = [base_date_str] + [d.strftime("%Y-%m-%d") for d in combined.index]

    total_return = float(nav[-1] - 100.0)
    max_drawdown = float(drawdown_pct.min())
    ann_vol      = float(np.std(port_rets) * np.sqrt(252) * 100.0)

    _log.info(
        f"portfolio-history: {len(tickers)} tickers, {n_obs} obs, "
        f"invested={invested_weight:.3f}, cash={cash_weight:.3f} ({cash_method})"
    )

    return {
        "dates":          date_strs,
        "nav":            [round(float(v), 4) for v in nav],
        "drawdown":       [round(float(v), 4) for v in drawdown_pct],
        "totalReturn":    round(total_return, 2),
        "maxDrawdown":    round(max_drawdown, 2),
        "annualizedVol":  round(ann_vol, 2),
        "numDays":        n_obs,
        "investedWeight": round(invested_weight, 6),
        "cashWeight":     round(cash_weight, 6),
        "daysUsed":       n_obs,
        "cashMethod":     cash_method,
    }


@app.post("/portfolio-corr-seed")
def portfolio_corr_seed(body: CorrSeedRequest):
    """
    Greedy correlation-constrained basket seeding.
    Candidates (already sorted best-alpha-first) are accepted only if their
    maximum pairwise correlation to every already-accepted name is ≤ max_corr.
    """
    import logging
    logger = logging.getLogger("server")

    if not body.tickers:
        raise HTTPException(status_code=400, detail="No tickers provided")

    status = engine.get_status()
    if status["status"] != "ready":
        raise HTTPException(status_code=503, detail="Data not ready")

    price_data = engine.get_price_data()
    if price_data is None:
        raise HTTPException(status_code=503, detail="Price data not available")

    # Keep only tickers that exist in the price matrix (preserve alpha order)
    valid = [t for t in body.tickers if t in price_data.columns]
    if not valid:
        raise HTTPException(status_code=400, detail="No valid tickers in price data")

    # Build log-return matrix once (shared lookback window, drop incomplete rows)
    prices_sub = price_data[valid].tail(body.lookback)
    log_rets = np.log(prices_sub / prices_sub.shift(1)).dropna()

    # Filter to tickers that survived the dropna (have full history)
    valid = [t for t in valid if t in log_rets.columns]

    selected: list[str] = []
    rets_selected: list[np.ndarray] = []   # pre-extracted columns for speed

    for ticker in valid:
        if len(selected) >= body.n:
            break

        col = log_rets[ticker].values

        if not selected:
            selected.append(ticker)
            rets_selected.append(col)
            continue

        # Max absolute pairwise correlation vs every current basket member
        max_corr_val = 0.0
        for existing_col in rets_selected:
            # Pearson correlation via numpy (fast, no overhead)
            corr = float(np.corrcoef(col, existing_col)[0, 1])
            if abs(corr) > max_corr_val:
                max_corr_val = abs(corr)
            if max_corr_val > body.max_corr:
                break   # early exit — already failed threshold

        if max_corr_val <= body.max_corr:
            selected.append(ticker)
            rets_selected.append(col)

    logger.info(
        f"corr-seed: selected {len(selected)}/{body.n} from {len(valid)} candidates "
        f"(max_corr={body.max_corr}, lookback={body.lookback})"
    )

    return {
        "tickers":           selected,
        "count":             len(selected),
        "requested":         body.n,
        "maxCorr":           body.max_corr,
        "lookback":          body.lookback,
        "candidatesScanned": len(valid),
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("EQUITY_ENGINE_PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
