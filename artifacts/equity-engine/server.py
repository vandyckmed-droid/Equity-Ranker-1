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

import engine

app = FastAPI(title="Equity Engine")

app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        "enrichment": status.get("enrichment", "pending"),
        "qualityCoverage": status.get("quality_coverage", ""),
        "timings": status.get("timings", {}),
    }


@app.get("/rankings")
def get_rankings(
    vol_adjust: bool = Query(True),
    use_quality: bool = Query(True),
    use_tstats: bool = Query(False),
    w6: float = Query(0.4),
    w12: float = Query(0.4),
    w_quality: float = Query(0.2),
    vol_floor: float = Query(0.05),
    winsor_p: float = Query(2.0),
    cluster_n: int = Query(100),
    cluster_k: int = Query(10),
    cluster_lookback: int = Query(252),
    sec_filer_only: bool = Query(False),
    exclude_sectors: Optional[str] = Query(None),
    require_quality: bool = Query(False),
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
        "use_quality": use_quality,
        "use_tstats": use_tstats,
        "w6": w6,
        "w12": w12,
        "w_quality": w_quality,
        "vol_floor": vol_floor,
        "winsor_p": winsor_p,
        "cluster_n": cluster_n,
        "cluster_k": cluster_k,
        "cluster_lookback": cluster_lookback,
        "sec_filer_only": sec_filer_only,
        "exclude_sectors": sectors_list,
        "require_quality": require_quality,
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

    stocks = []
    for _, row in df.iterrows():
        stocks.append({
            "ticker": row["ticker"],
            "name": row.get("name") or row["ticker"],
            "sector": safe(row.get("sector")),
            "industry": safe(row.get("industry")),
            "price": safe(row.get("price")),
            "marketCap": safe(row.get("market_cap")),
            "adv": safe(row.get("adv")),
            "r1": safe(row.get("r1")),
            "m6": safe(row.get("m6")),
            "m12": safe(row.get("m12")),
            "sigma6": safe(row.get("sigma6")),
            "sigma12": safe(row.get("sigma12")),
            "s6": safe(row.get("s6")),
            "s12": safe(row.get("s12")),
            "tstat6": safe(row.get("tstat6")),
            "tstat12": safe(row.get("tstat12")),
            "quality": safe(row.get("quality")),
            "zM6": safe(row.get("zM6")),
            "zM12": safe(row.get("zM12")),
            "zQuality": safe(row.get("zQuality")),
            "zS6": safe(row.get("zS6")),
            "zS12": safe(row.get("zS12")),
            "zT6": safe(row.get("zT6")),
            "zT12": safe(row.get("zT12")),
            "zQ": safe(row.get("zQ")),
            "sSleeve": safe(row.get("sSleeve")),
            "tSleeve": safe(row.get("tSleeve")),
            "qSleeve": safe(row.get("qSleeve")),
            "alpha": safe(row.get("alpha")),
            "rank": safe(row.get("rank")),
            "percentile": safe(row.get("percentile")),
            "cluster": safe(row.get("cluster")),
            # Quality component raw values
            "roe":          safe(row.get("roe")),
            "roa":          safe(row.get("roa")),
            "grossMargin":  safe(row.get("gross_margin")),
            "opMargin":     safe(row.get("op_margin")),
            "deRatio":      safe(row.get("de_ratio")),
            # Quality component z-scores
            "zRoe":         safe(row.get("z_roe")),
            "zRoa":         safe(row.get("z_roa")),
            "zGross":       safe(row.get("z_gross")),
            "zOp":          safe(row.get("z_op")),
            "zInvLev":      safe(row.get("z_inv_lev")),
            # Quality audit fields
            "qualityMissing":          safe_bool(row.get("quality_missing")),
            "alphaFormula":            safe_str(row.get("alpha_formula"), "S+T+Q"),
            "hasProfitabilityBucket":  safe_bool(row.get("has_profitability_bucket")),
            "hasMarginBucket":         safe_bool(row.get("has_margin_bucket")),
            "hasLeverageBucket":       safe_bool(row.get("has_leverage_bucket")),
            "qualityBucketCount":      safe(row.get("quality_bucket_count")),
            "qualityMissingReason":    safe_str(row.get("quality_missing_reason")),
        })

    cluster_vals = [s["cluster"] for s in stocks if s["cluster"] is not None]
    cluster_count = len(set(cluster_vals))

    return {
        "stocks": stocks,
        "total": len(stocks),
        "cluster_count": cluster_count,
        "cached_at": status.get("cached_at"),
        "audit": audit,
    }


class UniverseFiltersBody(BaseModel):
    min_price: float = 5.0
    min_adv: float = 1e7
    min_market_cap: float = 1e9
    vol_adjust: bool = True
    use_quality: bool = True
    use_tstats: bool = False
    w6: float = 0.4
    w12: float = 0.4
    w_quality: float = 0.2
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
            "ticker": row["ticker"],
            "name": row.get("name") or row["ticker"],
            "sector": safe(row.get("sector")),
            "industry": safe(row.get("industry")),
            "price": safe(row.get("price")),
            "marketCap": safe(row.get("market_cap")),
            "adv": safe(row.get("adv")),
            "r1": safe(row.get("r1")),
            "m6": safe(row.get("m6")),
            "m12": safe(row.get("m12")),
            "sigma6": safe(row.get("sigma6")),
            "sigma12": safe(row.get("sigma12")),
            "s6": safe(row.get("s6")),
            "s12": safe(row.get("s12")),
            "tstat6": safe(row.get("tstat6")),
            "tstat12": safe(row.get("tstat12")),
            "quality": safe(row.get("quality")),
            "zM6": safe(row.get("zM6")),
            "zM12": safe(row.get("zM12")),
            "zQuality": safe(row.get("zQuality")),
            "zS6": safe(row.get("zS6")),
            "zS12": safe(row.get("zS12")),
            "zT6": safe(row.get("zT6")),
            "zT12": safe(row.get("zT12")),
            "zQ": safe(row.get("zQ")),
            "sSleeve": safe(row.get("sSleeve")),
            "tSleeve": safe(row.get("tSleeve")),
            "qSleeve": safe(row.get("qSleeve")),
            "alpha": safe(row.get("alpha")),
            "rank": safe(row.get("rank")),
            "percentile": safe(row.get("percentile")),
            "cluster": safe(row.get("cluster")),
            # Quality component raw values
            "roe":          safe(row.get("roe")),
            "roa":          safe(row.get("roa")),
            "grossMargin":  safe(row.get("gross_margin")),
            "opMargin":     safe(row.get("op_margin")),
            "deRatio":      safe(row.get("de_ratio")),
            # Quality component z-scores
            "zRoe":         safe(row.get("z_roe")),
            "zRoa":         safe(row.get("z_roa")),
            "zGross":       safe(row.get("z_gross")),
            "zOp":          safe(row.get("z_op")),
            "zInvLev":      safe(row.get("z_inv_lev")),
            # Quality audit fields
            "qualityMissing":          safe_bool(row.get("quality_missing")),
            "alphaFormula":            safe_str(row.get("alpha_formula"), "S+T+Q"),
            "hasProfitabilityBucket":  safe_bool(row.get("has_profitability_bucket")),
            "hasMarginBucket":         safe_bool(row.get("has_margin_bucket")),
            "hasLeverageBucket":       safe_bool(row.get("has_leverage_bucket")),
            "qualityBucketCount":      safe(row.get("quality_bucket_count")),
            "qualityMissingReason":    safe_str(row.get("quality_missing_reason")),
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
    Equal Risk Contribution via the Spinu convex formulation:
        min   (1/2) x' Σ x  −  b · log(x)    s.t.  x > 0
    Then normalise: w = x / sum(x).

    The first-order condition is Σx = b/x  ⟺  x_i (Σx)_i = b_i, which is
    exactly the ERC condition.  This is a strictly convex problem so the
    solution is unique and L-BFGS-B converges reliably.

    A per-name cap (max_w) is enforced once on the converged solution.
    """
    from scipy.optimize import minimize

    n    = cov_ann.shape[0]
    b    = np.ones(n) / n
    vols = np.maximum(np.sqrt(np.diag(cov_ann)), vol_floor)
    x0   = 1.0 / vols
    x0  /= x0.sum()

    def objective(x: np.ndarray) -> float:
        return 0.5 * float(x @ cov_ann @ x) - float(b @ np.log(np.maximum(x, 1e-16)))

    def gradient(x: np.ndarray) -> np.ndarray:
        return cov_ann @ x - b / np.maximum(x, 1e-16)

    try:
        res = minimize(
            objective, x0=x0, jac=gradient, method="L-BFGS-B",
            bounds=[(1e-8, None)] * n,
            options={"maxiter": 2000, "ftol": 1e-14, "gtol": 1e-9},
        )
        x = res.x if (res.success and np.all(np.isfinite(res.x)) and np.all(res.x > 0)) else x0
    except Exception:
        x = x0

    w = x / x.sum()
    # Iterative projection: clip-normalise until all base weights ≤ max_w
    for _ in range(50):
        prev = w.copy()
        w = np.clip(w, 0.0, max_w)
        w /= w.sum()
        if np.max(np.abs(w - prev)) < 1e-9:
            break
    return w


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
    _rp = {"vol_adjust": True, "use_quality": True, "use_tstats": False,
           "w6": 0.4, "w12": 0.4, "w_quality": 0.2, "vol_floor": 0.05,
           "winsor_p": 2.0, "cluster_n": 100, "cluster_k": 10, "cluster_lookback": 252}
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
            base_w = _compute_risk_parity_weights(cov_ewma)
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

    # ── Step 2: Pre-scale portfolio vol (w' Σ w) ─────────────────────────────
    pre_var = float(base_w @ cov_ann @ base_w)
    pre_vol = float(np.sqrt(max(pre_var, 1e-12)))

    # ── Step 3: Vol-target overlay multiplier ────────────────────────────────
    multiplier     = VOL_TARGET / pre_vol
    final_w        = base_w * multiplier
    gross_exposure = float(final_w.sum())

    # ── Portfolio-level stats ─────────────────────────────────────────────────
    final_port_vol = pre_vol * multiplier   # = VOL_TARGET by construction

    if n > 1:
        corr     = log_rets.corr().values
        mask     = ~np.eye(n, dtype=bool)
        avg_corr = float(corr[mask].mean())
    else:
        avg_corr = 1.0

    holdings_out = []
    cluster_weights: dict[int, float] = {}
    for i, t in enumerate(tickers_v):
        fw = float(final_w[i])
        vol = float(vols_ann[i])
        raw_cluster = cluster_map.get(t)
        # Safely convert cluster — pandas may return NaN floats for missing values
        try:
            cluster = int(raw_cluster) if raw_cluster is not None and not (isinstance(raw_cluster, float) and np.isnan(raw_cluster)) else None
        except (ValueError, TypeError):
            cluster = None
        holdings_out.append({"ticker": t, "weight": fw, "vol": vol, "cluster": cluster})
        if cluster is not None:
            cluster_weights[cluster] = cluster_weights.get(cluster, 0.0) + fw

    cluster_dist = [
        {"cluster": k, "count": sum(1 for h in holdings_out if h["cluster"] == k),
         "weight": float(v)}
        for k, v in sorted(cluster_weights.items())
    ]

    largest_weight = float(max(final_w)) if n > 0 else 0.0

    return {
        "portfolioVol": round(final_port_vol, 6),
        "basePortVol": round(pre_vol, 6),
        "volTargetMultiplier": round(multiplier, 6),
        "grossExposure": round(gross_exposure, 6),
        "method": actual_method,
        "covModel": cov_model,
        "fallback": fallback,
        "volLookback": body.lookback,
        "covLookback": body.lookback,
        "avgCorrelation": round(avg_corr, 6),
        "holdings": holdings_out,
        "clusterDistribution": cluster_dist,
        "largestWeight": round(largest_weight, 6),
        "numHoldings": len(holdings_out),
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("EQUITY_ENGINE_PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
