"""
FastAPI server for the equity data engine.
Runs on a separate port, proxied by Express.
"""

import os
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import numpy as np

import engine

app = FastAPI(title="Equity Engine")

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
    return engine.get_status()


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
):
    status = engine.get_status()
    if status["status"] == "loading":
        return {"status": "loading", "message": status["message"],
                "progress": status["progress"], "total": status["total"],
                "loaded": status["loaded"]}

    if status["status"] == "error":
        raise HTTPException(status_code=500, detail=status["message"])

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
    }

    df = engine.get_ranked_data(params)
    if df is None or df.empty:
        return {"stocks": [], "total": 0, "cluster_count": 0, "cached_at": None}

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
    df = engine.get_ranked_data(params)
    if df is None or df.empty:
        return {"stocks": [], "total": 0, "cluster_count": 0, "cached_at": None}

    filtered = engine.apply_universe_filters(
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


def _compute_min_var_weights(cov_matrix: np.ndarray) -> Optional[np.ndarray]:
    """Long-only minimum-variance weights using SLSQP. Returns None on failure."""
    from scipy.optimize import minimize
    n = cov_matrix.shape[0]
    x0 = np.ones(n) / n
    constraints = [{"type": "eq", "fun": lambda w: w.sum() - 1.0}]
    bounds = [(0.0, 1.0)] * n
    result = minimize(
        lambda w: float(w @ cov_matrix @ w),
        x0=x0,
        method="SLSQP",
        constraints=constraints,
        bounds=bounds,
        options={"maxiter": 500, "ftol": 1e-10},
    )
    if result.success and np.all(np.isfinite(result.x)):
        w = np.maximum(result.x, 0.0)
        s = w.sum()
        if s > 1e-8:
            return w / s
    return None


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

    # Build returns and covariance matrix
    prices_sub = price_data[tickers_v].tail(body.lookback)
    log_rets = np.log(prices_sub / prices_sub.shift(1)).dropna()
    cov_ann = log_rets.cov().values * 252  # annualized covariance matrix
    vols_ann = np.sqrt(np.diag(cov_ann))   # per-stock annualized vol
    vols_ann = np.maximum(vols_ann, 0.05)  # vol floor

    n = len(tickers_v)
    fallback = None
    actual_method = body.weighting_method

    # ── Step 1: Base weights (normalized, sum = 1) ───────────────────────────
    if body.weighting_method == "equal":
        base_w = np.ones(n) / n

    elif body.weighting_method == "inverse_vol":
        inv_vols = 1.0 / vols_ann
        base_w = inv_vols / inv_vols.sum()

    elif body.weighting_method == "min_var":
        min_var_w = _compute_min_var_weights(cov_ann)
        if min_var_w is not None:
            base_w = min_var_w
        else:
            # Explicit fallback to inverse vol
            logger.warning("Min Var optimization failed — falling back to Inverse Vol")
            inv_vols = 1.0 / vols_ann
            base_w = inv_vols / inv_vols.sum()
            actual_method = "inverse_vol"
            fallback = "Min Var optimization failed (singular matrix). Used Inverse Vol."
    else:
        # Unknown method — default to equal
        base_w = np.ones(n) / n
        actual_method = "equal"
        fallback = f"Unknown method '{body.weighting_method}'. Used Equal weights."

    # ── Step 2: Pre-scale portfolio vol (w_base' Σ w_base) ───────────────────
    pre_var = float(base_w @ cov_ann @ base_w)
    pre_vol = float(np.sqrt(max(pre_var, 1e-12)))

    # ── Step 3: Vol-target overlay multiplier ────────────────────────────────
    multiplier = VOL_TARGET / pre_vol
    final_w = base_w * multiplier
    gross_exposure = float(final_w.sum())

    # ── Portfolio-level stats ─────────────────────────────────────────────────
    final_port_vol = pre_vol * multiplier  # = VOL_TARGET by construction

    # Average pairwise correlation
    if n > 1:
        corr = log_rets.corr().values
        mask = ~np.eye(n, dtype=bool)
        avg_corr = float(corr[mask].mean())
    else:
        avg_corr = 1.0

    # ── Cluster info ──────────────────────────────────────────────────────────
    params = {"vol_adjust": True, "use_quality": True, "use_tstats": False,
              "w6": 0.4, "w12": 0.4, "w_quality": 0.2, "vol_floor": 0.05,
              "winsor_p": 2.0, "cluster_n": 100, "cluster_k": 10, "cluster_lookback": 252}
    ranked = eng.get_ranked_data(params)
    cluster_map = {}
    if ranked is not None and not ranked.empty:
        cluster_map = dict(zip(ranked["ticker"], ranked["cluster"]))

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
