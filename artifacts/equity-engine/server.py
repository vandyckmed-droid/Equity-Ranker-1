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


class PortfolioHolding(BaseModel):
    ticker: str
    weight: float


class PortfolioRiskRequest(BaseModel):
    holdings: List[PortfolioHolding]
    lookback: int = 252
    weighting_method: str = "equal"


@app.post("/portfolio-risk")
def portfolio_risk(body: PortfolioRiskRequest):
    import engine as eng

    if not body.holdings:
        raise HTTPException(status_code=400, detail="No holdings provided")

    tickers = [h.ticker for h in body.holdings]
    raw_weights = [h.weight for h in body.holdings]

    status = eng.get_status()
    if status["status"] != "ready":
        raise HTTPException(status_code=503, detail="Data not ready")

    price_data = eng.get_price_data()
    meta = eng.get_meta_data()

    # Compute weights based on method
    if body.weighting_method == "equal":
        n = len(tickers)
        weights = [1.0 / n] * n
    elif body.weighting_method == "inverse_vol" and price_data is not None:
        vols = []
        for t in tickers:
            if t in price_data.columns:
                lr = price_data[t].pct_change().dropna()
                vol = lr.std() * np.sqrt(252)
                vols.append(max(vol, 0.05))
            else:
                vols.append(0.20)
        inv_vols = [1.0 / v for v in vols]
        total = sum(inv_vols)
        weights = [v / total for v in inv_vols]
    else:
        total = sum(raw_weights)
        weights = [w / total if total > 0 else 1.0 / len(raw_weights) for w in raw_weights]

    risk = eng.compute_portfolio_risk(tickers, weights, body.lookback)
    if "error" in risk:
        raise HTTPException(status_code=400, detail=risk["error"])

    # Get cluster info for tickers from latest rankings
    params = {"vol_adjust": True, "use_quality": True, "use_tstats": False,
              "w6": 0.4, "w12": 0.4, "w_quality": 0.2, "vol_floor": 0.05,
              "winsor_p": 2.0, "cluster_n": 100, "cluster_k": 10, "cluster_lookback": 252}
    ranked = eng.get_ranked_data(params)
    cluster_map = {}
    if ranked is not None and not ranked.empty:
        cluster_map = dict(zip(ranked["ticker"], ranked["cluster"]))

    holdings_out = []
    cluster_weights = {}
    for t, w in zip(tickers, weights):
        actual_w = risk["weights"].get(t, w)
        vol = risk["vols"].get(t, 0.20)
        cluster = cluster_map.get(t)
        holdings_out.append({
            "ticker": t,
            "weight": actual_w,
            "vol": vol,
            "cluster": cluster,
        })
        if cluster is not None:
            cluster_weights[cluster] = cluster_weights.get(cluster, 0) + actual_w

    cluster_dist = [{"cluster": int(k), "count": sum(1 for h in holdings_out if h["cluster"] == k),
                     "weight": float(v)} for k, v in cluster_weights.items()]
    cluster_dist.sort(key=lambda x: x["cluster"])

    actual_weights = [risk["weights"].get(t, w) for t, w in zip(tickers, weights)]

    return {
        "portfolioVol": risk["port_vol"],
        "avgCorrelation": risk["avg_corr"],
        "holdings": holdings_out,
        "clusterDistribution": cluster_dist,
        "largestWeight": max(actual_weights) if actual_weights else 0.0,
        "numHoldings": len(holdings_out),
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("EQUITY_ENGINE_PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
