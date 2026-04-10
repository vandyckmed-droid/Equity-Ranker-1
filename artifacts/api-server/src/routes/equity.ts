import { Router, type IRouter } from "express";

const router: IRouter = Router();

const EQUITY_ENGINE_URL = `http://localhost:${process.env.EQUITY_ENGINE_PORT || "8001"}`;

async function proxyRequest(
  url: string,
  options?: RequestInit,
  retries = 3,
  delayMs = 800,
): Promise<[number, unknown]> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, options);
      const data = await response.json();
      return [response.status, data];
    } catch (err: unknown) {
      const isConnRefused =
        err instanceof Error &&
        (err.message.includes("ECONNREFUSED") || err.message.includes("fetch failed"));
      if (isConnRefused && attempt < retries) {
        await new Promise((r) => setTimeout(r, delayMs * (attempt + 1)));
        continue;
      }
      if (isConnRefused) {
        return [503, { error: "engine_unavailable", message: "Data engine is starting up — please try again in a moment." }];
      }
      throw err;
    }
  }
  return [503, { error: "engine_unavailable", message: "Data engine is starting up — please try again in a moment." }];
}

router.get("/equity/status", async (req, res): Promise<void> => {
  try {
    const [status, data] = await proxyRequest(`${EQUITY_ENGINE_URL}/status`);
    const d = data as Record<string, unknown>;
    res.status(status).json({
      status: d.status || "loading",
      message: d.message || "",
      progress: d.progress ?? null,
      total: d.total ?? null,
      loaded: d.loaded ?? null,
      cachedAt: d.cached_at ?? d.cachedAt ?? null,
      timings: d.timings ?? {},
    });
  } catch (err) {
    req.log.error({ err }, "Error fetching equity status");
    res.status(503).json({
      status: "loading",
      message: "Data engine starting up...",
      progress: null,
      total: null,
      loaded: null,
      cachedAt: null,
      timings: {},
    });
  }
});

router.get("/equity/rankings", async (req, res): Promise<void> => {
  try {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(req.query)) {
      if (v !== undefined) {
        const mappings: Record<string, string> = {
          volAdjust: "vol_adjust",
          useTstats: "use_tstats",
          volFloor: "vol_floor",
          winsorP: "winsor_p",
          clusterN: "cluster_n",
          clusterK: "cluster_k",
          clusterLookback: "cluster_lookback",
          excludeSectors: "exclude_sectors",
        };
        const key = mappings[k] || k;
        params.set(key, String(v));
      }
    }
    const [status, data] = await proxyRequest(`${EQUITY_ENGINE_URL}/rankings?${params}`);
    const d = data as Record<string, unknown>;

    if (status === 200) {
      res.status(200).json({
        stocks: d.stocks || [],
        total: d.total || 0,
        clusterCount: d.cluster_count || 0,
        cachedAt: d.cached_at || null,
        audit: d.audit || null,
      });
    } else {
      res.status(202).json({
        status: "loading",
        message: (d as Record<string, unknown>).message || "Loading...",
        progress: (d as Record<string, unknown>).progress ?? null,
        total: (d as Record<string, unknown>).total ?? null,
        loaded: (d as Record<string, unknown>).loaded ?? null,
        cachedAt: null,
      });
    }
  } catch (err) {
    req.log.error({ err }, "Error fetching rankings");
    res.status(202).json({
      status: "loading",
      message: "Data engine starting up...",
      progress: null,
      total: null,
      loaded: null,
      cachedAt: null,
    });
  }
});

router.post("/equity/universe-filters", async (req, res): Promise<void> => {
  try {
    const body = req.body as Record<string, unknown>;
    const mapped: Record<string, unknown> = {
      min_price: body.minPrice ?? 5.0,
      min_adv: body.minAdv ?? 1e7,
      min_market_cap: body.minMarketCap ?? 1e9,
      vol_adjust: body.volAdjust ?? true,
      use_tstats: body.useTstats ?? false,
      w6: body.w6 ?? 0.5,
      w12: body.w12 ?? 0.5,
      vol_floor: body.volFloor ?? 0.05,
      winsor_p: body.winsorP ?? 2.0,
      cluster_n: body.clusterN ?? 100,
      cluster_k: body.clusterK ?? 10,
      cluster_lookback: body.clusterLookback ?? 252,
    };

    const [status, data] = await proxyRequest(`${EQUITY_ENGINE_URL}/universe-filters`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(mapped),
    });

    const d = data as Record<string, unknown>;
    res.status(status).json({
      stocks: d.stocks || [],
      total: d.total || 0,
      clusterCount: d.cluster_count || 0,
      cachedAt: d.cached_at || null,
    });
  } catch (err) {
    req.log.error({ err }, "Error applying universe filters");
    res.status(500).json({ error: "Failed to apply filters" });
  }
});

router.post("/portfolio/risk", async (req, res): Promise<void> => {
  try {
    const body = req.body as Record<string, unknown>;
    const mapped = {
      holdings: body.holdings,
      lookback: body.lookback ?? 252,
      weighting_method: body.weightingMethod ?? "equal",
      cluster_n:        body.clusterN        ?? 100,
      cluster_k:        body.clusterK        ?? 10,
      cluster_lookback: body.clusterLookback ?? 252,
    };

    const [status, data] = await proxyRequest(`${EQUITY_ENGINE_URL}/portfolio-risk`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(mapped),
    });

    const d = data as Record<string, unknown>;
    if (status === 200) {
      res.status(200).json({
        portfolioVol: d.portfolioVol,
        basePortVol: d.basePortVol,
        volTargetMultiplier: d.volTargetMultiplier,
        grossExposure: d.grossExposure,
        riskySleeve: d.riskySleeve ?? d.grossExposure,
        sgovWeight: d.sgovWeight ?? 0,
        diversificationRatio: d.diversificationRatio ?? 1,
        effectiveN: d.effectiveN ?? 1,
        method: d.method,
        fallback: d.fallback ?? null,
        covModel: d.covModel ?? null,
        volLookback: d.volLookback,
        covLookback: d.covLookback,
        avgCorrelation: d.avgCorrelation,
        namesCapped: Array.isArray(d.namesCapped) ? d.namesCapped : [],
        holdings: d.holdings,
        clusterDistribution: d.clusterDistribution,
        largestWeight: d.largestWeight,
        numHoldings: d.numHoldings,
      });
    } else {
      res.status(status).json({ error: (d as Record<string, unknown>).detail || "Error computing risk" });
    }
  } catch (err) {
    req.log.error({ err }, "Error computing portfolio risk");
    res.status(500).json({ error: "Failed to compute portfolio risk" });
  }
});

router.post("/portfolio/history", async (req, res): Promise<void> => {
  try {
    const body = req.body as Record<string, unknown>;
    const mapped = {
      holdings:    body.holdings,
      lookback:    body.lookback    ?? 252,
      sgov_weight: body.sgovWeight  ?? body.sgov_weight ?? 0,
    };
    const [status, data] = await proxyRequest(`${EQUITY_ENGINE_URL}/portfolio-history`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(mapped),
    });
    res.status(status).json(data);
  } catch (err) {
    req.log.error({ err }, "Error computing portfolio history");
    res.status(500).json({ error: "Failed to compute portfolio history" });
  }
});

router.post("/portfolio/corr-seed", async (req, res): Promise<void> => {
  try {
    const body = req.body as Record<string, unknown>;
    const mapped = {
      tickers: body.tickers,
      n: body.n ?? 20,
      max_corr: body.maxCorr ?? body.max_corr ?? 0.7,
      lookback: body.lookback ?? 252,
    };
    const [status, data] = await proxyRequest(`${EQUITY_ENGINE_URL}/portfolio-corr-seed`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(mapped),
    });
    res.status(status).json(data);
  } catch (err) {
    req.log.error({ err }, "Error computing correlation seed");
    res.status(500).json({ error: "Failed to compute correlation seed" });
  }
});

export default router;
