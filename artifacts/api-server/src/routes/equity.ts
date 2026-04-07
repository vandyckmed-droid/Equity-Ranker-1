import { Router, type IRouter } from "express";
import { logger } from "../lib/logger";

const router: IRouter = Router();

const EQUITY_ENGINE_URL = `http://localhost:${process.env.EQUITY_ENGINE_PORT || "8001"}`;

async function proxyRequest(url: string, options?: RequestInit): Promise<[number, unknown]> {
  const response = await fetch(url, options);
  const data = await response.json();
  return [response.status, data];
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
      enrichment: d.enrichment ?? "pending",
      qualityCoverage: d.qualityCoverage ?? "",
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
      enrichment: "pending",
      qualityCoverage: "",
      timings: {},
    });
  }
});

router.get("/equity/rankings", async (req, res): Promise<void> => {
  try {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(req.query)) {
      if (v !== undefined) {
        // Map camelCase to snake_case query params
        const mappings: Record<string, string> = {
          volAdjust: "vol_adjust",
          useQuality: "use_quality",
          useTstats: "use_tstats",
          wQuality: "w_quality",
          volFloor: "vol_floor",
          winsorP: "winsor_p",
          clusterN: "cluster_n",
          clusterK: "cluster_k",
          clusterLookback: "cluster_lookback",
          secFilerOnly: "sec_filer_only",
          excludeSectors: "exclude_sectors",
          requireQuality: "require_quality",
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
      // Data still loading
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
    // Map camelCase body keys to snake_case
    const body = req.body as Record<string, unknown>;
    const mapped: Record<string, unknown> = {
      min_price: body.minPrice ?? 5.0,
      min_adv: body.minAdv ?? 1e7,
      min_market_cap: body.minMarketCap ?? 1e9,
      vol_adjust: body.volAdjust ?? true,
      use_quality: body.useQuality ?? true,
      use_tstats: body.useTstats ?? false,
      w6: body.w6 ?? 0.4,
      w12: body.w12 ?? 0.4,
      w_quality: body.wQuality ?? 0.2,
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
        method: d.method,
        fallback: d.fallback ?? null,
        volLookback: d.volLookback,
        covLookback: d.covLookback,
        avgCorrelation: d.avgCorrelation,
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

export default router;
