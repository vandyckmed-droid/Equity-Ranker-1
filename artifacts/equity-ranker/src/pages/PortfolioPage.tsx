import { useState, useMemo, useEffect, useRef, useCallback } from "react";
import { Link } from "wouter";
import {
  useComputePortfolioRisk,
  useComputeCorrSeed,
  useComputePortfolioHistory,
  useComputePortfolioReversal,
  useGetRankings,
  PortfolioRiskRequestWeightingMethod,
  type ReversalItem,
  type Stock,
} from "@workspace/api-client-react";
import PortfolioHistoryCard from "@/components/PortfolioHistoryCard";
import { usePortfolio } from "@/hooks/use-portfolio";
import { formatNumber, formatPercent, cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Trash2, Calculator, Loader2, Info, AlertTriangle, Plus, Sparkles } from "lucide-react";

type SeedMode = "alpha" | "group" | "corr";
type SuggestMode = "group" | "sector" | "both";

interface DiversifySuggestion {
  ticker: string;
  name: string;
  alpha: number;
  sector: string;
  group: number;
  groupShare: number;
  sectorShare: number;
  deficit: number;
  via: "group" | "sector" | "both";
}

const PORTFOLIO_PREFS_KEY = "qt:portfolio-prefs-v1";
const loadPortfolioPrefs = (): Record<string, unknown> | null => {
  try {
    const raw = localStorage.getItem(PORTFOLIO_PREFS_KEY);
    if (raw) return JSON.parse(raw) as Record<string, unknown>;
  } catch {}
  return null;
};

const METHODS: { value: string; label: string; desc: string }[] = [
  { value: "equal",       label: "Equal Weight",  desc: "Same weight to every holding — simple and transparent" },
  { value: "inverse_vol", label: "Inverse Vol",   desc: "Smaller weight to more volatile names — diagonal risk control" },
  { value: "signal_vol",  label: "Signal / Vol",  desc: "Stronger alpha signal + lower vol → more weight" },
  { value: "risk_parity", label: "Risk Parity",   desc: "Each holding contributes equal portfolio risk (capped ERC via SLSQP)" },
];

const METHOD_LABELS: Record<string, string> = Object.fromEntries(
  METHODS.map((m) => [m.value, m.label])
);

const VOL_TARGET = 0.15;
const LOOKBACK = 126;

// ── Plain-English portfolio narrative ────────────────────────────────────────
function buildNarrative(
  riskData: NonNullable<ReturnType<typeof useComputePortfolioRisk>["data"]>,
  methodLabel: string,
  numSectors: number,
  numGroups: number,
): string {
  const n = riskData.numHoldings;
  const scale = riskData.volTargetMultiplier;
  const cash = riskData.sgovWeight;
  const avgCorr = riskData.avgCorrelation;
  const effN = riskData.effectiveN ?? riskData.numHoldings;
  const dr = riskData.diversificationRatio ?? 1;

  const sorted = [...riskData.holdings].sort((a, b) => b.riskContrib - a.riskContrib);
  const topContrib = sorted[0];

  const parts: string[] = [];

  // Core construction
  const cashLine = cash > 0.005
    ? ` scaled ×${formatNumber(scale, 2)} → ${formatPercent(cash, 1)} in SGOV`
    : " fully invested";
  parts.push(
    `${n}-name ${methodLabel} portfolio targeting 15% vol.` +
    ` Base vol ${formatPercent(riskData.basePortVol, 1)},${cashLine}.`
  );

  // Diversification quality
  const corrQual = avgCorr < 0.25 ? "low" : avgCorr < 0.45 ? "moderate" : "elevated";
  parts.push(
    `Avg pairwise correlation is ${corrQual} (${formatNumber(avgCorr, 2)}),` +
    ` with ${formatNumber(effN, 1)} effective positions` +
    (dr > 1.05 ? ` and ${formatNumber(dr, 2)}× diversification benefit.` : ".")
  );

  // Concentration
  if (topContrib) {
    const dominated = topContrib.riskContrib > 0.20;
    parts.push(
      `${topContrib.ticker} is the ${dominated ? "dominant" : "largest"} risk driver` +
      ` at ${formatPercent(topContrib.riskContrib, 1)} of total variance` +
      ` (${formatPercent(topContrib.baseWeight, 1)} weight).`
    );
  }

  // Breadth
  if (numSectors > 0 || numGroups > 0) {
    const breadth: string[] = [];
    if (numSectors > 0) breadth.push(`${numSectors} sector${numSectors > 1 ? "s" : ""}`);
    if (numGroups > 0) breadth.push(`${numGroups} momentum group${numGroups > 1 ? "s" : ""}`);
    parts.push(`Coverage spans ${breadth.join(" and ")}.`);
  }

  return parts.join(" ");
}

export default function PortfolioPage() {
  const { basket, addToBasket, removeFromBasket, clearBasket, seedBasket, allStocks, rankedStocks, setRankedStocks } = usePortfolio();

  // Read the same cluster + factor settings persisted by MainPage — no hook needed
  const mainControls = (() => {
    try {
      const raw = localStorage.getItem("qt:controls-v3");
      if (raw) {
        const p = JSON.parse(raw);
        return {
          w6:             typeof p.localW6          === "number" ? p.localW6          : 0.5,
          w12:            typeof p.localW12         === "number" ? p.localW12         : 0.5,
          volFloor:       typeof p.volFloor         === "number" ? p.volFloor         : 0.05,
          winsorP:        typeof p.winsorP          === "number" ? p.winsorP          : 2,
          clusterN:       typeof p.clusterN         === "number" ? p.clusterN         : 100,
          clusterK:       typeof p.clusterK         === "number" ? p.clusterK         : 10,
          clusterLookback:typeof p.clusterLookback  === "number" ? p.clusterLookback  : 252,
        };
      }
    } catch {}
    return { w6: 0.5, w12: 0.5, volFloor: 0.05, winsorP: 2, clusterN: 100, clusterK: 10, clusterLookback: 252 };
  })();

  const { data: autoFetchData } = useGetRankings(
    { volAdjust: true, useTstats: false, ...mainControls },
    { query: { enabled: basket.length > 0 && rankedStocks.length === 0, staleTime: 5 * 60 * 1000 } },
  );
  useEffect(() => {
    if (autoFetchData?.stocks && rankedStocks.length === 0) {
      const sorted = [...autoFetchData.stocks].sort((a, b) => (b.alpha ?? 0) - (a.alpha ?? 0));
      setRankedStocks(sorted);
    }
  }, [autoFetchData, rankedStocks.length, setRankedStocks]);

  const [weightingMethod, setWeightingMethod] = useState<PortfolioRiskRequestWeightingMethod>(() => {
    const s = loadPortfolioPrefs();
    const v = s?.weightingMethod as string | undefined;
    return (METHODS.map(m => m.value).includes(v ?? "")) ? v as PortfolioRiskRequestWeightingMethod : "equal";
  });
  const [seedCount, setSeedCount] = useState(() => {
    const s = loadPortfolioPrefs();
    return typeof s?.seedCount === "string" ? s.seedCount as string : "20";
  });
  const [seedMode, setSeedMode] = useState<SeedMode>(() => {
    const s = loadPortfolioPrefs();
    const v = s?.seedMode;
    return (v === "alpha" || v === "group" || v === "corr") ? v : "alpha";
  });
  const [maxCorr, setMaxCorr] = useState(() => {
    const s = loadPortfolioPrefs();
    return typeof s?.maxCorr === "string" ? s.maxCorr as string : "0.70";
  });
  const [suggestMode, setSuggestMode] = useState<SuggestMode>(() => {
    const s = loadPortfolioPrefs();
    const v = s?.suggestMode;
    return (v === "group" || v === "sector" || v === "both") ? v : "both";
  });

  useEffect(() => {
    try {
      localStorage.setItem(PORTFOLIO_PREFS_KEY, JSON.stringify({ weightingMethod, seedCount, seedMode, maxCorr, suggestMode }));
    } catch {}
  }, [weightingMethod, seedCount, seedMode, maxCorr, suggestMode]);

  const computeRisk = useComputePortfolioRisk();
  const corrSeed = useComputeCorrSeed();
  const computeHistory = useComputePortfolioHistory();
  const computeReversal = useComputePortfolioReversal();
  const histKeyRef = useRef("");
  const reversalKeyRef = useRef("");
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const triggerCompute = useCallback(() => {
    if (basket.length === 0) return;
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      computeRisk.mutate({
        data: {
          holdings: basket.map((ticker) => ({ ticker, weight: 1 })),
          lookback: LOOKBACK,
          weightingMethod,
          clusterN:        mainControls.clusterN,
          clusterK:        mainControls.clusterK,
          clusterLookback: mainControls.clusterLookback,
        },
      });
    }, 300);
  }, [basket, weightingMethod, computeRisk]);

  useEffect(() => {
    triggerCompute();
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [basket, weightingMethod]);

  const handleSeed = () => {
    const n = parseInt(seedCount);
    if (isNaN(n) || n <= 0) return;
    const universe: Stock[] = rankedStocks.length > 0 ? rankedStocks : allStocks;

    if (seedMode === "alpha") {
      seedBasket(universe.slice(0, n).map((s) => s.ticker));
      setWeightingMethod("equal");
    } else if (seedMode === "group") {
      const byGroup = new Map<number, Stock[]>();
      for (const s of universe) {
        const key = s.cluster ?? -1;
        if (!byGroup.has(key)) byGroup.set(key, []);
        byGroup.get(key)!.push(s);
      }
      const groups = [...byGroup.entries()]
        .sort(([a], [b]) => (a === -1 ? 1 : b === -1 ? -1 : a - b))
        .map(([, stocks]) => stocks);
      const selected: string[] = [];
      let round = 0;
      outer: while (selected.length < n) {
        let anyAdded = false;
        for (const group of groups) {
          if (round < group.length) {
            selected.push(group[round].ticker);
            anyAdded = true;
            if (selected.length >= n) break outer;
          }
        }
        if (!anyAdded) break;
        round++;
      }
      seedBasket(selected);
      setWeightingMethod("equal");
    } else {
      const parsedCorr = parseFloat(maxCorr);
      const threshold = isNaN(parsedCorr) ? 0.7 : Math.min(0.99, Math.max(0.10, parsedCorr));
      corrSeed.mutate(
        { data: { tickers: universe.map((s) => s.ticker), n, maxCorr: threshold, lookback: LOOKBACK } },
        { onSuccess: (data) => { seedBasket(data.tickers); setWeightingMethod("equal"); } }
      );
    }
  };

  const riskData = computeRisk.data;
  const isComputing = computeRisk.isPending;
  const hasError = computeRisk.isError;
  const isEngineDown = hasError && (computeRisk.error as { status?: number })?.status === 503;

  // Build reversal lookup map keyed by ticker
  // Only show data when it corresponds to the current basket (gate by basket size + key match)
  const reversalMap = useMemo((): Record<string, ReversalItem> => {
    if (basket.length < 2) return {};
    const items = computeReversal.data?.items;
    if (!items || items.length === 0) return {};
    // Only use data if all returned tickers are in the current basket
    const basketSet = new Set(basket);
    const allMatch = items.every((item) => basketSet.has(item.ticker));
    if (!allMatch) return {};
    return Object.fromEntries(items.map((item) => [item.ticker, item]));
  }, [computeReversal.data, basket]);

  // Max base weight across holdings
  const maxPosition = useMemo(() => {
    if (!riskData || riskData.holdings.length === 0) return 0;
    return Math.max(...riskData.holdings.map((h) => h.baseWeight));
  }, [riskData]);

  // Average alpha of basket holdings (simple mean)
  const avgAlpha = useMemo(() => {
    if (basket.length === 0 || rankedStocks.length === 0) return null;
    const alphaMap = new Map(rankedStocks.map((s) => [s.ticker, s.alpha ?? null]));
    const vals = basket.map((t) => alphaMap.get(t) ?? null).filter((v): v is number => v != null);
    if (vals.length === 0) return null;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  }, [basket, rankedStocks]);

  // Weight map for basket table
  const weightMap = useMemo(() => {
    if (!riskData) return {} as Record<string, number>;
    return Object.fromEntries(riskData.holdings.map((h) => [h.ticker, h.baseWeight]));
  }, [riskData]);

  // Sector breakdown
  const sectorStats = useMemo(() => {
    if (!riskData || riskData.holdings.length === 0)
      return { valid: false, numSectors: 0, breakdown: [] as { sector: string; weight: number; count: number }[] };
    const sectorMap = Object.fromEntries(allStocks.map((s) => [s.ticker, s.sector ?? ""]));
    const agg: Record<string, { weight: number; count: number }> = {};
    for (const h of riskData.holdings) {
      const sector = sectorMap[h.ticker];
      if (!sector) continue;
      if (!agg[sector]) agg[sector] = { weight: 0, count: 0 };
      agg[sector].weight += h.baseWeight;
      agg[sector].count += 1;
    }
    const breakdown = Object.entries(agg)
      .map(([sector, v]) => ({ sector, ...v }))
      .sort((a, b) => b.weight - a.weight);
    const mappedWeight = breakdown.reduce((s, r) => s + r.weight, 0);
    return { valid: mappedWeight > 0.5, numSectors: breakdown.length, breakdown };
  }, [riskData, allStocks]);

  // Group breakdown
  const groupStats = useMemo(() => {
    if (!riskData || riskData.holdings.length === 0)
      return { numGroups: 0, breakdown: [] as { cluster: number; weight: number; count: number }[] };
    const agg: Record<number, { weight: number; count: number }> = {};
    for (const h of riskData.holdings) {
      if (h.cluster == null) continue;
      if (!agg[h.cluster]) agg[h.cluster] = { weight: 0, count: 0 };
      agg[h.cluster].weight += h.baseWeight;
      agg[h.cluster].count += 1;
    }
    const breakdown = Object.entries(agg)
      .map(([c, v]) => ({ cluster: Number(c), ...v }))
      .sort((a, b) => b.weight - a.weight);
    return { numGroups: breakdown.length, breakdown };
  }, [riskData]);

  // Trigger history compute whenever holdings/weights change
  useEffect(() => {
    if (!riskData || riskData.holdings.length === 0) return;
    // Dedup key uses vol-target-scaled weight (h.weight) so a multiplier change triggers recompute
    const key = riskData.holdings.map((h) => `${h.ticker}:${h.weight.toFixed(5)}`).join(",");
    if (key === histKeyRef.current) return;
    histKeyRef.current = key;
    computeHistory.mutate({
      data: {
        // Pass vol-target-scaled weights (sum = risky sleeve, e.g. 0.8 if scaled down from 1.0)
        holdings:    riskData.holdings.map((h) => ({ ticker: h.ticker, weight: h.weight })),
        lookback:    504,
        sgovWeight:  riskData.sgovWeight,
      },
    });
  }, [riskData]);

  // Trigger reversal compute whenever basket ticker list changes
  useEffect(() => {
    if (basket.length < 2) return;
    const key = [...basket].sort().join(",");
    if (key === reversalKeyRef.current) return;
    reversalKeyRef.current = key;
    computeReversal.mutate({ data: { tickers: basket } });
  }, [basket]);

  // Diversify suggestions
  const diversifySuggestions = useMemo((): DiversifySuggestion[] => {
    if (basket.length === 0 || rankedStocks.length === 0) return [];
    const n = basket.length;
    const bSet = new Set(basket);
    const stockMap = new Map(rankedStocks.map((s) => [s.ticker, s]));

    const uGroupCounts = new Map<number, number>();
    for (const s of rankedStocks) {
      const g = s.cluster ?? -1;
      uGroupCounts.set(g, (uGroupCounts.get(g) ?? 0) + 1);
    }
    const groupTarget = 1 / uGroupCounts.size;
    const bGroupCounts = new Map<number, number>();
    for (const t of basket) {
      const g = stockMap.get(t)?.cluster ?? -1;
      bGroupCounts.set(g, (bGroupCounts.get(g) ?? 0) + 1);
    }
    const groupDeficits = new Map<number, number>();
    for (const g of uGroupCounts.keys()) {
      const d = groupTarget - (bGroupCounts.get(g) ?? 0) / n;
      if (d > 0) groupDeficits.set(g, d);
    }

    const uSectorCounts = new Map<string, number>();
    for (const s of rankedStocks) {
      const sec = s.sector ?? "";
      if (!sec) continue; // exclude unmapped tickers from sector target computation
      uSectorCounts.set(sec, (uSectorCounts.get(sec) ?? 0) + 1);
    }
    const sectorTarget = 1 / uSectorCounts.size;
    const bSectorCounts = new Map<string, number>();
    for (const t of basket) {
      const sec = stockMap.get(t)?.sector ?? "";
      if (!sec) continue; // exclude unmapped holdings from sector basket counts
      bSectorCounts.set(sec, (bSectorCounts.get(sec) ?? 0) + 1);
    }
    const sectorDeficits = new Map<string, number>();
    for (const sec of uSectorCounts.keys()) {
      const d = sectorTarget - (bSectorCounts.get(sec) ?? 0) / n;
      if (d > 0) sectorDeficits.set(sec, d);
    }

    const result: DiversifySuggestion[] = [];
    for (const s of rankedStocks) {
      if (bSet.has(s.ticker)) continue;
      const g = s.cluster ?? -1;
      const sec = s.sector ?? "";
      const gd = suggestMode !== "sector" ? (groupDeficits.get(g) ?? 0) : 0;
      // Unmapped tickers (no sector) never contribute a sector deficit
      const sd = sec && suggestMode !== "group" ? (sectorDeficits.get(sec) ?? 0) : 0;
      const deficit = Math.max(gd, sd);
      if (deficit === 0) continue;
      result.push({
        ticker: s.ticker, name: s.name ?? "", alpha: s.alpha ?? 0,
        sector: sec || "—", group: g,
        groupShare: (bGroupCounts.get(g) ?? 0) / n,
        sectorShare: sec ? (bSectorCounts.get(sec) ?? 0) / n : 0,
        deficit,
        via: gd > 0 && sd > 0 ? "both" : gd > 0 ? "group" : "sector",
      });
    }
    result.sort((a, b) => b.deficit - a.deficit || b.alpha - a.alpha);
    return result.slice(0, 10);
  }, [basket, rankedStocks, suggestMode]);

  // ── Empty state ────────────────────────────────────────────────────────────
  if (basket.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center min-h-full p-4 text-center">
        <div className="w-full max-w-sm space-y-4 bg-card/50 p-6 rounded-xl border border-border border-dashed">
          <div className="w-12 h-12 rounded-full bg-muted flex items-center justify-center mx-auto text-muted-foreground">
            <Calculator className="w-6 h-6" />
          </div>
          <h2 className="text-xl font-bold text-foreground">Empty Basket</h2>
          <p className="text-muted-foreground text-sm">
            Add equities from the universe rankings to construct a basket. Weights are automated by the selected method.
          </p>
          <div className="pt-2 flex flex-col gap-3">
            <Link href="/"><Button className="w-full">Go to Rankings</Button></Link>
            <div className="flex items-center gap-2 text-sm text-muted-foreground px-2">
              <span className="h-px bg-border flex-1"></span>
              <span>OR</span>
              <span className="h-px bg-border flex-1"></span>
            </div>
            <div className="flex gap-1">
              {(["alpha", "group", "corr"] as const).map((m) => (
                <Button key={m} variant={seedMode === m ? "secondary" : "outline"} size="sm"
                  className="flex-1 h-7 text-xs" onClick={() => setSeedMode(m)}>
                  {m === "alpha" ? "Alpha" : m === "group" ? "Group" : "Corr"}
                </Button>
              ))}
            </div>
            <p className="text-[11px] text-muted-foreground/70 leading-snug text-left -mt-1">
              {seedMode === "alpha" && "Top N names by composite alpha score."}
              {seedMode === "group" && `Round-robin across ${mainControls.clusterK} groups (top ${mainControls.clusterN} stocks): picks the best name from each group, then second-best, until full.`}
              {seedMode === "corr" && "Greedy selection: adds each candidate only if its max pairwise correlation to current basket is ≤ threshold."}
            </p>
            {seedMode === "corr" && (
              <div className="flex items-center gap-2">
                <span className="text-xs text-muted-foreground shrink-0">Max r</span>
                <Input type="number" value={maxCorr} onChange={(e) => setMaxCorr(e.target.value)}
                  className="w-20 text-center h-7 text-xs" min="0.10" max="0.99" step="0.05" />
                <span className="text-[11px] text-muted-foreground/50">abs. Pearson</span>
              </div>
            )}
            <div className="flex gap-2">
              <Input type="number" value={seedCount} onChange={(e) => setSeedCount(e.target.value)}
                className="w-20 text-center" min="1" max="60" />
              <Button variant="outline" className="flex-1" onClick={handleSeed}
                disabled={allStocks.length === 0 || corrSeed.isPending}>
                {corrSeed.isPending && seedMode === "corr"
                  ? <><Loader2 className="w-3 h-3 mr-1.5 animate-spin" />Seeding…</>
                  : "Seed Top N"}
              </Button>
            </div>
            {allStocks.length === 0 && (
              <p className="text-xs text-amber-500/80 flex items-center justify-center gap-1">
                <Info className="w-3 h-3" /> Rankings not loaded yet
              </p>
            )}
            {corrSeed.isError && (
              <p className="text-xs text-destructive/80 flex items-center justify-center gap-1">
                <AlertTriangle className="w-3 h-3" /> Corr seed failed — try a higher max r or Alpha mode.
              </p>
            )}
          </div>
        </div>
      </div>
    );
  }

  // ── Main layout ────────────────────────────────────────────────────────────
  return (
    <div className="p-3 md:p-4 max-w-7xl mx-auto flex gap-4 md:gap-6 flex-col lg:flex-row lg:h-full lg:overflow-hidden">

      {/* ── LEFT: Basket + method ────────────────────────────────────────── */}
      <div className="w-full lg:w-[320px] flex flex-col gap-3 flex-shrink-0 lg:h-full lg:overflow-hidden">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-base font-bold tracking-tight text-foreground">Basket</h1>
            <p className="text-[11px] text-muted-foreground/60 mt-0.5">{basket.length} holding{basket.length !== 1 ? "s" : ""} · weights automated</p>
          </div>
          <Button variant="ghost" size="sm" onClick={clearBasket}
            className="text-[11px] text-muted-foreground/60 hover:text-destructive h-7 px-2">
            Clear all
          </Button>
        </div>

        <Card className="lg:flex-1 flex flex-col lg:overflow-hidden bg-card border-border min-h-0">
          {/* Method selector */}
          <div className="p-3 border-b border-border/50">
            <Select value={weightingMethod}
              onValueChange={(v) => setWeightingMethod(v as PortfolioRiskRequestWeightingMethod)}>
              <SelectTrigger className="w-full text-sm h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {METHODS.map((m) => (
                  <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="text-[10px] text-muted-foreground/60 mt-1.5 leading-snug">
              {METHODS.find((m) => m.value === weightingMethod)?.desc}
            </p>
          </div>

          {/* Holdings table */}
          <div className="overflow-auto flex-1 min-h-0">
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-card z-10 border-b border-border/40">
                <tr>
                  <th className="text-left px-3 py-2 text-[10px] font-medium text-muted-foreground/60 uppercase tracking-wider">Ticker</th>
                  <th className="text-right px-3 py-2 text-[10px] font-medium text-muted-foreground/60 uppercase tracking-wider">Wt%</th>
                  <th className="w-8"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/20">
                {basket.map((ticker) => {
                  const fw = weightMap[ticker];
                  const pct = fw !== undefined ? fw : 0;
                  const isHigh = pct > 0.18;
                  return (
                    <tr key={ticker} className="group hover:bg-muted/30 transition-colors">
                      <td className="px-3 py-2.5">
                        <div className="font-bold text-foreground tracking-tight">{ticker}</div>
                        {/* Mini weight bar */}
                        {fw !== undefined && !isComputing && (
                          <div className="mt-1 h-0.5 w-full bg-muted rounded-full overflow-hidden">
                            <div
                              className={cn("h-full rounded-full transition-all", isHigh ? "bg-amber-400/60" : "bg-primary/50")}
                              style={{ width: `${Math.min(pct * 100 / 20 * 100, 100)}%` }}
                            />
                          </div>
                        )}
                      </td>
                      <td className="px-3 py-2.5 text-right font-mono">
                        {isComputing ? (
                          <span className="text-muted-foreground/30">—</span>
                        ) : fw !== undefined ? (
                          <span className={cn("font-semibold", isHigh ? "text-amber-400" : "text-primary")}>
                            {formatPercent(fw, 1)}
                          </span>
                        ) : (
                          <span className="text-muted-foreground/30">—</span>
                        )}
                      </td>
                      <td className="pr-2 text-center">
                        <button
                          onClick={() => removeFromBasket(ticker)}
                          className="h-6 w-6 flex items-center justify-center rounded text-muted-foreground/30 hover:text-destructive opacity-0 group-hover:opacity-100 transition-all"
                        >
                          <Trash2 className="w-3 h-3" />
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* Status footer */}
          <div className="px-3 py-2.5 border-t border-border/40 bg-muted/10">
            <StatusLine riskData={riskData} isComputing={isComputing}
              hasError={hasError} isEngineDown={isEngineDown} requestedMethod={weightingMethod} />
          </div>
        </Card>
      </div>

      {/* ── RIGHT: Analysis ─────────────────────────────────────────────── */}
      <div className="flex-1 flex flex-col gap-3 lg:overflow-hidden lg:h-full">

        {/* Error states */}
        {hasError && !riskData ? (
          <div className="flex-1 flex items-center justify-center border border-dashed rounded-xl bg-card/20 min-h-[140px] lg:min-h-0 border-destructive/30">
            <div className="flex flex-col items-center gap-3 p-6 text-center">
              {isEngineDown ? (
                <>
                  <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
                  <p className="text-sm text-muted-foreground">Engine is starting up — data loads in a few seconds.</p>
                  <button onClick={triggerCompute} className="text-xs text-primary underline underline-offset-2 hover:opacity-80">Retry now</button>
                </>
              ) : (
                <>
                  <AlertTriangle className="w-6 h-6 text-destructive/70" />
                  <p className="text-sm text-destructive">Failed to compute risk.</p>
                  <button onClick={triggerCompute} className="text-xs text-primary underline underline-offset-2 hover:opacity-80">Retry</button>
                </>
              )}
            </div>
          </div>
        ) : !riskData ? (
          <div className="flex-1 flex items-center justify-center border border-border border-dashed rounded-xl bg-card/20 text-muted-foreground min-h-[140px] lg:min-h-0">
            {isComputing ? (
              <div className="flex flex-col items-center gap-3 p-6 text-center">
                <Loader2 className="w-7 h-7 animate-spin text-primary" />
                <p className="text-sm">Computing covariance matrix…</p>
              </div>
            ) : (
              <p className="text-sm p-6 text-center">Add stocks to the basket to see risk metrics.</p>
            )}
          </div>
        ) : (
          <div className="lg:flex-1 lg:overflow-auto space-y-3 lg:pr-1 pb-[max(env(safe-area-inset-bottom,0px),1.5rem)]">

            {/* ── A. Portfolio Intelligence ─────────────────────────────── */}
            <div className="bg-card border border-border rounded-lg px-4 py-3">
              <p className="text-[10px] uppercase tracking-wider text-muted-foreground/60 font-semibold mb-1.5">Portfolio Intelligence</p>
              <p className="text-xs text-muted-foreground leading-relaxed">
                {buildNarrative(riskData, METHOD_LABELS[riskData.method] ?? riskData.method, sectorStats.numSectors, groupStats.numGroups)}
              </p>
              {riskData.fallback && (
                <p className="mt-2 text-[10px] text-amber-400/80 flex items-center gap-1.5">
                  <AlertTriangle className="w-3 h-3 shrink-0" />
                  {riskData.fallback}
                </p>
              )}
            </div>

            {/* ── B. Key metrics — 2×4 grid ─────────────────────────────── */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-px bg-border rounded-lg overflow-hidden border border-border">
              <MetricCell label="Method" value={METHOD_LABELS[riskData.method] ?? riskData.method} small />
              <MetricCell label="Base Vol" value={formatPercent(riskData.basePortVol, 1)} />
              <MetricCell label="Target Vol" value={formatPercent(VOL_TARGET, 0)} dim />
              <MetricCell label="Scale" value={`×${formatNumber(riskData.volTargetMultiplier, 2)}`}
                dim={riskData.volTargetMultiplier >= 0.999} />

              <MetricCell label="Names" value={String(riskData.numHoldings)} />
              <MetricCell label="Max Wt" value={formatPercent(maxPosition, 1)}
                warn={maxPosition >= 0.149} />
              <MetricCell label="Avg Corr" value={formatNumber(riskData.avgCorrelation, 2)}
                warn={riskData.avgCorrelation > 0.55} />
              <MetricCell label="Avg Alpha"
                value={avgAlpha != null ? (avgAlpha > 0 ? "+" : "") + formatNumber(avgAlpha, 2) : "—"}
                highlight={avgAlpha != null && avgAlpha > 0} />
            </div>

            {/* ── C. Risk Contribution chart ────────────────────────────── */}
            <RiskContribChart holdings={riskData.holdings} />

            {/* ── D. Sector exposure ────────────────────────────────────── */}
            {sectorStats.valid && sectorStats.breakdown.length > 0 && (
              <ExposureCard
                title="Sector Exposure"
                rows={sectorStats.breakdown.map(r => ({ label: r.sector, weight: r.weight, count: r.count }))}
              />
            )}

            {/* ── E. Group exposure ─────────────────────────────────────── */}
            {groupStats.breakdown.length > 0 && (
              <ExposureCard
                title="Group Exposure"
                rows={groupStats.breakdown.map(r => ({ label: `G${r.cluster}`, weight: r.weight, count: r.count }))}
              />
            )}

            {/* ── F. Constituent table ──────────────────────────────────── */}
            <ConstituentTable
              holdings={riskData.holdings}
              scale={riskData.volTargetMultiplier}
              reversalMap={reversalMap}
              reversalLoading={computeReversal.isPending}
            />

            {/* ── G. Historical Performance ─────────────────────────────── */}
            <PortfolioHistoryCard
              histData={computeHistory.data ?? null}
              isLoading={computeHistory.isPending}
            />

            {/* ── H. Diversify Suggestions ─────────────────────────────── */}
            {diversifySuggestions.length > 0 && (
              <DiversifyCard
                suggestions={diversifySuggestions}
                suggestMode={suggestMode}
                onModeChange={setSuggestMode}
                onAdd={addToBasket}
              />
            )}

          </div>
        )}
      </div>
    </div>
  );
}

// ── Sub-components ─────────────────────────────────────────────────────────

function MetricCell({
  label, value, small, dim, highlight, warn,
}: {
  label: string; value: string; small?: boolean; dim?: boolean; highlight?: boolean; warn?: boolean;
}) {
  return (
    <div className="bg-card px-3 py-2.5 flex flex-col gap-0.5">
      <span className="text-[9px] uppercase tracking-wider text-muted-foreground/50 font-medium">{label}</span>
      <span className={cn(
        "font-bold font-mono leading-tight",
        small ? "text-sm" : "text-base",
        dim ? "text-muted-foreground/50" :
        warn ? "text-amber-400" :
        highlight ? "text-primary" :
        "text-foreground"
      )}>{value}</span>
    </div>
  );
}

function RiskContribChart({
  holdings,
}: {
  holdings: Array<{ ticker: string; riskContrib: number; baseWeight: number; cluster: number | null | undefined }>;
}) {
  const sorted = useMemo(
    () => [...holdings].sort((a, b) => b.riskContrib - a.riskContrib),
    [holdings]
  );
  const maxRC = sorted[0]?.riskContrib ?? 0.01;

  return (
    <Card className="bg-card border-border">
      <CardHeader className="p-4 pb-2">
        <CardTitle className="text-sm">Risk Contribution by Holding</CardTitle>
        <CardDescription className="text-[11px]">
          Share of total portfolio variance · bar = risk%, label = position weight
        </CardDescription>
      </CardHeader>
      <CardContent className="px-4 pb-4 pt-2 space-y-1.5">
        {sorted.map((h) => {
          const rc = h.riskContrib;
          const bw = h.baseWeight;
          const isConcentrated = rc > bw * 1.4;
          const barPct = (rc / maxRC) * 100;
          const wtPct = (bw / maxRC) * 100;

          return (
            <div key={h.ticker} className="flex items-center gap-2">
              <div className="w-12 text-xs font-bold text-foreground/80 shrink-0 font-mono">{h.ticker}</div>
              <div className="flex-1 relative h-5 flex items-center">
                {/* Weight ghost bar */}
                <div
                  className="absolute h-5 rounded-sm bg-muted/60"
                  style={{ width: `${Math.min(wtPct, 100)}%` }}
                />
                {/* Risk contrib bar */}
                <div
                  className={cn(
                    "absolute h-5 rounded-sm opacity-80 transition-all",
                    isConcentrated ? "bg-amber-500/70" : "bg-primary/60"
                  )}
                  style={{ width: `${Math.min(barPct, 100)}%` }}
                />
                <div className="absolute inset-0 flex items-center px-1.5">
                  <span className={cn("text-[10px] font-bold mix-blend-plus-lighter",
                    isConcentrated ? "text-amber-200" : "text-primary-foreground/90"
                  )}>
                    {formatPercent(rc, 1)}
                  </span>
                </div>
              </div>
              <div className="w-12 text-right text-[10px] text-muted-foreground/60 shrink-0 font-mono">
                {formatPercent(bw, 1)}
              </div>
            </div>
          );
        })}
        <div className="flex items-center gap-2 pt-1 border-t border-border/30">
          <div className="w-12" />
          <div className="flex-1 flex items-center gap-3 text-[9px] text-muted-foreground/50">
            <span className="flex items-center gap-1">
              <span className="inline-block w-3 h-2 rounded-sm bg-primary/60 opacity-80" /> risk contrib
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block w-3 h-2 rounded-sm bg-muted/60" /> position wt
            </span>
          </div>
          <div className="w-12 text-right text-[9px] text-muted-foreground/40">pos wt</div>
        </div>
      </CardContent>
    </Card>
  );
}

function ExposureCard({
  title,
  rows,
}: {
  title: string;
  rows: { label: string; weight: number; count: number }[];
}) {
  const maxW = rows[0]?.weight ?? 0.001;
  const totalW = rows.reduce((s, r) => s + r.weight, 0);

  return (
    <Card className="bg-card border-border">
      <CardHeader className="p-4 pb-2">
        <CardTitle className="text-sm">{title}</CardTitle>
      </CardHeader>
      <CardContent className="px-4 pb-4 pt-1 space-y-2">
        {rows.map((row) => (
          <div key={row.label} className="flex items-center gap-2">
            <div className="w-24 text-xs font-mono text-foreground/70 truncate shrink-0">{row.label}</div>
            <div className="flex-1 h-1.5 bg-muted rounded-full overflow-hidden">
              <div
                className="h-full bg-primary/60 rounded-full transition-all"
                style={{ width: `${(row.weight / maxW) * 100}%` }}
              />
            </div>
            <div className="w-10 text-right text-xs font-mono text-primary">{formatPercent(row.weight, 1)}</div>
            <div className="w-6 text-right text-[10px] text-muted-foreground/50">{row.count}</div>
          </div>
        ))}
        {totalW < 0.98 && (
          <div className="flex items-center gap-2">
            <div className="w-24 text-xs font-mono text-muted-foreground/40 shrink-0">Unmapped</div>
            <div className="flex-1 h-1.5 bg-muted rounded-full overflow-hidden">
              <div className="h-full bg-muted-foreground/20 rounded-full"
                style={{ width: `${((1 - totalW) / maxW) * 100}%` }} />
            </div>
            <div className="w-10 text-right text-xs font-mono text-muted-foreground/40">
              {formatPercent(1 - totalW, 1)}
            </div>
            <div className="w-6" />
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function timingColor(pct: number): string {
  // pct: [0..1] where 1/n = most dipped (rank 1), 1.0 = most extended (rank n)
  const r = Math.round(pct * 220);
  const g = Math.round((1 - pct) * 180);
  return `rgb(${r},${g},40)`;
}

function ConstituentTable({
  holdings,
  scale,
  reversalMap,
  reversalLoading,
}: {
  holdings: Array<{ ticker: string; baseWeight: number; weight: number; vol: number; riskContrib: number; cluster?: number | null }>;
  scale: number;
  reversalMap: Record<string, ReversalItem>;
  reversalLoading: boolean;
}) {
  const sorted = useMemo(() => [...holdings].sort((a, b) => b.baseWeight - a.baseWeight), [holdings]);
  const maxBW = sorted[0]?.baseWeight ?? 0.01;
  const reversalN = useMemo(() => Object.keys(reversalMap).length, [reversalMap]);

  return (
    <Card className="bg-card border-border">
      <CardHeader className="p-4 pb-2">
        <CardTitle className="text-sm">Constituents</CardTitle>
        <CardDescription className="text-[11px]">
          Sorted by base weight · scaled ×{formatNumber(scale, 2)} for 15% vol target
        </CardDescription>
      </CardHeader>
      <CardContent className="p-0">
        <table className="w-full text-xs">
          <thead className="border-b border-border/40">
            <tr>
              <th className="text-left px-4 py-2 text-[10px] font-medium text-muted-foreground/50 uppercase tracking-wider">Ticker</th>
              <th className="text-right px-3 py-2 text-[10px] font-medium text-muted-foreground/50 uppercase tracking-wider">Wt%</th>
              <th className="text-right px-3 py-2 text-[10px] font-medium text-muted-foreground/50 uppercase tracking-wider">Risk%</th>
              <th className="text-right px-3 py-2 text-[10px] font-medium text-muted-foreground/50 uppercase tracking-wider">Vol</th>
              <th className="text-center px-3 py-2 text-[10px] font-medium text-muted-foreground/50 uppercase tracking-wider">Grp</th>
              <th className="text-center px-3 py-2 text-[10px] font-medium text-muted-foreground/50 uppercase tracking-wider">Timing</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border/20">
            {sorted.map((h) => {
              const bw = h.baseWeight;
              const rc = h.riskContrib;
              const isConcentratedRisk = rc > bw * 1.4;
              const rev = reversalMap[h.ticker];
              return (
                <tr key={h.ticker} className="hover:bg-muted/20 transition-colors">
                  <td className="px-4 py-2.5">
                    <div className="font-bold text-foreground">{h.ticker}</div>
                    <div className="mt-1 h-0.5 w-full bg-muted rounded-full overflow-hidden">
                      <div className="h-full bg-primary/40 rounded-full"
                        style={{ width: `${(bw / maxBW) * 100}%` }} />
                    </div>
                  </td>
                  <td className="px-3 py-2.5 text-right font-mono font-semibold text-primary">
                    {formatPercent(bw, 1)}
                  </td>
                  <td className={cn("px-3 py-2.5 text-right font-mono", isConcentratedRisk ? "text-amber-400" : "text-muted-foreground")}>
                    {formatPercent(rc, 1)}
                  </td>
                  <td className="px-3 py-2.5 text-right text-muted-foreground/70">
                    {formatPercent(h.vol, 1)}
                  </td>
                  <td className="px-3 py-2.5 text-center text-muted-foreground/60 font-mono">
                    {h.cluster != null ? `G${h.cluster}` : "—"}
                  </td>
                  <td className="px-3 py-2.5 text-center">
                    {reversalLoading && !rev ? (
                      <span className="text-muted-foreground/30 font-mono">—</span>
                    ) : rev ? (
                      <span
                        title={`Rank #${rev.rank} of ${reversalN} · reversal score ${rev.reversalScore.toFixed(2)} · 21d log return ${(rev.r21 * 100).toFixed(1)}%${rev.r21Res !== rev.r21 ? ` · sector-adj ${(rev.r21Res * 100).toFixed(1)}%` : ""} · ${rev.pct <= 0.25 ? "dipped" : rev.pct >= 0.75 ? "extended" : "neutral"}`}
                        className="inline-flex flex-col items-center gap-0.5"
                      >
                        <span
                          className="font-mono font-semibold text-[11px] leading-none px-1.5 py-0.5 rounded"
                          style={{ color: timingColor(rev.pct), background: `${timingColor(rev.pct)}22` }}
                        >
                          #{rev.rank}
                        </span>
                        {rev.pct <= 0.25 && (
                          <span className="text-[9px] leading-none text-emerald-500/70 font-medium">dip</span>
                        )}
                        {rev.pct >= 0.75 && (
                          <span className="text-[9px] leading-none text-red-400/70 font-medium">ext</span>
                        )}
                      </span>
                    ) : (
                      <span className="text-muted-foreground/30 font-mono">—</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </CardContent>
    </Card>
  );
}

function StatusLine({
  riskData, isComputing, hasError, isEngineDown, requestedMethod,
}: {
  riskData: ReturnType<typeof useComputePortfolioRisk>["data"];
  isComputing: boolean;
  hasError: boolean;
  isEngineDown?: boolean;
  requestedMethod: string;
}) {
  if (isComputing) {
    return (
      <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground/50">
        <Loader2 className="w-2.5 h-2.5 animate-spin" /><span>Computing…</span>
      </div>
    );
  }
  if (hasError && !riskData) {
    return isEngineDown ? (
      <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground/50">
        <Loader2 className="w-2.5 h-2.5 animate-spin" /><span>Engine starting…</span>
      </div>
    ) : (
      <div className="flex items-center gap-1.5 text-[10px] text-destructive/70">
        <AlertTriangle className="w-2.5 h-2.5" /><span>Computation failed</span>
      </div>
    );
  }
  if (!riskData) {
    return <div className="text-[10px] text-muted-foreground/40">Waiting for first compute…</div>;
  }

  const methodLabel = METHOD_LABELS[riskData.method] ?? riskData.method;
  return (
    <div className="space-y-0.5">
      <div className="font-mono text-[10px] text-muted-foreground/60 leading-relaxed truncate">
        <span className="text-foreground/60">{methodLabel}</span>
        {riskData.covModel && <> · <span className="text-foreground/50">{riskData.covModel}</span></>}
        {" · "}<span className="text-foreground/70">{formatPercent(riskData.basePortVol, 1)}</span>
        {" ×"}<span className="text-foreground/70">{formatNumber(riskData.volTargetMultiplier, 2)}</span>
        {" → "}<span className="text-foreground/70">{formatPercent(riskData.portfolioVol, 1)}</span>
      </div>
      {riskData.method !== requestedMethod && (
        <p className="text-[10px] text-amber-400/80">⚠ Method fallback active</p>
      )}
    </div>
  );
}

function DiversifyCard({
  suggestions, suggestMode, onModeChange, onAdd,
}: {
  suggestions: DiversifySuggestion[];
  suggestMode: SuggestMode;
  onModeChange: (m: SuggestMode) => void;
  onAdd: (ticker: string) => void;
}) {
  const modes: { value: SuggestMode; label: string }[] = [
    { value: "group", label: "Group" },
    { value: "sector", label: "Sector" },
    { value: "both", label: "Both" },
  ];

  return (
    <Card className="bg-card border-border">
      <CardHeader className="p-4 pb-3">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <Sparkles className="w-3.5 h-3.5 text-primary/70 shrink-0" />
            <CardTitle className="text-sm">Diversify Suggestions</CardTitle>
          </div>
          <div className="flex rounded-md border border-border overflow-hidden shrink-0">
            {modes.map((m) => (
              <button key={m.value} onClick={() => onModeChange(m.value)}
                className={cn(
                  "px-2.5 py-1 text-[11px] font-medium transition-colors",
                  suggestMode === m.value
                    ? "bg-primary text-primary-foreground"
                    : "bg-transparent text-muted-foreground hover:text-foreground"
                )}>
                {m.label}
              </button>
            ))}
          </div>
        </div>
        <CardDescription className="text-[11px] mt-1">
          Highest-alpha names from underrepresented{" "}
          {suggestMode === "group" ? "groups" : suggestMode === "sector" ? "sectors" : "groups & sectors"}{" "}
          · equal-weight target across {suggestMode === "group" ? "groups" : suggestMode === "sector" ? "sectors" : "both"}
        </CardDescription>
      </CardHeader>
      <CardContent className="p-0">
        <div className="divide-y divide-border/50">
          {suggestions.map((s) => {
            const alphaSign = s.alpha >= 0 ? "+" : "";
            const alphaColor = s.alpha >= 0 ? "text-emerald-400" : "text-rose-400";
            const groupLabel = s.group === -1 ? "—" : `G${s.group}`;
            const viaBadge =
              s.via === "both" ? "Grp+Sec"
              : s.via === "group" ? `Grp ${formatPercent(s.groupShare, 0)}`
              : `Sec ${formatPercent(s.sectorShare, 0)}`;
            const viaBadgeColor =
              s.via === "both" ? "text-amber-400/80"
              : s.via === "group" ? "text-sky-400/80"
              : "text-violet-400/80";

            return (
              <div key={s.ticker} className="flex items-center gap-2.5 px-4 py-2.5 hover:bg-muted/30 transition-colors">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-1.5">
                    <span className="font-bold text-sm text-foreground tracking-tight">{s.ticker}</span>
                    <span className={cn("font-mono text-xs font-semibold", alphaColor)}>{alphaSign}{s.alpha.toFixed(2)}</span>
                    <span className={cn("text-[10px] font-medium", viaBadgeColor)}>{viaBadge}</span>
                  </div>
                  <div className="flex items-center gap-1.5 mt-0.5">
                    <span className="text-[11px] text-muted-foreground truncate max-w-[120px]">{s.sector}</span>
                    <span className="text-[10px] text-muted-foreground/50">·</span>
                    <span className="text-[11px] text-muted-foreground/70 font-mono shrink-0">{groupLabel}</span>
                  </div>
                </div>
                <button onClick={() => onAdd(s.ticker)}
                  className="flex items-center gap-1 px-2.5 py-1 rounded-md bg-primary/10 hover:bg-primary/20 text-primary text-[11px] font-semibold transition-colors shrink-0">
                  <Plus className="w-3 h-3" />Add
                </button>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
