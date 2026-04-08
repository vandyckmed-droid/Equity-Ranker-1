import { useState, useMemo, useEffect, useRef, useCallback } from "react";
import { Link } from "wouter";
import {
  useComputePortfolioRisk,
  useComputeCorrSeed,
  useGetRankings,
  PortfolioRiskRequestWeightingMethod,
  type Stock,
} from "@workspace/api-client-react";
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
  { value: "equal",       label: "Equal Weight", desc: "Same weight to every holding — simple and transparent" },
  { value: "inverse_vol", label: "Inverse Vol",  desc: "Smaller weight to more volatile names — diagonal risk control" },
  { value: "signal_vol",  label: "Signal / Vol", desc: "Stronger alpha signal + lower vol → more weight" },
  { value: "risk_parity", label: "Risk Parity",  desc: "Each holding contributes equal portfolio risk (capped ERC via SLSQP)" },
];

const METHOD_LABELS: Record<string, string> = Object.fromEntries(
  METHODS.map((m) => [m.value, m.label])
);

const VOL_TARGET = 0.15;
const LOOKBACK = 126;

export default function PortfolioPage() {
  const { basket, addToBasket, removeFromBasket, clearBasket, seedBasket, allStocks, rankedStocks, setRankedStocks } = usePortfolio();

  // Auto-fetch universe when navigating directly to /portfolio (rankedStocks empty until MainPage loads)
  const { data: autoFetchData } = useGetRankings(undefined, {
    query: { enabled: basket.length > 0 && rankedStocks.length === 0, staleTime: 5 * 60 * 1000 },
  });
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

    // Source is always the current ranked universe from MainPage (alpha-sorted, mcap-filtered)
    const universe: Stock[] = rankedStocks.length > 0 ? rankedStocks : allStocks;

    if (seedMode === "alpha") {
      // ── Mode 1: Pure alpha rank — top N by composite score ─────────────────
      seedBasket(universe.slice(0, n).map((s) => s.ticker));
      setWeightingMethod("equal");

    } else if (seedMode === "group") {
      // ── Mode 2: Group-balanced — round-robin across cluster groups ──────────
      // universe is already sorted best-alpha-first within each group.
      const byGroup = new Map<number, Stock[]>();
      for (const s of universe) {
        const key = s.cluster ?? -1;   // -1 = unclustered (ranks > cluster_n)
        if (!byGroup.has(key)) byGroup.set(key, []);
        byGroup.get(key)!.push(s);
      }
      // Numbered groups first (ascending), unclustered (-1) last
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
      // ── Mode 3: Correlation-constrained — greedy server-side selection ──────
      const parsedCorr = parseFloat(maxCorr);
      const threshold = isNaN(parsedCorr) ? 0.7 : Math.min(0.99, Math.max(0.10, parsedCorr));
      corrSeed.mutate(
        { data: { tickers: universe.map((s) => s.ticker), n, maxCorr: threshold, lookback: LOOKBACK } },
        {
          onSuccess: (data) => {
            seedBasket(data.tickers);
            setWeightingMethod("equal");
          },
        }
      );
    }
  };

  const riskData = computeRisk.data;
  const isComputing = computeRisk.isPending;
  const hasError = computeRisk.isError;
  const isEngineDown = hasError && (computeRisk.error as { status?: number })?.status === 503;

  const weightMap = useMemo(() => {
    if (!riskData) return {} as Record<string, number>;
    return Object.fromEntries(riskData.holdings.map((h) => [h.ticker, h.baseWeight]));
  }, [riskData]);

  // Sector breakdown — filter out unmapped tickers; only trust if >50% weight is resolved
  const sectorStats = useMemo(() => {
    const empty = { valid: false, numSectors: 0, breakdown: [] as { sector: string; weight: number; count: number }[], topSector: "", topWeight: 0 };
    if (!riskData || riskData.holdings.length === 0) return empty;
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
    const valid = mappedWeight > 0.5;
    return { valid, numSectors: breakdown.length, breakdown, topSector: breakdown[0]?.sector ?? "", topWeight: breakdown[0]?.weight ?? 0 };
  }, [riskData, allStocks]);

  // Group (cluster) breakdown
  const groupStats = useMemo(() => {
    if (!riskData || riskData.holdings.length === 0) {
      return { numGroups: 0, breakdown: [] as { cluster: number; weight: number; count: number }[], topGroup: null as number | null, topWeight: 0 };
    }
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
    return { numGroups: breakdown.length, breakdown, topGroup: breakdown[0]?.cluster ?? null, topWeight: breakdown[0]?.weight ?? 0 };
  }, [riskData]);

  const maxPosition = useMemo(() => {
    if (!riskData || riskData.holdings.length === 0) return 0;
    return Math.max(...riskData.holdings.map((h) => h.baseWeight));
  }, [riskData]);

  // ── Diversify suggestions ────────────────────────────────────────────────
  const diversifySuggestions = useMemo((): DiversifySuggestion[] => {
    if (basket.length === 0 || rankedStocks.length === 0) return [];
    const n = basket.length;
    const bSet = new Set(basket);
    const stockMap = new Map(rankedStocks.map((s) => [s.ticker, s]));

    // Group (cluster) analysis
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

    // Sector analysis
    const uSectorCounts = new Map<string, number>();
    for (const s of rankedStocks) {
      const sec = s.sector ?? "Unknown";
      uSectorCounts.set(sec, (uSectorCounts.get(sec) ?? 0) + 1);
    }
    const sectorTarget = 1 / uSectorCounts.size;
    const bSectorCounts = new Map<string, number>();
    for (const t of basket) {
      const sec = stockMap.get(t)?.sector ?? "Unknown";
      bSectorCounts.set(sec, (bSectorCounts.get(sec) ?? 0) + 1);
    }
    const sectorDeficits = new Map<string, number>();
    for (const sec of uSectorCounts.keys()) {
      const d = sectorTarget - (bSectorCounts.get(sec) ?? 0) / n;
      if (d > 0) sectorDeficits.set(sec, d);
    }

    // Score every candidate (rankedStocks is alpha-sorted)
    const result: DiversifySuggestion[] = [];
    for (const s of rankedStocks) {
      if (bSet.has(s.ticker)) continue;
      const g = s.cluster ?? -1;
      const sec = s.sector ?? "Unknown";
      const gd = suggestMode !== "sector" ? (groupDeficits.get(g) ?? 0) : 0;
      const sd = suggestMode !== "group" ? (sectorDeficits.get(sec) ?? 0) : 0;
      const deficit = Math.max(gd, sd);
      if (deficit === 0) continue;
      result.push({
        ticker: s.ticker,
        name: s.name ?? "",
        alpha: s.alpha ?? 0,
        sector: sec,
        group: g,
        groupShare: (bGroupCounts.get(g) ?? 0) / n,
        sectorShare: (bSectorCounts.get(sec) ?? 0) / n,
        deficit,
        via: gd > 0 && sd > 0 ? "both" : gd > 0 ? "group" : "sector",
      });
    }
    result.sort((a, b) => b.deficit - a.deficit || b.alpha - a.alpha);
    return result.slice(0, 10);
  }, [basket, rankedStocks, suggestMode]);

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
            <Link href="/">
              <Button className="w-full">Go to Rankings</Button>
            </Link>
            <div className="flex items-center gap-2 text-sm text-muted-foreground px-2">
              <span className="h-px bg-border flex-1"></span>
              <span>OR</span>
              <span className="h-px bg-border flex-1"></span>
            </div>

            {/* ── Seed mode selector ─────────────────────────────────── */}
            <div className="flex gap-1">
              {(["alpha", "group", "corr"] as const).map((m) => (
                <Button
                  key={m}
                  variant={seedMode === m ? "secondary" : "outline"}
                  size="sm"
                  className="flex-1 h-7 text-xs"
                  onClick={() => setSeedMode(m)}
                >
                  {m === "alpha" ? "Alpha" : m === "group" ? "Group" : "Corr"}
                </Button>
              ))}
            </div>

            {/* ── Mode description ───────────────────────────────────── */}
            <p className="text-[11px] text-muted-foreground/70 leading-snug text-left -mt-1">
              {seedMode === "alpha" && "Top N names by composite alpha score."}
              {seedMode === "group" && "Round-robin across groups: picks the best name from each group, then second-best, until full."}
              {seedMode === "corr" && "Greedy selection: adds each candidate only if its max pairwise correlation to current basket is ≤ threshold."}
            </p>

            {/* ── Corr threshold (corr mode only) ────────────────────── */}
            {seedMode === "corr" && (
              <div className="flex items-center gap-2">
                <span className="text-xs text-muted-foreground shrink-0">Max r</span>
                <Input
                  type="number"
                  value={maxCorr}
                  onChange={(e) => setMaxCorr(e.target.value)}
                  className="w-20 text-center h-7 text-xs"
                  min="0.10"
                  max="0.99"
                  step="0.05"
                />
                <span className="text-[11px] text-muted-foreground/50">abs. Pearson</span>
              </div>
            )}

            {/* ── N input + seed button ──────────────────────────────── */}
            <div className="flex gap-2">
              <Input
                type="number"
                value={seedCount}
                onChange={(e) => setSeedCount(e.target.value)}
                className="w-20 text-center"
                min="1"
                max="60"
              />
              <Button
                variant="outline"
                className="flex-1"
                onClick={handleSeed}
                disabled={allStocks.length === 0 || corrSeed.isPending}
              >
                {corrSeed.isPending && seedMode === "corr"
                  ? <><Loader2 className="w-3 h-3 mr-1.5 animate-spin" />Seeding…</>
                  : "Seed Top N"
                }
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

  return (
    <div className="p-3 md:p-4 max-w-7xl mx-auto flex gap-4 md:gap-6 flex-col lg:flex-row lg:h-full lg:overflow-hidden">

      {/* LEFT PANEL: Basket & Method */}
      <div className="w-full lg:w-[360px] flex flex-col gap-3 flex-shrink-0 lg:h-full lg:overflow-hidden">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-lg md:text-xl font-bold tracking-tight text-foreground">Portfolio Basket</h1>
            <p className="text-[11px] text-muted-foreground/70 mt-0.5">Manually selected · weights automated</p>
          </div>
          <Button variant="ghost" size="sm" onClick={clearBasket} className="text-muted-foreground hover:text-destructive">
            Clear All
          </Button>
        </div>

        <Card className="lg:flex-1 flex flex-col lg:overflow-hidden bg-card border-border min-h-0">
          <CardHeader className="p-4 pb-3 border-b border-border/50 bg-muted/20">
            <Select
              value={weightingMethod}
              onValueChange={(v) => setWeightingMethod(v as PortfolioRiskRequestWeightingMethod)}
            >
              <SelectTrigger className="w-full text-sm h-9">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {METHODS.map((m) => (
                  <SelectItem key={m.value} value={m.value}>
                    {m.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <p className="text-[11px] text-muted-foreground/70 mt-1.5 leading-snug">
              {METHODS.find((m) => m.value === weightingMethod)?.desc}
            </p>
          </CardHeader>

          <div className="overflow-auto flex-1 p-0">
            <Table className="table-compact">
              <TableHeader className="sticky top-0 bg-card z-10">
                <TableRow>
                  <TableHead className="w-20">Ticker</TableHead>
                  <TableHead className="text-right">Base Wt%</TableHead>
                  <TableHead className="w-10"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {basket.map((ticker) => {
                  const fw = weightMap[ticker];
                  return (
                    <TableRow key={ticker} className="hover:bg-muted/50 group">
                      <TableCell className="font-bold text-foreground">{ticker}</TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {isComputing ? (
                          <span className="text-muted-foreground/40">—</span>
                        ) : fw !== undefined ? (
                          <span className={cn(fw > 0.20 ? "text-amber-400" : "text-primary")}>
                            {formatPercent(fw, 1)}
                          </span>
                        ) : (
                          <span className="text-muted-foreground/40">—</span>
                        )}
                      </TableCell>
                      <TableCell className="p-0 text-center">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-6 w-6 rounded-sm text-muted-foreground hover:text-destructive opacity-0 group-hover:opacity-100 transition-opacity"
                          onClick={() => removeFromBasket(ticker)}
                        >
                          <Trash2 className="w-3 h-3" />
                        </Button>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>

          <div className="px-4 py-3 border-t border-border/50 bg-muted/10">
            <AuditLine
              riskData={riskData}
              isComputing={isComputing}
              hasError={hasError}
              isEngineDown={isEngineDown}
              requestedMethod={weightingMethod}
            />
          </div>
        </Card>
      </div>

      {/* RIGHT PANEL: Risk Metrics */}
      <div className="flex-1 flex flex-col gap-3 lg:overflow-hidden lg:h-full">
        <h2 className="text-base font-semibold text-muted-foreground hidden lg:block">Risk Analysis</h2>

        {hasError && !riskData ? (
          <div className="flex-1 flex items-center justify-center border border-dashed rounded-xl bg-card/20 min-h-[120px] lg:min-h-0 border-destructive/30">
            <div className="flex flex-col items-center gap-3 p-6 text-center">
              {isEngineDown ? (
                <>
                  <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
                  <p className="text-sm text-muted-foreground">Engine is starting up — data loads in a few seconds.</p>
                  <button
                    onClick={triggerCompute}
                    className="text-xs text-primary underline underline-offset-2 hover:opacity-80"
                  >
                    Retry now
                  </button>
                </>
              ) : (
                <>
                  <AlertTriangle className="w-6 h-6 text-destructive/70" />
                  <p className="text-sm text-destructive">Failed to compute risk.</p>
                  <button
                    onClick={triggerCompute}
                    className="text-xs text-primary underline underline-offset-2 hover:opacity-80"
                  >
                    Retry
                  </button>
                </>
              )}
            </div>
          </div>
        ) : !riskData ? (
          <div className="flex-1 flex items-center justify-center border border-border border-dashed rounded-xl bg-card/20 text-muted-foreground min-h-[120px] lg:min-h-0">
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
          <div className="lg:flex-1 lg:overflow-auto space-y-3 lg:pr-2 pb-[max(env(safe-area-inset-bottom,0px),1.5rem)]">

            {/* ── A. Construction strip ─────────────────────────────────── */}
            <div className="bg-card border border-border rounded-lg px-4 py-3">
              <p className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-2">Construction</p>
              <div className="flex flex-wrap gap-x-5 gap-y-1.5 text-[12px]">
                <Stat label="Method" value={METHOD_LABELS[riskData.method] ?? riskData.method} />
                <Stat label="Port Vol" value={formatPercent(riskData.portfolioVol, 1)} highlight />
                <Stat label="Target" value={formatPercent(VOL_TARGET, 0)} />
                <Stat label="Scale" value={`×${formatNumber(riskData.volTargetMultiplier, 2)}`} />
                <Stat label="Max pos" value={formatPercent(maxPosition, 1)} />
                <Stat label="Names" value={String(riskData.numHoldings)} />
              </div>
            </div>

            {/* ── B. Health metrics grid ────────────────────────────────── */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <KpiCard label="Avg Corr" value={formatNumber(riskData.avgCorrelation, 2)} />
              <KpiCard
                label="Names at Cap"
                value={String((riskData.namesCapped ?? []).length)}
                sub={(riskData.namesCapped ?? []).length > 0 ? "at 15% limit" : "none at cap"}
              />
              <KpiCard
                label="Groups"
                value={String(groupStats.numGroups)}
                sub={groupStats.topGroup != null ? `top: G${groupStats.topGroup} ${formatPercent(groupStats.topWeight, 0)}` : undefined}
              />
              {sectorStats.valid ? (
                <KpiCard
                  label="Sectors"
                  value={String(sectorStats.numSectors)}
                  sub={sectorStats.topSector ? `top: ${sectorStats.topSector.slice(0, 14)}` : undefined}
                />
              ) : (
                <KpiCard label="Sectors" value="—" sub="data pending" muted />
              )}
            </div>

            {/* ── C. Group exposure ─────────────────────────────────────── */}
            {groupStats.breakdown.length > 0 && (
              <BreakdownCard
                title="Group Exposure"
                description="Base weight by momentum / quality group · top 3 shown"
                rows={groupStats.breakdown.slice(0, 3)}
                otherWeight={groupStats.breakdown.slice(3).reduce((s, r) => s + r.weight, 0)}
                otherCount={groupStats.breakdown.slice(3).reduce((s, r) => s + r.count, 0)}
                labelFn={(r) => `G${(r as { cluster: number }).cluster}`}
                maxWeight={groupStats.breakdown[0]?.weight ?? 1}
              />
            )}

            {/* ── D. Sector concentration (only if mapped) ──────────────── */}
            {sectorStats.valid && sectorStats.breakdown.length > 0 && (
              <BreakdownCard
                title="Sector Concentration"
                description="Base weight allocated per sector · top 3 shown"
                rows={sectorStats.breakdown.slice(0, 3)}
                otherWeight={sectorStats.breakdown.slice(3).reduce((s, r) => s + r.weight, 0)}
                otherCount={sectorStats.breakdown.slice(3).reduce((s, r) => s + r.count, 0)}
                labelFn={(r) => r.sector}
                maxWeight={sectorStats.breakdown[0]?.weight ?? 1}
              />
            )}

            {/* ── E. Constituent table ──────────────────────────────────── */}
            <Card className="bg-card border-border">
              <CardHeader className="p-4">
                <CardTitle className="text-sm">Constituent Weights &amp; Risk</CardTitle>
                <CardDescription>
                  Base wt sums to 100% · scaled ×{formatNumber(riskData.volTargetMultiplier, 2)} to {formatPercent(VOL_TARGET, 0)} vol target ·{" "}
                  {riskData.sgovWeight > 0.001
                    ? `${formatPercent(riskData.sgovWeight, 1)} in SGOV / cash`
                    : "fully invested in equity"}{" "}
                  · no leverage
                </CardDescription>
              </CardHeader>
              <CardContent className="p-0">
                <Table className="table-compact">
                  <TableHeader>
                    <TableRow>
                      <TableHead>Ticker</TableHead>
                      <TableHead className="text-right">Base Wt%</TableHead>
                      <TableHead className="text-right">Risk%</TableHead>
                      <TableHead className="text-right">Ann. Vol</TableHead>
                      <TableHead className="text-center">Grp</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {[...riskData.holdings].sort((a, b) => b.baseWeight - a.baseWeight).map((h) => (
                      <TableRow key={h.ticker}>
                        <TableCell className="font-bold">{h.ticker}</TableCell>
                        <TableCell className="text-right font-mono text-primary">
                          {formatPercent(h.baseWeight, 1)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-muted-foreground">
                          {formatPercent(h.riskContrib, 1)}
                        </TableCell>
                        <TableCell className="text-right text-muted-foreground">
                          {formatPercent(h.vol, 1)}
                        </TableCell>
                        <TableCell className="text-center text-muted-foreground">
                          {h.cluster != null ? `G${h.cluster}` : "—"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>

            {/* ── F. Diversify Suggestions ──────────────────────────────── */}
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

function Stat({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <span>
      <span className="text-muted-foreground">{label} </span>
      <span className={cn("font-medium", highlight ? "text-primary" : "text-foreground")}>{value}</span>
    </span>
  );
}

function KpiCard({ label, value, sub, muted }: { label: string; value: string; sub?: string; muted?: boolean }) {
  return (
    <Card className="bg-card">
      <CardHeader className="pb-1 p-3">
        <CardTitle className="text-[10px] text-muted-foreground uppercase tracking-wider font-normal">{label}</CardTitle>
      </CardHeader>
      <CardContent className="p-3 pt-0">
        <div className={cn("text-xl font-bold font-mono", muted ? "text-muted-foreground/40" : "text-foreground")}>{value}</div>
        {sub && <div className="text-[10px] text-muted-foreground mt-0.5">{sub}</div>}
      </CardContent>
    </Card>
  );
}

function BreakdownCard<T extends { weight: number; count: number }>({
  title,
  description,
  rows,
  otherWeight,
  otherCount,
  labelFn,
  maxWeight,
}: {
  title: string;
  description: string;
  rows: T[];
  otherWeight: number;
  otherCount: number;
  labelFn: (row: T) => string;
  maxWeight: number;
}) {
  const barMax = Math.max(maxWeight, 0.001);
  return (
    <Card className="bg-card border-border">
      <CardHeader className="p-4 pb-2">
        <CardTitle className="text-sm">{title}</CardTitle>
        <CardDescription className="text-[11px]">{description}</CardDescription>
      </CardHeader>
      <CardContent className="p-4 pt-0 space-y-2">
        {rows.map((row) => (
          <div key={labelFn(row)} className="flex items-center gap-3">
            <div className="w-24 text-xs font-mono text-foreground/80 truncate shrink-0">{labelFn(row)}</div>
            <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
              <div
                className="h-full bg-primary/70 rounded-full transition-all"
                style={{ width: `${(row.weight / barMax) * 100}%` }}
              />
            </div>
            <div className="w-12 text-right text-xs font-mono text-primary">{formatPercent(row.weight, 1)}</div>
            <div className="w-10 text-right text-[11px] text-muted-foreground">{row.count}n</div>
          </div>
        ))}
        {otherWeight > 0.001 && (
          <div className="flex items-center gap-3">
            <div className="w-24 text-xs font-mono text-muted-foreground/60 shrink-0">Other</div>
            <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
              <div
                className="h-full bg-muted-foreground/30 rounded-full"
                style={{ width: `${(otherWeight / barMax) * 100}%` }}
              />
            </div>
            <div className="w-12 text-right text-xs font-mono text-muted-foreground">{formatPercent(otherWeight, 1)}</div>
            <div className="w-10 text-right text-[11px] text-muted-foreground">{otherCount}n</div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function DiversifyCard({
  suggestions,
  suggestMode,
  onModeChange,
  onAdd,
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
              <button
                key={m.value}
                onClick={() => onModeChange(m.value)}
                className={cn(
                  "px-2.5 py-1 text-[11px] font-medium transition-colors",
                  suggestMode === m.value
                    ? "bg-primary text-primary-foreground"
                    : "bg-transparent text-muted-foreground hover:text-foreground"
                )}
              >
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
            const alphaStr = `${alphaSign}${s.alpha.toFixed(2)}`;
            const alphaColor = s.alpha >= 0 ? "text-emerald-400" : "text-rose-400";
            const groupLabel = s.group === -1 ? "—" : `G${s.group}`;
            const viaBadge =
              s.via === "both"
                ? "Grp+Sec"
                : s.via === "group"
                ? `Grp ${formatPercent(s.groupShare, 0)}`
                : `Sec ${formatPercent(s.sectorShare, 0)}`;
            const viaBadgeColor =
              s.via === "both"
                ? "text-amber-400/80"
                : s.via === "group"
                ? "text-sky-400/80"
                : "text-violet-400/80";

            return (
              <div key={s.ticker} className="flex items-center gap-2.5 px-4 py-2.5 hover:bg-muted/30 transition-colors">
                {/* Left: ticker + alpha */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-1.5">
                    <span className="font-bold text-sm text-foreground tracking-tight">{s.ticker}</span>
                    <span className={cn("font-mono text-xs font-semibold", alphaColor)}>{alphaStr}</span>
                    <span className={cn("text-[10px] font-medium", viaBadgeColor)}>{viaBadge}</span>
                  </div>
                  <div className="flex items-center gap-1.5 mt-0.5">
                    <span className="text-[11px] text-muted-foreground truncate max-w-[120px]">{s.sector}</span>
                    <span className="text-[10px] text-muted-foreground/50">·</span>
                    <span className="text-[11px] text-muted-foreground/70 font-mono shrink-0">{groupLabel}</span>
                  </div>
                </div>
                {/* Right: Add button */}
                <button
                  onClick={() => onAdd(s.ticker)}
                  className="flex items-center gap-1 px-2.5 py-1 rounded-md bg-primary/10 hover:bg-primary/20 text-primary text-[11px] font-semibold transition-colors shrink-0"
                >
                  <Plus className="w-3 h-3" />
                  Add
                </button>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}

function AuditLine({
  riskData,
  isComputing,
  hasError,
  isEngineDown,
  requestedMethod,
}: {
  riskData: ReturnType<typeof useComputePortfolioRisk>["data"];
  isComputing: boolean;
  hasError: boolean;
  isEngineDown?: boolean;
  requestedMethod: string;
}) {
  if (isComputing) {
    return (
      <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground/60">
        <Loader2 className="w-3 h-3 animate-spin" />
        <span>Computing…</span>
      </div>
    );
  }

  if (hasError && !riskData) {
    return isEngineDown ? (
      <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground/60">
        <Loader2 className="w-3 h-3 animate-spin" />
        <span>Engine starting…</span>
      </div>
    ) : (
      <div className="flex items-center gap-1.5 text-[11px] text-destructive/80">
        <AlertTriangle className="w-3 h-3" />
        <span>Computation failed</span>
      </div>
    );
  }

  if (!riskData) {
    return (
      <div className="text-[11px] text-muted-foreground/50">
        Audit: waiting for first compute
      </div>
    );
  }

  const methodLabel = METHOD_LABELS[riskData.method] ?? riskData.method;
  const covModel = riskData.covModel;

  return (
    <div className="space-y-1">
      <div className="font-mono text-[10px] text-muted-foreground leading-relaxed">
        <span className="text-foreground/70 font-semibold">{methodLabel}</span>
        {covModel && <> · <span className="text-foreground/60">{covModel}</span></>}
        {" · "}base vol <span className="text-foreground/80">{formatPercent(riskData.basePortVol, 1)}</span>
        {" · "}×<span className="text-foreground/80">{formatNumber(riskData.volTargetMultiplier, 2)}</span>
        {" → "}<span className="text-foreground/80">{formatPercent(riskData.portfolioVol, 1)}</span>
        {" target "}<span className="text-foreground/80">{formatPercent(VOL_TARGET, 0)}</span>
      </div>
      {riskData.method !== requestedMethod && (
        <p className="text-[10px] text-amber-400/80">
          ⚠ Returned method ({riskData.method}) differs from requested ({requestedMethod})
        </p>
      )}
    </div>
  );
}
