import React, { useState, useMemo, useEffect, useCallback, useRef } from "react";
import {
  useGetDataStatus,
  useGetRankings,
  Stock,
  GetRankingsParams,
} from "@workspace/api-client-react";
import { useQueryClient } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { usePortfolio } from "@/hooks/use-portfolio";
import {
  loadRankingsCache,
  saveRankingsCache,
  clearRankingsCache,
  formatCacheAge,
  CachedRankings,
} from "@/hooks/use-rankings-cache";
import {
  useColumnConfig,
  ColumnId,
  COLUMN_LABELS,
  ALL_COLUMN_IDS,
} from "@/hooks/use-column-config";
import { formatNumber, formatPercent, formatCompactCurrency, formatCurrency, cn } from "@/lib/utils";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Slider } from "@/components/ui/slider";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import {
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Search,
  Plus,
  Check,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  SlidersHorizontal,
  RefreshCw,
  Loader2,
  Columns3,
  RotateCcw,
  X,
} from "lucide-react";

type SortField = keyof Stock;
type SortDirection = "asc" | "desc";

const CLUSTER_COLORS = [
  "bg-rose-500/10 text-rose-500 border-rose-500/20",
  "bg-blue-500/10 text-blue-500 border-blue-500/20",
  "bg-emerald-500/10 text-emerald-500 border-emerald-500/20",
  "bg-amber-500/10 text-amber-500 border-amber-500/20",
  "bg-violet-500/10 text-violet-500 border-violet-500/20",
  "bg-cyan-500/10 text-cyan-500 border-cyan-500/20",
  "bg-orange-500/10 text-orange-500 border-orange-500/20",
  "bg-fuchsia-500/10 text-fuchsia-500 border-fuchsia-500/20",
  "bg-lime-500/10 text-lime-500 border-lime-500/20",
  "bg-sky-500/10 text-sky-500 border-sky-500/20",
];

const CLUSTER_DOT_COLORS = [
  "bg-rose-500",
  "bg-blue-500",
  "bg-emerald-500",
  "bg-amber-500",
  "bg-violet-500",
  "bg-cyan-500",
  "bg-orange-500",
  "bg-fuchsia-500",
  "bg-lime-500",
  "bg-sky-500",
];

const CLUSTER_TEXT_COLORS = [
  "text-rose-500",
  "text-blue-500",
  "text-emerald-500",
  "text-amber-500",
  "text-violet-500",
  "text-cyan-500",
  "text-orange-500",
  "text-fuchsia-500",
  "text-lime-500",
  "text-sky-500",
];

type McapFilter = "all" | "no_small" | "large_only";
const MCAP_THRESHOLDS: Record<McapFilter, number | null> = {
  all:        null,
  no_small:   2_000_000_000,    // Exclude Small Caps ≈ $2B+
  large_only: 10_000_000_000,   // Large Caps Only   ≈ $10B+
};
const MCAP_LABELS: Record<McapFilter, string> = {
  all:        "All",
  no_small:   "≥$2B",
  large_only: "≥$10B",
};

const SECTOR_ABBR: Record<string, string> = {
  "Information Technology": "Tech",
  "Technology": "Tech",
  "Health Care": "HC",
  "Healthcare": "HC",
  "Financials": "Fin",
  "Financial Services": "Fin",
  "Consumer Discretionary": "Cons Disc",
  "Consumer Cyclical": "Cyc",
  "Consumer Staples": "Cons Stap",
  "Consumer Defensive": "Def",
  "Telecommunications": "Telecom",
  "Communication Services": "Comm",
  "Industrials": "Ind",
  "Energy": "En",
  "Utilities": "Util",
  "Materials": "Mat",
  "Basic Materials": "Mat",
  "Real Estate": "RE",
};

/**
 * Maps an alpha percentile [0=lowest, 1=highest] to a dark-mode-safe CSS color.
 * Neutral zone ±0.3 from center stays gray; tails get diverging green/red with
 * intensity proportional to extremeness (power-curved so middle stays quiet).
 */
function getAlphaColor(p: number): string {
  const dev = (p - 0.5) * 2; // -1..+1
  const absdev = Math.abs(dev);
  const THRESHOLD = 0.3; // inner ±30% = neutral gray zone (~60% of stocks)
  if (absdev < THRESHOLD) return "hsl(220, 6%, 46%)";
  // Remap outer region to [0..1] with slight power curve
  const t = Math.pow((absdev - THRESHOLD) / (1 - THRESHOLD), 1.1);
  if (dev > 0) {
    // Positive alpha — green
    const sat = Math.round(t * 62);
    const lum = Math.round(58 - t * 7);
    return `hsl(142, ${sat}%, ${lum}%)`;
  } else {
    // Negative alpha — red (slight orange shift at moderate, pure red at extreme)
    const hue = Math.round(6 - t * 6);
    const sat = Math.round(t * 60);
    const lum = Math.round(58 - t * 6);
    return `hsl(${hue}, ${sat}%, ${lum}%)`;
  }
}

export default function MainPage() {
  const { basket, basketSet, addToBasket, removeFromBasket, setAllStocks, setRankedStocks } = usePortfolio();
  const { config, orderedVisible, toggleColumn, moveColumn, resetColumns } = useColumnConfig();
  const hiddenColumns = ALL_COLUMN_IDS.filter(id => !config.visible.includes(id));

  const queryClient = useQueryClient();

  // ── localStorage snapshot (warm-start) ──────────────────────────────────
  // Read once at mount; never mutated — fresh API data replaces it atomically.
  const [localCache] = useState<CachedRankings | null>(() => loadRankingsCache());

  const { data: statusData } = useGetDataStatus({
    query: {
      refetchInterval: (query) => {
        const d = query.state.data;
        if (!d || d.status !== "ready") return 5000;
        return false;
      },
    },
  });

  const isReady = statusData?.status === "ready";

  // Factor controls — split into server-bound (debounced) and local (instant) params
  const [controlsOpen, setControlsOpen] = useState(false);
  const [colsOpen, setColsOpen] = useState(false);
  const [recentlyMoved, setRecentlyMoved] = useState<ColumnId | null>(null);
  const movedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleMove = useCallback((id: ColumnId, dir: "up" | "down") => {
    moveColumn(id, dir);
    if (movedTimerRef.current) clearTimeout(movedTimerRef.current);
    setRecentlyMoved(id);
    movedTimerRef.current = setTimeout(() => setRecentlyMoved(null), 600);
  }, [moveColumn]);

  const CONTROLS_KEY = "qt:controls-v3";

  const loadControlsFromStorage = () => {
    try {
      const raw = localStorage.getItem(CONTROLS_KEY);
      if (raw) return JSON.parse(raw);
    } catch {}
    return null;
  };

  // Server params: only these trigger an API call (debounced)
  const [serverParams, setServerParams] = useState(() => {
    const saved = loadControlsFromStorage();
    return {
      volFloor: saved?.volFloor ?? 0.05,
      winsorP: saved?.winsorP ?? 2,
      clusterN: saved?.clusterN ?? 100,
      clusterK: saved?.clusterK ?? 10,
      clusterLookback: saved?.clusterLookback ?? 252,
    };
  });
  const [debouncedServerParams, setDebouncedServerParams] = useState(serverParams);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedServerParams(serverParams), 400);
    return () => clearTimeout(timer);
  }, [serverParams]);

  // Local params: handled client-side (instant, no API call)
  const [localW6, setLocalW6] = useState(() => {
    const saved = loadControlsFromStorage();
    return saved?.localW6 ?? 0.5;
  });
  const [localW12, setLocalW12] = useState(() => {
    const saved = loadControlsFromStorage();
    return saved?.localW12 ?? 0.5;
  });
  const [mcapFilter, setMcapFilter] = useState<McapFilter>(() => {
    const saved = loadControlsFromStorage();
    const v = saved?.mcapFilter;
    return (v === "no_small" || v === "large_only") ? v : "all";
  });

  // UI state — persisted together with controls in CONTROLS_KEY
  const [sortField, setSortField] = useState<SortField>(() => {
    const s = loadControlsFromStorage();
    return typeof s?.sortField === "string" ? s.sortField as SortField : "alpha";
  });
  const [sortDir, setSortDirection] = useState<SortDirection>(() => {
    const s = loadControlsFromStorage();
    return s?.sortDir === "asc" ? "asc" : "desc";
  });
  const [alphaMode, setAlphaMode] = useState<'z' | 'pct'>(() => {
    const s = loadControlsFromStorage();
    return s?.alphaMode === 'pct' ? 'pct' : 'z';
  });

  // Persist controls to localStorage whenever they change
  useEffect(() => {
    try {
      localStorage.setItem(CONTROLS_KEY, JSON.stringify({ ...serverParams, localW6, localW12, mcapFilter, sortField, sortDir, alphaMode }));
    } catch {}
  }, [serverParams, localW6, localW12, mcapFilter, sortField, sortDir, alphaMode]);

  const params: GetRankingsParams = useMemo(() => {
    const p: GetRankingsParams = {
      volAdjust: true,
      useTstats: false,
      w6: 0.5,
      w12: 0.5,
      volFloor: debouncedServerParams.volFloor,
      winsorP: debouncedServerParams.winsorP,
      clusterN: debouncedServerParams.clusterN,
      clusterK: debouncedServerParams.clusterK,
      clusterLookback: debouncedServerParams.clusterLookback,
    };
    return p;
  }, [debouncedServerParams]);

  // Session-only UI state (not persisted)
  const [search, setSearch] = useState("");
  const [expandedTicker, setExpandedTicker] = useState<string | null>(null);

  // Fetch rankings once engine is ready — staleTime matches engine 8-hour cache
  const { data: rankingsData, isFetching: isRankingsLoading } = useGetRankings(params, {
    query: {
      enabled: isReady,
      staleTime: 8 * 60 * 60 * 1000,
      gcTime: 8 * 60 * 60 * 1000,
    },
  });

  const rankingsResult = rankingsData && "stocks" in rankingsData ? rankingsData : null;
  const freshStocks = rankingsResult?.stocks || [];
  const audit = rankingsResult?.audit;

  // Persist fresh API data to localStorage so next startup is instant
  useEffect(() => {
    if (freshStocks.length > 0 && rankingsResult) {
      saveRankingsCache(
        { stocks: freshStocks, total: rankingsResult.total ?? freshStocks.length, cachedAt: rankingsResult.cachedAt },
        JSON.stringify(params),
      );
    }
  }, [freshStocks, rankingsResult, params]);

  // Stale-while-revalidate: show fresh data if available, fall back to localStorage snapshot
  const stocks = freshStocks.length > 0 ? freshStocks : (localCache?.stocks || []);
  const isShowingCachedData = freshStocks.length === 0 && (localCache?.stocks?.length ?? 0) > 0;
  // True cold start: no localStorage and engine not ready yet
  const isColdStart = stocks.length === 0 && !isReady;

  // Manual refresh: clear localStorage + invalidate React Query so rankings re-fetch immediately
  const handleRefresh = useCallback(() => {
    clearRankingsCache();
    queryClient.invalidateQueries({ queryKey: ["/api/equity/rankings"] });
  }, [queryClient]);

  useEffect(() => {
    if (stocks.length > 0) setAllStocks(stocks);
    if (stocks.length > 0) {
      // Inline alpha rerank (mirrors clientAlphaStocks) — seeds the portfolio context
      // with the current ranked+filtered universe without a forward ref to the useMemo below.
      const wS = localW6, wT = localW12;
      const totalW = (wS + wT) || 1;
      const reranked = stocks
        .map((s) => {
          const alpha = (wS * ((s as any).sSleeve ?? 0) + wT * ((s as any).tSleeve ?? 0)) / totalW;
          return { ...s, alpha };
        })
        .sort((a, b) => (b.alpha ?? 0) - (a.alpha ?? 0));
      const threshold = MCAP_THRESHOLDS[mcapFilter];
      setRankedStocks(
        threshold !== null
          ? reranked.filter((s) => s.marketCap == null || s.marketCap >= threshold)
          : reranked
      );
    }
  }, [stocks, localW6, localW12, mcapFilter, setAllStocks, setRankedStocks]);

  const scrollContainerRef = useRef<HTMLDivElement>(null);

  const clientAlphaStocks: Stock[] = useMemo(() => {
    if (!stocks.length) return stocks;
    const wS = localW6;
    const wT = localW12;
    const totalW = (wS + wT) || 1;

    const reranked = stocks.map((s: Stock) => {
      const alpha = (wS * ((s as any).sSleeve ?? 0) + wT * ((s as any).tSleeve ?? 0)) / totalW;
      return { ...s, alpha };
    });

    reranked.sort((a: Stock, b: Stock) => (b.alpha ?? 0) - (a.alpha ?? 0));
    return reranked.map((s: Stock, i: number) => ({
      ...s,
      rank: i + 1,
      percentile: 100 * (1 - i / reranked.length),
    }));
  }, [stocks, localW6, localW12]);

  // Filtering and Sorting
  const processedStocks = useMemo(() => {
    let result = [...clientAlphaStocks];

    const mcapThreshold = MCAP_THRESHOLDS[mcapFilter];
    if (mcapThreshold !== null) {
      result = result.filter((s) => s.marketCap == null || s.marketCap >= mcapThreshold);
    }

    if (search) {
      const q = search.toLowerCase();
      result = result.filter(
        (s) => s.ticker.toLowerCase().includes(q) || s.name.toLowerCase().includes(q)
      );
    }

    if (sortField && sortDir) {
      result.sort((a, b) => {
        // Cluster: primary = cluster number, secondary = alpha descending within each cluster
        if (sortField === "cluster") {
          const ca = a.cluster ?? (sortDir === "asc" ? Infinity : -Infinity);
          const cb = b.cluster ?? (sortDir === "asc" ? Infinity : -Infinity);
          if (ca !== cb) return sortDir === "asc" ? ca - cb : cb - ca;
          // Secondary: alpha descending (best names first inside each cluster)
          return (b.alpha ?? 0) - (a.alpha ?? 0);
        }

        const valA = a[sortField];
        const valB = b[sortField];

        if (valA === null || valA === undefined) return 1;
        if (valB === null || valB === undefined) return -1;

        if (valA < valB) return sortDir === "asc" ? -1 : 1;
        if (valA > valB) return sortDir === "asc" ? 1 : -1;
        return 0;
      });
    }

    return result;
  }, [clientAlphaStocks, mcapFilter, search, sortField, sortDir]);

  const virtualizer = useVirtualizer({
    count: processedStocks.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: () => (typeof window !== "undefined" && window.innerWidth < 1024 ? 60 : 32),
    overscan: 20,
  });

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection((prev) => (prev === "desc" ? "asc" : "desc"));
    } else {
      setSortField(field);
      setSortDirection("desc");
    }
  };

  const getSortIcon = (field: SortField) => {
    if (sortField !== field) return <ArrowUpDown className="w-3 h-3 ml-1 opacity-20" />;
    return sortDir === "asc"
      ? <ArrowUp className="w-3 h-3 ml-1" />
      : <ArrowDown className="w-3 h-3 ml-1" />;
  };

  const handleServerParamChange = (key: string, value: unknown) => {
    setServerParams((prev) => ({ ...prev, [key]: value }));
  };

  const portfolioSet = basketSet;

  // Alpha percentile map — relative to current visible universe (no hook, plain IIFE)
  // Computed here so renderCell can close over it.
  const alphaPercentileMap: Map<string, number> = (() => {
    const withAlpha = processedStocks.filter(s => s.alpha != null);
    const sorted = [...withAlpha].sort((a, b) => (a.alpha ?? 0) - (b.alpha ?? 0));
    const n = sorted.length;
    const map = new Map<string, number>();
    sorted.forEach((s, i) => {
      map.set(s.ticker, n > 1 ? i / (n - 1) : 0.5);
    });
    return map;
  })();

  // ─── Loading state ────────────────────────────────────────────────────────
  if (!isReady) {
    const progress = statusData?.progress ? statusData.progress * 100 : 0;
    return (
      <div className="flex flex-col items-center justify-center min-h-[calc(100vh-3rem)] lg:min-h-screen p-4 text-center">
        <div className="w-full max-w-sm space-y-4 bg-card p-6 rounded-xl border border-border shadow-lg">
          <div className="w-10 h-10 rounded-full bg-primary/20 flex items-center justify-center mx-auto">
            <RefreshCw className="w-5 h-5 text-primary animate-spin" />
          </div>
          <h2 className="text-xl font-bold tracking-tight text-foreground">Initializing Quant Engine</h2>
          <p className="text-muted-foreground text-sm leading-relaxed">
            {statusData?.message || "Fetching price data from Yahoo Finance..."}
          </p>
          <div className="space-y-2 pt-2">
            <Progress value={progress} className="h-2" />
            <div className="flex justify-between text-xs text-muted-foreground font-mono">
              <span>{formatNumber(progress, 0)}%</span>
              <span>{statusData?.loaded || 0} / {statusData?.total || "~700"} equities</span>
            </div>
          </div>
        </div>
      </div>
    );
  }

  const activeColumns = orderedVisible;

  // ─── Column header renderer ────────────────────────────────────────────────
  const renderHeader = (colId: ColumnId) => {
    switch (colId) {
      case "rank":
        return (
          <TableHead key={colId} className="w-8 text-center cursor-pointer hover:text-foreground" onClick={() => handleSort("rank")}>
            <div className="flex items-center justify-center">Rank {getSortIcon("rank")}</div>
          </TableHead>
        );
      case "name":
        return (
          <TableHead key={colId} className="w-40 cursor-pointer hover:text-foreground" onClick={() => handleSort("name")}>
            <div className="flex items-center">Name {getSortIcon("name")}</div>
          </TableHead>
        );
      case "sector":
        return (
          <TableHead key={colId} className="w-24 cursor-pointer hover:text-foreground" onClick={() => handleSort("sector")}>
            <div className="flex items-center">Sector {getSortIcon("sector")}</div>
          </TableHead>
        );
      case "price":
        return (
          <TableHead key={colId} className="text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("price")}>
            <div className="flex items-center justify-end">Price {getSortIcon("price")}</div>
          </TableHead>
        );
      case "marketCap":
        return (
          <TableHead key={colId} className="text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("marketCap")}>
            <div className="flex items-center justify-end">Mkt Cap {getSortIcon("marketCap")}</div>
          </TableHead>
        );
      case "adv":
        return (
          <TableHead key={colId} className="text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("adv")}>
            <div className="flex items-center justify-end">ADV {getSortIcon("adv")}</div>
          </TableHead>
        );
      case "momentum6":
        return (
          <TableHead key={colId} className="text-right bg-blue-950/20 cursor-pointer hover:text-foreground" onClick={() => handleSort("s6")}>
            <div className="flex items-center justify-end" title="S Sleeve — 6M Sharpe (S6)">
              S6 {getSortIcon("s6")}
            </div>
          </TableHead>
        );
      case "momentum12":
        return (
          <TableHead key={colId} className="text-right bg-blue-950/20 cursor-pointer hover:text-foreground" onClick={() => handleSort("s12")}>
            <div className="flex items-center justify-end" title="S Sleeve — 12M Sharpe (S12)">
              S12 {getSortIcon("s12")}
            </div>
          </TableHead>
        );
      case "vol12":
        return (
          <TableHead key={colId} className="text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("sigma12")}>
            <div className="flex items-center justify-end" title="12-Month Volatility">
              Vol(12m) {getSortIcon("sigma12")}
            </div>
          </TableHead>
        );
      case "alpha":
        return (
          <TableHead key={colId} className="text-right bg-emerald-950/20 font-bold cursor-pointer hover:text-foreground" onClick={() => handleSort("alpha")}>
            <div className="flex items-center justify-end gap-1 text-emerald-500">
              {alphaMode === 'pct' ? 'Pctl' : 'Alpha'} {getSortIcon("alpha")}
            </div>
          </TableHead>
        );
      case "cluster":
        return (
          <TableHead key={colId} className="text-center cursor-pointer hover:text-foreground" onClick={() => handleSort("cluster")}>
            <div className="flex items-center justify-center text-xs">Grp {getSortIcon("cluster")}</div>
          </TableHead>
        );
    }
  };

  // ─── Column cell renderer ──────────────────────────────────────────────────
  const renderCell = (colId: ColumnId, stock: Stock, badgeColor: string, alphaP?: number) => {
    switch (colId) {
      case "rank":
        return (
          <TableCell key={colId} className="text-center text-muted-foreground font-mono">
            {stock.rank}
          </TableCell>
        );
      case "name":
        return (
          <TableCell key={colId} className="text-muted-foreground truncate max-w-[150px]" title={stock.name}>
            {stock.name}
          </TableCell>
        );
      case "sector":
        return (
          <TableCell key={colId} className="text-muted-foreground text-[10px]">
            {stock.sector ? (SECTOR_ABBR[stock.sector] ?? stock.sector) : "—"}
          </TableCell>
        );
      case "price":
        return <TableCell key={colId} className="text-right">{formatCurrency(stock.price)}</TableCell>;
      case "marketCap":
        return <TableCell key={colId} className="text-right text-muted-foreground">{formatCompactCurrency(stock.marketCap)}</TableCell>;
      case "adv":
        return <TableCell key={colId} className="text-right text-muted-foreground">{formatCompactCurrency(stock.adv)}</TableCell>;
      case "momentum6":
        return (
          <TableCell key={colId} className={cn("text-right bg-blue-950/10", (stock.s6 ?? 0) > 0 ? "text-positive" : "text-negative")}>
            {formatNumber(stock.s6)}
          </TableCell>
        );
      case "momentum12":
        return (
          <TableCell key={colId} className={cn("text-right bg-blue-950/10", (stock.s12 ?? 0) > 0 ? "text-positive" : "text-negative")}>
            {formatNumber(stock.s12)}
          </TableCell>
        );
      case "vol12":
        return <TableCell key={colId} className="text-right text-muted-foreground">{formatPercent(stock.sigma12)}</TableCell>;
      case "alpha":
        return (
          <TableCell key={colId} className="text-right font-bold bg-emerald-950/10"
            style={{ color: getAlphaColor(alphaP ?? 0.5) }}>
            {alphaMode === 'pct'
              ? (stock.percentile != null ? stock.percentile.toFixed(1) + "%" : "—")
              : formatNumber(stock.alpha)}
          </TableCell>
        );
      case "cluster":
        return (
          <TableCell key={colId} className="text-center p-1">
            {stock.cluster !== null && stock.cluster !== undefined ? (
              <Badge variant="outline" className={cn("text-[9px] px-1 py-0 h-4 border-opacity-30 rounded-sm font-mono", badgeColor)}>
                G{stock.cluster}
              </Badge>
            ) : "—"}
          </TableCell>
        );
    }
  };

  // ─── Main render ──────────────────────────────────────────────────────────
  // Active filter chips — computed inline (O(5), no useMemo needed)
  const activeFilterChips: string[] = [];
  if (mcapFilter === "no_small") activeFilterChips.push("≥$2B");
  if (mcapFilter === "large_only") activeFilterChips.push("≥$10B");

  // Suggested % for top-25: alpha/vol normalized weights (no hook — plain IIFE)
  const suggestedWeights: Map<string, number> = (() => {
    const VOL_FLOOR = 0.10; // floor at 10% annualized vol to cap extreme weights
    const top25 = clientAlphaStocks
      .filter(s => s.rank != null && s.rank <= 25 && s.alpha != null && s.sigma12 != null)
      .sort((a, b) => (a.rank ?? 999) - (b.rank ?? 999))
      .slice(0, 25);
    const scores = top25.map(s => ({
      ticker: s.ticker,
      score: Math.max(s.alpha ?? 0, 0) / Math.max(s.sigma12 ?? VOL_FLOOR, VOL_FLOOR),
    }));
    const total = scores.reduce((acc, s) => acc + s.score, 0);
    const map = new Map<string, number>();
    if (total > 0) {
      for (const { ticker, score } of scores) {
        map.set(ticker, score / total);
      }
    }
    return map;
  })();

  // Rank-within-group: plain IIFE const (no hook) — full pre-filter universe
  const clusterRankMap: Map<string, { rankInGroup: number; groupSize: number }> = (() => {
    const byCluster = new Map<number, { ticker: string; rank: number }[]>();
    for (const s of clientAlphaStocks) {
      if (s.cluster == null) continue;
      if (!byCluster.has(s.cluster)) byCluster.set(s.cluster, []);
      byCluster.get(s.cluster)!.push({ ticker: s.ticker, rank: s.rank ?? Infinity });
    }
    const map = new Map<string, { rankInGroup: number; groupSize: number }>();
    for (const [, items] of byCluster) {
      items.sort((a, b) => a.rank - b.rank);
      const size = items.length;
      items.forEach((item, idx) => {
        map.set(item.ticker, { rankInGroup: idx + 1, groupSize: size });
      });
    }
    return map;
  })();

  return (
    <div className="flex flex-col h-full overflow-hidden">

      {/* ── Sticky header shell ──────────────────────────────────────────── */}
      <div className="flex-none border-b border-border bg-card/60 backdrop-blur-sm sticky top-0 z-20">

        {/* Row 1: Title + action buttons */}
        <div className="overflow-hidden max-h-12">
          <div className="flex items-center justify-between px-3 md:px-5 h-10 gap-2">
            <h1 className="text-sm font-bold tracking-tight truncate">Universe Rankings</h1>
            <div className="flex items-center gap-1 shrink-0">
              <Button
                variant="ghost"
                size="sm"
                className="h-7 w-7 p-0 text-muted-foreground hover:text-foreground"
                onClick={handleRefresh}
                title="Force refresh rankings"
                disabled={isRankingsLoading || (!isReady && !isShowingCachedData)}
              >
                <RefreshCw className={cn("w-3.5 h-3.5", isRankingsLoading && "animate-spin")} />
              </Button>
              <Button
                variant="ghost"
                size="sm"
                className="hidden lg:inline-flex h-7 px-2 gap-1 text-xs text-muted-foreground hover:text-foreground"
                onClick={() => setColsOpen(true)}
              >
                <Columns3 className="w-3.5 h-3.5" />
                <span className="hidden sm:inline">Columns</span>
              </Button>
              {/* Group sort toggle — mobile only */}
              <Button
                variant={sortField === "cluster" ? "secondary" : "ghost"}
                size="sm"
                className={cn(
                  "lg:hidden h-7 px-2 gap-1 text-xs",
                  sortField === "cluster" ? "text-primary" : "text-muted-foreground hover:text-foreground"
                )}
                onClick={() => {
                  if (sortField === "cluster") {
                    setSortField("alpha");
                    setSortDirection("desc");
                  } else {
                    setSortField("cluster");
                    setSortDirection("asc");
                  }
                }}
              >
                <span>Group</span>
              </Button>
              {/* Alpha display mode toggle */}
              <Button
                variant={alphaMode === 'pct' ? "secondary" : "ghost"}
                size="sm"
                className={cn(
                  "h-7 px-2 text-xs font-mono tracking-tight",
                  alphaMode === 'pct' ? "text-primary" : "text-muted-foreground hover:text-foreground"
                )}
                onClick={() => setAlphaMode(prev => prev === 'z' ? 'pct' : 'z')}
                title="Toggle alpha display: z-score ↔ percentile"
              >
                α·{alphaMode === 'z' ? 'Z' : '%'}
              </Button>
              <Button
                variant={controlsOpen ? "secondary" : "ghost"}
                size="sm"
                className={cn(
                  "h-7 px-2 gap-1 text-xs",
                  controlsOpen ? "" : "text-muted-foreground hover:text-foreground",
                  activeFilterChips.length > 0 && !controlsOpen && "text-primary"
                )}
                onClick={() => setControlsOpen(true)}
              >
                <SlidersHorizontal className="w-3.5 h-3.5" />
                <span className="hidden sm:inline">Filters</span>
                {activeFilterChips.length > 0 && (
                  <span className="ml-0.5 tabular-nums bg-primary/20 text-primary rounded-full px-1 text-[10px] leading-none py-0.5">
                    {activeFilterChips.length}
                  </span>
                )}
              </Button>
            </div>
          </div>
        </div>

        {/* Row 2: Status chips — horizontally scrollable */}
        <div className="overflow-hidden max-h-8">
          <div className="flex items-center gap-1.5 px-3 md:px-5 pb-1.5 overflow-x-auto scrollbar-none">
            {/* Universe count */}
            <span className="inline-flex items-center h-5 rounded-full px-2 text-[11px] bg-muted/60 border border-border/40 whitespace-nowrap shrink-0 text-muted-foreground">
              {processedStocks.length.toLocaleString()}
              {stocks.length > 0 && processedStocks.length < stocks.length && (
                <span className="opacity-60 ml-0.5">/{stocks.length.toLocaleString()}</span>
              )}
              {" "}eq
            </span>
            {/* Active filter chips */}
            {activeFilterChips.map(chip => (
              <span key={chip} className="inline-flex items-center h-5 rounded-full px-2 text-[11px] bg-primary/10 border border-primary/20 whitespace-nowrap shrink-0 text-primary">
                {chip}
              </span>
            ))}
            {/* Timestamp / cache state */}
            {rankingsResult?.cachedAt && !isShowingCachedData && (
              <span className="inline-flex items-center h-5 rounded-full px-2 text-[11px] bg-muted/60 border border-border/40 whitespace-nowrap shrink-0 text-muted-foreground/70">
                {new Date(rankingsResult.cachedAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
              </span>
            )}
            {isShowingCachedData && localCache && (
              <span className="inline-flex items-center h-5 rounded-full px-2 text-[11px] bg-amber-500/10 border border-amber-500/20 whitespace-nowrap shrink-0 text-amber-500/90">
                {formatCacheAge(localCache.savedAt)}
              </span>
            )}
            {(isRankingsLoading || (!isReady && isShowingCachedData)) && (
              <span className="inline-flex items-center gap-1 h-5 rounded-full px-2 text-[11px] bg-muted/40 border border-border/30 whitespace-nowrap shrink-0 text-muted-foreground/60">
                <Loader2 className="w-2.5 h-2.5 animate-spin" />
                syncing
              </span>
            )}
            {isColdStart && (
              <span className="inline-flex items-center gap-1 h-5 rounded-full px-2 text-[11px] bg-muted/40 border border-border/30 whitespace-nowrap shrink-0 text-muted-foreground/70">
                <Loader2 className="w-2.5 h-2.5 animate-spin" />
                loading
              </span>
            )}
          </div>
        </div>

        {/* Row 3: Search — always visible */}
        <div className="px-3 md:px-5 pb-2 pt-1">
          <div className="relative">
            <Search className="w-3.5 h-3.5 absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground/60 pointer-events-none" />
            <Input
              placeholder="Search ticker or name…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-8 h-7 text-xs bg-background/40 border-border/40 focus-visible:ring-1 w-full"
            />
          </div>
        </div>
      </div>

      {/* ── Filters Sheet ────────────────────────────────────────────────── */}
      <Sheet open={controlsOpen} onOpenChange={setControlsOpen}>
        <SheetContent side="right" className="w-80 p-0 flex flex-col overflow-y-auto">
          <SheetHeader className="px-4 pt-4 pb-3 border-b border-border shrink-0">
            <SheetTitle className="text-sm font-semibold">Filters &amp; Controls</SheetTitle>
          </SheetHeader>
          <div className="flex-1 overflow-y-auto px-4 py-4 space-y-6">

            {/* Factor Weights */}
            <div className="space-y-3">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Factor Weights</h3>
              <div className="space-y-3">
                <div className="space-y-1.5">
                  <Label className="text-xs">S Sleeve — wS ({formatNumber(localW6 * 100, 0)}%)</Label>
                  <Slider value={[localW6 * 100]} min={0} max={100} step={5}
                    onValueChange={(v) => setLocalW6(v[0] / 100)} />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs">T Sleeve — wT ({formatNumber(localW12 * 100, 0)}%)</Label>
                  <Slider value={[localW12 * 100]} min={0} max={100} step={5}
                    onValueChange={(v) => setLocalW12(v[0] / 100)} />
                </div>
              </div>
            </div>

            {/* Universe Filters */}
            <div className="space-y-3">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Universe</h3>
              <div className="space-y-2">
                <div className="bg-muted/40 px-3 py-2 rounded-md space-y-2">
                  <Label className="text-xs">Market Cap</Label>
                  <div className="flex rounded-md overflow-hidden border border-border">
                    {(["all", "no_small", "large_only"] as McapFilter[]).map((v, i) => (
                      <button
                        key={v}
                        className={cn(
                          "flex-1 py-1 text-[11px]",
                          i > 0 && "border-l border-border",
                          mcapFilter === v ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground"
                        )}
                        onClick={() => setMcapFilter(v)}
                      >
                        {MCAP_LABELS[v]}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
              {(() => {
                const threshold  = MCAP_THRESHOLDS[mcapFilter];
                const baseCount  = clientAlphaStocks.length;
                const mcapCount  = threshold != null
                  ? clientAlphaStocks.filter(s => (s.marketCap ?? 0) >= threshold).length
                  : baseCount;
                const universeN  = audit?.preFilterCount ?? null;
                return (
                  <div className="text-[10px] font-mono pt-1.5 border-t border-border/40 space-y-1">
                    {universeN != null && (
                      <div className="flex justify-between text-muted-foreground">
                        <span className="font-sans font-normal">Universe (raw)</span>
                        <span>{universeN.toLocaleString()}</span>
                      </div>
                    )}
                    <div className="flex justify-between text-muted-foreground">
                      <span className="font-sans font-normal">After base filters</span>
                      <span>{baseCount > 0 ? baseCount.toLocaleString() : "—"}</span>
                    </div>
                    {mcapFilter !== "all" && (
                      <div className="flex justify-between text-foreground/90">
                        <span className="font-sans font-normal">Mkt cap {MCAP_LABELS[mcapFilter]}</span>
                        <span className="text-primary font-semibold">{mcapCount.toLocaleString()}</span>
                      </div>
                    )}
                    <p className="font-sans text-muted-foreground/50 pt-0.5 leading-snug normal-case tracking-normal text-[9px]">
                      Filters applied before cross-sectional z-scoring
                    </p>
                  </div>
                );
              })()}
            </div>

            {/* Groups */}
            <div className="space-y-3">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Groups</h3>
              <div className="space-y-3">
                <div className="space-y-1.5">
                  <Label className="text-xs">Number of Groups K ({serverParams.clusterK})</Label>
                  <Slider value={[serverParams.clusterK || 10]} min={2} max={20} step={1}
                    onValueChange={(v) => handleServerParamChange("clusterK", v[0])} />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs">Top N to Group ({serverParams.clusterN})</Label>
                  <Slider value={[serverParams.clusterN || 100]} min={20} max={500} step={10}
                    onValueChange={(v) => handleServerParamChange("clusterN", v[0])} />
                </div>
              </div>
            </div>

          </div>
        </SheetContent>
      </Sheet>

      {/* ── Columns Sheet ────────────────────────────────────────────────── */}
      <Sheet open={colsOpen} onOpenChange={setColsOpen}>
        <SheetContent side="right" className="w-72 p-0 flex flex-col">
          <SheetHeader className="px-4 pt-4 pb-3 border-b border-border">
            <SheetTitle className="text-sm font-semibold">Columns</SheetTitle>
            <p className="text-xs text-muted-foreground mt-0.5">
              Ticker is always shown.
            </p>
          </SheetHeader>

          <div className="flex-1 overflow-y-auto">
            {/* Visible */}
            <div className="px-4 pt-3 pb-1">
              <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                Visible ({orderedVisible.length})
              </p>
            </div>
            {orderedVisible.map((id, i) => (
              <div
                key={id}
                className={cn(
                  "flex items-center gap-2 px-3 mx-1 rounded-lg h-12 transition-colors duration-500",
                  recentlyMoved === id ? "bg-primary/10" : "hover:bg-muted/40"
                )}
              >
                <span className="w-5 text-center text-[11px] font-mono text-muted-foreground shrink-0 select-none">
                  {i + 1}
                </span>
                <span className="flex-1 text-sm">{COLUMN_LABELS[id]}</span>
                <div className="flex items-center shrink-0">
                  <button
                    onClick={() => handleMove(id, "up")}
                    disabled={i === 0}
                    className="h-10 w-9 flex items-center justify-center rounded hover:bg-muted disabled:opacity-20 disabled:cursor-default transition-colors"
                    aria-label="Move up"
                  >
                    <ChevronUp className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => handleMove(id, "down")}
                    disabled={i === orderedVisible.length - 1}
                    className="h-10 w-9 flex items-center justify-center rounded hover:bg-muted disabled:opacity-20 disabled:cursor-default transition-colors"
                    aria-label="Move down"
                  >
                    <ChevronDown className="w-4 h-4" />
                  </button>
                  <button
                    onClick={() => toggleColumn(id)}
                    className="h-10 w-9 flex items-center justify-center rounded hover:bg-destructive/10 hover:text-destructive transition-colors text-muted-foreground ml-0.5"
                    aria-label="Hide column"
                  >
                    <X className="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>
            ))}

            {/* Hidden */}
            {hiddenColumns.length > 0 && (
              <>
                <div className="px-4 pt-4 pb-1">
                  <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                    Hidden ({hiddenColumns.length})
                  </p>
                </div>
                {hiddenColumns.map(id => (
                  <div
                    key={id}
                    className="flex items-center gap-2 px-3 mx-1 rounded-lg h-12 opacity-45 hover:opacity-80 transition-opacity"
                  >
                    <span className="w-5 shrink-0" />
                    <span className="flex-1 text-sm">{COLUMN_LABELS[id]}</span>
                    <button
                      onClick={() => toggleColumn(id)}
                      className="h-10 w-9 flex items-center justify-center rounded hover:bg-muted transition-colors text-muted-foreground"
                      aria-label="Show column"
                    >
                      <Plus className="w-4 h-4" />
                    </button>
                  </div>
                ))}
              </>
            )}
          </div>

          <div className="px-4 py-3 border-t border-border">
            <Button variant="outline" size="sm" className="w-full gap-2 text-xs" onClick={resetColumns}>
              <RotateCcw className="w-3.5 h-3.5" />
              Reset to defaults
            </Button>
          </div>
        </SheetContent>
      </Sheet>

      {/* ── Main Table (virtualized) ─────────────────────────────────────── */}
      <div ref={scrollContainerRef} className="flex-1 overflow-auto relative">
        <div className="lg:min-w-max pb-10 w-full">
          <Table className="table-compact w-full">
            <TableHeader className="hidden lg:table-header-group sticky top-0 bg-background/95 backdrop-blur z-10 shadow-sm">
              <TableRow className="border-b-border/50 hover:bg-transparent">
                <TableHead className="w-10 text-center sticky left-0 z-20 bg-background/95" />
                <TableHead
                  className="w-20 cursor-pointer hover:text-foreground sticky left-10 z-20 bg-background/95"
                  onClick={() => handleSort("ticker")}
                >
                  <div className="flex items-center">Ticker {getSortIcon("ticker")}</div>
                </TableHead>
                {activeColumns.map(renderHeader)}
              </TableRow>
            </TableHeader>

            <TableBody>
              {processedStocks.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={activeColumns.length + 2} className="h-32 text-center text-muted-foreground">
                    {isColdStart ? (
                      <span className="flex flex-col items-center gap-2">
                        <Loader2 className="w-5 h-5 animate-spin text-primary" />
                        <span>Loading quant engine…</span>
                        <span className="text-xs opacity-60">Data will appear automatically</span>
                      </span>
                    ) : search ? (
                      "No equities found matching search."
                    ) : (
                      "No equities found matching filters."
                    )}
                  </TableCell>
                </TableRow>
              ) : (
                <>
                  {virtualizer.getVirtualItems()[0]?.start > 0 && (
                    <tr><td style={{ height: virtualizer.getVirtualItems()[0].start }} /></tr>
                  )}
                  {virtualizer.getVirtualItems().map((virtualRow) => {
                    const stock = processedStocks[virtualRow.index];
                    if (!stock) return null;
                    const inPortfolio = portfolioSet.has(stock.ticker);
                    const dotColor =
                      stock.cluster !== null && stock.cluster !== undefined && stock.cluster < CLUSTER_DOT_COLORS.length
                        ? CLUSTER_DOT_COLORS[stock.cluster]
                        : "bg-muted-foreground/40";
                    const badgeColor =
                      stock.cluster !== null && stock.cluster !== undefined && stock.cluster < CLUSTER_COLORS.length
                        ? CLUSTER_COLORS[stock.cluster]
                        : "bg-muted text-muted-foreground";
                    const clusterText =
                      stock.cluster !== null && stock.cluster !== undefined && stock.cluster < CLUSTER_TEXT_COLORS.length
                        ? CLUSTER_TEXT_COLORS[stock.cluster]
                        : "text-muted-foreground";
                    const sectorAbbr = stock.sector ? (SECTOR_ABBR[stock.sector] ?? stock.sector) : "—";

                    const prevCluster = processedStocks[virtualRow.index - 1]?.cluster;
                    const isNewGroup = sortField === "cluster" && stock.cluster !== prevCluster;

                    return (
                      <React.Fragment key={stock.ticker}>
                      {/* Group section header — only when sorted by group */}
                      {isNewGroup && stock.cluster != null && (
                        <tr>
                          <td colSpan={99} className="pt-3 pb-0.5 px-3">
                            <div className="flex items-center gap-2">
                              <span className={cn("text-[11px] font-bold font-mono tracking-wide", clusterText)}>
                                Group {stock.cluster}
                              </span>
                              {clusterRankMap.get(stock.ticker) && (
                                <span className="text-[10px] text-muted-foreground/50 font-mono">
                                  {clusterRankMap.get(stock.ticker)!.groupSize} names
                                </span>
                              )}
                            </div>
                          </td>
                        </tr>
                      )}
                      <TableRow className="group transition-colors border-b border-border/30 hover:bg-muted/30">

                        {/* ── +/- button — always visible, bigger on mobile ── */}
                        <TableCell className="p-0 text-center sticky left-0 z-10 bg-background">
                          <button
                            className={cn(
                              "flex items-center justify-center rounded transition-colors",
                              "h-14 w-12 lg:h-8 lg:w-8",
                              inPortfolio
                                ? "text-primary"
                                : "text-muted-foreground/50 hover:text-foreground"
                            )}
                            onClick={() => inPortfolio ? removeFromBasket(stock.ticker) : addToBasket(stock.ticker)}
                            aria-label={inPortfolio ? "Remove from portfolio" : "Add to portfolio"}
                          >
                            {inPortfolio
                              ? <Check className="w-4 h-4 lg:w-3.5 lg:h-3.5" />
                              : <Plus className="w-4 h-4 lg:w-3.5 lg:h-3.5" />}
                          </button>
                        </TableCell>

                        {/* ── Mobile 2-line layout (hidden on lg+) ── */}
                        <TableCell
                          className="lg:hidden py-2 pr-3 w-[calc(100vw-3.5rem)] cursor-pointer select-none"
                          onClick={() => setExpandedTicker(prev => prev === stock.ticker ? null : stock.ticker)}
                        >
                          {/* Line 1: ticker (de-emphasized) · score (dominant) */}
                          <div className="flex items-center justify-between gap-2">
                            <div className="flex items-center gap-1.5 min-w-0">
                              <span className="text-[13px] font-medium tracking-tight text-muted-foreground">
                                {stock.ticker}
                              </span>
                              {expandedTicker === stock.ticker
                                ? <ChevronDown className="w-3 h-3 text-muted-foreground shrink-0" />
                                : <ChevronRight className="w-3 h-3 text-muted-foreground shrink-0 opacity-0 group-hover:opacity-100" />}
                            </div>
                            <span
                              className="font-bold tabular-nums text-[17px] shrink-0 tracking-tight"
                              style={{ color: getAlphaColor(alphaPercentileMap.get(stock.ticker) ?? 0.5) }}
                            >
                              {alphaMode === 'pct'
                                ? (stock.percentile != null ? stock.percentile.toFixed(1) + "%" : "—")
                                : (stock.alpha != null ? (stock.alpha > 0 ? "+" : "") + stock.alpha.toFixed(2) : "—")}
                            </span>
                          </div>

                          {/* Line 2: #rank · G{n} · rankInGroup/groupSize · Vol X% · sector */}
                          {(() => {
                            const grp = stock.cluster != null ? clusterRankMap.get(stock.ticker) : undefined;
                            return (
                              <div className="flex items-center gap-1.5 mt-0.5 text-[11px] text-muted-foreground">
                                <span className="font-mono">#{stock.rank ?? "—"}</span>
                                {stock.cluster != null && (
                                  <>
                                    <span className="opacity-40">·</span>
                                    <span className={cn("font-semibold font-mono", clusterText)}>
                                      G{stock.cluster}
                                    </span>
                                    {grp && (
                                      <>
                                        <span className="opacity-40">·</span>
                                        <span className="font-mono opacity-60 text-[10px]">
                                          #{grp.rankInGroup}/{grp.groupSize}
                                        </span>
                                      </>
                                    )}
                                  </>
                                )}
                                {stock.sigma12 != null && (
                                  <>
                                    <span className="opacity-40">·</span>
                                    <span>Vol {(stock.sigma12 * 100).toFixed(1)}%</span>
                                  </>
                                )}
                                {stock.sector && (
                                  <>
                                    <span className="opacity-40">·</span>
                                    <span>{sectorAbbr}</span>
                                  </>
                                )}
                              </div>
                            );
                          })()}

                          {/* Line 3 (top-25 only): suggested position size */}
                          {(() => {
                            const w = suggestedWeights.get(stock.ticker);
                            if (w == null) return null;
                            return (
                              <div className="mt-0.5 text-[10px] font-mono text-primary/70 tracking-tight">
                                Suggested {(w * 100).toFixed(1)}%
                              </div>
                            );
                          })()}
                        </TableCell>

                        {/* ── Desktop: ticker (hidden on mobile) ── */}
                        <TableCell
                          className="hidden lg:table-cell font-bold text-foreground sticky left-10 z-10 bg-background shadow-[1px_0_0_0_rgba(0,0,0,0.1)] cursor-pointer select-none"
                          onClick={() => setExpandedTicker(prev => prev === stock.ticker ? null : stock.ticker)}
                          title="Click to audit alpha components"
                        >
                          <span className="flex items-center gap-1.5">
                            <span className={cn("inline-block w-1.5 h-1.5 rounded-full shrink-0", dotColor)} />
                            <span>{stock.ticker}</span>
                            {expandedTicker === stock.ticker
                              ? <ChevronDown className="w-2.5 h-2.5 text-muted-foreground ml-0.5" />
                              : <ChevronRight className="w-2.5 h-2.5 text-muted-foreground ml-0.5 opacity-0 group-hover:opacity-100" />}
                          </span>
                        </TableCell>

                        {/* ── Desktop: data columns (hidden on mobile) ── */}
                        {activeColumns.map((id) => {
                          const cell = renderCell(id, stock, badgeColor, alphaPercentileMap.get(stock.ticker) ?? 0.5);
                          if (!cell) return null;
                          return React.cloneElement(cell as React.ReactElement<{ className?: string }>, {
                            className: cn("hidden lg:table-cell", (cell as React.ReactElement<{ className?: string }>).props.className),
                          });
                        })}
                      </TableRow>

                      {expandedTicker === stock.ticker && (() => {
                        const fmt2 = (v: number | null | undefined) =>
                          v == null ? "—" : (v > 0 ? "+" : "") + v.toFixed(2);
                        const heat = (v: number | null | undefined): React.CSSProperties => {
                          if (v == null) return { color: "hsl(var(--muted-foreground))" };
                          const x = Math.max(-3, Math.min(3, v)) / 3;
                          if (x >= 0) {
                            const s = Math.round(25 + x * 46);
                            const l = Math.round(48 + x * 10);
                            return { color: `hsl(142 ${s}% ${l}%)` };
                          } else {
                            const ax = Math.abs(x);
                            const s = Math.round(25 + ax * 48);
                            const l = Math.round(48 + ax * 7);
                            return { color: `hsl(0 ${s}% ${l}%)` };
                          }
                        };
                        return (
                          <TableRow key={`${stock.ticker}-audit`} className="bg-muted/20 border-b-border/20">
                            <TableCell colSpan={activeColumns.length + 2} className="px-3 py-2">
                              <div className="flex flex-wrap gap-x-8 gap-y-2 text-[10px] font-mono">

                                {/* ── Sleeves ── */}
                                {(() => {
                                  const zR6  = (stock as any).zR6  as number | null | undefined;
                                  const zR12 = (stock as any).zR12 as number | null | undefined;
                                  const zS6  = (stock as any).zS6  as number | null | undefined;
                                  const zS12 = (stock as any).zS12 as number | null | undefined;
                                  const hasResid = zR6 != null && zR12 != null && (Math.abs(zR6) + Math.abs(zR12)) > 0.0001;
                                  const momComp = hasResid && zS6 != null && zS12 != null
                                    ? 0.7 * (0.5 * zS6 + 0.5 * zS12)
                                    : null;
                                  const resComp = hasResid
                                    ? 0.3 * (0.5 * (zR6 ?? 0) + 0.5 * (zR12 ?? 0))
                                    : null;
                                  return (
                                    <div className="space-y-1">
                                      <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Momentum</p>
                                      <p className="flex items-center gap-2">
                                        <span className="text-muted-foreground w-10">S sleeve</span>
                                        <span className="font-semibold" style={heat(stock.sSleeve)}>{fmt2(stock.sSleeve)}</span>
                                      </p>
                                      {hasResid && momComp != null && resComp != null && (
                                        <>
                                          <p className="flex items-center gap-1 pl-2">
                                            <span className="text-muted-foreground/60 w-1">·</span>
                                            <span className="text-muted-foreground/70 w-12">0.7× Mom</span>
                                            <span className="text-[9px]" style={heat(momComp)}>{fmt2(momComp)}</span>
                                          </p>
                                          <p className="flex items-center gap-1 pl-2">
                                            <span className="text-muted-foreground/60 w-1">·</span>
                                            <span className="text-muted-foreground/70 w-12">0.3× Res</span>
                                            <span className="text-[9px]" style={heat(resComp)}>{fmt2(resComp)}</span>
                                          </p>
                                        </>
                                      )}
                                      <p className="flex items-center gap-2">
                                        <span className="text-muted-foreground w-10">T sleeve</span>
                                        <span className="font-semibold" style={heat(stock.tSleeve)}>{fmt2(stock.tSleeve)}</span>
                                      </p>
                                    </div>
                                  );
                                })()}

                                {/* ── Composite ── */}
                                <div className="space-y-1">
                                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Composite</p>
                                  <p className="flex items-center gap-2">
                                    <span className="text-muted-foreground w-10">Alpha</span>
                                    <span className="font-bold" style={heat(stock.alpha)}>{fmt2(stock.alpha)}</span>
                                  </p>
                                  <p className="flex items-center gap-2">
                                    <span className="text-muted-foreground w-10">Rank</span>
                                    <span className="text-foreground">#{stock.rank}</span>
                                  </p>
                                  <p className="flex items-center gap-2">
                                    <span className="text-muted-foreground w-10">Pct</span>
                                    <span className="text-foreground">{stock.percentile != null ? stock.percentile.toFixed(1) + "%" : "—"}</span>
                                  </p>
                                </div>

                              </div>
                            </TableCell>
                          </TableRow>
                        );
                      })()}
                      </React.Fragment>
                    );
                  })}
                  {(() => {
                    const items = virtualizer.getVirtualItems();
                    const lastItem = items[items.length - 1];
                    const paddingBottom = lastItem
                      ? virtualizer.getTotalSize() - lastItem.end
                      : 0;
                    return paddingBottom > 0 ? <tr><td style={{ height: paddingBottom }} /></tr> : null;
                  })()}
                </>
              )}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  );
}

