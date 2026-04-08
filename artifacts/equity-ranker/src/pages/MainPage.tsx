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

export default function MainPage() {
  const { basket, basketSet, addToBasket, removeFromBasket, setAllStocks } = usePortfolio();
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
        if (d.enrichment !== "complete") return 8000;
        return false;
      },
    },
  });

  const isReady = statusData?.status === "ready";
  const qualityEpoch = statusData?.qualityEpoch ?? 0;

  const prevEpochRef = useRef(qualityEpoch);
  useEffect(() => {
    if (qualityEpoch > 0 && qualityEpoch !== prevEpochRef.current && prevEpochRef.current > 0) {
      queryClient.invalidateQueries({ queryKey: ["/api/equity/rankings"] });
    }
    prevEpochRef.current = qualityEpoch;
  }, [qualityEpoch, queryClient]);

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

  const CONTROLS_KEY = "qt:controls-v1";

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
      secFilerOnly: saved?.secFilerOnly ?? false,
      excludeSectors: (saved?.excludeSectors ?? "") as string,
      requireQuality: saved?.requireQuality ?? false,
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
    return saved?.localW6 ?? 0.4;
  });
  const [localW12, setLocalW12] = useState(() => {
    const saved = loadControlsFromStorage();
    return saved?.localW12 ?? 0.4;
  });
  const [localWQ, setLocalWQ] = useState(() => {
    const saved = loadControlsFromStorage();
    return saved?.localWQ ?? 0.2;
  });
  const [mcapFilter, setMcapFilter] = useState<McapFilter>(() => {
    const saved = loadControlsFromStorage();
    const v = saved?.mcapFilter;
    return (v === "no_small" || v === "large_only") ? v : "all";
  });

  // Persist controls to localStorage whenever they change
  useEffect(() => {
    try {
      localStorage.setItem(CONTROLS_KEY, JSON.stringify({ ...serverParams, localW6, localW12, localWQ, mcapFilter }));
    } catch {}
  }, [serverParams, localW6, localW12, localWQ, mcapFilter]);

  // Unified params object for backward compat (e.g. localStorage, cache key)
  const params: GetRankingsParams = useMemo(() => {
    const p: GetRankingsParams = {
      volAdjust: true,
      useQuality: true,
      useTstats: false,
      w6: 0.4,
      w12: 0.4,
      wQuality: 0.2,
      volFloor: debouncedServerParams.volFloor,
      winsorP: debouncedServerParams.winsorP,
      clusterN: debouncedServerParams.clusterN,
      clusterK: debouncedServerParams.clusterK,
      clusterLookback: debouncedServerParams.clusterLookback,
    };
    if (debouncedServerParams.secFilerOnly) p.secFilerOnly = true;
    if (debouncedServerParams.excludeSectors) p.excludeSectors = debouncedServerParams.excludeSectors;
    if (debouncedServerParams.requireQuality) p.requireQuality = true;
    return p;
  }, [debouncedServerParams]);

  // Alpha highlight state — persisted in localStorage
  const [topN, setTopN] = useState<number>(() => {
    try {
      const stored = localStorage.getItem("qt:topHighlight");
      if (stored) {
        const parsed = JSON.parse(stored);
        const mode = parsed.topNMode === 'pct' ? 'pct' : 'n';
        const max = mode === 'pct' ? 25 : 100;
        const n = Number(parsed.topN);
        if (Number.isFinite(n) && n >= 0 && n <= max) return n;
      }
    } catch {}
    return 20;
  });
  const [topNMode, setTopNMode] = useState<'n' | 'pct'>(() => {
    try {
      const stored = localStorage.getItem("qt:topHighlight");
      if (stored) {
        const parsed = JSON.parse(stored);
        if (parsed.topNMode === 'n' || parsed.topNMode === 'pct') return parsed.topNMode;
      }
    } catch {}
    return 'n';
  });

  useEffect(() => {
    try {
      localStorage.setItem("qt:topHighlight", JSON.stringify({ topN, topNMode }));
    } catch {}
  }, [topN, topNMode]);

  // UI state
  const [showZScores, setShowZScores] = useState(false);
  const [search, setSearch] = useState("");
  const [sortField, setSortField] = useState<SortField>("alpha");
  const [sortDir, setSortDirection] = useState<SortDirection>("desc");
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
  }, [stocks, setAllStocks]);

  const scrollContainerRef = useRef<HTMLDivElement>(null);

  const clientAlphaStocks: Stock[] = useMemo(() => {
    if (!stocks.length) return stocks;
    const wS = localW6;
    const wT = localW12;
    const wQ = localWQ;

    const reranked = stocks.map((s: Stock) => {
      const hasQ = !(s as any).qualityMissing && wQ > 0;
      const totalW = hasQ ? (wS + wT + wQ) : (wS + wT);
      if (totalW === 0) return { ...s, alpha: 0 };
      const alpha = hasQ
        ? (wS * ((s as any).sSleeve ?? 0) + wT * ((s as any).tSleeve ?? 0) + wQ * ((s as any).qSleeve ?? 0)) / totalW
        : (wS * ((s as any).sSleeve ?? 0) + wT * ((s as any).tSleeve ?? 0)) / totalW;
      return { ...s, alpha };
    });

    reranked.sort((a: Stock, b: Stock) => (b.alpha ?? 0) - (a.alpha ?? 0));
    return reranked.map((s: Stock, i: number) => ({
      ...s,
      rank: i + 1,
      percentile: 100 * (1 - i / reranked.length),
    }));
  }, [stocks, localW6, localW12, localWQ]);

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

  const ROW_HEIGHT = 32;
  const virtualizer = useVirtualizer({
    count: processedStocks.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: () => ROW_HEIGHT,
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
      case "momentum6": {
        const sf = showZScores ? "zS6" : "s6";
        return (
          <TableHead key={colId} className="text-right bg-blue-950/20 cursor-pointer hover:text-foreground" onClick={() => handleSort(sf)}>
            <div className="flex items-center justify-end" title="S Sleeve — 6M Sharpe (S6)">
              {showZScores ? "z(S6)" : "S6"} {getSortIcon(sf)}
            </div>
          </TableHead>
        );
      }
      case "momentum12": {
        const sf = showZScores ? "zS12" : "s12";
        return (
          <TableHead key={colId} className="text-right bg-blue-950/20 cursor-pointer hover:text-foreground" onClick={() => handleSort(sf)}>
            <div className="flex items-center justify-end" title="S Sleeve — 12M Sharpe (S12)">
              {showZScores ? "z(S12)" : "S12"} {getSortIcon(sf)}
            </div>
          </TableHead>
        );
      }
      case "quality": {
        const sf = showZScores ? "zQ" : "quality";
        return (
          <TableHead key={colId} className="text-right bg-purple-950/20 cursor-pointer hover:text-foreground" onClick={() => handleSort(sf)}>
            <div className="flex items-center justify-end" title="Quality Composite">
              {showZScores ? "z(Qual)" : "Qual"} {getSortIcon(sf)}
            </div>
          </TableHead>
        );
      }
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
            <div className="flex items-center justify-end text-emerald-500">Alpha {getSortIcon("alpha")}</div>
          </TableHead>
        );
      case "cluster":
        return (
          <TableHead key={colId} className="text-center cursor-pointer hover:text-foreground" onClick={() => handleSort("cluster")}>
            <div className="flex items-center justify-center text-xs">Cls {getSortIcon("cluster")}</div>
          </TableHead>
        );
    }
  };

  // ─── Column cell renderer ──────────────────────────────────────────────────
  const renderCell = (colId: ColumnId, stock: Stock, badgeColor: string) => {
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
      case "momentum6": {
        const val = showZScores ? stock.zS6 : stock.s6;
        return (
          <TableCell key={colId} className={cn("text-right bg-blue-950/10", val! > 0 ? "text-positive" : "text-negative")}>
            {formatNumber(val)}
          </TableCell>
        );
      }
      case "momentum12": {
        const val = showZScores ? stock.zS12 : stock.s12;
        return (
          <TableCell key={colId} className={cn("text-right bg-blue-950/10", val! > 0 ? "text-positive" : "text-negative")}>
            {formatNumber(val)}
          </TableCell>
        );
      }
      case "quality": {
        const val = showZScores ? stock.zQ : stock.quality;
        return (
          <TableCell key={colId} className={cn("text-right bg-purple-950/10", val! > 0 ? "text-positive" : "text-negative")}>
            {formatNumber(val)}
          </TableCell>
        );
      }
      case "vol12":
        return <TableCell key={colId} className="text-right text-muted-foreground">{formatPercent(stock.sigma12)}</TableCell>;
      case "alpha":
        return (
          <TableCell key={colId} className={cn("text-right font-bold bg-emerald-950/10", stock.alpha! > 0 ? "text-emerald-400" : "text-rose-400")}>
            {formatNumber(stock.alpha)}
          </TableCell>
        );
      case "cluster":
        return (
          <TableCell key={colId} className="text-center p-1">
            {stock.cluster !== null && stock.cluster !== undefined ? (
              <Badge variant="outline" className={cn("text-[9px] px-1 py-0 h-4 border-opacity-30 rounded-sm font-mono", badgeColor)}>
                C{stock.cluster}
              </Badge>
            ) : "—"}
          </TableCell>
        );
    }
  };

  // ─── Main render ──────────────────────────────────────────────────────────
  // Active filter chips — computed inline (O(5), no useMemo needed)
  const activeFilterChips: string[] = [];
  if (serverParams.secFilerOnly) activeFilterChips.push("SEC Only");
  if (serverParams.excludeSectors.includes("Finance")) activeFilterChips.push("No Fin");
  if (serverParams.requireQuality) activeFilterChips.push("Quality");
  if (mcapFilter === "no_small") activeFilterChips.push("≥$2B");
  if (mcapFilter === "large_only") activeFilterChips.push("≥$10B");
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
                className="h-7 px-2 gap-1 text-xs text-muted-foreground hover:text-foreground"
                onClick={() => setColsOpen(true)}
              >
                <Columns3 className="w-3.5 h-3.5" />
                <span className="hidden sm:inline">Columns</span>
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
            {/* Quality coverage */}
            {audit && (
              <span className="inline-flex items-center h-5 rounded-full px-2 text-[11px] bg-muted/60 border border-border/40 whitespace-nowrap shrink-0 text-muted-foreground">
                Q {audit.qualityPct ?? 0}%
              </span>
            )}
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
                <div className="space-y-1.5">
                  <Label className="text-xs">Q Sleeve — wQ ({formatNumber(localWQ * 100, 0)}%)</Label>
                  <Slider value={[localWQ * 100]} min={0} max={100} step={5}
                    onValueChange={(v) => setLocalWQ(v[0] / 100)} />
                </div>
              </div>
            </div>

            {/* Universe Filters */}
            <div className="space-y-3">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Universe Filters</h3>
              <div className="space-y-2">
                <div className="flex items-center justify-between bg-muted/40 px-3 py-2 rounded-md">
                  <Label htmlFor="secFiler" className="text-xs cursor-pointer">SEC Filers Only</Label>
                  <Switch id="secFiler" checked={serverParams.secFilerOnly}
                    onCheckedChange={(v) => handleServerParamChange("secFilerOnly", v)} />
                </div>
                <div className="flex items-center justify-between bg-muted/40 px-3 py-2 rounded-md">
                  <Label htmlFor="exclFin" className="text-xs cursor-pointer">Exclude Financials</Label>
                  <Switch id="exclFin" checked={serverParams.excludeSectors.includes("Finance")}
                    onCheckedChange={(v) => handleServerParamChange("excludeSectors",
                      v ? "Finance,Financial Services,Financials" : "")} />
                </div>
                <div className="flex items-center justify-between bg-muted/40 px-3 py-2 rounded-md">
                  <Label htmlFor="reqQual" className="text-xs cursor-pointer">Require Quality</Label>
                  <Switch id="reqQual" checked={serverParams.requireQuality}
                    onCheckedChange={(v) => handleServerParamChange("requireQuality", v)} />
                </div>
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
              {audit && (
                <div className="text-[10px] text-muted-foreground space-y-0.5 pt-1 border-t border-border/40">
                  <p>{audit.postFilterCount ?? "—"} / {audit.preFilterCount ?? "—"} stocks pass filters</p>
                  <p>Quality coverage: {audit.qualityCoverage ?? "—"} ({audit.qualityPct ?? 0}%)</p>
                </div>
              )}
            </div>

            {/* Clustering */}
            <div className="space-y-3">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Clustering</h3>
              <div className="space-y-3">
                <div className="space-y-1.5">
                  <Label className="text-xs">Clusters K ({serverParams.clusterK})</Label>
                  <Slider value={[serverParams.clusterK || 10]} min={2} max={20} step={1}
                    onValueChange={(v) => handleServerParamChange("clusterK", v[0])} />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs">Top N to Cluster ({serverParams.clusterN})</Label>
                  <Slider value={[serverParams.clusterN || 100]} min={20} max={500} step={10}
                    onValueChange={(v) => handleServerParamChange("clusterN", v[0])} />
                </div>
              </div>
            </div>

            {/* Display */}
            <div className="space-y-3">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Display</h3>
              <div className="flex items-center justify-between bg-muted/40 px-3 py-2 rounded-md">
                <Label htmlFor="zscores" className="text-xs cursor-pointer">Show Raw Z-Scores</Label>
                <Switch id="zscores" checked={showZScores} onCheckedChange={setShowZScores} />
              </div>
              <div className="space-y-2 bg-muted/40 px-3 py-2 rounded-md">
                <div className="flex items-center justify-between">
                  <Label className="text-xs">Alpha highlight</Label>
                  <div className="flex rounded-md overflow-hidden border border-border">
                    <button
                      className={cn("px-2 py-0.5 text-xs", topNMode === 'n' ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground")}
                      onClick={() => { setTopNMode('n'); setTopN(prev => Math.min(prev, 100)); }}
                    >N</button>
                    <button
                      className={cn("px-2 py-0.5 text-xs border-l border-border", topNMode === 'pct' ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground")}
                      onClick={() => { setTopNMode('pct'); setTopN(prev => Math.min(prev, 25)); }}
                    >%</button>
                  </div>
                </div>
                <Slider
                  value={[topN]}
                  min={0}
                  max={topNMode === 'n' ? 100 : 25}
                  step={1}
                  onValueChange={(v) => setTopN(v[0])}
                />
                <p className="text-[11px] text-muted-foreground">
                  {topN === 0 ? "Off" : topNMode === 'n' ? `Top ${topN} names` : `Top ${topN}%`}
                </p>
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
        <div className="min-w-max pb-10">
          <Table className="table-compact">
            <TableHeader className="sticky top-0 bg-background/95 backdrop-blur z-10 shadow-sm">
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
                    const isHighlighted = topN > 0 && (
                      topNMode === 'n'
                        ? stock.rank !== null && stock.rank !== undefined && stock.rank <= topN
                        : stock.percentile !== null && stock.percentile !== undefined && stock.percentile >= (100 - topN)
                    );

                    return (
                      <React.Fragment key={stock.ticker}>
                      <TableRow className="group transition-colors border-b-border/30 hover:bg-muted/30">
                        <TableCell className="p-0 text-center sticky left-0 z-10 bg-background shadow-[1px_0_0_0_rgba(0,0,0,0.1)]">
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-6 w-6 rounded-sm opacity-50 group-hover:opacity-100"
                            onClick={() => inPortfolio ? removeFromBasket(stock.ticker) : addToBasket(stock.ticker)}
                          >
                            {inPortfolio
                              ? <Check className="w-3 h-3 text-primary" />
                              : <Plus className="w-3 h-3" />}
                          </Button>
                        </TableCell>
                        <TableCell
                          className="font-bold text-foreground sticky left-10 z-10 bg-background shadow-[1px_0_0_0_rgba(0,0,0,0.1)] cursor-pointer select-none"
                          onClick={() => setExpandedTicker(prev => prev === stock.ticker ? null : stock.ticker)}
                          title="Click to audit alpha components"
                        >
                          <span className="flex items-center gap-1.5">
                            <span className={cn("inline-block w-1.5 h-1.5 rounded-full shrink-0", dotColor)} title={`Cluster ${stock.cluster ?? "?"}`} />
                            <span className={cn(isHighlighted && "text-emerald-400")}>{stock.ticker}</span>
                            {expandedTicker === stock.ticker
                              ? <ChevronDown className="w-2.5 h-2.5 text-muted-foreground ml-0.5" />
                              : <ChevronRight className="w-2.5 h-2.5 text-muted-foreground ml-0.5 opacity-0 group-hover:opacity-100" />}
                          </span>
                        </TableCell>
                        {activeColumns.map((id) => renderCell(id, stock, badgeColor))}
                      </TableRow>

                      {expandedTicker === stock.ticker && (() => {
                        const fmtZ = (v: number | null | undefined) =>
                          v == null ? "—" : (v > 0 ? "+" : "") + v.toFixed(3);
                        const fmtR = (v: number | null | undefined) =>
                          v == null ? "—" : v.toFixed(3);
                        const fmtPct = (v: number | null | undefined) =>
                          v == null ? "—" : (v * 100).toFixed(1) + "%";
                        const zCol = (v: number | null | undefined) =>
                          v == null ? "text-muted-foreground" : v > 0 ? "text-positive" : "text-negative";
                        return (
                          <TableRow key={`${stock.ticker}-audit`} className="bg-muted/20 border-b-border/20">
                            <TableCell colSpan={activeColumns.length + 2} className="px-3 py-2.5">
                              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-x-6 gap-y-3 text-[10px] font-mono">

                                {/* ── Quality raw inputs ── */}
                                <div className="space-y-1">
                                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Quality (raw)</p>
                                  <p><span className="text-muted-foreground">ROE:</span>        <span className="text-foreground">{fmtPct(stock.roe)}</span></p>
                                  <p><span className="text-muted-foreground">ROA:</span>        <span className="text-foreground">{fmtPct(stock.roa)}</span></p>
                                  <p><span className="text-muted-foreground">GrossM:</span>     <span className="text-foreground">{fmtPct(stock.grossMargin)}</span></p>
                                  <p><span className="text-muted-foreground">OpM:</span>        <span className="text-foreground">{fmtPct(stock.opMargin)}</span></p>
                                  <p><span className="text-muted-foreground">D/E:</span>        <span className="text-foreground">{fmtR(stock.deRatio)}</span></p>
                                  {(stock as any).qualityMissing && (
                                    <p className="text-destructive/80 text-[9px] mt-1 font-sans break-words">
                                      ✗ {(stock as any).qualityMissingReason ?? "missing"}
                                    </p>
                                  )}
                                </div>

                                {/* ── Quality z-scores ── */}
                                <div className="space-y-1">
                                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Quality (z)</p>
                                  <p><span className="text-muted-foreground">z_ROE:</span>     <span className={zCol(stock.zRoe)}>{fmtZ(stock.zRoe)}</span></p>
                                  <p><span className="text-muted-foreground">z_ROA:</span>     <span className={zCol(stock.zRoa)}>{fmtZ(stock.zRoa)}</span></p>
                                  <p><span className="text-muted-foreground">z_Gross:</span>   <span className={zCol(stock.zGross)}>{fmtZ(stock.zGross)}</span></p>
                                  <p><span className="text-muted-foreground">z_Op:</span>      <span className={zCol(stock.zOp)}>{fmtZ(stock.zOp)}</span></p>
                                  <p><span className="text-muted-foreground">z_InvLev:</span>  <span className={zCol(stock.zInvLev)}>{fmtZ(stock.zInvLev)}</span></p>
                                  <p className="mt-1"><span className="text-muted-foreground">Q (sleeve):</span> <span className={zCol(stock.qSleeve)}>{fmtZ(stock.qSleeve)}</span></p>
                                </div>

                                {/* ── Momentum raw ── */}
                                <div className="space-y-1">
                                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Momentum (raw)</p>
                                  <p><span className="text-muted-foreground">s6:</span>   <span className="text-foreground">{fmtR(stock.s6)}</span></p>
                                  <p><span className="text-muted-foreground">s12:</span>  <span className="text-foreground">{fmtR(stock.s12)}</span></p>
                                  <p><span className="text-muted-foreground">t6:</span>   <span className="text-foreground">{fmtR(stock.tstat6)}</span></p>
                                  <p><span className="text-muted-foreground">t12:</span>  <span className="text-foreground">{fmtR(stock.tstat12)}</span></p>
                                </div>

                                {/* ── Momentum z / sleeves ── */}
                                <div className="space-y-1">
                                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Momentum (z)</p>
                                  <p><span className="text-muted-foreground">z_s6:</span>  <span className={zCol(stock.zS6)}>{fmtZ(stock.zS6)}</span></p>
                                  <p><span className="text-muted-foreground">z_s12:</span> <span className={zCol(stock.zS12)}>{fmtZ(stock.zS12)}</span></p>
                                  <p><span className="text-muted-foreground">z_t6:</span>  <span className={zCol(stock.zT6)}>{fmtZ(stock.zT6)}</span></p>
                                  <p><span className="text-muted-foreground">z_t12:</span> <span className={zCol(stock.zT12)}>{fmtZ(stock.zT12)}</span></p>
                                  <p className="mt-1"><span className="text-muted-foreground">S (sleeve):</span> <span className={zCol(stock.sSleeve)}>{fmtZ(stock.sSleeve)}</span></p>
                                  <p><span className="text-muted-foreground">T (sleeve):</span> <span className={zCol(stock.tSleeve)}>{fmtZ(stock.tSleeve)}</span></p>
                                </div>

                                {/* ── Composite ── */}
                                <div className="space-y-1">
                                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Composite</p>
                                  <p><span className="text-muted-foreground">Alpha:</span> <span className="text-primary font-bold">{fmtZ(stock.alpha)}</span></p>
                                  <p><span className="text-muted-foreground">Rank:</span>  <span className="text-foreground">#{stock.rank}</span></p>
                                  <p><span className="text-muted-foreground">Pct:</span>   <span className="text-foreground">{stock.percentile != null ? stock.percentile.toFixed(1) + "%" : "—"}</span></p>
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

