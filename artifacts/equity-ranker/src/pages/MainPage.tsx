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
import { Collapsible, CollapsibleTrigger } from "@/components/ui/collapsible";
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
  Settings2,
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
  const queryClient = useQueryClient();

  // ── localStorage snapshot (warm-start) ──────────────────────────────────
  // Read once at mount; never mutated — fresh API data replaces it atomically.
  const [localCache] = useState<CachedRankings | null>(() => loadRankingsCache());

  // Polling data status — stops when ready
  const { data: statusData } = useGetDataStatus({
    query: {
      refetchInterval: (query) => (query.state.data?.status === "ready" ? false : 5000),
    },
  });

  const isReady = statusData?.status === "ready";

  // Factor controls — split into server-bound (debounced) and local (instant) params
  const [controlsOpen, setControlsOpen] = useState(false);
  const [colsOpen, setColsOpen] = useState(false);

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

  // Persist controls to localStorage whenever they change
  useEffect(() => {
    try {
      localStorage.setItem(CONTROLS_KEY, JSON.stringify({ ...serverParams, localW6, localW12, localWQ }));
    } catch {}
  }, [serverParams, localW6, localW12, localWQ]);

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
  }, [clientAlphaStocks, search, sortField, sortDir]);

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
  return (
    <div className="flex flex-col h-full overflow-hidden">

      {/* ── Header & Controls ────────────────────────────────────────────── */}
      <div className="flex-none px-3 py-3 md:px-6 md:py-4 border-b border-border bg-card/50 backdrop-blur sticky top-0 z-20">
        <div className="flex items-start justify-between gap-2 mb-3">
          <div className="min-w-0">
            <h1 className="text-lg md:text-2xl font-bold tracking-tight leading-tight">Universe Rankings</h1>
            <p className="text-xs text-muted-foreground flex flex-wrap items-center gap-1.5 mt-0.5">
              <span>{stocks.length > 0 ? stocks.length : (rankingsResult?.total || 0)} equities</span>
              {audit && (
                <>
                  <span>&bull;</span>
                  <span>Q {audit.qualityPct ?? 0}%</span>
                  {((audit.activeFilters?.length ?? 0) > 5) && (
                    <>
                      <span>&bull;</span>
                      <span className="text-primary/80">+{(audit.activeFilters?.length ?? 0) - 5} filter{(audit.activeFilters?.length ?? 5) - 5 !== 1 ? "s" : ""}</span>
                    </>
                  )}
                </>
              )}
              {/* Fresh data timestamp */}
              {rankingsResult?.cachedAt && !isShowingCachedData && (
                <>
                  <span>&bull;</span>
                  <span className="hidden sm:inline">
                    Updated: {new Date(rankingsResult.cachedAt).toLocaleString()}
                  </span>
                </>
              )}
              {/* Warm-start: showing localStorage snapshot */}
              {isShowingCachedData && localCache && (
                <>
                  <span>&bull;</span>
                  <span className="text-amber-500/80">
                    Cached · {formatCacheAge(localCache.savedAt)}
                  </span>
                </>
              )}
              {/* Background refresh spinner */}
              {(isRankingsLoading || (!isReady && isShowingCachedData)) && (
                <span className="flex items-center gap-1 text-muted-foreground/60">
                  <Loader2 className="w-3 h-3 animate-spin" />
                  <span className="hidden sm:inline">Refreshing…</span>
                </span>
              )}
              {/* Cold-start: nothing in cache, engine loading */}
              {isColdStart && (
                <span className="flex items-center gap-1">
                  <Loader2 className="w-3 h-3 animate-spin" />
                  Loading engine…
                </span>
              )}
            </p>
          </div>

          {/* Control buttons */}
          <div className="flex items-center gap-1.5 shrink-0">
            <Button
              variant="outline"
              size="sm"
              className="h-8 w-8 p-0"
              onClick={handleRefresh}
              title="Force refresh rankings"
              disabled={isRankingsLoading || (!isReady && !isShowingCachedData)}
            >
              <RefreshCw className={cn("w-3.5 h-3.5", isRankingsLoading && "animate-spin")} />
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="gap-1.5 h-8 px-2.5"
              onClick={() => setColsOpen(true)}
              title="Manage columns"
            >
              <Columns3 className="w-3.5 h-3.5" />
              <span className="hidden sm:inline text-xs">Columns</span>
            </Button>
            <Collapsible open={controlsOpen} onOpenChange={setControlsOpen}>
              <CollapsibleTrigger asChild>
                <Button variant={controlsOpen ? "secondary" : "outline"} size="sm" className="gap-1.5 h-8 px-2.5">
                  <Settings2 className="w-3.5 h-3.5" />
                  <span className="hidden sm:inline text-xs">Controls</span>
                  {controlsOpen ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                </Button>
              </CollapsibleTrigger>
            </Collapsible>
          </div>
        </div>

        {/* Search bar */}
        <div className="relative">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
          <Input
            placeholder="Search ticker or name…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-9 w-full bg-background/50 border-border/50 h-9"
          />
        </div>

        {/* Collapsible Controls Panel */}
        {controlsOpen && (
          <div className="bg-background rounded-lg border border-border p-4 mt-3">
            <div className="flex items-center justify-between mb-3 sm:hidden">
              <span className="text-sm font-semibold text-foreground">Controls</span>
              <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={() => setControlsOpen(false)}>
                <X className="w-4 h-4" />
              </Button>
            </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 md:gap-6">
            <div className="space-y-4">
              <h3 className="text-sm font-semibold text-primary">Factor Weights</h3>
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

            <div className="space-y-4">
              <h3 className="text-sm font-semibold text-primary">Clustering</h3>
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

            <div className="space-y-4">
              <h3 className="text-sm font-semibold text-primary">Universe Filters</h3>
              <div className="space-y-2">
                <div className="flex items-center space-x-2 bg-muted/50 p-2 rounded-md">
                  <Label htmlFor="secFiler" className="text-xs flex-1 cursor-pointer">SEC Filers Only</Label>
                  <Switch id="secFiler" checked={serverParams.secFilerOnly}
                    onCheckedChange={(v) => handleServerParamChange("secFilerOnly", v)} />
                </div>
                <div className="flex items-center space-x-2 bg-muted/50 p-2 rounded-md">
                  <Label htmlFor="exclFin" className="text-xs flex-1 cursor-pointer">Exclude Financials</Label>
                  <Switch id="exclFin" checked={serverParams.excludeSectors.includes("Finance")}
                    onCheckedChange={(v) => handleServerParamChange("excludeSectors",
                      v ? "Finance,Financial Services,Financials" : "")} />
                </div>
                <div className="flex items-center space-x-2 bg-muted/50 p-2 rounded-md">
                  <Label htmlFor="reqQual" className="text-xs flex-1 cursor-pointer">Require Quality</Label>
                  <Switch id="reqQual" checked={serverParams.requireQuality}
                    onCheckedChange={(v) => handleServerParamChange("requireQuality", v)} />
                </div>
              </div>
              {audit && (
                <div className="text-[10px] text-muted-foreground space-y-0.5 pt-1 border-t border-border/40">
                  <p>{audit.postFilterCount ?? "—"} / {audit.preFilterCount ?? "—"} stocks</p>
                  <p>Quality: {audit.qualityCoverage ?? "—"} ({audit.qualityPct ?? 0}%)</p>
                  {((audit.activeFilters?.length ?? 0) > 5) && (
                    <p className="text-primary/80">{(audit.activeFilters?.length ?? 0) - 5} extra filter{(audit.activeFilters?.length ?? 5) - 5 !== 1 ? "s" : ""} active</p>
                  )}
                </div>
              )}
            </div>

            <div className="space-y-4">
              <h3 className="text-sm font-semibold text-primary">Display</h3>
              <div className="flex items-center space-x-2 bg-muted/50 p-2 rounded-md">
                <Label htmlFor="zscores" className="text-xs flex-1 cursor-pointer">Show Raw Z-Scores</Label>
                <Switch id="zscores" checked={showZScores} onCheckedChange={setShowZScores} />
              </div>
              <div className="space-y-2">
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
                <p className="text-xs text-muted-foreground">
                  {topN === 0 ? "Off" : topNMode === 'n' ? `Top ${topN} names` : `Top ${topN}%`}
                </p>
              </div>
            </div>
          </div>
          </div>
        )}
      </div>

      {/* ── Columns Sheet (PATCH 1) ───────────────────────────────────────── */}
      <Sheet open={colsOpen} onOpenChange={setColsOpen}>
        <SheetContent side="right" className="w-72 p-0 flex flex-col">
          <SheetHeader className="px-4 pt-4 pb-3 border-b border-border">
            <SheetTitle className="text-sm font-semibold flex items-center gap-2">
              <Columns3 className="w-4 h-4" />
              Manage Columns
            </SheetTitle>
            <p className="text-xs text-muted-foreground mt-0.5">
              Toggle visibility and reorder. Ticker is always shown.
            </p>
          </SheetHeader>

          <div className="flex-1 overflow-y-auto py-2">
            {ALL_COLUMN_IDS.map((id, idx) => {
              const isVisible = config.visible.includes(id);
              const pos = config.order.indexOf(id);
              return (
                <div
                  key={id}
                  className="flex items-center gap-3 px-4 py-2.5 border-b border-border/40"
                >
                  <Switch
                    checked={isVisible}
                    onCheckedChange={() => toggleColumn(id)}
                    disabled={false}
                    className="shrink-0"
                  />
                  <span className="flex-1 text-sm">{COLUMN_LABELS[id]}</span>
                  <div className="flex flex-col gap-0.5">
                    <button
                      onClick={() => moveColumn(id, "up")}
                      disabled={pos <= 0}
                      className="p-0.5 rounded hover:bg-muted disabled:opacity-20 disabled:cursor-default"
                      aria-label="Move up"
                    >
                      <ChevronUp className="w-3.5 h-3.5" />
                    </button>
                    <button
                      onClick={() => moveColumn(id, "down")}
                      disabled={pos >= ALL_COLUMN_IDS.length - 1}
                      className="p-0.5 rounded hover:bg-muted disabled:opacity-20 disabled:cursor-default"
                      aria-label="Move down"
                    >
                      <ChevronDown className="w-3.5 h-3.5" />
                    </button>
                  </div>
                </div>
              );
            })}
          </div>

          <div className="px-4 py-3 border-t border-border">
            <Button variant="outline" size="sm" className="w-full gap-2 text-xs" onClick={resetColumns}>
              <RotateCcw className="w-3.5 h-3.5" />
              Reset to Defaults
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
                        return (
                          <TableRow key={`${stock.ticker}-audit`} className="bg-muted/20 border-b-border/20">
                            <TableCell colSpan={activeColumns.length + 2} className="px-3 py-2.5">
                              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-x-6 gap-y-2 text-[10px] font-mono">
                                <div className="space-y-1">
                                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Raw Inputs</p>
                                  <p><span className="text-muted-foreground">s6:</span>  <span className="text-foreground">{fmtR(stock.s6)}</span></p>
                                  <p><span className="text-muted-foreground">s12:</span> <span className="text-foreground">{fmtR(stock.s12)}</span></p>
                                  <p><span className="text-muted-foreground">t6:</span>  <span className="text-foreground">{fmtR(stock.tstat6)}</span></p>
                                  <p><span className="text-muted-foreground">t12:</span> <span className="text-foreground">{fmtR(stock.tstat12)}</span></p>
                                  <p><span className="text-muted-foreground">q:</span>   <span className="text-foreground">{fmtR(stock.quality)}</span></p>
                                </div>
                                <div className="space-y-1">
                                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Z-Scores</p>
                                  <p><span className="text-muted-foreground">z_s6:</span>  <span className={cn(stock.zS6! > 0 ? "text-positive" : "text-negative")}>{fmtZ(stock.zS6)}</span></p>
                                  <p><span className="text-muted-foreground">z_s12:</span> <span className={cn(stock.zS12! > 0 ? "text-positive" : "text-negative")}>{fmtZ(stock.zS12)}</span></p>
                                  <p><span className="text-muted-foreground">z_t6:</span>  <span className={cn(stock.zT6! > 0 ? "text-positive" : "text-negative")}>{fmtZ(stock.zT6)}</span></p>
                                  <p><span className="text-muted-foreground">z_t12:</span> <span className={cn(stock.zT12! > 0 ? "text-positive" : "text-negative")}>{fmtZ(stock.zT12)}</span></p>
                                  <p><span className="text-muted-foreground">z_q:</span>   <span className={cn(stock.zQ! > 0 ? "text-positive" : "text-negative")}>{fmtZ(stock.zQ)}</span></p>
                                </div>
                                <div className="space-y-1">
                                  <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold mb-1.5">Sleeves</p>
                                  <p><span className="text-muted-foreground">S (dual-horizon sharpe):</span> <span className={cn(stock.sSleeve! > 0 ? "text-positive" : "text-negative")}>{fmtZ(stock.sSleeve)}</span></p>
                                  <p><span className="text-muted-foreground">T (dual-horizon trend sharpe):</span>  <span className={cn(stock.tSleeve! > 0 ? "text-positive" : "text-negative")}>{fmtZ(stock.tSleeve)}</span></p>
                                  <p><span className="text-muted-foreground">Q (quality):</span><span className={cn(stock.qSleeve! > 0 ? "text-positive" : "text-negative")}>{fmtZ(stock.qSleeve)}</span></p>
                                </div>
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
