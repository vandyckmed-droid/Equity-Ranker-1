import { useState, useMemo, useEffect } from "react";
import { 
  useGetDataStatus, 
  useGetRankings,
  Stock,
  GetRankingsParams
} from "@workspace/api-client-react";
import { usePortfolio } from "@/hooks/use-portfolio";
import { formatNumber, formatPercent, formatCompactCurrency, formatCurrency, cn } from "@/lib/utils";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Slider } from "@/components/ui/slider";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { 
  ChevronDown, 
  ChevronRight, 
  Search, 
  Plus, 
  Check, 
  ArrowUpDown, 
  ArrowUp, 
  ArrowDown, 
  Settings2,
  RefreshCw,
  Loader2
} from "lucide-react";
import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import { Checkbox } from "@/components/ui/checkbox";

type SortField = keyof Stock;
type SortDirection = "asc" | "desc" | null;

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

const ROW_CLUSTER_COLORS = [
  "bg-rose-950/20 hover:bg-rose-950/40",
  "bg-blue-950/20 hover:bg-blue-950/40",
  "bg-emerald-950/20 hover:bg-emerald-950/40",
  "bg-amber-950/20 hover:bg-amber-950/40",
  "bg-violet-950/20 hover:bg-violet-950/40",
  "bg-cyan-950/20 hover:bg-cyan-950/40",
  "bg-orange-950/20 hover:bg-orange-950/40",
  "bg-fuchsia-950/20 hover:bg-fuchsia-950/40",
  "bg-lime-950/20 hover:bg-lime-950/40",
  "bg-sky-950/20 hover:bg-sky-950/40",
];

export default function MainPage() {
  const { holdings, addHolding, removeHolding, setAllStocks } = usePortfolio();
  
  // Polling data status
  const { data: statusData, isLoading: isStatusLoading } = useGetDataStatus({
    query: {
      refetchInterval: (query) => (query.state.data?.status === "ready" ? false : 5000),
    }
  });

  const isReady = statusData?.status === "ready";

  // Factor controls state
  const [controlsOpen, setControlsOpen] = useState(false);
  const [params, setParams] = useState<GetRankingsParams>({
    volAdjust: true,
    useQuality: true,
    useTstats: false,
    w6: 0.4,
    w12: 0.4,
    wQuality: 0.2,
    volFloor: 0.05,
    winsorP: 2,
    clusterN: 100,
    clusterK: 10,
    clusterLookback: 252,
  });

  // UI state
  const [showZScores, setShowZScores] = useState(false);
  const [search, setSearch] = useState("");
  const [sortField, setSortField] = useState<SortField>("alpha");
  const [sortDir, setSortDirection] = useState<SortDirection>("desc");

  // Fetch rankings once ready
  const { data: rankingsData, isFetching: isRankingsLoading } = useGetRankings(params, {
    query: {
      enabled: isReady,
      staleTime: 60000,
    }
  });

  // Safe data extraction
  const rankingsResult = rankingsData && "stocks" in rankingsData ? rankingsData : null;
  const stocks = rankingsResult?.stocks || [];

  // Update context with stocks when loaded
  useEffect(() => {
    if (stocks.length > 0) {
      setAllStocks(stocks);
    }
  }, [stocks, setAllStocks]);

  // Filtering and Sorting
  const processedStocks = useMemo(() => {
    let result = [...stocks];

    if (search) {
      const q = search.toLowerCase();
      result = result.filter(
        (s) => s.ticker.toLowerCase().includes(q) || s.name.toLowerCase().includes(q)
      );
    }

    if (sortField && sortDir) {
      result.sort((a, b) => {
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
  }, [stocks, search, sortField, sortDir]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(prev => prev === "asc" ? "desc" : prev === "desc" ? null : "asc");
      if (sortDir === "desc") setSortField("alpha"); // reset to default
    } else {
      setSortField(field);
      setSortDirection("desc");
    }
  };

  const getSortIcon = (field: SortField) => {
    if (sortField !== field) return <ArrowUpDown className="w-3 h-3 ml-1 opacity-20" />;
    return sortDir === "asc" ? <ArrowUp className="w-3 h-3 ml-1" /> : <ArrowDown className="w-3 h-3 ml-1" />;
  };

  const handleParamChange = (key: keyof GetRankingsParams, value: any) => {
    setParams(prev => ({ ...prev, [key]: value }));
  };

  if (!isReady) {
    const progress = statusData?.progress ? statusData.progress * 100 : 0;
    return (
      <div className="flex flex-col items-center justify-center h-full p-8 text-center space-y-6">
        <div className="max-w-md w-full space-y-4 bg-card p-8 rounded-xl border border-border shadow-lg">
          <div className="w-12 h-12 rounded-full bg-primary/20 flex items-center justify-center mx-auto mb-4">
            <RefreshCw className="w-6 h-6 text-primary animate-spin" />
          </div>
          <h2 className="text-2xl font-bold tracking-tight text-foreground">Initializing Quant Engine</h2>
          <p className="text-muted-foreground text-sm">
            {statusData?.message || "Fetching price data from Yahoo Finance..."}
          </p>
          
          <div className="space-y-2 pt-4">
            <Progress value={progress} className="h-2" />
            <div className="flex justify-between text-xs text-muted-foreground font-mono">
              <span>{formatNumber(progress, 0)}%</span>
              <span>{statusData?.loaded || 0} / {statusData?.total || '~1000'} equities</span>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* Header & Controls */}
      <div className="flex-none p-4 md:p-6 border-b border-border bg-card/50 backdrop-blur sticky top-0 z-20">
        <div className="flex flex-col md:flex-row justify-between gap-4 mb-4">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">Universe Rankings</h1>
            <p className="text-sm text-muted-foreground flex items-center gap-2 mt-1">
              <span>{rankingsResult?.total || 0} Equities</span>
              <span>&bull;</span>
              <span>Updated: {rankingsResult?.cachedAt ? new Date(rankingsResult.cachedAt).toLocaleString() : 'Just now'}</span>
              {isRankingsLoading && <Loader2 className="w-3 h-3 animate-spin ml-2" />}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="relative">
              <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <Input 
                placeholder="Search ticker or name..." 
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="pl-9 w-[250px] bg-background/50 border-border/50"
              />
            </div>
            <Collapsible open={controlsOpen} onOpenChange={setControlsOpen}>
              <CollapsibleTrigger asChild>
                <Button variant={controlsOpen ? "secondary" : "outline"} size="sm" className="gap-2">
                  <Settings2 className="w-4 h-4" />
                  Methodology Controls
                  {controlsOpen ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                </Button>
              </CollapsibleTrigger>
            </Collapsible>
          </div>
        </div>

        {/* Collapsible Controls Panel */}
        {controlsOpen && (
          <div className="bg-background rounded-lg border border-border p-4 mb-4 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="space-y-4">
              <h3 className="text-sm font-semibold text-primary">Factor Weights</h3>
              <div className="space-y-3">
                <div className="space-y-1.5">
                  <div className="flex justify-between text-xs">
                    <Label className="text-xs">6M Momentum ({formatNumber((params.w6||0)*100, 0)}%)</Label>
                  </div>
                  <Slider 
                    value={[(params.w6 || 0) * 100]} 
                    min={0} max={100} step={5}
                    onValueChange={(v) => handleParamChange('w6', v[0]/100)}
                  />
                </div>
                <div className="space-y-1.5">
                  <div className="flex justify-between text-xs">
                    <Label className="text-xs">12M Momentum ({formatNumber((params.w12||0)*100, 0)}%)</Label>
                  </div>
                  <Slider 
                    value={[(params.w12 || 0) * 100]} 
                    min={0} max={100} step={5}
                    onValueChange={(v) => handleParamChange('w12', v[0]/100)}
                  />
                </div>
                <div className="space-y-1.5">
                  <div className="flex justify-between text-xs">
                    <Label className="text-xs">Quality ({formatNumber((params.wQuality||0)*100, 0)}%)</Label>
                  </div>
                  <Slider 
                    value={[(params.wQuality || 0) * 100]} 
                    min={0} max={100} step={5}
                    disabled={!params.useQuality}
                    onValueChange={(v) => handleParamChange('wQuality', v[0]/100)}
                  />
                </div>
              </div>
            </div>

            <div className="space-y-4">
              <h3 className="text-sm font-semibold text-primary">Features</h3>
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <Label htmlFor="useQuality" className="text-xs">Use Quality Factor</Label>
                  <Switch id="useQuality" checked={params.useQuality} onCheckedChange={(c) => handleParamChange('useQuality', c)} />
                </div>
                <div className="flex items-center justify-between">
                  <Label htmlFor="volAdjust" className="text-xs">Sharpe Vol-Adjust</Label>
                  <Switch id="volAdjust" checked={params.volAdjust} onCheckedChange={(c) => handleParamChange('volAdjust', c)} />
                </div>
                <div className="flex items-center justify-between">
                  <Label htmlFor="useTstats" className="text-xs">OLS T-Stats</Label>
                  <Switch id="useTstats" checked={params.useTstats} onCheckedChange={(c) => handleParamChange('useTstats', c)} />
                </div>
              </div>
            </div>

            <div className="space-y-4">
              <h3 className="text-sm font-semibold text-primary">Clustering</h3>
              <div className="space-y-3">
                <div className="space-y-1.5">
                  <div className="flex justify-between text-xs">
                    <Label className="text-xs">Clusters K ({params.clusterK})</Label>
                  </div>
                  <Slider 
                    value={[params.clusterK || 10]} 
                    min={2} max={20} step={1}
                    onValueChange={(v) => handleParamChange('clusterK', v[0])}
                  />
                </div>
                <div className="space-y-1.5">
                  <div className="flex justify-between text-xs">
                    <Label className="text-xs">Top N to Cluster ({params.clusterN})</Label>
                  </div>
                  <Slider 
                    value={[params.clusterN || 100]} 
                    min={20} max={500} step={10}
                    onValueChange={(v) => handleParamChange('clusterN', v[0])}
                  />
                </div>
              </div>
            </div>

            <div className="space-y-4">
              <h3 className="text-sm font-semibold text-primary">Display</h3>
              <div className="flex items-center space-x-2 bg-muted/50 p-2 rounded-md">
                <Label htmlFor="zscores" className="text-xs flex-1 cursor-pointer">Show Raw Z-Scores instead of Factors</Label>
                <Switch id="zscores" checked={showZScores} onCheckedChange={setShowZScores} />
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Main Table Area */}
      <div className="flex-1 overflow-hidden relative">
        <ScrollArea className="h-full w-full">
          <div className="min-w-max pb-10">
            <Table className="table-compact">
              <TableHeader className="sticky top-0 bg-background/95 backdrop-blur z-10 shadow-sm">
                <TableRow className="border-b-border/50 hover:bg-transparent">
                  <TableHead className="w-10 text-center sticky left-0 z-20 bg-background/95"></TableHead>
                  <TableHead className="w-8 text-center">Rank</TableHead>
                  <TableHead className="w-20 cursor-pointer hover:text-foreground sticky left-10 z-20 bg-background/95" onClick={() => handleSort("ticker")}>
                    <div className="flex items-center">Ticker {getSortIcon("ticker")}</div>
                  </TableHead>
                  <TableHead className="w-40 cursor-pointer hover:text-foreground" onClick={() => handleSort("name")}>
                    <div className="flex items-center">Name {getSortIcon("name")}</div>
                  </TableHead>
                  <TableHead className="w-24 cursor-pointer hover:text-foreground" onClick={() => handleSort("sector")}>
                    <div className="flex items-center">Sector {getSortIcon("sector")}</div>
                  </TableHead>
                  <TableHead className="text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("price")}>
                    <div className="flex items-center justify-end">Price {getSortIcon("price")}</div>
                  </TableHead>
                  <TableHead className="text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("marketCap")}>
                    <div className="flex items-center justify-end">Mkt Cap {getSortIcon("marketCap")}</div>
                  </TableHead>
                  <TableHead className="text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("adv")}>
                    <div className="flex items-center justify-end">ADV {getSortIcon("adv")}</div>
                  </TableHead>
                  
                  <TableHead className="text-right bg-blue-950/20 cursor-pointer hover:text-foreground" onClick={() => handleSort(showZScores ? "zM6" : (params.volAdjust ? "s6" : "m6"))}>
                    <div className="flex items-center justify-end" title="6-Month Momentum">
                      {showZScores ? "z(M6)" : (params.volAdjust ? "S6" : "M6")} {getSortIcon(showZScores ? "zM6" : (params.volAdjust ? "s6" : "m6"))}
                    </div>
                  </TableHead>
                  <TableHead className="text-right bg-blue-950/20 cursor-pointer hover:text-foreground" onClick={() => handleSort(showZScores ? "zM12" : (params.volAdjust ? "s12" : "m12"))}>
                    <div className="flex items-center justify-end" title="12-Month Momentum">
                      {showZScores ? "z(M12)" : (params.volAdjust ? "S12" : "M12")} {getSortIcon(showZScores ? "zM12" : (params.volAdjust ? "s12" : "m12"))}
                    </div>
                  </TableHead>
                  
                  {params.useQuality && (
                    <TableHead className="text-right bg-purple-950/20 cursor-pointer hover:text-foreground" onClick={() => handleSort(showZScores ? "zQuality" : "quality")}>
                      <div className="flex items-center justify-end" title="Quality Composite">
                        {showZScores ? "z(Qual)" : "Qual"} {getSortIcon(showZScores ? "zQuality" : "quality")}
                      </div>
                    </TableHead>
                  )}
                  
                  <TableHead className="text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("sigma12")}>
                    <div className="flex items-center justify-end" title="12-Month Volatility">Vol(12m) {getSortIcon("sigma12")}</div>
                  </TableHead>
                  
                  <TableHead className="text-right bg-emerald-950/20 font-bold cursor-pointer hover:text-foreground" onClick={() => handleSort("alpha")}>
                    <div className="flex items-center justify-end text-emerald-500">Alpha {getSortIcon("alpha")}</div>
                  </TableHead>
                  <TableHead className="text-center cursor-pointer hover:text-foreground" onClick={() => handleSort("cluster")}>
                    <div className="flex items-center justify-center text-xs">Cls {getSortIcon("cluster")}</div>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {processedStocks.map((stock, i) => {
                  const inPortfolio = holdings.some(h => h.ticker === stock.ticker);
                  const isTop20 = stock.rank && stock.rank <= 20;
                  const clusterColor = stock.cluster !== null && stock.cluster !== undefined && stock.cluster < ROW_CLUSTER_COLORS.length
                    ? ROW_CLUSTER_COLORS[stock.cluster]
                    : "";
                  const badgeColor = stock.cluster !== null && stock.cluster !== undefined && stock.cluster < CLUSTER_COLORS.length
                    ? CLUSTER_COLORS[stock.cluster]
                    : "bg-muted text-muted-foreground";

                  return (
                    <TableRow 
                      key={stock.ticker} 
                      className={cn(
                        "group transition-colors border-b-border/30",
                        isTop20 ? "bg-emerald-950/10 hover:bg-emerald-950/20" : clusterColor || "hover:bg-muted/30"
                      )}
                    >
                      <TableCell className="p-0 text-center sticky left-0 z-10 bg-inherit shadow-[1px_0_0_0_rgba(0,0,0,0.1)]">
                        <Button 
                          variant="ghost" 
                          size="icon" 
                          className="h-6 w-6 rounded-sm opacity-50 group-hover:opacity-100"
                          onClick={() => inPortfolio ? removeHolding(stock.ticker) : addHolding(stock.ticker)}
                        >
                          {inPortfolio ? <Check className="w-3 h-3 text-primary" /> : <Plus className="w-3 h-3" />}
                        </Button>
                      </TableCell>
                      <TableCell className="text-center text-muted-foreground font-mono">{stock.rank}</TableCell>
                      <TableCell className="font-bold text-foreground sticky left-10 z-10 bg-inherit shadow-[1px_0_0_0_rgba(0,0,0,0.1)]">
                        {stock.ticker}
                      </TableCell>
                      <TableCell className="text-muted-foreground truncate max-w-[150px]" title={stock.name}>
                        {stock.name}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-[10px]">{stock.sector || "-"}</TableCell>
                      <TableCell className="text-right">{formatCurrency(stock.price)}</TableCell>
                      <TableCell className="text-right text-muted-foreground">{formatCompactCurrency(stock.marketCap)}</TableCell>
                      <TableCell className="text-right text-muted-foreground">{formatCompactCurrency(stock.adv)}</TableCell>
                      
                      <TableCell className={cn("text-right bg-blue-950/10", (showZScores ? stock.zM6 : (params.volAdjust ? stock.s6 : stock.m6))! > 0 ? "text-positive" : "text-negative")}>
                        {formatNumber(showZScores ? stock.zM6 : (params.volAdjust ? stock.s6 : stock.m6))}
                      </TableCell>
                      <TableCell className={cn("text-right bg-blue-950/10", (showZScores ? stock.zM12 : (params.volAdjust ? stock.s12 : stock.m12))! > 0 ? "text-positive" : "text-negative")}>
                        {formatNumber(showZScores ? stock.zM12 : (params.volAdjust ? stock.s12 : stock.m12))}
                      </TableCell>
                      
                      {params.useQuality && (
                        <TableCell className={cn("text-right bg-purple-950/10", (showZScores ? stock.zQuality : stock.quality)! > 0 ? "text-positive" : "text-negative")}>
                          {formatNumber(showZScores ? stock.zQuality : stock.quality)}
                        </TableCell>
                      )}
                      
                      <TableCell className="text-right text-muted-foreground">{formatPercent(stock.sigma12)}</TableCell>
                      
                      <TableCell className={cn("text-right font-bold bg-emerald-950/10", stock.alpha! > 0 ? "text-emerald-400" : "text-rose-400")}>
                        {formatNumber(stock.alpha)}
                      </TableCell>
                      
                      <TableCell className="text-center p-1">
                        {stock.cluster !== null && stock.cluster !== undefined ? (
                          <Badge variant="outline" className={cn("text-[9px] px-1 py-0 h-4 border-opacity-30 rounded-sm font-mono", badgeColor)}>
                            C{stock.cluster}
                          </Badge>
                        ) : "-"}
                      </TableCell>
                    </TableRow>
                  );
                })}
                {processedStocks.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={15} className="h-32 text-center text-muted-foreground">
                      No equities found matching filters.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
          <ScrollBar orientation="horizontal" />
        </ScrollArea>
      </div>
    </div>
  );
}
