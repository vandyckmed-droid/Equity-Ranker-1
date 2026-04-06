import { useState, useMemo } from "react";
import { Link } from "wouter";
import { 
  useComputePortfolioRisk, 
  PortfolioRiskRequestWeightingMethod
} from "@workspace/api-client-react";
import { usePortfolio } from "@/hooks/use-portfolio";
import { formatNumber, formatPercent, cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
import { Trash2, Calculator, Loader2, Info } from "lucide-react";

export default function PortfolioPage() {
  const { holdings, removeHolding, updateWeight, clearHoldings, setHoldings, allStocks } = usePortfolio();
  
  const [weightingMethod, setWeightingMethod] = useState<PortfolioRiskRequestWeightingMethod>("inverse_vol");
  const [lookback, setLookback] = useState<60|126|252>(252);
  const [seedCount, setSeedCount] = useState("20");

  const computeRisk = useComputePortfolioRisk();

  const handleComputeRisk = () => {
    if (holdings.length === 0) return;
    
    // Auto-calculate equal weights for UI sync if equal
    if (weightingMethod === "equal") {
      const w = 1 / holdings.length;
      holdings.forEach(h => updateWeight(h.ticker, w));
    }
    
    computeRisk.mutate({
      data: {
        holdings: holdings.map(h => ({ ticker: h.ticker, weight: h.weight })),
        lookback,
        weightingMethod
      }
    });
  };

  const handleSeed = () => {
    const n = parseInt(seedCount);
    if (isNaN(n) || n <= 0) return;
    
    // Sort allStocks by alpha descending and take top N
    const sorted = [...allStocks].sort((a, b) => (b.alpha || 0) - (a.alpha || 0));
    const topN = sorted.slice(0, n);
    
    const newHoldings = topN.map(s => ({ ticker: s.ticker, weight: 1/n }));
    setHoldings(newHoldings);
    setWeightingMethod("equal");
  };

  const totalManualWeight = useMemo(() => {
    return holdings.reduce((sum, h) => sum + h.weight, 0);
  }, [holdings]);

  if (holdings.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full p-8 text-center space-y-6 bg-background">
        <div className="max-w-md w-full space-y-4 bg-card/50 p-10 rounded-xl border border-border border-dashed">
          <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mx-auto mb-2 text-muted-foreground">
            <Calculator className="w-8 h-8" />
          </div>
          <h2 className="text-xl font-bold text-foreground">Empty Portfolio</h2>
          <p className="text-muted-foreground text-sm">
            Add equities from the universe rankings to construct a basket and analyze risk metrics.
          </p>
          <div className="pt-4 flex flex-col gap-3">
            <Link href="/">
              <Button className="w-full">Go to Rankings</Button>
            </Link>
            <div className="flex items-center gap-2 mt-4 text-sm text-muted-foreground px-2">
              <span className="h-px bg-border flex-1"></span>
              <span>OR</span>
              <span className="h-px bg-border flex-1"></span>
            </div>
            <div className="flex gap-2">
              <Input 
                type="number" 
                value={seedCount} 
                onChange={e => setSeedCount(e.target.value)} 
                className="w-20 text-center" 
                min="1" max="100"
              />
              <Button variant="outline" className="flex-1" onClick={handleSeed} disabled={allStocks.length === 0}>
                Seed Top N
              </Button>
            </div>
            {allStocks.length === 0 && (
              <p className="text-xs text-amber-500/80 flex items-center justify-center gap-1 mt-2">
                <Info className="w-3 h-3" /> Rankings not loaded yet
              </p>
            )}
          </div>
        </div>
      </div>
    );
  }

  const riskData = computeRisk.data;
  const isComputing = computeRisk.isPending;

  return (
    <div className="p-6 max-w-7xl mx-auto flex gap-6 h-full overflow-hidden flex-col lg:flex-row">
      
      {/* LEFT PANEL: Holdings & Config */}
      <div className="w-full lg:w-[400px] flex flex-col gap-4 flex-shrink-0 h-full overflow-hidden">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-bold tracking-tight text-foreground">Portfolio Basket</h1>
          <Button variant="ghost" size="sm" onClick={clearHoldings} className="text-muted-foreground hover:text-destructive">
            Clear All
          </Button>
        </div>

        <Card className="flex-1 flex flex-col overflow-hidden bg-card border-border">
          <CardHeader className="p-4 pb-2 border-b border-border/50 bg-muted/20">
            <Tabs value={weightingMethod} onValueChange={(v) => setWeightingMethod(v as PortfolioRiskRequestWeightingMethod)} className="w-full">
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="equal" className="text-xs">Equal</TabsTrigger>
                <TabsTrigger value="inverse_vol" className="text-xs">Inv. Vol</TabsTrigger>
                <TabsTrigger value="manual" className="text-xs">Manual</TabsTrigger>
              </TabsList>
            </Tabs>
          </CardHeader>
          
          <div className="overflow-auto flex-1 p-0">
            <Table className="table-compact">
              <TableHeader className="sticky top-0 bg-card z-10">
                <TableRow>
                  <TableHead className="w-20">Ticker</TableHead>
                  <TableHead className="text-right">Weight</TableHead>
                  <TableHead className="w-10"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {holdings.map((h) => (
                  <TableRow key={h.ticker} className="hover:bg-muted/50 group">
                    <TableCell className="font-bold text-foreground">{h.ticker}</TableCell>
                    <TableCell className="text-right">
                      {weightingMethod === "manual" ? (
                        <div className="flex justify-end items-center gap-1">
                          <Input 
                            type="number" 
                            step="0.01" 
                            min="0" 
                            value={Number((h.weight * 100).toFixed(2))} 
                            onChange={(e) => updateWeight(h.ticker, parseFloat(e.target.value) / 100)}
                            className="w-20 h-7 text-right px-2 text-xs font-mono"
                          />
                          <span className="text-muted-foreground text-xs">%</span>
                        </div>
                      ) : (
                        <span className="text-muted-foreground font-mono">
                          {formatPercent(h.weight, 2)}
                        </span>
                      )}
                    </TableCell>
                    <TableCell className="p-0 text-center">
                      <Button 
                        variant="ghost" 
                        size="icon" 
                        className="h-6 w-6 rounded-sm text-muted-foreground hover:text-destructive opacity-0 group-hover:opacity-100 transition-opacity"
                        onClick={() => removeHolding(h.ticker)}
                      >
                        <Trash2 className="w-3 h-3" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          
          {weightingMethod === "manual" && (
            <div className={cn("p-2 text-xs text-right font-mono border-t border-border", 
              Math.abs(totalManualWeight - 1) > 0.01 ? "text-amber-500 bg-amber-500/10" : "text-emerald-500 bg-emerald-500/10"
            )}>
              Total Weight: {formatPercent(totalManualWeight, 2)}
            </div>
          )}

          <div className="p-4 border-t border-border bg-muted/20 space-y-4">
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">Lookback Period</span>
              <div className="flex items-center gap-2">
                {[60, 126, 252].map(days => (
                  <Button
                    key={days}
                    variant={lookback === days ? "secondary" : "outline"}
                    size="sm"
                    className="h-7 text-xs px-2"
                    onClick={() => setLookback(days as 60|126|252)}
                  >
                    {days}d
                  </Button>
                ))}
              </div>
            </div>
            
            <Button 
              className="w-full font-bold" 
              onClick={handleComputeRisk}
              disabled={isComputing || holdings.length === 0}
            >
              {isComputing ? (
                <><Loader2 className="w-4 h-4 mr-2 animate-spin" /> Computing Covariance...</>
              ) : "Compute Risk Metrics"}
            </Button>
          </div>
        </Card>
      </div>

      {/* RIGHT PANEL: Risk Metrics */}
      <div className="flex-1 overflow-hidden h-full flex flex-col gap-4">
        <h2 className="text-2xl font-bold tracking-tight text-foreground invisible">Results</h2>
        
        {!riskData ? (
          <div className="flex-1 flex items-center justify-center border border-border border-dashed rounded-xl bg-card/20 text-muted-foreground">
            {isComputing ? (
              <div className="flex flex-col items-center gap-4">
                <Loader2 className="w-8 h-8 animate-spin text-primary" />
                <p>Generating covariance matrix and computing risk...</p>
              </div>
            ) : (
              <p>Configure portfolio and click Compute to view risk metrics.</p>
            )}
          </div>
        ) : (
          <div className="flex-1 overflow-auto space-y-6 pr-4">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <Card className="bg-card">
                <CardHeader className="pb-2 p-4">
                  <CardTitle className="text-xs text-muted-foreground uppercase tracking-wider font-normal">Portfolio Volatility</CardTitle>
                </CardHeader>
                <CardContent className="p-4 pt-0">
                  <div className="text-3xl font-bold text-foreground font-mono">{formatPercent(riskData.portfolioVol, 2)}</div>
                </CardContent>
              </Card>
              <Card className="bg-card">
                <CardHeader className="pb-2 p-4">
                  <CardTitle className="text-xs text-muted-foreground uppercase tracking-wider font-normal">Avg Correlation</CardTitle>
                </CardHeader>
                <CardContent className="p-4 pt-0">
                  <div className="text-3xl font-bold text-foreground font-mono">{formatNumber(riskData.avgCorrelation, 2)}</div>
                </CardContent>
              </Card>
              <Card className="bg-card">
                <CardHeader className="pb-2 p-4">
                  <CardTitle className="text-xs text-muted-foreground uppercase tracking-wider font-normal">Holdings</CardTitle>
                </CardHeader>
                <CardContent className="p-4 pt-0">
                  <div className="text-3xl font-bold text-foreground font-mono">{riskData.numHoldings}</div>
                </CardContent>
              </Card>
              <Card className="bg-card">
                <CardHeader className="pb-2 p-4">
                  <CardTitle className="text-xs text-muted-foreground uppercase tracking-wider font-normal">Max Weight</CardTitle>
                </CardHeader>
                <CardContent className="p-4 pt-0">
                  <div className="text-3xl font-bold text-foreground font-mono">{formatPercent(riskData.largestWeight, 2)}</div>
                </CardContent>
              </Card>
            </div>

            {riskData.clusterDistribution.length > 0 && (
              <Card className="bg-card border-border">
                <CardHeader className="p-4">
                  <CardTitle className="text-sm">Factor Cluster Exposure</CardTitle>
                  <CardDescription>Capital allocation across momentum/quality clusters</CardDescription>
                </CardHeader>
                <CardContent className="p-4 pt-0">
                  <div className="space-y-4">
                    {riskData.clusterDistribution.map((c) => (
                      <div key={c.cluster} className="flex items-center gap-4">
                        <div className="w-8 text-xs font-mono text-muted-foreground">C{c.cluster}</div>
                        <div className="flex-1 h-3 bg-muted rounded-full overflow-hidden">
                          <div 
                            className="h-full bg-primary" 
                            style={{ width: `${c.weight * 100}%` }}
                          />
                        </div>
                        <div className="w-16 text-right text-xs font-mono text-foreground">{formatPercent(c.weight, 1)}</div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            <Card className="bg-card border-border">
              <CardHeader className="p-4">
                <CardTitle className="text-sm">Constituent Risk Contribution</CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <Table className="table-compact">
                  <TableHeader>
                    <TableRow>
                      <TableHead>Ticker</TableHead>
                      <TableHead className="text-right">Alloc Weight</TableHead>
                      <TableHead className="text-right">Ann. Volatility</TableHead>
                      <TableHead className="text-center">Cluster</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {[...riskData.holdings].sort((a, b) => b.weight - a.weight).map((h) => (
                      <TableRow key={h.ticker}>
                        <TableCell className="font-bold">{h.ticker}</TableCell>
                        <TableCell className="text-right text-primary">{formatPercent(h.weight, 2)}</TableCell>
                        <TableCell className="text-right text-muted-foreground">{formatPercent(h.vol, 1)}</TableCell>
                        <TableCell className="text-center text-muted-foreground">{h.cluster !== null ? `C${h.cluster}` : '-'}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </div>
  );
}
