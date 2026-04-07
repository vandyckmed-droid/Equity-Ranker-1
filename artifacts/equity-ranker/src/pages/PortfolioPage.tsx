import { useState, useMemo, useEffect, useRef, useCallback } from "react";
import { Link } from "wouter";
import {
  useComputePortfolioRisk,
  PortfolioRiskRequestWeightingMethod,
} from "@workspace/api-client-react";
import { usePortfolio } from "@/hooks/use-portfolio";
import { formatNumber, formatPercent, cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Trash2, Calculator, Loader2, Info, AlertTriangle } from "lucide-react";

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

export default function PortfolioPage() {
  const { basket, removeFromBasket, clearBasket, seedBasket, allStocks } = usePortfolio();

  const [weightingMethod, setWeightingMethod] = useState<PortfolioRiskRequestWeightingMethod>("equal");
  const [lookback, setLookback] = useState<60 | 126 | 252>(252);
  const [seedCount, setSeedCount] = useState("20");

  const computeRisk = useComputePortfolioRisk();
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const triggerCompute = useCallback(() => {
    if (basket.length === 0) return;
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      computeRisk.mutate({
        data: {
          holdings: basket.map((ticker) => ({ ticker, weight: 1 })),
          lookback,
          weightingMethod,
        },
      });
    }, 300);
  }, [basket, lookback, weightingMethod, computeRisk]);

  useEffect(() => {
    triggerCompute();
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [basket, weightingMethod, lookback]);

  const handleSeed = () => {
    const n = parseInt(seedCount);
    if (isNaN(n) || n <= 0) return;
    const sorted = [...allStocks].sort((a, b) => (b.alpha || 0) - (a.alpha || 0));
    const topN = sorted.slice(0, n).map((s) => s.ticker);
    seedBasket(topN);
    setWeightingMethod("equal");
  };

  const riskData = computeRisk.data;
  const isComputing = computeRisk.isPending;
  const hasError = computeRisk.isError;

  const weightMap = useMemo(() => {
    if (!riskData) return {} as Record<string, number>;
    return Object.fromEntries(riskData.holdings.map((h) => [h.ticker, h.baseWeight]));
  }, [riskData]);

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
            <div className="flex gap-2">
              <Input
                type="number"
                value={seedCount}
                onChange={(e) => setSeedCount(e.target.value)}
                className="w-20 text-center"
                min="1"
                max="40"
              />
              <Button variant="outline" className="flex-1" onClick={handleSeed} disabled={allStocks.length === 0}>
                Seed Top N
              </Button>
            </div>
            {allStocks.length === 0 && (
              <p className="text-xs text-amber-500/80 flex items-center justify-center gap-1">
                <Info className="w-3 h-3" /> Rankings not loaded yet
              </p>
            )}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-3 md:p-6 max-w-7xl mx-auto flex gap-4 md:gap-6 h-full overflow-hidden flex-col lg:flex-row">

      {/* LEFT PANEL: Basket & Method */}
      <div className="w-full lg:w-[380px] flex flex-col gap-3 flex-shrink-0 lg:h-full lg:overflow-hidden">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-lg md:text-2xl font-bold tracking-tight text-foreground">Portfolio Basket</h1>
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

          <div className="p-4 border-t border-border bg-muted/20 space-y-3">
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">Cov lookback</span>
              <div className="flex items-center gap-2">
                {([60, 126, 252] as const).map((days) => (
                  <Button
                    key={days}
                    variant={lookback === days ? "secondary" : "outline"}
                    size="sm"
                    className="h-7 text-xs px-2"
                    onClick={() => setLookback(days)}
                  >
                    {days}d
                  </Button>
                ))}
              </div>
            </div>

            <AuditLine
              riskData={riskData}
              isComputing={isComputing}
              hasError={hasError}
              requestedMethod={weightingMethod}
              lookback={lookback}
            />
          </div>
        </Card>
      </div>

      {/* RIGHT PANEL: Risk Metrics */}
      <div className="flex-1 lg:overflow-hidden lg:h-full flex flex-col gap-3">
        <h2 className="text-base font-semibold text-muted-foreground hidden lg:block">Risk Analysis</h2>

        {hasError && !riskData ? (
          <div className="flex-1 flex items-center justify-center border border-destructive/30 border-dashed rounded-xl bg-card/20 text-destructive min-h-[120px] lg:min-h-0">
            <p className="text-sm p-6 text-center">Failed to compute risk. Check that data is fully loaded.</p>
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
          <div className="flex-1 overflow-auto space-y-4 lg:pr-2">
            {/* KPI Grid */}
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
              <KpiCard label="Portfolio Vol" value={formatPercent(riskData.portfolioVol, 1)}
                sub="15% target" />
              <KpiCard label="Equity Sleeve" value={formatPercent(riskData.riskySleeve, 1)}
                sub={`×${formatNumber(riskData.volTargetMultiplier, 2)} scale`} />
              <KpiCard label="SGOV / Cash" value={formatPercent(riskData.sgovWeight, 1)}
                sub={riskData.sgovWeight > 0 ? "residual in cash" : "fully invested"} />
              <KpiCard label="Div. Ratio" value={formatNumber(riskData.diversificationRatio, 2)}
                sub="avg vol / port vol" />
              <KpiCard label="Effective N" value={formatNumber(riskData.effectiveN, 1)}
                sub={`of ${riskData.numHoldings} names`} />
              <KpiCard label="Avg Corr" value={formatNumber(riskData.avgCorrelation, 2)} />
            </div>

            {/* Cluster Distribution */}
            {riskData.clusterDistribution.length > 0 && (
              <Card className="bg-card border-border">
                <CardHeader className="p-4">
                  <CardTitle className="text-sm">Factor Cluster Exposure</CardTitle>
                  <CardDescription>Capital allocation across momentum/quality clusters (final weights)</CardDescription>
                </CardHeader>
                <CardContent className="p-4 pt-0">
                  <div className="space-y-4">
                    {riskData.clusterDistribution.map((c) => (
                      <div key={c.cluster} className="flex items-center gap-4">
                        <div className="w-8 text-xs font-mono text-muted-foreground">C{c.cluster}</div>
                        <div className="flex-1 h-3 bg-muted rounded-full overflow-hidden">
                          <div
                            className="h-full bg-primary"
                            style={{ width: `${Math.min(c.weight * 100 / riskData.grossExposure * 100, 100)}%` }}
                          />
                        </div>
                        <div className="w-16 text-right text-xs font-mono text-foreground">
                          {formatPercent(c.weight, 1)}
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Constituent Table */}
            <Card className="bg-card border-border">
              <CardHeader className="p-4">
                <CardTitle className="text-sm">Constituent Weights &amp; Risk</CardTitle>
                <CardDescription>
                  Base wt sums to 100% · scaled by ×{formatNumber(riskData.volTargetMultiplier, 2)} to target{" "}
                  {formatPercent(VOL_TARGET, 0)} vol ·{" "}
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
                      <TableHead className="text-center">Cls</TableHead>
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
                          {h.cluster != null ? `C${h.cluster}` : "—"}
                        </TableCell>
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

function KpiCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <Card className="bg-card">
      <CardHeader className="pb-1 p-3">
        <CardTitle className="text-[10px] text-muted-foreground uppercase tracking-wider font-normal">{label}</CardTitle>
      </CardHeader>
      <CardContent className="p-3 pt-0">
        <div className="text-2xl font-bold text-foreground font-mono">{value}</div>
        {sub && <div className="text-[10px] text-muted-foreground mt-0.5">{sub}</div>}
      </CardContent>
    </Card>
  );
}

function AuditLine({
  riskData,
  isComputing,
  hasError,
  requestedMethod,
  lookback,
}: {
  riskData: ReturnType<typeof useComputePortfolioRisk>["data"];
  isComputing: boolean;
  hasError: boolean;
  requestedMethod: string;
  lookback: number;
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
    return (
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
        {" · "}equity <span className="text-foreground/80">{formatPercent(riskData.riskySleeve, 0)}</span>
        {riskData.sgovWeight > 0.001 && (
          <> · SGOV <span className="text-blue-400/80">{formatPercent(riskData.sgovWeight, 0)}</span></>
        )}
        {" · "}cov {riskData.covLookback}d
      </div>
      {riskData.fallback && (
        <div className="flex items-center gap-1 text-[10px] text-amber-400/90">
          <AlertTriangle className="w-3 h-3 flex-shrink-0" />
          <span>{riskData.fallback}</span>
        </div>
      )}
    </div>
  );
}
