import { useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Loader2, BarChart3 } from "lucide-react";
import { cn } from "@/lib/utils";
import type { PortfolioHistoryResponse } from "@workspace/api-client-react";

const VW = 400;
const H_NAV = 112;
const H_DD = 64;
const P = { t: 6, r: 42, b: 18, l: 3 };

const PRIMARY = "hsl(142 71% 45%)";
const AMBER   = "hsl(38 92% 50%)";
const GRID    = "rgba(255,255,255,0.055)";
const LABEL   = "rgba(255,255,255,0.32)";
const REF     = "rgba(255,255,255,0.18)";

function xOf(i: number, n: number) {
  return P.l + (i / Math.max(n - 1, 1)) * (VW - P.l - P.r);
}

function getMonthTicks(dates: string[]): { idx: number; label: string }[] {
  const out: { idx: number; label: string }[] = [];
  let lastMonth = -1;
  const n = dates.length;
  dates.forEach((d, i) => {
    const month = new Date(d).getMonth();
    if (month !== lastMonth) {
      if (i > 0 && i < n - 4) {
        out.push({ idx: i, label: new Date(d).toLocaleString("en-US", { month: "short" }) });
      }
      lastMonth = month;
    }
  });
  return out;
}

function niceYTicks(minV: number, maxV: number, count = 4): number[] {
  const range = maxV - minV || 1;
  const rawStep = range / (count - 1);
  const magnitude = Math.pow(10, Math.floor(Math.log10(rawStep)));
  const step = Math.ceil(rawStep / magnitude) * magnitude;
  const start = Math.ceil(minV / step) * step;
  const ticks: number[] = [];
  for (let v = start; v <= maxV + step * 0.01; v += step) {
    ticks.push(Math.round(v * 100) / 100);
  }
  return ticks.slice(0, count + 2);
}

function NavChart({ dates, nav }: { dates: string[]; nav: number[] }) {
  const n = nav.length;
  const minV = Math.min(...nav);
  const maxV = Math.max(...nav);
  const span = maxV - minV || 1;
  const padded = { min: minV - span * 0.04, max: maxV + span * 0.04 };
  const vSpan = padded.max - padded.min;

  const yOf = (v: number) => P.t + (1 - (v - padded.min) / vSpan) * (H_NAV - P.t - P.b);
  const zeroY = yOf(100);
  const inRange = 100 >= padded.min && 100 <= padded.max;

  const pts = nav.map((v, i) => `${xOf(i, n).toFixed(1)},${yOf(v).toFixed(1)}`).join(" ");
  const bottom = (H_NAV - P.b).toFixed(1);
  const areaPath =
    `M ${xOf(0, n).toFixed(1)},${bottom} ` +
    nav.map((v, i) => `L ${xOf(i, n).toFixed(1)},${yOf(v).toFixed(1)}`).join(" ") +
    ` L ${xOf(n - 1, n).toFixed(1)},${bottom} Z`;

  const yTicks = useMemo(() => niceYTicks(padded.min, padded.max), [padded.min, padded.max]);
  const monthTicks = useMemo(() => getMonthTicks(dates), [dates]);

  return (
    <svg viewBox={`0 0 ${VW} ${H_NAV}`} className="w-full" preserveAspectRatio="none" style={{ display: "block" }}>
      <defs>
        <linearGradient id="phist-nav-fill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={PRIMARY} stopOpacity="0.22" />
          <stop offset="100%" stopColor={PRIMARY} stopOpacity="0.01" />
        </linearGradient>
      </defs>
      {yTicks.map((v) => (
        <line key={v} x1={P.l} y1={yOf(v)} x2={VW - P.r} y2={yOf(v)}
          stroke={GRID} strokeWidth="0.5" />
      ))}
      {inRange && (
        <line x1={P.l} y1={zeroY} x2={VW - P.r} y2={zeroY}
          stroke={REF} strokeWidth="0.75" strokeDasharray="3,3" />
      )}
      <path d={areaPath} fill="url(#phist-nav-fill)" />
      <polyline points={pts} fill="none" stroke={PRIMARY} strokeWidth="1.3"
        strokeLinejoin="round" strokeLinecap="round" />
      {yTicks.map((v) => {
        const rel = v - 100;
        const label = rel === 0 ? "0%" : `${rel > 0 ? "+" : ""}${rel.toFixed(0)}%`;
        return (
          <text key={v} x={VW - P.r + 3} y={yOf(v) + 3.5}
            fontSize="7.5" fill={LABEL} textAnchor="start" fontFamily="ui-monospace,monospace">
            {label}
          </text>
        );
      })}
      {monthTicks.map(({ idx, label }) => (
        <text key={idx} x={xOf(idx, n)} y={H_NAV - 2}
          fontSize="7.5" fill={LABEL} textAnchor="middle" fontFamily="ui-sans-serif,sans-serif">
          {label}
        </text>
      ))}
    </svg>
  );
}

function DrawdownChart({ dates, drawdown }: { dates: string[]; drawdown: number[] }) {
  const n = drawdown.length;
  const minV = Math.min(...drawdown);
  const paddedMin = minV - Math.abs(minV) * 0.06;
  const span = Math.abs(paddedMin) || 1;

  const yOf = (v: number) => P.t + (1 - v / paddedMin) * (H_DD - P.t - P.b);
  const zeroY = P.t;

  const pts = drawdown.map((v, i) => `${xOf(i, n).toFixed(1)},${yOf(v).toFixed(1)}`).join(" ");
  const areaPath =
    `M ${xOf(0, n).toFixed(1)},${zeroY.toFixed(1)} ` +
    drawdown.map((v, i) => `L ${xOf(i, n).toFixed(1)},${yOf(v).toFixed(1)}`).join(" ") +
    ` L ${xOf(n - 1, n).toFixed(1)},${zeroY.toFixed(1)} Z`;

  const yTicks = useMemo(() => {
    const niceTicks = niceYTicks(paddedMin, 0, 3);
    return niceTicks.filter((v) => v <= 0);
  }, [paddedMin]);

  const monthTicks = useMemo(() => getMonthTicks(dates), [dates]);

  return (
    <svg viewBox={`0 0 ${VW} ${H_DD}`} className="w-full" preserveAspectRatio="none" style={{ display: "block" }}>
      <defs>
        <linearGradient id="phist-dd-fill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={AMBER} stopOpacity="0.04" />
          <stop offset="100%" stopColor={AMBER} stopOpacity="0.28" />
        </linearGradient>
      </defs>
      {yTicks.map((v) => (
        <line key={v} x1={P.l} y1={yOf(v)} x2={VW - P.r} y2={yOf(v)}
          stroke={GRID} strokeWidth="0.5" />
      ))}
      <line x1={P.l} y1={zeroY} x2={VW - P.r} y2={zeroY}
        stroke={REF} strokeWidth="0.75" strokeDasharray="3,3" />
      <path d={areaPath} fill="url(#phist-dd-fill)" />
      <polyline points={pts} fill="none" stroke={AMBER} strokeWidth="1.1"
        strokeLinejoin="round" strokeLinecap="round" />
      {yTicks.map((v) => (
        <text key={v} x={VW - P.r + 3} y={yOf(v) + 3.5}
          fontSize="7.5" fill={LABEL} textAnchor="start" fontFamily="ui-monospace,monospace">
          {`${v.toFixed(0)}%`}
        </text>
      ))}
      {monthTicks.map(({ idx, label }) => (
        <text key={idx} x={xOf(idx, n)} y={H_DD - 2}
          fontSize="7.5" fill={LABEL} textAnchor="middle" fontFamily="ui-sans-serif,sans-serif">
          {label}
        </text>
      ))}
    </svg>
  );
}

function fmtReturn(v: number) {
  return `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`;
}
function fmtVol(v: number) {
  return `${v.toFixed(1)}%`;
}

export default function PortfolioHistoryCard({
  histData,
  isLoading,
}: {
  histData: PortfolioHistoryResponse | null;
  isLoading: boolean;
}) {
  return (
    <Card className="bg-card border-border overflow-hidden">
      <CardHeader className="px-4 pt-4 pb-3">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <BarChart3 className="w-3.5 h-3.5 text-primary/70 shrink-0" />
            <CardTitle className="text-sm">Historical Performance</CardTitle>
          </div>
          {isLoading && <Loader2 className="w-3.5 h-3.5 animate-spin text-muted-foreground/50 shrink-0" />}
        </div>
        <CardDescription className="text-[10px] mt-0.5">
          Current basket at static weights · {histData ? `~${histData.numDays}d` : "1yr"} look-back · not a backtest
        </CardDescription>
      </CardHeader>

      {isLoading && !histData && (
        <CardContent className="px-4 pb-4 flex items-center justify-center h-24">
          <Loader2 className="w-5 h-5 animate-spin text-muted-foreground/40" />
        </CardContent>
      )}

      {!isLoading && !histData && (
        <CardContent className="px-4 pb-4 flex items-center justify-center h-16">
          <p className="text-[11px] text-muted-foreground/40">No data</p>
        </CardContent>
      )}

      {histData && (
        <CardContent className="px-0 pb-4">
          {/* NAV chart */}
          <div className="px-3 mb-1">
            <NavChart dates={histData.dates} nav={histData.nav} />
          </div>

          {/* Stats row */}
          <div className="mx-4 mb-3 mt-2 grid grid-cols-3 gap-px bg-border rounded-lg overflow-hidden border border-border/60">
            <div className="bg-card/80 py-2 flex flex-col items-center gap-0.5">
              <span className="text-[9px] uppercase tracking-wider text-muted-foreground/50 font-medium">Total Return</span>
              <span className={cn(
                "text-sm font-bold font-mono tabular-nums",
                histData.totalReturn >= 0 ? "text-emerald-400" : "text-rose-400"
              )}>
                {fmtReturn(histData.totalReturn)}
              </span>
            </div>
            <div className="bg-card/80 py-2 flex flex-col items-center gap-0.5">
              <span className="text-[9px] uppercase tracking-wider text-muted-foreground/50 font-medium">Max Drawdown</span>
              <span className="text-sm font-bold font-mono tabular-nums text-amber-400">
                {fmtReturn(histData.maxDrawdown)}
              </span>
            </div>
            <div className="bg-card/80 py-2 flex flex-col items-center gap-0.5">
              <span className="text-[9px] uppercase tracking-wider text-muted-foreground/50 font-medium">Ann. Vol</span>
              <span className="text-sm font-bold font-mono tabular-nums text-foreground/80">
                {fmtVol(histData.annualizedVol)}
              </span>
            </div>
          </div>

          {/* Drawdown chart */}
          <div className="px-3">
            <p className="text-[9px] uppercase tracking-wider text-muted-foreground/40 font-medium ml-0.5 mb-1">
              Drawdown from peak
            </p>
            <DrawdownChart dates={histData.dates} drawdown={histData.drawdown} />
          </div>
        </CardContent>
      )}
    </Card>
  );
}
