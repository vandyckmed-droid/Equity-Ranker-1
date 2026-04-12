import { useState, useMemo } from "react";
import { useGetRankings, Stock } from "@workspace/api-client-react";
import { cn } from "@/lib/utils";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { ChevronRight, Loader2 } from "lucide-react";

const CONTROLS_KEY = "qt:controls-v8";

function loadSavedParams() {
  try {
    const raw = localStorage.getItem(CONTROLS_KEY);
    if (raw) return JSON.parse(raw);
  } catch {}
  return null;
}

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

const CLUSTER_BORDER_COLORS = [
  "border-rose-500/25",
  "border-blue-500/25",
  "border-emerald-500/25",
  "border-amber-500/25",
  "border-violet-500/25",
  "border-cyan-500/25",
  "border-orange-500/25",
  "border-fuchsia-500/25",
  "border-lime-500/25",
  "border-sky-500/25",
];

const CLUSTER_GLOW_COLORS = [
  "shadow-[0_0_18px_rgba(244,63,94,0.07)]",
  "shadow-[0_0_18px_rgba(59,130,246,0.07)]",
  "shadow-[0_0_18px_rgba(16,185,129,0.07)]",
  "shadow-[0_0_18px_rgba(245,158,11,0.07)]",
  "shadow-[0_0_18px_rgba(139,92,246,0.07)]",
  "shadow-[0_0_18px_rgba(6,182,212,0.07)]",
  "shadow-[0_0_18px_rgba(249,115,22,0.07)]",
  "shadow-[0_0_18px_rgba(217,70,239,0.07)]",
  "shadow-[0_0_18px_rgba(132,204,22,0.07)]",
  "shadow-[0_0_18px_rgba(14,165,233,0.07)]",
];

interface GroupData {
  id: number;
  stocks: Stock[];
  avgAlpha: number;
  topAlpha: number;
  size: number;
  topMembers: Stock[];
}

function AlphaPill({ alpha, small }: { alpha: number; small?: boolean }) {
  const isPos = alpha >= 0;
  return (
    <span className={cn(
      "inline-flex items-center font-mono font-bold tabular-nums rounded leading-5",
      small ? "text-[10px] px-1.5 py-0" : "text-[11px] px-2 py-0.5",
      isPos
        ? "bg-emerald-500/15 text-emerald-400"
        : "bg-rose-500/15 text-rose-400"
    )}>
      {isPos ? "+" : ""}{alpha.toFixed(2)}
    </span>
  );
}

function GroupDetailSheet({
  group,
  open,
  onClose,
}: {
  group: GroupData | null;
  open: boolean;
  onClose: () => void;
}) {
  if (!group) return null;
  const idx = group.id % CLUSTER_TEXT_COLORS.length;
  const textColor = CLUSTER_TEXT_COLORS[idx];
  const dotColor = CLUSTER_DOT_COLORS[idx];

  return (
    <Sheet open={open} onOpenChange={(v) => { if (!v) onClose(); }}>
      <SheetContent
        side="bottom"
        className="h-[80vh] bg-slate-950 border-t border-blue-900/30 px-0 pb-0 flex flex-col"
      >
        <SheetHeader className="px-4 py-3 border-b border-border/20 shrink-0">
          <SheetTitle className="flex items-center gap-2.5 text-sm">
            <div className={cn("w-2.5 h-2.5 rounded-full shrink-0", dotColor)} />
            <span className={cn("font-bold font-mono tracking-wide", textColor)}>
              Group {group.id}
            </span>
            <span className="text-muted-foreground/50 font-mono text-xs font-normal">
              {group.size} members
            </span>
            <div className="flex-1" />
            <span className="text-[10px] text-muted-foreground/50 font-mono">avg α</span>
            <AlphaPill alpha={group.avgAlpha} small />
          </SheetTitle>
        </SheetHeader>

        <div className="overflow-y-auto flex-1 pb-4">
          {group.stocks.map((s, i) => (
            <div
              key={s.ticker}
              className="flex items-center gap-3 px-4 py-2.5 border-b border-border/10 last:border-0"
            >
              <span className="text-[10px] font-mono text-muted-foreground/30 w-6 shrink-0 text-right">
                {i + 1}
              </span>
              <span className="font-bold text-sm font-mono text-foreground tracking-wide flex-1">
                {s.ticker}
              </span>
              <span className="text-[10px] font-mono text-muted-foreground/40">
                #{s.rank}
              </span>
              {s.alpha != null && <AlphaPill alpha={s.alpha} small />}
            </div>
          ))}
        </div>
      </SheetContent>
    </Sheet>
  );
}

export default function GroupsPage() {
  const params = useMemo(() => {
    const saved = loadSavedParams();
    const profCoverage = saved?.profCoverage;
    return {
      volAdjust: true,
      useTstats: false,
      volFloor: saved?.volFloor ?? 0.10,
      winsorP: saved?.winsorP ?? 2,
      clusterK: saved?.clusterK ?? 10,
      clusterLookback: saved?.clusterLookback ?? 252,
      minPrice: saved?.minPrice ?? 5,
      minAdv: saved?.minAdv ?? 10_000_000,
      minMarketCap: saved?.minMarketCap ?? 0,
      ...(profCoverage && profCoverage !== "off" ? { profCoverage } : {}),
    };
  }, []);

  const { data, isLoading, isFetching } = useGetRankings(params as Parameters<typeof useGetRankings>[0]);

  const [selectedGroup, setSelectedGroup] = useState<GroupData | null>(null);
  const [sheetOpen, setSheetOpen] = useState(false);

  const groups = useMemo((): GroupData[] => {
    const stocks = data?.stocks ?? [];
    const byCluster = new Map<number, Stock[]>();
    for (const s of stocks) {
      if (s.cluster == null) continue;
      const id = s.cluster as number;
      if (!byCluster.has(id)) byCluster.set(id, []);
      byCluster.get(id)!.push(s as Stock);
    }
    return Array.from(byCluster.entries())
      .map(([id, members]) => {
        const sorted = [...members].sort(
          (a, b) => (a.rank ?? Infinity) - (b.rank ?? Infinity)
        );
        const avgAlpha =
          members.reduce((sum, x) => sum + (x.alpha ?? 0), 0) /
          Math.max(1, members.length);
        const topAlpha = sorted[0]?.alpha ?? 0;
        return {
          id,
          stocks: sorted,
          avgAlpha,
          topAlpha,
          size: members.length,
          topMembers: sorted.slice(0, 5),
        };
      })
      .sort((a, b) => b.avgAlpha - a.avgAlpha);
  }, [data?.stocks]);

  const handleGroupTap = (g: GroupData) => {
    setSelectedGroup(g);
    setSheetOpen(true);
  };

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center h-full min-h-[300px] gap-3">
        <Loader2 className="w-5 h-5 animate-spin text-muted-foreground/40" />
        <span className="text-xs font-mono text-muted-foreground/40">Loading groups…</span>
      </div>
    );
  }

  if (groups.length === 0) {
    return (
      <div className="flex items-center justify-center h-full min-h-[300px] text-muted-foreground/40 text-sm font-mono">
        No group data available
      </div>
    );
  }

  const totalStocks = data?.total ?? 0;

  return (
    <div className="h-full overflow-auto">
      {/* Page header */}
      <div className="sticky top-0 z-10 bg-background/95 backdrop-blur-sm border-b border-border/20 px-4 h-11 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <h2 className="text-xs font-semibold tracking-widest text-muted-foreground uppercase">
            Behavior Groups
          </h2>
          {isFetching && !isLoading && (
            <Loader2 className="w-3 h-3 animate-spin text-muted-foreground/40" />
          )}
        </div>
        <span className="text-[10px] font-mono text-muted-foreground/40">
          {groups.length} groups · {totalStocks.toLocaleString()} stocks
        </span>
      </div>

      {/* Island grid */}
      <div className="px-3 py-3 space-y-2.5 pb-6">
        {groups.map((g) => {
          const colorIdx = g.id % CLUSTER_TEXT_COLORS.length;
          const textColor = CLUSTER_TEXT_COLORS[colorIdx];
          const dotColor = CLUSTER_DOT_COLORS[colorIdx];
          const borderColor = CLUSTER_BORDER_COLORS[colorIdx];
          const glowColor = CLUSTER_GLOW_COLORS[colorIdx];

          return (
            <button
              key={g.id}
              className={cn(
                "w-full text-left rounded-xl bg-slate-900/60 border",
                "transition-all active:scale-[0.99] active:bg-slate-800/70",
                borderColor,
                glowColor
              )}
              onClick={() => handleGroupTap(g)}
            >
              {/* Header row */}
              <div className="flex items-center gap-2.5 px-4 pt-3 pb-2.5">
                <div className={cn("w-2 h-2 rounded-full shrink-0 ring-2 ring-offset-1 ring-offset-slate-900", dotColor, dotColor.replace("bg-", "ring-") + "/30")} />
                <span className={cn("text-sm font-bold font-mono tracking-wider", textColor)}>
                  G{g.id}
                </span>
                <span className="text-[11px] font-mono text-muted-foreground/50">
                  {g.size} members
                </span>
                <div className="flex-1" />
                <div className="flex items-center gap-1.5">
                  <span className="text-[9px] font-mono text-muted-foreground/40 uppercase tracking-wider">avg α</span>
                  <AlphaPill alpha={g.avgAlpha} small />
                </div>
                <ChevronRight className="w-3.5 h-3.5 text-muted-foreground/20 ml-1 shrink-0" />
              </div>

              {/* Divider */}
              <div className="h-px bg-border/15 mx-4" />

              {/* Top 5 members */}
              <div className="px-4 py-1">
                {g.topMembers.map((s, i) => (
                  <div
                    key={s.ticker}
                    className="flex items-center gap-2 py-1.5 border-b border-border/8 last:border-0"
                  >
                    <span className="text-[9px] font-mono text-muted-foreground/25 w-3.5 shrink-0 text-right">
                      {i + 1}
                    </span>
                    <span className="font-bold text-[13px] font-mono text-foreground tracking-wide flex-1">
                      {s.ticker}
                    </span>
                    <span className="text-[10px] font-mono text-muted-foreground/35">
                      #{s.rank}
                    </span>
                    {s.alpha != null && <AlphaPill alpha={s.alpha} small />}
                  </div>
                ))}
              </div>

              {/* Footer */}
              {g.size > 5 && (
                <div className="px-4 pb-2.5 pt-1">
                  <span className="text-[9px] font-mono text-muted-foreground/30">
                    +{g.size - 5} more — tap to expand
                  </span>
                </div>
              )}
            </button>
          );
        })}
      </div>

      <GroupDetailSheet
        group={selectedGroup}
        open={sheetOpen}
        onClose={() => setSheetOpen(false)}
      />
    </div>
  );
}
