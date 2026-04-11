/**
 * AlphaBasketButton + AlphaBasketPanel
 *
 * Self-contained component — manages its own open state so no new hooks are
 * added directly to MainPage (preserves HMR stability).
 *
 * Tabs:
 *   1. Active Basket  — active parts, weights, formulas, final formula, summary
 *   2. Parts Library  — all parts grouped by category, status badges, add/remove
 *   3. Presets        — one-click preset cards
 *   4. Audit          — coverage, alignment notes, limitations
 */

import React, { useState } from "react";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  ChevronUp,
  ChevronDown,
  FlaskConical,
  Check,
  Info,
  TriangleAlert,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  useAlphaBasket,
  ALPHA_PRESETS,
} from "@/hooks/use-alpha-basket";
import {
  ALPHA_PARTS,
  ALPHA_PARTS_MAP,
  type AlphaPart,
  type PartStatus,
  type PartCategory,
} from "@/hooks/alpha-parts-library";

// ─── Status badge config ──────────────────────────────────────────────────────

const STATUS_CONFIG: Record<
  PartStatus,
  { label: string; className: string }
> = {
  institutional_core: {
    label: "Institutional Core",
    className: "bg-emerald-900/50 text-emerald-400 border-emerald-700/40",
  },
  institutional_approx: {
    label: "Approx.",
    className: "bg-amber-900/50 text-amber-400 border-amber-700/40",
  },
  research_extension: {
    label: "Research",
    className: "bg-violet-900/50 text-violet-400 border-violet-700/40",
  },
  future: {
    label: "Future",
    className: "bg-slate-800/60 text-slate-500 border-slate-600/40",
  },
};

const CATEGORY_ORDER: PartCategory[] = [
  "momentum",
  "residual",
  "quality",
  "risk",
  "research",
];
const CATEGORY_LABELS: Record<PartCategory, string> = {
  momentum: "Momentum",
  residual: "Residual",
  quality: "Quality",
  risk: "Risk",
  research: "Research",
};

// ─── StatusBadge ─────────────────────────────────────────────────────────────

function StatusBadge({ status }: { status: PartStatus }) {
  const cfg = STATUS_CONFIG[status];
  return (
    <span
      className={cn(
        "inline-flex items-center h-4 rounded-full px-1.5 text-[9px] font-medium border shrink-0",
        cfg.className
      )}
    >
      {cfg.label}
    </span>
  );
}

// ─── Tab button ───────────────────────────────────────────────────────────────

function TabBtn({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "flex-1 py-1.5 text-[11px] font-medium transition-colors",
        active
          ? "text-foreground border-b-2 border-primary"
          : "text-muted-foreground hover:text-foreground border-b-2 border-transparent"
      )}
    >
      {children}
    </button>
  );
}

// ─── Active Basket tab ────────────────────────────────────────────────────────

function ActiveBasketTab() {
  const {
    basket,
    setWeight,
    togglePart,
    moveUp,
    moveDown,
    totalWeight,
    activeCount,
    activePresetId,
  } = useAlphaBasket();

  const activeItems = basket.filter((i) => i.active && i.weight > 0);
  const inactiveItems = basket.filter((i) => !i.active || i.weight === 0);
  const totalW = totalWeight || 1;

  const finalFormula = activeItems
    .map((item) => {
      const part = ALPHA_PARTS_MAP.get(item.partId);
      if (!part) return "";
      const pct = Math.round((item.weight / totalW) * 100);
      return `${pct}%·${part.shortLabel}`;
    })
    .join(" + ");

  const matchedPreset = ALPHA_PRESETS.find((p) => p.id === activePresetId);

  const plainSummary = (() => {
    if (activeCount === 0) return "No parts active — all stocks will rank equally.";
    const parts = activeItems.map((i) => ALPHA_PARTS_MAP.get(i.partId)?.label ?? i.partId);
    if (parts.length === 1) return `Ranking purely on ${parts[0]}.`;
    const last = parts.pop();
    return `Ranking on ${parts.join(", ")} and ${last}.`;
  })();

  return (
    <div className="space-y-4">
      {/* Matched preset indicator */}
      {matchedPreset && (
        <div className="flex items-center gap-1.5 px-2 py-1.5 rounded-md bg-emerald-900/20 border border-emerald-700/30">
          <Check className="w-3 h-3 text-emerald-400 shrink-0" />
          <span className="text-[11px] text-emerald-400">{matchedPreset.label}</span>
        </div>
      )}

      {/* Active parts */}
      {activeItems.length === 0 ? (
        <div className="text-center py-6 text-xs text-muted-foreground/60">
          No active parts. Go to Parts Library to add some.
        </div>
      ) : (
        <div className="space-y-2">
          {basket.map((item, idx) => {
            const part = ALPHA_PARTS_MAP.get(item.partId);
            if (!part || !item.active) return null;
            const pct = ((item.weight / totalW) * 100).toFixed(0);
            const isFirst = idx === 0 || !basket.slice(0, idx).some((i) => i.active);
            const isLast =
              idx === basket.length - 1 ||
              !basket.slice(idx + 1).some((i) => i.active);

            return (
              <div
                key={item.partId}
                className="border border-border/50 rounded-lg px-3 py-2.5 bg-muted/20 space-y-1.5"
              >
                <div className="flex items-start gap-2">
                  {/* Reorder arrows */}
                  <div className="flex flex-col gap-0.5 mt-0.5 shrink-0">
                    <button
                      onClick={() => moveUp(item.partId)}
                      disabled={isFirst}
                      className="w-4 h-4 flex items-center justify-center text-muted-foreground/50 hover:text-muted-foreground disabled:opacity-20 disabled:cursor-not-allowed"
                    >
                      <ChevronUp className="w-3 h-3" />
                    </button>
                    <button
                      onClick={() => moveDown(item.partId)}
                      disabled={isLast}
                      className="w-4 h-4 flex items-center justify-center text-muted-foreground/50 hover:text-muted-foreground disabled:opacity-20 disabled:cursor-not-allowed"
                    >
                      <ChevronDown className="w-3 h-3" />
                    </button>
                  </div>

                  {/* Part info */}
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <span className="text-xs font-medium text-foreground">
                        {part.label}
                      </span>
                      <StatusBadge status={part.status} />
                    </div>
                    <p className="text-[10px] text-muted-foreground/60 font-mono mt-0.5">
                      {part.displayFormula}
                    </p>
                  </div>

                  {/* Weight + pct */}
                  <div className="flex items-center gap-1.5 shrink-0">
                    <input
                      type="number"
                      min={0}
                      step={1}
                      value={item.weight}
                      onChange={(e) => setWeight(item.partId, Number(e.target.value))}
                      className="w-12 bg-muted border border-border rounded px-1.5 py-0.5 text-xs text-center text-foreground [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
                    />
                    <span className="text-[10px] text-muted-foreground/50 w-7 text-right tabular-nums">
                      {pct}%
                    </span>
                    <button
                      onClick={() => togglePart(item.partId)}
                      className="text-[10px] text-muted-foreground/40 hover:text-rose-400 ml-1 transition-colors"
                      title="Remove from basket"
                    >
                      ✕
                    </button>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Inactive parts — quick-add row */}
      {inactiveItems.length > 0 && (
        <div className="space-y-1">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground/50 font-semibold">
            Inactive
          </p>
          <div className="flex flex-wrap gap-1.5">
            {inactiveItems.map((item) => {
              const part = ALPHA_PARTS_MAP.get(item.partId);
              if (!part) return null;
              return (
                <button
                  key={item.partId}
                  onClick={() => {
                    if (item.weight === 0) setWeight(item.partId, part.defaultWeight);
                    togglePart(item.partId);
                  }}
                  className="inline-flex items-center gap-1 h-6 rounded-full px-2.5 text-[10px] border border-border/40 text-muted-foreground/60 hover:text-foreground hover:border-border/70 bg-muted/30 transition-colors"
                >
                  + {part.shortLabel}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Final formula */}
      {activeItems.length > 0 && (
        <div className="border border-border/40 rounded-lg px-3 py-2.5 bg-background/40 space-y-1.5">
          <p className="text-[9px] uppercase tracking-wider text-muted-foreground/50 font-semibold">
            Combined Score
          </p>
          <p className="text-[11px] font-mono text-foreground/80 leading-snug break-all">
            α = {finalFormula || "—"}
          </p>
          <p className="text-[10px] text-muted-foreground/60 leading-relaxed pt-1 border-t border-border/30">
            {plainSummary}
          </p>
        </div>
      )}
    </div>
  );
}

// ─── Parts Library tab ────────────────────────────────────────────────────────

function PartsLibraryTab() {
  const { basket, togglePart, setWeight } = useAlphaBasket();
  const [expanded, setExpanded] = useState<string | null>(null);

  const grouped = CATEGORY_ORDER.map((cat) => ({
    category: cat,
    parts: ALPHA_PARTS.filter((p) => p.category === cat),
  })).filter((g) => g.parts.length > 0);

  return (
    <div className="space-y-5">
      {grouped.map(({ category, parts }) => (
        <div key={category} className="space-y-2">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground/60 font-semibold">
            {CATEGORY_LABELS[category]}
          </p>
          <div className="space-y-1.5">
            {parts.map((part) => {
              const item = basket.find((i) => i.partId === part.id);
              const isActive = item?.active ?? false;
              const isOpen = expanded === part.id;

              return (
                <div
                  key={part.id}
                  className={cn(
                    "border rounded-lg transition-colors",
                    isActive
                      ? "border-border/60 bg-muted/20"
                      : "border-border/30 bg-transparent opacity-60"
                  )}
                >
                  <div className="flex items-start gap-2 px-3 py-2">
                    {/* Checkbox */}
                    <button
                      onClick={() => {
                        if (!isActive && item && item.weight === 0) {
                          setWeight(part.id, part.defaultWeight);
                        }
                        togglePart(part.id);
                      }}
                      className={cn(
                        "w-4 h-4 mt-0.5 shrink-0 rounded border flex items-center justify-center transition-colors",
                        isActive
                          ? "bg-primary border-primary"
                          : "border-border/50 hover:border-border"
                      )}
                    >
                      {isActive && <Check className="w-2.5 h-2.5 text-primary-foreground" />}
                    </button>

                    {/* Content */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-1.5 flex-wrap">
                        <span className="text-xs font-medium text-foreground">
                          {part.label}
                        </span>
                        <StatusBadge status={part.status} />
                      </div>
                      <p className="text-[10px] font-mono text-muted-foreground/60 mt-0.5">
                        {part.displayFormula}
                      </p>
                    </div>

                    {/* Expand toggle */}
                    <button
                      onClick={() => setExpanded(isOpen ? null : part.id)}
                      className="text-muted-foreground/40 hover:text-muted-foreground mt-0.5 shrink-0"
                    >
                      <Info className="w-3.5 h-3.5" />
                    </button>
                  </div>

                  {/* Expanded: description + variable defs */}
                  {isOpen && (
                    <div className="px-3 pb-3 pt-0 space-y-2 border-t border-border/30">
                      <p className="text-[10px] text-muted-foreground/70 leading-relaxed">
                        {part.description}
                      </p>
                      <div className="space-y-0.5">
                        {part.variableDefinitions.map((def, i) => (
                          <p
                            key={i}
                            className="text-[9px] font-mono text-muted-foreground/50 leading-snug"
                          >
                            {def}
                          </p>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Presets tab ──────────────────────────────────────────────────────────────

function PresetsTab() {
  const { applyPreset, activePresetId } = useAlphaBasket();

  return (
    <div className="space-y-3">
      <p className="text-[10px] text-muted-foreground/60 leading-relaxed">
        One-click recipes. Applying a preset replaces the current basket.
      </p>
      {ALPHA_PRESETS.map((preset) => {
        const isActive = activePresetId === preset.id;
        return (
          <div
            key={preset.id}
            className={cn(
              "border rounded-lg px-3 py-3 space-y-2 transition-colors",
              isActive
                ? "border-primary/40 bg-primary/5"
                : "border-border/40 bg-muted/10 hover:bg-muted/20"
            )}
          >
            <div className="flex items-start justify-between gap-2">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  {isActive && (
                    <Check className="w-3 h-3 text-primary shrink-0" />
                  )}
                  <span className="text-xs font-semibold text-foreground">
                    {preset.label}
                  </span>
                </div>
                <p className="text-[10px] text-muted-foreground/70 mt-0.5 leading-snug">
                  {preset.description}
                </p>
              </div>
              {!isActive && (
                <button
                  onClick={() => applyPreset(preset.id)}
                  className="shrink-0 h-6 px-2.5 rounded text-[10px] font-medium border border-border/50 text-muted-foreground hover:text-foreground hover:border-border transition-colors"
                >
                  Apply
                </button>
              )}
            </div>

            {/* Active parts summary */}
            <div className="flex flex-wrap gap-1">
              {preset.items
                .filter((i) => i.active)
                .map((i) => {
                  const part = ALPHA_PARTS_MAP.get(i.partId);
                  return (
                    <span
                      key={i.partId}
                      className="inline-flex items-center h-4.5 rounded-full px-2 text-[9px] bg-muted/60 border border-border/40 text-muted-foreground/80"
                    >
                      {part?.shortLabel ?? i.partId}
                      <span className="ml-1 opacity-50">×{i.weight}</span>
                    </span>
                  );
                })}
            </div>

            {/* Omission note */}
            <div className="flex items-start gap-1.5">
              <TriangleAlert className="w-2.5 h-2.5 text-amber-500/60 shrink-0 mt-px" />
              <p className="text-[9px] text-muted-foreground/50 leading-snug">
                {preset.omissionNote}
              </p>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ─── Audit tab ────────────────────────────────────────────────────────────────

function AuditTab({
  stockCount,
  lastRefresh,
  audit,
}: {
  stockCount: number;
  lastRefresh?: string;
  audit?: Record<string, unknown>;
}) {
  const { basket, totalWeight, activeCount } = useAlphaBasket();
  const inactiveCount = basket.filter((i) => !i.active || i.weight === 0).length;

  const rows = [
    { label: "Total weight", value: totalWeight.toString() },
    { label: "Active parts", value: `${activeCount} / ${basket.length}` },
    { label: "Inactive parts", value: inactiveCount.toString() },
    { label: "Universe", value: `${stockCount.toLocaleString()} stocks` },
    {
      label: "Last refresh",
      value: lastRefresh
        ? new Date(lastRefresh).toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
          })
        : "—",
    },
  ];

  const qualityCoverage = audit?.qualityCoverage as string | undefined;
  const qualityPct = audit?.qualityPct as number | undefined;

  return (
    <div className="space-y-5">
      {/* Stats */}
      <div className="space-y-1">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground/50 font-semibold">
          Basket Stats
        </p>
        <div className="border border-border/40 rounded-lg overflow-hidden">
          {rows.map((r, i) => (
            <div
              key={r.label}
              className={cn(
                "flex items-center justify-between px-3 py-2",
                i > 0 && "border-t border-border/30"
              )}
            >
              <span className="text-[10px] text-muted-foreground/70">{r.label}</span>
              <span className="text-[10px] font-mono text-foreground/80">{r.value}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Coverage warnings */}
      {qualityPct != null && (
        <div className="space-y-1">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground/50 font-semibold">
            Signal Coverage
          </p>
          <div className="border border-border/40 rounded-lg px-3 py-2.5 space-y-1.5">
            <div className="flex justify-between items-center">
              <span className="text-[10px] text-muted-foreground/70">Profitability (OPA)</span>
              <span className="text-[10px] font-mono text-foreground/80">
                {qualityCoverage ?? "—"} ({qualityPct.toFixed(1)}%)
              </span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-[10px] text-muted-foreground/70">Momentum signals</span>
              <span className="text-[10px] font-mono text-emerald-400">~100%</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-[10px] text-muted-foreground/70">Residual momentum</span>
              <span className="text-[10px] font-mono text-emerald-400">~100%</span>
            </div>
          </div>
        </div>
      )}

      {/* Institutional alignment */}
      <div className="space-y-1">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground/50 font-semibold">
          Institutional Alignment
        </p>
        <div className="border border-amber-700/30 rounded-lg px-3 py-2.5 bg-amber-900/10 space-y-2">
          <p className="text-[10px] text-amber-400/80 font-medium">Current limitations</p>
          <div className="space-y-1.5">
            {[
              {
                label: "Residual momentum",
                note: "Market + industry peer approximation. Institutional models use a full Barra-style factor model (15+ factors).",
              },
              {
                label: "Quality",
                note: "OPA (profitability) only. Institutional quality adds investment, accruals, and earnings stability.",
              },
              {
                label: "Value",
                note: "Not yet built. Institutional multi-factor models typically include book-to-market and earnings yield.",
              },
              {
                label: "Factor neutralization",
                note: "Signals are not neutralized for sector or country — weights can embed latent tilts.",
              },
            ].map((item) => (
              <div key={item.label} className="flex gap-2">
                <TriangleAlert className="w-2.5 h-2.5 text-amber-500/50 shrink-0 mt-0.5" />
                <div>
                  <span className="text-[9px] font-medium text-muted-foreground/80">
                    {item.label}:{" "}
                  </span>
                  <span className="text-[9px] text-muted-foreground/50 leading-snug">
                    {item.note}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────

type TabId = "basket" | "library" | "presets" | "audit";

interface AlphaBasketButtonProps {
  stockCount?: number;
  lastRefresh?: string;
  audit?: Record<string, unknown>;
}

export function AlphaBasketButton({
  stockCount = 0,
  lastRefresh,
  audit,
}: AlphaBasketButtonProps) {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<TabId>("basket");
  const { activeCount, activePresetId } = useAlphaBasket();

  const matchedPreset = ALPHA_PRESETS.find((p) => p.id === activePresetId);

  return (
    <>
      <button
        onClick={() => setOpen(true)}
        className={cn(
          "inline-flex items-center gap-1 h-7 rounded px-2 text-xs font-medium transition-colors",
          "border border-border/40 text-muted-foreground hover:text-foreground hover:border-border/70",
          open && "bg-muted text-foreground border-border/70"
        )}
        title="Alpha Basket — configure alpha construction"
      >
        <FlaskConical className="w-3.5 h-3.5 shrink-0" />
        <span className="hidden sm:inline">Alpha</span>
        {matchedPreset && (
          <span className="hidden md:inline text-[10px] text-muted-foreground/60 font-normal">
            · {matchedPreset.label}
          </span>
        )}
        <span className="tabular-nums bg-primary/15 text-primary rounded-full px-1 text-[9px] leading-none py-0.5 ml-0.5">
          {activeCount}
        </span>
      </button>

      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent side="bottom" className="h-[85dvh] p-0 flex flex-col rounded-t-xl">
          <SheetHeader className="px-4 pt-4 pb-0 shrink-0">
            <SheetTitle className="text-sm font-semibold">Alpha Basket</SheetTitle>
            <p className="text-[10px] text-muted-foreground/60 mt-0.5 pb-3">
              Compose alpha from rigidly defined, auditable signal parts.
            </p>
          </SheetHeader>

          {/* Tab bar */}
          <div className="flex border-b border-border/40 px-4 shrink-0">
            {(
              [
                { id: "basket", label: "Active Basket" },
                { id: "library", label: "Parts Library" },
                { id: "presets", label: "Presets" },
                { id: "audit", label: "Audit" },
              ] as { id: TabId; label: string }[]
            ).map(({ id, label }) => (
              <TabBtn key={id} active={tab === id} onClick={() => setTab(id)}>
                {label}
              </TabBtn>
            ))}
          </div>

          {/* Tab content */}
          <div className="flex-1 overflow-y-auto px-4 py-4">
            {tab === "basket"  && <ActiveBasketTab />}
            {tab === "library" && <PartsLibraryTab />}
            {tab === "presets" && <PresetsTab />}
            {tab === "audit"   && (
              <AuditTab
                stockCount={stockCount}
                lastRefresh={lastRefresh}
                audit={audit}
              />
            )}
          </div>
        </SheetContent>
      </Sheet>
    </>
  );
}
