import React, { useState, useRef } from "react";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { GripVertical, X, Check } from "lucide-react";
import { useAlphaBasket, BasketEntry } from "@/hooks/use-alpha-basket";
import {
  ALPHA_PARTS,
  AlphaPart,
  PartCategory,
  PartStatus,
  STATUS_BADGE_STYLES,
} from "@/hooks/alpha-parts-library";

type Tab = "active" | "library" | "presets" | "audit";

const TAB_LABELS: Record<Tab, string> = {
  active:  "Active Basket",
  library: "Parts Library",
  presets: "Presets",
  audit:   "Audit",
};

const CATEGORY_ORDER: PartCategory[] = ["Momentum", "Residual", "Quality", "Risk", "Research"];

type AlphaBasketState = ReturnType<typeof useAlphaBasket>;

interface AlphaBasketPanelProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  cachedAt?: string | null;
  basket: AlphaBasketState;
}

function StatusBadge({ status }: { status: PartStatus }) {
  const { label, cls } = STATUS_BADGE_STYLES[status];
  return (
    <span className={cn("inline-flex items-center rounded border px-1.5 text-[9px] font-semibold tracking-wide leading-4 shrink-0", cls)}>
      {label}
    </span>
  );
}

function WeightInput({
  value,
  onChange,
}: {
  value: number;
  onChange: (v: number) => void;
}) {
  return (
    <input
      type="number"
      min={0}
      step={1}
      value={value}
      onChange={(e) => onChange(Number(e.target.value))}
      className="w-12 bg-muted border border-border rounded px-1.5 py-0.5 text-xs text-center text-foreground [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
    />
  );
}

function ActiveBasketTab({
  entries,
  setWeight,
  togglePart,
  movePartToIndex,
  buildFormula,
}: {
  entries: BasketEntry[];
  setWeight: (id: string, w: number) => void;
  togglePart: (id: string) => void;
  movePartToIndex: (partId: string, toIndex: number) => void;
  buildFormula: () => string;
}) {
  const activeEntries = entries.filter((e) => e.active);
  const totalW = activeEntries.reduce((s, e) => s + e.weight, 0) || 1;
  const dragIdRef = useRef<string | null>(null);
  const [dragOverId, setDragOverId] = useState<string | null>(null);

  if (activeEntries.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-40 text-muted-foreground text-sm gap-2">
        <span>No active parts.</span>
        <span className="text-xs">Go to Parts Library to add parts to your basket.</span>
      </div>
    );
  }

  const parts = ALPHA_PARTS.reduce<Record<string, AlphaPart>>((acc, p) => {
    acc[p.id] = p;
    return acc;
  }, {});

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        {activeEntries.map((entry, idx) => {
          const part = parts[entry.partId];
          if (!part) return null;
          const pct = ((entry.weight / totalW) * 100).toFixed(0);
          const isDragOver = dragOverId === entry.partId && dragIdRef.current !== entry.partId;
          return (
            <div
              key={entry.partId}
              draggable
              onDragStart={() => { dragIdRef.current = entry.partId; }}
              onDragEnd={() => { dragIdRef.current = null; setDragOverId(null); }}
              onDragOver={(e) => { e.preventDefault(); setDragOverId(entry.partId); }}
              onDrop={(e) => {
                e.preventDefault();
                if (dragIdRef.current && dragIdRef.current !== entry.partId) {
                  movePartToIndex(dragIdRef.current, idx);
                }
                setDragOverId(null);
              }}
              className={cn(
                "flex items-start gap-2 p-2.5 rounded-lg border transition-colors",
                isDragOver
                  ? "bg-primary/10 border-primary/40"
                  : "bg-muted/40 border-border/40"
              )}
            >
              <div
                className="shrink-0 mt-1 cursor-grab active:cursor-grabbing text-muted-foreground/40 hover:text-muted-foreground transition-colors"
                aria-label="Drag to reorder"
              >
                <GripVertical className="w-4 h-4" />
              </div>
              <div className="flex-1 min-w-0 space-y-1">
                <div className="flex items-center gap-1.5 flex-wrap">
                  <span className="text-xs font-semibold">{part.label}</span>
                  <StatusBadge status={part.status} />
                </div>
                <p className="text-[10px] font-mono text-muted-foreground/70">{part.displayFormula}</p>
              </div>
              <div className="flex items-center gap-1.5 shrink-0">
                <WeightInput value={entry.weight} onChange={(v) => setWeight(entry.partId, v)} />
                <span className="text-[10px] text-muted-foreground/60 w-8 text-right tabular-nums">{pct}%</span>
                <button
                  onClick={() => togglePart(entry.partId)}
                  className="h-6 w-6 flex items-center justify-center rounded hover:bg-destructive/10 hover:text-destructive text-muted-foreground/50 transition-colors"
                  aria-label="Remove from basket"
                >
                  <X className="w-3 h-3" />
                </button>
              </div>
            </div>
          );
        })}
      </div>

      <div className="rounded-lg bg-muted/30 border border-border/30 px-3 py-2.5 space-y-1.5">
        <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold">Combined Formula</p>
        <p className="text-[11px] font-mono text-foreground/80 leading-snug break-all">{buildFormula()}</p>
      </div>

      <div className="rounded-lg bg-muted/20 border border-border/20 px-3 py-2.5 space-y-1.5">
        <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold">What this basket represents</p>
        <p className="text-[11px] text-muted-foreground/70 leading-relaxed">
          {activeEntries.length === 0
            ? "No active parts — alpha will be zero for all stocks."
            : `This basket scores each stock by a weighted combination of ${activeEntries.length} alpha signal${activeEntries.length > 1 ? "s" : ""}. Signals are cross-sectionally z-scored before weighting, so each part is comparable in scale. The final alpha is unitless and used only for relative ranking.`}
        </p>
      </div>
    </div>
  );
}

function PartsLibraryTab({
  entries,
  togglePart,
}: {
  entries: BasketEntry[];
  togglePart: (id: string) => void;
}) {
  const activeIds = new Set(entries.filter((e) => e.active).map((e) => e.partId));

  const grouped = CATEGORY_ORDER.reduce<Record<string, AlphaPart[]>>((acc, cat) => {
    acc[cat] = ALPHA_PARTS.filter((p) => p.category === cat);
    return acc;
  }, {});

  return (
    <div className="space-y-5">
      {CATEGORY_ORDER.filter((cat) => grouped[cat].length > 0).map((cat) => (
        <div key={cat} className="space-y-2">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{cat}</p>
          {grouped[cat].map((part) => {
            const isActive = activeIds.has(part.id);
            const isFuture = part.status === "future";
            return (
              <div
                key={part.id}
                className={cn(
                  "flex items-start gap-3 p-2.5 rounded-lg border transition-colors",
                  isActive
                    ? "bg-primary/5 border-primary/20"
                    : isFuture
                    ? "bg-muted/20 border-border/20 opacity-50"
                    : "bg-muted/30 border-border/30 opacity-70"
                )}
              >
                <button
                  disabled={isFuture}
                  onClick={() => !isFuture && togglePart(part.id)}
                  className={cn(
                    "mt-0.5 h-4 w-4 rounded border flex items-center justify-center shrink-0 transition-colors",
                    isFuture
                      ? "border-border/30 cursor-not-allowed"
                      : isActive
                      ? "bg-primary border-primary text-primary-foreground"
                      : "border-border hover:border-primary/60"
                  )}
                  aria-label={isActive ? `Remove ${part.label}` : `Add ${part.label}`}
                >
                  {isActive && <Check className="w-2.5 h-2.5" />}
                </button>
                <div className="flex-1 min-w-0 space-y-1">
                  <div className="flex items-center gap-1.5 flex-wrap">
                    <span className="text-xs font-semibold">{part.label}</span>
                    <StatusBadge status={part.status} />
                  </div>
                  <p className="text-[10px] font-mono text-muted-foreground/70">{part.displayFormula}</p>
                  <p className="text-[10px] text-muted-foreground/60 leading-snug">{part.description}</p>
                </div>
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

function PresetsTab({
  activePresetId,
  applyPreset,
  presets,
}: {
  activePresetId: string | null;
  applyPreset: (id: string) => void;
  presets: ReturnType<typeof useAlphaBasket>["presets"];
}) {
  return (
    <div className="space-y-3">
      {presets.map((preset) => {
        const isActive = preset.id === activePresetId;
        return (
          <div
            key={preset.id}
            className={cn(
              "rounded-lg border p-3 space-y-2 transition-colors",
              isActive ? "bg-primary/5 border-primary/30" : "bg-muted/30 border-border/30"
            )}
          >
            <div className="flex items-start justify-between gap-2">
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-xs font-semibold">{preset.label}</span>
                {isActive && (
                  <span className="inline-flex items-center rounded-full px-1.5 text-[9px] font-semibold bg-primary/15 text-primary border border-primary/20">
                    Active
                  </span>
                )}
              </div>
              <Button
                variant={isActive ? "secondary" : "outline"}
                size="sm"
                className="h-6 px-2 text-xs shrink-0"
                onClick={() => applyPreset(preset.id)}
              >
                {isActive ? "Applied" : "Apply"}
              </Button>
            </div>
            <p className="text-[11px] text-muted-foreground/80 leading-relaxed">{preset.description}</p>
            <div className="flex flex-wrap gap-1">
              {preset.entries
                .filter((e) => e.active)
                .map((e) => {
                  const part = ALPHA_PARTS.find((p) => p.id === e.partId);
                  return (
                    <span
                      key={e.partId}
                      className="inline-flex items-center rounded px-1.5 text-[9px] font-mono bg-muted/60 border border-border/40 text-muted-foreground"
                    >
                      {e.weight}×{part?.label ?? e.partId}
                    </span>
                  );
                })}
            </div>
            <p className="text-[10px] text-muted-foreground/50 italic leading-snug">{preset.omitNote}</p>
          </div>
        );
      })}
    </div>
  );
}

function AuditTab({
  entries,
  totalWeight,
  activeCount,
  cachedAt,
}: {
  entries: BasketEntry[];
  totalWeight: number;
  activeCount: number;
  cachedAt?: string | null;
}) {
  const activeEntries = entries.filter((e) => e.active);

  const MOMENTUM_IDS = new Set(["mom_12_1","ram_12_1","mom_6_1","ram_6_1","rm_12_1","rm_6_1","ts_12"]);
  const REVERSAL_IDS = new Set(["rev_ram_1","rev_mom_1"]);
  const hasMomentum    = activeEntries.some((e) => MOMENTUM_IDS.has(e.partId));
  const hasLowVol      = activeEntries.some((e) => e.partId === "low_volatility");
  const hasQuality     = activeEntries.some((e) => e.partId === "prof");
  const hasReversal    = activeEntries.some((e) => REVERSAL_IDS.has(e.partId));

  const coverageWarnings: string[] = [];
  if (!hasMomentum)  coverageWarnings.push("No momentum signal — basket has no trend-following component.");
  if (!hasReversal)  coverageWarnings.push("No short-term reversal — basket may overweight recently overbought names.");

  const zeroWeight = activeEntries.filter((e) => e.weight === 0);
  if (zeroWeight.length > 0) {
    coverageWarnings.push(
      `${zeroWeight.length} active part(s) have weight 0 and contribute nothing: ${zeroWeight.map((e) => e.partId).join(", ")}.`
    );
  }

  return (
    <div className="space-y-5">
      <div className="rounded-lg bg-muted/30 border border-border/30 px-3 py-3 space-y-2">
        <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold">Basket Summary</p>
        <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 text-[11px] font-mono">
          <span className="text-muted-foreground">Active parts</span>
          <span className="font-semibold">{activeCount}</span>
          <span className="text-muted-foreground">Total weight</span>
          <span className="font-semibold">{totalWeight}</span>
          <span className="text-muted-foreground">Momentum</span>
          <span className={hasMomentum ? "text-emerald-400" : "text-muted-foreground/50"}>
            {hasMomentum ? "Yes" : "No"}
          </span>
          <span className="text-muted-foreground">Low Vol</span>
          <span className={hasLowVol ? "text-emerald-400" : "text-muted-foreground/50"}>
            {hasLowVol ? "Yes" : "No"}
          </span>
          <span className="text-muted-foreground">Quality</span>
          <span className={hasQuality ? "text-emerald-400" : "text-muted-foreground/50"}>
            {hasQuality ? "Yes" : "No"}
          </span>
        </div>
        {cachedAt && (
          <div className="pt-1 border-t border-border/30">
            <span className="text-[10px] text-muted-foreground/60">
              Data as of: {new Date(cachedAt).toLocaleString([], { dateStyle: "medium", timeStyle: "short" })}
            </span>
          </div>
        )}
      </div>

      {coverageWarnings.length > 0 && (
        <div className="rounded-lg bg-amber-950/30 border border-amber-700/30 px-3 py-3 space-y-2">
          <p className="text-[9px] uppercase tracking-wider text-amber-400 font-semibold">Coverage Warnings</p>
          <ul className="space-y-1.5">
            {coverageWarnings.map((w, i) => (
              <li key={i} className="text-[11px] text-amber-300/80 leading-snug flex gap-1.5">
                <span className="text-amber-400 shrink-0">·</span>
                {w}
              </li>
            ))}
          </ul>
        </div>
      )}

      <div className="rounded-lg bg-muted/20 border border-border/20 px-3 py-3 space-y-2.5">
        <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold">Institutional Alignment</p>
        <div className="space-y-2 text-[11px] text-muted-foreground/70 leading-relaxed">
          <div>
            <span className="text-emerald-400 font-semibold">What is institutional-grade:</span>
            <p className="mt-0.5">MOM, RAM (volatility-adjusted momentum), RM (residual momentum after market/sector), TS_12 (trend t-stat), and short-term reversal are all clean, standard institutional signals with strong academic backing.</p>
          </div>
          <div>
            <span className="text-amber-400 font-semibold">What is approximate:</span>
            <p className="mt-0.5">PROF uses a fallback cascade (op. income → EBIT → net income) / avg. assets, which may be noisier than primary-sourced data. LowVol uses total volatility rather than idiosyncratic vol.</p>
          </div>
          <div>
            <span className="text-muted-foreground font-semibold">What is not yet built:</span>
            <p className="mt-0.5">Value signals (earnings yield, book-to-price) and extended quality dimensions (leverage, accruals, earnings quality) are not yet implemented.</p>
          </div>
        </div>
      </div>
    </div>
  );
}

export function AlphaBasketPanel({ open, onOpenChange, cachedAt, basket }: AlphaBasketPanelProps) {
  const [activeTab, setActiveTab] = useState<Tab>("active");
  const {
    entries,
    setWeight,
    togglePart,
    movePartToIndex,
    applyPreset,
    activePresetId,
    totalWeight,
    activeCount,
    buildFormula,
    presets,
  } = basket;

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="bottom" className="rounded-t-xl h-[85vh] p-0 flex flex-col">
        <SheetHeader className="px-4 pt-4 pb-0 shrink-0">
          <SheetTitle className="text-sm font-semibold">Alpha Basket</SheetTitle>
        </SheetHeader>

        {/* Tab bar */}
        <div className="px-4 pt-3 pb-0 shrink-0">
          <div className="flex gap-0 border-b border-border">
            {(["active", "library", "presets", "audit"] as Tab[]).map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={cn(
                  "px-3 py-2 text-xs font-medium border-b-2 -mb-px transition-colors whitespace-nowrap",
                  activeTab === tab
                    ? "border-primary text-primary"
                    : "border-transparent text-muted-foreground hover:text-foreground"
                )}
              >
                {TAB_LABELS[tab]}
              </button>
            ))}
          </div>
        </div>

        {/* Tab content */}
        <div className="flex-1 overflow-y-auto px-4 py-4">
          {activeTab === "active" && (
            <ActiveBasketTab
              entries={entries}
              setWeight={setWeight}
              togglePart={togglePart}
              movePartToIndex={movePartToIndex}
              buildFormula={buildFormula}
            />
          )}
          {activeTab === "library" && (
            <PartsLibraryTab entries={entries} togglePart={togglePart} />
          )}
          {activeTab === "presets" && (
            <PresetsTab
              activePresetId={activePresetId}
              applyPreset={applyPreset}
              presets={presets}
            />
          )}
          {activeTab === "audit" && (
            <AuditTab
              entries={entries}
              totalWeight={totalWeight}
              activeCount={activeCount}
              cachedAt={cachedAt}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
