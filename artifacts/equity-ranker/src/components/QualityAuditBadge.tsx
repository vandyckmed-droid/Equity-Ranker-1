import { useState, useRef, useEffect } from "react";
import { X } from "lucide-react";
import { cn } from "@/lib/utils";
import type { UniverseAudit } from "lib/api-client-react/src/generated/api.schemas";

interface Props {
  audit: UniverseAudit | null | undefined;
}

const FIELD_LABELS: Record<string, string> = {
  roe: "ROE",
  roa: "ROA",
  grossMargin: "GrossM",
  opMargin: "OpM",
  deRatio: "D/E",
};
const FIELD_ORDER = ["roe", "roa", "grossMargin", "opMargin", "deRatio"];

export function QualityAuditBadge({ audit }: Props) {
  const [open, setOpen] = useState(false);
  const [dropdownPos, setDropdownPos] = useState({ top: 0, left: 0 });
  const btnRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!open) return;
    const rect = btnRef.current?.getBoundingClientRect();
    if (rect) {
      setDropdownPos({ top: rect.bottom + 6, left: rect.left });
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const close = () => setOpen(false);
    document.addEventListener("pointerdown", close, { capture: true });
    return () => document.removeEventListener("pointerdown", close, { capture: true });
  }, [open]);

  if (!audit) return null;

  const pct = audit.qualityPct ?? 0;
  const dist = audit.qualityInputDist ?? {};
  const missingRates = audit.qualityFieldMissingRates ?? {};
  const total = Object.values(dist).reduce((a, b) => a + b, 0);

  const badgeCls =
    pct >= 80
      ? "text-emerald-400 border-emerald-500/25 bg-emerald-500/10"
      : pct >= 60
      ? "text-amber-400 border-amber-500/25 bg-amber-500/10"
      : "text-rose-400 border-rose-500/25 bg-rose-500/10";

  return (
    <>
      <button
        ref={btnRef}
        onClick={(e) => {
          e.stopPropagation();
          setOpen((v) => !v);
        }}
        className={cn(
          "inline-flex items-center h-5 rounded-full px-2 text-[11px] border whitespace-nowrap shrink-0 transition-colors cursor-pointer",
          badgeCls,
        )}
      >
        Q {pct}%
      </button>

      {open && (
        <div
          onPointerDown={(e) => e.stopPropagation()}
          style={{
            position: "fixed",
            top: dropdownPos.top,
            left: dropdownPos.left,
            zIndex: 9999,
            minWidth: 288,
          }}
          className="w-72 rounded-lg border border-border/60 bg-background/95 backdrop-blur-sm shadow-xl p-3 text-[10px] font-mono"
        >
          {/* Header */}
          <div className="flex items-center justify-between mb-2.5">
            <p className="text-[9px] uppercase tracking-wider text-muted-foreground font-sans font-semibold">
              Quality Coverage · Universe
            </p>
            <button
              onClick={() => setOpen(false)}
              className="text-muted-foreground/60 hover:text-foreground transition-colors"
            >
              <X className="w-3 h-3" />
            </button>
          </div>

          <div className="grid grid-cols-2 gap-x-4">
            {/* Input-count distribution */}
            <div>
              <p className="text-[8.5px] uppercase tracking-wider text-muted-foreground/70 font-sans mb-1.5">
                Inputs / stock
              </p>
              {[5, 4, 3, 2, 1, 0].map((n) => {
                const count = dist[String(n)] ?? 0;
                const barW = total > 0 ? Math.round((count / total) * 100) : 0;
                const confCls =
                  n >= 5
                    ? "bg-emerald-500/60"
                    : n >= 3
                    ? "bg-amber-500/50"
                    : "bg-rose-500/40";
                const labelCls =
                  n >= 5
                    ? "text-emerald-400"
                    : n >= 3
                    ? "text-amber-400"
                    : "text-rose-400";
                return (
                  <div key={n} className="flex items-center gap-1.5 mb-0.5">
                    <span className={cn("w-4 text-right shrink-0", labelCls)}>
                      {n}/5
                    </span>
                    <div className="flex-1 bg-muted/30 rounded-full h-1.5 overflow-hidden">
                      <div
                        className={cn("h-full rounded-full", confCls)}
                        style={{ width: `${barW}%` }}
                      />
                    </div>
                    <span className="w-8 text-right text-muted-foreground/70 shrink-0">
                      {count}
                    </span>
                  </div>
                );
              })}
            </div>

            {/* Per-field missing rates */}
            <div>
              <p className="text-[8.5px] uppercase tracking-wider text-muted-foreground/70 font-sans mb-1.5">
                Field miss rate
              </p>
              {FIELD_ORDER.map((field) => {
                const rate = missingRates[field];
                const hasRate = rate != null;
                const barW = hasRate ? Math.round(rate) : 0;
                const rateCls =
                  !hasRate
                    ? "text-muted-foreground/30"
                    : rate <= 10
                    ? "text-emerald-400"
                    : rate <= 30
                    ? "text-amber-400"
                    : "text-rose-400";
                const barCls =
                  rate <= 10
                    ? "bg-emerald-500/50"
                    : rate <= 30
                    ? "bg-amber-500/50"
                    : "bg-rose-500/50";
                return (
                  <div key={field} className="flex items-center gap-1.5 mb-0.5">
                    <span className="text-muted-foreground/70 w-10 shrink-0">
                      {FIELD_LABELS[field]}
                    </span>
                    <div className="flex-1 bg-muted/30 rounded-full h-1.5 overflow-hidden">
                      <div
                        className={cn("h-full rounded-full", barCls)}
                        style={{ width: `${barW}%` }}
                      />
                    </div>
                    <span className={cn("w-7 text-right shrink-0", rateCls)}>
                      {hasRate ? `${rate}%` : "—"}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Legend */}
          <div className="flex items-center gap-3 mt-2.5 pt-2 border-t border-border/25 text-[8.5px] text-muted-foreground/60 font-sans">
            <span className="text-emerald-400">■</span> High (5/5)
            <span className="text-amber-400">■</span> Med (3-4/5)
            <span className="text-rose-400">■</span> Low (0-2/5)
          </div>
        </div>
      )}
    </>
  );
}
