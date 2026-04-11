import { useState, useCallback } from "react";
import { ALPHA_PARTS, ALPHA_PARTS_MAP, AlphaPart, ZScores } from "./alpha-parts-library";

const BASKET_KEY = "qt:basket-v2";

export interface BasketEntry {
  partId: string;
  weight: number;
  active: boolean;
}

export interface AlphaPreset {
  id: string;
  label: string;
  description: string;
  omitNote: string;
  entries: BasketEntry[];
}

export interface PartContribution {
  part: AlphaPart;
  weight: number;
  active: boolean;
  score: number;
  contribution: number;
}

export const ALPHA_PRESETS: AlphaPreset[] = [
  {
    id: "institutional_default",
    label: "Institutional Default",
    description: "Balanced 5-factor model: core momentum, residual momentum, short-term reversal, low vol, and quality. The recommended starting point.",
    omitNote: "Omits value signals and extended quality dimensions (not yet built).",
    entries: [
      { partId: "momentum_core",     weight: 4, active: true },
      { partId: "residual_momentum", weight: 3, active: true },
      { partId: "short_reversal",    weight: 1, active: true },
      { partId: "low_volatility",    weight: 2, active: true },
      { partId: "quality_opa",       weight: 2, active: true },
    ],
  },
  {
    id: "institutional_quality",
    label: "Institutional + Quality",
    description: "Tilts heavier toward quality — doubles the OPA weight relative to the default. Suitable for quality-oriented mandates.",
    omitNote: "Uses approximate OPA quality (operating income / assets with fallbacks). Value not included.",
    entries: [
      { partId: "momentum_core",     weight: 4, active: true },
      { partId: "residual_momentum", weight: 3, active: true },
      { partId: "short_reversal",    weight: 1, active: true },
      { partId: "low_volatility",    weight: 1, active: true },
      { partId: "quality_opa",       weight: 4, active: true },
    ],
  },
  {
    id: "institutional_lowvol",
    label: "Institutional + Low Vol",
    description: "Defensive tilt: doubles low-vol weight to reduce portfolio volatility while keeping momentum exposure.",
    omitNote: "Low-vol signal uses total vol rather than idiosyncratic vol — an approximation.",
    entries: [
      { partId: "momentum_core",     weight: 4, active: true },
      { partId: "residual_momentum", weight: 3, active: true },
      { partId: "short_reversal",    weight: 1, active: true },
      { partId: "low_volatility",    weight: 4, active: true },
      { partId: "quality_opa",       weight: 1, active: true },
    ],
  },
  {
    id: "full_model",
    label: "Full Model",
    description: "All 5 active parts weighted equally. Maximally diversified across the available signal universe.",
    omitNote: "All 5 active signals are approximations to varying degrees. Value and extended quality not yet built.",
    entries: [
      { partId: "momentum_core",     weight: 2, active: true },
      { partId: "residual_momentum", weight: 2, active: true },
      { partId: "short_reversal",    weight: 2, active: true },
      { partId: "low_volatility",    weight: 2, active: true },
      { partId: "quality_opa",       weight: 2, active: true },
    ],
  },
  {
    id: "momentum_only",
    label: "Momentum Only",
    description: "Pure momentum exposure combining core and residual momentum signals. No reversal dampener, no quality, no low-vol.",
    omitNote: "High momentum concentration — may perform poorly in crowded or reversing momentum regimes.",
    entries: [
      { partId: "momentum_core",     weight: 6, active: true  },
      { partId: "residual_momentum", weight: 4, active: true  },
      { partId: "short_reversal",    weight: 0, active: false },
      { partId: "low_volatility",    weight: 0, active: false },
      { partId: "quality_opa",       weight: 0, active: false },
    ],
  },
];

const DEFAULT_ENTRIES: BasketEntry[] = ALPHA_PRESETS[0].entries;

function loadBasket(): BasketEntry[] {
  try {
    const raw = localStorage.getItem(BASKET_KEY);
    if (!raw) return [...DEFAULT_ENTRIES];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [...DEFAULT_ENTRIES];
    const valid = parsed.filter(
      (e: unknown) =>
        e &&
        typeof (e as BasketEntry).partId === "string" &&
        typeof (e as BasketEntry).weight === "number" &&
        typeof (e as BasketEntry).active === "boolean"
    ) as BasketEntry[];
    return valid.length > 0 ? valid : [...DEFAULT_ENTRIES];
  } catch {
    return [...DEFAULT_ENTRIES];
  }
}

function saveBasket(entries: BasketEntry[]): void {
  try {
    localStorage.setItem(BASKET_KEY, JSON.stringify(entries));
  } catch {}
}

function matchesPreset(entries: BasketEntry[], preset: AlphaPreset): boolean {
  const activeEntries = entries.filter((e) => e.active);
  const activePreset  = preset.entries.filter((e) => e.active);
  if (activeEntries.length !== activePreset.length) return false;
  return activePreset.every((pe, i) => {
    const e = activeEntries[i];
    return e && e.partId === pe.partId && e.weight === pe.weight;
  });
}

export function useAlphaBasket() {
  const [entries, setEntries] = useState<BasketEntry[]>(() => loadBasket());
  const [panelOpen, setPanelOpen] = useState(false);

  const persist = useCallback((next: BasketEntry[]) => {
    saveBasket(next);
    setEntries(next);
  }, []);

  const setWeight = useCallback((partId: string, weight: number) => {
    setEntries((prev) => {
      const next = prev.map((e) =>
        e.partId === partId ? { ...e, weight: Math.max(0, Math.round(weight)) } : e
      );
      saveBasket(next);
      return next;
    });
  }, []);

  const togglePart = useCallback((partId: string) => {
    setEntries((prev) => {
      const exists = prev.find((e) => e.partId === partId);
      let next: BasketEntry[];
      if (exists) {
        next = prev.map((e) =>
          e.partId === partId ? { ...e, active: !e.active } : e
        );
      } else {
        const part = ALPHA_PARTS_MAP.get(partId);
        if (!part) return prev;
        next = [...prev, { partId, weight: part.defaultWeight, active: true }];
      }
      saveBasket(next);
      return next;
    });
  }, []);

  /**
   * Reorder only among active entries. The full entries array preserves inactive
   * entries in their original positions; active entries are re-ordered as a group.
   */
  const reorderPart = useCallback((partId: string, direction: "up" | "down") => {
    setEntries((prev) => {
      const activeIds = prev.filter((e) => e.active).map((e) => e.partId);
      const idx = activeIds.indexOf(partId);
      if (idx === -1) return prev;
      if (direction === "up" && idx === 0) return prev;
      if (direction === "down" && idx === activeIds.length - 1) return prev;

      const newActiveIds = [...activeIds];
      if (direction === "up") {
        [newActiveIds[idx - 1], newActiveIds[idx]] = [newActiveIds[idx], newActiveIds[idx - 1]];
      } else {
        [newActiveIds[idx + 1], newActiveIds[idx]] = [newActiveIds[idx], newActiveIds[idx + 1]];
      }

      const activeOrderMap = new Map(newActiveIds.map((id, i) => [id, i]));
      const activeSorted = prev
        .filter((e) => e.active)
        .sort((a, b) => (activeOrderMap.get(a.partId) ?? 0) - (activeOrderMap.get(b.partId) ?? 0));
      const inactive = prev.filter((e) => !e.active);

      const next = [...activeSorted, ...inactive];
      saveBasket(next);
      return next;
    });
  }, []);

  /**
   * Move an active entry to a new position index within the active list (drag-and-drop).
   */
  const movePartToIndex = useCallback((partId: string, toIndex: number) => {
    setEntries((prev) => {
      const activeEntries = prev.filter((e) => e.active);
      const inactiveEntries = prev.filter((e) => !e.active);
      const fromIndex = activeEntries.findIndex((e) => e.partId === partId);
      if (fromIndex === -1 || fromIndex === toIndex) return prev;

      const reordered = [...activeEntries];
      const [moved] = reordered.splice(fromIndex, 1);
      reordered.splice(toIndex, 0, moved);

      const next = [...reordered, ...inactiveEntries];
      saveBasket(next);
      return next;
    });
  }, []);

  const applyPreset = useCallback((presetId: string) => {
    const preset = ALPHA_PRESETS.find((p) => p.id === presetId);
    if (!preset) return;
    persist([...preset.entries]);
  }, [persist]);

  const computeAlpha = useCallback(
    (zScores: ZScores): number => {
      const active = entries.filter((e) => e.active && e.weight > 0);
      const totalW = active.reduce((s, e) => s + e.weight, 0) || 1;
      return active.reduce((sum, e) => {
        const part = ALPHA_PARTS_MAP.get(e.partId);
        if (!part) return sum;
        return sum + (e.weight / totalW) * part.compute(zScores);
      }, 0);
    },
    [entries]
  );

  const getContributions = useCallback(
    (zScores: ZScores): PartContribution[] => {
      const activeEntries = entries.filter((e) => e.active && e.weight > 0);
      const totalW = activeEntries.reduce((s, e) => s + e.weight, 0) || 1;
      return entries.map((entry) => {
        const part = ALPHA_PARTS_MAP.get(entry.partId);
        if (!part) return null;
        const score = entry.active ? part.compute(zScores) : 0;
        const contribution =
          entry.active && entry.weight > 0 ? (entry.weight / totalW) * score : 0;
        return { part, weight: entry.weight, active: entry.active, score, contribution };
      }).filter((x): x is PartContribution => x !== null);
    },
    [entries]
  );

  const activePresetId = ALPHA_PRESETS.find((p) => matchesPreset(entries, p))?.id ?? null;

  const totalWeight = entries
    .filter((e) => e.active && e.weight > 0)
    .reduce((s, e) => s + e.weight, 0);

  const activeCount = entries.filter((e) => e.active && e.weight > 0).length;

  const buildFormula = useCallback((): string => {
    const active = entries.filter((e) => e.active && e.weight > 0);
    const totalW = active.reduce((s, e) => s + e.weight, 0) || 1;
    if (active.length === 0) return "α = 0";
    const terms = active.map((e) => {
      const part = ALPHA_PARTS_MAP.get(e.partId);
      const pct = ((e.weight / totalW) * 100).toFixed(0) + "%";
      return `${pct}·${part?.shortLabel ?? e.partId}`;
    });
    return "α = " + terms.join(" + ");
  }, [entries]);

  return {
    entries,
    setWeight,
    togglePart,
    reorderPart,
    movePartToIndex,
    applyPreset,
    computeAlpha,
    getContributions,
    activePresetId,
    totalWeight,
    activeCount,
    buildFormula,
    allParts: ALPHA_PARTS,
    presets: ALPHA_PRESETS,
    panelOpen,
    setPanelOpen,
  };
}
