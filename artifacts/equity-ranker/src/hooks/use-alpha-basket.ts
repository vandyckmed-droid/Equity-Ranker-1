/**
 * Alpha Basket System — Signal Parts Library + Recipe Engine
 *
 * Three layers:
 *   1. ALPHA_PARTS      — fixed, auditable definitions of each alpha component
 *   2. useAlphaBasket() — recipe engine (which parts are active, what weight)
 *   3. UI layer         — AlphaBasketButton (separate component)
 *
 * Every signal follows exactly one transformation chain:
 *   compute raw metric → winsorize (2%/98%) → cross-sectional z-score
 * (The engine handles winsorize+zscore; here we only combine the resulting z-scores.)
 */

import { useCallback, useEffect, useState } from "react";

// ─── Types ────────────────────────────────────────────────────────────────────

export type PartStatus =
  | "institutional_core"
  | "institutional_approx"
  | "research_extension"
  | "future";

export type PartCategory =
  | "momentum"
  | "residual"
  | "quality"
  | "risk"
  | "research";

export interface SubSignal {
  key: string;   // z-score field name, e.g. "zM6"
  label: string; // display label
}

export interface AlphaPart {
  id: string;
  label: string;
  shortLabel: string;
  category: PartCategory;
  description: string;
  displayFormula: string;
  variableDefinitions: string[];
  status: PartStatus;
  defaultWeight: number;
  /** Sub-signals shown in the detail panel breakdown (composite parts only). */
  subSignals?: SubSignal[];
  /** Compute the part's raw score from z-scores. Returns 0 if inputs are missing. */
  compute: (z: Partial<Record<string, number | null | undefined>>) => number;
}

export interface BasketItem {
  partId: string;
  weight: number;
  active: boolean;
}

export interface AlphaPreset {
  id: string;
  label: string;
  description: string;
  omissionNote: string;
  items: BasketItem[];
}

export interface PartContribution {
  part: AlphaPart;
  weight: number;
  active: boolean;
  score: number;
  /** weight_fraction × score — the part's weighted contribution to final alpha */
  contribution: number;
}

// ─── Signal Parts Library ─────────────────────────────────────────────────────

export const ALPHA_PARTS: AlphaPart[] = [
  {
    id: "momentum_core",
    label: "Core Momentum 12-1",
    shortLabel: "Core Mom",
    category: "momentum",
    description:
      "Equal-weight blend of 6-month and 12-month skip-adjusted cumulative log-returns " +
      "plus their OLS trend t-statistics. The t-statistics add signal robustness by " +
      "rewarding consistent price drift rather than a single large move.",
    displayFormula: "¼(zM6 + zM12 + zT6 + zT12)",
    variableDefinitions: [
      "zM6  = Z(Σ log-ret, t-126:t-22)   — 6-month cumulative return (skip-adjusted)",
      "zM12 = Z(Σ log-ret, t-252:t-22)   — 12-month cumulative return (skip-adjusted)",
      "zT6  = Z(OLS β/SE[β], ln(P)~t, 126d)  — 6-month trend quality",
      "zT12 = Z(OLS β/SE[β], ln(P)~t, 252d)  — 12-month trend quality",
    ],
    status: "institutional_core",
    defaultWeight: 4,
    subSignals: [
      { key: "zM6",  label: "zM6"  },
      { key: "zM12", label: "zM12" },
      { key: "zT6",  label: "zT6"  },
      { key: "zT12", label: "zT12" },
    ],
    compute: (z) =>
      0.25 * (z.zM6  ?? 0) +
      0.25 * (z.zM12 ?? 0) +
      0.25 * (z.zT6  ?? 0) +
      0.25 * (z.zT12 ?? 0),
  },
  {
    id: "residual_momentum",
    label: "Residual Momentum 12-1",
    shortLabel: "Resid Mom",
    category: "residual",
    description:
      "Momentum in returns after removing market and industry/sector co-movement. " +
      "Captures stock-specific alpha accumulation that is less sensitive to sector rotations. " +
      "Regression uses OLS with intercept for beta estimation; residuals retain the alpha term.",
    displayFormula: "0.4·zR6 + 0.6·zR12",
    variableDefinitions: [
      "r_i = α + β_m·r_mkt + β_p·r_peer + ε̃  (OLS with intercept)",
      "ε   = r_i − β_m·r_mkt − β_p·r_peer    (betas-only residual, alpha retained)",
      "zR6  = Z(Σ ε, t-126:t-22)  — 6-month cumulative residual",
      "zR12 = Z(Σ ε, t-252:t-22)  — 12-month cumulative residual",
    ],
    status: "institutional_approx",
    defaultWeight: 3,
    subSignals: [
      { key: "zR6",  label: "zR6"  },
      { key: "zR12", label: "zR12" },
    ],
    compute: (z) =>
      0.4 * (z.zR6  ?? 0) +
      0.6 * (z.zR12 ?? 0),
  },
  {
    id: "short_reversal",
    label: "Short-Term Reversal",
    shortLabel: "Reversal",
    category: "momentum",
    description:
      "Fades last-month winners. Standard institutional alpha component paired with " +
      "cross-sectional momentum — prevents doubling up on the same recent move that " +
      "the 12-1 momentum window already captures.",
    displayFormula: "−zM1",
    variableDefinitions: [
      "zM1 = Z(Σ log-ret, t-21:t)  — 1-month cumulative return",
      "Signal enters α with a negative sign (reversal)",
    ],
    status: "institutional_core",
    defaultWeight: 1,
    subSignals: [
      { key: "zM1", label: "zM1" },
    ],
    compute: (z) => -(z.zM1 ?? 0),
  },
  {
    id: "low_volatility",
    label: "Low Volatility",
    shortLabel: "Low Vol",
    category: "risk",
    description:
      "Favors stocks with lower realized short-term volatility. Captures the low-vol " +
      "anomaly — lower-risk stocks have historically delivered risk-adjusted outperformance. " +
      "Uses 60-day realized std, no smoothing or floor.",
    displayFormula: "−Z(σ₆₀)",
    variableDefinitions: [
      "σ₆₀ = std(log-ret[-60:]) × √252  — 60-day annualized realized volatility",
      "zLowVol = −Z(winsorize(σ₆₀))     — sign flipped: lower vol → higher score",
    ],
    status: "institutional_approx",
    defaultWeight: 2,
    subSignals: [
      { key: "zLowVol", label: "zLowVol" },
    ],
    compute: (z) => z.zLowVol ?? 0,
  },
  {
    id: "quality_opa",
    label: "Profitability Proxy",
    shortLabel: "Profitability",
    category: "quality",
    description:
      "Operating income over average total assets. A partial quality signal — covers " +
      "profitability only. An institutional quality factor would additionally incorporate " +
      "investment (asset growth), accruals, and earnings stability.",
    displayFormula: "zOPA",
    variableDefinitions: [
      "OPA = Operating Income / Average Total Assets",
      "zOPA = Z(winsorize(OPA, 2%, 98%))",
      "Note: partial quality proxy. Fama-French quality adds investment and accruals.",
    ],
    status: "institutional_approx",
    defaultWeight: 2,
    subSignals: [
      { key: "zOPA", label: "zOPA" },
    ],
    compute: (z) => z.zOPA ?? 0,
  },
];

export const ALPHA_PARTS_MAP: Record<string, AlphaPart> =
  Object.fromEntries(ALPHA_PARTS.map((p) => [p.id, p]));

// ─── Presets ──────────────────────────────────────────────────────────────────

export const ALPHA_PRESETS: AlphaPreset[] = [
  {
    id: "institutional_default",
    label: "Institutional Default",
    description: "Cross-sectional momentum + residual momentum + reversal skip.",
    omissionNote:
      "Excludes quality and low-vol. Value signals not yet built. " +
      "Residual uses market+peer approximation, not a full factor model.",
    items: [
      { partId: "momentum_core",     weight: 4, active: true  },
      { partId: "residual_momentum", weight: 3, active: true  },
      { partId: "short_reversal",    weight: 1, active: true  },
      { partId: "low_volatility",    weight: 0, active: false },
      { partId: "quality_opa",       weight: 0, active: false },
    ],
  },
  {
    id: "institutional_quality",
    label: "Institutional + Quality",
    description: "Institutional core with a profitability overlay.",
    omissionNote:
      "Quality is a partial OPA proxy — no investment or accruals term. Value not yet built.",
    items: [
      { partId: "momentum_core",     weight: 4, active: true  },
      { partId: "residual_momentum", weight: 3, active: true  },
      { partId: "short_reversal",    weight: 1, active: true  },
      { partId: "low_volatility",    weight: 0, active: false },
      { partId: "quality_opa",       weight: 2, active: true  },
    ],
  },
  {
    id: "institutional_lowvol",
    label: "Institutional + Low Vol",
    description: "Institutional core with a defensive volatility tilt.",
    omissionNote: "Quality and value not included.",
    items: [
      { partId: "momentum_core",     weight: 4, active: true  },
      { partId: "residual_momentum", weight: 3, active: true  },
      { partId: "short_reversal",    weight: 1, active: true  },
      { partId: "low_volatility",    weight: 2, active: true  },
      { partId: "quality_opa",       weight: 0, active: false },
    ],
  },
  {
    id: "full_model",
    label: "Full Model",
    description: "All five available parts active. Balanced institutional recipe.",
    omissionNote:
      "Value not yet built. Quality is partial proxy. " +
      "Residual is market+peer approximation, not a full Barra-style factor model.",
    items: [
      { partId: "momentum_core",     weight: 4, active: true },
      { partId: "residual_momentum", weight: 3, active: true },
      { partId: "short_reversal",    weight: 1, active: true },
      { partId: "low_volatility",    weight: 2, active: true },
      { partId: "quality_opa",       weight: 2, active: true },
    ],
  },
  {
    id: "momentum_only",
    label: "Momentum Only",
    description: "Pure cross-sectional momentum. Clean and simple benchmark.",
    omissionNote: "No quality, vol, reversal, or residual adjustment.",
    items: [
      { partId: "momentum_core",     weight: 1, active: true  },
      { partId: "residual_momentum", weight: 0, active: false },
      { partId: "short_reversal",    weight: 0, active: false },
      { partId: "low_volatility",    weight: 0, active: false },
      { partId: "quality_opa",       weight: 0, active: false },
    ],
  },
];

// ─── Helpers ──────────────────────────────────────────────────────────────────

const BASKET_KEY = "qt:basket-v2";

function defaultBasket(): BasketItem[] {
  return ALPHA_PRESETS.find((p) => p.id === "full_model")!.items.map((i) => ({
    ...i,
  }));
}

function loadBasket(): BasketItem[] {
  try {
    const raw = localStorage.getItem(BASKET_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as BasketItem[];
      const knownIds = ALPHA_PARTS.map((p) => p.id);
      if (
        Array.isArray(parsed) &&
        knownIds.every((id) => parsed.some((i) => i.partId === id))
      ) {
        return parsed;
      }
    }
  } catch {}
  return defaultBasket();
}

export function getActivePresetId(basket: BasketItem[]): string | null {
  for (const preset of ALPHA_PRESETS) {
    if (preset.items.length !== basket.length) continue;
    const match = preset.items.every((pi) => {
      const bi = basket.find((b) => b.partId === pi.partId);
      return bi && bi.active === pi.active && bi.weight === pi.weight;
    });
    if (match) return preset.id;
  }
  return null;
}

// ─── Recipe Hook ──────────────────────────────────────────────────────────────

export function useAlphaBasket() {
  const [basket, setBasket] = useState<BasketItem[]>(() => loadBasket());

  useEffect(() => {
    try {
      localStorage.setItem(BASKET_KEY, JSON.stringify(basket));
    } catch {}
  }, [basket]);

  const setWeight = useCallback((partId: string, weight: number) => {
    setBasket((prev) =>
      prev.map((i) =>
        i.partId === partId ? { ...i, weight: Math.max(0, Math.round(weight)) } : i
      )
    );
  }, []);

  const togglePart = useCallback((partId: string) => {
    setBasket((prev) =>
      prev.map((i) => (i.partId === partId ? { ...i, active: !i.active } : i))
    );
  }, []);

  const moveUp = useCallback((partId: string) => {
    setBasket((prev) => {
      const idx = prev.findIndex((i) => i.partId === partId);
      if (idx <= 0) return prev;
      const next = [...prev];
      [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]];
      return next;
    });
  }, []);

  const moveDown = useCallback((partId: string) => {
    setBasket((prev) => {
      const idx = prev.findIndex((i) => i.partId === partId);
      if (idx < 0 || idx >= prev.length - 1) return prev;
      const next = [...prev];
      [next[idx], next[idx + 1]] = [next[idx + 1], next[idx]];
      return next;
    });
  }, []);

  const applyPreset = useCallback((presetId: string) => {
    const preset = ALPHA_PRESETS.find((p) => p.id === presetId);
    if (!preset) return;
    setBasket(preset.items.map((i) => ({ ...i })));
  }, []);

  const computeAlpha = useCallback(
    (zScores: Partial<Record<string, number | null | undefined>>): number => {
      const activeItems = basket.filter((i) => i.active && i.weight > 0);
      const totalW = activeItems.reduce((s, i) => s + i.weight, 0) || 1;
      return activeItems.reduce((acc, item) => {
        const part = ALPHA_PARTS_MAP[item.partId];
        if (!part) return acc;
        return acc + (item.weight / totalW) * part.compute(zScores);
      }, 0);
    },
    [basket]
  );

  const getContributions = useCallback(
    (
      zScores: Partial<Record<string, number | null | undefined>>
    ): PartContribution[] => {
      const totalW =
        basket
          .filter((i) => i.active && i.weight > 0)
          .reduce((s, i) => s + i.weight, 0) || 1;
      return basket
        .map((item) => {
          const part = ALPHA_PARTS_MAP[item.partId];
          if (!part) return null;
          const score = item.active ? part.compute(zScores) : 0;
          const contribution =
            item.active && item.weight > 0 ? (item.weight / totalW) * score : 0;
          return { part, weight: item.weight, active: item.active, score, contribution };
        })
        .filter((x): x is PartContribution => x !== null);
    },
    [basket]
  );

  const activePresetId = getActivePresetId(basket);
  const totalWeight = basket
    .filter((i) => i.active && i.weight > 0)
    .reduce((s, i) => s + i.weight, 0);
  const activeCount = basket.filter((i) => i.active && i.weight > 0).length;

  return {
    basket,
    computeAlpha,
    getContributions,
    setWeight,
    togglePart,
    moveUp,
    moveDown,
    applyPreset,
    activePresetId,
    totalWeight,
    activeCount,
  };
}
