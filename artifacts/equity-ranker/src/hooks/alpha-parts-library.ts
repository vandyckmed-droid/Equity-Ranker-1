export type PartStatus =
  | "institutional_core"
  | "institutional_approximation"
  | "research_extension"
  | "future";

export type PartCategory = "Momentum" | "Residual" | "Quality" | "Risk" | "Research";

export interface SubSignal {
  key: string;
  label: string;
}

export interface ZScores {
  zM6?:     number | null;
  zM12?:    number | null;
  zT6?:     number | null;
  zT12?:    number | null;
  zR6?:     number | null;
  zR12?:    number | null;
  zM1?:     number | null;
  zLowVol?: number | null;
  zOPA?:    number | null;
  zRam6?:   number | null;
  zRam12?:  number | null;
  zRam1?:   number | null;
  [key: string]: number | null | undefined;
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
  statusLabel: string;
  defaultWeight: number;
  subSignals?: SubSignal[];
  compute: (z: ZScores) => number;
}

function n(v: number | null | undefined): number {
  return v ?? 0;
}

export const ALPHA_PARTS: AlphaPart[] = [
  // ── Core 9 building blocks ────────────────────────────────────────────────

  {
    id: "mom_12_1",
    label: "MOM_12-1",
    shortLabel: "MOM_12-1",
    category: "Momentum",
    description:
      "Baseline medium-term momentum. Cumulative log-return over the prior 12 months, " +
      "skipping the most recent month to avoid microstructure reversal. " +
      "The foundational cross-sectional momentum signal.",
    displayFormula: "MOM_12\u20111 = Z(\u03A3 ln P, t\u2212252 : t\u221222)",
    variableDefinitions: [
      "MOM_12\u20111 = Z(winsorize(\u03A3 log_ret[t\u2212252 : t\u221222]))",
      "Skip month prevents doubling up on 1-month microstructure reversal",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zM12", label: "MOM_12-1" }],
    compute: (z) => n(z.zM12),
  },
  {
    id: "ram_12_1",
    label: "RAM_12-1",
    shortLabel: "RAM_12-1",
    category: "Momentum",
    description:
      "Volatility-adjusted 12-month momentum. Divides the 12-1 cumulative return by " +
      "realized 63-day annualized volatility (floored at 15%), producing a Sharpe-style " +
      "signal that rewards consistent risk-adjusted drift. Usually the best default core sleeve.",
    displayFormula: "RAM_12\u20111 = Z(MOM_12\u20111 / max(\u03C3\u2086\u2083, 0.15))",
    variableDefinitions: [
      "\u03C3\u2086\u2083 = std(log_ret[\u221263:]) \u00D7 \u221A252  (3-month realized vol, annualized)",
      "vol floor = 0.15 prevents division by near-zero vol in low-activity stocks",
      "RAM_12\u20111 = Z(winsorize(m12 / max(\u03C3\u2086\u2083, 0.15)))",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zRam12", label: "RAM_12-1" }],
    compute: (z) => n(z.zRam12),
  },
  {
    id: "rm_12_1",
    label: "RM_12-1",
    shortLabel: "RM_12-1",
    category: "Residual",
    description:
      "Medium-term residual momentum after removing market and sector/industry co-movement. " +
      "Isolates stock-specific alpha accumulation and is less sensitive to sector rotations. " +
      "Good for adding signal that diversifies against broad momentum crowding.",
    displayFormula: "RM_12\u20111 = Z(\u03A3 \u03B5, t\u2212252 : t\u221222)",
    variableDefinitions: [
      "r_i = \u03B1 + \u03B2_m\u00B7r_mkt + \u03B2_p\u00B7r_peer + \u03B5\u0303  (OLS with intercept)",
      "\u03B5 = r_i \u2212 \u03B2_m\u00B7r_mkt \u2212 \u03B2_p\u00B7r_peer  (betas-only residual; \u03B1 retained)",
      "RM_12\u20111 = Z(winsorize(\u03A3 \u03B5[t\u2212252 : t\u221222]))",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zR12", label: "RM_12-1" }],
    compute: (z) => n(z.zR12),
  },
  {
    id: "ts_12",
    label: "TS_12",
    shortLabel: "TS_12",
    category: "Momentum",
    description:
      "Trend-strength over 12 months via the t-statistic of the OLS log-price trend. " +
      "Rewards persistence and smoothness of a price trend rather than just the total return magnitude. " +
      "A good complement to return-based momentum signals.",
    displayFormula: "TS_12 = Z(OLS \u03B2/SE[\u03B2], ln(P) ~ t, 252d)",
    variableDefinitions: [
      "Fit ln(P_t) = \u03B1 + \u03B2\u00B7t + \u03B5 by OLS over the past 252 trading days",
      "TS_12 = Z(winsorize(\u03B2 / SE[\u03B2]))  — the slope t-statistic",
      "High t-stat \u21D2 smooth, persistent uptrend; low t-stat \u21D2 noisy or stalling price",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zT12", label: "TS_12" }],
    compute: (z) => n(z.zT12),
  },
  {
    id: "prof",
    label: "PROF",
    shortLabel: "PROF",
    category: "Quality",
    description:
      "Operating profitability / average assets with a fallback hierarchy (operating income \u2192 EBIT \u2192 net income). " +
      "A non-momentum quality anchor that tends to be uncorrelated with momentum signals " +
      "and adds diversification, particularly in momentum-reversal regimes.",
    displayFormula: "PROF = Z(OPA)",
    variableDefinitions: [
      "OPA = Operating Income / Average Total Assets",
      "Fallback: EBIT/avg_assets, then net_income/avg_assets",
      "PROF = Z(winsorize(OPA, 2%, 98%))",
    ],
    status: "institutional_approximation",
    statusLabel: "Institutional Approximation",
    defaultWeight: 1,
    subSignals: [{ key: "zOPA", label: "PROF" }],
    compute: (z) => n(z.zOPA),
  },
  {
    id: "mom_6_1",
    label: "MOM_6-1",
    shortLabel: "MOM_6-1",
    category: "Momentum",
    description:
      "More responsive medium-horizon momentum. Captures the same skip-adjusted return " +
      "concept as MOM_12-1 but over a 6-month window, reacting faster to recent price trends. " +
      "Useful when you want a shorter horizon complement to the 12-month signal.",
    displayFormula: "MOM_6\u20111 = Z(\u03A3 ln P, t\u2212126 : t\u221222)",
    variableDefinitions: [
      "MOM_6\u20111 = Z(winsorize(\u03A3 log_ret[t\u2212126 : t\u221222]))",
      "Skip month consistent with MOM_12-1 convention",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zM6", label: "MOM_6-1" }],
    compute: (z) => n(z.zM6),
  },
  {
    id: "ram_6_1",
    label: "RAM_6-1",
    shortLabel: "RAM_6-1",
    category: "Momentum",
    description:
      "Faster volatility-adjusted momentum sleeve. The same Sharpe-style construction as " +
      "RAM_12-1 but over a 6-month horizon. Useful paired with RAM_12-1 to span the medium " +
      "momentum horizon while rewarding consistency over pure magnitude.",
    displayFormula: "RAM_6\u20111 = Z(MOM_6\u20111 / max(\u03C3\u2086\u2083, 0.15))",
    variableDefinitions: [
      "\u03C3\u2086\u2083 = std(log_ret[\u221263:]) \u00D7 \u221A252  (same 3-month vol window as RAM_12-1)",
      "RAM_6\u20111 = Z(winsorize(m6 / max(\u03C3\u2086\u2083, 0.15)))",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zRam6", label: "RAM_6-1" }],
    compute: (z) => n(z.zRam6),
  },
  {
    id: "rm_6_1",
    label: "RM_6-1",
    shortLabel: "RM_6-1",
    category: "Residual",
    description:
      "Faster residual momentum sleeve. Captures stock-specific drift after removing " +
      "market and sector co-movement over a 6-month window. Useful when you want " +
      "responsiveness without going fully into short-term noise.",
    displayFormula: "RM_6\u20111 = Z(\u03A3 \u03B5, t\u2212126 : t\u221222)",
    variableDefinitions: [
      "Same OLS residual construction as RM_12-1, shorter 6-month window",
      "RM_6\u20111 = Z(winsorize(\u03A3 \u03B5[t\u2212126 : t\u221222]))",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zR6", label: "RM_6-1" }],
    compute: (z) => n(z.zR6),
  },
  {
    id: "rev_ram_1",
    label: "REV (RAM_1)",
    shortLabel: "REV",
    category: "Momentum",
    description:
      "Short-term reversal using the volatility-adjusted 1-month return, entered with a " +
      "negative sign. Fades last-month winners after adjusting for vol, preventing doubling " +
      "up on the same recent move already captured by the skip-adjusted momentum signals.",
    displayFormula: "REV = \u2212RAM_1 = \u2212Z(MOM_1 / max(\u03C3\u2086\u2083, 0.15))",
    variableDefinitions: [
      "MOM_1 = \u03A3 log_ret[t\u221221 : t]  (last-month return, no skip)",
      "RAM_1 = MOM_1 / max(\u03C3\u2086\u2083, 0.15)  (vol-adjusted; same \u03C3\u2086\u2083 denominator)",
      "Signal enters \u03B1 with a negative sign (reversal: fade last-month winners)",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zRam1", label: "RAM_1" }],
    compute: (z) => -n(z.zRam1),
  },

  // ── Additional library parts (not in default basket) ─────────────────────

  {
    id: "low_volatility",
    label: "Low Volatility",
    shortLabel: "LowVol",
    category: "Risk",
    description:
      "Favors stocks with lower 60-day realized volatility. Captures the low-volatility anomaly \u2014 " +
      "lower-risk stocks have historically delivered risk-adjusted outperformance. " +
      "Note: this uses total vol, not idiosyncratic vol \u2014 a cleaner version would partial out market beta first.",
    displayFormula: "LV = \u2212Z(\u03C3\u2086\u2080)",
    variableDefinitions: [
      "\u03C3\u2086\u2080 = std(log_ret[\u221260:]) \u00D7 \u221A252  (60-day realized vol, annualized)",
      "LowVol = \u2212Z(winsorize(\u03C3\u2086\u2080))  (sign flipped: lower vol \u2192 higher score)",
    ],
    status: "institutional_approximation",
    statusLabel: "Institutional Approximation",
    defaultWeight: 1,
    subSignals: [{ key: "zLowVol", label: "LowVol" }],
    compute: (z) => n(z.zLowVol),
  },
  {
    id: "ts_6",
    label: "TS_6",
    shortLabel: "TS_6",
    category: "Momentum",
    description:
      "Trend-strength over 6 months via the OLS log-price trend t-statistic. " +
      "A shorter-horizon complement to TS_12, more responsive to recent price persistence.",
    displayFormula: "TS_6 = Z(OLS \u03B2/SE[\u03B2], ln(P) ~ t, 126d)",
    variableDefinitions: [
      "Fit ln(P_t) = \u03B1 + \u03B2\u00B7t + \u03B5 by OLS over the past 126 trading days",
      "TS_6 = Z(winsorize(\u03B2 / SE[\u03B2]))",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zT6", label: "TS_6" }],
    compute: (z) => n(z.zT6),
  },
  {
    id: "rev_mom_1",
    label: "REV (MOM_1)",
    shortLabel: "REV\u2082",
    category: "Momentum",
    description:
      "Short-term reversal using the raw (non-vol-adjusted) 1-month return, entered with a " +
      "negative sign. Simpler alternative to REV (RAM_1). Prefer the RAM variant for a " +
      "cleaner signal; use this when you want to compare raw vs vol-adjusted reversal.",
    displayFormula: "REV = \u2212MOM_1",
    variableDefinitions: [
      "MOM_1 = Z(\u03A3 log_ret[t\u221221 : t])  (last-month return, no skip, no vol adjustment)",
      "Signal enters \u03B1 with a negative sign (reversal)",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [{ key: "zM1", label: "MOM_1" }],
    compute: (z) => -n(z.zM1),
  },
  {
    id: "value_stub",
    label: "Value (Future)",
    shortLabel: "Value",
    category: "Research",
    description:
      "Placeholder for a value signal (e.g., earnings yield, book-to-price). Not yet implemented \u2014 requires fundamental data pipeline expansion.",
    displayFormula: "V = (not yet built)",
    variableDefinitions: [],
    status: "future",
    statusLabel: "Future",
    defaultWeight: 0,
    compute: () => 0,
  },
];

export const ALPHA_PARTS_MAP: Map<string, AlphaPart> = new Map(
  ALPHA_PARTS.map((p) => [p.id, p])
);

export const STATUS_BADGE_STYLES: Record<PartStatus, { label: string; cls: string }> = {
  institutional_core:         { label: "Institutional Core",         cls: "bg-emerald-950/60 text-emerald-400 border-emerald-700/40" },
  institutional_approximation:{ label: "Institutional Approximation", cls: "bg-amber-950/60  text-amber-400  border-amber-700/40"  },
  research_extension:         { label: "Research Extension",          cls: "bg-violet-950/60 text-violet-400 border-violet-700/40" },
  future:                     { label: "Future",                      cls: "bg-muted/60       text-muted-foreground border-border/40" },
};
