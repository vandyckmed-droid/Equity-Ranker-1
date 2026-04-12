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
  zM6?:    number | null;
  zM12?:   number | null;
  zT6?:    number | null;
  zT12?:   number | null;
  zR6?:    number | null;
  zR12?:   number | null;
  zM1?:    number | null;
  zLowVol?: number | null;
  zOPA?:   number | null;
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
  {
    id: "momentum_core",
    label: "Momentum Composite",
    shortLabel: "MOM",
    category: "Momentum",
    description:
      "Equal-weight blend of 6-month and 12-month skip-adjusted cumulative log-returns " +
      "plus their OLS trend t-statistics. The t-statistics add signal robustness by " +
      "rewarding consistent price drift rather than a single large move.",
    displayFormula: "M = \u00BC\u00B7(MOM_6\u20111 + MOM_12\u20111 + TS_6\u20111 + TS_12\u20111)",
    variableDefinitions: [
      "MOM_6\u20111 = Z(\u03A3 ln(P_t/P_{t\u22121}), t\u2212126 : t\u221222)",
      "MOM_12\u20111 = Z(\u03A3 ln(P_t/P_{t\u22121}), t\u2212252 : t\u221222)",
      "TS_6\u20111 = Z(OLS \u03B2/SE[\u03B2], ln(P)~t, 126d)",
      "TS_12\u20111 = Z(OLS \u03B2/SE[\u03B2], ln(P)~t, 252d)",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 4,
    subSignals: [
      { key: "zM6",  label: "MOM 6\u20111"  },
      { key: "zM12", label: "MOM 12\u20111" },
      { key: "zT6",  label: "TS 6\u20111"   },
      { key: "zT12", label: "TS 12\u20111"  },
    ],
    compute: (z) =>
      0.25 * n(z.zM6) + 0.25 * n(z.zM12) + 0.25 * n(z.zT6) + 0.25 * n(z.zT12),
  },
  {
    id: "residual_momentum",
    label: "Residual Momentum",
    shortLabel: "RM",
    category: "Residual",
    description:
      "Momentum in returns after removing market and industry/sector co-movement. " +
      "Captures stock-specific alpha accumulation that is less sensitive to sector rotations. " +
      "Regression uses OLS with intercept for beta estimation; residuals retain the alpha term.",
    displayFormula: "RM = 0.4\u00B7RM_6\u20111 + 0.6\u00B7RM_12\u20111",
    variableDefinitions: [
      "r_i = \u03B1 + \u03B2_m\u00B7r_mkt + \u03B2_p\u00B7r_peer + \u03B5\u0303  (OLS with intercept)",
      "\u03B5 = r_i \u2212 \u03B2_m\u00B7r_mkt \u2212 \u03B2_p\u00B7r_peer  (betas-only residual, \u03B1 retained)",
      "RM_6\u20111 = Z(\u03A3 \u03B5, t\u2212126 : t\u221222)",
      "RM_12\u20111 = Z(\u03A3 \u03B5, t\u2212252 : t\u221222)",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 3,
    subSignals: [
      { key: "zR6",  label: "RM 6\u20111"  },
      { key: "zR12", label: "RM 12\u20111" },
    ],
    compute: (z) => 0.4 * n(z.zR6) + 0.6 * n(z.zR12),
  },
  {
    id: "short_reversal",
    label: "Short-Term Reversal",
    shortLabel: "REV",
    category: "Momentum",
    description:
      "Fades last-month winners. Standard institutional alpha component paired with " +
      "cross-sectional momentum \u2014 prevents doubling up on the same recent move that " +
      "the 12\u20111 momentum window already captures.",
    displayFormula: "REV = \u2212MOM_1",
    variableDefinitions: [
      "MOM_1 = Z(\u03A3 ln(P_t/P_{t\u22121}), t\u221221 : t)",
      "Signal enters \u03B1 with a negative sign (reversal)",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [
      { key: "zM1", label: "MOM 1" },
    ],
    compute: (z) => -n(z.zM1),
  },
  {
    id: "low_volatility",
    label: "Low Volatility",
    shortLabel: "LowVol",
    category: "Risk",
    description:
      "Favors stocks with lower realized short-term volatility. Captures the low-vol " +
      "anomaly \u2014 lower-risk stocks have historically delivered risk-adjusted outperformance. " +
      "Note: this is an approximation \u2014 a cleaner implementation would use idiosyncratic vol rather than total vol.",
    displayFormula: "LV = LowVol",
    variableDefinitions: [
      "\u03C3\u2086\u2080 = std(ln(P_t/P_{t\u22121})[\u221260:]) \u00D7 \u221A252",
      "LowVol = \u2212Z(winsorize(\u03C3\u2086\u2080))  (sign flipped: lower vol \u2192 higher score)",
    ],
    status: "institutional_approximation",
    statusLabel: "Institutional Approximation",
    defaultWeight: 2,
    subSignals: [
      { key: "zLowVol", label: "LowVol" },
    ],
    compute: (z) => n(z.zLowVol),
  },
  {
    id: "quality_opa",
    label: "Profitability",
    shortLabel: "PROF",
    category: "Quality",
    description:
      "Operating profitability / assets. A standard quality signal. This implementation is approximate \u2014 " +
      "operating income is used with EBIT and net income as fallbacks. " +
      "A fuller quality factor would additionally incorporate investment, accruals, and earnings stability.",
    displayFormula: "PROF = z(OPA)",
    variableDefinitions: [
      "OPA = Operating Income / Average Total Assets",
      "PROF = Z(winsorize(OPA, 2%, 98%))",
      "Note: partial quality proxy. Fama\u2013French quality adds investment and accruals.",
    ],
    status: "institutional_approximation",
    statusLabel: "Institutional Approximation",
    defaultWeight: 2,
    subSignals: [
      { key: "zOPA", label: "PROF" },
    ],
    compute: (z) => n(z.zOPA),
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
  {
    id: "quality_extended_stub",
    label: "Quality Extended (Future)",
    shortLabel: "Quality+",
    category: "Research",
    description:
      "Fuller quality signal combining multiple profitability, leverage, and earnings quality dimensions. Requires additional fundamental data.",
    displayFormula: "QE = (not yet built)",
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
