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
    label: "Core Momentum",
    shortLabel: "Core Mom",
    category: "Momentum",
    description:
      "Equal-weight blend of 6-month and 12-month skip-adjusted cumulative log-returns " +
      "plus their OLS trend t-statistics. The t-statistics add signal robustness by " +
      "rewarding consistent price drift rather than a single large move.",
    displayFormula: "M = ¼·(zM6 + zM12 + zT6 + zT12)",
    variableDefinitions: [
      "zM6  = Z(Σ log-ret, t-126:t-22)   — 6-month cumulative return (skip-adjusted)",
      "zM12 = Z(Σ log-ret, t-252:t-22)   — 12-month cumulative return (skip-adjusted)",
      "zT6  = Z(OLS β/SE[β], ln(P)~t, 126d)  — 6-month trend quality",
      "zT12 = Z(OLS β/SE[β], ln(P)~t, 252d)  — 12-month trend quality",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 4,
    subSignals: [
      { key: "zM6",  label: "zM6"  },
      { key: "zM12", label: "zM12" },
      { key: "zT6",  label: "zT6"  },
      { key: "zT12", label: "zT12" },
    ],
    compute: (z) =>
      0.25 * n(z.zM6) + 0.25 * n(z.zM12) + 0.25 * n(z.zT6) + 0.25 * n(z.zT12),
  },
  {
    id: "residual_momentum",
    label: "Residual Momentum",
    shortLabel: "Resid Mom",
    category: "Residual",
    description:
      "Momentum in returns after removing market and industry/sector co-movement. " +
      "Captures stock-specific alpha accumulation that is less sensitive to sector rotations. " +
      "Regression uses OLS with intercept for beta estimation; residuals retain the alpha term.",
    displayFormula: "RM = 0.4·zR6 + 0.6·zR12",
    variableDefinitions: [
      "r_i = α + β_m·r_mkt + β_p·r_peer + ε̃  (OLS with intercept)",
      "ε   = r_i − β_m·r_mkt − β_p·r_peer    (betas-only residual, alpha retained)",
      "zR6  = Z(Σ ε, t-126:t-22)  — 6-month cumulative residual",
      "zR12 = Z(Σ ε, t-252:t-22)  — 12-month cumulative residual",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 3,
    subSignals: [
      { key: "zR6",  label: "zR6"  },
      { key: "zR12", label: "zR12" },
    ],
    compute: (z) => 0.4 * n(z.zR6) + 0.6 * n(z.zR12),
  },
  {
    id: "short_reversal",
    label: "Short-Term Reversal",
    shortLabel: "Reversal",
    category: "Momentum",
    description:
      "Fades last-month winners. Standard institutional alpha component paired with " +
      "cross-sectional momentum — prevents doubling up on the same recent move that " +
      "the 12-1 momentum window already captures.",
    displayFormula: "R = −zM1",
    variableDefinitions: [
      "zM1 = Z(Σ log-ret, t-21:t)  — 1-month cumulative return",
      "Signal enters α with a negative sign (reversal)",
    ],
    status: "institutional_core",
    statusLabel: "Institutional Core",
    defaultWeight: 1,
    subSignals: [
      { key: "zM1", label: "zM1" },
    ],
    compute: (z) => -n(z.zM1),
  },
  {
    id: "low_volatility",
    label: "Low Volatility",
    shortLabel: "Low Vol",
    category: "Risk",
    description:
      "Favors stocks with lower realized short-term volatility. Captures the low-vol " +
      "anomaly — lower-risk stocks have historically delivered risk-adjusted outperformance. " +
      "Note: this is an approximation — a cleaner implementation would use idiosyncratic vol rather than total vol.",
    displayFormula: "LV = zLowVol",
    variableDefinitions: [
      "σ₆₀ = std(log-ret[-60:]) × √252  — 60-day annualized realized volatility",
      "zLowVol = −Z(winsorize(σ₆₀))     — sign flipped: lower vol → higher score",
    ],
    status: "institutional_approximation",
    statusLabel: "Institutional Approximation",
    defaultWeight: 2,
    subSignals: [
      { key: "zLowVol", label: "zLowVol" },
    ],
    compute: (z) => n(z.zLowVol),
  },
  {
    id: "quality_opa",
    label: "Quality (OPA)",
    shortLabel: "Quality",
    category: "Quality",
    description:
      "Operating profitability / assets. A standard quality signal. This implementation is approximate — " +
      "operating income is used with EBIT and net income as fallbacks. " +
      "A fuller quality factor would additionally incorporate investment, accruals, and earnings stability.",
    displayFormula: "Q = zOPA",
    variableDefinitions: [
      "OPA = Operating Income / Average Total Assets",
      "zOPA = Z(winsorize(OPA, 2%, 98%))",
      "Note: partial quality proxy. Fama-French quality adds investment and accruals.",
    ],
    status: "institutional_approximation",
    statusLabel: "Institutional Approximation",
    defaultWeight: 2,
    subSignals: [
      { key: "zOPA", label: "zOPA" },
    ],
    compute: (z) => n(z.zOPA),
  },
  {
    id: "value_stub",
    label: "Value (Future)",
    shortLabel: "Value",
    category: "Research",
    description:
      "Placeholder for a value signal (e.g., earnings yield, book-to-price). Not yet implemented — requires fundamental data pipeline expansion.",
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
