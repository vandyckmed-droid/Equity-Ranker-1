import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export default function MethodologyPage() {
  return (
    <div className="p-4 md:p-6 max-w-4xl mx-auto space-y-4 md:space-y-6">
      <div>
        <h1 className="text-2xl md:text-3xl font-bold tracking-tight mb-1 text-foreground">Methodology</h1>
        <p className="text-muted-foreground text-sm">
          Formulas and calculations driving the ranking and risk models.
        </p>
      </div>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Returns &amp; Momentum</CardTitle>
          <CardDescription>Base return calculations used for momentum and volatility.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-sm space-y-2 text-muted-foreground">
            <p><span className="text-foreground">r1</span>  = ln(P_t / P_{"{t-21}"})</p>
            <p><span className="text-foreground">r6</span>  = ln(P_t / P_{"{t-126}"})</p>
            <p><span className="text-foreground">r12</span> = ln(P_t / P_{"{t-252}"})</p>
          </div>

          <h3 className="font-semibold text-sm mt-4">Skip-month Momentum</h3>
          <p className="text-sm text-muted-foreground mb-2">
            Standard momentum skips the most recent month to avoid the short-term reversal effect.
          </p>
          <div className="bg-muted p-4 rounded-md font-mono text-sm space-y-2 text-muted-foreground">
            <p><span className="text-foreground">m6</span>  = r6 − r1</p>
            <p><span className="text-foreground">m12</span> = r12 − r1</p>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Volatility &amp; Sharpe Adjustment</CardTitle>
          <CardDescription>Risk-adjusted momentum — divides raw return by realized volatility.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-sm space-y-2 text-muted-foreground">
            <p><span className="text-foreground">sigma6</span>  = std(126-day daily log returns) × √252</p>
            <p><span className="text-foreground">s6</span>      = m6 / max(sigma6, vol_floor)</p>
          </div>
          <p className="text-xs text-muted-foreground">
            vol_floor = 0.05 by default. Prevents division by near-zero volatility.
          </p>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Quality Factor</CardTitle>
          <CardDescription>Composite quality score based on profitability and balance-sheet health.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-sm text-muted-foreground leading-relaxed space-y-1">
            <p>1. Winsorize outliers at 2nd and 98th percentiles</p>
            <p>2. Z-score: ROE, ROA, Gross Margin, Operating Margin, 1/Leverage</p>
            <p>3. <span className="text-foreground">quality</span> = equal-weight average of the five Z-scores</p>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Alpha &amp; Ranking — Sleeve Construction</CardTitle>
          <CardDescription>
            Three independent sleeves combined into a single composite score. Each atomic input is
            z-scored cross-sectionally before sleeve construction.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">

          <div>
            <h3 className="font-semibold text-sm mb-2">Step 1 — Atomic Z-scores</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p><span className="text-foreground">z_s6</span>  = Z(s6)      <span className="opacity-60">— cross-sectional winsorize → z-score of 6M Sharpe</span></p>
              <p><span className="text-foreground">z_s12</span> = Z(s12)     <span className="opacity-60">— 12M Sharpe</span></p>
              <p><span className="text-foreground">z_t6</span>  = Z(tstat6)  <span className="opacity-60">— OLS t-stat on 6M log-price trend</span></p>
              <p><span className="text-foreground">z_t12</span> = Z(tstat12) <span className="opacity-60">— OLS t-stat on 12M log-price trend</span></p>
              <p><span className="text-foreground">z_q</span>   = Z(quality) <span className="opacity-60">— quality composite (ROE, margins, leverage)</span></p>
            </div>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Step 2 — Sleeve construction (50/50 within each pair)</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p><span className="text-foreground">S</span> = 0.5 × z_s6  + 0.5 × z_s12  <span className="opacity-60">— return-strength sleeve</span></p>
              <p><span className="text-foreground">T</span> = 0.5 × z_t6  + 0.5 × z_t12  <span className="opacity-60">— trend-quality sleeve</span></p>
              <p><span className="text-foreground">Q</span> = z_q                          <span className="opacity-60">— quality sleeve</span></p>
            </div>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Step 3 — Final alpha</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p><span className="text-foreground">Alpha</span> = wS × S + wT × T + wQ × Q</p>
              <p className="mt-2 opacity-70">Expanded: Alpha = 0.2×z_s6 + 0.2×z_s12 + 0.2×z_t6 + 0.2×z_t12 + 0.2×z_q</p>
              <p className="opacity-60 mt-1">Default weights: wS = 0.4, wT = 0.4, wQ = 0.2</p>
            </div>
          </div>

          <div className="text-sm text-muted-foreground space-y-1">
            <p>
              <span className="font-medium text-foreground">S (return-strength)</span> — captures how
              strongly prices have risen relative to their own risk. Uses Sharpe-adjusted returns so
              high-vol names don't dominate.
            </p>
            <p>
              <span className="font-medium text-foreground">T (trend-quality)</span> — captures how
              consistent the trend is via OLS slope t-statistics. A high t-stat means price has risen
              steadily, not just in one burst.
            </p>
            <p>
              <span className="font-medium text-foreground">Q (quality)</span> — minority stabilizing
              sleeve. Tilts toward profitable, low-leverage businesses. Prevents pure momentum from
              concentrating in deteriorating names.
            </p>
            <p className="mt-2">
              All atomic inputs are standardized before combination, so no single factor can dominate
              through scale differences. Missing data is treated as neutral (0). Stocks ranked
              descending by Alpha. Top-20 marked with ★.
            </p>
          </div>

        </CardContent>
      </Card>

      {/* PATCH 5 — Variance / Covariance / Correlation definitions */}
      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Variance, Covariance &amp; Correlation</CardTitle>
          <CardDescription>
            Precise definitions of the statistical quantities used in portfolio risk calculations.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">

          <div>
            <h3 className="font-semibold text-sm mb-2">Variance — single asset dispersion</h3>
            <div className="bg-muted p-3 rounded-md font-mono text-sm text-muted-foreground">
              <p>Var(X) = E[(X − μ)²]</p>
            </div>
            <p className="text-sm text-muted-foreground mt-2">
              Measures how much a single asset's returns deviate from their mean. Units are squared returns.
            </p>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Covariance — joint movement in raw units</h3>
            <div className="bg-muted p-3 rounded-md font-mono text-sm text-muted-foreground">
              <p>Cov(X, Y) = E[(X − μ_X)(Y − μ_Y)]</p>
              <p className="mt-1 opacity-70">Note: Var(X) = Cov(X, X)</p>
            </div>
            <p className="text-sm text-muted-foreground mt-2">
              Describes how two assets move together. Positive means they tend to move in the same
              direction; negative means opposite. Units are squared returns — not normalized.
            </p>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Correlation — normalized covariance</h3>
            <div className="bg-muted p-3 rounded-md font-mono text-sm text-muted-foreground">
              <p>Corr(X, Y) = Cov(X, Y) / (σ_X × σ_Y)   ∈ [−1, +1]</p>
            </div>
            <p className="text-sm text-muted-foreground mt-2">
              Correlation is covariance scaled by the product of standard deviations, giving a
              dimensionless score from −1 (perfectly inverse) to +1 (perfectly co-moving).
              Used for similarity, clustering, and identifying names that move together.
            </p>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Covariance matrix Σ</h3>
            <div className="bg-muted p-3 rounded-md font-mono text-sm text-muted-foreground">
              <p>Σ[i,j] = Cov(R_i, R_j)</p>
              <p className="opacity-70">Σ[i,i] = Var(R_i) = σ_i²   (diagonal entries)</p>
            </div>
            <p className="text-sm text-muted-foreground mt-2">
              An N×N symmetric matrix where each off-diagonal entry is the covariance between two assets
              and each diagonal entry is that asset's variance. Required for true portfolio variance computation.
            </p>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Correlation matrix vs. covariance matrix</h3>
            <p className="text-sm text-muted-foreground">
              The correlation matrix rescales Σ so all diagonal entries equal 1. It is useful for
              visualizing similarity and grouping correlated names (as this app does for clustering),
              but it is <span className="text-foreground font-medium">not sufficient</span> for portfolio
              variance on its own — it discards each asset's absolute volatility. You must combine
              correlations with individual asset volatilities (σ) to recover the true covariance matrix.
            </p>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Portfolio Construction &amp; Risk</CardTitle>
          <CardDescription>Weighting schemes and portfolio variance math.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">

          <h3 className="font-semibold text-sm">Inverse-Vol Weights</h3>
          <div className="bg-muted p-4 rounded-md font-mono text-sm text-muted-foreground mb-4 space-y-1">
            <p>w_i ∝ 1 / σ_i</p>
            <p className="opacity-70">Normalize so ∑ w_i = 1</p>
          </div>

          <h3 className="font-semibold text-sm">Portfolio Variance &amp; Volatility</h3>
          <div className="bg-muted p-4 rounded-md font-mono text-sm text-muted-foreground space-y-2">
            <p>Portfolio Variance  = w&#x27; Σ w</p>
            <p>Portfolio Volatility = √(w&#x27; Σ w)</p>
            <p className="text-xs mt-2 opacity-70">
              w = weight vector (N×1), Σ = empirical covariance matrix (N×N) over the chosen lookback.
              Annualized by multiplying by 252 before taking the square root.
            </p>
          </div>

          <p className="text-sm text-muted-foreground">
            This is the exact quadratic form. Diversification benefit appears when assets have low
            pairwise covariances — the off-diagonal terms in Σ reduce portfolio variance below a
            weighted average of individual variances.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
