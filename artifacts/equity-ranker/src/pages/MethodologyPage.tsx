import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export default function MethodologyPage() {
  return (
    <div className="p-4 md:p-6 max-w-4xl mx-auto space-y-4 md:space-y-6">
      <div>
        <h1 className="text-2xl md:text-3xl font-bold tracking-tight mb-1 text-foreground">Methodology</h1>
        <p className="text-muted-foreground text-sm">
          Formulas and calculations driving the ranking and risk models, in pipeline order.
        </p>
      </div>

      {/* ── 1. UNIVERSE ─────────────────────────────────────────────────────── */}
      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>1 — Universe</CardTitle>
          <CardDescription>What goes in before any ranking begins.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4 text-sm text-muted-foreground">
          <p>
            The starting universe is approximately <span className="text-foreground font-medium">1,800 large liquid US equities</span> sourced
            from NASDAQ and NYSE listings. Before ranking, every stock must pass baseline liquidity screens:
          </p>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>Price         ≥ $5</p>
            <p>Avg daily vol ≥ $5 M (30-day)</p>
            <p>Market cap    ≥ $200 M</p>
            <p>Price history ≥ 252 trading days</p>
          </div>
          <p>
            Optional toggles further refine the universe:
          </p>
          <ul className="list-disc list-inside space-y-1 text-xs">
            <li><span className="text-foreground font-medium">SEC Filers Only</span> — restrict to companies with SEC EDGAR filings (excludes foreign issuers)</li>
            <li><span className="text-foreground font-medium">Exclude Financials</span> — removes banks, insurers, and financial services (accounting ratios not comparable)</li>
            <li><span className="text-foreground font-medium">Require Quality Coverage</span> — keeps only stocks with a computed Q score (≈ 84% of universe)</li>
          </ul>
        </CardContent>
      </Card>

      {/* ── 2. S SLEEVE ─────────────────────────────────────────────────────── */}
      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>2 — S Sleeve: Return Strength</CardTitle>
          <CardDescription>
            Captures how strongly prices have risen relative to their own risk. Built from
            Sharpe-adjusted momentum at two horizons, averaged 50/50.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">

          <div>
            <h3 className="font-semibold text-sm mb-2">Log returns &amp; skip-month momentum</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p><span className="text-foreground">r1</span>  = ln(P_t / P_{"t-21"})  <span className="opacity-60">— 1-month return (skip buffer)</span></p>
              <p><span className="text-foreground">r6</span>  = ln(P_t / P_{"t-126"}) <span className="opacity-60">— 6-month return</span></p>
              <p><span className="text-foreground">r12</span> = ln(P_t / P_{"t-252"}) <span className="opacity-60">— 12-month return</span></p>
              <p className="mt-2"><span className="text-foreground">m6</span>  = r6  − r1 <span className="opacity-60">— skip-month: drops most recent month to avoid short-term reversal</span></p>
              <p><span className="text-foreground">m12</span> = r12 − r1</p>
            </div>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Sharpe-adjusted momentum</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p><span className="text-foreground">sigma6</span>  = std(126-day daily log returns) × √252 <span className="opacity-60">— annualized realized vol</span></p>
              <p><span className="text-foreground">sigma12</span> = std(252-day daily log returns) × √252</p>
              <p className="mt-2"><span className="text-foreground">s6</span>  = m6  / max(sigma6,  vol_floor) <span className="opacity-60">— risk-adjusted 6M momentum</span></p>
              <p><span className="text-foreground">s12</span> = m12 / max(sigma12, vol_floor) <span className="opacity-60">— risk-adjusted 12M momentum</span></p>
              <p className="mt-1 opacity-60">vol_floor = 0.05 — prevents division by near-zero volatility</p>
            </div>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">S sleeve — 50/50 average of z-scored pair</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p><span className="text-foreground">z_s6</span>  = Z(s6)  <span className="opacity-60">— cross-sectional winsorize then z-score</span></p>
              <p><span className="text-foreground">z_s12</span> = Z(s12)</p>
              <p className="mt-2"><span className="text-foreground">S</span> = 0.5 × z_s6 + 0.5 × z_s12</p>
            </div>
          </div>

        </CardContent>
      </Card>

      {/* ── 3. T SLEEVE ─────────────────────────────────────────────────────── */}
      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>3 — T Sleeve: Trend Consistency</CardTitle>
          <CardDescription>
            Captures how steady the price trend is via OLS regression t-statistics. A high t-stat means
            price rose consistently, not just in one burst. Built 50/50 at two horizons.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">

          <div>
            <h3 className="font-semibold text-sm mb-2">OLS t-statistics on log-price</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p>ln(P_t) = α + β × t + ε  <span className="opacity-60">— OLS regression of log price on time</span></p>
              <p className="mt-2"><span className="text-foreground">tstat6</span>  = β̂ / SE(β̂)  <span className="opacity-60">— t-stat over 126-day window</span></p>
              <p><span className="text-foreground">tstat12</span> = β̂ / SE(β̂)  <span className="opacity-60">— t-stat over 252-day window</span></p>
            </div>
            <p className="text-xs text-muted-foreground mt-2">
              Unlike raw return, the t-stat penalizes choppy trajectories. Two stocks with the same total return
              but different path smoothness will have different t-stats.
            </p>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">T sleeve — 50/50 average of z-scored pair</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p><span className="text-foreground">z_t6</span>  = Z(tstat6)</p>
              <p><span className="text-foreground">z_t12</span> = Z(tstat12)</p>
              <p className="mt-2"><span className="text-foreground">T</span> = 0.5 × z_t6 + 0.5 × z_t12</p>
            </div>
          </div>

        </CardContent>
      </Card>

      {/* ── 4. Q SLEEVE ─────────────────────────────────────────────────────── */}
      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>4 — Q Sleeve: Quality</CardTitle>
          <CardDescription>Composite quality score built from annual fundamentals. Ranks profitability, efficiency, and balance-sheet health.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">

          <div>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p><span className="text-foreground">ROE</span>          = Net Income / Stockholders' Equity <span className="opacity-60">— profitability on equity</span></p>
              <p><span className="text-foreground">ROA</span>          = Net Income / Total Assets <span className="opacity-60">— profitability on assets</span></p>
              <p><span className="text-foreground">Gross Margin</span> = Gross Profit / Revenue <span className="opacity-60">— pricing power</span></p>
              <p><span className="text-foreground">Op Margin</span>    = Operating Income / Revenue <span className="opacity-60">— operational efficiency</span></p>
              <p><span className="text-foreground">1 / D-E</span>      = Equity / Total Debt <span className="opacity-60">— inverse leverage (lower debt → higher score)</span></p>
            </div>
            <p className="text-xs text-muted-foreground mt-2">
              All five metrics are sourced from the most recent annual 10-K filing in SEC EDGAR XBRL.
              US-GAAP taxonomy is used with IFRS-full as a fallback for dual-filers.
            </p>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Building the quality composite</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p>1. Winsorize each metric at the 2nd and 98th percentile cross-sectionally</p>
              <p>2. Z-score each metric: z_metric = (x − μ) / σ</p>
              <p className="mt-2"><span className="text-foreground">quality</span> = (z_ROE + z_ROA + z_GrossMargin + z_OpMargin + z_InvLeverage) / 5</p>
            </div>
            <p className="text-xs text-muted-foreground mt-2">
              Each metric is standardized before averaging, so no single ratio can dominate through scale.
              Missing metrics reduce the denominator rather than being treated as zero.
            </p>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Q sleeve</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground">
              <p><span className="text-foreground">Q</span> = Z(quality)</p>
            </div>
          </div>

        </CardContent>
      </Card>

      {/* ── 5. COMPOSITE ALPHA ──────────────────────────────────────────────── */}
      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>5 — Composite Alpha &amp; Ranking</CardTitle>
          <CardDescription>
            The three sleeves are combined into a single score. Stocks are ranked descending by alpha.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">

          <div>
            <h3 className="font-semibold text-sm mb-2">Sleeve combination</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p><span className="text-foreground">Alpha</span> = wS × S + wT × T + wQ × Q</p>
              <p className="mt-2 opacity-70">Expanded (default weights):</p>
              <p>Alpha = 0.2×z_s6 + 0.2×z_s12 + 0.2×z_t6 + 0.2×z_t12 + 0.2×z_q</p>
              <p className="mt-2 opacity-60">Default: wS = 0.4  ·  wT = 0.4  ·  wQ = 0.2</p>
            </div>
          </div>

          <div className="text-sm text-muted-foreground space-y-2">
            <p>
              <span className="font-medium text-foreground">S (return strength, 40%)</span> — how strongly
              the price has risen relative to its own realized risk. Uses Sharpe-adjusted returns so
              high-vol names don't dominate on raw return alone.
            </p>
            <p>
              <span className="font-medium text-foreground">T (trend consistency, 40%)</span> — how
              steady the trend has been, measured by OLS t-statistics at two horizons. Penalizes
              choppy recoveries versus smooth, persistent uptrends.
            </p>
            <p>
              <span className="font-medium text-foreground">Q (quality, 20%)</span> — minority
              stabilizing sleeve from annual fundamentals. Tilts toward profitable, capital-efficient,
              low-leverage businesses. Prevents the composite from concentrating purely in momentum names
              with deteriorating underlying businesses.
            </p>
            <p className="text-xs mt-1">
              All inputs are standardized cross-sectionally before combination so no factor dominates
              through scale. Missing data is treated as neutral (z = 0). Stocks are ranked descending
              by Alpha; the top 20 are marked with ★.
            </p>
          </div>

        </CardContent>
      </Card>

      {/* ── 6. VARIANCE / COV / CORR ─────────────────────────────────────────── */}
      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>6 — Variance, Covariance &amp; Correlation</CardTitle>
          <CardDescription>
            Statistical foundations for portfolio risk calculations.
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
              Describes how two assets move together. Positive means they tend to move in the same direction;
              negative means opposite. Units are squared returns — not normalized.
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
              An N×N symmetric matrix where each off-diagonal entry is the covariance between two assets and
              each diagonal entry is that asset's variance. Required for true portfolio variance computation.
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

      {/* ── 7. PORTFOLIO CONSTRUCTION ───────────────────────────────────────── */}
      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>7 — Portfolio Construction</CardTitle>
          <CardDescription>
            Stocks are manually selected into a basket. Weights are fully automated by the selected method
            and a volatility-target overlay. Final displayed weights are not forced to sum to 100%.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">

          <div>
            <h3 className="font-semibold text-sm mb-3">Step 1 — Base Weights (three methods)</h3>

            <div className="space-y-4">
              <div>
                <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-1">Equal</p>
                <div className="bg-muted p-3 rounded-md font-mono text-sm text-muted-foreground">
                  <p>w_i = 1 / N</p>
                  <p className="opacity-60 text-xs mt-1">All N basket stocks receive equal base weight. ∑ w_i = 1.</p>
                </div>
              </div>

              <div>
                <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-1">Inverse Vol</p>
                <div className="bg-muted p-3 rounded-md font-mono text-sm text-muted-foreground space-y-1">
                  <p>w_i ∝ 1 / σ_i   →   w_i = (1/σ_i) / ∑ (1/σ_j)</p>
                  <p className="opacity-60 text-xs mt-1">σ_i = annualized realized vol (lookback window). Vol floor = 5%. ∑ w_i = 1.</p>
                </div>
              </div>

              <div>
                <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-1">Min Var (long-only, institutionally stabilized)</p>
                <div className="bg-muted p-3 rounded-md font-mono text-sm text-muted-foreground space-y-1">
                  <p>min<sub>w</sub>  w′ Σ_reg w</p>
                  <p>subject to:  ∑ w_i = 1,  0 ≤ w_i ≤ 0.40</p>
                </div>
                <div className="text-xs text-muted-foreground mt-2 space-y-1">
                  <p>
                    <span className="text-foreground font-medium">Covariance:</span> Ledoit-Wolf shrinkage fitted on raw daily returns.
                    Shrinkage intensity ρ is computed analytically (oracle approximating shrinkage) and shown in the audit line.
                    A small diagonal ridge λ is added for numerical stability; λ escalates automatically if the condition number exceeds 10⁶.
                  </p>
                  <p>
                    <span className="text-foreground font-medium">Optimizer:</span> SLSQP with multi-start (equal-weight seed + 3 Dirichlet random seeds).
                    Single-name cap: 40% per position. Fully invested before the vol-target overlay.
                  </p>
                  <p>
                    <span className="text-foreground font-medium">Fallback:</span> if all optimizer starts fail, the method falls back
                    transparently to Inverse Vol. The audit line explicitly flags any fallback — never silently swapped.
                  </p>
                </div>
              </div>
            </div>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-3">Step 2 — 15% Vol-Target Overlay</h3>
            <div className="bg-muted p-4 rounded-md font-mono text-sm text-muted-foreground space-y-2">
              <p><span className="text-foreground">pre_vol</span>   = √(w_base′ Σ w_base)  <span className="opacity-60 text-xs">— basket vol before scaling</span></p>
              <p><span className="text-foreground">multiplier</span> = 15% / pre_vol</p>
              <p><span className="text-foreground">w_final</span>    = w_base × multiplier</p>
              <p className="text-xs opacity-60 mt-2 leading-relaxed">
                Σ is annualized (×252). w_final does NOT sum to 100% in general:
                gross exposure = multiplier.
                If pre_vol &lt; 15%, gross exposure &gt; 100% (levered).
                If pre_vol &gt; 15%, gross exposure &lt; 100% (de-levered).
              </p>
            </div>
          </div>

          <div>
            <h3 className="font-semibold text-sm mb-2">Audit Line</h3>
            <p className="text-sm text-muted-foreground">
              Every portfolio computation shows a compact audit line: active method, covariance model and
              shrinkage intensity (Min Var only), target vol (15%), pre-scale basket vol, multiplier, gross
              exposure, and covariance lookback window. Any fallback is explicitly named — never hidden.
            </p>
          </div>

        </CardContent>
      </Card>
    </div>
  );
}
