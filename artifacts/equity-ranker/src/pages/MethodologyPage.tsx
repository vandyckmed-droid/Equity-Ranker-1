import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function MethodologyPage() {
  return (
    <div className="p-4 md:p-6 max-w-4xl mx-auto space-y-4 md:space-y-6">
      <div>
        <h1 className="text-2xl md:text-3xl font-bold tracking-tight mb-1 text-foreground">Methodology</h1>
        <p className="text-muted-foreground text-sm">Pipeline order: Universe → S → T → Q → Alpha → Portfolio</p>
      </div>

      {/* 1. UNIVERSE */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>1 — Universe</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>Price         ≥ $5</p>
            <p>Avg daily vol ≥ $5 M (30-day)</p>
            <p>Market cap    ≥ $200 M</p>
            <p>Price history ≥ 252 trading days</p>
          </div>
          <div className="text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground font-medium">SEC Filers Only</span> — excludes foreign issuers without SEC filings</p>
            <p><span className="text-foreground font-medium">Exclude Financials</span> — removes banks, insurers, financial services</p>
            <p><span className="text-foreground font-medium">Require Quality</span> — keeps only stocks with a computed Q score (≈ 84% of universe)</p>
          </div>
        </CardContent>
      </Card>

      {/* 2. S SLEEVE */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>2 — S Sleeve: Return Strength</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground">r1</span>     = ln(P_t / P_{"t-21"})</p>
            <p><span className="text-foreground">r6</span>     = ln(P_t / P_{"t-126"})</p>
            <p><span className="text-foreground">r12</span>    = ln(P_t / P_{"t-252"})</p>
            <p className="mt-2"><span className="text-foreground">m6</span>     = r6  − r1  <span className="opacity-60">— skip-month</span></p>
            <p><span className="text-foreground">m12</span>    = r12 − r1</p>
            <p className="mt-2"><span className="text-foreground">sigma6</span> = std(126d log returns) × √252</p>
            <p><span className="text-foreground">sigma12</span>= std(252d log returns) × √252</p>
            <p className="mt-2"><span className="text-foreground">s6</span>     = m6  / max(sigma6,  0.05)</p>
            <p><span className="text-foreground">s12</span>    = m12 / max(sigma12, 0.05)</p>
            <p className="mt-2"><span className="text-foreground">S</span>      = 0.5 × Z(s6) + 0.5 × Z(s12)</p>
          </div>
        </CardContent>
      </Card>

      {/* 3. T SLEEVE */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>3 — T Sleeve: Trend Consistency</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>ln(P_t) = α + β × t + ε  <span className="opacity-60">— OLS on log price</span></p>
            <p className="mt-2"><span className="text-foreground">tstat6</span>  = β̂ / SE(β̂)  <span className="opacity-60">— 126-day window</span></p>
            <p><span className="text-foreground">tstat12</span> = β̂ / SE(β̂)  <span className="opacity-60">— 252-day window</span></p>
            <p className="mt-2"><span className="text-foreground">T</span>       = 0.5 × Z(tstat6) + 0.5 × Z(tstat12)</p>
          </div>
        </CardContent>
      </Card>

      {/* 4. Q SLEEVE */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>4 — Q Sleeve: Quality</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground">ROE</span>        = Net Income / Equity</p>
            <p><span className="text-foreground">ROA</span>        = Net Income / Assets</p>
            <p><span className="text-foreground">GrossMargin</span>= Gross Profit / Revenue</p>
            <p><span className="text-foreground">OpMargin</span>   = Operating Income / Revenue</p>
            <p><span className="text-foreground">InvLev</span>     = Equity / Total Debt</p>
            <p className="mt-2 opacity-60">Winsorize [2%, 98%], then z-score each metric cross-sectionally</p>
            <p className="mt-2"><span className="text-foreground">Q</span>           = Z(z_ROE + z_ROA + z_GrossMargin + z_OpMargin + z_InvLev)</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">Source: SEC EDGAR XBRL, most recent 10-K. US-GAAP with IFRS-full fallback.</p>
        </CardContent>
      </Card>

      {/* 5. COMPOSITE ALPHA */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>5 — Composite Alpha</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground">Alpha</span> = wS × S + wT × T</p>
            <p className="mt-2 opacity-60">Default: wS = 0.5 · wT = 0.5  (quality weight = 0, display only)</p>
            <p className="mt-2 opacity-70">= 0.25×Z(s6) + 0.25×Z(s12) + 0.25×Z(tstat6) + 0.25×Z(tstat12)</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">Stocks ranked descending by Alpha. Top 20 marked ★. Missing data → z = 0.</p>
          <p className="text-xs text-muted-foreground mt-3 border-t border-border pt-3">
            <span className="font-semibold text-foreground">Profitability / Quality data (OPA):</span>{" "}
            Operating Profit / Assets (OPA) is computed for approximately 97–98% of the universe using a
            formula hierarchy: Operating Income → EBIT → Net Income, each divided by average total assets.
            The resulting value and its peer-relative z-score (zQ, normalized within industry then sector
            then the full universe) are displayed in the "Prof" column and in each stock's expanded detail
            panel for transparency and auditing purposes. <em>OPA does not currently enter the alpha formula</em>{" "}
            — rank, alpha, and sort order are unchanged by its presence.
          </p>
        </CardContent>
      </Card>

      {/* 6. VARIANCE / COV / CORR */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>6 — Covariance &amp; Risk Model</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p className="text-foreground/60">EWMA covariance (Risk Parity)</p>
            <p>Σ_ewma = Σ_t  λ^(T−t) r_t r_t'  / Σ_t λ^(T−t)   λ = 0.94</p>
            <p>Σ_ann  = Σ_ewma × 252  +  ridge × I</p>
            <p className="mt-1 opacity-60">ridge = max(1e-4 × trace/n, 1e-6) — ensures positive-definiteness</p>
          </div>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p className="text-foreground/60">Sample covariance (simpler methods, baseline)</p>
            <p>Σ_ann  = cov(log_returns) × 252</p>
            <p>σ_i    = max(√Σ_ann[i,i], 0.05)  <span className="opacity-60">— 5% vol floor</span></p>
          </div>
          <p className="text-xs text-muted-foreground">
            Risk Parity uses EWMA covariance for both weight optimisation and the vol-target overlay,
            ensuring the predicted and realized portfolio volatility are consistent.
          </p>
        </CardContent>
      </Card>

      {/* 7. PORTFOLIO CONSTRUCTION */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>7 — Portfolio Construction</CardTitle></CardHeader>
        <CardContent className="space-y-4">

          <p className="text-xs text-muted-foreground">
            <span className="text-foreground font-semibold">Risk Parity</span> is the preferred advanced method.
            The simpler alternatives (Equal, Inverse Vol, Signal/Vol) are available for comparison and transparency.
          </p>

          <div className="space-y-3">
            <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Weighting methods — base weights (sum = 1 before overlay)</p>

            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-foreground/60">Equal Weight</p>
              <p>w_i = 1 / N</p>
              <p className="mt-1 opacity-60">Signal: no · Covariance: no · Cap: none</p>
            </div>

            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-foreground/60">Inverse Vol</p>
              <p>w_i = (1/σ_i) / ∑(1/σ_j)   <span className="opacity-60">— σ_i diagonal of sample Σ, floor 5%</span></p>
              <p className="mt-1 opacity-60">Signal: no · Covariance: diagonal only · Cap: none</p>
            </div>

            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-foreground/60">Signal / Vol</p>
              <p>raw_i = max(α_i, 0) / σ_i</p>
              <p>w_i   = raw_i / ∑ raw_j</p>
              <p className="mt-1 opacity-60">α_i = composite Alpha score · winsorised at 99th pct · floor 5%</p>
              <p className="opacity-60">Signal: yes · Covariance: diagonal only · Cap: none · Fallback → Inverse Vol if all α ≤ 0</p>
            </div>

            <div className="border border-primary/30 bg-primary/5 p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-primary/80 font-semibold">Risk Parity — Preferred Advanced Method</p>
              <p className="mt-1">Capped Equal Risk Contribution (ERC) via SLSQP</p>
              <p className="mt-2">Objective (all constraints inside the solver):</p>
              <p>  min_w  Σ_i (RC_i − RC_mean)²</p>
              <p>  where  RC_i = w_i · (Σ_ewma w)_i</p>
              <p className="mt-2">Constraints:</p>
              <p>  Σ w_i = 1       (fully invested)</p>
              <p>  0 ≤ w_i ≤ 0.15  (long-only + per-name cap inside solver)</p>
              <p className="mt-2">Gradient (analytical):</p>
              <p>  ∂f/∂w_k = 2 · [ v_k·(Σw)_k + (Σ(v·w))_k ]</p>
              <p>  where  v_i = RC_i − RC_mean</p>
              <p className="mt-2 opacity-60">Multi-start: equal weights + 3 Dirichlet seeds · deterministic (seed=42)</p>
              <p className="opacity-60">Covariance: EWMA(λ=0.94)+ridge · Fallback → Spinu L-BFGS-B if all starts fail</p>
              <p className="opacity-60">Fallback² → Inverse Vol if Spinu also fails</p>
              <p className="mt-1 opacity-60">Cap is a first-class constraint — not post-hoc clipping</p>
            </div>
          </div>

          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p className="text-foreground/60">Vol-target overlay (15% annualised target)</p>
            <p>pre_vol    = √(w_base′ Σ w_base)   <span className="opacity-60">— using same Σ as optimisation</span></p>
            <p>multiplier = min(0.15 / pre_vol, 1.0)  <span className="opacity-60">— capped at 1 · no leverage</span></p>
            <p>w_equity   = w_base × multiplier    <span className="opacity-60">— sum = multiplier</span></p>
            <p>w_sgov     = max(0, 1 − multiplier)  <span className="opacity-60">— residual in cash / SGOV</span></p>
            <p className="mt-2 opacity-60">When basket vol &lt; 15%: multiplier = 1.0 · fully invested · no SGOV</p>
            <p className="opacity-60">When basket vol &gt; 15%: multiplier &lt; 1.0 · residual explicitly in SGOV</p>
          </div>

          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p className="text-foreground/60">Portfolio diagnostics</p>
            <p>portfolioVol       = √(w_equity′ Σ w_equity)</p>
            <p>diversificationRatio = (Σ w_i_base · σ_i) / pre_vol  <span className="opacity-60">— &gt;1 = diversification benefit</span></p>
            <p>effectiveN         = 1 / Σ(w_base_i²)  <span className="opacity-60">— Herfindahl-based</span></p>
            <p>RC_i               = w_i · (Σw)_i / (w′Σw)  <span className="opacity-60">— risk fraction, sums to 1</span></p>
          </div>

        </CardContent>
      </Card>

    </div>
  );
}
