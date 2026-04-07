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
            <p><span className="text-foreground">Alpha</span> = wS × S + wT × T + wQ × Q</p>
            <p className="mt-2 opacity-60">Default: wS = 0.4 · wT = 0.4 · wQ = 0.2</p>
            <p className="mt-2 opacity-70">= 0.2×Z(s6) + 0.2×Z(s12) + 0.2×Z(tstat6) + 0.2×Z(tstat12) + 0.2×Z(quality)</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">Stocks ranked descending by Alpha. Top 20 marked ★. Missing data → z = 0.</p>
        </CardContent>
      </Card>

      {/* 6. VARIANCE / COV / CORR */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>6 — Variance, Covariance &amp; Correlation</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>Var(X)      = E[(X − μ)²]</p>
            <p>Cov(X, Y)   = E[(X − μ_X)(Y − μ_Y)]</p>
            <p>Corr(X, Y)  = Cov(X, Y) / (σ_X × σ_Y)   ∈ [−1, +1]</p>
            <p className="mt-2">Σ[i,j]      = Cov(R_i, R_j)   <span className="opacity-60">— N×N covariance matrix</span></p>
            <p>Σ[i,i]      = σ_i²             <span className="opacity-60">— diagonal = variance</span></p>
          </div>
          <p className="text-xs text-muted-foreground">
            The correlation matrix normalizes Σ to unit diagonal. It is used for clustering but is
            insufficient for portfolio variance — individual volatilities must be restored.
          </p>
        </CardContent>
      </Card>

      {/* 7. PORTFOLIO CONSTRUCTION */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>7 — Portfolio Construction</CardTitle></CardHeader>
        <CardContent className="space-y-4">

          <div className="space-y-3">
            <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Base weights</p>

            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-foreground/60">Equal</p>
              <p>w_i = 1 / N</p>
            </div>

            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-foreground/60">Inverse Vol</p>
              <p>w_i = (1/σ_i) / ∑(1/σ_j)   <span className="opacity-60">— σ_i annualized, floor 5%</span></p>
            </div>

            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-foreground/60">Min Var</p>
              <p>min_w  w′ Σ_reg w</p>
              <p>s.t.   ∑ w_i = 1,  0 ≤ w_i ≤ 0.40</p>
              <p className="mt-1 opacity-60">Σ_reg = Ledoit-Wolf shrinkage on daily returns + diagonal ridge</p>
              <p className="opacity-60">SLSQP, multi-start. Fallback → Inverse Vol if all starts fail.</p>
            </div>
          </div>

          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p className="text-foreground/60">Vol-target overlay (15%)</p>
            <p>pre_vol    = √(w′ Σ w)</p>
            <p>multiplier = 0.15 / pre_vol</p>
            <p>w_final    = w_base × multiplier</p>
            <p className="mt-1 opacity-60">gross exposure = multiplier · 100%  (may exceed 100%)</p>
          </div>

        </CardContent>
      </Card>

    </div>
  );
}
