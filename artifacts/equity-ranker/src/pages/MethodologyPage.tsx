import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function MethodologyPage() {
  return (
    <div className="p-4 md:p-6 max-w-4xl mx-auto space-y-4 md:space-y-6">
      <div>
        <h1 className="text-2xl md:text-3xl font-bold tracking-tight mb-1 text-foreground">Methodology</h1>
        <p className="text-muted-foreground text-sm">Pipeline: Universe &rarr; Raw signals &rarr; Winsorize &rarr; Z-score &rarr; Composites &rarr; Alpha &rarr; Portfolio</p>
      </div>

      {/* 1. UNIVERSE */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>1 &mdash; Universe</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>Price         &ge; $5</p>
            <p>Avg daily vol &ge; $10M (30-day)</p>
            <p>Market cap    &ge; $1B (backfilled)</p>
            <p>Price history &ge; 252 trading days</p>
          </div>
          <p className="text-xs text-muted-foreground">
            ~1,800 large liquid US stocks (NYSE + NASDAQ). ETFs, mutual funds, SPACs, OTC, and non-equity instruments excluded.
          </p>
        </CardContent>
      </Card>

      {/* 2. MOMENTUM COMPOSITE */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>2 &mdash; Momentum Composite (MOM)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground">MOM_6&#x2011;1</span>  = Z(&Sigma; ln(P_t/P_&#123;t&#x2212;1&#125;), t&#x2212;126 : t&#x2212;22)</p>
            <p><span className="text-foreground">MOM_12&#x2011;1</span> = Z(&Sigma; ln(P_t/P_&#123;t&#x2212;1&#125;), t&#x2212;252 : t&#x2212;22)</p>
            <p className="mt-2"><span className="text-foreground">TS_6&#x2011;1</span>  = Z(OLS &beta;/SE[&beta;], ln(P)~t, 126d)</p>
            <p><span className="text-foreground">TS_12&#x2011;1</span> = Z(OLS &beta;/SE[&beta;], ln(P)~t, 252d)</p>
            <p className="mt-2"><span className="text-foreground">MOM</span>      = &frac14;&middot;(MOM_6&#x2011;1 + MOM_12&#x2011;1 + TS_6&#x2011;1 + TS_12&#x2011;1)</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Equal-weight blend of skip-adjusted cumulative log-returns and their OLS trend t-statistics.
            T-statistics reward consistent price drift over a single large move.
          </p>
        </CardContent>
      </Card>

      {/* 3. RESIDUAL MOMENTUM */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>3 &mdash; Residual Momentum (RM)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>r_i = &alpha; + &beta;_m&middot;r_mkt + &beta;_p&middot;r_peer + &epsilon;&#x0303;  <span className="opacity-60">&mdash; OLS with intercept</span></p>
            <p>&epsilon; = r_i &minus; &beta;_m&middot;r_mkt &minus; &beta;_p&middot;r_peer  <span className="opacity-60">&mdash; betas-only residual, &alpha; retained</span></p>
            <p className="mt-2"><span className="text-foreground">RM_6&#x2011;1</span>  = Z(&Sigma; &epsilon;, t&#x2212;126 : t&#x2212;22)</p>
            <p><span className="text-foreground">RM_12&#x2011;1</span> = Z(&Sigma; &epsilon;, t&#x2212;252 : t&#x2212;22)</p>
            <p className="mt-2"><span className="text-foreground">RM</span>       = 0.4&middot;RM_6&#x2011;1 + 0.6&middot;RM_12&#x2011;1</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Momentum in returns after removing market and sector co-movement.
            Captures stock-specific alpha accumulation less sensitive to sector rotations.
          </p>
        </CardContent>
      </Card>

      {/* 4. SHORT-TERM REVERSAL */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>4 &mdash; Short-Term Reversal (REV)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground">MOM_1</span>  = Z(&Sigma; ln(P_t/P_&#123;t&#x2212;1&#125;), t&#x2212;21 : t)</p>
            <p className="mt-2"><span className="text-foreground">REV</span>    = &minus;MOM_1</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Fades last-month winners. Prevents doubling up on the same recent move
            that the 12&#x2011;1 momentum window already captures.
          </p>
        </CardContent>
      </Card>

      {/* 5. LOW VOLATILITY */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>5 &mdash; Low Volatility (LowVol)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>&sigma;&#x2086;&#x2080; = std(ln(P_t/P_&#123;t&#x2212;1&#125;)[&#x2212;60:]) &times; &radic;252</p>
            <p className="mt-2"><span className="text-foreground">LowVol</span> = &minus;Z(winsorize(&sigma;&#x2086;&#x2080;))  <span className="opacity-60">&mdash; sign flipped: lower vol &rarr; higher score</span></p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Captures the low-vol anomaly. This is an approximation &mdash; a cleaner implementation
            would use idiosyncratic vol rather than total vol.
          </p>
        </CardContent>
      </Card>

      {/* 6. PROFITABILITY */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>6 &mdash; Profitability (PROF)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground">OPA</span>  = Operating Income / Average Total Assets</p>
            <p className="mt-1 opacity-60">Fallback: EBIT / Avg Assets &rarr; Net Income / Avg Assets</p>
            <p className="mt-2"><span className="text-foreground">PROF</span> = Z(winsorize(OPA, 2%, 98%))</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Partial quality proxy. Fama&ndash;French quality adds investment, accruals, and earnings stability.
            Coverage is ~97&ndash;98% of the universe.
          </p>
        </CardContent>
      </Card>

      {/* 7. COMPOSITE ALPHA */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>7 &mdash; Composite Alpha</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground">&alpha;</span> = &Sigma;(w_i &middot; part_i) / &Sigma;w_i</p>
            <p className="mt-2 opacity-60">Default weights: MOM&times;4 + RM&times;3 + LowVol&times;2 + PROF&times;2 + REV&times;1</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Client owns the weight math for instant re-ranking. Weights are configured in the Alpha Basket panel.
            Stocks ranked descending by alpha. Missing data &rarr; z = 0.
          </p>
        </CardContent>
      </Card>

      {/* 8. COVARIANCE & RISK */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>8 &mdash; Covariance &amp; Risk Model</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p className="text-foreground/60">EWMA covariance (Risk Parity)</p>
            <p>&Sigma;_ewma = &Sigma;_t  &lambda;^(T&minus;t) r_t r_t&prime;  / &Sigma;_t &lambda;^(T&minus;t)   &lambda; = 0.94</p>
            <p>&Sigma;_ann  = &Sigma;_ewma &times; 252  +  ridge &times; I</p>
            <p className="mt-1 opacity-60">ridge = max(1e-4 &times; trace/n, 1e-6) &mdash; ensures positive-definiteness</p>
          </div>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p className="text-foreground/60">Sample covariance (simpler methods, baseline)</p>
            <p>&Sigma;_ann  = cov(log_returns) &times; 252</p>
            <p>&sigma;_i    = max(&radic;&Sigma;_ann[i,i], 0.05)  <span className="opacity-60">&mdash; 5% vol floor</span></p>
          </div>
        </CardContent>
      </Card>

      {/* 9. PORTFOLIO CONSTRUCTION */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>9 &mdash; Portfolio Construction</CardTitle></CardHeader>
        <CardContent className="space-y-4">

          <p className="text-xs text-muted-foreground">
            <span className="text-foreground font-semibold">Risk Parity</span> is the preferred advanced method.
            The simpler alternatives (Equal, Inverse Vol, Signal/Vol) are available for comparison.
          </p>

          <div className="space-y-3">
            <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Weighting methods &mdash; base weights (sum = 1 before overlay)</p>

            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-foreground/60">Equal Weight</p>
              <p>w_i = 1 / N</p>
            </div>

            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-foreground/60">Inverse Vol</p>
              <p>w_i = (1/&sigma;_i) / &sum;(1/&sigma;_j)</p>
            </div>

            <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-foreground/60">Signal / Vol</p>
              <p>raw_i = max(&alpha;_i, 0) / &sigma;_i</p>
              <p>w_i   = raw_i / &sum; raw_j</p>
            </div>

            <div className="border border-primary/30 bg-primary/5 p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
              <p className="text-primary/80 font-semibold">Risk Parity &mdash; Capped ERC via SLSQP</p>
              <p className="mt-2">min_w  &Sigma;_i (RC_i &minus; RC_mean)&sup2;</p>
              <p>RC_i = w_i &middot; (&Sigma;_ewma w)_i</p>
              <p className="mt-2">Constraints: &Sigma; w_i = 1, 0 &le; w_i &le; 0.15</p>
              <p className="mt-1 opacity-60">Multi-start: equal weights + 3 Dirichlet seeds (seed=42)</p>
            </div>
          </div>

          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p className="text-foreground/60">Vol-target overlay (15% annualised target)</p>
            <p>pre_vol    = &radic;(w_base&prime; &Sigma; w_base)</p>
            <p>multiplier = min(0.15 / pre_vol, 1.0)</p>
            <p>w_equity   = w_base &times; multiplier</p>
            <p>w_sgov     = max(0, 1 &minus; multiplier)</p>
          </div>

        </CardContent>
      </Card>

    </div>
  );
}
