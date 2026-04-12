import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function MethodologyPage() {
  return (
    <div className="p-4 md:p-6 max-w-4xl mx-auto space-y-4 md:space-y-6">
      <div>
        <h1 className="text-2xl md:text-3xl font-bold tracking-tight mb-1 text-foreground">Methodology</h1>
        <p className="text-muted-foreground text-sm">Pipeline: Universe &rarr; Raw signals &rarr; Winsorize &rarr; Z-score &rarr; Client-weighted alpha &rarr; Portfolio</p>
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

      {/* 2. MOMENTUM SIGNALS */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>2 &mdash; Momentum Signals (MOM_12-1, MOM_6-1)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground">MOM_12&#x2011;1</span> = Z(winsorize(&Sigma; ln P, t&#x2212;252 : t&#x2212;22))</p>
            <p><span className="text-foreground">MOM_6&#x2011;1</span>  = Z(winsorize(&Sigma; ln P, t&#x2212;126 : t&#x2212;22))</p>
            <p className="mt-1 opacity-60">Skip month (t&#x2212;22) avoids doubling up on 1-month microstructure reversal</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Baseline skip-adjusted cumulative log-returns. MOM_12-1 is the standard medium-term momentum signal;
            MOM_6-1 is more responsive. Both are individual building blocks — combine freely.
          </p>
        </CardContent>
      </Card>

      {/* 3. RISK-ADJUSTED MOMENTUM */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>3 &mdash; Risk-Adjusted Momentum (RAM_12-1, RAM_6-1)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>&sigma;&#x2086;&#x2083; = std(log_ret[&#x2212;63:]) &times; &radic;252 &nbsp; <span className="opacity-60">&mdash; 3-month realized vol, annualized</span></p>
            <p className="mt-2"><span className="text-foreground">RAM_12&#x2011;1</span> = Z(winsorize(m12 / max(&sigma;&#x2086;&#x2083;, 0.15)))</p>
            <p><span className="text-foreground">RAM_6&#x2011;1</span>  = Z(winsorize(m6 &nbsp;/ max(&sigma;&#x2086;&#x2083;, 0.15)))</p>
            <p className="mt-1 opacity-60">Vol floor = 0.15 (15% ann.) prevents division by near-zero vol</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Sharpe-style signals: same cumulative return as MOM but divided by recent realized volatility.
            Rewards stocks with consistent, low-noise uptrends. A single common 63-day vol window is used
            for both RAM_12-1 and RAM_6-1.
          </p>
        </CardContent>
      </Card>

      {/* 4. RESIDUAL MOMENTUM */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>4 &mdash; Residual Momentum (RM_12-1, RM_6-1)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>r_i = &alpha; + &beta;_m&middot;r_mkt + &beta;_p&middot;r_peer + &epsilon;&#x0303; &nbsp; <span className="opacity-60">&mdash; OLS with intercept</span></p>
            <p>&epsilon; = r_i &minus; &beta;_m&middot;r_mkt &minus; &beta;_p&middot;r_peer &nbsp; <span className="opacity-60">&mdash; betas-only residual; &alpha; retained</span></p>
            <p className="mt-2"><span className="text-foreground">RM_12&#x2011;1</span> = Z(winsorize(&Sigma; &epsilon;, t&#x2212;252 : t&#x2212;22))</p>
            <p><span className="text-foreground">RM_6&#x2011;1</span>  = Z(winsorize(&Sigma; &epsilon;, t&#x2212;126 : t&#x2212;22))</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Momentum after removing market beta and industry peer co-movement.
            Captures stock-specific alpha accumulation less sensitive to sector rotations.
            Peer group regression falls back from industry to sector to universe when the group has fewer than 10 stocks.
          </p>
        </CardContent>
      </Card>

      {/* 5. TREND STRENGTH */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>5 &mdash; Trend Strength (TS_12)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>Fit ln(P_t) = &alpha; + &beta;&middot;t + &epsilon; by OLS over 252 trading days</p>
            <p className="mt-2"><span className="text-foreground">TS_12</span> = Z(winsorize(&beta; / SE[&beta;]))</p>
            <p className="mt-1 opacity-60">High t-stat &rarr; smooth, persistent uptrend; low t-stat &rarr; noisy or stalling price</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            The OLS t-statistic of the log-price trend. Complements return-magnitude signals because it rewards
            trend persistence and smoothness rather than a single large move. No skip-month is applied
            (the regression uses the full 252-day window).
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
            Non-momentum quality anchor. Tends to be uncorrelated with momentum signals and adds
            diversification in momentum-reversal regimes. Coverage is ~97&ndash;98% of the universe.
            Partial quality proxy &mdash; Fama&ndash;French quality adds investment, accruals, and earnings stability.
          </p>
        </CardContent>
      </Card>

      {/* 7. SHORT-TERM REVERSAL */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>7 &mdash; Short-Term Reversal (REV)</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p>MOM_1  = &Sigma; log_ret[t&#x2212;21 : t] &nbsp; <span className="opacity-60">&mdash; last-month return, no skip</span></p>
            <p>RAM_1  = MOM_1 / max(&sigma;&#x2086;&#x2083;, 0.15) &nbsp; <span className="opacity-60">&mdash; vol-adjusted 1-month return</span></p>
            <p className="mt-2"><span className="text-foreground">REV (RAM_1)</span> = &minus;Z(winsorize(RAM_1))</p>
            <p className="mt-1 opacity-60">Negative sign: fade last-month winners (reversal direction)</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            Fades last-month winners. Prevents doubling up on the same recent move already captured by
            skip-adjusted momentum signals. The vol-adjusted variant (RAM_1) is recommended; a raw
            MOM_1 version is also available in the parts library.
          </p>
        </CardContent>
      </Card>

      {/* 8. ALPHA BUILDING BLOCKS */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>8 &mdash; Alpha Building Blocks</CardTitle></CardHeader>
        <CardContent>
          <div className="bg-muted p-4 rounded-md font-mono text-xs text-muted-foreground space-y-1">
            <p><span className="text-foreground">&alpha;</span> = &Sigma;(w_i &middot; signal_i) / &Sigma;w_i</p>
            <p className="mt-2 opacity-60">All signals are individually winsorized (2%/98%) and cross-sectionally z-scored</p>
            <p className="opacity-60">No re-z-scoring of composites &mdash; each block enters the alpha directly</p>
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            The alpha is a flat weighted sum of individual z-scored signals. There are no intermediate
            composite layers &mdash; each building block (MOM_12-1, RAM_12-1, RM_12-1, TS_12, PROF,
            MOM_6-1, RAM_6-1, RM_6-1, REV) enters alpha directly with its own weight.
            Client owns the weights for instant re-ranking. Missing data &rarr; z&nbsp;=&nbsp;0.
          </p>
        </CardContent>
      </Card>

      {/* 9. COVARIANCE & RISK */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>9 &mdash; Covariance &amp; Risk Model</CardTitle></CardHeader>
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

      {/* 10. PORTFOLIO CONSTRUCTION */}
      <Card className="border-border bg-card">
        <CardHeader><CardTitle>10 &mdash; Portfolio Construction</CardTitle></CardHeader>
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
