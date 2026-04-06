import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export default function MethodologyPage() {
  return (
    <div className="p-6 max-w-4xl mx-auto space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight mb-2 text-foreground">Methodology</h1>
        <p className="text-muted-foreground">
          Formulas and calculations driving the ranking and risk models.
        </p>
      </div>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Returns & Momentum</CardTitle>
          <CardDescription>Base return calculations used for momentum and volatility.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-sm space-y-2 text-muted-foreground">
            <p><span className="text-foreground">r1</span> = ln(P_t / P_{"{t-21}"})</p>
            <p><span className="text-foreground">r6</span> = ln(P_t / P_{"{t-126}"})</p>
            <p><span className="text-foreground">r12</span> = ln(P_t / P_{"{t-252}"})</p>
          </div>
          
          <h3 className="font-semibold text-sm mt-4">Skip-month Momentum</h3>
          <p className="text-sm text-muted-foreground mb-2">Standard momentum factors skip the most recent month to avoid the short-term reversal effect.</p>
          <div className="bg-muted p-4 rounded-md font-mono text-sm space-y-2 text-muted-foreground">
            <p><span className="text-foreground">m6</span> = r6 - r1</p>
            <p><span className="text-foreground">m12</span> = r12 - r1</p>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Volatility & Sharpe</CardTitle>
          <CardDescription>Risk-adjusted performance metrics.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-sm space-y-2 text-muted-foreground">
            <p><span className="text-foreground">sigma6</span> = std(126d daily log returns) × √252</p>
            <p><span className="text-foreground">s6</span> = m6 / sigma6</p>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Quality Factor</CardTitle>
          <CardDescription>Composite quality score based on profitability and leverage.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-sm text-muted-foreground leading-relaxed">
            <p>1. Winsorize outliers at 2nd and 98th percentiles</p>
            <p>2. Calculate Z-scores for: ROE, ROA, Gross Margins, Operating Margins, Leverage (Inverse)</p>
            <p>3. Average Z-scores to create composite <span className="text-foreground">quality</span> score</p>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Alpha & Ranking</CardTitle>
          <CardDescription>The final composite score used to rank the universe.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="bg-muted p-4 rounded-md font-mono text-sm text-muted-foreground">
            <p><span className="text-foreground">Alpha</span> = w6 × Z(m6) + w12 × Z(m12) + wQuality × Z(quality)</p>
          </div>
          <p className="text-sm text-muted-foreground">Stocks are ranked in descending order of Alpha.</p>
        </CardContent>
      </Card>

      <Card className="border-border bg-card">
        <CardHeader>
          <CardTitle>Portfolio Construction</CardTitle>
          <CardDescription>Weighting schemes and risk calculations.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <h3 className="font-semibold text-sm">Inverse-Vol Weights</h3>
          <div className="bg-muted p-4 rounded-md font-mono text-sm text-muted-foreground mb-4">
            <p>w_i ∝ 1 / σ_i</p>
            <p>Normalized to sum to 100%</p>
          </div>

          <h3 className="font-semibold text-sm">Portfolio Risk</h3>
          <div className="bg-muted p-4 rounded-md font-mono text-sm text-muted-foreground">
            <p>Portfolio Volatility = sqrt(w'Σw)</p>
            <p className="text-xs mt-2 opacity-70">Where w is the weight vector and Σ is the empirical covariance matrix over the lookback period.</p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
