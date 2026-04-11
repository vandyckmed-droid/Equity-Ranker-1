import katex from "katex";
import "katex/dist/katex.min.css";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

function M({ children }: { children: string }) {
  return (
    <span
      dangerouslySetInnerHTML={{
        __html: katex.renderToString(children, { throwOnError: false }),
      }}
    />
  );
}

function Block({ children }: { children: string }) {
  return (
    <div
      className="overflow-x-auto"
      dangerouslySetInnerHTML={{
        __html: katex.renderToString(children, { throwOnError: false, displayMode: true }),
      }}
    />
  );
}

function Row({ label, formula, note }: { label: string; formula: string; note?: string }) {
  return (
    <div className="py-2 border-b border-border/40 last:border-0">
      <div className="flex items-start gap-3 flex-wrap">
        <span className="text-[11px] font-mono text-muted-foreground shrink-0 w-24 pt-0.5">{label}</span>
        <div className="flex-1 min-w-0">
          <Block>{formula}</Block>
          {note && <p className="text-[10px] text-muted-foreground/70 mt-0.5">{note}</p>}
        </div>
      </div>
    </div>
  );
}

export default function FormulasPage() {
  return (
    <div className="p-4 md:p-6 max-w-3xl mx-auto space-y-5">
      <div>
        <h1 className="text-2xl md:text-3xl font-bold tracking-tight mb-1 text-foreground">
          Formulas Reference
        </h1>
        <p className="text-muted-foreground text-sm">
          Pipeline order: Universe → S Sleeve → T Sleeve → Rev Sleeve → Alpha → Portfolio
        </p>
      </div>

      {/* ── 1. RAW RETURNS ── */}
      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">1 — Raw Returns</CardTitle>
        </CardHeader>
        <CardContent className="space-y-0">
          <Row label="r₁ (1 mo)" formula="r_1 = \ln\!\left(\frac{P_t}{P_{t-21}}\right)" note="21 trading days" />
          <Row label="r₆ (6 mo)" formula="r_6 = \ln\!\left(\frac{P_t}{P_{t-126}}\right)" note="126 trading days — raw, no skip" />
          <Row label="r₁₂ (12 mo)" formula="r_{12} = \ln\!\left(\frac{P_t}{P_{t-252}}\right)" note="252 trading days — raw, no skip" />
          <Row
            label="σ₁"
            formula="\sigma_1 = \hat{\sigma}_{21} \times \sqrt{252}"
            note="Annualized; std of 21-day log returns. Floor: 10%"
          />
          <Row
            label="σ₆"
            formula="\sigma_6 = \hat{\sigma}_{126} \times \sqrt{252}"
            note="Annualized; std of 126-day log returns. Floor: 10%"
          />
          <Row
            label="σ₁₂"
            formula="\sigma_{12} = \hat{\sigma}_{252} \times \sqrt{252}"
            note="Annualized; std of 252-day log returns. Floor: 10%"
          />
        </CardContent>
      </Card>

      {/* ── 2. S SLEEVE ── */}
      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">2 — S Sleeve: Return Strength</CardTitle>
        </CardHeader>
        <CardContent className="space-y-0">
          <Row
            label="s₆"
            formula="s_6 = \frac{r_6}{\max(\sigma_6,\,0.10)} \times \sqrt{2}"
            note="Annualized Sharpe proxy — 6-month window"
          />
          <Row
            label="s₁₂"
            formula="s_{12} = \frac{r_{12}}{\max(\sigma_{12},\,0.10)} \times 1"
            note="Annualized Sharpe proxy — 12-month window"
          />
          <Row
            label="S Sleeve"
            formula="\text{sSleeve} = 0.5 \cdot Z(s_6) \;+\; 0.5 \cdot Z(s_{12})"
            note="Z = cross-sectional z-score after winsorizing at ±winsorP%"
          />
        </CardContent>
      </Card>

      {/* ── 3. T SLEEVE ── */}
      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">3 — T Sleeve: Trend Consistency</CardTitle>
        </CardHeader>
        <CardContent className="space-y-0">
          <Row
            label="OLS"
            formula="\ln P_t = \alpha + \beta\,t + \varepsilon"
            note="Log-price OLS regression"
          />
          <Row
            label="tstat₆"
            formula="\text{tstat}_6 = \frac{\hat\beta}{\mathrm{SE}(\hat\beta)}\;\Bigg|_{126\text{-day}}"
          />
          <Row
            label="tstat₁₂"
            formula="\text{tstat}_{12} = \frac{\hat\beta}{\mathrm{SE}(\hat\beta)}\;\Bigg|_{252\text{-day}}"
          />
          <Row
            label="T Sleeve"
            formula="\text{tSleeve} = 0.5 \cdot Z(\text{tstat}_6) \;+\; 0.5 \cdot Z(\text{tstat}_{12})"
          />
        </CardContent>
      </Card>

      {/* ── 4. REV SLEEVE ── */}
      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">4 — Rev Sleeve: Short-Term Reversal</CardTitle>
        </CardHeader>
        <CardContent className="space-y-0">
          <Row
            label="s₁"
            formula="s_1 = -\frac{r_1}{\max(\sigma_1,\,0.10)} \times \sqrt{12}"
            note="Annualized reversal signal — sign-flipped so high = mean-reverting winner"
          />
          <Row
            label="Rev Sleeve"
            formula="\text{revSleeve} = Z(s_1)"
            note="Standalone z-score; penalises last-month momentum"
          />
        </CardContent>
      </Card>

      {/* ── 5. COMPOSITE ALPHA ── */}
      <Card className="border-border bg-card border-primary/30">
        <CardHeader className="pb-2">
          <CardTitle className="text-base text-primary">5 — Composite Alpha</CardTitle>
        </CardHeader>
        <CardContent className="space-y-0">
          <Row
            label="Alpha"
            formula="\alpha = \frac{w_S \cdot \text{sSleeve} \;+\; w_T \cdot \text{tSleeve} \;+\; w_{\text{Rev}} \cdot \text{revSleeve}}{w_S + w_T + w_{\text{Rev}}}"
          />
          <div className="pt-3 text-[11px] text-muted-foreground space-y-0.5">
            <p>Defaults: <M>{"w_S = 0.5"}</M>, <M>{"w_T = 0.5"}</M>, <M>{"w_{\\text{Rev}} = 0.2"}</M></p>
            <p>All sleeves are already cross-sectional z-scores — no further normalization needed.</p>
            <p>Stocks ranked descending by α. Top 20 marked ★. Missing sleeve → z = 0.</p>
          </div>
        </CardContent>
      </Card>

      {/* ── 6. QUALITY ── */}
      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">6 — Profitability / Quality (Display Only)</CardTitle>
        </CardHeader>
        <CardContent className="space-y-0">
          <Row
            label="OPA"
            formula="\text{OPA} = \frac{\text{Operating Income}}{\text{Avg Total Assets}}"
            note="Fallback hierarchy: Op Income → EBIT → Net Income"
          />
          <Row
            label="zQ"
            formula="z_Q = Z\!\left(\text{OPA}\right)"
            note="Cross-sectional within industry → sector → universe. Display only; does not enter α."
          />
        </CardContent>
      </Card>

      {/* ── 7. CROSS-SECTIONAL Z-SCORE ── */}
      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">7 — Cross-Sectional Z-Score</CardTitle>
        </CardHeader>
        <CardContent className="space-y-0">
          <Row
            label="Winsorise"
            formula="x_i \leftarrow \mathrm{clip}(x_i,\; Q_p,\; Q_{1-p})"
            note="p = winsorP (default 2%)"
          />
          <Row
            label="Z-score"
            formula="Z(x_i) = \frac{x_i - \mu_{\mathcal{U}}}{\sigma_{\mathcal{U}}}"
            note="μ, σ computed over the full ranked universe 𝒰"
          />
        </CardContent>
      </Card>

      {/* ── 8. PORTFOLIO ── */}
      <Card className="border-border bg-card">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">8 — Portfolio Construction</CardTitle>
        </CardHeader>
        <CardContent className="space-y-0">
          <Row
            label="Risk Parity"
            formula="\min_w \sum_i \!\left(RC_i - \overline{RC}\right)^2 \quad \text{s.t.}\; \mathbf{1}^\top w = 1,\; 0 \le w_i \le 0.15"
          />
          <Row
            label="Risk contrib"
            formula="RC_i = \frac{w_i\,(\Sigma w)_i}{w^\top \Sigma w}"
            note="EWMA covariance λ = 0.94, annualized + ridge"
          />
          <Row
            label="Vol target"
            formula="\text{mult} = \min\!\left(\frac{0.15}{\sqrt{w^\top\Sigma w}},\; 1\right)"
            note="15% annualised target — no leverage"
          />
          <Row
            label="Inv Vol"
            formula="w_i = \frac{1/\sigma_i}{\sum_j 1/\sigma_j}"
          />
          <Row
            label="Signal/Vol"
            formula="w_i = \frac{\max(\alpha_i,0)/\sigma_i}{\sum_j \max(\alpha_j,0)/\sigma_j}"
          />
        </CardContent>
      </Card>
    </div>
  );
}
