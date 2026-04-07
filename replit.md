# Quant Terminal — Equity Ranking & Risk App

## Overview

A mobile-first equity ranking and risk application that pulls real market data from Yahoo Finance, ranks ~1,800 large liquid US stocks (NYSE + NASDAQ, market cap ≥ $2B) using a 3-sleeve alpha composite (S=return-strength, T=trend-quality, Q=quality), and provides portfolio construction and risk analysis tools.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5 (Node.js backend)
- **Data engine**: Python 3.12 + FastAPI + yfinance + pandas + numpy + scipy + scikit-learn + aiohttp
- **Database**: PostgreSQL + Drizzle ORM (available but not used for main data)
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)
- **Frontend**: React + Vite + Tailwind CSS + shadcn/ui + Wouter + @tanstack/react-virtual
- **Caching**: diskcache (Python, 8h TTL) at /tmp/equity_cache

## Architecture

### Python Equity Engine (`artifacts/equity-engine/`)
- `engine.py` — Core data loading, factor computation, clustering, and risk analytics
  - **Two-stage startup**: Stage 1 loads prices + essential metadata (NASDAQ screener + batch quotes) → engine usable in <1s from cache, <60s cold; Stage 2 loads quality fundamentals in background thread
  - **Async price downloader** (`price_adapter.py`): aiohttp with 50 concurrent connections, crumb/cookie auth, exponential backoff; falls back to yfinance sequential batches
  - **Vectorized factors**: numpy matrix ops replace per-ticker loops; batch OLS t-stat via single matrix multiply (0.038s for 2,000 stocks vs ~3s sequential)
  - **Three-layer cache**: factors→rankings→clustering; weight-only changes skip factor recomputation (0.000s cache hits)
  - **Quality enrichment**: Background thread fetches ROE/ROA/margins/D/E via yfinance .info; increments quality_epoch to cascade cache invalidation
  - Computes momentum factors: r1, r6, r12, m6 (6-1), m12 (12-1)
  - Computes vol-adjusted Sharpe factors: s6, s12
  - OLS t-stats (always computed): tstat6, tstat12 — required for T sleeve
  - Quality composite: profitability (ROE/ROA), margins, leverage (winsorized, z-scored, averaged)
  - Sleeve-based alpha: S=0.5×Z(s6)+0.5×Z(s12), T=0.5×Z(t6)+0.5×Z(t12), Q=Z(quality)
  - Alpha = wS×S + wT×T + wQ×Q (defaults: 0.4, 0.4, 0.2)
  - All atomic inputs individually z-scored before sleeve construction
  - Clustering: AgglomerativeClustering (Ward linkage, euclidean) on log-return z-scores for top-N stocks
  - Portfolio risk: covariance matrix, portfolio vol = sqrt(w'Σw), avg pairwise correlation
- `price_adapter.py` — Async Yahoo Finance chart API client (aiohttp, concurrent connections, retry with backoff)
- `server.py` — FastAPI server (port 8001)
  - GZip compression middleware (minimum 1000 bytes)
  - `/status` — data loading progress + enrichment status + timings
  - `/rankings` — ranked stock universe with all factors + sleeve z-scores
  - `/universe-filters` — POST endpoint for applying filters
  - `/portfolio-risk` — POST endpoint for computing portfolio risk metrics

### Express API Server (`artifacts/api-server/`)
- Proxies requests to Python engine at localhost:8001
- Routes: `/api/equity/status`, `/api/equity/rankings`, `/api/equity/universe-filters`, `/api/portfolio/risk`

### React Frontend (`artifacts/equity-ranker/`)
- Dark navy/charcoal financial terminal aesthetic
- **Table virtualization**: @tanstack/react-virtual renders only visible rows (~40 at a time) for 1,800+ stock table
- **Client-side alpha**: Weight slider changes recompute alpha from sleeve z-scores locally (instant, no API call)
- **Debounced server params**: Structural parameter changes (vol_floor, winsor_p, cluster K/N) debounced 400ms before triggering API call
- **Set-based portfolio lookup**: O(1) membership check via `useMemo(() => new Set(holdings))`
- Pages:
  - `/` — Universe Rankings: virtualized sortable table, factor controls, cluster color-coding, add to portfolio
  - `/portfolio` — Portfolio & Risk: holdings basket, weighting modes, risk metrics, cluster distribution
  - `/methodology` — Formula reference panel

## Key Commands

- `pnpm run typecheck` — full typecheck across all packages
- `pnpm run build` — typecheck + build all packages
- `pnpm --filter @workspace/api-spec run codegen` — regenerate API hooks and Zod schemas from OpenAPI spec
- `pnpm --filter @workspace/api-server run dev` — run API server locally
- `python3 artifacts/equity-engine/server.py` — run Python data engine

## Workflows

- `Equity Engine` — Python FastAPI server on port 8001, downloads Yahoo Finance data on start
- `artifacts/api-server: API Server` — Express API on port 8080
- `artifacts/equity-ranker: web` — React Vite frontend on port 24321

## Universe

~1,800 large, liquid US stocks from NYSE + NASDAQ (dynamic, via NASDAQ screener API). Typical load: 2,000–2,100 downloaded → 1,750–1,850 qualifying after filters.

**Pre-filter (NASDAQ API)**: market cap ≥ $2B  
**Engine filters:**
- Price ≥ $5
- Avg daily dollar volume ≥ $10M (63-day median, computed from yfinance Close×Volume)
- Market cap ≥ $1B (backfilled from NASDAQ screener when yfinance fails)
- ≥ 252 trading days history

Excludes: ETFs/mutual funds, LPs/MLPs, SPACs, OTC/pink sheets, non-equity instruments

**Cache keys:**
- `universe_v1` — NASDAQ ticker list (24h TTL)
- `nasdaq_meta_v1` — NASDAQ screener metadata (market cap, name, exchange) for backfill (24h TTL)
- `price_data_v5` — downloaded Close + Volume history (8h TTL)
- `meta_data_v2` — metadata (24h TTL)
- `quality_data_v1` — quality fundamentals from yfinance .info (24h TTL)

## Startup Cache Strategy

### Warm start (has localStorage snapshot — common case)
1. Page loads → `loadRankingsCache()` reads `qt:rankings-v3` from localStorage instantly
2. Table renders immediately with cached rows; header shows "Cached · Xm ago" + spinning Refreshing indicator
3. Status polling continues in background (engine typically responds "ready" within 1–2s from disk cache)
4. When engine is ready, fresh rankings fetched; table atomically swapped; localStorage updated
5. Header shows "Updated: [timestamp]", spinner gone

### Cold start (no localStorage — first run or cache expired)
1. Page loads → `loadRankingsCache()` returns null
2. Table shows "Loading quant engine…" spinner inside empty table body
3. Status polling finds engine "ready" (disk cache) in ~1–2s or "loading" during full download
4. When engine ready, rankings fetched and displayed; saved to localStorage for next warm start

### Cache details
- Cache key: `qt:rankings-v3` (version bump auto-invalidates old entries on schema changes)
- Max age: 24h from `savedAt` — after that, treated as cold start
- Manual refresh: header ↺ button clears localStorage + invalidates React Query, forcing fresh fetch
- No fake data: localStorage only ever holds a real API response from a previous successful fetch

## Data Flow

### Two-Stage Startup
1. **Stage 1** (fast, <1s from cache, <60s cold):
   - Equity Engine starts → fetches universe from NASDAQ screener API
   - Downloads prices via async adapter (50 concurrent connections) or falls back to yfinance sequential batches
   - Builds essential metadata from NASDAQ screener + batch quote API (no per-ticker .info calls)
   - Engine marked "ready" — rankings available with S+T sleeves
2. **Stage 2** (background, ~20-30s):
   - Quality enrichment: per-ticker yfinance .info calls for ROE/ROA/margins/D/E
   - Increments quality_epoch → factor cache invalidated → next ranking request includes Q sleeve
   - Status endpoint shows enrichment: "loading" → "complete" with quality coverage stats

### Three-Layer Cache Architecture
- **Layer 1 — Factors**: Keyed on (vol_floor, winsor_p, quality_epoch). Cost: ~1s (vectorized).
- **Layer 2 — Rankings**: Keyed on factor_key + (w6, w12, w_quality, use_quality). Cost: ~0.05s.
- **Layer 3 — Clustering**: Keyed on ranking_key + (cluster_n, cluster_k, cluster_lookback). Cost: ~0.1s.
- Weight-only changes: Layer 1 HIT, Layer 2+3 MISS → ~0.15s total.
- Full cache hit: 0.000s server-side.

## Factor Details

- **r1** = ln(P_t / P_{t-21}) — 1-month return
- **r6** = ln(P_t / P_{t-126}) — 6-month return
- **r12** = ln(P_t / P_{t-252}) — 12-month return
- **m6** = r6 - r1 — skip-month 6-month momentum
- **m12** = r12 - r1 — skip-month 12-month momentum
- **sigma6** = std(126d daily log returns) × √252 — 6m annualized vol
- **s6** = m6 / max(sigma6, vol_floor) — Sharpe-style adjusted m6
- **S sleeve** = 0.5×Z(s6) + 0.5×Z(s12) — return-strength (Sharpe-adjusted momentum)
- **T sleeve** = 0.5×Z(tstat6) + 0.5×Z(tstat12) — trend-quality (OLS t-stat)
- **Q sleeve** = Z(quality) — quality composite
- **Alpha** = wS×S + wT×T + wQ×Q (defaults 0.4/0.4/0.2); fully auditable per-stock

## Portfolio Construction — 2-Step Process

### Step 1: Base weights (normalized, sum = 1)
- **Equal**: w_i = 1/N
- **Inverse Vol**: w_i ∝ 1/σ_i, σ from log-return std over cov lookback, vol floor = 5%
- **Min Var**: long-only min-variance via SLSQP on empirical covariance matrix Σ; falls back to Inverse Vol (with audit line) if optimizer fails

### Step 2: 15% Vol-target overlay
- `pre_vol = sqrt(w_base' Σ w_base)` — pre-scale portfolio vol (annualized)
- `multiplier = 0.15 / pre_vol`
- `w_final = w_base × multiplier`
- Final weights do NOT sum to 100%. Gross exposure = multiplier.
  - If basket vol < 15% → gross > 100% (levered)
  - If basket vol > 15% → gross < 100% (de-levered)

### Response fields
- `portfolioVol`: final vol after scaling (= 15% by construction)
- `basePortVol`: pre-scale portfolio vol
- `volTargetMultiplier`: scaling multiplier
- `grossExposure`: sum of final weights
- `method`: actual method used (may differ from requested if fallback)
- `fallback`: non-null string when a fallback was triggered, null otherwise
- `volLookback`/`covLookback`: lookback used (same value, both from request)

### Audit line (UI)
Displayed in portfolio page footer: method · target 15% · pre-scale vol · ×multiplier · gross exposure · cov Nd

## Performance Metrics (Typical)
- **Stage 1 cache restore**: ~0.06s
- **Factor computation (vectorized)**: ~1.0s for 2,000 stocks
- **OLS t-stat (batch matrix)**: ~0.04s for 2,000 stocks
- **Quality enrichment**: ~20-30s (background)
- **Rankings (cache hit)**: 0.000s server-side
- **GZip compressed response**: ~550ms for 1,809 stocks (dominated by JSON serialization)
