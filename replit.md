# Quant Terminal — Equity Ranking & Risk App

## Overview

A mobile-first equity ranking and risk application that pulls real market data from Yahoo Finance, ranks ~1,800 large liquid US stocks (NYSE + NASDAQ, market cap ≥ $2B) using a 3-sleeve alpha composite (S=return-strength, T=trend-quality, Q=quality), and provides portfolio construction and risk analysis tools.

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5 (Node.js backend)
- **Data engine**: Python 3.12 + FastAPI + yfinance + pandas + numpy + scipy + scikit-learn
- **Database**: PostgreSQL + Drizzle ORM (available but not used for main data)
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)
- **Frontend**: React + Vite + Tailwind CSS + shadcn/ui + Wouter
- **Caching**: diskcache (Python, 8h TTL) at /tmp/equity_cache

## Architecture

### Python Equity Engine (`artifacts/equity-engine/`)
- `engine.py` — Core data loading, factor computation, clustering, and risk analytics
  - Downloads adjusted daily prices (2y history) from Yahoo Finance via yfinance
  - Computes momentum factors: r1, r6, r12, m6 (6-1), m12 (12-1)
  - Computes vol-adjusted Sharpe factors: s6, s12
  - OLS t-stats (always computed): tstat6, tstat12 — required for T sleeve
  - Quality composite: profitability (ROE/ROA), margins, leverage (winsorized, z-scored, averaged)
  - Sleeve-based alpha: S=0.5×Z(s6)+0.5×Z(s12), T=0.5×Z(t6)+0.5×Z(t12), Q=Z(quality)
  - Alpha = wS×S + wT×T + wQ×Q (defaults: 0.4, 0.4, 0.2)
  - All atomic inputs individually z-scored before sleeve construction
  - Clustering: AgglomerativeClustering (Ward linkage, euclidean) on log-return z-scores for top-N stocks
  - Portfolio risk: covariance matrix, portfolio vol = sqrt(w'Σw), avg pairwise correlation
  - Cache: 8-hour disk cache using diskcache
- `server.py` — FastAPI server (port 8001)
  - `/status` — data loading progress
  - `/rankings` — ranked stock universe with all factors
  - `/universe-filters` — POST endpoint for applying filters
  - `/portfolio-risk` — POST endpoint for computing portfolio risk metrics

### Express API Server (`artifacts/api-server/`)
- Proxies requests to Python engine at localhost:8001
- Routes: `/api/equity/status`, `/api/equity/rankings`, `/api/equity/universe-filters`, `/api/portfolio/risk`

### React Frontend (`artifacts/equity-ranker/`)
- Dark navy/charcoal financial terminal aesthetic
- Pages:
  - `/` — Universe Rankings: sortable table, factor controls, cluster color-coding, add to portfolio
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
- `meta_data_v2` — yfinance quality metadata (24h TTL)

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

## Data Flow (full download)

1. Equity Engine starts → calls `_fetch_universe()` to get NYSE+NASDAQ ticker list from NASDAQ screener API (≥$2B mcap), saves NASDAQ metadata (market cap, name) to `nasdaq_meta_v1`
2. Downloads prices for ~2,100 tickers in sequential batches of 100 via yfinance (avoids Yahoo rate-limit thread exhaustion from parallel batching)
3. Loads quality metadata (ROE, ROA, margins, DE ratio, quoteType) via yfinance — backfills market_cap, exchange, name from `nasdaq_meta_v1` when yfinance returns 401
4. Results cached to disk for 8h (diskcache at /tmp/equity_cache)
5. Frontend polls `/api/equity/status` every 5s until ready
6. When ready, fetches full rankings via `/api/equity/rankings`
7. On success, response saved to localStorage `qt:rankings-v3` for next warm start

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

## Weighting Methods

- **Equal weight**: w_i = 1/N
- **Inverse vol**: w_i ∝ 1/sigma12_i, normalized to 100%
- **Manual**: user-specified weights
