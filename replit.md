# Quant Terminal — Equity Ranking & Risk App

## Overview

Quant Terminal is a mobile-first equity ranking and risk application that ranks ~1,817 large liquid US stocks (NYSE + NASDAQ, market cap ≥ $2B) using real Yahoo Finance data and a residual momentum alpha model.

## User Preferences

I prefer clear, concise, and structured explanations.
I value iterative development and prefer to be consulted before major architectural or design changes.
I expect the coding agent to maintain high code quality, adhering to the established monorepo structure and technology stack.
All user-facing labels for "clusters" should be "Group" / "Grp" / "G0…G9". Internal code, API schema, Python engine, and cache keys should retain "cluster" as the data field name.

## System Architecture

The application is built as a monorepo using `pnpm workspaces`, targeting Node.js 24 and TypeScript 5.9.

**Technology Stack:**
- **Backend API**: Express 5 (Node.js)
- **Data Engine**: Python 3.12 with FastAPI, leveraging `yfinance`, `pandas`, `numpy`, `scipy`, `scikit-learn`, and `aiohttp` for data processing and mathematical operations.
- **Frontend**: React with Vite, styled using Tailwind CSS and `shadcn/ui`, `Wouter` for routing, and `@tanstack/react-virtual` for performance.
- **Database**: PostgreSQL with Drizzle ORM (available but not primary for main data).
- **Validation**: Zod.
- **API Codegen**: Orval (from OpenAPI spec).
- **Build Tool**: esbuild.
- **Caching**: `diskcache` (Python) for persistent data storage.

**Architectural Components:**

1.  **Python Equity Engine (`artifacts/equity-engine/`)**:
    *   Handles data loading, factor computation, clustering, and risk analytics.
    *   **Two-stage startup**: Fast initial loading from cache for essential data (<1s) followed by background quality fundamental enrichment.
    *   Features an asynchronous price downloader with robust error handling.
    *   Employs vectorized factor computation using `numpy` for efficiency.
    *   **Three-layer caching**: Factors, rankings, and clustering results are aggressively cached to ensure high performance.
    *   Quality enrichment fetches data from SEC EDGAR XBRL API in a background thread.
    *   **Alpha model** (full formula):
        - `m6/m12` = skip-last-month log return (6m/12m), Sharpe-adjusted by vol → `s6`, `s12`
        - OLS trend t-stats: `tstat6`, `tstat12`
        - **Residual momentum**: batch OLS of each stock on [VTI, sector_ETF] (no intercept) → summed residuals `res6`, `res12`
        - `zR6/zR12` = winsorize(2%) + z-score residuals cross-sectionally
        - `S6_blend = 0.7·zS6 + 0.3·zR6`, `S12_blend = 0.7·zS12 + 0.3·zR12`
        - `sSleeve = 0.5·S6_blend + 0.5·S12_blend`
        - `tSleeve = 0.5·zT6 + 0.5·zT12`
        - `alpha = 0.5·sSleeve + 0.5·tSleeve`  (formula: `S+T+Resid`)
    *   **Sector/benchmark layer**: GICS sector map (99.9% coverage), VTI market benchmark, 11 XL sector ETFs; built deterministically from metadata with `_TICKER_SECTOR_OVERRIDE` for hard-to-classify names.
    *   Uses AgglomerativeClustering for stock grouping.
    *   Calculates portfolio risk metrics including covariance matrix and average pairwise correlation.
    *   FastAPI server (`server.py`) exposes endpoints for status, rankings, universe filtering, portfolio risk, and correlation-constrained basket seeding.

2.  **Express API Server (`artifacts/api-server/`)**:
    *   Acts as a proxy for requests to the Python Equity Engine.
    *   Handles `camelCase` to `snake_case` field mapping for seamless integration.

3.  **React Frontend (`artifacts/equity-ranker/`)**:
    *   Presents a dark navy/charcoal financial terminal aesthetic.
    *   Utilizes table virtualization for efficient rendering of large datasets.
    *   Enables client-side alpha recomputation for instant feedback on weight changes.
    *   Debounces structural parameter changes to optimize API calls.
    *   **Pages**:
        *   `/`: Universe Rankings with sortable table, factor controls, and cluster color-coding.
        *   `/portfolio`: Portfolio & Risk analysis, including holdings basket, weighting modes, and risk metrics.
        *   `/methodology`: Reference for formula details.
    *   **Mobile-first Design**: Compact 2-line stock row layout for small screens.
    *   **Portfolio Summary**: Redesigned with premium analysis panels:
        *   **Portfolio Intelligence**: Auto-generated plain-English narrative from live metrics (vol scaling, correlation quality, largest risk driver, breadth).
        *   **9-metric grid (3×3)**: Method, Base Vol, Target Vol, Scale, Invested%, Cash/SGOV%, Names, Max Wt%, Avg Corr — with color signals (amber=warning, green=healthy, dim=inactive).
        *   **Risk Contribution Chart**: Horizontal bars overlaying risk-contrib vs position weight per holding, flagging amber when risk > 1.4× weight.
        *   **Sector & Group Exposure**: Full bar charts for all sectors/groups, not capped at top-3.
        *   **Constituent Table**: Base weight (with mini bar), risk%, individual vol, group ID — sorted by weight.
        *   **Diversify Suggestions**: Auto-suggests high-alpha stocks from underrepresented groups/sectors via deficit scoring, with Group/Sector/Both toggle.
    *   **Auto-fetch on direct navigation**: PortfolioPage fetches `/api/equity/rankings` automatically when `rankedStocks` is empty (e.g., user lands on /portfolio directly), seeding the in-memory universe needed for Diversify Suggestions.

**Universe & Filtering**:
- Approximately 1,800 large, liquid US stocks (NYSE + NASDAQ) filtered dynamically.
- **Pre-filters**: Market cap ≥ $2B.
- **Engine filters**: Price ≥ $5, Avg daily dollar volume ≥ $10M, Market cap ≥ $1B (backfilled), ≥ 252 trading days history.
- Excludes ETFs, mutual funds, SPACs, OTC, and non-equity instruments.
- **Optional universe filters**: `secFilerOnly`, `excludeSectors`, `requireQuality`.
- `/rankings` responses include an `audit` object with filter details, exclusions, sector breakdown, quality coverage, `qualityInputDist` (distribution of 0-5 input counts per stock), and `qualityFieldMissingRates` (per-field missing %).
- Each `Stock` now carries `qualityInputCount` (0-5, count of non-null individual quality inputs: ROE, ROA, GrossM, OpM, D/E).

**Quality Audit Transparency Layer (implemented)**:
- **Per-stock expanded row**: "Quality Audit" section replaces separate Raw/Z columns; shows confidence badge (High ≥5/5, Medium 3-4/5, Low 0-2/5), 5 paired raw→z-score rows with availability dots, Q sleeve value, and Q contribution to alpha (wQ × qSleeve / totalW α points).
- **Universe-level badge**: "Q X%" status badge is now an interactive `QualityAuditBadge` component (`src/components/QualityAuditBadge.tsx`) that opens a dropdown showing the input-count distribution bar chart and per-field missing-rate bars.

**Startup and Caching Strategy (Snapshot-First Architecture)**:
- **Engine Startup**: Restores universe, prices, and metadata from persistent disk cache for rapid initialization. Quality enrichment is incremental and backgrounded.
- **Frontend Warm Start**: Renders immediately from `localStorage` snapshot, then updates with fresh data from the engine.
- **Frontend Cold Start**: Displays loading spinner until engine is ready and data is fetched.
- **Cache Degradation Protection**: Prevents overwriting rich data with less complete snapshots.
- **Cache Details**: `localStorage` uses versioned keys (`qt:rankings-v3`) with a 24h max age.

**Data Flow**:
- **Two-Stage Startup**: Engine first provides S+T sleeves rapidly, then Q sleeve after background quality enrichment.
- **Three-Layer Cache Architecture**: Optimizes performance by caching factors, rankings, and clustering results, allowing for rapid recomputation on parameter changes.

**Portfolio Construction (2-Step Process)**:
- **Step 1: Base weights (normalized)**: Six methods available (Equal, Inverse Vol, Signal / Vol, Risk Parity, Min Variance, Mean-Variance), with fallbacks to Inverse Vol.
- **Step 2: 15% Vol-target overlay**: Scales base weights to achieve a target portfolio volatility, potentially leading to levered or delevered gross exposure.

## External Dependencies

-   **Yahoo Finance**: Market data (prices, volumes, metadata)
-   **NASDAQ API**: Stock screener data for universe selection and metadata backfill
-   **SEC EDGAR XBRL API**: Quality fundamentals (ROE, ROA, margins, D/E)
-   **PostgreSQL**: Database (currently available but not primary for main data)
-   **Express.js**: Node.js web application framework
-   **FastAPI**: Python web framework for the data engine
-   **React**: Frontend JavaScript library
-   **Vite**: Frontend build tool
-   **Tailwind CSS**: Utility-first CSS framework
-   **shadcn/ui**: UI component library
-   **Wouter**: React router
-   **@tanstack/react-virtual**: React virtualization library for large lists
-   **pnpm**: Package manager
-   **TypeScript**: Programming language
-   **Zod**: Schema declaration and validation library
-   **Orval**: OpenAPI to client code generator
-   **esbuild**: JavaScript bundler
-   **yfinance**: Python library for downloading Yahoo Finance data
-   **pandas**: Python data analysis and manipulation library
-   **numpy**: Python library for numerical computing
-   **scipy**: Python library for scientific computing
-   **scikit-learn**: Python machine learning library (for clustering)
-   **aiohttp**: Asynchronous HTTP client/server for Python
-   **diskcache**: Python disk-backed cache library