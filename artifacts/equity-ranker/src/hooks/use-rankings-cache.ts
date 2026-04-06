/**
 * Local rankings cache — stale-while-revalidate for startup performance.
 *
 * Strategy:
 *   - On startup: read from localStorage, render table immediately
 *   - Background: poll engine status, fetch fresh data when ready
 *   - On fresh data arrival: atomically replace display + persist to localStorage
 *
 * Cold start:  no localStorage entry → show empty table + full-screen loading indicator
 * Warm start:  valid localStorage entry → show table instantly + compact background spinner
 *
 * Cache key includes a schema version so structural changes auto-invalidate old entries.
 * Cache expires after MAX_AGE_MS (24h) to ensure staleness is bounded.
 */

import { Stock } from "@workspace/api-client-react";

const CACHE_KEY = "qt:rankings-v3";      // bump version on schema changes
const MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours

export interface CachedRankings {
  stocks: Stock[];
  total: number;
  cachedAt: string | null;   // server-side data timestamp
  savedAt: number;           // client-side localStorage write timestamp (ms)
  paramsHash: string;        // JSON of params used, for display only
}

export function saveRankingsCache(data: {
  stocks: Stock[];
  total: number;
  cachedAt?: string | null;
}, paramsHash: string): void {
  try {
    const entry: CachedRankings = {
      stocks: data.stocks,
      total: data.total,
      cachedAt: data.cachedAt ?? null,
      savedAt: Date.now(),
      paramsHash,
    };
    localStorage.setItem(CACHE_KEY, JSON.stringify(entry));
  } catch {
    // localStorage full or unavailable — silently ignore
  }
}

export function loadRankingsCache(): CachedRankings | null {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const entry = JSON.parse(raw) as CachedRankings;
    // Validate shape
    if (!Array.isArray(entry.stocks) || typeof entry.savedAt !== "number") return null;
    // Check TTL
    if (Date.now() - entry.savedAt > MAX_AGE_MS) {
      localStorage.removeItem(CACHE_KEY);
      return null;
    }
    return entry;
  } catch {
    return null;
  }
}

export function clearRankingsCache(): void {
  try {
    localStorage.removeItem(CACHE_KEY);
  } catch {}
}

/** Human-readable age of a cache entry, e.g. "2 min ago" or "3 hr ago" */
export function formatCacheAge(savedAt: number): string {
  const secs = Math.floor((Date.now() - savedAt) / 1000);
  if (secs < 60) return "just now";
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins} min ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}
