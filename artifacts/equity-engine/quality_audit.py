"""
quality_audit.py — Coverage stats and per-ticker audit records.

Provides:
  build_coverage_report(opa_results, universe) -> dict
      Returns summary stats: total, available count/pct, breakdown by formula,
      unavailable count, top reasons unavailable.

  build_per_ticker_audit(opa_results, universe) -> list[dict]
      Returns one audit record per ticker (in universe order).
      Each record matches the spec: ticker, available, formula, numerator,
      denominator, using_avg_assets, source_fields, period_date, reason.

  export_audit_json(audit_records) -> str
      Returns JSON string of the full audit list (for the /quality-export endpoint).
"""

import json
from collections import Counter
from typing import Optional


def build_coverage_report(opa_results: dict, universe: list) -> dict:
    """
    Args:
        opa_results:  {ticker: formula_result} from quality_formula.compute_opa_all
        universe:     list of tickers in the ranked universe

    Returns a summary dict:
        {
          "universe_count":    int,
          "quality_computed":  int,   # tickers present in opa_results
          "available":         int,
          "available_pct":     float,
          "unavailable":       int,
          "formula_breakdown": {"op_income/...": int, ...},
          "reason_breakdown":  {"total_assets_missing": int, ...},  # top reasons
          "using_avg_assets":  int,
          "using_cur_assets":  int,
        }
    """
    universe_set = set(universe)
    n_universe   = len(universe)

    available       = 0
    unavailable     = 0
    formula_counts  = Counter()
    reason_counts   = Counter()
    avg_assets_ct   = 0
    cur_assets_ct   = 0

    for ticker in universe:
        rec = opa_results.get(ticker)
        if rec is None:
            reason_counts["not_computed"] += 1
            unavailable += 1
            continue
        if rec.get("available"):
            available += 1
            formula_counts[rec["formula"]] += 1
            if rec.get("using_avg_assets"):
                avg_assets_ct += 1
            else:
                cur_assets_ct += 1
        else:
            unavailable += 1
            reason = rec.get("reason") or "unknown"
            # Bucket fetch errors together
            if reason.startswith("fetch_error"):
                reason = "fetch_error"
            reason_counts[reason] += 1

    avail_pct = round(available / n_universe * 100, 2) if n_universe > 0 else 0.0

    return {
        "universe_count":    n_universe,
        "quality_computed":  len(opa_results),
        "available":         available,
        "available_pct":     avail_pct,
        "unavailable":       unavailable,
        "formula_breakdown": dict(formula_counts.most_common()),
        "reason_breakdown":  dict(reason_counts.most_common(10)),
        "using_avg_assets":  avg_assets_ct,
        "using_cur_assets":  cur_assets_ct,
    }


def build_per_ticker_audit(opa_results: dict, universe: list) -> list:
    """
    Returns a list of per-ticker audit dicts, one per ticker in universe.
    """
    records = []
    for ticker in universe:
        rec = opa_results.get(ticker)
        if rec is None:
            records.append({
                "ticker":           ticker,
                "available":        False,
                "formula":          "unavailable",
                "opa":              None,
                "numerator":        None,
                "denominator":      None,
                "using_avg_assets": False,
                "assets_current":   None,
                "assets_prior":     None,
                "period_date":      None,
                "source_fields":    {},
                "reason":           "not_computed",
            })
        else:
            records.append({
                "ticker":           rec.get("ticker", ticker),
                "available":        rec.get("available", False),
                "formula":          rec.get("formula", "unavailable"),
                "opa":              rec.get("opa"),
                "numerator":        rec.get("numerator"),
                "denominator":      rec.get("denominator"),
                "using_avg_assets": rec.get("using_avg_assets", False),
                "assets_current":   rec.get("assets_current"),
                "assets_prior":     rec.get("assets_prior"),
                "period_date":      rec.get("period_date"),
                "source_fields":    rec.get("source_fields", {}),
                "reason":           rec.get("reason"),
            })
    return records


def export_audit_json(audit_records: list, indent: int = 2) -> str:
    """Return JSON string of the full per-ticker audit list."""
    return json.dumps(audit_records, indent=indent, default=str)
