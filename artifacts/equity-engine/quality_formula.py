"""
quality_formula.py — Formula application layer.

Applies the OpProfAssets formula hierarchy to raw financial data:

  Priority 1:  Operating Income  / Average Total Assets  → formula: "op_income/avg_assets"
  Priority 2:  EBIT              / Average Total Assets  → formula: "ebit/avg_assets"
  Priority 3:  Net Income        / Average Total Assets  → formula: "net_income/avg_assets"

  (If only current assets available, no prior: uses current directly)
  (If total assets = 0 or missing: marks unavailable)

Output per ticker:
  {
    "ticker":         str,
    "available":      bool,
    "formula":        "op_income/avg_assets" | "ebit/avg_assets" | "net_income/avg_assets" | "unavailable",
    "opa":            float | None,      # Operating Profit / Assets ratio
    "numerator":      float | None,
    "denominator":    float | None,      # assets used in denominator
    "using_avg_assets": bool,
    "assets_current": float | None,
    "assets_prior":   float | None,
    "period_date":    str | None,
    "source_fields":  dict,             # which raw fields were found/missing
    "reason":         str | None,       # if unavailable, why
  }
"""

from typing import Optional

_MIN_ASSETS = 1e6    # $1M minimum assets to be considered valid


def _avg_assets(current: Optional[float], prior: Optional[float]) -> tuple:
    """
    Compute denominator and whether averaging was used.
    Returns (denominator, using_avg_assets).
    """
    if current is None or current < _MIN_ASSETS:
        return None, False
    if prior is not None and prior >= _MIN_ASSETS:
        return (current + prior) / 2.0, True
    return current, False


def _apply_formula(raw: dict) -> dict:
    """Apply the formula hierarchy to one raw record."""
    ticker = raw.get("ticker", "?")

    assets_current = raw.get("total_assets_current")
    assets_prior   = raw.get("total_assets_prior")
    denom, using_avg = _avg_assets(assets_current, assets_prior)

    source_fields = {
        "operating_income_key": raw.get("operating_income_key"),
        "ebit_key":             raw.get("ebit_key"),
        "net_income_key":       raw.get("net_income_key"),
        "total_assets_key":     raw.get("total_assets_key"),
        "period_date":          raw.get("period_date"),
        "fetch_error":          raw.get("error"),
    }

    base = {
        "ticker":           ticker,
        "available":        False,
        "formula":          "unavailable",
        "opa":              None,
        "numerator":        None,
        "denominator":      denom,
        "using_avg_assets": using_avg,
        "assets_current":   assets_current,
        "assets_prior":     assets_prior,
        "period_date":      raw.get("period_date"),
        "source_fields":    source_fields,
        "reason":           None,
    }

    # Gate: need a valid denominator
    if denom is None:
        if raw.get("error"):
            base["reason"] = f"fetch_error: {raw['error'][:120]}"
        elif assets_current is None:
            base["reason"] = "total_assets_missing"
        else:
            base["reason"] = f"assets_below_minimum (${assets_current:,.0f})"
        return base

    # Try formula hierarchy
    candidates = [
        (raw.get("operating_income"), "op_income/avg_assets" if using_avg else "op_income/assets"),
        (raw.get("ebit"),             "ebit/avg_assets"      if using_avg else "ebit/assets"),
        (raw.get("net_income"),       "net_income/avg_assets" if using_avg else "net_income/assets"),
    ]

    for numerator, formula_label in candidates:
        if numerator is not None:
            base["available"]      = True
            base["formula"]        = formula_label
            base["opa"]            = numerator / denom
            base["numerator"]      = numerator
            base["reason"]         = None
            return base

    # All numerators missing
    missing = []
    if raw.get("operating_income") is None:
        missing.append("operating_income")
    if raw.get("ebit") is None:
        missing.append("ebit")
    if raw.get("net_income") is None:
        missing.append("net_income")
    base["reason"] = "numerator_missing: " + ", ".join(missing) if missing else "numerator_missing"
    return base


def compute_opa_all(raw_data: dict) -> dict:
    """
    Apply formula hierarchy to all tickers.

    Args:
        raw_data: {ticker: raw_record}  from quality_loader

    Returns:
        {ticker: formula_result}
    """
    return {ticker: _apply_formula(rec) for ticker, rec in raw_data.items()}
