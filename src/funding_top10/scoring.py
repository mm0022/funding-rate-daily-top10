"""Scoring / ranking for funding-rate symbols.

Pipeline used by the daily report:
  1. Annualize std_7d_funding_rate using each symbol's funding_interval_hours.
     std_annual = std_per_event * sqrt(N_events_per_year)
     N_events_per_year = (24 / interval_hours) * 365
  2. Hard filter: drop rows with haircut < MIN_HAIRCUT or OI < MIN_OI_USD.
  3. Score = one-sided confidence lower bound on annualized return:
         score = annualized_apr - z * annualized_std
     where annualized_apr = sum_7d_funding_rate * 365 / 7 and z is the
     one-sided z-score (default 1.645 ⇔ 95% confidence). The score equals
     "with z-confidence, real annualized return will be at least this much".
  4. Sort by score desc, take top N.

`select_rows_to_show` then merges that top-N with the biyi-strategy tickers
(no filtering on biyi rows) so the operator always sees their book regardless
of where biyi tickers fall on the score leaderboard.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

import pandas as pd


TOP_N_FINAL = 20
MIN_HAIRCUT = 0.5
MIN_OI_USD = 5_000_000  # $5M minimum open interest for a symbol to be a candidate
DEFAULT_CONFIDENCE_Z = 1.645   # one-sided 95% confidence z-score


@dataclass(frozen=True)
class ScoreWeights:
    """Deprecated: composite-rank weights are no longer used; score is now
    Sharpe-like (apr / std). Kept so older config.yaml doesn't break loading."""
    apr7: float = 0.4
    std: float = 0.2
    haircut: float = 0.2
    oi: float = 0.2


def annualize_std(std_per_event: float, interval_hours: float) -> float:
    """std_per_event * sqrt(events_per_year). NaN/None safe."""
    if std_per_event is None or interval_hours is None:
        return float("nan")
    try:
        s = float(std_per_event)
        h = float(interval_hours)
    except (TypeError, ValueError):
        return float("nan")
    if math.isnan(s) or math.isnan(h) or h <= 0:
        return float("nan")
    events_per_year = (24.0 / h) * 365.0
    return s * math.sqrt(events_per_year)


def add_annualized_std(df: pd.DataFrame) -> pd.DataFrame:
    """Add a ``std_7d_annualized`` column derived from std_7d × sqrt(N_yr)."""
    df = df.copy()
    df["std_7d_annualized"] = [
        annualize_std(s, h)
        for s, h in zip(df["std_7d_funding_rate"], df["funding_interval_hours"])
    ]
    return df


def compute_score(df: pd.DataFrame, *, z: float = DEFAULT_CONFIDENCE_Z) -> pd.Series:
    """One-sided confidence-bound score: annualized_apr - z * annualized_std.

    annualized_apr = sum_7d_funding_rate * 365 / 7
    annualized_std comes from ``add_annualized_std`` (`std_7d_annualized`).

    Interpretation: with the chosen one-sided confidence (default 95%, z=1.645),
    the real annualized funding return will be at least this much. Negative
    score = "could lose money at that confidence".

    z=1.0   ≈ 84% confidence, lighter std penalty (aggressive)
    z=1.645 ≈ 95% confidence, default
    z=2.0   ≈ 97.7% confidence, heavier penalty (conservative)
    """
    if df.empty:
        return pd.Series([], dtype=float, index=df.index)
    annualized_apr = df["sum_7d_funding_rate"] * 365.0 / 7.0
    annualized_std = df["std_7d_annualized"]
    return annualized_apr - z * annualized_std


# Aliases kept for backwards compatibility with older imports / tests.
def compute_sharpe_score(df: pd.DataFrame) -> pd.Series:
    return compute_score(df)


def compute_composite_score(df: pd.DataFrame, weights: ScoreWeights) -> pd.Series:  # noqa: ARG001
    return compute_score(df)


def select_top(
    df: pd.DataFrame,
    weights: ScoreWeights,  # noqa: ARG001  (kept for signature compat; unused)
    *,
    top_n_final: int = TOP_N_FINAL,
    min_haircut: float | None = MIN_HAIRCUT,
    min_oi_usd: float | None = MIN_OI_USD,
    confidence_z: float = DEFAULT_CONFIDENCE_Z,
) -> pd.DataFrame:
    """Hard-filter + confidence-bound score + sort + top N.

    The input ``df`` is expected to already contain ``std_7d_annualized``
    (call :func:`add_annualized_std` first).

    Hard filters (any None to skip):
      - haircut >= min_haircut (NaN/"None" string rows dropped)
      - open_interest_value >= min_oi_usd  (NaN rows dropped)
    """
    required = {
        "sum_7d_funding_rate",
        "std_7d_annualized",
        "haircut",
        "open_interest_value",
    }
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"select_top: missing required columns {missing}")

    filtered = df
    if min_haircut is not None and "haircut" in df.columns:
        filtered = filtered[filtered["haircut"].astype(str) != "None"]
        haircut_numeric = pd.to_numeric(filtered["haircut"], errors="coerce")
        filtered = filtered[haircut_numeric >= min_haircut]

    if min_oi_usd is not None and "open_interest_value" in df.columns:
        oi_numeric = pd.to_numeric(filtered["open_interest_value"], errors="coerce")
        filtered = filtered[oi_numeric >= min_oi_usd]

    if filtered.empty:
        out = filtered.copy()
        out["score"] = pd.Series([], dtype=float)
        return out

    scored = filtered.copy()
    scored["score"] = compute_score(scored, z=confidence_z)
    return (
        scored.sort_values("score", ascending=False, na_position="last")
        .head(top_n_final)
        .reset_index(drop=True)
    )


def _ticker_series(df: pd.DataFrame) -> pd.Series:
    return df["base"].astype(str) + "/" + df["quote"].astype(str)


def select_rows_to_show(
    funding_df: pd.DataFrame,
    biyi_tickers: Iterable[str],
    weights: ScoreWeights,
    *,
    top_n_final: int = TOP_N_FINAL,
    min_haircut: float | None = MIN_HAIRCUT,
    min_oi_usd: float | None = MIN_OI_USD,
    confidence_z: float = DEFAULT_CONFIDENCE_Z,
) -> pd.DataFrame:
    """Pipeline entry: annualize std, hard-filter + score top-N, merge biyi rows."""
    df = add_annualized_std(funding_df)

    top = select_top(
        df,
        weights,
        top_n_final=top_n_final,
        min_haircut=min_haircut,
        min_oi_usd=min_oi_usd,
        confidence_z=confidence_z,
    )

    biyi_set = set(biyi_tickers)
    if biyi_set and "base" in df.columns and "quote" in df.columns:
        all_tickers = _ticker_series(df)
        biyi_rows = df[all_tickers.isin(biyi_set)].copy()
    else:
        biyi_rows = df.iloc[0:0].copy()

    if not biyi_rows.empty:
        biyi_rows["score"] = compute_score(biyi_rows, z=confidence_z)

    if len(top):
        top_keys = set(_ticker_series(top).tolist())
    else:
        top_keys = set()

    if len(biyi_rows):
        biyi_keys = _ticker_series(biyi_rows)
        biyi_extra = biyi_rows[~biyi_keys.isin(top_keys)]
    else:
        biyi_extra = biyi_rows

    merged = pd.concat([top, biyi_extra], ignore_index=True)
    if len(merged):
        merged = merged.sort_values(
            "score", ascending=False, na_position="last"
        ).reset_index(drop=True)
    return merged
