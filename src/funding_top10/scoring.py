"""Scoring / ranking for funding-rate symbols.

Pipeline used by the daily report:
  1. Annualize std_7d_funding_rate using each symbol's funding_interval_hours.
     std_annual = std_per_event * sqrt(N_events_per_year)
     N_events_per_year = (24 / interval_hours) * 365
  2. Drop rows whose haircut is null / < MIN_HAIRCUT.
  3. Compute a composite 0..1 score = sum(weight_i * percentile_rank_i) over
     four metrics (7d_apr, std, haircut, OI). std uses an inverted rank
     because lower std is better.
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


@dataclass(frozen=True)
class ScoreWeights:
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


def compute_composite_score(df: pd.DataFrame, weights: ScoreWeights) -> pd.Series:
    """Rank-based composite score, returned as a Series aligned with df.index.

    Each metric is converted to a percentile rank in [0, 1] (pct=True). std's
    rank is inverted so that smaller-std rows contribute MORE to the score.
    The output is the weighted sum (NOT normalised — caller treats it as a
    relative score, sort-key only).
    """
    if df.empty:
        return pd.Series([], dtype=float, index=df.index)

    apr_rank = df["sum_7d_funding_rate"].rank(pct=True, na_option="bottom")
    std_rank = df["std_7d_annualized"].rank(pct=True, na_option="top")
    hc_rank = df["haircut"].rank(pct=True, na_option="bottom")
    oi_rank = df["open_interest_value"].rank(pct=True, na_option="bottom")

    # std: lower is better → invert rank
    std_inv_rank = 1.0 - std_rank

    return (
        weights.apr7 * apr_rank
        + weights.std * std_inv_rank
        + weights.haircut * hc_rank
        + weights.oi * oi_rank
    )


def select_top(
    df: pd.DataFrame,
    weights: ScoreWeights,
    *,
    top_n_final: int = TOP_N_FINAL,
    min_haircut: float | None = MIN_HAIRCUT,
) -> pd.DataFrame:
    """Filter haircut + score + sort + top N.

    The input ``df`` is expected to already contain ``std_7d_annualized``
    (call :func:`add_annualized_std` first).
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
        # Drop string "None" and rows with haircut < min_haircut (NaN dropped too)
        filtered = filtered[filtered["haircut"].astype(str) != "None"]
        haircut_numeric = pd.to_numeric(filtered["haircut"], errors="coerce")
        filtered = filtered[haircut_numeric >= min_haircut]

    if filtered.empty:
        out = filtered.copy()
        out["score"] = pd.Series([], dtype=float)
        return out

    scored = filtered.copy()
    scored["score"] = compute_composite_score(scored, weights)
    return (
        scored.sort_values("score", ascending=False)
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
) -> pd.DataFrame:
    """Pipeline entry point: annualize std, score top N, merge biyi rows.

    The merged result is sorted by composite ``score`` descending. Biyi rows
    that didn't make top-N appear at the bottom with their own score, so the
    operator can still see them. Biyi rows that ARE in top-N keep their score
    spot.
    """
    df = add_annualized_std(funding_df)

    top = select_top(
        df,
        weights,
        top_n_final=top_n_final,
        min_haircut=min_haircut,
    )

    biyi_set = set(biyi_tickers)
    if biyi_set and "base" in df.columns and "quote" in df.columns:
        all_tickers = _ticker_series(df)
        biyi_rows = df[all_tickers.isin(biyi_set)].copy()
    else:
        biyi_rows = df.iloc[0:0].copy()

    # Score biyi rows too so the score column is populated
    if not biyi_rows.empty:
        biyi_rows["score"] = compute_composite_score(biyi_rows, weights)

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
        merged = merged.sort_values("score", ascending=False).reset_index(drop=True)
    return merged
