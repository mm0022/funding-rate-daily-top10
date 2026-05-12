"""Scoring / ranking for funding-rate symbols.

Two-step procedure for picking the daily top 10 (Q6=b):
  1. Drop rows whose haircut is null/"None" or strictly less than MIN_HAIRCUT
     (in this codebase a *higher* haircut means a more attractive candidate —
     it is a margin / weight allowance, not a risk penalty).
  2. Sort the remainder by sum_7d_funding_rate DESC, take the top TOP_X_BY_MEAN.
  3. From that subset, drop NaN-std rows, sort by std_7d_funding_rate ASC,
     take the top TOP_N_FINAL.

`select_rows_to_show` then merges that top10 with the biyi-strategy tickers
(no haircut filter on biyi rows) and sorts the union by mean_7d desc, which
makes the position of each biyi ticker on the funding leaderboard visible at
a glance.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd


TOP_X_BY_MEAN = 50
TOP_N_FINAL = 10
MIN_HAIRCUT = 0.5


def select_top10(
    df: pd.DataFrame,
    *,
    top_x_by_mean: int = TOP_X_BY_MEAN,
    top_n_final: int = TOP_N_FINAL,
    min_haircut: float | None = MIN_HAIRCUT,
) -> pd.DataFrame:
    """Apply the two-step funding-stability ranking.

    Args:
        df: DataFrame with at least sum_7d_funding_rate, std_7d_funding_rate,
            and (if min_haircut is not None) haircut.
        top_x_by_mean: how many to keep after the mean-desc cut.
        top_n_final:   how many to keep after the std-asc cut.
        min_haircut:   drop rows whose haircut is missing (NaN or the literal
                       string "None") or strictly less than this threshold.
                       Pass None to skip the filter entirely.

    Returns:
        A new DataFrame of at most ``top_n_final`` rows, ordered by
        ``std_7d_funding_rate`` ascending.
    """
    required = {"sum_7d_funding_rate", "std_7d_funding_rate"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"select_top10: missing required columns {missing}")

    filtered = df
    if min_haircut is not None and "haircut" in df.columns:
        # Some DB drivers return null haircuts as the literal string "None";
        # drop those first, then numerically compare. to_numeric coerces any
        # still-non-numeric value to NaN, which fails the >= test and is dropped.
        filtered = filtered[filtered["haircut"].astype(str) != "None"]
        haircut_numeric = pd.to_numeric(filtered["haircut"], errors="coerce")
        filtered = filtered[haircut_numeric >= min_haircut]

    by_mean = filtered.sort_values("sum_7d_funding_rate", ascending=False).head(top_x_by_mean)
    by_std = by_mean.dropna(subset=["std_7d_funding_rate"]).sort_values(
        "std_7d_funding_rate", ascending=True
    )
    return by_std.head(top_n_final).reset_index(drop=True)


def _ticker_series(df: pd.DataFrame) -> pd.Series:
    """Build a 'BASE/QUOTE' Series (str) aligned with df.index."""
    return df["base"].astype(str) + "/" + df["quote"].astype(str)


def select_rows_to_show(
    funding_df: pd.DataFrame,
    biyi_tickers: Iterable[str],
    *,
    top_x_by_mean: int = TOP_X_BY_MEAN,
    top_n_final: int = TOP_N_FINAL,
    min_haircut: float | None = MIN_HAIRCUT,
) -> pd.DataFrame:
    """Merge top10 with biyi rows from funding_df.

    Top10 is selected with the haircut filter applied (haircut must be >=
    min_haircut). Biyi rows are pulled directly from funding_df with NO
    haircut filter, so a biyi ticker with low/missing haircut still appears.

    The merged result is deduped by 'BASE/QUOTE' ticker and sorted by
    ``sum_7d_funding_rate`` descending — this layout lets the reader see where
    each biyi ticker falls on the funding leaderboard.
    """
    top10 = select_top10(
        funding_df,
        top_x_by_mean=top_x_by_mean,
        top_n_final=top_n_final,
        min_haircut=min_haircut,
    )
    biyi_set = set(biyi_tickers)

    if biyi_set and "base" in funding_df.columns and "quote" in funding_df.columns:
        all_tickers = _ticker_series(funding_df)
        biyi_rows = funding_df[all_tickers.isin(biyi_set)]
    else:
        biyi_rows = funding_df.iloc[0:0]  # empty, same schema

    if len(top10):
        top10_keys = set(_ticker_series(top10).tolist())
    else:
        top10_keys = set()

    if len(biyi_rows):
        biyi_keys = _ticker_series(biyi_rows)
        biyi_extra = biyi_rows[~biyi_keys.isin(top10_keys)]
    else:
        biyi_extra = biyi_rows

    merged = pd.concat([top10, biyi_extra], ignore_index=True)
    if len(merged):
        merged = merged.sort_values(
            "sum_7d_funding_rate", ascending=False
        ).reset_index(drop=True)
    return merged
