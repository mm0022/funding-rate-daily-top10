import math

import numpy as np
import pandas as pd
import pytest

from funding_top10.scoring import (
    ScoreWeights,
    add_annualized_std,
    annualize_std,
    compute_composite_score,
    select_rows_to_show,
    select_top,
)


# ---------- annualize_std ----------


def test_annualize_std_8h():
    # 8h interval -> N_per_year = 3 * 365 = 1095, sqrt ≈ 33.09
    got = annualize_std(0.0001, 8.0)
    assert got == pytest.approx(0.0001 * math.sqrt(1095), rel=1e-9)


def test_annualize_std_1h():
    got = annualize_std(0.0001, 1.0)
    assert got == pytest.approx(0.0001 * math.sqrt(24 * 365), rel=1e-9)


def test_annualize_std_nan_inputs():
    assert math.isnan(annualize_std(None, 8))
    assert math.isnan(annualize_std(0.0001, None))
    assert math.isnan(annualize_std(float("nan"), 8))
    assert math.isnan(annualize_std(0.0001, 0))


def test_add_annualized_std_column():
    df = pd.DataFrame(
        [
            {"std_7d_funding_rate": 0.0001, "funding_interval_hours": 8},
            {"std_7d_funding_rate": 0.0002, "funding_interval_hours": 4},
        ]
    )
    out = add_annualized_std(df)
    assert "std_7d_annualized" in out.columns
    assert out.iloc[0]["std_7d_annualized"] == pytest.approx(0.0001 * math.sqrt(1095))
    assert out.iloc[1]["std_7d_annualized"] == pytest.approx(0.0002 * math.sqrt(2190))


# ---------- compute_composite_score ----------


def _base_df():
    return pd.DataFrame(
        [
            {"base": "A", "quote": "USDT", "sum_7d_funding_rate": 0.02, "std_7d_annualized": 0.10, "haircut": 0.9, "open_interest_value": 1e9},
            {"base": "B", "quote": "USDT", "sum_7d_funding_rate": 0.01, "std_7d_annualized": 0.30, "haircut": 0.5, "open_interest_value": 1e7},
            {"base": "C", "quote": "USDT", "sum_7d_funding_rate": 0.005, "std_7d_annualized": 0.40, "haircut": 0.6, "open_interest_value": 1e6},
        ]
    )


def test_composite_score_higher_apr_higher_score():
    df = _base_df()
    w = ScoreWeights(apr7=1.0, std=0.0, haircut=0.0, oi=0.0)
    scores = compute_composite_score(df, w)
    # A has highest apr → highest score
    assert scores.iloc[0] > scores.iloc[1] > scores.iloc[2]


def test_composite_score_lower_std_higher_score():
    df = _base_df()
    w = ScoreWeights(apr7=0.0, std=1.0, haircut=0.0, oi=0.0)
    scores = compute_composite_score(df, w)
    # A has lowest std → highest score
    assert scores.iloc[0] > scores.iloc[1] > scores.iloc[2]


def test_composite_score_higher_haircut_higher_score():
    df = _base_df()
    w = ScoreWeights(apr7=0.0, std=0.0, haircut=1.0, oi=0.0)
    scores = compute_composite_score(df, w)
    # A has highest haircut
    assert scores.iloc[0] > scores.iloc[2] > scores.iloc[1]


def test_composite_score_empty_df():
    df = pd.DataFrame(
        columns=["sum_7d_funding_rate", "std_7d_annualized", "haircut", "open_interest_value"]
    )
    scores = compute_composite_score(df, ScoreWeights())
    assert len(scores) == 0


# ---------- select_top ----------


def test_select_top_filters_haircut_and_sorts_by_score():
    df = pd.DataFrame(
        [
            {"base": "GOOD", "quote": "USDT", "sum_7d_funding_rate": 0.02, "std_7d_annualized": 0.10, "haircut": 0.8, "open_interest_value": 1e9},
            {"base": "BAD_HAIRCUT", "quote": "USDT", "sum_7d_funding_rate": 0.03, "std_7d_annualized": 0.05, "haircut": 0.3, "open_interest_value": 1e9},
            {"base": "MID", "quote": "USDT", "sum_7d_funding_rate": 0.01, "std_7d_annualized": 0.20, "haircut": 0.7, "open_interest_value": 1e8},
        ]
    )
    out = select_top(df, ScoreWeights(), top_n_final=10, min_haircut=0.5)
    assert "BAD_HAIRCUT" not in out["base"].values
    # GOOD and MID survive; score column populated
    assert "score" in out.columns
    assert len(out) == 2


def test_select_top_caps_to_top_n():
    rng = np.random.default_rng(0)
    n = 50
    df = pd.DataFrame(
        {
            "base": [f"S{i}" for i in range(n)],
            "quote": ["USDT"] * n,
            "sum_7d_funding_rate": rng.normal(0.01, 0.005, n),
            "std_7d_annualized": rng.uniform(0.01, 0.3, n),
            "haircut": [0.8] * n,
            "open_interest_value": rng.uniform(1e6, 1e9, n),
        }
    )
    out = select_top(df, ScoreWeights(), top_n_final=10, min_haircut=0.5)
    assert len(out) == 10


def test_select_top_missing_required_columns_raises():
    df = pd.DataFrame({"base": ["A"], "haircut": [0.8]})
    with pytest.raises(KeyError):
        select_top(df, ScoreWeights())


# ---------- select_rows_to_show ----------


def test_select_rows_to_show_includes_biyi_with_low_haircut():
    df = pd.DataFrame(
        [
            {"base": "TOP", "quote": "USDT", "sum_7d_funding_rate": 0.02, "std_7d_funding_rate": 0.0001, "funding_interval_hours": 8, "haircut": 0.8, "open_interest_value": 1e9},
            {"base": "BIYI_LOW_HAIRCUT", "quote": "USDT", "sum_7d_funding_rate": 0.005, "std_7d_funding_rate": 0.0002, "funding_interval_hours": 4, "haircut": 0.1, "open_interest_value": 1e7},
        ]
    )
    out = select_rows_to_show(df, biyi_tickers=["BIYI_LOW_HAIRCUT/USDT"], weights=ScoreWeights())
    bases = set(out["base"])
    assert "TOP" in bases
    assert "BIYI_LOW_HAIRCUT" in bases  # biyi bypasses haircut filter


def test_select_rows_to_show_sorts_by_score_desc():
    df = pd.DataFrame(
        [
            {"base": "LOW_APR", "quote": "USDT", "sum_7d_funding_rate": 0.001, "std_7d_funding_rate": 0.0001, "funding_interval_hours": 8, "haircut": 0.8, "open_interest_value": 1e9},
            {"base": "HIGH_APR", "quote": "USDT", "sum_7d_funding_rate": 0.05, "std_7d_funding_rate": 0.0001, "funding_interval_hours": 8, "haircut": 0.8, "open_interest_value": 1e9},
        ]
    )
    out = select_rows_to_show(df, biyi_tickers=[], weights=ScoreWeights())
    # Higher apr should score higher with default weights → appear first
    assert list(out["base"]) == ["HIGH_APR", "LOW_APR"]


def test_select_rows_to_show_empty_biyi_returns_top_only():
    df = pd.DataFrame(
        [
            {"base": "A", "quote": "USDT", "sum_7d_funding_rate": 0.02, "std_7d_funding_rate": 0.0001, "funding_interval_hours": 8, "haircut": 0.8, "open_interest_value": 1e9},
            {"base": "B", "quote": "USDT", "sum_7d_funding_rate": 0.01, "std_7d_funding_rate": 0.0001, "funding_interval_hours": 8, "haircut": 0.7, "open_interest_value": 1e8},
        ]
    )
    out = select_rows_to_show(df, biyi_tickers=[], weights=ScoreWeights(), top_n_final=10)
    assert set(out["base"]) == {"A", "B"}
