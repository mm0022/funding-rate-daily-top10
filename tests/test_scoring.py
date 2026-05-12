import numpy as np
import pandas as pd
import pytest

from funding_top10.scoring import select_rows_to_show, select_top10


def _make_df(rows, columns=None):
    if columns is None:
        columns = ["base", "quote", "mean_7d_funding_rate", "std_7d_funding_rate", "haircut"]
    return pd.DataFrame(rows, columns=columns)


# ---------- select_top10 ----------


def test_two_step_filter_picks_high_mean_then_low_std():
    df = _make_df(
        [
            ("HIGH_MEAN_HIGH_STD", "USDT", 0.10, 0.05, 0.1),
            ("HIGH_MEAN_LOW_STD", "USDT", 0.09, 0.001, 0.1),
            ("LOW_MEAN_LOW_STD", "USDT", -0.05, 0.0001, 0.1),
            ("MED_MEAN_MED_STD", "USDT", 0.05, 0.01, 0.1),
        ]
    )
    out = select_top10(df, top_x_by_mean=3, top_n_final=2)
    assert list(out["base"]) == ["HIGH_MEAN_LOW_STD", "MED_MEAN_MED_STD"]


def test_low_mean_rows_are_excluded_by_first_cut():
    df = _make_df(
        [
            ("A", "USDT", 0.10, 0.001, 0.1),
            ("B", "USDT", 0.09, 0.001, 0.1),
            ("C_LOW_MEAN_BUT_STABLE", "USDT", -0.20, 0.00001, 0.1),
        ]
    )
    out = select_top10(df, top_x_by_mean=2, top_n_final=2)
    assert "C_LOW_MEAN_BUT_STABLE" not in out["base"].values


def test_nan_std_rows_are_dropped_in_second_cut():
    df = _make_df(
        [
            ("ONLY_ONE_SAMPLE", "USDT", 0.20, np.nan, 0.1),
            ("STABLE", "USDT", 0.10, 0.001, 0.1),
        ]
    )
    out = select_top10(df, top_x_by_mean=10, top_n_final=5)
    assert "ONLY_ONE_SAMPLE" not in out["base"].values
    assert "STABLE" in out["base"].values


def test_result_is_sorted_by_std_ascending():
    df = _make_df(
        [
            ("X", "USDT", 0.10, 0.02, 0.1),
            ("Y", "USDT", 0.10, 0.01, 0.1),
            ("Z", "USDT", 0.10, 0.03, 0.1),
        ]
    )
    out = select_top10(df, top_x_by_mean=10, top_n_final=10)
    assert list(out["base"]) == ["Y", "X", "Z"]


def test_missing_required_columns_raises():
    df = pd.DataFrame({"base": ["A"], "mean_7d_funding_rate": [0.1]})
    with pytest.raises(KeyError):
        select_top10(df)


def test_haircut_filter_drops_high_haircut():
    df = _make_df(
        [
            ("ATTRACTIVE_BUT_HIGH_HAIRCUT", "USDT", 0.10, 0.001, 0.8),
            ("ATTRACTIVE_LOW_HAIRCUT", "USDT", 0.09, 0.001, 0.1),
        ]
    )
    out = select_top10(df, top_x_by_mean=10, top_n_final=10, max_haircut=0.5)
    assert "ATTRACTIVE_BUT_HIGH_HAIRCUT" not in out["base"].values
    assert "ATTRACTIVE_LOW_HAIRCUT" in out["base"].values


def test_haircut_filter_keeps_nan_haircut():
    df = _make_df(
        [
            ("NAN_HAIRCUT", "USDT", 0.10, 0.001, np.nan),
            ("LOW_HAIRCUT", "USDT", 0.09, 0.001, 0.1),
        ]
    )
    out = select_top10(df, top_x_by_mean=10, top_n_final=10, max_haircut=0.5)
    assert "NAN_HAIRCUT" in out["base"].values


def test_haircut_filter_none_skips_filter():
    df = _make_df(
        [
            ("HIGH_HAIRCUT", "USDT", 0.10, 0.001, 0.9),
        ]
    )
    out = select_top10(df, top_x_by_mean=10, top_n_final=10, max_haircut=None)
    assert "HIGH_HAIRCUT" in out["base"].values


def test_haircut_filter_exact_threshold_kept():
    # > strictly greater; haircut == max_haircut is kept
    df = _make_df(
        [
            ("EQUAL_HAIRCUT", "USDT", 0.10, 0.001, 0.5),
        ]
    )
    out = select_top10(df, top_x_by_mean=10, top_n_final=10, max_haircut=0.5)
    assert "EQUAL_HAIRCUT" in out["base"].values


# ---------- select_rows_to_show ----------


def test_select_rows_to_show_merges_top10_and_biyi():
    df = _make_df(
        [
            ("TOP_A", "USDT", 0.10, 0.001, 0.1),
            ("TOP_B", "USDT", 0.09, 0.002, 0.1),
            ("BIYI_EXTRA", "USDT", -0.05, 0.001, 0.1),  # negative mean, not in top10
            ("UNRELATED", "USDT", -0.10, 0.001, 0.1),
        ]
    )
    out = select_rows_to_show(
        df,
        biyi_tickers=["BIYI_EXTRA/USDT"],
        top_x_by_mean=2,
        top_n_final=2,
    )
    bases = set(out["base"])
    assert "TOP_A" in bases
    assert "TOP_B" in bases
    assert "BIYI_EXTRA" in bases
    assert "UNRELATED" not in bases


def test_biyi_high_haircut_still_appears():
    df = _make_df(
        [
            ("TOP_A", "USDT", 0.10, 0.001, 0.1),
            ("BIYI_HIGH_HAIRCUT", "USDT", 0.05, 0.001, 0.9),
        ]
    )
    out = select_rows_to_show(
        df,
        biyi_tickers=["BIYI_HIGH_HAIRCUT/USDT"],
        top_x_by_mean=10,
        top_n_final=10,
        max_haircut=0.5,
    )
    bases = set(out["base"])
    assert "BIYI_HIGH_HAIRCUT" in bases
    assert "TOP_A" in bases


def test_merged_is_sorted_by_mean_desc():
    df = _make_df(
        [
            ("A", "USDT", 0.20, 0.001, 0.1),
            ("B", "USDT", 0.05, 0.001, 0.1),
            ("C", "USDT", 0.10, 0.001, 0.1),
        ]
    )
    out = select_rows_to_show(df, biyi_tickers=[])
    assert list(out["base"]) == ["A", "C", "B"]


def test_no_dupes_when_biyi_already_in_top10():
    df = _make_df(
        [
            ("X", "USDT", 0.10, 0.001, 0.1),
            ("Y", "USDT", 0.09, 0.001, 0.1),
        ]
    )
    out = select_rows_to_show(
        df,
        biyi_tickers=["X/USDT"],
        top_x_by_mean=10,
        top_n_final=10,
    )
    bases = list(out["base"])
    assert bases.count("X") == 1


def test_empty_biyi_returns_just_top10():
    df = _make_df(
        [
            ("A", "USDT", 0.10, 0.001, 0.1),
            ("B", "USDT", 0.05, 0.001, 0.1),
        ]
    )
    out = select_rows_to_show(df, biyi_tickers=[], top_x_by_mean=10, top_n_final=10)
    assert set(out["base"]) == {"A", "B"}
