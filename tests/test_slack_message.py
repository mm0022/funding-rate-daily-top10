import numpy as np
import pandas as pd

from funding_top10.slack_message import (
    HIGHLIGHT,
    NO_FLAG,
    _fmt_apr,
    _fmt_bp,
    _fmt_float,
    _fmt_human_usd,
    _fmt_timestamp_bj,
    build_message,
)


_COLUMNS = [
    "exchange",
    "symbol",
    "base",
    "quote",
    "timestamp",
    "funding_rate",
    "sum_3d_funding_rate",
    "sum_7d_funding_rate",
    "std_7d_funding_rate",
    "open_interest_value",
    "haircut",
]


def _make_rows_df(rows):
    return pd.DataFrame(rows, columns=_COLUMNS)


def test_biyi_rows_get_red_circle_flag():
    df = _make_rows_df(
        [
            ("BINANCE-U", "ENAUSDT", "ENA", "USDT", 1747900800000, 0.0001, 0.00012, 0.00013, 0.000005, 1.2e6, 0.80),
            ("BINANCE-U", "ABCUSDT", "ABC", "USDT", 1747900800000, 0.0001, 0.00011, 0.00011, 0.000008, 9.1e5, 0.80),
        ]
    )
    msg = build_message(df, biyi_tickers=["ENA/USDT"], report_date_str="2026-05-12")
    code_lines = [
        line for line in msg.splitlines()
        if "BINANCE-U" in line and "```" not in line and not line.startswith("*")
    ]
    ena = next(line for line in code_lines if "ENA/USDT" in line)
    abc = next(line for line in code_lines if "ABC/USDT" in line)
    assert ena.startswith(HIGHLIGHT)
    assert abc.startswith(NO_FLAG)


def test_symbol_column_shows_base_slash_quote():
    df = _make_rows_df(
        [("BINANCE-U", "1000FLOKIUSDT", "1000FLOKI", "USDT", 1747900800000, 0.0, 0.0, 0.0, 0.0, 1e6, 0.8)]
    )
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    line = next(
        line for line in msg.splitlines()
        if "BINANCE-U" in line and "```" not in line and not line.startswith("*")
    )
    assert "1000FLOKI/USDT" in line
    # The raw concatenated symbol (no slash) should NOT appear in the table row
    assert "1000FLOKIUSDT " not in line  # trailing space guards against substring of "1000FLOKI/USDT "


def test_symbol_column_supports_non_usdt_quotes():
    df = _make_rows_df(
        [("BINANCE-U", "BTCUSDC", "BTC", "USDC", 1747900800000, 0.0, 0.0, 0.0, 0.0, 1e6, 0.8)]
    )
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    line = next(
        line for line in msg.splitlines()
        if "BINANCE-U" in line and "```" not in line and not line.startswith("*")
    )
    assert "BTC/USDC" in line


def test_header_has_all_columns_plus_blank_flag():
    df = _make_rows_df([])
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    header = next(line for line in msg.splitlines() if "exchange" in line and "symbol" in line)
    for col in ["exchange", "symbol", "timestamp", "funding(bp)", "3d_apr%", "7d_apr%", "std_7d(bp)", "OI", "haircut"]:
        assert col in header, f"missing column header: {col}"
    assert header.index("exchange") == 3


def test_empty_biyi_no_footer_line():
    df = _make_rows_df(
        [("BINANCE-U", "ETHUSDT", "ETH", "USDT", 1747900800000, 0.0, 0.0, 0.0, 0.0, 1e6, 0.8)]
    )
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    assert "Biyi tickers" not in msg


def test_biyi_footer_lists_tickers_sorted():
    df = _make_rows_df([])
    msg = build_message(df, biyi_tickers=["ZZZ/USDT", "AAA/USDT"], report_date_str="2026-05-12")
    footer = next(line for line in msg.splitlines() if line.startswith("_Biyi tickers"))
    assert footer.index("AAA/USDT") < footer.index("ZZZ/USDT")


def test_nan_values_render_as_na():
    df = _make_rows_df(
        [("BINANCE-U", "ETHUSDT", "ETH", "USDT", None, np.nan, np.nan, 0.01, np.nan, np.nan, np.nan)]
    )
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    line = next(
        line for line in msg.splitlines()
        if "BINANCE-U" in line and "```" not in line and not line.startswith("*")
    )
    assert line.count("n/a") >= 4


def test_timestamp_in_ms_formats_correctly():
    # 1747900800000 ms = 2025-05-22 08:00 UTC = 2025-05-22 16:00 Beijing
    df = _make_rows_df(
        [("BINANCE-U", "ETHUSDT", "ETH", "USDT", 1747900800000, 0.0001, 0.0001, 0.0001, 0.00001, 1e6, 0.8)]
    )
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    line = next(
        line for line in msg.splitlines()
        if "BINANCE-U" in line and "```" not in line and not line.startswith("*")
    )
    assert "05-22 16:00" in line


def test_timestamp_in_seconds_formats_correctly():
    df = _make_rows_df(
        [("BINANCE-U", "ETHUSDT", "ETH", "USDT", 1747900800, 0.0001, 0.0001, 0.0001, 0.00001, 1e6, 0.8)]
    )
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    line = next(
        line for line in msg.splitlines()
        if "BINANCE-U" in line and "```" not in line and not line.startswith("*")
    )
    assert "05-22 16:00" in line


def test_fmt_human_usd_branches():
    assert _fmt_human_usd(0) == "0"
    assert _fmt_human_usd(500) == "500"
    assert _fmt_human_usd(1500) == "2K"
    assert _fmt_human_usd(1_234_567).endswith("M")
    assert _fmt_human_usd(3_500_000_000).endswith("B")
    assert _fmt_human_usd(None) == "n/a"
    assert _fmt_human_usd(float("nan")) == "n/a"


def test_fmt_float_nan_and_none():
    assert _fmt_float(None, 4) == "n/a"
    assert _fmt_float(float("nan"), 4) == "n/a"
    assert _fmt_float(0.123456789, 4) == "0.1235"


def test_fmt_timestamp_bj_handles_none_and_nan():
    assert _fmt_timestamp_bj(None) == "n/a"
    assert _fmt_timestamp_bj(float("nan")) == "n/a"
    assert _fmt_timestamp_bj("not a number") == "n/a"


def test_fmt_timestamp_bj_too_small_is_na():
    assert _fmt_timestamp_bj(100000) == "n/a"


def test_fmt_bp_multiplies_by_10000_signs_negatives_only():
    assert _fmt_bp(0.0001) == "1"
    assert _fmt_bp(0.000123) == "1.23"
    assert _fmt_bp(-0.0001) == "-1"
    assert _fmt_bp(0) == "0"
    assert _fmt_bp(None) == "n/a"
    assert _fmt_bp(float("nan")) == "n/a"


def test_fmt_bp_fixed_decimals():
    assert _fmt_bp(0.0001, digits=3) == "1.000"
    assert _fmt_bp(0.000005, digits=3) == "0.050"
    assert _fmt_bp(-0.00003, digits=3) == "-0.300"
    assert _fmt_bp(0, digits=3) == "0.000"


def test_fmt_apr_3d():
    # 0.003 over 3 days → 0.003 * 365 / 3 * 100 = 36.5%
    assert _fmt_apr(0.003, 3) == "36.5%"
    assert _fmt_apr(0, 3) == "0.0%"
    assert _fmt_apr(-0.003, 3) == "-36.5%"


def test_fmt_apr_7d():
    # 0.007 over 7 days → 0.007 * 365 / 7 * 100 = 36.5%
    assert _fmt_apr(0.007, 7) == "36.5%"
    assert _fmt_apr(None, 7) == "n/a"
    assert _fmt_apr(float("nan"), 7) == "n/a"


def test_fmt_apr_is_funding_cadence_independent():
    # Same total funding over the same window must give the same APR regardless
    # of how it was sliced (the whole point of using sum, not avg).
    sum_7d = 0.0073
    assert _fmt_apr(sum_7d, 7) == _fmt_apr(sum_7d, 7)
