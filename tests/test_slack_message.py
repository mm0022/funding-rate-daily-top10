import numpy as np
import pandas as pd

from funding_top10.slack_message import (
    HIGHLIGHT,
    NO_HIGHLIGHT,
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
    "mean_3d_funding_rate",
    "mean_7d_funding_rate",
    "std_7d_funding_rate",
    "open_interest_value",
    "haircut",
]


def _make_rows_df(rows):
    return pd.DataFrame(rows, columns=_COLUMNS)


def test_biyi_rows_get_red_circle_prefix():
    df = _make_rows_df(
        [
            ("BINANCE-U", "ENAUSDT", "ENA", "USDT", 1747900800000, 0.0001, 0.00012, 0.00013, 0.000005, 1.2e6, 0.10),
            ("BINANCE-U", "ABCUSDT", "ABC", "USDT", 1747900800000, 0.0001, 0.00011, 0.00011, 0.000008, 9.1e5, 0.10),
        ]
    )
    msg = build_message(df, biyi_tickers=["ENA/USDT"], report_date_str="2026-05-12")
    code_lines = [line for line in msg.splitlines() if "USDT" in line and "```" not in line and "Biyi" not in line]
    ena = next(line for line in code_lines if "ENAUSDT" in line)
    abc = next(line for line in code_lines if "ABCUSDT" in line)
    assert ena.startswith(HIGHLIGHT)
    assert abc.startswith(NO_HIGHLIGHT)


def test_header_has_all_nine_columns():
    df = _make_rows_df([])
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    header = [line for line in msg.splitlines() if "exchange" in line and "symbol" in line]
    assert len(header) == 1
    h = header[0]
    for col in ["exchange", "symbol", "timestamp", "funding", "mean_3d", "mean_7d", "std_7d", "OI", "haircut"]:
        assert col in h, f"missing column header: {col}"


def test_empty_biyi_no_footer_line():
    df = _make_rows_df(
        [("BINANCE-U", "ETHUSDT", "ETH", "USDT", 1747900800000, 0.0, 0.0, 0.0, 0.0, 1e6, 0.1)]
    )
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    assert "Biyi tickers" not in msg


def test_biyi_footer_lists_tickers_sorted():
    df = _make_rows_df([])
    msg = build_message(df, biyi_tickers=["ZZZ/USDT", "AAA/USDT"], report_date_str="2026-05-12")
    footer = [line for line in msg.splitlines() if line.startswith("_Biyi tickers")]
    assert len(footer) == 1
    f = footer[0]
    assert f.index("AAA/USDT") < f.index("ZZZ/USDT")


def test_nan_values_render_as_na():
    df = _make_rows_df(
        [("BINANCE-U", "ETHUSDT", "ETH", "USDT", None, np.nan, np.nan, 0.01, np.nan, np.nan, np.nan)]
    )
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    line = [line for line in msg.splitlines() if "ETHUSDT" in line][0]
    assert line.count("n/a") >= 4


def test_timestamp_formatted_as_beijing_mm_dd_hh_mm():
    # 1747900800000 ms = 2025-05-22 08:00 UTC = 2025-05-22 16:00 Beijing
    df = _make_rows_df(
        [("BINANCE-U", "ETHUSDT", "ETH", "USDT", 1747900800000, 0.0001, 0.0001, 0.0001, 0.00001, 1e6, 0.1)]
    )
    msg = build_message(df, biyi_tickers=[], report_date_str="2026-05-12")
    line = [line for line in msg.splitlines() if "ETHUSDT" in line][0]
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
