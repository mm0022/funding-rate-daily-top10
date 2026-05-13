"""Tests for the pure-Python helpers in datahub.

The DataHub class itself (which talks to nexus_data_hub_sdk) is not exercised
here — the SDK isn't on pypi and isn't installed in the test environment.
Integration testing happens on the Windows deployment box.
"""

import pandas as pd

from funding_top10.datahub import (
    extract_haircut_value,
    normalize_key,
    parse_haircut_from_market_data_df,
    strip_denomination_prefix,
)


# ---- normalize_key ----


def test_normalize_key_adds_prefix_when_missing():
    assert normalize_key("BINANCE_MARGIN_BTC.HAIRCUT", "CYBERX_PROD") == "CYBERX_PROD-BINANCE_MARGIN_BTC.HAIRCUT"


def test_normalize_key_leaves_alone_when_prefix_already_present():
    full = "CYBERX_PROD-BINANCE_MARGIN_BTC.HAIRCUT"
    assert normalize_key(full, "CYBERX_PROD") == full


def test_normalize_key_no_prefix_returns_input():
    assert normalize_key("BINANCE_MARGIN_BTC.HAIRCUT", "") == "BINANCE_MARGIN_BTC.HAIRCUT"


# ---- extract_haircut_value ----


def test_extract_from_bare_number():
    assert extract_haircut_value(0.95) == 0.95
    assert extract_haircut_value(1) == 1.0


def test_extract_from_numeric_string():
    assert extract_haircut_value("0.95") == 0.95


def test_extract_from_dict_value_key():
    assert extract_haircut_value({"value": 0.85}) == 0.85


def test_extract_from_dict_haircut_key():
    assert extract_haircut_value({"haircut": "0.75"}) == 0.75


def test_extract_from_dict_collateralRate_key():
    assert extract_haircut_value({"collateralRate": 0.5}) == 0.5


def test_extract_from_list_of_dicts():
    payload = [{"foo": 1}, {"value": 0.6}]
    assert extract_haircut_value(payload) == 0.6


def test_extract_returns_none_for_unknown_shape():
    assert extract_haircut_value({"unknown_key": 0.5}) is None
    assert extract_haircut_value(None) is None
    assert extract_haircut_value("not a number") is None
    assert extract_haircut_value([{"unknown": 1}]) is None


def test_extract_from_versioned_records_picks_latest_sample_time():
    # Real shape returned by DataHub for BINANCE_MARGIN_ETHFI.HAIRCUT
    value = [
        {
            "sample_time": 1778652000000,
            "close_time": 1778655599999,
            "start_time": 1778652000000,
            "haircut": [{"left": 0, "right": 9999999999999, "value": 0.5}],
            "symbol": "ETHFI",
        },
        {
            "sample_time": 1778648400000,  # earlier
            "close_time": 1778651999999,
            "start_time": 1778648400000,
            "haircut": [{"left": 0, "right": 9999999999999, "value": 0.6}],
            "symbol": "ETHFI",
        },
    ]
    # The latest record (sample_time 1778652000000) has value 0.5
    assert extract_haircut_value(value) == 0.5


def test_extract_from_versioned_records_picks_latest_when_order_reversed():
    # Same as above but with earliest record first — exercise max() not first()
    value = [
        {
            "sample_time": 1000,
            "haircut": [{"left": 0, "right": 9999, "value": 0.3}],
            "symbol": "X",
        },
        {
            "sample_time": 2000,
            "haircut": [{"left": 0, "right": 9999, "value": 0.7}],
            "symbol": "X",
        },
    ]
    assert extract_haircut_value(value) == 0.7


def test_extract_from_single_record_with_tier_list():
    # Same record-style but not wrapped in a list
    value = {
        "sample_time": 1778652000000,
        "haircut": [{"left": 0, "right": 9999, "value": 0.42}],
        "symbol": "BTC",
    }
    assert extract_haircut_value(value) == 0.42


def test_extract_skips_record_with_empty_haircut_tiers():
    value = [
        {"sample_time": 1000, "haircut": []},
    ]
    assert extract_haircut_value(value) is None


# ---- strip_denomination_prefix ----


def test_strip_denom_handles_plain_token():
    assert strip_denomination_prefix("BTC") == "BTC"
    assert strip_denomination_prefix("ETH") == "ETH"


def test_strip_denom_handles_1000_prefix():
    assert strip_denomination_prefix("1000FLOKI") == "FLOKI"
    assert strip_denomination_prefix("1000PEPE") == "PEPE"


def test_strip_denom_handles_larger_denom_prefixes():
    assert strip_denomination_prefix("10000PEPE") == "PEPE"
    assert strip_denomination_prefix("100000XYZ") == "XYZ"
    assert strip_denomination_prefix("1000000MOG") == "MOG"


def test_strip_denom_keeps_1inch_style():
    # 1INCH is a real token name; the regex requires at least three trailing zeros
    assert strip_denomination_prefix("1INCH") == "1INCH"


def test_strip_denom_keeps_token_without_leading_digit():
    assert strip_denomination_prefix("A") == "A"
    assert strip_denomination_prefix("LINK") == "LINK"


# ---- parse_haircut_from_market_data_df (the new path used in production) ----


def test_parse_market_data_picks_latest_sample():
    # Real shape from alpha: data_hub.market_data_request("BINANCE_MARGIN_BTC.HAIRCUT", ...)
    df = pd.DataFrame(
        [
            {
                "start_time": 1775001600000,
                "close_time": 1775005199999,
                "sample_time": 1775001600000,
                "symbol": "BTC",
                "haircut": [{"left": 0, "right": 9999999999999, "value": 0.5}],
                "amt_in_usd": True,
            },
            {
                "start_time": 1775008800000,
                "close_time": 1775012399999,
                "sample_time": 1775008800000,   # later
                "symbol": "BTC",
                "haircut": [{"left": 0, "right": 9999999999999, "value": 0.7}],
                "amt_in_usd": True,
            },
        ]
    )
    assert parse_haircut_from_market_data_df(df) == 0.7


def test_parse_market_data_handles_empty_df():
    assert parse_haircut_from_market_data_df(pd.DataFrame()) is None


def test_parse_market_data_handles_none():
    assert parse_haircut_from_market_data_df(None) is None


def test_parse_market_data_handles_missing_value():
    df = pd.DataFrame([{"sample_time": 1, "haircut": [{"left": 0}]}])
    assert parse_haircut_from_market_data_df(df) is None


def test_parse_market_data_handles_empty_tier_list():
    df = pd.DataFrame([{"sample_time": 1, "haircut": []}])
    assert parse_haircut_from_market_data_df(df) is None
