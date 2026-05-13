"""Tests for the pure-Python helpers in datahub.

The DataHub class itself (which talks to nexus_data_hub_sdk) is not exercised
here — the SDK isn't on pypi and isn't installed in the test environment.
Integration testing happens on the Windows deployment box.
"""

from funding_top10.datahub import extract_haircut_value, normalize_key, strip_denomination_prefix


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
