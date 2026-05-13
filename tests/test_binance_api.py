"""Unit tests for the pure-Python helpers in binance_api.

The HTTP layer is left to manual / smoke testing against the live Binance API.
"""

import hashlib
import hmac

import pytest

from funding_top10.binance_api import (
    _aggregate,
    _base_from_symbol,
    _is_usdt_perp,
    _sign,
)


def test_is_usdt_perp_accepts_usdt_quote():
    assert _is_usdt_perp("BTCUSDT")
    assert _is_usdt_perp("ENAUSDT")
    assert _is_usdt_perp("1000FLOKIUSDT")


def test_is_usdt_perp_rejects_other_quotes():
    assert not _is_usdt_perp("BTCUSDC")
    assert not _is_usdt_perp("BTCBUSD")
    assert not _is_usdt_perp("ETHBTC")


def test_base_from_symbol_strips_usdt():
    assert _base_from_symbol("BTCUSDT") == "BTC"
    assert _base_from_symbol("ENAUSDT") == "ENA"
    assert _base_from_symbol("1000FLOKIUSDT") == "1000FLOKI"


def test_aggregate_sums_only_within_window():
    now_ms = 1_000_000_000_000
    one_day_ms = 86400 * 1000
    history = [
        {"fundingTime": str(now_ms - 1 * one_day_ms), "fundingRate": "0.001"},   # 1 day ago
        {"fundingTime": str(now_ms - 4 * one_day_ms), "fundingRate": "0.002"},   # 4 days ago
        {"fundingTime": str(now_ms - 8 * one_day_ms), "fundingRate": "0.003"},   # 8 days ago
    ]

    sum_3d, rates_3d = _aggregate(history, now_ms=now_ms, days=3)
    assert sum_3d == pytest.approx(0.001)
    assert rates_3d == [0.001]

    sum_7d, rates_7d = _aggregate(history, now_ms=now_ms, days=7)
    assert sum_7d == pytest.approx(0.003)
    assert sorted(rates_7d) == [0.001, 0.002]


def test_aggregate_skips_malformed_rows():
    now_ms = 1_000_000_000_000
    history = [
        {"fundingTime": "not_a_number", "fundingRate": "0.001"},
        {"fundingTime": str(now_ms), "fundingRate": "not_a_number"},
        {"fundingTime": str(now_ms - 1000), "fundingRate": "0.5"},
    ]
    sum_, rates = _aggregate(history, now_ms=now_ms, days=7)
    assert sum_ == pytest.approx(0.5)
    assert rates == [0.5]


def test_aggregate_empty_history():
    sum_, rates = _aggregate([], now_ms=1_000_000_000_000, days=7)
    assert sum_ == 0
    assert rates == []


def test_sign_matches_hmac_sha256_reference():
    expected = hmac.new(b"my_secret", b"timestamp=1234", hashlib.sha256).hexdigest()
    assert _sign("timestamp=1234", "my_secret") == expected
