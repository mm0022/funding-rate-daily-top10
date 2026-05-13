"""Binance API client for the funding-top10 report.

Endpoints used (all public, no auth — all USDT-quoted BINANCE-U perps):
  - GET  /fapi/v1/premiumIndex          : latest funding + mark price for ALL perps
  - GET  /fapi/v1/fundingRate?symbol=X  : 7-day funding history for sum/std
  - GET  /fapi/v1/openInterest?symbol=X : current OI in base units

Haircut data is NOT fetched here — it now comes from DataHub (see datahub.py).

Returns a DataFrame matching the historical SQL schema so downstream scoring /
slack_message code stays identical:
  exchange, symbol, base, quote, timestamp, funding_rate,
  sum_3d_funding_rate, sum_7d_funding_rate, std_7d_funding_rate,
  open_interest_value, haircut (always NaN here; populated later from DataHub)
"""

from __future__ import annotations

import asyncio
import logging
import math
import statistics
import time
from typing import Any

import httpx
import pandas as pd

logger = logging.getLogger(__name__)


FAPI_BASE = "https://fapi.binance.com"

MAX_CONCURRENCY = 2  # was 30 -> 5 -> 2; Binance fapi 403s aggressively under burst load
HTTP_TIMEOUT = 30.0


async def _get_json(client: httpx.AsyncClient, url: str, *, params: dict | None = None,
                    headers: dict | None = None) -> Any:
    resp = await client.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


async def fetch_premium_index_all(client: httpx.AsyncClient) -> list[dict]:
    """All perps' latest funding rate, mark price, and snapshot time. Single request."""
    return await _get_json(client, f"{FAPI_BASE}/fapi/v1/premiumIndex")


async def fetch_active_usdt_perps(client: httpx.AsyncClient) -> set[str]:
    """Return the set of BINANCE-U symbols that are TRADING + PERPETUAL + USDT-quoted.

    Symbols in any other state (PRE_TRADING / BREAK / SETTLING / delisted) tend
    to return 4xx on /fundingRate or /openInterest — better to skip them upfront.
    """
    info = await _get_json(client, f"{FAPI_BASE}/fapi/v1/exchangeInfo")
    return {
        str(s.get("symbol"))
        for s in info.get("symbols", [])
        if s.get("status") == "TRADING"
        and s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
    }


DEFAULT_FUNDING_INTERVAL_HOURS = 8


async def fetch_funding_intervals(client: httpx.AsyncClient) -> dict[str, int]:
    """Return {symbol: hours} for perps whose funding interval ISN'T the default 8h.

    Binance only lists symbols with non-default intervals in /fapi/v1/fundingInfo.
    Symbols absent from the response use the default 8h. Caller should treat
    a missing key as DEFAULT_FUNDING_INTERVAL_HOURS.
    """
    rows = await _get_json(client, f"{FAPI_BASE}/fapi/v1/fundingInfo")
    out: dict[str, int] = {}
    for r in rows or []:
        try:
            out[str(r["symbol"])] = int(r["fundingIntervalHours"])
        except (TypeError, ValueError, KeyError):
            continue
    return out


async def fetch_funding_history(client: httpx.AsyncClient, symbol: str,
                                *, days: int = 7) -> list[dict]:
    """Past `days` of funding events for one symbol. limit=1000 covers any cadence."""
    start_ms = int((time.time() - days * 86400) * 1000)
    return await _get_json(
        client,
        f"{FAPI_BASE}/fapi/v1/fundingRate",
        params={"symbol": symbol, "startTime": start_ms, "limit": 1000},
    )


async def fetch_open_interest(client: httpx.AsyncClient, symbol: str) -> dict:
    """Current open interest (in base coin units) for one symbol."""
    return await _get_json(
        client,
        f"{FAPI_BASE}/fapi/v1/openInterest",
        params={"symbol": symbol},
    )


def _is_usdt_perp(symbol: str) -> bool:
    """USDT-quoted perp like BTCUSDT, ENAUSDT, 1000FLOKIUSDT. Excludes USDC/BUSD/etc."""
    return symbol.endswith("USDT")


def _base_from_symbol(symbol: str) -> str:
    """Strip the 'USDT' suffix to get the base. Caller must ensure symbol is USDT-quoted."""
    return symbol[:-4]


def _aggregate(history: list[dict], *, now_ms: int, days: int) -> tuple[float, list[float]]:
    """Sum of fundingRate within (now - days, now], and the list of rates used."""
    cutoff_ms = now_ms - days * 86400 * 1000
    rates: list[float] = []
    for h in history:
        try:
            ft = int(h.get("fundingTime"))
            rate = float(h.get("fundingRate"))
        except (TypeError, ValueError):
            continue
        if ft >= cutoff_ms:
            rates.append(rate)
    return (sum(rates), rates)


async def _fetch_all_async(proxy: str = "") -> pd.DataFrame:
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def bounded(coro):
        async with sem:
            return await coro

    # trust_env=False: ignore ambient HTTP_PROXY env vars; proxy comes only from
    # the explicit `proxy` arg (sourced from config.yaml). Avoids surprises from
    # whatever the shell happens to have set.
    client_kwargs: dict = {"timeout": HTTP_TIMEOUT, "trust_env": False}
    if proxy:
        client_kwargs["proxy"] = proxy

    async with httpx.AsyncClient(**client_kwargs) as client:
        # Phase 0: get the set of actively-trading USDT-perp symbols so we don't
        # waste calls on delisted / pre-trading / settling entries.
        active_symbols = await fetch_active_usdt_perps(client)
        logger.info("BINANCE-U active USDT perps: %d", len(active_symbols))

        # Phase 0b: per-symbol funding cadence overrides (default 8h applies
        # to anything absent from the response).
        interval_overrides = await fetch_funding_intervals(client)
        logger.info("BINANCE-U non-default funding intervals: %d", len(interval_overrides))

        premium = await fetch_premium_index_all(client)
        usdt_rows = [
            p for p in premium
            if str(p.get("symbol", "")) in active_symbols
        ]
        symbols = [p["symbol"] for p in usdt_rows]

        # Phase 1: per-symbol funding history.
        history_coros = [bounded(fetch_funding_history(client, s, days=7)) for s in symbols]
        history_results = await asyncio.gather(*history_coros, return_exceptions=True)

        history_by_symbol: dict[str, list[dict]] = {}
        for sym, hist in zip(symbols, history_results):
            if isinstance(hist, Exception):
                logger.warning("funding history fetch for %s failed: %r", sym, hist)
                continue
            if not hist:
                logger.info("funding history for %s returned empty list (likely recently-listed)", sym)
                continue
            history_by_symbol[sym] = hist

        # Phase 2: OI only for symbols whose funding history was non-empty —
        # symbols that returned empty history (recently-listed / delisted) tend
        # to also 400 on /openInterest, so skipping them removes noise.
        oi_symbols = list(history_by_symbol.keys())
        oi_coros = [bounded(fetch_open_interest(client, s)) for s in oi_symbols]
        oi_results = await asyncio.gather(*oi_coros, return_exceptions=True)

        oi_by_symbol: dict[str, dict] = {}
        for sym, oi in zip(oi_symbols, oi_results):
            if isinstance(oi, Exception):
                logger.warning("openInterest fetch for %s failed: %r", sym, oi)
                continue
            if isinstance(oi, dict):
                oi_by_symbol[sym] = oi

    now_ms = int(time.time() * 1000)
    rows: list[dict] = []
    for p in usdt_rows:
        symbol = str(p["symbol"])
        base = _base_from_symbol(symbol)
        history = history_by_symbol.get(symbol, [])
        oi = oi_by_symbol.get(symbol, {})

        try:
            mark_price = float(p.get("markPrice"))
        except (TypeError, ValueError):
            mark_price = float("nan")

        try:
            last_funding = float(p.get("lastFundingRate"))
        except (TypeError, ValueError):
            last_funding = float("nan")

        sum_3d, _ = _aggregate(history, now_ms=now_ms, days=3)
        sum_7d, rates_7d = _aggregate(history, now_ms=now_ms, days=7)
        std_7d = statistics.stdev(rates_7d) if len(rates_7d) > 1 else float("nan")

        if history:
            try:
                ts = int(history[-1].get("fundingTime"))
            except (TypeError, ValueError):
                ts = int(p.get("time", 0))
        else:
            try:
                ts = int(p.get("time", 0))
            except (TypeError, ValueError):
                ts = 0

        try:
            oi_base = float(oi.get("openInterest")) if isinstance(oi, dict) else float("nan")
        except (TypeError, ValueError):
            oi_base = float("nan")

        if math.isnan(oi_base) or math.isnan(mark_price):
            oi_value = float("nan")
        else:
            oi_value = oi_base * mark_price

        rows.append({
            "exchange": "BINANCE-U",
            "symbol": symbol,
            "base": base,
            "quote": "USDT",
            "timestamp": ts,
            "funding_rate": last_funding,
            "sum_3d_funding_rate": sum_3d if rates_7d or sum_3d else float("nan"),
            "sum_7d_funding_rate": sum_7d if rates_7d else float("nan"),
            "std_7d_funding_rate": std_7d,
            "funding_interval_hours": interval_overrides.get(symbol, DEFAULT_FUNDING_INTERVAL_HOURS),
            "open_interest_value": oi_value,
            "haircut": float("nan"),  # populated later from DataHub in main.py
        })

    return pd.DataFrame(rows)


def fetch_funding_dataframe(proxy: str = "") -> pd.DataFrame:
    """Sync entrypoint: fetch funding/OI from Binance and return the DataFrame.

    proxy: if non-empty, all HTTP calls route through this URL (e.g.
    "http://proxy.company.com:8080"). If empty, calls go direct.

    The 'haircut' column on the returned DataFrame is always NaN; populate it
    from DataHub in the caller (see funding_top10/datahub.load_binance_haircuts).
    """
    return asyncio.run(_fetch_all_async(proxy))
