"""Binance API client for the funding-top10 report.

Endpoints used (all USDT-quoted BINANCE-U perps):
  - GET  /fapi/v1/premiumIndex            (public): latest funding + mark price for ALL perps
  - GET  /fapi/v1/fundingRate?symbol=X    (public): 7-day funding history for sum/std
  - GET  /fapi/v1/openInterest?symbol=X   (public): current OI in base units
  - GET  /sapi/v1/portfolio/collateralRate (signed): per-asset collateral rate ("haircut")

Returns a DataFrame matching the historical SQL schema so downstream scoring /
slack_message code stays identical:
  exchange, symbol, base, quote, timestamp, funding_rate,
  sum_3d_funding_rate, sum_7d_funding_rate, std_7d_funding_rate,
  open_interest_value, haircut
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import math
import statistics
import time
from typing import Any
from urllib.parse import urlencode

import httpx
import pandas as pd

logger = logging.getLogger(__name__)


FAPI_BASE = "https://fapi.binance.com"
SAPI_BASE = "https://api.binance.com"

MAX_CONCURRENCY = 30
HTTP_TIMEOUT = 30.0


async def _get_json(client: httpx.AsyncClient, url: str, *, params: dict | None = None,
                    headers: dict | None = None) -> Any:
    resp = await client.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()


async def fetch_premium_index_all(client: httpx.AsyncClient) -> list[dict]:
    """All perps' latest funding rate, mark price, and snapshot time. Single request."""
    return await _get_json(client, f"{FAPI_BASE}/fapi/v1/premiumIndex")


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


def _sign(query_string: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()


async def fetch_collateral_rates(client: httpx.AsyncClient, api_key: str,
                                 api_secret: str) -> list[dict]:
    """All-asset collateral rates from Portfolio Margin sapi. Signed."""
    params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
    qs = urlencode(params)
    sig = _sign(qs, api_secret)
    url = f"{SAPI_BASE}/sapi/v1/portfolio/collateralRate?{qs}&signature={sig}"
    return await _get_json(client, url, headers={"X-MBX-APIKEY": api_key})


async def fetch_collateral_rates_safe(client: httpx.AsyncClient, api_key: str,
                                      api_secret: str) -> list[dict]:
    """Fetch collateral rates; on failure log a warning + Binance's response
    body and return [] so the rest of the report can still be produced.

    Common reasons this fails:
      - account is not Portfolio Margin enabled (this endpoint is PM-only)
      - API key lacks required permissions
      - server clock skew (timestamp outside recvWindow)
      - signature error
    """
    if not api_key or not api_secret:
        return []
    try:
        return await fetch_collateral_rates(client, api_key, api_secret)
    except httpx.HTTPStatusError as e:
        body = e.response.text[:1000] if e.response is not None else ""
        status = e.response.status_code if e.response is not None else "?"
        logger.warning(
            "collateralRate fetch failed: HTTP %s — %s. haircut column will be NaN.",
            status, body,
        )
        return []
    except Exception as e:  # noqa: BLE001
        logger.warning("collateralRate fetch failed: %s. haircut column will be NaN.", e)
        return []


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


async def _fetch_all_async(api_key: str, api_secret: str, proxy: str = "") -> pd.DataFrame:
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
        premium = await fetch_premium_index_all(client)
        usdt_rows = [p for p in premium if _is_usdt_perp(str(p.get("symbol", "")))]
        symbols = [p["symbol"] for p in usdt_rows]

        history_coros = [bounded(fetch_funding_history(client, s, days=7)) for s in symbols]
        oi_coros = [bounded(fetch_open_interest(client, s)) for s in symbols]

        # Note: haircut is sourced from DataHub now (see funding_top10/datahub.py),
        # not from Binance's /sapi/v1/portfolio/collateralRate. api_key / api_secret
        # are kept on the function signature for forward-compat but unused here.
        del api_key, api_secret

        histories, ois = await asyncio.gather(
            asyncio.gather(*history_coros, return_exceptions=True),
            asyncio.gather(*oi_coros, return_exceptions=True),
            return_exceptions=False,
        )
        collat: list[dict] = []

    haircut_map: dict[str, float] = {}
    if isinstance(collat, list):
        for row in collat:
            try:
                haircut_map[str(row["asset"])] = float(row["collateralRate"])
            except (TypeError, ValueError, KeyError):
                continue

    now_ms = int(time.time() * 1000)
    rows: list[dict] = []
    for p, history, oi in zip(usdt_rows, histories, ois):
        symbol = str(p["symbol"])
        base = _base_from_symbol(symbol)

        if isinstance(history, Exception):
            history = []
        if isinstance(oi, Exception):
            oi = {}

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
            "open_interest_value": oi_value,
            "haircut": haircut_map.get(base, float("nan")),
        })

    return pd.DataFrame(rows)


def fetch_funding_dataframe(api_key: str, api_secret: str, proxy: str = "") -> pd.DataFrame:
    """Sync entrypoint: fetch all Binance data and return the DataFrame.

    api_key / api_secret may be empty; in that case the haircut column is all NaN
    (only the signed collateralRate endpoint needs auth).

    proxy: if non-empty, all HTTP calls route through this URL (e.g.
    "http://proxy.company.com:8080"). If empty, calls go direct.
    """
    return asyncio.run(_fetch_all_async(api_key, api_secret, proxy))
