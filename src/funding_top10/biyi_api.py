"""Biyi strategy API client.

Mirrors alpha's funding/funding_service.py:FundingService.load_current_position:
  - POST /biyi/api/strategies/list with body {"query": "<user_query> and $productType like SM-PU|SS-PU"}
  - keep only strategies where strategyType == "LONGSHORT"
  - return aggregated per-ticker positions
    (ticker + position_usd + strategy_names + accounts). Same shape as alpha's
    DataFrame (ticker, position_usd, token), summed when the same symbol
    appears in multiple strategies.

Account / minPositionQty filtering lives in the caller-supplied ``query`` —
server-side query expression handles them, e.g.:
    $accountMap like XXX and $maxPositionQty gt 10

No auth headers — alpha's BaseApiClient calls this endpoint with a plain
httpx.Client and biyi auto-trusts it from the internal network.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import httpx

from funding_top10.cache_util import (
    LIVE,
    NONE,
    DataSource,
    read_cache,
    write_cache,
)

logger = logging.getLogger(__name__)


DEFAULT_BASE_URL = "https://biyi.tky.laozi.pro/biyi/api"
PRODUCT_TYPE_SUFFIX = "$productType like SM-PU|SS-PU"


class BiyiApiClient:
    """Minimal client for the biyi strategy API. Use as a context manager."""

    def __init__(self, base_url: str = DEFAULT_BASE_URL,
                 *, timeout: float = 15.0, proxy: str = ""):
        client_kwargs: dict[str, Any] = {"timeout": timeout, "trust_env": False}
        if proxy:
            client_kwargs["proxy"] = proxy
        self._client = httpx.Client(**client_kwargs)
        self.base_url = base_url.rstrip("/")

    def __enter__(self) -> "BiyiApiClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self._client.close()

    def close(self) -> None:
        self._client.close()

    def list_strategies(self, query: str = "") -> list[dict]:
        """POST /strategies/list. Returns the list of strategy dicts.

        ``query`` is passed as-is to the API. Empty string = no filter.
        """
        url = f"{self.base_url}/strategies/list"
        payload = {"query": query} if query else {}
        resp = self._client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            raise RuntimeError(f"biyi API non-success response: {data!r}")
        return data.get("data") or []


def _join_query(user_query: str) -> str:
    """Append the productType filter that alpha always uses."""
    user_query = (user_query or "").strip()
    if PRODUCT_TYPE_SUFFIX in user_query:
        return user_query
    if user_query:
        return f"{user_query} and {PRODUCT_TYPE_SUFFIX}"
    return PRODUCT_TYPE_SUFFIX


def filter_longshort(strategies: list[dict]) -> list[dict]:
    """Keep only strategyType == 'LONGSHORT' (mirror alpha's assertion)."""
    return [s for s in strategies if s.get("strategyType") == "LONGSHORT"]


def aggregate_positions(strategies: list[dict]) -> list[dict]:
    """Collapse strategies into per-ticker positions.

    Output: ``[{ticker, position_usd}, ...]`` — ``position_usd`` sums
    ``maxPositionQty`` across strategies on the same ticker. Anything else
    the biyi API returns (strategyName, accountMap, …) is dropped.
    """
    agg: dict[str, float] = {}
    for s in strategies:
        t = s.get("ticker")
        if not isinstance(t, str) or "/" not in t:
            continue
        try:
            qty = float(s.get("maxPositionQty") or 0.0)
        except (TypeError, ValueError):
            continue
        agg[t] = agg.get(t, 0.0) + qty
    return [
        {"ticker": t, "position_usd": q}
        for t, q in sorted(agg.items())
    ]


def fetch_biyi_positions(
    base_url: str = DEFAULT_BASE_URL,
    *,
    query: str = "",
    proxy: str = "",
    timeout: float = 15.0,
) -> list[dict]:
    """Top-level helper used by main.py.

    ``query`` is the caller-supplied filter prefix; the productType filter alpha
    always uses is appended automatically. Account / minPositionQty filtering
    belong in ``query`` (server-side), e.g.
    ``$accountMap like XXX and $maxPositionQty gt 10``.

    Returns aggregated per-ticker positions; ``maxPositionQty`` is treated as
    USD notional (matching alpha's ``position_usd = float(maxPositionQty)``).
    """
    full_query = _join_query(query)
    logger.info("biyi /strategies/list query=%r", full_query)

    with BiyiApiClient(base_url=base_url, timeout=timeout, proxy=proxy) as c:
        strategies = c.list_strategies(query=full_query)

    kept = filter_longshort(strategies)
    positions = aggregate_positions(kept)
    logger.info(
        "biyi returned %d strategies, %d after LONGSHORT filter, %d unique tickers",
        len(strategies), len(kept), len(positions),
    )
    return positions


def fetch_biyi_positions_with_cache(
    cache_path: Path | str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    query: str = "",
    proxy: str = "",
    timeout: float = 15.0,
) -> tuple[list[dict], DataSource]:
    """Like fetch_biyi_positions but with on-disk cache fallback.

      - live success (non-empty list) → write cache, return (positions, LIVE)
      - live raised OR returned []    → read cache, return (cached, DataSource(cache))
      - cache also missing            → return ([], NONE)

    Note: an empty result from biyi (no LONGSHORT strategies today) is treated
    the same as "fetch failed" for cache-fallback purposes — we only consider
    biyi's reply authoritative when it returned at least one position. If you
    legitimately want "today there are zero positions" to clear the cache,
    that's a separate decision.
    """
    cache_path = Path(cache_path)
    positions: list[dict] = []
    try:
        positions = fetch_biyi_positions(
            base_url=base_url,
            query=query,
            proxy=proxy,
            timeout=timeout,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("biyi fetch raised (%s); will try cache", e)

    if positions:
        try:
            write_cache(cache_path, positions)
            logger.info(
                "biyi cache updated: %d tickers → %s",
                len(positions), cache_path,
            )
        except Exception:
            logger.exception("Failed to write biyi cache to %s", cache_path)
        return positions, LIVE

    cached = read_cache(cache_path)
    if cached is None:
        logger.error(
            "biyi fetch returned nothing AND cache %s is missing — "
            "no biyi positions available",
            cache_path,
        )
        return [], NONE

    raw_payload, saved_at_ms = cached
    if not isinstance(raw_payload, list):
        logger.warning("biyi cache payload not a list; treating as empty")
        return [], NONE

    if saved_at_ms > 0:
        age_hours = (time.time() * 1000 - saved_at_ms) / 3_600_000
        logger.warning(
            "Using cached biyi (%d tickers, %.1fh old) — biyi unavailable",
            len(raw_payload), age_hours,
        )
    else:
        logger.warning(
            "Using cached biyi (%d tickers, unknown age) — biyi unavailable",
            len(raw_payload),
        )
    return raw_payload, DataSource(kind="cache", saved_at_ms=saved_at_ms or None)
