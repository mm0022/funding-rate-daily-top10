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
from typing import Any

import httpx

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
