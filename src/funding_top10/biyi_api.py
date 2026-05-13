"""Biyi strategy API client.

Replaces the previous SQL query against biyi_strategy_data_his. Hits the
internal POST /biyi/api/strategies/list endpoint (same as alpha's
funding_clients/strategy_info.py:DataApiClient.load_strategy_info), extracts
the ``tickers`` from each returned strategy, dedupes, and returns the flat
"BASE/QUOTE" list the rest of the pipeline expects.

The endpoint is internal (biyi.tky.laozi.pro) and historically reachable
without going through the corporate proxy. We default ``trust_env=False`` +
no proxy; pass a proxy explicitly if your network needs one.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


DEFAULT_BASE_URL = "https://biyi.tky.laozi.pro/biyi/api"


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

        ``query`` is a free-form filter string passed straight to the API
        (caller's responsibility to know the syntax — empty = all).
        """
        url = f"{self.base_url}/strategies/list"
        payload = {"query": query} if query else {}
        resp = self._client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            raise RuntimeError(f"biyi API non-success response: {data!r}")
        return data.get("data") or []


def extract_tickers(strategies: list[dict]) -> list[str]:
    """Pull all BASE/QUOTE tickers from a list of strategy dicts (deduped, sorted).

    Each strategy may have either ``tickers`` (list[str]) or a single
    ``ticker`` (str) — extract whatever's present.
    """
    out: set[str] = set()
    for s in strategies:
        for t in s.get("tickers") or []:
            if isinstance(t, str) and t:
                out.add(t)
        single = s.get("ticker")
        if isinstance(single, str) and single:
            out.add(single)
    return sorted(out)


def fetch_biyi_tickers(base_url: str = DEFAULT_BASE_URL, *,
                       query: str = "", proxy: str = "",
                       timeout: float = 15.0) -> list[str]:
    """One-shot helper: open a client, call /strategies/list, return tickers."""
    with BiyiApiClient(base_url=base_url, timeout=timeout, proxy=proxy) as c:
        strategies = c.list_strategies(query=query)
    return extract_tickers(strategies)
