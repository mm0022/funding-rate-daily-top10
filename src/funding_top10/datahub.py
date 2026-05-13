"""Minimal DataHub client for fetching BINANCE haircut data.

Only implements what funding-top10 needs:
  - load latest sequenced value for a given key
  - convenience function to fetch BINANCE_MARGIN_<TOKEN>.HAIRCUT for a list of tokens

The underlying `nexus_data_hub_sdk` package is NOT on public pypi; it lives in
the alpha repo at ``vendor/nexus-data-hub-sdk``. The Windows deployment must
install it from there:

    pip install C:\\path\\to\\alpha\\vendor\\nexus-data-hub-sdk

The SDK is lazy-imported inside ``DataHub.__init__`` so this module can still be
imported (and the pure helpers below tested) without the SDK present.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

logger = logging.getLogger(__name__)


# Local cache directory the nexus_data_hub_sdk uses to spool downloaded
# market-data files. Defaults to ~/.datahub_cache — outside AppData/Temp,
# which corporate AV products often quarantine .tmp writes from. Override
# via DataHub(cache_directory=...) or the [datahub] cache_dir config field.
DEFAULT_SDK_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".datahub_cache")


# Default look-back for the haircut market-data query. We open the window
# wide (90 days back, end pushed to far future) so even tokens whose haircut
# data updates infrequently — or whose data feed stopped a while ago — still
# return at least the most recent sample. The DataFrame we get back will be
# sorted by sample_time and the latest row is used.
DEFAULT_HAIRCUT_LOOKBACK_DAYS = 90

# Sentinel for the end_time: mirrors alpha's Constants.MAX_TIMESTAMP.
# ~year 5138 — far beyond any real funding-rate event.
_FAR_FUTURE_MS = 99_999_999_999_999

# Binance USDT-perp denomination prefixes: 1000, 10000, 100000, 1000000, ...
# These are NOT part of the underlying token name (e.g. 1000FLOKI's haircut is
# FLOKI's haircut). Single-zero / no-zero leading "1" tokens like 1INCH are
# legitimate token names and must NOT be stripped.
_DENOMINATION_PREFIX = re.compile(r"^10{3,}")


def strip_denomination_prefix(base: str) -> str:
    """Remove a Binance perp denomination prefix if present.

    Examples:
        BTC          → BTC
        1000FLOKI    → FLOKI
        10000PEPE    → PEPE
        1000000MOG   → MOG
        1INCH        → 1INCH   (only one "0" — not a denom prefix)
    """
    return _DENOMINATION_PREFIX.sub("", base)


def normalize_key(key: str, prefix: str) -> str:
    """Mirror of alpha's normalize_key: ensure the prefix is the first '-'-segment."""
    parts = key.split("-")
    if prefix and parts[0] != prefix:
        parts.insert(0, prefix)
    return "-".join(parts)


def extract_haircut_value(value: Any) -> float | None:
    """Best-effort extraction of a numeric haircut from whatever DataHub returns.

    Forms handled, in priority order:

    1. ``list[dict]`` of versioned records — each record carries a ``haircut``
       list (the tier table) plus a timestamp (``sample_time`` / ``close_time``
       / ``start_time``). We pick the record with the newest timestamp and
       take ``haircut[0].value``. This is the shape DataHub returns for
       ``BINANCE_MARGIN_<TOKEN>.HAIRCUT``::

           [
             {"sample_time": 1778652000000,
              "haircut": [{"left": 0, "right": 9999..., "value": 0.5}],
              "symbol": "ETHFI", ...},
             ...
           ]

    2. ``dict`` with a ``haircut`` key whose value is a tier list — same idea
       but unwrapped (single record, no versioning).
    3. ``dict`` with a scalar key (``value`` / ``haircut`` / ``collateralRate``
       / ``collateral_rate``).
    4. bare number (int / float / numeric string).
    5. generic list — recursively try each element.

    Returns ``None`` when nothing convertible is found.
    """

    def _to_float(x: Any) -> float | None:
        if isinstance(x, bool):
            return None  # avoid True/False being coerced to 1.0/0.0
        if isinstance(x, (int, float)):
            try:
                f = float(x)
            except (TypeError, ValueError):
                return None
            return None if f != f else f  # filter NaN
        if isinstance(x, str):
            try:
                return float(x)
            except ValueError:
                return None
        return None

    direct = _to_float(value)
    if direct is not None:
        return direct

    # Case 1: versioned record list
    if isinstance(value, list) and value and isinstance(value[0], dict) and "haircut" in value[0]:
        def _ts(entry: dict) -> int:
            for k in ("sample_time", "close_time", "start_time"):
                t = entry.get(k)
                if isinstance(t, (int, float)):
                    return int(t)
            return 0

        latest = max(value, key=_ts)
        tiers = latest.get("haircut")
        if isinstance(tiers, list) and tiers and isinstance(tiers[0], dict):
            v = _to_float(tiers[0].get("value"))
            if v is not None:
                return v

    # Case 2 / 3: dict
    if isinstance(value, dict):
        tiers = value.get("haircut")
        if isinstance(tiers, list) and tiers and isinstance(tiers[0], dict):
            v = _to_float(tiers[0].get("value"))
            if v is not None:
                return v
        for k in ("value", "haircut", "collateralRate", "collateral_rate"):
            if k in value:
                v = _to_float(value[k])
                if v is not None:
                    return v

    # Case 5: generic list — recurse
    if isinstance(value, list):
        for entry in value:
            got = extract_haircut_value(entry)
            if got is not None:
                return got

    return None


class DataHub:
    """Thin wrapper around nexus_data_hub_sdk.Client.

    Two read methods:
      - load_value: latest sequenced value (JSON content), via
        request_latest_sequenced_data. Used by miscellaneous key-value reads;
        not used for the haircut path anymore.
      - load_haircut_value: market-data time-series request via client.request,
        same call alpha makes via DataHubClient.market_data_request. Returns
        the latest haircut value parsed from the resulting DataFrame.
    """

    def __init__(self, prefix: str, api_key: str, gateway_url: str,
                 *, api_timeout: float = 30.0,
                 cache_directory: str | None = None):
        if not prefix or not api_key or not gateway_url:
            raise ValueError(
                "DataHub requires non-empty prefix, api_key, and gateway_url. "
                "Fill in the [datahub] section in config.yaml."
            )
        # Lazy import: keeps this module importable in environments where the
        # SDK isn't installed (e.g. CI, mac dev box).
        from nexus_data_hub_sdk import Client  # noqa: PLC0415
        self.prefix = prefix

        # Resolve the SDK's local cache dir to somewhere we can definitely write.
        # The SDK's default is './.data' (relative to CWD) which fails with
        # WinError 5 on locked-down Windows. Pre-create the dir so the SDK
        # doesn't have to.
        cache_dir = cache_directory or DEFAULT_SDK_CACHE_DIR
        os.makedirs(cache_dir, exist_ok=True)

        self._client = Client(
            api_key=api_key,
            gateway_url=gateway_url,
            api_timeout=api_timeout,
            route_meta_uri="",
            missing_exception=False,
            updated_exception=False,
            directory=cache_dir,
        )

    def load_value(self, key: str) -> Any | None:
        """Return the latest JSON-decoded value for `key`, or None if missing.

        For sequenced JSON keys. Use ``load_haircut_value`` for haircut data
        (which lives under the market-data namespace).
        """
        full_key = normalize_key(key, self.prefix)
        hub_data = self._client.request_latest_sequenced_data(full_key)
        if hub_data.data is None or hub_data.data.empty:
            return None
        item = hub_data.data.iloc[0]
        content = item["content"]
        content_type = item["content_type"]
        if content_type == "JSON":
            return json.loads(content)
        return content

    def load_haircut_value(self, symbol: str,
                           *, lookback_days: int = DEFAULT_HAIRCUT_LOOKBACK_DAYS) -> float | None:
        """Fetch the latest haircut for a market-data symbol.

        One ``client.request`` call with a wide window:
          - start_time = now - 90 days
          - end_time   = FAR_FUTURE (~year 5138)

        We always pull all rows in that window and return the latest one's
        first-tier ``value``. Wide-by-default is safer than tight-then-retry
        because a token whose haircut data feed paused weeks ago is invisible
        to a narrow window, no matter how the retry is shaped.

        Returns ``None`` only when truly no rows exist for that symbol.
        """
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - lookback_days * 24 * 3600 * 1000
        hub_data = self._client.request(symbol, start_time=start_ms, end_time=_FAR_FUTURE_MS)
        return parse_haircut_from_market_data_df(hub_data.data)


def parse_haircut_from_market_data_df(df: Any) -> float | None:
    """Pull the latest haircut value from a market-data DataFrame.

    Expected columns: ``sample_time`` (int ms), ``haircut`` (list of dicts
    with ``left``, ``right``, ``value``). The most recent row's first tier
    ``value`` is returned.
    """
    if df is None:
        return None
    try:
        if df.empty:
            return None
    except AttributeError:
        return None

    if "sample_time" in df.columns:
        df_sorted = df.sort_values("sample_time")
    else:
        df_sorted = df
    latest = df_sorted.iloc[-1]
    tiers = latest.get("haircut") if hasattr(latest, "get") else latest["haircut"]
    if isinstance(tiers, list) and tiers and isinstance(tiers[0], dict):
        v = tiers[0].get("value")
        if isinstance(v, (int, float)) and not (isinstance(v, float) and v != v):
            return float(v)
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                return None
    return None


def load_binance_haircuts(datahub: DataHub, tokens: list[str]) -> dict[str, float]:
    """Fetch the haircut for each ASCII token in `tokens`.

    Each token is the perp base symbol (e.g. '1000FLOKI'). We strip Binance's
    denomination prefix (so '1000FLOKI' → 'FLOKI') before forming the DataHub
    key. The returned dict keys are the ORIGINAL token names so the caller can
    map directly from funding_df['base'].

    Tokens with non-ASCII characters (e.g. Chinese meme-coin names) are skipped
    silently — DataHub does not store haircuts for them and querying just
    pollutes the log.

    For the first few tokens we log the raw DataHub return value at INFO level
    so we can diagnose key-format / value-shape problems.
    """
    haircuts: dict[str, float] = {}
    skipped_non_ascii = 0
    queried = 0
    not_found: list[str] = []
    diag_budget = 3

    for token in tokens:
        if not token.isascii():
            skipped_non_ascii += 1
            continue

        # Try the perp base name first (e.g. '1000FLOKI'), then the underlying
        # token (e.g. 'FLOKI'). alpha uploads haircut data under both
        # conventions for different tokens, so we don't have to guess.
        candidates = [token]
        stripped = strip_denomination_prefix(token)
        if stripped and stripped != token:
            candidates.append(stripped)

        value: float | None = None
        used_symbol: str | None = None
        for candidate in candidates:
            symbol = f"BINANCE_MARGIN_{candidate}.HAIRCUT"
            try:
                value = datahub.load_haircut_value(symbol)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "haircut fetch for %s (symbol=%s) failed: %s",
                    token, symbol, e,
                )
                value = None
                continue
            if value is not None:
                used_symbol = symbol
                break

        queried += 1
        if diag_budget > 0:
            logger.info(
                "haircut diag — token=%s used_symbol=%s value=%s",
                token, used_symbol, value,
            )
            diag_budget -= 1

        if value is None:
            not_found.append(token)
            continue

        haircuts[token] = value
        logger.info(
            "haircut found — token=%s symbol=%s value=%s",
            token, used_symbol, value,
        )

    if skipped_non_ascii:
        logger.info("Skipped %d non-ASCII token(s) (no DataHub haircut for those)", skipped_non_ascii)
    logger.info(
        "haircut summary: %d/%d queried tokens had data; not-found=%r",
        len(haircuts), queried, not_found[:20],
    )
    return haircuts
