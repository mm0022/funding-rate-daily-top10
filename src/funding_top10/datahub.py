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
import re
from typing import Any

logger = logging.getLogger(__name__)

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

    The exact JSON shape is not formally documented; this function copes with
    several common forms:
      - bare number: 0.95
      - dict with one of {value, haircut, collateralRate, collateral_rate}
      - list of such dicts (returns the first match)
    Returns None when nothing convertible is found.
    """
    def _to_float(x: Any) -> float | None:
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            try:
                return float(x)
            except ValueError:
                return None
        return None

    direct = _to_float(value)
    if direct is not None:
        return direct

    if isinstance(value, dict):
        for k in ("value", "haircut", "collateralRate", "collateral_rate"):
            if k in value:
                got = _to_float(value[k])
                if got is not None:
                    return got

    if isinstance(value, list):
        for entry in value:
            got = extract_haircut_value(entry)
            if got is not None:
                return got

    return None


class DataHub:
    """Thin wrapper around nexus_data_hub_sdk.Client for read-only key lookups."""

    def __init__(self, prefix: str, api_key: str, gateway_url: str,
                 *, api_timeout: float = 30.0):
        if not prefix or not api_key or not gateway_url:
            raise ValueError(
                "DataHub requires non-empty prefix, api_key, and gateway_url. "
                "Fill in the [datahub] section in config.yaml."
            )
        # Lazy import: keeps this module importable in environments where the
        # SDK isn't installed (e.g. CI, mac dev box).
        from nexus_data_hub_sdk import Client  # noqa: PLC0415
        self.prefix = prefix
        self._client = Client(
            api_key=api_key,
            gateway_url=gateway_url,
            api_timeout=api_timeout,
            route_meta_uri="",
            missing_exception=False,
            updated_exception=False,
        )

    def load_value(self, key: str) -> Any | None:
        """Return the latest JSON-decoded value for `key`, or None if missing."""
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
    diag_budget = 3

    for token in tokens:
        if not token.isascii():
            skipped_non_ascii += 1
            continue

        underlying = strip_denomination_prefix(token)
        key = f"BINANCE_MARGIN_{underlying}.HAIRCUT"
        try:
            raw = datahub.load_value(key)
        except Exception as e:  # noqa: BLE001
            logger.warning("haircut fetch for %s (key=%s) failed: %s", token, key, e)
            continue

        if diag_budget > 0:
            logger.info("haircut diag — token=%s key=%s raw=%r", token, key, raw)
            diag_budget -= 1

        if raw is None:
            continue

        parsed = extract_haircut_value(raw)
        if parsed is not None:
            haircuts[token] = parsed
        else:
            logger.warning("haircut for %s (key=%s) has unrecognised shape: %r", token, key, raw)

    if skipped_non_ascii:
        logger.info("Skipped %d non-ASCII token(s) (no DataHub haircut for those)", skipped_non_ascii)
    return haircuts
