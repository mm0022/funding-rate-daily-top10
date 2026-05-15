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
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# Local cache directory the nexus_data_hub_sdk uses to spool downloaded
# market-data files. Defaults to ~/.datahub_cache — outside AppData/Temp,
# which corporate AV products often quarantine .tmp writes from. Override
# via DataHub(cache_directory=...) or the [datahub] cache_dir config field.
DEFAULT_SDK_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".datahub_cache")


# Default look-back for the haircut market-data query. 7 days is enough — the
# SDK retries with MAX end_time if the initial window is empty (mirrors
# alpha's is_backfill=True), so an inactive feed is still captured.
DEFAULT_HAIRCUT_LOOKBACK_DAYS = 7

# Sentinel for the end_time. Must match the SDK's Constants.MAX_TIMESTAMP
# (13 digits, ~year 2286); passing a larger value gets silently filtered to
# empty results.
_FAR_FUTURE_MS = 9_999_999_999_999

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
        _patch_sdk_move_file_for_windows_av()

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
        """Fetch the latest haircut for a market-data symbol via the SDK's
        ``Client.request``. Mirrors alpha's market_data_request(is_backfill=True):
          1. request [now - lookback, now]
          2. if empty, retry [now - lookback, MAX_TIMESTAMP]
        Returns the latest sample's first-tier ``value``, or ``None``.
        """
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - lookback_days * 24 * 3600 * 1000

        hub_data = self._client.request(symbol, start_time=start_ms, end_time=now_ms)
        if hub_data.data is None or hub_data.data.empty:
            hub_data = self._client.request(symbol, start_time=start_ms, end_time=_FAR_FUTURE_MS)
        return parse_haircut_from_market_data_df(hub_data.data)


_sdk_move_file_patched = False


def _patch_sdk_move_file_for_windows_av() -> None:
    """Replace nexus_data_hub_sdk.FileHelper.move_file with a plain os.rename.

    The SDK's original does ``os.chmod(src, READ_ONLY) + os.rename(src, dst)``.
    Corporate endpoint AV on Windows often treats "chmod-readonly then rename"
    as ransomware-staging and denies the rename with WinError 5. A plain
    rename without the chmod doesn't trip that heuristic.

    Idempotent: only patches on first call.
    """
    global _sdk_move_file_patched
    if _sdk_move_file_patched:
        return

    try:
        from nexus_data_hub_sdk.util import file_helper as _fh  # noqa: PLC0415
    except ImportError:
        return  # SDK not installed; nothing to patch

    def _move_no_chmod(src_file_name: str, desc_file_name: str) -> None:
        # os.replace overwrites the destination on Windows, unlike os.rename
        # which raises WinError 183 if dst exists. Replicates the upstream
        # behaviour on POSIX too.
        if src_file_name and desc_file_name and os.path.exists(src_file_name):
            os.replace(src_file_name, desc_file_name)

    _fh.FileHelper.move_file = staticmethod(_move_no_chmod)
    _sdk_move_file_patched = True
    logger.info("nexus_data_hub_sdk.FileHelper.move_file patched (no chmod) to dodge Windows AV")


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
        if v is None:
            return None
        # The SDK returns the value as decimal.Decimal — float() handles that as
        # well as int / float / numeric-string.
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        if f != f:  # NaN
            return None
        return f
    return None


def load_binance_haircuts(datahub: DataHub, tokens: list[str],
                          *, max_workers: int = 10) -> dict[str, float]:
    """Fetch the haircut for each ASCII token in `tokens` in parallel.

    Each token is the perp base symbol (e.g. '1000FLOKI'). We strip Binance's
    denomination prefix (so '1000FLOKI' → 'FLOKI') before forming the DataHub
    key. The returned dict keys are the ORIGINAL token names so the caller can
    map directly from funding_df['base'].

    Tokens with non-ASCII characters are skipped silently.

    Concurrency: ``max_workers`` threads share the single DataHub SDK client.
    httpx.Client (the SDK's transport) is documented thread-safe; the patched
    FileHelper.move_file uses os.replace which is atomic on Windows. ~500
    tokens at 10 workers takes ~10 seconds vs ~90 sequential.
    """
    from concurrent.futures import ThreadPoolExecutor  # noqa: PLC0415

    def _fetch_one(token: str) -> tuple[str, str | None, float | None]:
        """Return (token, used_symbol, value). value=None if no data / errored."""
        candidates = [token]
        stripped = strip_denomination_prefix(token)
        if stripped and stripped != token:
            candidates.append(stripped)
        for candidate in candidates:
            symbol = f"BINANCE_MARGIN_{candidate}.HAIRCUT"
            try:
                v = datahub.load_haircut_value(symbol)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "haircut fetch for %s (symbol=%s) failed: %s",
                    token, symbol, e,
                )
                continue
            if v is not None:
                return token, symbol, v
        return token, None, None

    ascii_tokens = [t for t in tokens if t.isascii()]
    skipped_non_ascii = len(tokens) - len(ascii_tokens)

    haircuts: dict[str, float] = {}
    not_found: list[str] = []
    diag_budget = 3

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for token, used_symbol, value in executor.map(_fetch_one, ascii_tokens):
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
        len(haircuts), len(ascii_tokens), not_found[:20],
    )
    return haircuts


# ---------------------------------------------------------------------------
# Cache-aside: persist successful haircut lookups so a future DataHub outage
# still produces a usable report. Cache lives in project_root/cache/haircuts.json
# (git-ignored). Format:
#   {"saved_at_unix_ms": 1747200000000, "haircuts": {"BTC": 0.95, ...}}
# ---------------------------------------------------------------------------


def _save_haircut_cache(path: Path, haircuts: dict[str, float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "saved_at_unix_ms": int(time.time() * 1000),
        "haircuts": haircuts,
    }
    # Atomic write: tmp + replace so a crash mid-write can't corrupt the cache.
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(tmp, path)


def _load_haircut_cache(path: Path) -> tuple[dict[str, float], int] | None:
    """Return (haircuts, saved_at_unix_ms) or None if the cache is missing /
    corrupt. ``saved_at_unix_ms`` may be 0 if the field is absent."""
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to parse haircut cache at %s: %s", path, e)
        return None

    raw = payload.get("haircuts") if isinstance(payload, dict) else None
    if not isinstance(raw, dict):
        return None
    out: dict[str, float] = {}
    for k, v in raw.items():
        try:
            out[str(k)] = float(v)
        except (TypeError, ValueError):
            continue
    try:
        saved_at_ms = int(payload.get("saved_at_unix_ms") or 0)
    except (TypeError, ValueError):
        saved_at_ms = 0
    return (out, saved_at_ms)


def load_haircuts_with_cache(
    cache_path: Path | str,
    *,
    datahub: DataHub | None,
    tokens: list[str],
    max_workers: int = 10,
) -> dict[str, float]:
    """Try DataHub; on failure fall back to the on-disk cache.

    ``datahub`` may be ``None`` (caller's DataHub.__init__ raised — e.g. the SDK
    isn't installed, or the gateway URL is unreachable). In that case we skip
    the live fetch and go straight to the cache.

    Behaviour:
      - DataHub returns a non-empty dict  → persist + return it
      - DataHub raises OR returns {}      → log a warning, return cache contents
                                             (empty dict if no cache file)

    Partial DataHub success (some tokens missing) still counts as success — the
    cache is overwritten with whatever DataHub returned this run. So a token
    that disappears from DataHub also disappears from the cache the next time
    DataHub responds at all.
    """
    cache_path = Path(cache_path)
    haircuts: dict[str, float] = {}

    if datahub is not None:
        try:
            haircuts = load_binance_haircuts(datahub, tokens, max_workers=max_workers)
        except Exception as e:  # noqa: BLE001
            logger.warning("DataHub haircut fetch raised (%s); will try cache", e)

    if haircuts:
        try:
            _save_haircut_cache(cache_path, haircuts)
            logger.info(
                "haircut cache updated: %d tokens written to %s",
                len(haircuts), cache_path,
            )
        except Exception:
            logger.exception("Failed to write haircut cache to %s", cache_path)
        return haircuts

    # Live fetch yielded nothing — try cache.
    cached = _load_haircut_cache(cache_path)
    if cached is None:
        logger.error(
            "DataHub returned no haircut data AND cache %s is missing — "
            "all haircuts will be 0",
            cache_path,
        )
        return {}

    haircuts_from_cache, saved_at_ms = cached
    if saved_at_ms > 0:
        age_hours = (time.time() * 1000 - saved_at_ms) / 3_600_000
        logger.warning(
            "Using cached haircut (%d tokens, %.1fh old) — DataHub unavailable",
            len(haircuts_from_cache), age_hours,
        )
    else:
        logger.warning(
            "Using cached haircut (%d tokens, unknown age) — DataHub unavailable",
            len(haircuts_from_cache),
        )
    return haircuts_from_cache
