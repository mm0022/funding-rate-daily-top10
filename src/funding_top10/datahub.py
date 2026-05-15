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

import csv
import io
import json
import logging
import os
import re
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
    """DataHub haircut reader.

    Historically wrapped ``nexus_data_hub_sdk.Client``, but the SDK's
    CSV→DataFrame parser drops every row whose haircut column contains an
    embedded JSON tier list (the embedded commas break field splitting; the
    SDK reports ``shape=(0, 6)`` even when the raw response has 1000+ rows).
    So ``load_haircut_value`` now talks to ``/data-hub-prime/data-api/v1/list``
    directly with httpx and parses ``hot_data[].data`` ourselves.

    The SDK Client is still constructed (lazily) so other callers using
    ``load_value`` for sequenced JSON keys keep working, but it's no longer
    on the haircut path.
    """

    def __init__(self, prefix: str, api_key: str, gateway_url: str,
                 *, api_timeout: float = 30.0,
                 cache_directory: str | None = None):
        if not prefix or not api_key or not gateway_url:
            raise ValueError(
                "DataHub requires non-empty prefix, api_key, and gateway_url. "
                "Fill in the [datahub] section in config.yaml."
            )

        self.prefix = prefix
        self._gateway_url = gateway_url
        self._api_key = api_key
        self._api_timeout = api_timeout

        # SDK Client is lazy: only constructed if load_value is actually called.
        # The haircut path no longer needs it, so we can ride out runs where
        # the SDK isn't installed (e.g. mac dev box).
        self._sdk_client: Any = None
        self._sdk_cache_dir = cache_directory or DEFAULT_SDK_CACHE_DIR

    def _get_sdk_client(self) -> Any:
        if self._sdk_client is None:
            from nexus_data_hub_sdk import Client  # noqa: PLC0415
            _patch_sdk_move_file_for_windows_av()
            os.makedirs(self._sdk_cache_dir, exist_ok=True)
            self._sdk_client = Client(
                api_key=self._api_key,
                gateway_url=self._gateway_url,
                api_timeout=self._api_timeout,
                route_meta_uri="",
                missing_exception=False,
                updated_exception=False,
                directory=self._sdk_cache_dir,
            )
        return self._sdk_client

    def load_value(self, key: str) -> Any | None:
        """Return the latest JSON-decoded value for `key`, or None if missing.

        For sequenced JSON keys. Use ``load_haircut_value`` for haircut data
        (which lives under the market-data namespace).
        """
        full_key = normalize_key(key, self.prefix)
        hub_data = self._get_sdk_client().request_latest_sequenced_data(full_key)
        if hub_data.data is None or hub_data.data.empty:
            return None
        item = hub_data.data.iloc[0]
        content = item["content"]
        content_type = item["content_type"]
        if content_type == "JSON":
            return json.loads(content)
        return content

    def load_haircut_value(self, symbol: str,
                           *, lookback_days: int = DEFAULT_HAIRCUT_LOOKBACK_DAYS) -> float | None:  # noqa: ARG002
        """Latest haircut for the given DataHub symbol, or None.

        ``lookback_days`` kept for signature compatibility but unused — the
        list API returns whatever DataHub currently retains for the sym, and
        we pick the row with the newest ``sample_time``.
        """
        return _fetch_haircut_via_list_api(
            self._gateway_url, self._api_key, symbol, timeout=self._api_timeout,
        )


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


# Matches "BINANCE_MARGIN_BTC.HAIRCUT" -> exchange=BINANCE, business=MARGIN,
# sym=BTC, category=HAIRCUT. Token names can contain digits / underscores / dots,
# but the exchange / business prefixes are always alpha.
_SYMBOL_RE = re.compile(r"^([A-Z]+)_([A-Z]+)_(.+)\.([A-Z_]+)$")


def _parse_hot_data_row(row_str: str) -> tuple[int, float] | None:
    """Parse one ``hot_data[].data[]`` CSV string into (sample_time_ms, value).

    Each row looks like:
        '1772323200000,1772326799999,1772323200000,BTC,"[{""left"":0,""right"":9999999999999,""value"":0.95}]",true'

    Column 3 is sample_time. Column 5 is a JSON tier list (quoted, with
    embedded commas — that's what trips the SDK's naïve CSV parser). We use
    csv.reader with ``quotechar='"'`` so the embedded commas are respected.
    """
    try:
        reader = csv.reader(io.StringIO(row_str), quotechar='"', skipinitialspace=True)
        row = next(reader, None)
    except Exception:  # noqa: BLE001
        return None
    if not row or len(row) < 5:
        return None
    try:
        sample_time = int(row[2])
    except (TypeError, ValueError):
        return None
    try:
        tiers = json.loads(row[4])
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(tiers, list) or not tiers or not isinstance(tiers[0], dict):
        return None
    try:
        value = float(tiers[0].get("value"))
    except (TypeError, ValueError):
        return None
    if value != value:  # NaN
        return None
    return sample_time, value


def _fetch_haircut_via_list_api(
    gateway_url: str,
    api_key: str,
    symbol: str,
    *,
    timeout: float = 15.0,
) -> float | None:
    """Hit /data-hub-prime/data-api/v1/list directly and return the most-recent
    haircut value for ``symbol``, or ``None`` if no parseable row.

    Why this exists: ``nexus_data_hub_sdk.Client.request()`` parses
    ``hot_data[].data`` into a DataFrame internally, and its CSV splitter does
    NOT respect ``quotechar`` — every row whose haircut column contains an
    embedded JSON list gets dropped silently. The SDK returns ``shape=(0, 6)``
    even when the raw response has 1000+ valid rows. We can't fix the SDK in
    place, so we bypass it.

    Time window: we ask for ``[0, MAX_TIMESTAMP]`` so DataHub returns all of
    its currently-retained data for this sym. Picking the row with the newest
    ``sample_time`` gives us the latest haircut. This handles the "yesterday
    had haircut, today it's 0" case cleanly: today's 0 wins because it has the
    newest sample_time.
    """
    m = _SYMBOL_RE.match(symbol)
    if not m:
        logger.warning("haircut symbol does not match expected pattern: %s", symbol)
        return None
    exchange, business, sym, category = m.groups()

    base = gateway_url.rstrip("/")
    if base.endswith("/nexus-data-hub-gateway"):
        base = base[: -len("/nexus-data-hub-gateway")]
    url = f"{base}/data-hub-prime/data-api/v1/list"

    params = {
        "exchange": exchange,
        "business": business,
        "category": category,
        "sym": sym,
        "start": 0,
        "end": 9_999_999_999_999,
    }
    headers = {"X-API-Key": api_key} if api_key else {}

    try:
        resp = httpx.get(url, params=params, headers=headers, timeout=timeout)
    except (httpx.RequestError, httpx.HTTPError) as e:
        logger.warning("haircut list-API request failed for %s: %r", symbol, e)
        return None
    if resp.status_code != 200:
        # 422 "is not configured" is common for tokens DataHub doesn't track —
        # downgrade to debug-ish so it doesn't drown the log on hundreds of
        # such tokens.
        if resp.status_code == 422:
            return None
        logger.warning(
            "haircut list-API non-200 for %s: status=%d body[:200]=%r",
            symbol, resp.status_code, resp.text[:200],
        )
        return None

    try:
        data = resp.json()
    except ValueError:
        logger.warning("haircut list-API returned non-JSON for %s", symbol)
        return None

    hot = data.get("hot_data") or []
    latest_sample_time = -1
    latest_value: float | None = None
    for group in hot:
        for row_str in group.get("data") or []:
            parsed = _parse_hot_data_row(row_str)
            if parsed is None:
                continue
            sample_time, value = parsed
            if sample_time > latest_sample_time:
                latest_sample_time = sample_time
                latest_value = value
    return latest_value


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
# still produces a usable report.
# ---------------------------------------------------------------------------


def load_haircuts_with_cache(
    cache_path: Path | str,
    *,
    datahub: DataHub | None,
    tokens: list[str],
    max_workers: int = 10,
) -> tuple[dict[str, float], DataSource]:
    """Try DataHub; on failure fall back to the on-disk cache.

    Returns ``(haircuts_dict, DataSource)``. ``DataSource.kind`` is:
      - "live"  → fresh DataHub data (cache also updated)
      - "cache" → DataHub unavailable, served from cache
      - "none"  → DataHub failed AND cache missing; dict is empty

    ``datahub`` may be ``None`` (caller's DataHub.__init__ raised). In that
    case the live fetch is skipped and we go straight to the cache.

    Partial DataHub success (some tokens missing) still counts as live — the
    cache is overwritten with whatever DataHub returned this run.
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
            write_cache(cache_path, haircuts)
            logger.info(
                "haircut cache updated: %d tokens written to %s",
                len(haircuts), cache_path,
            )
        except Exception:
            logger.exception("Failed to write haircut cache to %s", cache_path)
        return haircuts, LIVE

    # Live fetch yielded nothing — try cache.
    cached = read_cache(cache_path)
    if cached is None:
        logger.error(
            "DataHub returned no haircut data AND cache %s is missing — "
            "all haircuts will be 0",
            cache_path,
        )
        return {}, NONE

    raw_payload, saved_at_ms = cached
    if not isinstance(raw_payload, dict):
        logger.warning("Haircut cache payload not a dict; treating as empty")
        return {}, NONE
    haircuts_from_cache: dict[str, float] = {}
    for k, v in raw_payload.items():
        try:
            haircuts_from_cache[str(k)] = float(v)
        except (TypeError, ValueError):
            continue

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
    return haircuts_from_cache, DataSource(kind="cache", saved_at_ms=saved_at_ms or None)
