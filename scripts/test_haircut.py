"""Standalone DataHub haircut probe — fast feedback loop for prefix / key debugging.

Usage (from the project root):

    python scripts\\test_haircut.py
    python scripts\\test_haircut.py ETHFI ENA BTC FLOKI

What it does:
  - Loads config.yaml.
  - Tries the configured prefix first, then PROD/OFFICE/DEV/STAGING (the four
    cyberx env prefixes).
  - For each prefix, queries BINANCE_MARGIN_<TOKEN>.HAIRCUT for each token.
  - Prints raw return value, parsed haircut, and whether the key was found.

This way you can identify which env/prefix actually hosts the haircut data
without running the full main pipeline (which downloads ~300 Binance symbols
first).
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Make src/ importable without requiring `pip install -e .`.
_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from funding_top10.config import load_config  # noqa: E402
from funding_top10.datahub import DataHub  # noqa: E402


DEFAULT_TOKENS = ["ETHFI", "ENA", "PENDLE", "XAUT", "BTC", "FLOKI"]
PREFIXES_TO_TRY = ["CYBERX_PROD", "CYBERX_STAGING", "CYBERX_OFFICE", "CYBERX_DEV"]


def main() -> int:
    tokens = sys.argv[1:] or DEFAULT_TOKENS
    cfg = load_config()

    print(f"Configured prefix: {cfg.datahub.prefix!r}")
    print(f"Gateway:           {cfg.datahub.gateway_url!r}")
    print(f"Tokens to probe:   {tokens}")
    print()

    # Put the configured prefix first, then the others (dedup).
    ordered_prefixes: list[str] = []
    seen: set[str] = set()
    for p in [cfg.datahub.prefix] + PREFIXES_TO_TRY:
        if p and p not in seen:
            ordered_prefixes.append(p)
            seen.add(p)

    for prefix in ordered_prefixes:
        print(f"=== prefix={prefix} ===")
        try:
            dh = DataHub(
                prefix=prefix,
                api_key=cfg.datahub.api_key,
                gateway_url=cfg.datahub.gateway_url,
                cache_directory=cfg.datahub.cache_dir or None,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  init failed: {e}")
            print()
            continue

        any_hit = False
        for token in tokens:
            symbol = f"BINANCE_MARGIN_{token}.HAIRCUT"
            # Call the raw SDK twice (mirroring alpha's is_backfill) so we can
            # introspect the returned HubData shape directly, not just the
            # parsed haircut value.
            now_ms = int(time.time() * 1000)
            start_ms = now_ms - 30 * 24 * 3600 * 1000
            try:
                raw1 = dh._client.request(symbol, start_time=start_ms, end_time=now_ms)
                shape1 = None if raw1.data is None else raw1.data.shape
                print(f"  {token:<10s} attempt1 [start, now] shape={shape1}")
                if raw1.data is None or raw1.data.empty:
                    far_ms = 9_999_999_999_999
                    raw2 = dh._client.request(symbol, start_time=start_ms, end_time=far_ms)
                    shape2 = None if raw2.data is None else raw2.data.shape
                    print(f"  {token:<10s} attempt2 [start, MAX] shape={shape2}")
                    if raw2.data is not None and not raw2.data.empty:
                        print(f"  {token:<10s} columns={list(raw2.data.columns)}")
                        print(f"  {token:<10s} first row: {raw2.data.iloc[0].to_dict()}")
                else:
                    print(f"  {token:<10s} columns={list(raw1.data.columns)}")
                    print(f"  {token:<10s} first row: {raw1.data.iloc[0].to_dict()}")
            except Exception as e:  # noqa: BLE001
                print(f"  {token:<10s} ERROR: {e}")
                continue

            try:
                value = dh.load_haircut_value(symbol)
            except Exception as e:  # noqa: BLE001
                print(f"  {token:<10s} load_haircut_value ERROR: {e}")
                continue
            if value is None:
                print(f"  {token:<10s} parsed=None (no data in window)")
                continue
            any_hit = True
            print(f"  {token:<10s} parsed value={value}  symbol={symbol}")

        if any_hit:
            print(f"  --> prefix {prefix!r} HAS data — use this one in config.yaml")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
