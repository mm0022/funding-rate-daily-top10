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

import sys
from pathlib import Path

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
            )
        except Exception as e:  # noqa: BLE001
            print(f"  init failed: {e}")
            print()
            continue

        any_hit = False
        for token in tokens:
            symbol = f"BINANCE_MARGIN_{token}.HAIRCUT"
            try:
                value = dh.load_haircut_value(symbol)
            except Exception as e:  # noqa: BLE001
                print(f"  {token:<10s} ERROR: {e}")
                continue
            if value is None:
                print(f"  {token:<10s} no data (empty market-data window)")
                continue
            any_hit = True
            print(f"  {token:<10s} value={value}  symbol={symbol}")

        if any_hit:
            print(f"  --> prefix {prefix!r} HAS data — use this one in config.yaml")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
