"""Diagnostic: hit DataHub for one BINANCE_MARGIN_*.HAIRCUT key and print the
raw DataFrame shape so we can see whether the SDK is getting nothing back vs.
getting data we can't parse.

Run:
    python scripts/diag_haircut.py [TOKEN]     # default token = ETHFI
"""

from __future__ import annotations

import sys
import time

from funding_top10.config import load_config
from funding_top10.datahub import DataHub

TOKEN = sys.argv[1] if len(sys.argv) > 1 else "ETHFI"
SYMBOL = f"BINANCE_MARGIN_{TOKEN}.HAIRCUT"


def main() -> int:
    cfg = load_config()
    print(f"DataHub gateway: {cfg.datahub.gateway_url}")
    print(f"prefix={cfg.datahub.prefix}  api_key starts with: {cfg.datahub.api_key[:6]}…")
    print(f"Probing symbol: {SYMBOL}\n")

    dh = DataHub(
        prefix=cfg.datahub.prefix,
        api_key=cfg.datahub.api_key,
        gateway_url=cfg.datahub.gateway_url,
        cache_directory=cfg.datahub.cache_dir or None,
    )

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 7 * 86400 * 1000

    # First: 7-day window. Same call our load_haircut_value() makes.
    hub_data = dh._client.request(SYMBOL, start_time=start_ms, end_time=now_ms)
    df = hub_data.data
    print(f"[7d window] data is None: {df is None}")
    if df is not None:
        print(f"[7d window] empty: {df.empty}")
        if not df.empty:
            print(f"[7d window] columns: {df.columns.tolist()}")
            print(f"[7d window] rows: {len(df)}")
            print(f"[7d window] last row: {df.iloc[-1].to_dict()}")

    # Second: wide window (matches the SDK's is_backfill retry).
    print()
    hub_data2 = dh._client.request(SYMBOL, start_time=start_ms, end_time=9_999_999_999_999)
    df2 = hub_data2.data
    print(f"[backfill window] data is None: {df2 is None}")
    if df2 is not None:
        print(f"[backfill window] empty: {df2.empty}")
        if not df2.empty:
            print(f"[backfill window] columns: {df2.columns.tolist()}")
            print(f"[backfill window] rows: {len(df2)}")
            print(f"[backfill window] last row: {df2.iloc[-1].to_dict()}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
