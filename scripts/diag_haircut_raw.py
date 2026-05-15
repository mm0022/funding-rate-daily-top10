"""Diagnostic: bypass nexus_data_hub_sdk and hit the list API with plain httpx.

Hypothesis: SDK's CSV-string -> DataFrame parser is dropping rows because the
haircut field contains an embedded JSON list with commas. Browser shows
hot_data has rows; SDK gives shape=(0, 6). This script proves the data is
there in the raw HTTP response.

Run:
    python scripts/diag_haircut_raw.py [TOKEN]    # default = ZORA (known to have rows)
"""

from __future__ import annotations

import json
import sys

import httpx

from funding_top10.config import load_config

TOKEN = sys.argv[1] if len(sys.argv) > 1 else "ZORA"


def main() -> int:
    cfg = load_config()
    base = cfg.datahub.gateway_url.rstrip("/")
    # gateway_url typically ends in /nexus-data-hub-gateway/ — strip it to land
    # at the host root, then add /data-hub-prime/...
    if base.endswith("/nexus-data-hub-gateway"):
        host = base[: -len("/nexus-data-hub-gateway")]
    else:
        host = base
    url = f"{host}/data-hub-prime/data-api/v1/list"

    params = {
        "exchange": "BINANCE",
        "business": "MARGIN",
        "category": "HAIRCUT",
        "sym": TOKEN,
        "start": 0,
        "end": 9_999_999_999_999,
    }
    print(f"URL: {url}")
    print(f"params: {params}")
    print(f"api_key starts with: {cfg.datahub.api_key[:6]}…\n")

    # Try several auth styles. Print which one(s) succeed.
    attempts = [
        ("no auth", {}),
        ("api-key header", {"api-key": cfg.datahub.api_key}),
        ("X-API-Key header", {"X-API-Key": cfg.datahub.api_key}),
        ("Authorization Bearer", {"Authorization": f"Bearer {cfg.datahub.api_key}"}),
    ]
    last_ok_body = None
    for label, headers in attempts:
        try:
            resp = httpx.get(url, params=params, headers=headers, timeout=20.0)
        except Exception as e:  # noqa: BLE001
            print(f"[{label}] EXCEPTION: {type(e).__name__}: {e}")
            continue
        print(f"[{label}] status={resp.status_code}, body[:200]={resp.text[:200]!r}")
        if resp.status_code == 200:
            last_ok_body = resp.text

    if not last_ok_body:
        print("\nAll auth styles failed.")
        return 1

    # Parse the most recent OK response: list rows under hot_data[].data
    try:
        data = json.loads(last_ok_body)
    except Exception as e:  # noqa: BLE001
        print(f"\nCould not parse body as JSON: {e}")
        return 1

    hot = data.get("hot_data") or []
    print(f"\nhot_data has {len(hot)} group(s)")
    for i, group in enumerate(hot):
        rows = group.get("data") or []
        print(f"  group[{i}]: {len(rows)} row(s); first row: {rows[0] if rows else '-'}")
        if rows:
            print(f"  last row: {rows[-1]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
