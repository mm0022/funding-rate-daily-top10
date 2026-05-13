"""Daily entry point: fetch -> rank -> format -> post to Slack."""

from __future__ import annotations

# Bootstrap: ensure src/ is on sys.path so `python src/funding_top10/main.py`
# (script-path invocation) works the same as `python -m funding_top10.main`.
# Harmless when the package was already installed via `pip install -e .`.
import sys
from pathlib import Path

_SRC_DIR = Path(__file__).resolve().parent.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

import datetime  # noqa: E402
import logging  # noqa: E402

import pandas as pd  # noqa: E402

from funding_top10.binance_api import fetch_funding_dataframe  # noqa: E402
from funding_top10.biyi_api import fetch_biyi_tickers as fetch_biyi_tickers_api  # noqa: E402
from funding_top10.config import load_config  # noqa: E402
from funding_top10.datahub import DataHub, load_binance_haircuts  # noqa: E402
from funding_top10.scoring import ScoreWeights, select_rows_to_show  # noqa: E402
from funding_top10.slack_message import build_message, post_to_slack  # noqa: E402

logger = logging.getLogger(__name__)

BEIJING_TZ = datetime.timezone(datetime.timedelta(hours=8))




def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    proxy_repr = cfg.proxy or "(none)"

    logger.info("Fetching funding/OI from Binance API (proxy=%s)…", proxy_repr)
    funding_df = fetch_funding_dataframe(proxy=cfg.proxy)
    logger.info("Got %d BINANCE-U USDT-perp rows from Binance API", len(funding_df))

    logger.info("Fetching biyi tickers from %s (query=%r)…", cfg.biyi.base_url, cfg.biyi.query)
    biyi = fetch_biyi_tickers_api(base_url=cfg.biyi.base_url, query=cfg.biyi.query)
    logger.info("Got %d biyi tickers from API", len(biyi))

    # Haircut: query DataHub for ALL Binance USDT-perp bases + biyi. The new
    # ranking by composite score needs every symbol's haircut (BTC/ETH have
    # high haircut but low funding so a top-50-by-funding cut would miss them).
    all_bases = set(funding_df["base"].astype(str).tolist())
    biyi_bases = {t.split("/")[0] for t in biyi if "/" in t}
    tokens_to_fetch = sorted(all_bases | biyi_bases)
    logger.info(
        "Fetching haircut for %d tokens from DataHub (all bases ∪ biyi)…",
        len(tokens_to_fetch),
    )
    datahub = DataHub(
        prefix=cfg.datahub.prefix,
        api_key=cfg.datahub.api_key,
        gateway_url=cfg.datahub.gateway_url,
        cache_directory=cfg.datahub.cache_dir or None,
    )
    haircuts = load_binance_haircuts(datahub, tokens_to_fetch)
    logger.info("Got %d haircut values from DataHub", len(haircuts))
    # Tokens DataHub doesn't return get haircut=0 (per user spec). This means
    # they fail the haircut>=0.5 filter and are excluded from the top-N pool,
    # but the value is still rendered as "0.00" in the table rather than "n/a".
    funding_df["haircut"] = funding_df["base"].astype(str).map(haircuts).fillna(0.0)

    # Visibility: log haircut for each biyi token so it's obvious in the cmd
    # console whether DataHub gave us a value or not (no log-file dig needed).
    for biyi_base in sorted(biyi_bases):
        in_dict = haircuts.get(biyi_base)
        row_value = funding_df.loc[funding_df["base"].astype(str) == biyi_base, "haircut"]
        in_row = float(row_value.iloc[0]) if not row_value.empty else None
        logger.info(
            "biyi haircut check — base=%s in_dict=%s in_df=%s",
            biyi_base, in_dict, in_row,
        )

    weights = ScoreWeights(
        apr7=cfg.score_weights.apr7,
        std=cfg.score_weights.std,
        haircut=cfg.score_weights.haircut,
        oi=cfg.score_weights.oi,
    )
    logger.info(
        "Filters: min_haircut=%.2f min_oi_usd=%s; score = annualized_apr - %.3f * annualized_std",
        cfg.filters.min_haircut, f"{cfg.filters.min_oi_usd:,.0f}", cfg.score.confidence_z,
    )
    merged = select_rows_to_show(
        funding_df,
        biyi,
        weights,
        min_haircut=cfg.filters.min_haircut,
        min_oi_usd=cfg.filters.min_oi_usd,
        confidence_z=cfg.score.confidence_z,
    )
    logger.info("Merged display set: %d rows (top by confidence-bound score ∪ biyi)", len(merged))

    today_beijing = datetime.datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    message = build_message(merged, biyi, report_date_str=today_beijing)

    logger.info("Posting to Slack (proxy=%s)…", proxy_repr)
    post_to_slack(cfg.slack.webhook, message, proxy=cfg.proxy)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
