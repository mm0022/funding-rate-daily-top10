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
from sqlalchemy import create_engine, text  # noqa: E402

from funding_top10.binance_api import fetch_funding_dataframe  # noqa: E402
from funding_top10.config import load_config  # noqa: E402
from funding_top10.datahub import DataHub, load_binance_haircuts  # noqa: E402
from funding_top10.queries import biyi_tickers_sql  # noqa: E402
from funding_top10.scoring import TOP_X_BY_MEAN, select_rows_to_show  # noqa: E402
from funding_top10.slack_message import build_message, post_to_slack  # noqa: E402

logger = logging.getLogger(__name__)

BEIJING_TZ = datetime.timezone(datetime.timedelta(hours=8))


def fetch_biyi_tickers(engine, lookback_interval: str = "1 day") -> list[str]:
    with engine.connect() as conn:
        df = pd.read_sql_query(text(biyi_tickers_sql(lookback_interval)), conn)
    return df["ticker"].dropna().tolist()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    proxy_repr = cfg.proxy or "(none)"

    logger.info("Fetching funding/OI from Binance API (proxy=%s)…", proxy_repr)
    funding_df = fetch_funding_dataframe(
        cfg.binance.api_key, cfg.binance.api_secret, proxy=cfg.proxy
    )
    logger.info("Got %d BINANCE-U USDT-perp rows from Binance API", len(funding_df))

    # DB uses psycopg2 — it doesn't consult HTTP_PROXY env vars at all. We pass
    # nothing here; the DSN drives the TCP connection directly.
    engine = create_engine(cfg.qijia.to_dsn())
    biyi = fetch_biyi_tickers(engine)
    logger.info("Got %d biyi tickers from DB (last 24h)", len(biyi))

    # Haircut: only fetch for the tokens we'll actually display — top-X by
    # sum_7d (the haircut-filter input) plus biyi tokens. Avoids hammering
    # DataHub with 300+ requests when ~50 are enough.
    top_x_tokens = set(
        funding_df.nlargest(TOP_X_BY_MEAN, "sum_7d_funding_rate")["base"].astype(str).tolist()
    )
    biyi_bases = {t.split("/")[0] for t in biyi if "/" in t}
    tokens_to_fetch = sorted(top_x_tokens | biyi_bases)
    logger.info(
        "Fetching haircut for %d tokens from DataHub (top-%d ∪ biyi)…",
        len(tokens_to_fetch), TOP_X_BY_MEAN,
    )
    datahub = DataHub(
        prefix=cfg.datahub.prefix,
        api_key=cfg.datahub.api_key,
        gateway_url=cfg.datahub.gateway_url,
    )
    haircuts = load_binance_haircuts(datahub, tokens_to_fetch)
    logger.info("Got %d haircut values from DataHub", len(haircuts))
    # Tokens DataHub doesn't return get haircut=0 (per user spec). This means
    # they fail the haircut>=0.5 filter and are excluded from the top-N pool,
    # but the value is still rendered as "0.00" in the table rather than "n/a".
    funding_df["haircut"] = funding_df["base"].astype(str).map(haircuts).fillna(0.0)

    merged = select_rows_to_show(funding_df, biyi)
    logger.info("Merged display set: %d rows (top20 haircut>=0.5 ∪ biyi)", len(merged))

    today_beijing = datetime.datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    message = build_message(merged, biyi, report_date_str=today_beijing)

    logger.info("Posting to Slack (proxy=%s)…", proxy_repr)
    post_to_slack(cfg.slack.webhook, message, proxy=cfg.proxy)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
