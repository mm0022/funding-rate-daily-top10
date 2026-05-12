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
import os  # noqa: E402
from contextlib import contextmanager  # noqa: E402

import pandas as pd  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402

from funding_top10.config import load_config  # noqa: E402
from funding_top10.queries import FUNDING_STATS_SQL, biyi_tickers_sql  # noqa: E402
from funding_top10.scoring import select_rows_to_show  # noqa: E402
from funding_top10.slack_message import build_message, post_to_slack  # noqa: E402

logger = logging.getLogger(__name__)

BEIJING_TZ = datetime.timezone(datetime.timedelta(hours=8))

# DB lives on the internal network and must NOT go through the corp HTTP proxy.
# Slack does need the proxy. The context manager unsets the proxy env vars only
# for the DB block; on exit they are restored so the Slack POST can use them.
_PROXY_KEYS = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
    "http_proxy", "https_proxy", "all_proxy",
)


@contextmanager
def proxy_off():
    saved = {k: os.environ.pop(k, None) for k in _PROXY_KEYS}
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


def fetch_funding_stats(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql_query(text(FUNDING_STATS_SQL), conn)
    # Defensive: drop any duplicate columns (e.g. if a future SQL change brings
    # back the historical duplicate mean_7d_funding_rate column).
    return df.loc[:, ~df.columns.duplicated()]


def fetch_biyi_tickers(engine, lookback_interval: str = "1 day") -> list[str]:
    with engine.connect() as conn:
        df = pd.read_sql_query(text(biyi_tickers_sql(lookback_interval)), conn)
    return df["ticker"].dropna().tolist()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()

    with proxy_off():
        engine = create_engine(cfg.qijia.to_dsn())

        logger.info("Fetching funding_stats (BINANCE-U) [proxy off]…")
        funding_df = fetch_funding_stats(engine)
        logger.info("Got %d funding_stats rows", len(funding_df))

        biyi = fetch_biyi_tickers(engine)
        logger.info("Got %d biyi tickers (last 24h)", len(biyi))

    merged = select_rows_to_show(funding_df, biyi)
    logger.info("Merged display set: %d rows (top10 haircut<=0.5 ∪ biyi)", len(merged))

    today_beijing = datetime.datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    message = build_message(merged, biyi, report_date_str=today_beijing)

    logger.info("Posting to Slack [proxy restored]…")
    post_to_slack(cfg.slack.webhook, message)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
