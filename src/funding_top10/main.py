"""Daily entry point: fetch -> rank -> format -> post to Slack."""

from __future__ import annotations

# Bootstrap: ensure src/ is on sys.path so `python src/funding_top10/main.py`
# (script-path invocation) works the same as `python -m funding_top10.main`.
# Harmless when the package was already installed via `pip install -e .`.
import sys
from pathlib import Path

_SRC_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _SRC_DIR.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

# Cache files (git-ignored). Each pipeline source has its own envelope file
# under <project_root>/cache/.
_CACHE_DIR = _PROJECT_ROOT / "cache"
_FUNDING_CACHE_PATH = _CACHE_DIR / "funding.json"
_BIYI_CACHE_PATH = _CACHE_DIR / "biyi_positions.json"
_HAIRCUT_CACHE_PATH = _CACHE_DIR / "haircuts.json"

import datetime  # noqa: E402
import logging  # noqa: E402
import socket  # noqa: E402
import traceback  # noqa: E402

import pandas as pd  # noqa: E402

from funding_top10.binance_api import fetch_funding_dataframe_with_cache  # noqa: E402
from funding_top10.biyi_api import fetch_biyi_positions_with_cache  # noqa: E402
from funding_top10.config import load_config  # noqa: E402
from funding_top10.datahub import DataHub, load_haircuts_with_cache  # noqa: E402
from funding_top10.scoring import ScoreWeights, select_rows_to_show  # noqa: E402
from funding_top10.slack_message import build_message, post_to_slack  # noqa: E402

logger = logging.getLogger(__name__)

BEIJING_TZ = datetime.timezone(datetime.timedelta(hours=8))

# Slack accepts up to ~40k chars per message; keep the traceback portion well
# under that so the "what failed" line + traceback fit comfortably.
_MAX_TRACEBACK_CHARS = 6000


def _notify_failure(cfg, exc: BaseException) -> None:
    """Best-effort: send a failure notice to Slack. Never raises."""
    tb = traceback.format_exc()
    if len(tb) > _MAX_TRACEBACK_CHARS:
        tb = "…(truncated)…\n" + tb[-_MAX_TRACEBACK_CHARS:]
    host = socket.gethostname()
    now = datetime.datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        f":rotating_light: *funding-top10 跑挂了* — {now} (host: `{host}`)\n"
        f"`{type(exc).__name__}: {exc}`\n"
        f"```\n{tb}\n```"
    )
    try:
        post_to_slack(cfg.slack.webhook, msg, proxy=cfg.proxy)
    except Exception:
        logger.exception("Failed to deliver failure notice to Slack")


def _run_pipeline(cfg) -> None:
    proxy_repr = cfg.proxy or "(none)"

    logger.info("Fetching funding/OI from Binance API (proxy=%s)…", proxy_repr)
    funding_df, funding_src = fetch_funding_dataframe_with_cache(
        _FUNDING_CACHE_PATH, proxy=cfg.proxy,
    )
    logger.info(
        "Got %d BINANCE-U USDT-perp rows (source=%s)",
        len(funding_df), funding_src.kind,
    )
    if funding_df.empty:
        # Without funding/OI there's nothing meaningful to score or render.
        raise RuntimeError(
            "No funding data available (live fetch failed and no cache). "
            "Cannot continue."
        )

    logger.info("Fetching biyi positions from %s (query=%r)…", cfg.biyi.base_url, cfg.biyi.query)
    biyi_positions, biyi_src = fetch_biyi_positions_with_cache(
        _BIYI_CACHE_PATH,
        base_url=cfg.biyi.base_url,
        query=cfg.biyi.query,
    )
    biyi = [p["ticker"] for p in biyi_positions]
    position_by_ticker = {p["ticker"]: float(p["position_usd"]) for p in biyi_positions}
    total_position_usd = sum(position_by_ticker.values())
    logger.info(
        "Got %d biyi tickers (source=%s, total position_usd=%.2f)",
        len(biyi), biyi_src.kind, total_position_usd,
    )
    for p in biyi_positions:
        pct = (p["position_usd"] / total_position_usd) if total_position_usd > 0 else 0.0
        logger.info("  biyi %-16s  pos=%.2f  pct=%.2f%%", p["ticker"], p["position_usd"], pct * 100)

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
    try:
        datahub = DataHub(
            prefix=cfg.datahub.prefix,
            api_key=cfg.datahub.api_key,
            gateway_url=cfg.datahub.gateway_url,
            cache_directory=cfg.datahub.cache_dir or None,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("DataHub init failed (%s); will try haircut cache", e)
        datahub = None
    haircuts, haircut_src = load_haircuts_with_cache(
        _HAIRCUT_CACHE_PATH,
        datahub=datahub,
        tokens=tokens_to_fetch,
    )
    logger.info(
        "Got %d haircut values (source=%s)",
        len(haircuts), haircut_src.kind,
    )
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
    message = build_message(
        merged,
        biyi,
        report_date_str=today_beijing,
        position_by_ticker=position_by_ticker,
        total_position_usd=total_position_usd,
        data_sources={
            "funding": funding_src,
            "biyi": biyi_src,
            "haircut": haircut_src,
        },
    )

    logger.info("Posting to Slack (proxy=%s)…", proxy_repr)
    post_to_slack(cfg.slack.webhook, message, proxy=cfg.proxy)
    logger.info("Done.")


def main() -> int:
    """Entry point. Wraps the pipeline so any failure is reported to Slack
    rather than killing the run silently from Task Scheduler's perspective."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    try:
        _run_pipeline(cfg)
        return 0
    except KeyboardInterrupt:
        # Manual cancel — don't spam Slack with a "failure" notice.
        raise
    except Exception:
        logger.exception("Pipeline failed")
        _notify_failure(cfg, sys.exc_info()[1])
        return 1


if __name__ == "__main__":
    sys.exit(main())
