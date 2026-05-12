"""Build and post the daily Slack report.

A single table merges the funding-stability Top 10 with biyi-strategy tickers,
sorted by mean_7d_funding_rate desc. Biyi rows are prefixed with 🔴.

Columns (in order):
  exchange | symbol | timestamp | funding_rate | mean_3d | mean_7d | std_7d | OI | haircut
"""

from __future__ import annotations

import math
from typing import Iterable

import httpx
import pandas as pd


HIGHLIGHT = "🔴"
NO_HIGHLIGHT = "  "  # two ASCII spaces — visual width approximates the emoji cell


def _fmt_float(x, digits: int) -> str:
    if x is None:
        return "n/a"
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return "n/a"
    if math.isnan(xf):
        return "n/a"
    return f"{xf:.{digits}f}"


def _fmt_human_usd(x) -> str:
    """Format a USD value with K/M/B suffix. Returns 'n/a' for NaN/None."""
    if x is None:
        return "n/a"
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return "n/a"
    if math.isnan(xf):
        return "n/a"
    a = abs(xf)
    if a >= 1e9:
        return f"{xf / 1e9:.2f}B"
    if a >= 1e6:
        return f"{xf / 1e6:.2f}M"
    if a >= 1e3:
        return f"{xf / 1e3:.0f}K"
    return f"{xf:.0f}"


def _fmt_timestamp_bj(ts) -> str:
    """Format epoch-ms timestamp as 'MM-DD HH:MM' in Asia/Shanghai. 'n/a' on failure."""
    if ts is None:
        return "n/a"
    if isinstance(ts, float) and math.isnan(ts):
        return "n/a"
    try:
        dt = pd.Timestamp(int(ts), unit="ms", tz="UTC").tz_convert("Asia/Shanghai")
        return dt.strftime("%m-%d %H:%M")
    except Exception:
        return "n/a"


_HEADER_FMT = (
    "{prefix:<2s} {exchange:<10s} {symbol:<12s} {ts:<11s} "
    "{fr:>10s} {m3:>10s} {m7:>10s} {s7:>10s} {oi:>8s} {hc:>8s}"
)


def _header_line() -> str:
    return _HEADER_FMT.format(
        prefix="",
        exchange="exchange",
        symbol="symbol",
        ts="timestamp",
        fr="funding",
        m3="mean_3d",
        m7="mean_7d",
        s7="std_7d",
        oi="OI",
        hc="haircut",
    )


def _row_line(row: pd.Series, biyi_set: set[str]) -> str:
    base = row.get("base") if "base" in row else None
    quote = row.get("quote") if "quote" in row else None
    ticker = f"{base}/{quote}" if base is not None and quote is not None else ""
    prefix = HIGHLIGHT if ticker in biyi_set else NO_HIGHLIGHT
    return _HEADER_FMT.format(
        prefix=prefix,
        exchange=str(row.get("exchange") or "n/a")[:10],
        symbol=str(row.get("symbol") or "n/a")[:12],
        ts=_fmt_timestamp_bj(row.get("timestamp")),
        fr=_fmt_float(row.get("funding_rate"), 6),
        m3=_fmt_float(row.get("mean_3d_funding_rate"), 6),
        m7=_fmt_float(row.get("mean_7d_funding_rate"), 6),
        s7=_fmt_float(row.get("std_7d_funding_rate"), 6),
        oi=_fmt_human_usd(row.get("open_interest_value")),
        hc=_fmt_float(row.get("haircut"), 4),
    )


def build_message(
    rows_df: pd.DataFrame,
    biyi_tickers: Iterable[str],
    report_date_str: str,
) -> str:
    """Render the merged Top10 + biyi table for Slack.

    Args:
        rows_df:        the merged rows to display (already sorted as desired).
        biyi_tickers:   iterable of "BASE/QUOTE" strings — used to mark rows 🔴.
        report_date_str: e.g. "2026-05-12".
    """
    biyi_set = set(biyi_tickers)
    lines: list[str] = [
        f"*Funding Top 10 ∪ Biyi (BINANCE-U) — {report_date_str}*",
        "```",
        _header_line(),
    ]
    for _, row in rows_df.iterrows():
        lines.append(_row_line(row, biyi_set))
    lines.append("```")

    if biyi_set:
        lines.append("")
        lines.append(f"_Biyi tickers (🔴): {', '.join(sorted(biyi_set))}_")

    return "\n".join(lines)


def post_to_slack(webhook_url: str, message: str, *, timeout: float = 15.0) -> None:
    """POST a plain-text message to a Slack incoming webhook. Raises on non-2xx."""
    resp = httpx.post(webhook_url, json={"text": message}, timeout=timeout)
    resp.raise_for_status()
