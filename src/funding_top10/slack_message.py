"""Build and post the daily Slack report.

A single table merges the funding-stability Top 10 with biyi-strategy tickers,
sorted by sum_7d_funding_rate desc. Biyi rows are flagged with 🔴 in a
dedicated leading column so column alignment is not affected by emoji width.

Columns (in order):
  flag | exchange | symbol(base) | timestamp | int | funding(bp) |
  3d_apr% | 7d_apr% | std_7d_y% | OI | haircut | score | pos | pct%

- funding(bp):  raw funding_rate × 10000, no fixed decimals (Python ``:g``).
- 3d_apr% / 7d_apr%: annualized return from sum of funding over the last 3/7
  days: ``sum × 365 / N_days × 100``. This is INDEPENDENT of the per-symbol
  funding cadence (1h/4h/8h) because sum captures total funding paid.
- std_7d_y%: per-event funding stddev annualized to %, cadence-adjusted.
- pos / pct%: biyi position size (USD) and its share of total biyi position.
  Non-biyi rows leave these blank.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone, timedelta
from typing import Iterable

import httpx
import pandas as pd

from funding_top10.cache_util import DataSource, format_age_human

_BEIJING_TZ = timezone(timedelta(hours=8))


HIGHLIGHT = "🔴 "  # emoji + 1 ASCII space ≈ 3 monospace cells in Slack
NO_FLAG = "   "    # 3 ASCII spaces — same visual width as HIGHLIGHT
FLAG_HEADER = "   "  # blank header for the flag column


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


def _fmt_bp(x, digits: int | None = None) -> str:
    """raw × 10000, formatted as basis points (sign shown).

    - digits=None (default): use ``:g`` — variable decimals, trailing zeros
      stripped. Good for funding_rate where exact magnitude matters.
    - digits=N: fixed N decimal places via ``:+.Nf``. Used for std_7d where a
      stable column width is preferred.
    """
    if x is None:
        return "n/a"
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return "n/a"
    if math.isnan(xf):
        return "n/a"
    bp = xf * 10000
    if digits is None:
        return f"{bp:g}"
    return f"{bp:.{digits}f}"


def _fmt_apr(x, days: int) -> str:
    """Annualize a sum-of-funding over N days as +/-X.X%.

    APR = sum × 365 / days × 100. Funding-cadence-independent (sum captures
    total payments regardless of 1h/4h/8h cadence).
    """
    if x is None:
        return "n/a"
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return "n/a"
    if math.isnan(xf):
        return "n/a"
    return f"{xf * 365 / days * 100:.1f}%"


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
    """Format an epoch timestamp as 'MM-DD HH:MM' in Asia/Shanghai.

    Auto-detects the unit (seconds / milliseconds / microseconds) based on
    magnitude so the function works regardless of which form the DB returns.
    Returns 'n/a' on any failure.
    """
    if ts is None:
        return "n/a"
    if isinstance(ts, float) and math.isnan(ts):
        return "n/a"

    try:
        val = int(ts)
    except (TypeError, ValueError):
        # Already a Timestamp-like? Try direct conversion.
        try:
            dt = pd.Timestamp(ts)
            if dt.tz is None:
                dt = dt.tz_localize("UTC")
            return dt.tz_convert("Asia/Shanghai").strftime("%m-%d %H:%M")
        except Exception:
            return "n/a"

    if val > 10**15:
        unit = "us"
    elif val > 10**12:
        unit = "ms"
    elif val > 10**9:
        unit = "s"
    else:
        return "n/a"

    try:
        dt = pd.Timestamp(val, unit=unit, tz="UTC").tz_convert("Asia/Shanghai")
        return dt.strftime("%m-%d %H:%M")
    except Exception:
        return "n/a"


def _fmt_pct_value(x, digits: int = 1) -> str:
    """Format a decimal as +/-X.X% (e.g. 0.123 -> '12.3%'). Negatives carry '-'."""
    if x is None:
        return "n/a"
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return "n/a"
    if math.isnan(xf):
        return "n/a"
    return f"{xf * 100:.{digits}f}%"


def _fmt_score(x) -> str:
    """Score is the annualized confidence-bound (a decimal rate). Display as %."""
    if x is None:
        return "n/a"
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return "n/a"
    if math.isnan(xf):
        return "n/a"
    return f"{xf * 100:.1f}%"


def _fmt_interval(h) -> str:
    if h is None:
        return "n/a"
    try:
        hi = int(h)
    except (TypeError, ValueError):
        return "n/a"
    return f"{hi}h"


_BODY_FMT = (
    "{flag}{exchange:<10s} {symbol:<16s} {ts:<11s} {fint:>4s} "
    "{fr:>11s} {apr3:>9s} {apr7:>9s} {sy:>11s} {oi:>12s} {hc:>9s} {sc:>7s} "
    "{pos:>10s} {pct:>7s}"
)


def _header_line() -> str:
    return _BODY_FMT.format(
        flag=FLAG_HEADER,
        exchange="exchange",
        symbol="symbol",
        ts="timestamp",
        fint="int",
        fr="funding(bp)",
        apr3="3d_apr%",
        apr7="7d_apr%",
        sy="std_7d_y%",
        oi="OI",
        hc="haircut",
        sc="score",
        pos="pos",
        pct="pct%",
    )


def _row_line(
    row: pd.Series,
    biyi_set: set[str],
    position_by_ticker: dict[str, float],
    total_position_usd: float,
) -> str:
    base = row.get("base") if "base" in row else None
    quote = row.get("quote") if "quote" in row else None
    ticker = f"{base}/{quote}" if base is not None and quote is not None else ""
    is_biyi = ticker in biyi_set
    flag = HIGHLIGHT if is_biyi else NO_FLAG

    if base is not None and quote is not None:
        symbol_display = ticker[:16]
    else:
        symbol_display = str(row.get("symbol") or "n/a")[:16]

    if is_biyi:
        pos_usd = position_by_ticker.get(ticker, 0.0)
        pos_str = _fmt_human_usd(pos_usd)
        pct_str = (
            _fmt_pct_value(pos_usd / total_position_usd, 1)
            if total_position_usd > 0
            else "n/a"
        )
    else:
        pos_str = ""
        pct_str = ""

    return _BODY_FMT.format(
        flag=flag,
        exchange=str(row.get("exchange") or "n/a")[:10],
        symbol=symbol_display,
        ts=_fmt_timestamp_bj(row.get("timestamp")),
        fint=_fmt_interval(row.get("funding_interval_hours")),
        fr=_fmt_bp(row.get("funding_rate")),
        apr3=_fmt_apr(row.get("sum_3d_funding_rate"), 3),
        apr7=_fmt_apr(row.get("sum_7d_funding_rate"), 7),
        sy=_fmt_pct_value(row.get("std_7d_annualized"), 1),
        oi=_fmt_human_usd(row.get("open_interest_value")),
        hc=_fmt_float(row.get("haircut"), 2),
        sc=_fmt_score(row.get("score")),
        pos=pos_str,
        pct=pct_str,
    )


def _format_source_line(label: str, src: DataSource | None) -> str | None:
    """Single 'biyi: live' / 'biyi: cached 05-14 08:01 (1d 2h ago)' line.

    Returns ``None`` if src is missing — caller filters those out. ``None`` for
    src effectively means "the source wasn't even attempted this run", which
    shouldn't normally happen.
    """
    if src is None:
        return None
    if src.kind == "live":
        return f"{label}: live"
    if src.kind == "cache":
        if src.saved_at_ms:
            bj = datetime.fromtimestamp(src.saved_at_ms / 1000, tz=_BEIJING_TZ)
            ts = bj.strftime("%Y-%m-%d %H:%M")
            return f"{label}: cached from {ts} ({format_age_human(src.saved_at_ms)})"
        return f"{label}: cached (unknown age)"
    # "none"
    return f"{label}: unavailable"


def build_message(
    rows_df: pd.DataFrame,
    biyi_tickers: Iterable[str],
    report_date_str: str,
    *,
    position_by_ticker: dict[str, float] | None = None,
    total_position_usd: float = 0.0,
    data_sources: dict[str, DataSource] | None = None,
) -> str:
    """Render the merged Top10 + biyi table for Slack.

    ``position_by_ticker`` maps biyi ticker -> position_usd (sum of
    maxPositionQty across that ticker's strategies). ``total_position_usd`` is
    the denominator used for the per-row pct% column. Both default to empty
    so callers / tests written before the pos/pct columns existed still work.

    ``data_sources`` is an optional mapping of {"funding"|"biyi"|"haircut" ->
    DataSource}. When provided, a footer is appended listing where each piece
    of data came from and how stale a cache fallback is.
    """
    biyi_set = set(biyi_tickers)
    pos_map = position_by_ticker or {}
    lines: list[str] = [
        f"*Funding Score Top N ∪ Biyi (BINANCE-U) — {report_date_str}*",
        "```",
        _header_line(),
    ]
    for _, row in rows_df.iterrows():
        lines.append(_row_line(row, biyi_set, pos_map, total_position_usd))
    lines.append("```")

    if biyi_set:
        lines.append("")
        lines.append(f"_Biyi tickers (🔴): {', '.join(sorted(biyi_set))}_")

    if data_sources:
        # Stable order: funding → biyi → haircut. Skip any source not present.
        order = (("funding", "funding/OI"), ("biyi", "biyi"), ("haircut", "haircut"))
        source_lines: list[str] = []
        for key, label in order:
            line = _format_source_line(label, data_sources.get(key))
            if line is not None:
                source_lines.append(line)
        if source_lines:
            lines.append("")
            lines.append("_data sources (Beijing time):_")
            for sl in source_lines:
                lines.append(f"_  • {sl}_")

    return "\n".join(lines)


_TRANSIENT_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.ReadError,
    httpx.RemoteProtocolError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
)


def _post_once(webhook_url: str, message: str, *, proxy: str,
               timeout: float, max_retries: int, _logger) -> None:
    """One transport (proxy or direct) with exponential-backoff retries."""
    import time as _time

    client_kwargs: dict = {"trust_env": False}
    if proxy:
        client_kwargs["proxy"] = proxy
    label = f"proxy={proxy}" if proxy else "direct"

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            with httpx.Client(**client_kwargs) as client:
                resp = client.post(webhook_url, json={"text": message}, timeout=timeout)
                resp.raise_for_status()
                return
        except _TRANSIENT_EXCEPTIONS as e:
            last_exc = e
            if attempt < max_retries:
                wait = 2 ** attempt
                _logger.warning(
                    "Slack POST (%s) attempt %d failed (%s); retrying in %ds",
                    label, attempt + 1, type(e).__name__, wait,
                )
                _time.sleep(wait)
                continue
            raise
    if last_exc:
        raise last_exc


def post_to_slack(webhook_url: str, message: str, *, proxy: str = "",
                  timeout: float = 15.0, max_retries: int = 3) -> None:
    """POST a plain-text message to a Slack incoming webhook. Raises on non-2xx.

    Path order:
      1. If ``proxy`` is set, try it (with ``max_retries`` retries).
      2. On transient transport failure of step 1, fall back to **direct** (no
         proxy) with one retry attempt. Critical when the local proxy itself is
         dead — failure notices still get out.
      3. If ``proxy`` is empty, only direct is tried.

    trust_env=False everywhere, so ambient HTTP_PROXY env vars are NOT consulted
    — the proxy comes only from config.yaml.
    """
    import logging

    logger = logging.getLogger(__name__)

    if not proxy:
        _post_once(webhook_url, message, proxy="",
                   timeout=timeout, max_retries=max_retries, _logger=logger)
        return

    try:
        _post_once(webhook_url, message, proxy=proxy,
                   timeout=timeout, max_retries=max_retries, _logger=logger)
        return
    except _TRANSIENT_EXCEPTIONS as e:
        logger.warning(
            "Slack POST via proxy=%s failed (%s); falling back to direct",
            proxy, type(e).__name__,
        )

    # Direct as last resort. One attempt is enough — if direct is also broken
    # the network is completely down.
    _post_once(webhook_url, message, proxy="",
               timeout=timeout, max_retries=1, _logger=logger)
