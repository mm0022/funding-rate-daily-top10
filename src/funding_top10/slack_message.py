"""Build and post the daily Slack report.

A single table merges the funding-stability Top 10 with biyi-strategy tickers,
sorted by sum_7d_funding_rate desc. Biyi rows are flagged with 🔴 in a
dedicated leading column so column alignment is not affected by emoji width.

Columns (in order):
  flag | exchange | symbol(base) | timestamp | funding(bp) |
  3d_apr% | 7d_apr% | std_7d(bp) | OI | haircut

- funding(bp):  raw funding_rate × 10000, no fixed decimals (Python ``:g``).
- 3d_apr% / 7d_apr%: annualized return from sum of funding over the last 3/7
  days: ``sum × 365 / N_days × 100``. This is INDEPENDENT of the per-symbol
  funding cadence (1h/4h/8h) because sum captures total funding paid.
- std_7d(bp):  per-event funding stddev × 10000. Not annualized — its scale
  depends on the funding cadence, so it's a within-symbol indicator only.
"""

from __future__ import annotations

import math
from typing import Iterable

import httpx
import pandas as pd


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


_BODY_FMT = (
    "{flag}{exchange:<10s} {symbol:<16s} {ts:<11s} "
    "{fr:>11s} {apr3:>9s} {apr7:>9s} {s7:>10s} {oi:>12s} {hc:>10s}"
)


def _header_line() -> str:
    return _BODY_FMT.format(
        flag=FLAG_HEADER,
        exchange="exchange",
        symbol="symbol",
        ts="timestamp",
        fr="funding(bp)",
        apr3="3d_apr%",
        apr7="7d_apr%",
        s7="std_7d(bp)",
        oi="OI",
        hc="haircut",
    )


def _row_line(row: pd.Series, biyi_set: set[str]) -> str:
    base = row.get("base") if "base" in row else None
    quote = row.get("quote") if "quote" in row else None
    ticker = f"{base}/{quote}" if base is not None and quote is not None else ""
    flag = HIGHLIGHT if ticker in biyi_set else NO_FLAG

    # symbol column shows the BASE/QUOTE pair (e.g. "BTC/USDT", "BTC/USDC"),
    # not the raw exchange symbol code "BTCUSDT".
    if base is not None and quote is not None:
        symbol_display = ticker[:16]
    else:
        symbol_display = str(row.get("symbol") or "n/a")[:16]

    return _BODY_FMT.format(
        flag=flag,
        exchange=str(row.get("exchange") or "n/a")[:10],
        symbol=symbol_display,
        ts=_fmt_timestamp_bj(row.get("timestamp")),
        fr=_fmt_bp(row.get("funding_rate")),
        apr3=_fmt_apr(row.get("sum_3d_funding_rate"), 3),
        apr7=_fmt_apr(row.get("sum_7d_funding_rate"), 7),
        s7=_fmt_bp(row.get("std_7d_funding_rate"), digits=3),
        oi=_fmt_human_usd(row.get("open_interest_value")),
        hc=_fmt_float(row.get("haircut"), 2),
    )


def build_message(
    rows_df: pd.DataFrame,
    biyi_tickers: Iterable[str],
    report_date_str: str,
) -> str:
    """Render the merged Top10 + biyi table for Slack."""
    biyi_set = set(biyi_tickers)
    lines: list[str] = [
        f"*Funding Top 20 ∪ Biyi (BINANCE-U) — {report_date_str}*",
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


def post_to_slack(webhook_url: str, message: str, *, proxy: str = "",
                  timeout: float = 15.0, max_retries: int = 3) -> None:
    """POST a plain-text message to a Slack incoming webhook. Raises on non-2xx.

    proxy: if non-empty, the POST routes through this URL. trust_env is False,
    so ambient HTTP_PROXY env vars are NOT consulted — the proxy comes only
    from config.yaml.

    Retries up to ``max_retries`` times on transient connection errors
    (proxy / Slack closing the connection mid-flight, etc.) with simple
    exponential back-off.
    """
    import logging
    import time as _time

    logger = logging.getLogger(__name__)

    client_kwargs: dict = {"trust_env": False}
    if proxy:
        client_kwargs["proxy"] = proxy

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            with httpx.Client(**client_kwargs) as client:
                resp = client.post(webhook_url, json={"text": message}, timeout=timeout)
                resp.raise_for_status()
                return
        except (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError,
                httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            last_exc = e
            if attempt < max_retries:
                wait = 2 ** attempt
                logger.warning(
                    "Slack POST attempt %d failed (%s); retrying in %ds",
                    attempt + 1, type(e).__name__, wait,
                )
                _time.sleep(wait)
                continue
            raise
    if last_exc:
        raise last_exc
