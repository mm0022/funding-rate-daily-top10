"""Tiny on-disk cache: JSON envelope with a timestamp.

Used as a fallback when live data fetches fail. Each cache file looks like:

    {
      "saved_at_unix_ms": 1747200000000,
      "payload": <arbitrary JSON-compatible value>
    }

DataFrames are stored as records (list of dict): callers should convert
``df.to_json(orient="records")`` -> Python list before passing to write_cache,
and rebuild via ``pd.DataFrame(records)`` after read_cache.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)


class DataSource(NamedTuple):
    """Where a piece of data came from. Threaded into the Slack report so the
    operator can spot when a number is stale."""
    kind: str               # "live" | "cache" | "none"
    saved_at_ms: int | None  # ms timestamp recorded with the cache, or None


LIVE = DataSource(kind="live", saved_at_ms=None)
NONE = DataSource(kind="none", saved_at_ms=None)


def write_cache(path: Path | str, payload: Any) -> int:
    """Atomic write of {saved_at_unix_ms, payload} as JSON.

    Returns the saved_at_unix_ms used (caller may want to log it).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    saved_at = int(time.time() * 1000)
    envelope = {
        "saved_at_unix_ms": saved_at,
        "payload": payload,
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(envelope, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    os.replace(tmp, path)
    return saved_at


def read_cache(path: Path | str) -> tuple[Any, int] | None:
    """Return (payload, saved_at_unix_ms) or None on missing / corrupt."""
    path = Path(path)
    if not path.exists():
        return None
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to parse cache %s: %s", path, e)
        return None
    if not isinstance(envelope, dict) or "payload" not in envelope:
        return None
    try:
        saved_at = int(envelope.get("saved_at_unix_ms") or 0)
    except (TypeError, ValueError):
        saved_at = 0
    return envelope["payload"], saved_at


def format_age_human(saved_at_ms: int | None) -> str:
    """Return '12h ago' / '2d 3h ago' / 'unknown age'. For Slack footer use."""
    if not saved_at_ms:
        return "unknown age"
    now_ms = int(time.time() * 1000)
    seconds = max(0, (now_ms - saved_at_ms) / 1000)
    if seconds < 3600:
        return f"{int(seconds / 60)}m ago"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h ago"
    days = int(seconds / 86400)
    hours = int((seconds % 86400) / 3600)
    return f"{days}d {hours}h ago"
