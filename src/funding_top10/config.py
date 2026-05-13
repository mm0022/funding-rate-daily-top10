"""Load configuration from config.yaml at the project root.

Required fields:
  qijia: host, port, user, password, database
  slack: webhook

Optional:
  slack: channel  (defaults to "")
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

import yaml


@dataclass(frozen=True)
class QijiaConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    def to_dsn(self) -> str:
        """Build the sqlalchemy DSN, URL-encoding user and password for safety."""
        user = quote_plus(self.user)
        pwd = quote_plus(self.password)
        return f"postgresql+psycopg2://{user}:{pwd}@{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class SlackConfig:
    webhook: str
    channel: str


@dataclass(frozen=True)
class BinanceConfig:
    api_key: str
    api_secret: str


@dataclass(frozen=True)
class Config:
    qijia: QijiaConfig
    slack: SlackConfig
    binance: BinanceConfig


_REQUIRED_QIJIA = ("host", "port", "user", "password", "database")


def load_config(path: Path | None = None) -> Config:
    """Load and validate config.yaml. Raises on missing file or required field."""
    if path is None:
        path = Path(__file__).resolve().parents[2] / "config.yaml"

    if not path.exists():
        raise FileNotFoundError(
            f"config.yaml not found at {path}. "
            "Copy config.yaml.example to config.yaml and fill in real values."
        )

    # Explicit utf-8: on Windows the default locale encoding is cp936/gbk, which
    # blows up on any non-gbk byte (Chinese comments, em-dashes, emoji…).
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    qijia_raw = raw.get("qijia") or {}
    slack_raw = raw.get("slack") or {}
    binance_raw = raw.get("binance") or {}

    missing_qijia = [k for k in _REQUIRED_QIJIA if not qijia_raw.get(k)]
    if missing_qijia:
        raise RuntimeError(
            f"config.yaml: missing required qijia fields: {', '.join(missing_qijia)}"
        )

    if not slack_raw.get("webhook"):
        raise RuntimeError("config.yaml: missing required slack.webhook")

    return Config(
        qijia=QijiaConfig(
            host=str(qijia_raw["host"]),
            port=int(qijia_raw["port"]),
            user=str(qijia_raw["user"]),
            password=str(qijia_raw["password"]),
            database=str(qijia_raw["database"]),
        ),
        slack=SlackConfig(
            webhook=str(slack_raw["webhook"]),
            channel=str(slack_raw.get("channel") or ""),
        ),
        binance=BinanceConfig(
            api_key=str(binance_raw.get("api_key") or ""),
            api_secret=str(binance_raw.get("api_secret") or ""),
        ),
    )
