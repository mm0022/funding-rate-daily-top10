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
class DataHubConfig:
    prefix: str          # e.g. "CYBERX_PROD"
    api_key: str
    gateway_url: str     # e.g. "https://nexus.tyo.cyberx.com/nexus-data-hub-gateway/"
    cache_dir: str       # local cache path; "" → use DEFAULT_SDK_CACHE_DIR


@dataclass(frozen=True)
class ScoreWeightsConfig:
    """Deprecated; kept for config compatibility. See FiltersConfig."""
    apr7: float
    std: float
    haircut: float
    oi: float


@dataclass(frozen=True)
class FiltersConfig:
    min_haircut: float      # hard filter; rows below this are dropped
    min_oi_usd: float       # hard filter; OI in USD


@dataclass(frozen=True)
class ScoreConfig:
    confidence_z: float     # one-sided z-score. 1.0=84% / 1.645=95% / 2.0=97.7%


@dataclass(frozen=True)
class Config:
    qijia: QijiaConfig
    slack: SlackConfig
    datahub: DataHubConfig
    score_weights: ScoreWeightsConfig  # deprecated
    filters: FiltersConfig
    score: ScoreConfig
    proxy: str  # full URL, e.g. "http://proxy.host:8080"; empty disables proxy


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
    datahub_raw = raw.get("datahub") or {}
    score_raw = raw.get("score_weights") or {}
    filters_raw = raw.get("filters") or {}
    score_cfg_raw = raw.get("score") or {}
    proxy_raw = raw.get("proxy") or ""

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
        datahub=DataHubConfig(
            prefix=str(datahub_raw.get("prefix") or ""),
            api_key=str(datahub_raw.get("api_key") or ""),
            gateway_url=str(datahub_raw.get("gateway_url") or ""),
            cache_dir=str(datahub_raw.get("cache_dir") or ""),
        ),
        score_weights=ScoreWeightsConfig(
            apr7=float(score_raw.get("apr7", 0.4)),
            std=float(score_raw.get("std", 0.2)),
            haircut=float(score_raw.get("haircut", 0.2)),
            oi=float(score_raw.get("oi", 0.2)),
        ),
        filters=FiltersConfig(
            min_haircut=float(filters_raw.get("min_haircut", 0.5)),
            min_oi_usd=float(filters_raw.get("min_oi_usd", 5_000_000)),
        ),
        score=ScoreConfig(
            confidence_z=float(score_cfg_raw.get("confidence_z", 1.645)),
        ),
        proxy=str(proxy_raw or ""),
    )
