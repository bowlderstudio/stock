"""Runtime settings, read from environment variables (prefix STOCKDATA_) or a local .env file.

Defaults are the initial engineering values from the design doc (§5, §7, §8); they are not
Yahoo or vendor guarantees and are expected to be tuned from monitoring.
"""

from functools import lru_cache
from urllib.parse import urlsplit, urlunsplit

from pydantic import AliasChoices, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _to_libpq_url(url: str) -> str:
    """Accept SQLAlchemy-style URLs (postgresql+asyncpg://...?ssl=require) for psycopg."""
    parts = urlsplit(url)
    scheme = parts.scheme.split("+", 1)[0]
    query = "&".join(
        "sslmode=" + kv.split("=", 1)[1] if kv.startswith("ssl=") else kv
        for kv in parts.query.split("&")
        if kv
    )
    return urlunsplit((scheme, parts.netloc, parts.path, query, parts.fragment))


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STOCKDATA_", env_file=".env", extra="ignore")

    # PostgreSQL connection string, write role only. STOCKDATA_DB_URL wins over DATABASE_URL.
    db_url: SecretStr | None = Field(
        None, validation_alias=AliasChoices("STOCKDATA_DB_URL", "DATABASE_URL")
    )

    # Yahoo request pacing (§8)
    batch_size: int = 25
    concurrency: int = 2
    batch_delay_min_s: float = 2.0
    batch_delay_max_s: float = 5.0
    retry_backoff_s: tuple[int, ...] = (15, 30, 60, 120)

    # Incremental sync and backfill (§5)
    lookback_trading_days: int = 10
    reconcile_trading_days: int = 60
    backfill_years: int = 10
    max_flow_seconds: int = 600

    # Data quality (§7)
    split_anomaly_window_days: int = 3
    split_anomaly_tolerance: float = Field(0.15, gt=0, lt=1)
    data_gap_trading_days: int = 10

    exchange_calendar: str = "XOSL"
    timezone: str = "Europe/Oslo"

    @field_validator("db_url", mode="before")
    @classmethod
    def _normalize_db_url(cls, v):
        if v is None:
            return v
        raw = v.get_secret_value() if isinstance(v, SecretStr) else v
        return _to_libpq_url(raw)


@lru_cache
def get_settings() -> Settings:
    return Settings()
