"""Runtime settings, read from environment variables (prefix STOCKDATA_) or a local .env file.

Defaults are the initial engineering values from the design doc (§5, §7, §8); they are not
Yahoo or vendor guarantees and are expected to be tuned from monitoring.
"""

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STOCKDATA_", env_file=".env", extra="ignore")

    # Supabase pooler connection string (not the IPv6-only direct host), write role only.
    db_url: SecretStr | None = None

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


@lru_cache
def get_settings() -> Settings:
    return Settings()
