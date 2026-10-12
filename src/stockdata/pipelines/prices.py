"""Flow B/C/D logic: daily incremental sync, backfill / full refetch, recent reconcile (§5)."""

from datetime import date


def sync_daily_prices() -> dict:
    raise NotImplementedError


def backfill_history(
    tickers: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    batch_size: int | None = None,
    refetch_pending: bool = False,
    dry_run: bool = False,
) -> dict:
    raise NotImplementedError


def reconcile_recent_prices() -> dict:
    raise NotImplementedError
