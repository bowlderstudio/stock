"""Prefect flow entrypoints. Keep these thin: logic lives in stockdata.pipelines.

Hobby plan allows 5 deployments; these 4 use 4 of them.
"""

from datetime import date

from prefect import flow

from stockdata.pipelines import instruments, prices


@flow(name="sync_instruments")
def sync_instruments_flow() -> dict:
    return instruments.sync_instruments()


@flow(name="sync_daily_prices")
def sync_daily_prices_flow() -> dict:
    return prices.sync_daily_prices()


@flow(name="backfill_history")
def backfill_history_flow(
    tickers: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    batch_size: int | None = None,
    refetch_pending: bool = False,
    dry_run: bool = False,
) -> dict:
    return prices.backfill_history(tickers, start, end, batch_size, refetch_pending, dry_run)


@flow(name="reconcile_recent_prices")
def reconcile_recent_prices_flow() -> dict:
    return prices.reconcile_recent_prices()
