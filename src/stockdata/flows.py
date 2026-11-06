"""Prefect flow entrypoints. Keep these thin: logic lives in stockdata.pipelines.

Hobby plan allows 5 deployments; these 3 use 3 of them. sync_instruments is not a Prefect
flow: Euronext terms require a manual download, so it runs locally
(scripts/import_euronext_list.py).
"""

import random
import time
import uuid
from datetime import date

from prefect import flow

from stockdata import db
from stockdata.config import get_settings
from stockdata.pipelines import prices
from stockdata.sources.yahoo import download_prices, fetch_history
from stockdata.trading_calendar import last_closed_session


@flow(name="sync_daily_prices")
def sync_daily_prices_flow() -> dict:
    s = get_settings()
    run_id = uuid.uuid4()
    with db.connect() as conn:
        db.start_run(conn, run_id, "sync_daily_prices")
        conn.commit()
        counts = prices.sync_daily_prices(
            conn, run_id, download_prices, fetch_history,
            today=date.today(), last_closed=last_closed_session(), settings=s,
            pause=lambda: time.sleep(random.uniform(s.batch_delay_min_s, s.batch_delay_max_s)),
        )  # fmt: skip
        db.finish_run(
            conn, run_id, "skipped" if counts["skipped"] else "completed",
            total_symbols=counts["symbols"], successful_symbols=counts["updated"],
            rows_upserted=counts["rows"], error_count=counts["failed"],
        )  # fmt: skip
        conn.commit()
    return counts


@flow(name="backfill_history")
def backfill_history_flow(
    tickers: list[str] | None = None,
    start: date | None = None,
    refetch_pending: bool = False,
) -> dict:
    s = get_settings()
    run_id = uuid.uuid4()
    with db.connect() as conn:
        db.start_run(conn, run_id, "backfill_history")
        conn.commit()
        counts = prices.backfill_history(
            conn, run_id, fetch_history,
            start=start or date.today().replace(year=date.today().year - s.backfill_years),
            last_closed=last_closed_session(), tickers=tickers,
            refetch_pending=refetch_pending, max_seconds=s.max_flow_seconds,
            pause=lambda: time.sleep(random.uniform(s.batch_delay_min_s, s.batch_delay_max_s)),
        )  # fmt: skip
        db.finish_run(
            conn, run_id, "completed" if not counts["remaining"] else "partial",
            successful_symbols=counts["done"], rows_upserted=counts["rows"],
        )  # fmt: skip
        conn.commit()
    return counts


@flow(name="reconcile_recent_prices")
def reconcile_recent_prices_flow() -> dict:
    return prices.reconcile_recent_prices()
