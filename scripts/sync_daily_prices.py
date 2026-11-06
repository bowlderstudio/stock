"""Flow B: incremental daily price sync for all backfilled instruments (run locally).

    uv run python scripts/sync_daily_prices.py            # skipped on non-trading days
    uv run python scripts/sync_daily_prices.py --force    # run even if today is not a session

Scheduled runs go through the Prefect flow `sync_daily_prices` (Europe/Oslo, after close).
"""

import argparse
import random
import time
import uuid
from datetime import date

from stockdata import db
from stockdata.config import get_settings
from stockdata.pipelines.prices import sync_daily_prices
from stockdata.sources.yahoo import download_prices, fetch_history
from stockdata.trading_calendar import last_closed_session


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--force", action="store_true", help="ignore the trading-day check")
    args = parser.parse_args()
    s = get_settings()
    last_closed = last_closed_session()
    today = last_closed if args.force else date.today()

    def download(symbols, start):
        t0 = time.monotonic()
        frames = download_prices(symbols, start)
        print(f"  batch from {start}: {len(frames)}/{len(symbols)} symbols "
              f"in {time.monotonic() - t0:.1f}s", flush=True)  # fmt: skip
        return frames

    run_id = uuid.uuid4()
    with db.connect() as conn:
        db.start_run(conn, run_id, "sync_daily_prices")
        conn.commit()
        counts = sync_daily_prices(
            conn, run_id, download, fetch_history,
            today=today, last_closed=last_closed, settings=s,
            pause=lambda: time.sleep(random.uniform(s.batch_delay_min_s, s.batch_delay_max_s)),
        )  # fmt: skip
        status = (
            "skipped" if counts["skipped"]
            else "rate_limited" if counts["rate_limited"]
            else "completed_with_failures" if counts["failed"]
            else "completed"
        )  # fmt: skip
        db.finish_run(
            conn, run_id, status,
            total_symbols=counts["symbols"], successful_symbols=counts["updated"],
            rows_upserted=counts["rows"],
            error_count=counts["failed"] + counts["quality_findings"],
        )  # fmt: skip
        conn.commit()
    print(f"run {run_id} ({status}, last_closed={last_closed}):", counts)
    return 0 if status in ("completed", "skipped") else 1


if __name__ == "__main__":
    raise SystemExit(main())
