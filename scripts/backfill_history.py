"""Backfill full daily history for pending instruments (Flow C, run locally or via Prefect).

    uv run python scripts/backfill_history.py                      # all pending, no time limit
    uv run python scripts/backfill_history.py --max-minutes 10     # stop starting new ones after 10 min
    uv run python scripts/backfill_history.py --tickers EQNR.OL,DNB.OL   # force these
    uv run python scripts/backfill_history.py --refetch-pending    # also split/dividend refetches
    uv run python scripts/backfill_history.py --dry-run            # list what would be fetched

Commits after each instrument; safe to interrupt and re-run.
"""

import argparse
import random
import time
import uuid
from datetime import date

from stockdata import db
from stockdata.config import get_settings
from stockdata.pipelines.prices import backfill_history
from stockdata.sources.yahoo import fetch_history
from stockdata.trading_calendar import last_closed_session


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--tickers", help="comma-separated Yahoo symbols to force")
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--max-minutes", type=float)
    parser.add_argument("--refetch-pending", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    s = get_settings()
    tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
    start = args.start or date.today().replace(year=date.today().year - s.backfill_years)
    last_closed = last_closed_session()

    with db.connect() as conn:
        candidates = db.backfill_candidates(conn, tickers, args.refetch_pending)
        print(f"{len(candidates)} instruments to fetch, start={start}, last_closed={last_closed}")
        if args.dry_run:
            print(", ".join(sym for _, sym in candidates))
            return 0

        def fetch(symbol, start_):
            t0 = time.monotonic()
            df = fetch_history(symbol, start_)
            print(f"  {symbol:12} {len(df):5} rows  {time.monotonic() - t0:.1f}s", flush=True)
            return df

        run_id = uuid.uuid4()
        db.start_run(conn, run_id, "backfill_history")
        conn.commit()
        counts = backfill_history(
            conn, run_id, fetch,
            start=start, last_closed=last_closed, tickers=tickers,
            refetch_pending=args.refetch_pending,
            max_seconds=args.max_minutes * 60 if args.max_minutes else None,
            pause=lambda: time.sleep(random.uniform(s.batch_delay_min_s, s.batch_delay_max_s)),
        )  # fmt: skip
        status = (
            "rate_limited" if counts["rate_limited"]
            else "partial" if counts["remaining"] or counts["failed"]
            else "completed"
        )  # fmt: skip
        db.finish_run(
            conn, run_id, status,
            total_symbols=len(candidates), successful_symbols=counts["done"],
            rows_upserted=counts["rows"],
            error_count=counts["failed"] + counts["no_data"] + counts["quality_findings"],
        )  # fmt: skip
        conn.commit()
    print(f"run {run_id} ({status}):", counts)
    return 0 if status == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
