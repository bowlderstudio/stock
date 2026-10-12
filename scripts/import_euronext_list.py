"""Import a manually downloaded Euronext Oslo equities list (Flow A, run locally).

Download: https://live.euronext.com/en/markets/oslo/equities/list -> "Download" (Excel),
save under data/euronext/ (gitignored; personal use only). Then:

    uv run python scripts/import_euronext_list.py --dry-run     # parse only, no DB / Yahoo
    uv run python scripts/import_euronext_list.py               # newest file in data/euronext/
    uv run python scripts/import_euronext_list.py path/to/file.xlsx

New or not-yet-verified symbols are checked on Yahoo (one request each, paced), so the
first import of ~300 instruments takes several minutes.
"""

import argparse
import random
import sys
import time
import uuid
from collections import Counter
from pathlib import Path

from yfinance.exceptions import YFRateLimitError

from stockdata import db
from stockdata.config import get_settings
from stockdata.models import ErrorType
from stockdata.pipelines.instruments import sync_instruments
from stockdata.sources.euronext import parse_list
from stockdata.sources.yahoo import verify_symbol

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "euronext"


def paced_verifier():
    s = get_settings()
    state = {"n": 0}

    def verify(symbol: str):
        if state["n"]:
            time.sleep(random.uniform(s.batch_delay_min_s, s.batch_delay_max_s))
        state["n"] += 1
        for wait in (*s.retry_backoff_s, None):
            try:
                result = verify_symbol(symbol)
                print(f"  verify {symbol:12} -> {result}", flush=True)
                return result
            except YFRateLimitError:
                if wait is None:
                    raise
                print(f"  rate limited on {symbol}, retrying in {wait}s", flush=True)
                time.sleep(wait + random.uniform(0, wait / 4))

    return verify


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("file", nargs="?", type=Path)
    parser.add_argument("--dry-run", action="store_true", help="parse and summarize only")
    args = parser.parse_args()

    path = args.file or max(DATA_DIR.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, default=None)
    if path is None:
        print(f"no .xlsx file given and none found in {DATA_DIR}", file=sys.stderr)
        return 2
    listing = parse_list(path)
    print(f"{path.name}: list date {listing.as_of}, {len(listing.instruments)} instruments")
    print("  by market:", dict(Counter(i.market for i in listing.instruments)))
    print("  by currency:", dict(Counter(i.currency for i in listing.instruments)))
    if args.dry_run:
        return 0

    run_id = uuid.uuid4()
    with db.connect() as conn:
        db.start_run(conn, run_id, "sync_instruments")
        conn.commit()
        try:
            counts = sync_instruments(
                conn, listing, paced_verifier(), run_id,
                missing_threshold=get_settings().missing_runs_before_inactive_candidate,
            )  # fmt: skip
        except Exception as exc:
            conn.rollback()
            db.finish_run(conn, run_id, "failed")
            db.log_error(
                conn, run_id, None, ErrorType.SYNC_FAILED, f"{type(exc).__name__}: {exc}"[:500]
            )
            conn.commit()
            raise
        problems = counts["unmapped"] + counts["currency_mismatch"]
        db.finish_run(
            conn, run_id, "completed" if not problems else "completed_with_issues",
            total_symbols=len(listing.instruments),
            successful_symbols=len(listing.instruments) - problems,
            error_count=problems,
        )  # fmt: skip
        conn.commit()
    print(f"run {run_id}:", counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
