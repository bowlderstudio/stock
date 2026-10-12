"""Flow B/C/D logic: daily incremental sync, backfill / full refetch, recent reconcile (§5)."""

import time
import uuid
from collections.abc import Callable
from datetime import date

import pandas as pd
import psycopg
from yfinance.exceptions import YFRateLimitError

from stockdata import db
from stockdata.config import Settings, get_settings
from stockdata.models import ActionType, BackfillStatus, ErrorType
from stockdata.pipelines.corporate_actions import extract_actions
from stockdata.quality import detect_gaps, detect_split_anomalies, invalid_ohlc_mask
from stockdata.sources.yahoo import normalize_history, to_price_rows
from stockdata.trading_calendar import trading_days

# (yahoo_symbol, start) -> raw yfinance history (auto_adjust=False, actions=True)
Fetcher = Callable[[str, date], pd.DataFrame]


def sync_daily_prices() -> dict:
    raise NotImplementedError


def store_history(
    conn: psycopg.Connection,
    run_id: uuid.UUID,
    instrument_id: int,
    raw: pd.DataFrame,
    last_closed: date,
    settings: Settings,
) -> tuple[int, int, list[date]]:
    """Normalize, upsert prices and corporate actions, log quality findings.

    Returns (rows_upserted, quality_findings, stored trade dates). Rows are always kept;
    findings go to ingestion_errors for review (§7)."""
    df = normalize_history(raw, last_closed=last_closed)
    rows = db.upsert_prices(conn, to_price_rows(instrument_id, df))
    actions = extract_actions(instrument_id, df)
    db.upsert_corporate_actions(conn, actions)

    findings = []
    for ts in df.index[invalid_ohlc_mask(df)]:
        r = df.loc[ts]
        message = (
            f"{ts.date()} OHLC inconsistent: o={r.open} h={r.high} l={r.low} c={r.close} "
            f"v={r.volume}"
        )
        findings.append((ErrorType.QUALITY_CHECK, message))
    splits = [a for a in actions if a.action_type == ActionType.SPLIT]
    for day in detect_split_anomalies(
        df, splits, settings.split_anomaly_window_days, settings.split_anomaly_tolerance
    ):
        findings.append((ErrorType.SPLIT_ANOMALY, f"{day} close off by ~split ratio"))
    have = [ts.date() for ts in df.index]
    if have:
        # up to the last closed session, so a history that stopped early shows as a gap
        expected = trading_days(have[0], max(have[-1], last_closed))
        for gap in detect_gaps(instrument_id, have, expected, settings.data_gap_trading_days):
            findings.append(
                (ErrorType.DATA_GAP, f"no rows {gap.start} .. {gap.end} (unclassified)")
            )
    for error_type, message in findings:
        db.log_error(conn, run_id, instrument_id, error_type, message)
    return rows, len(findings), have


def backfill_history(
    conn: psycopg.Connection,
    run_id: uuid.UUID,
    fetch: Fetcher,
    *,
    start: date,
    last_closed: date,
    tickers: list[str] | None = None,
    refetch_pending: bool = False,
    max_seconds: float | None = None,
    clock: Callable[[], float] = time.monotonic,
    pause: Callable[[], None] = lambda: None,
    settings: Settings | None = None,
) -> dict:
    """Fetch full history for pending (or explicitly listed / refetch-flagged) instruments.

    Commits after every instrument so an interrupted run resumes where it stopped. Stops
    starting new instruments once `max_seconds` is used up, and stops immediately on a
    Yahoo rate limit; unprocessed instruments stay pending."""
    settings = settings or get_settings()
    candidates = db.backfill_candidates(conn, tickers, refetch_pending)
    counts = {"done": 0, "no_data": 0, "failed": 0, "rows": 0, "quality_findings": 0,
              "remaining": 0, "rate_limited": False}  # fmt: skip
    started = clock()

    for i, (iid, symbol) in enumerate(candidates):
        if max_seconds is not None and clock() - started >= max_seconds:
            counts["remaining"] = len(candidates) - i
            break
        if i:
            pause()
        db.update_sync_state(conn, iid, backfill_status=BackfillStatus.IN_PROGRESS)
        conn.commit()
        try:
            raw = fetch(symbol, start)
        except YFRateLimitError:
            conn.rollback()
            db.update_sync_state(conn, iid, backfill_status=BackfillStatus.PENDING)
            db.log_error(conn, run_id, iid, ErrorType.RATE_LIMITED, f"{symbol}: rate limited")
            conn.commit()
            counts["rate_limited"] = True
            counts["remaining"] = len(candidates) - i
            break
        except Exception as exc:  # noqa: BLE001 - one symbol must not stop the run
            conn.rollback()
            failures = _failures(conn, iid) + 1
            db.update_sync_state(
                conn, iid, backfill_status=BackfillStatus.PENDING, consecutive_failures=failures
            )
            db.log_error(
                conn, run_id, iid, ErrorType.HTTP_ERROR,
                f"{symbol}: {type(exc).__name__}: {exc}"[:500],
            )  # fmt: skip
            conn.commit()
            counts["failed"] += 1
            continue

        rows, findings, have = store_history(conn, run_id, iid, raw, last_closed, settings)
        if not have:
            db.update_sync_state(conn, iid, backfill_status=BackfillStatus.NO_DATA,
                                 needs_full_refetch=False)  # fmt: skip
            db.log_error(conn, run_id, iid, ErrorType.EMPTY_RESULT, f"{symbol}: no rows")
            counts["no_data"] += 1
        else:
            db.update_sync_state(
                conn, iid,
                backfill_status=BackfillStatus.DONE, backfilled_from=have[0],
                last_price_date=have[-1], last_success_at=pd.Timestamp.now(tz="UTC"),
                consecutive_failures=0, needs_full_refetch=False,
            )  # fmt: skip
            counts["done"] += 1
            counts["rows"] += rows
            counts["quality_findings"] += findings
        conn.commit()

    return counts


def _failures(conn: psycopg.Connection, instrument_id: int) -> int:
    row = conn.execute(
        "SELECT consecutive_failures FROM instrument_sync_state WHERE instrument_id = %s",
        (instrument_id,),
    ).fetchone()
    return row[0] if row else 0


def reconcile_recent_prices() -> dict:
    raise NotImplementedError
