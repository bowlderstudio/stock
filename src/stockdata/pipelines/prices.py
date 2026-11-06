"""Flow B/C/D logic: daily incremental sync, backfill / full refetch, recent reconcile (§5)."""

import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import date
from typing import NamedTuple

import pandas as pd
import psycopg
from yfinance.exceptions import YFRateLimitError

from stockdata import db
from stockdata.config import Settings, get_settings
from stockdata.models import ActionType, BackfillStatus, CorporateAction, ErrorType
from stockdata.pipelines.corporate_actions import extract_actions
from stockdata.quality import detect_gaps, detect_split_anomalies, invalid_ohlc_mask
from stockdata.sources.yahoo import normalize_history, to_price_rows
from stockdata.trading_calendar import is_trading_day, session_n_before, trading_days

# (yahoo_symbol, start) -> raw yfinance history (auto_adjust=False, actions=True)
Fetcher = Callable[[str, date], pd.DataFrame]
# (yahoo_symbols, start) -> {symbol: raw history}; symbols without data may be absent
BatchDownloader = Callable[[list[str], date], dict[str, pd.DataFrame]]


class StoredHistory(NamedTuple):
    rows: int
    quality_findings: int
    dates: list[date]
    new_actions: list[CorporateAction]


def store_history(
    conn: psycopg.Connection,
    run_id: uuid.UUID,
    instrument_id: int,
    raw: pd.DataFrame,
    last_closed: date,
    settings: Settings,
    check_gaps: bool = True,
) -> StoredHistory:
    """Normalize, upsert prices and corporate actions, log quality findings.

    Rows are always kept; findings go to ingestion_errors for review (§7). Gap detection is
    for full histories (backfill, weekly reconcile); the daily window skips it so the same
    gap is not logged every day."""
    df = normalize_history(raw, last_closed=last_closed)
    rows = db.upsert_prices(conn, to_price_rows(instrument_id, df))
    actions = extract_actions(instrument_id, df)
    new_actions = db.upsert_corporate_actions(conn, actions)

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
    if have and check_gaps:
        # up to the last closed session, so a history that stopped early shows as a gap
        expected = trading_days(have[0], max(have[-1], last_closed))
        for gap in detect_gaps(instrument_id, have, expected, settings.data_gap_trading_days):
            findings.append(
                (ErrorType.DATA_GAP, f"no rows {gap.start} .. {gap.end} (unclassified)")
            )
    logged = sum(
        db.log_error_once(conn, run_id, instrument_id, error_type, message)
        for error_type, message in findings
    )
    return StoredHistory(rows, logged, have, new_actions)


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

        stored = store_history(conn, run_id, iid, raw, last_closed, settings)
        have = stored.dates
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
            counts["rows"] += stored.rows
            counts["quality_findings"] += stored.quality_findings
        conn.commit()

    return counts


def _failures(conn: psycopg.Connection, instrument_id: int) -> int:
    row = conn.execute(
        "SELECT consecutive_failures FROM instrument_sync_state WHERE instrument_id = %s",
        (instrument_id,),
    ).fetchone()
    return row[0] if row else 0


def sync_daily_prices(
    conn: psycopg.Connection,
    run_id: uuid.UUID,
    download: BatchDownloader,
    fetch: Fetcher,
    *,
    today: date,
    last_closed: date,
    settings: Settings | None = None,
    pause: Callable[[], None] = lambda: None,
) -> dict:
    """Flow B: incremental daily sync for backfilled instruments (§5).

    Each instrument restarts `lookback_trading_days` sessions before its last stored date,
    so recent revisions are picked up. Instruments sharing a start date are downloaded in
    batches of `batch_size`; a symbol missing from a batch (or a failed batch) is retried
    alone. New splits/dividends set needs_full_refetch for backfill_history. A Yahoo rate
    limit ends the run. Commits after each batch."""
    settings = settings or get_settings()
    counts = {"skipped": False, "symbols": 0, "updated": 0, "failed": 0, "rows": 0,
              "quality_findings": 0, "refetch_flagged": 0, "rate_limited": False,
              "remaining": 0}  # fmt: skip
    if not is_trading_day(today):
        counts["skipped"] = True
        return counts

    groups: dict[date, list[tuple[int, str]]] = defaultdict(list)
    for iid, symbol, last in db.daily_sync_candidates(conn):
        groups[session_n_before(last, settings.lookback_trading_days)].append((iid, symbol))
    batches = [
        (start, members[i : i + settings.batch_size])
        for start, members in sorted(groups.items())
        for i in range(0, len(members), settings.batch_size)
    ]
    counts["symbols"] = sum(len(b) for _, b in batches)

    for n, (start, batch) in enumerate(batches):
        if n:
            pause()
        symbols = [symbol for _, symbol in batch]
        try:
            try:
                frames = download(symbols, start)
            except YFRateLimitError:
                raise
            except Exception:  # noqa: BLE001 - whole batch failed: retry symbols alone
                frames = {}
            for iid, symbol in batch:
                raw = frames.get(symbol)
                if raw is None or raw.empty:
                    try:
                        raw = fetch(symbol, start)
                    except YFRateLimitError:
                        raise
                    except Exception as exc:  # noqa: BLE001 - fail this symbol only
                        _fail(conn, run_id, iid, ErrorType.HTTP_ERROR,
                              f"{symbol}: {type(exc).__name__}: {exc}"[:500], counts)  # fmt: skip
                        continue
                _store_daily(conn, run_id, iid, symbol, raw, last_closed, settings, counts)
        except YFRateLimitError:
            conn.rollback()
            db.log_error(conn, run_id, None, ErrorType.RATE_LIMITED, f"batch from {start}")
            conn.commit()
            counts["rate_limited"] = True
            counts["remaining"] = sum(len(b) for _, b in batches[n:])
            break
        conn.commit()

    return counts


def _store_daily(conn, run_id, iid, symbol, raw, last_closed, settings, counts) -> None:
    stored = store_history(conn, run_id, iid, raw, last_closed, settings, check_gaps=False)
    if not stored.dates:
        _fail(conn, run_id, iid, ErrorType.EMPTY_RESULT, f"{symbol}: no rows", counts)
        return
    fields = {
        "last_price_date": stored.dates[-1],
        "last_success_at": pd.Timestamp.now(tz="UTC"),
        "consecutive_failures": 0,
    }
    if stored.new_actions:
        fields["needs_full_refetch"] = True
        counts["refetch_flagged"] += 1
    db.update_sync_state(conn, iid, **fields)
    counts["updated"] += 1
    counts["rows"] += stored.rows
    counts["quality_findings"] += stored.quality_findings


def _fail(conn, run_id, iid, error_type: ErrorType, message: str, counts: dict) -> None:
    db.update_sync_state(conn, iid, consecutive_failures=_failures(conn, iid) + 1)
    db.log_error(conn, run_id, iid, error_type, message)
    counts["failed"] += 1


def reconcile_recent_prices() -> dict:
    raise NotImplementedError
