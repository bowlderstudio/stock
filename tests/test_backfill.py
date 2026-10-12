import uuid
from datetime import date

import numpy as np
import pandas as pd

from stockdata import db
from stockdata.models import Instrument
from stockdata.pipelines.prices import backfill_history

COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends", "Stock Splits"]


def history(rows: dict[str, tuple]) -> pd.DataFrame:
    df = pd.DataFrame.from_dict(rows, orient="index", columns=COLUMNS)
    df.index = pd.DatetimeIndex(pd.to_datetime(list(rows))).tz_localize("Europe/Oslo")
    return df


GOOD = history(
    {
        "2026-10-07": (10.0, 11.0, 9.5, 10.5, 9.8, 1000, 0.0, 0.0),
        "2026-10-08": (10.5, 11.5, 10.0, 11.0, 10.3, 2000, 0.25, 0.0),
        "2026-10-09": (11.0, 11.2, 10.4, 10.2, 10.2, 500, 0.0, 0.0),  # close < low
    }
)


class FakeFetch:
    def __init__(self, results=None):
        self.results = results or {}
        self.calls = []

    def __call__(self, symbol, start):
        self.calls.append(symbol)
        r = self.results.get(symbol, GOOD)
        if isinstance(r, Exception):
            raise r
        return r


def add(conn, symbol, status="active", verified=True, backfill="pending", refetch=False):
    iid = db.upsert_instrument(conn, Instrument(None, symbol, symbol, "XOSL", "NOK"))
    db.set_instrument_status(conn, iid, status)
    db.set_provider_symbol(conn, iid, "yahoo", f"{symbol}.OL", verified)
    db.update_sync_state(conn, iid, backfill_status=backfill, needs_full_refetch=refetch)
    conn.commit()
    return iid


def run(conn, fetch, **kw):
    run_id = uuid.uuid4()
    db.start_run(conn, run_id, "backfill_history")
    conn.commit()
    return backfill_history(
        conn, run_id, fetch, start=date(2016, 10, 1), last_closed=date(2026, 10, 9), **kw
    )


def state(conn, iid):
    return conn.execute(
        "select backfill_status, backfilled_from, last_price_date, consecutive_failures, "
        "needs_full_refetch from instrument_sync_state where instrument_id = %s",
        (iid,),
    ).fetchone()


def test_pending_instrument_is_loaded_and_marked_done(conn):
    iid = add(conn, "ALPHA")
    counts = run(conn, FakeFetch())
    assert counts["done"] == 1 and counts["rows"] == 3
    assert state(conn, iid) == ("done", date(2026, 10, 7), date(2026, 10, 9), 0, False)
    assert conn.execute("select count(*) from corporate_actions").fetchone() == (1,)


def test_quality_findings_are_logged_but_rows_kept(conn):
    add(conn, "ALPHA")
    counts = run(conn, FakeFetch())
    assert counts["quality_findings"] == 1
    assert conn.execute("select error_type from ingestion_errors").fetchall() == [
        ("quality_check",)
    ]
    assert conn.execute("select count(*) from daily_prices").fetchone() == (3,)


def test_only_eligible_instruments_are_selected(conn):
    add(conn, "DONE", backfill="done")
    add(conn, "REVIEW", status="mapping_review")
    add(conn, "UNVERIFIED", verified=False)
    add(conn, "INACTIVE", status="inactive_candidate")
    add(conn, "TODO")
    fetch = FakeFetch()
    run(conn, fetch)
    assert fetch.calls == ["TODO.OL"]


def test_failure_is_recorded_and_does_not_stop_others(conn):
    bad = add(conn, "BAD")
    good = add(conn, "GOOD")
    counts = run(conn, FakeFetch({"BAD.OL": RuntimeError("boom")}))
    assert counts["failed"] == 1 and counts["done"] == 1
    assert state(conn, bad)[0] == "pending" and state(conn, bad)[3] == 1
    assert state(conn, good)[0] == "done"
    errors = conn.execute("select error_type, message from ingestion_errors").fetchall()
    assert [e[0] for e in errors if "BAD.OL" in e[1]] == ["http_error"]


def test_empty_history_marks_no_data(conn):
    iid = add(conn, "EMPTY")
    counts = run(conn, FakeFetch({"EMPTY.OL": GOOD.iloc[0:0]}))
    assert counts["no_data"] == 1
    assert state(conn, iid)[0] == "no_data"


def test_full_refetch_only_with_flag_and_flag_is_cleared(conn):
    iid = add(conn, "SPLIT", backfill="done", refetch=True)
    fetch = FakeFetch()
    run(conn, fetch)
    assert fetch.calls == []
    run(conn, fetch, refetch_pending=True)
    assert fetch.calls == ["SPLIT.OL"]
    assert state(conn, iid)[4] is False


def test_explicit_tickers_are_processed_even_if_done(conn):
    add(conn, "DONE", backfill="done")
    add(conn, "TODO")
    fetch = FakeFetch()
    run(conn, fetch, tickers=["DONE.OL"])
    assert fetch.calls == ["DONE.OL"]


def test_time_budget_stops_before_next_instrument_and_rest_stays_pending(conn):
    first = add(conn, "A1")
    second = add(conn, "A2")
    ticks = iter([0.0, 0.0, 100.0, 100.0, 100.0])
    counts = run(conn, FakeFetch(), max_seconds=50, clock=lambda: next(ticks))
    assert counts["done"] == 1 and counts["remaining"] == 1
    assert {state(conn, first)[0], state(conn, second)[0]} == {"done", "pending"}


def test_rows_after_last_closed_session_are_not_stored(conn):
    add(conn, "ALPHA")
    late = pd.concat([GOOD, history({"2026-10-12": (1, 1, 1, 1, 1, 1, 0.0, 0.0)})])
    run(conn, FakeFetch({"ALPHA.OL": late}))
    assert conn.execute("select max(trade_date) from daily_prices").fetchone() == (
        date(2026, 10, 9),
    )


def test_missing_volume_is_stored_as_null(conn):
    add(conn, "ALPHA")
    gappy = history({"2026-10-07": (10.0, 11.0, 9.5, 10.5, 9.8, np.nan, 0.0, 0.0)})
    run(conn, FakeFetch({"ALPHA.OL": gappy}))
    assert conn.execute("select volume from daily_prices").fetchone() == (None,)


def test_rate_limit_stops_the_run_and_leaves_rest_pending(conn):
    from yfinance.exceptions import YFRateLimitError

    first = add(conn, "A1")
    second = add(conn, "A2")
    fetch = FakeFetch({"A1.OL": YFRateLimitError()})
    counts = run(conn, fetch)
    assert fetch.calls == ["A1.OL"]
    assert counts["rate_limited"] is True and counts["remaining"] == 2
    assert state(conn, first)[0] == "pending" and state(conn, second)[0] == "pending"
    assert state(conn, first)[3] == 0  # not a failure of the symbol itself


def test_history_ending_long_before_last_closed_session_is_a_gap(conn):
    # data stops 2026-09-10; last closed session 2026-10-09 is 21 sessions later
    add(conn, "STALE")
    stale = history({"2026-09-09": (1, 1, 1, 1, 1, 1, 0.0, 0.0),
                     "2026-09-10": (1, 1, 1, 1, 1, 1, 0.0, 0.0)})  # fmt: skip
    run(conn, FakeFetch({"STALE.OL": stale}))
    gaps = conn.execute(
        "select message from ingestion_errors where error_type = 'data_gap'"
    ).fetchall()
    assert gaps == [("no rows 2026-09-11 .. 2026-10-09 (unclassified)",)]
