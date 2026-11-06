import uuid
from datetime import date

from yfinance.exceptions import YFRateLimitError

from stockdata import db
from stockdata.config import Settings
from stockdata.models import ActionType, CorporateAction
from stockdata.pipelines.prices import sync_daily_prices
from tests.helpers import add, history

# lookback 2 sessions, 2 symbols per download call
SETTINGS = Settings(_env_file=None, lookback_trading_days=2, batch_size=2)
TODAY = date(2026, 10, 9)  # Friday, a session
WINDOW = history(
    {
        "2026-10-06": (10.0, 11.0, 9.0, 10.0, 10.0, 100, 0.0, 0.0),
        "2026-10-07": (10.0, 11.0, 9.0, 10.5, 10.5, 100, 0.0, 0.0),
        "2026-10-08": (10.5, 11.0, 10.0, 10.8, 10.8, 100, 0.0, 0.0),
        "2026-10-09": (10.8, 11.5, 10.5, 11.2, 11.2, 100, 0.0, 0.0),
    }
)


class FakeYahoo:
    """download(symbols, start) -> {symbol: frame}; fetch(symbol, start) -> frame."""

    def __init__(self, batch=None, single=None, batch_error=None):
        self.batch, self.single, self.batch_error = batch or {}, single or {}, batch_error
        self.downloads, self.fetches = [], []

    def download(self, symbols, start):
        self.downloads.append((list(symbols), start))
        if self.batch_error:
            raise self.batch_error
        return {
            s: self.batch.get(s, WINDOW) for s in symbols if self.batch.get(s, WINDOW) is not None
        }

    def fetch(self, symbol, start):
        self.fetches.append(symbol)
        return self.single.get(symbol, WINDOW.iloc[0:0])


def run(conn, yahoo, today=TODAY):
    run_id = uuid.uuid4()
    db.start_run(conn, run_id, "sync_daily_prices")
    conn.commit()
    return sync_daily_prices(
        conn, run_id, yahoo.download, yahoo.fetch,
        today=today, last_closed=date(2026, 10, 9), settings=SETTINGS,
    )  # fmt: skip


def last_price(conn, iid):
    return conn.execute(
        "select last_price_date, consecutive_failures, needs_full_refetch "
        "from instrument_sync_state where instrument_id = %s",
        (iid,),
    ).fetchone()


def test_non_trading_day_does_nothing(conn):
    add(conn, "A", backfill="done", last_price=date(2026, 10, 8))
    yahoo = FakeYahoo()
    counts = run(conn, yahoo, today=date(2026, 10, 10))  # Saturday
    assert counts["skipped"] is True
    assert yahoo.downloads == []


def test_start_is_lookback_sessions_before_last_price_date(conn):
    add(conn, "A", backfill="done", last_price=date(2026, 10, 8))
    yahoo = FakeYahoo()
    run(conn, yahoo)
    assert yahoo.downloads == [(["A.OL"], date(2026, 10, 6))]


def test_symbols_are_grouped_by_start_and_batched(conn):
    for s in ("A", "B", "C"):
        add(conn, s, backfill="done", last_price=date(2026, 10, 8))
    add(conn, "D", backfill="done", last_price=date(2026, 10, 7))
    yahoo = FakeYahoo()
    run(conn, yahoo)
    assert sorted(yahoo.downloads) == [
        (["A.OL", "B.OL"], date(2026, 10, 6)),
        (["C.OL"], date(2026, 10, 6)),
        (["D.OL"], date(2026, 10, 5)),
    ]


def test_new_rows_are_stored_and_last_price_date_advances(conn):
    iid = add(conn, "A", backfill="done", last_price=date(2026, 10, 8))
    counts = run(conn, FakeYahoo())
    assert counts["updated"] == 1 and counts["rows"] == 4
    assert last_price(conn, iid) == (date(2026, 10, 9), 0, False)


def test_only_backfilled_active_verified_instruments_are_synced(conn):
    add(conn, "DONE", backfill="done", last_price=date(2026, 10, 8))
    add(conn, "PENDING", backfill="pending")
    add(conn, "REVIEW", status="mapping_review", backfill="done", last_price=date(2026, 10, 8))
    add(conn, "UNVERIFIED", verified=False, backfill="done", last_price=date(2026, 10, 8))
    yahoo = FakeYahoo()
    run(conn, yahoo)
    assert [s for symbols, _ in yahoo.downloads for s in symbols] == ["DONE.OL"]


def test_symbol_missing_from_batch_is_retried_alone(conn):
    iid = add(conn, "A", backfill="done", last_price=date(2026, 10, 8))
    yahoo = FakeYahoo(batch={"A.OL": None}, single={"A.OL": WINDOW})
    counts = run(conn, yahoo)
    assert yahoo.fetches == ["A.OL"]
    assert counts["updated"] == 1
    assert last_price(conn, iid)[0] == date(2026, 10, 9)


def test_symbol_without_data_after_retry_counts_as_failure(conn):
    bad = add(conn, "BAD", backfill="done", last_price=date(2026, 10, 8))
    good = add(conn, "GOOD", backfill="done", last_price=date(2026, 10, 8))
    counts = run(conn, FakeYahoo(batch={"BAD.OL": None}))
    assert counts["failed"] == 1 and counts["updated"] == 1
    assert last_price(conn, bad) == (date(2026, 10, 8), 1, False)
    assert last_price(conn, good)[0] == date(2026, 10, 9)
    assert conn.execute("select error_type from ingestion_errors").fetchall() == [("empty_result",)]


def test_new_split_flags_full_refetch_but_known_dividend_does_not(conn):
    split = add(conn, "SPLIT", backfill="done", last_price=date(2026, 10, 8))
    div = add(conn, "DIV", backfill="done", last_price=date(2026, 10, 8))
    db.upsert_corporate_actions(
        conn, [CorporateAction(div, date(2026, 10, 7), ActionType.DIVIDEND, 0.5)]
    )
    conn.commit()
    with_split = WINDOW.copy()
    with_split.loc[with_split.index[2], "Stock Splits"] = 2.0
    with_div = WINDOW.copy()
    with_div.loc[with_div.index[1], "Dividends"] = 0.5
    counts = run(conn, FakeYahoo(batch={"SPLIT.OL": with_split, "DIV.OL": with_div}))
    assert counts["refetch_flagged"] == 1
    assert last_price(conn, split)[2] is True
    assert last_price(conn, div)[2] is False


def test_rate_limit_stops_the_run(conn):
    add(conn, "A", backfill="done", last_price=date(2026, 10, 8))
    counts = run(conn, FakeYahoo(batch_error=YFRateLimitError()))
    assert counts["rate_limited"] is True and counts["updated"] == 0


def test_whole_batch_error_falls_back_to_single_fetches(conn):
    iid = add(conn, "A", backfill="done", last_price=date(2026, 10, 8))
    yahoo = FakeYahoo(batch_error=RuntimeError("batch broke"), single={"A.OL": WINDOW})
    counts = run(conn, yahoo)
    assert yahoo.fetches == ["A.OL"]
    assert counts["updated"] == 1
    assert last_price(conn, iid)[0] == date(2026, 10, 9)


def test_instrument_without_last_price_date_is_skipped(conn):
    add(conn, "NOLAST", backfill="done", last_price=None)
    yahoo = FakeYahoo()
    run(conn, yahoo)
    assert yahoo.downloads == []


def test_error_in_single_retry_fails_only_that_symbol(conn):
    bad = add(conn, "BAD", backfill="done", last_price=date(2026, 10, 8))
    good = add(conn, "GOOD", backfill="done", last_price=date(2026, 10, 8))

    class Boom(FakeYahoo):
        def fetch(self, symbol, start):
            raise RuntimeError("HTTP 500")

    counts = run(conn, Boom(batch={"BAD.OL": None}))
    assert counts["failed"] == 1 and counts["updated"] == 1
    assert last_price(conn, bad)[1] == 1
    assert last_price(conn, good)[0] == date(2026, 10, 9)
    assert ("http_error",) in conn.execute("select error_type from ingestion_errors").fetchall()


def test_same_quality_finding_is_not_logged_again_on_next_run(conn):
    add(conn, "A", backfill="done", last_price=date(2026, 10, 8))
    bad = WINDOW.copy()
    bad.loc[bad.index[3], "Close"] = 12.0  # above the day's high
    run(conn, FakeYahoo(batch={"A.OL": bad}))
    run(conn, FakeYahoo(batch={"A.OL": bad}))  # lookback re-reads the same row
    assert conn.execute(
        "select count(*) from ingestion_errors where error_type = 'quality_check'"
    ).fetchone() == (1,)
