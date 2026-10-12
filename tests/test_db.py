import uuid
from datetime import date

from stockdata import db
from stockdata.models import ActionType, CorporateAction, Instrument, PriceRow

EQNR = Instrument(
    isin="NO0010096985", exchange_symbol="EQNR", name="Equinor", market="XOSL", currency="NOK"
)


def price(instrument_id: int, day: str, close: float) -> PriceRow:
    return PriceRow(instrument_id, date.fromisoformat(day), close, close, close, close, close, 100)


def test_upsert_instrument_is_idempotent_and_updates_attributes(conn):
    first = db.upsert_instrument(conn, EQNR)
    second = db.upsert_instrument(conn, Instrument(**{**EQNR.__dict__, "name": "Equinor ASA"}))
    assert first == second
    assert conn.execute("select count(*), max(name) from instruments").fetchone() == (
        1,
        "Equinor ASA",
    )


def test_same_provider_symbol_twice_keeps_one_open_alias(conn):
    iid = db.upsert_instrument(conn, EQNR)
    db.set_provider_symbol(conn, iid, "yahoo", "EQNR.OL", verified=False)
    db.set_provider_symbol(conn, iid, "yahoo", "EQNR.OL", verified=True)
    rows = conn.execute(
        "select provider_symbol, is_verified, valid_to from symbol_aliases"
    ).fetchall()
    assert rows == [("EQNR.OL", True, None)]


def test_new_provider_symbol_closes_previous_alias(conn):
    iid = db.upsert_instrument(conn, EQNR)
    db.set_provider_symbol(conn, iid, "yahoo", "STL.OL", verified=True)
    db.set_provider_symbol(conn, iid, "yahoo", "EQNR.OL", verified=True)
    rows = conn.execute(
        "select provider_symbol, valid_to is null from symbol_aliases order by alias_id"
    ).fetchall()
    assert rows == [("STL.OL", False), ("EQNR.OL", True)]


def test_upsert_prices_rerun_updates_instead_of_duplicating(conn):
    iid = db.upsert_instrument(conn, EQNR)
    assert (
        db.upsert_prices(conn, [price(iid, "2026-10-08", 300.0), price(iid, "2026-10-09", 301.0)])
        == 2
    )
    assert db.upsert_prices(conn, [price(iid, "2026-10-09", 299.5)]) == 1
    rows = conn.execute("select trade_date, close from daily_prices order by 1").fetchall()
    assert rows == [(date(2026, 10, 8), 300.0), (date(2026, 10, 9), 299.5)]


def test_upsert_corporate_actions_returns_only_new_ones(conn):
    iid = db.upsert_instrument(conn, EQNR)
    div = CorporateAction(iid, date(2026, 5, 15), ActionType.DIVIDEND, 3.9)
    split = CorporateAction(iid, date(2026, 6, 3), ActionType.SPLIT, 5.0)
    assert db.upsert_corporate_actions(conn, [div]) == [div]
    assert db.upsert_corporate_actions(conn, [div, split]) == [split]
    assert conn.execute("select count(*) from corporate_actions").fetchone() == (2,)


def test_update_sync_state_creates_then_updates(conn):
    iid = db.upsert_instrument(conn, EQNR)
    db.update_sync_state(conn, iid, backfill_status="in_progress")
    db.update_sync_state(conn, iid, backfill_status="done", last_price_date=date(2026, 10, 9))
    row = conn.execute(
        "select backfill_status, last_price_date from instrument_sync_state where instrument_id=%s",
        (iid,),
    ).fetchone()
    assert row == ("done", date(2026, 10, 9))


def test_run_and_error_are_recorded(conn):
    iid = db.upsert_instrument(conn, EQNR)
    run_id = uuid.uuid4()
    db.start_run(conn, run_id, "backfill_history")
    db.log_error(conn, run_id, iid, "split_anomaly", "2021-12-13 close off by split ratio")
    db.finish_run(
        conn,
        run_id,
        "completed",
        total_symbols=1,
        successful_symbols=1,
        rows_upserted=2511,
        error_count=1,
    )
    run = conn.execute(
        "select status, finished_at is not null, rows_upserted, error_count from ingestion_runs"
    ).fetchone()
    assert run == ("completed", True, 2511, 1)
    assert conn.execute("select error_type from ingestion_errors").fetchall() == [
        ("split_anomaly",)
    ]


def test_run_duration_reflects_wall_clock_within_one_transaction(conn):
    import time

    run_id = uuid.uuid4()
    db.start_run(conn, run_id, "sync_instruments")
    time.sleep(0.3)  # work done inside the same transaction
    db.finish_run(conn, run_id, "completed")
    seconds = conn.execute(
        "select extract(epoch from finished_at - started_at) from ingestion_runs"
    ).fetchone()[0]
    assert seconds >= 0.3
