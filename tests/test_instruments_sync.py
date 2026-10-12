import uuid
from datetime import date

from stockdata import db
from stockdata.models import Instrument
from stockdata.pipelines.instruments import sync_instruments
from stockdata.sources.euronext import EuronextList

ALPHA = Instrument("NO0000000001", "ALPHA", "ALPHA SHIPPING", "XOSL", "NOK")
BETA = Instrument("NO0000000002", "BETA", "BETA TECH", "XOSL", "NOK")
GAMMA = Instrument("NO0000000003", "GAMMA", "GAMMA GROWTH", "MERK", "NOK")


class FakeYahoo:
    """verify(symbol) -> (has_data, currency); records which symbols were checked."""

    def __init__(self, answers=None):
        self.answers = answers or {}
        self.calls = []

    def __call__(self, symbol):
        self.calls.append(symbol)
        return self.answers.get(symbol, (True, "NOK"))


def sync(conn, instruments, yahoo=None, threshold=2):
    run_id = uuid.uuid4()
    db.start_run(conn, run_id, "sync_instruments")
    return sync_instruments(
        conn,
        EuronextList(date(2026, 10, 12), instruments),
        yahoo or FakeYahoo(),
        run_id,
        missing_threshold=threshold,
    )


def status_of(conn, symbol):
    return conn.execute(
        "SELECT status FROM instruments WHERE exchange_symbol = %s", (symbol,)
    ).fetchone()[0]


def missing_count(conn, symbol):
    return conn.execute(
        "SELECT s.consecutive_missing FROM instrument_sync_state s "
        "JOIN instruments USING (instrument_id) WHERE exchange_symbol = %s",
        (symbol,),
    ).fetchone()[0]


def test_new_instrument_is_added_and_yahoo_mapping_verified(conn):
    yahoo = FakeYahoo()
    counts = sync(conn, [ALPHA], yahoo)
    assert yahoo.calls == ["ALPHA.OL"]
    assert counts["added"] == 1 and counts["verified"] == 1
    assert status_of(conn, "ALPHA") == "active"
    alias = conn.execute("SELECT provider_symbol, is_verified FROM symbol_aliases").fetchall()
    assert alias == [("ALPHA.OL", True)]


def test_symbol_without_yahoo_data_goes_to_mapping_review(conn):
    counts = sync(conn, [ALPHA], FakeYahoo({"ALPHA.OL": (False, None)}))
    assert counts["unmapped"] == 1
    assert status_of(conn, "ALPHA") == "mapping_review"
    assert conn.execute("SELECT is_verified FROM symbol_aliases").fetchone() == (False,)


def test_currency_mismatch_goes_to_mapping_review_and_is_logged(conn):
    counts = sync(conn, [ALPHA], FakeYahoo({"ALPHA.OL": (True, "USD")}))
    assert counts["currency_mismatch"] == 1
    assert status_of(conn, "ALPHA") == "mapping_review"
    errors = conn.execute("SELECT error_type FROM ingestion_errors").fetchall()
    assert errors == [("currency_mismatch",)]


def test_verified_mapping_is_not_rechecked(conn):
    sync(conn, [ALPHA])
    yahoo = FakeYahoo()
    counts = sync(conn, [ALPHA], yahoo)
    assert yahoo.calls == []
    assert counts["added"] == 0 and counts["updated"] == 1


def test_unverified_mapping_is_rechecked_and_recovers(conn):
    sync(conn, [ALPHA], FakeYahoo({"ALPHA.OL": (False, None)}))
    yahoo = FakeYahoo()
    sync(conn, [ALPHA], yahoo)
    assert yahoo.calls == ["ALPHA.OL"]
    assert status_of(conn, "ALPHA") == "active"


def test_existing_instrument_gets_isin_without_duplicate(conn):
    db.upsert_instrument(conn, Instrument(None, "ALPHA", "Alpha Shipping ASA", "XOSL", "NOK"))
    sync(conn, [ALPHA])
    rows = conn.execute("SELECT isin, name FROM instruments").fetchall()
    assert rows == [("NO0000000001", "ALPHA SHIPPING")]


def test_missing_instrument_becomes_inactive_candidate_after_threshold(conn):
    sync(conn, [ALPHA, BETA])
    counts = sync(conn, [ALPHA])
    assert counts["missing"] == 1 and counts["inactive_candidates"] == 0
    assert missing_count(conn, "BETA") == 1
    assert status_of(conn, "BETA") == "active"
    counts = sync(conn, [ALPHA])
    assert counts["inactive_candidates"] == 1
    assert status_of(conn, "BETA") == "inactive_candidate"


def test_reappearing_instrument_is_reactivated(conn):
    sync(conn, [ALPHA, BETA])
    sync(conn, [ALPHA])
    sync(conn, [ALPHA])
    sync(conn, [ALPHA, BETA])
    assert status_of(conn, "BETA") == "active"
    assert missing_count(conn, "BETA") == 0


def test_markets_absent_from_the_file_are_not_counted_missing(conn):
    sync(conn, [ALPHA, GAMMA])
    counts = sync(conn, [ALPHA])  # e.g. an Oslo Børs-only download: no MERK rows at all
    assert counts["missing"] == 0
    assert missing_count(conn, "GAMMA") == 0
