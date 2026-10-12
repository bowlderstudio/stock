"""Flow A logic: sync the Euronext instrument list, map and verify Yahoo symbols (§5).

Runs locally on a manually downloaded list (Euronext terms forbid automated download).
"""

import uuid
from collections.abc import Callable

import psycopg

from stockdata import db
from stockdata.models import ErrorType, InstrumentStatus
from stockdata.sources.euronext import EuronextList, candidate_yahoo_symbol

# symbol -> (has_data, currency); see stockdata.sources.yahoo.verify_symbol
Verifier = Callable[[str], tuple[bool, str | None]]

COUNT_KEYS = (
    "added", "updated", "verified", "unmapped", "currency_mismatch", "missing",
    "inactive_candidates",
)  # fmt: skip


def sync_instruments(
    conn: psycopg.Connection,
    listing: EuronextList,
    verify: Verifier,
    run_id: uuid.UUID,
    missing_threshold: int,
) -> dict[str, int]:
    """Upsert listed instruments, verify Yahoo mappings not yet verified, and count
    absences. Absence only applies to markets present in this file, and only ever leads to
    inactive_candidate (a human decides on inactive). Caller commits."""
    counts = dict.fromkeys(COUNT_KEYS, 0)
    before = db.instrument_states(conn)
    seen = set()

    for inst in listing.instruments:
        key = (inst.market, inst.exchange_symbol)
        prev = before.get(key)
        iid = db.upsert_instrument(conn, inst)
        seen.add(key)
        counts["added" if prev is None else "updated"] += 1
        status = prev.status if prev else InstrumentStatus.ACTIVE

        if prev is None or not prev.yahoo_verified:
            symbol = candidate_yahoo_symbol(inst.exchange_symbol)
            has_data, currency = verify(symbol)
            if not has_data:
                status, verified = InstrumentStatus.MAPPING_REVIEW, False
                counts["unmapped"] += 1
            elif currency != inst.currency:
                status, verified = InstrumentStatus.MAPPING_REVIEW, False
                counts["currency_mismatch"] += 1
                db.log_error(
                    conn, run_id, iid, ErrorType.CURRENCY_MISMATCH,
                    f"{symbol}: Euronext {inst.currency}, Yahoo {currency}",
                )  # fmt: skip
            else:
                status, verified = InstrumentStatus.ACTIVE, True
                counts["verified"] += 1
            db.set_provider_symbol(conn, iid, "yahoo", symbol, verified)

        if status == InstrumentStatus.INACTIVE_CANDIDATE:
            status = InstrumentStatus.ACTIVE
        db.set_instrument_status(conn, iid, status)
        db.update_sync_state(conn, iid, consecutive_missing=0)

    listed_markets = {i.market for i in listing.instruments}
    for (market, symbol), state in before.items():
        if (market, symbol) in seen or market not in listed_markets:
            continue
        if state.status == InstrumentStatus.INACTIVE:
            continue
        missing = state.consecutive_missing + 1
        counts["missing"] += 1
        db.update_sync_state(conn, state.instrument_id, consecutive_missing=missing)
        if missing >= missing_threshold and state.status != InstrumentStatus.INACTIVE_CANDIDATE:
            db.set_instrument_status(conn, state.instrument_id, InstrumentStatus.INACTIVE_CANDIDATE)
            counts["inactive_candidates"] += 1

    return counts
