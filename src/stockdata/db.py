"""PostgreSQL access (Neon). All writes are idempotent; callers own the transaction (commit)."""

import uuid
from collections.abc import Iterable
from dataclasses import astuple
from typing import NamedTuple

import psycopg
from psycopg import sql

from stockdata.config import get_settings
from stockdata.models import ActionType, CorporateAction, Instrument, PriceRow


def connect() -> psycopg.Connection:
    """Open a connection using STOCKDATA_DB_URL (or DATABASE_URL).

    prepare_threshold=None disables server-side prepared statements, which transaction-mode
    connection poolers (PgBouncer / Neon -pooler hosts) do not support.
    """
    url = get_settings().db_url
    if url is None:
        raise RuntimeError("STOCKDATA_DB_URL / DATABASE_URL is not set")
    return psycopg.connect(url.get_secret_value(), prepare_threshold=None)


def upsert_instrument(conn: psycopg.Connection, inst: Instrument) -> int:
    """Insert or update by (market, exchange_symbol); return instrument_id."""
    row = conn.execute(
        """
        INSERT INTO instruments (isin, exchange_symbol, name, market, currency, instrument_type)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (market, exchange_symbol) DO UPDATE SET
            isin = COALESCE(EXCLUDED.isin, instruments.isin),
            name = EXCLUDED.name,
            currency = EXCLUDED.currency,
            instrument_type = EXCLUDED.instrument_type,
            last_seen_at = now()
        RETURNING instrument_id
        """,
        (
            inst.isin,
            inst.exchange_symbol,
            inst.name,
            inst.market,
            inst.currency,
            inst.instrument_type,
        ),
    ).fetchone()
    return row[0]


def set_provider_symbol(
    conn: psycopg.Connection, instrument_id: int, provider: str, symbol: str, verified: bool
) -> None:
    """Make `symbol` the open mapping for (instrument, provider), closing a different one."""
    open_alias = conn.execute(
        """
        SELECT alias_id, provider_symbol FROM symbol_aliases
        WHERE instrument_id = %s AND provider = %s AND valid_to IS NULL
        """,
        (instrument_id, provider),
    ).fetchone()
    if open_alias and open_alias[1] == symbol:
        conn.execute(
            "UPDATE symbol_aliases SET is_verified = %s WHERE alias_id = %s",
            (verified, open_alias[0]),
        )
        return
    if open_alias:
        conn.execute(
            "UPDATE symbol_aliases SET valid_to = CURRENT_DATE WHERE alias_id = %s",
            (open_alias[0],),
        )
    conn.execute(
        """
        INSERT INTO symbol_aliases (instrument_id, provider, provider_symbol, is_verified)
        VALUES (%s, %s, %s, %s)
        """,
        (instrument_id, provider, symbol, verified),
    )


def upsert_prices(conn: psycopg.Connection, rows: Iterable[PriceRow]) -> int:
    """Upsert daily_prices on (instrument_id, trade_date, source_id); return rows written."""
    params = [astuple(r) for r in rows]
    if not params:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO daily_prices
                (instrument_id, trade_date, open, high, low, close, adj_close, volume, source_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_id, trade_date, source_id) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, adj_close = EXCLUDED.adj_close,
                volume = EXCLUDED.volume, fetched_at = now()
            """,
            params,
        )
    return len(params)


def upsert_corporate_actions(
    conn: psycopg.Connection, actions: Iterable[CorporateAction]
) -> list[CorporateAction]:
    """Insert corporate actions; return the ones not previously known (they trigger a full
    refetch)."""
    params = [astuple(a) for a in actions]
    if not params:
        return []
    new: list[CorporateAction] = []
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO corporate_actions
                (instrument_id, action_date, action_type, value, source_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING instrument_id, action_date, action_type, value, source_id
            """,
            params,
            returning=True,
        )
        while True:
            for iid, day, kind, value, source_id in cur.fetchall():
                new.append(CorporateAction(iid, day, ActionType(kind), value, source_id))
            if not cur.nextset():
                break
    return new


_SYNC_STATE_FIELDS = {
    "backfill_status",
    "backfilled_from",
    "last_price_date",
    "last_success_at",
    "consecutive_failures",
    "consecutive_missing",
    "needs_full_refetch",
}


def update_sync_state(conn: psycopg.Connection, instrument_id: int, **fields) -> None:
    """Create or update the instrument_sync_state row with the given columns."""
    unknown = set(fields) - _SYNC_STATE_FIELDS
    if unknown:
        raise ValueError(f"unknown sync state fields: {sorted(unknown)}")
    cols = [sql.Identifier(k) for k in fields]
    query = sql.SQL(
        """
        INSERT INTO instrument_sync_state (instrument_id, {cols}) VALUES (%s, {vals})
        ON CONFLICT (instrument_id) DO UPDATE SET {sets}, updated_at = now()
        """
    ).format(
        cols=sql.SQL(", ").join(cols),
        vals=sql.SQL(", ").join(sql.Placeholder() for _ in cols),
        sets=sql.SQL(", ").join(sql.SQL("{c} = EXCLUDED.{c}").format(c=c) for c in cols),
    )
    conn.execute(query, (instrument_id, *fields.values()))


def start_run(conn: psycopg.Connection, run_id: uuid.UUID, flow_name: str) -> None:
    conn.execute(
        "INSERT INTO ingestion_runs (run_id, flow_name, started_at, status) "
        "VALUES (%s, %s, clock_timestamp(), 'running')",
        (run_id, flow_name),
    )


def finish_run(
    conn: psycopg.Connection,
    run_id: uuid.UUID,
    status: str,
    *,
    total_symbols: int = 0,
    successful_symbols: int = 0,
    rows_upserted: int = 0,
    error_count: int = 0,
) -> None:
    conn.execute(
        """
        UPDATE ingestion_runs SET finished_at = clock_timestamp(), status = %s, total_symbols = %s,
            successful_symbols = %s, rows_upserted = %s, error_count = %s
        WHERE run_id = %s
        """,
        (status, total_symbols, successful_symbols, rows_upserted, error_count, run_id),
    )


def log_error(
    conn: psycopg.Connection,
    run_id: uuid.UUID | None,
    instrument_id: int | None,
    error_type: str,
    message: str,
) -> None:
    conn.execute(
        "INSERT INTO ingestion_errors (run_id, instrument_id, error_type, message) "
        "VALUES (%s, %s, %s, %s)",
        (run_id, instrument_id, error_type, message),
    )


class InstrumentState(NamedTuple):
    instrument_id: int
    status: str
    consecutive_missing: int
    yahoo_verified: bool


def instrument_states(conn: psycopg.Connection) -> dict[tuple[str, str], InstrumentState]:
    """Current state of every instrument, keyed by (market, exchange_symbol)."""
    rows = conn.execute(
        """
        SELECT i.market, i.exchange_symbol, i.instrument_id, i.status,
               COALESCE(s.consecutive_missing, 0), COALESCE(a.is_verified, FALSE)
        FROM instruments i
        LEFT JOIN instrument_sync_state s USING (instrument_id)
        LEFT JOIN symbol_aliases a
               ON a.instrument_id = i.instrument_id AND a.provider = 'yahoo'
              AND a.valid_to IS NULL
        """
    ).fetchall()
    return {(m, sym): InstrumentState(*rest) for m, sym, *rest in rows}


def set_instrument_status(conn: psycopg.Connection, instrument_id: int, status: str) -> None:
    conn.execute(
        "UPDATE instruments SET status = %s WHERE instrument_id = %s", (status, instrument_id)
    )


def backfill_candidates(
    conn: psycopg.Connection, tickers: list[str] | None = None, refetch_pending: bool = False
) -> list[tuple[int, str]]:
    """(instrument_id, yahoo_symbol) to backfill, in instrument_id order.

    Default: active instruments with a verified Yahoo mapping whose backfill is pending or
    was interrupted; plus those flagged needs_full_refetch when refetch_pending. Explicit
    `tickers` (Yahoo symbols) are selected regardless of backfill status.
    """
    rows = conn.execute(
        """
        SELECT i.instrument_id, a.provider_symbol
        FROM instruments i
        JOIN symbol_aliases a
          ON a.instrument_id = i.instrument_id AND a.provider = 'yahoo'
         AND a.valid_to IS NULL AND a.is_verified
        LEFT JOIN instrument_sync_state s ON s.instrument_id = i.instrument_id
        WHERE i.status = 'active'
          AND CASE WHEN %(tickers)s::text[] IS NOT NULL
                   THEN a.provider_symbol = ANY(%(tickers)s::text[])
                   ELSE COALESCE(s.backfill_status, 'pending') IN ('pending', 'in_progress')
                        OR (%(refetch)s AND COALESCE(s.needs_full_refetch, FALSE))
              END
        ORDER BY i.instrument_id
        """,
        {"tickers": tickers, "refetch": refetch_pending},
    ).fetchall()
    return [(iid, sym) for iid, sym in rows]
