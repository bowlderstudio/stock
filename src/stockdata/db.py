"""PostgreSQL (Supabase) access. All writes are idempotent INSERT ... ON CONFLICT DO UPDATE."""

from collections.abc import Iterable

import psycopg

from stockdata.config import get_settings
from stockdata.models import CorporateAction, PriceRow


def connect() -> psycopg.Connection:
    """Open a connection via the Supabase pooler URL.

    prepare_threshold=None disables server-side prepared statements, which the
    transaction-mode pooler (port 6543) does not support.
    """
    url = get_settings().db_url
    if url is None:
        raise RuntimeError("STOCKDATA_DB_URL is not set")
    return psycopg.connect(url.get_secret_value(), prepare_threshold=None)


def upsert_prices(conn: psycopg.Connection, rows: Iterable[PriceRow]) -> int:
    """Upsert daily_prices on (instrument_id, trade_date, source_id); return rows written."""
    raise NotImplementedError


def upsert_corporate_actions(
    conn: psycopg.Connection, actions: Iterable[CorporateAction]
) -> list[CorporateAction]:
    """Insert corporate actions; return the ones not previously known (they trigger a full refetch)."""
    raise NotImplementedError
