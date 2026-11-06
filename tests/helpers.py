"""Shared builders for pipeline tests (yfinance-shaped frames, seeded instruments)."""

from datetime import date

import pandas as pd

from stockdata import db
from stockdata.models import Instrument

COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends", "Stock Splits"]


def history(rows: dict[str, tuple]) -> pd.DataFrame:
    """Shape of yf.Ticker(...).history(auto_adjust=False, actions=True) for an .OL symbol."""
    df = pd.DataFrame.from_dict(rows, orient="index", columns=COLUMNS)
    df.index = pd.DatetimeIndex(pd.to_datetime(list(rows))).tz_localize("Europe/Oslo")
    return df


def add(
    conn,
    symbol: str,
    status: str = "active",
    verified: bool = True,
    backfill: str = "pending",
    refetch: bool = False,
    last_price: date | None = None,
) -> int:
    iid = db.upsert_instrument(conn, Instrument(None, symbol, symbol, "XOSL", "NOK"))
    db.set_instrument_status(conn, iid, status)
    db.set_provider_symbol(conn, iid, "yahoo", f"{symbol}.OL", verified)
    db.update_sync_state(
        conn, iid, backfill_status=backfill, needs_full_refetch=refetch, last_price_date=last_price
    )
    conn.commit()
    return iid
