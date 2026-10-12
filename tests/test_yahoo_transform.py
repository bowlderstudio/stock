from datetime import date

import numpy as np
import pandas as pd

from stockdata.models import PriceRow
from stockdata.sources.yahoo import normalize_history, to_price_rows

NAN = np.nan


def yahoo_history(rows: dict[str, tuple]) -> pd.DataFrame:
    """Shape of yf.Ticker(...).history(auto_adjust=False, actions=True) for an .OL symbol."""
    df = pd.DataFrame.from_dict(
        rows,
        orient="index",
        columns=[
            "Open",
            "High",
            "Low",
            "Close",
            "Adj Close",
            "Volume",
            "Dividends",
            "Stock Splits",
        ],
    )
    df.index = pd.DatetimeIndex(pd.to_datetime(list(rows))).tz_localize("Europe/Oslo")
    df.index.name = "Date"
    return df


RAW = yahoo_history(
    {
        "2026-10-07": (10.0, 11.0, 9.5, 10.5, 9.8, 1000, 0.0, 0.0),
        "2026-10-08": (10.5, 11.5, 10.0, 11.0, 10.3, 2000, 0.25, 0.0),
        "2026-10-09": (NAN, NAN, NAN, NAN, NAN, NAN, 0.0, 0.0),  # holiday-style empty row
        "2026-10-12": (11.0, 11.2, 10.8, 11.1, 11.1, 500, 0.0, 0.0),  # intraday, not closed
    }
)


def test_normalize_uses_lowercase_columns_and_naive_trade_dates():
    df = normalize_history(RAW, last_closed=date(2026, 10, 12))
    assert {
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "dividends",
        "stock_splits",
    } <= set(df.columns)
    assert df.index[0] == pd.Timestamp("2026-10-07")
    assert df.index.tz is None


def test_normalize_drops_rows_without_any_price():
    df = normalize_history(RAW, last_closed=date(2026, 10, 12))
    assert pd.Timestamp("2026-10-09") not in df.index


def test_normalize_drops_rows_after_last_closed_session():
    df = normalize_history(RAW, last_closed=date(2026, 10, 9))
    assert [ts.date() for ts in df.index] == [date(2026, 10, 7), date(2026, 10, 8)]


def test_to_price_rows_maps_values_and_missing_volume():
    raw = yahoo_history({"2026-10-07": (10.0, 11.0, 9.5, 10.5, 9.8, NAN, 0.0, 0.0)})
    rows = to_price_rows(42, normalize_history(raw, last_closed=date(2026, 10, 7)))
    assert rows == [
        PriceRow(
            instrument_id=42,
            trade_date=date(2026, 10, 7),
            open=10.0,
            high=11.0,
            low=9.5,
            close=10.5,
            adj_close=9.8,
            volume=None,
        )
    ]
    assert isinstance(rows[0].open, float)


def test_to_price_rows_volume_is_python_int():
    rows = to_price_rows(42, normalize_history(RAW, last_closed=date(2026, 10, 8)))
    assert [r.volume for r in rows] == [1000, 2000]
    assert all(type(r.volume) is int for r in rows)


# --- verify_symbol -------------------------------------------------------------


class FakeTicker:
    def __init__(self, history: pd.DataFrame, metadata: dict | None = None, error=None):
        self._history, self.history_metadata, self._error = history, metadata or {}, error

    def history(self, **kwargs):
        if self._error:
            raise self._error
        return self._history


def test_verify_symbol_returns_currency_from_history_metadata():
    from stockdata.sources.yahoo import verify_symbol

    fake = FakeTicker(RAW, {"currency": "NOK"})
    assert verify_symbol("EQNR.OL", ticker_factory=lambda s: fake) == (True, "NOK")


def test_verify_symbol_empty_history_means_no_data():
    from stockdata.sources.yahoo import verify_symbol

    fake = FakeTicker(RAW.iloc[0:0])
    assert verify_symbol("NOPE.OL", ticker_factory=lambda s: fake) == (False, None)


def test_verify_symbol_lets_rate_limit_errors_propagate():
    import pytest

    from stockdata.sources.yahoo import verify_symbol

    class RateLimited(Exception):
        pass

    fake = FakeTicker(RAW, error=RateLimited("Too Many Requests"))
    with pytest.raises(RateLimited):
        verify_symbol("EQNR.OL", ticker_factory=lambda s: fake)
