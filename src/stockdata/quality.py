"""Data quality checks (design doc §7). Pure functions: no network, no database."""

from datetime import date

import pandas as pd

from stockdata.models import CorporateAction, DataGap


def invalid_ohlc_mask(df: pd.DataFrame) -> pd.Series:
    """True for rows violating low <= min(open, close), high >= max(open, close),
    low <= high, or volume >= 0. Rows with any null OHLC are not checked."""
    raise NotImplementedError


def detect_split_anomalies(
    df: pd.DataFrame, splits: list[CorporateAction], window_days: int, tolerance: float
) -> list[date]:
    """Dates near a split where Close jumps by ~the split ratio (or its inverse) vs. the
    adjacent day, i.e. Yahoo left that day unadjusted (e.g. BORR.OL 2021-12-13)."""
    raise NotImplementedError


def detect_gaps(
    instrument_id: int, have: list[date], expected: list[date], min_trading_days: int
) -> list[DataGap]:
    """Runs of at least `min_trading_days` expected trading days with no price row."""
    raise NotImplementedError
