"""Data quality checks (design doc §7). Pure functions: no network, no database.

Inputs are normalized price frames: lowercase columns open/high/low/close/volume,
indexed by trade date.
"""

from bisect import bisect_left
from datetime import date

import pandas as pd

from stockdata.models import ActionType, CorporateAction, DataGap


def invalid_ohlc_mask(df: pd.DataFrame) -> pd.Series:
    """True for rows violating low <= min(open, close) or high >= max(open, close)
    (together these imply low <= high), or with negative volume.
    OHLC rules are only applied when all four prices are present."""
    ohlc = df[["open", "high", "low", "close"]]
    body_low = ohlc[["open", "close"]].min(axis=1)
    body_high = ohlc[["open", "close"]].max(axis=1)
    bad_prices = ohlc.notna().all(axis=1) & ((df["low"] > body_low) | (df["high"] < body_high))
    bad_volume = df["volume"] < 0
    return bad_prices | bad_volume


def detect_split_anomalies(
    df: pd.DataFrame, splits: list[CorporateAction], window_days: int, tolerance: float
) -> list[date]:
    """Dates within ±window_days rows of a split whose Close is off by ~the split ratio
    (or its inverse), i.e. Yahoo left that day unadjusted (e.g. BORR.OL 2021-12-13).

    Each day is compared with the median Close of the other days in the window, so the
    correct day next to an outlier is not flagged. Splits whose ratio is so close to 1
    that a normal daily move would match within tolerance are skipped.
    """
    trade_dates = [ts.date() for ts in pd.DatetimeIndex(df.index)]
    close = df["close"].to_numpy()
    min_checkable = (1 + tolerance) / (1 - tolerance)
    found: set[date] = set()

    for action in splits:
        if action.action_type != ActionType.SPLIT:
            continue
        ratio = action.value
        if max(ratio, 1 / ratio) <= min_checkable:
            continue
        center = bisect_left(trade_dates, action.action_date)
        lo, hi = max(center - window_days, 0), min(center + window_days + 1, len(trade_dates))
        for i in range(lo, hi):
            others = [close[j] for j in range(lo, hi) if j != i]
            if not others or pd.isna(close[i]):
                continue
            jump = close[i] / pd.Series(others).median()
            if any(abs(jump / r - 1) <= tolerance for r in (ratio, 1 / ratio)):
                found.add(trade_dates[i])

    return sorted(found)


def detect_gaps(
    instrument_id: int, have: list[date], expected: list[date], min_trading_days: int
) -> list[DataGap]:
    """Runs of at least `min_trading_days` consecutive expected sessions with no price row.

    `expected` comes from the trading calendar and defines the range checked; the caller
    chooses it (typically first stored date .. last closed session). Gaps are unclassified.
    """
    have_set = set(have)
    gaps: list[DataGap] = []
    run: list[date] = []
    for day in [*expected, None]:  # None flushes a run that reaches the end
        if day is not None and day not in have_set:
            run.append(day)
            continue
        if len(run) >= min_trading_days:
            gaps.append(DataGap(instrument_id=instrument_id, start=run[0], end=run[-1]))
        run = []
    return gaps
