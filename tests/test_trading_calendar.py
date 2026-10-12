from datetime import date

import pandas as pd

from stockdata.trading_calendar import is_trading_day, last_closed_session, trading_days


def test_weekend_and_holidays_are_not_trading_days():
    assert not is_trading_day(date(2026, 10, 10))  # Saturday
    assert not is_trading_day(date(2026, 4, 3))  # Good Friday
    assert not is_trading_day(date(2026, 5, 14))  # Ascension Day
    assert is_trading_day(date(2026, 10, 12))  # Monday


def test_trading_days_inclusive_range():
    assert trading_days(date(2026, 10, 9), date(2026, 10, 12)) == [
        date(2026, 10, 9),
        date(2026, 10, 12),
    ]


def test_last_closed_session_excludes_open_session():
    # Oslo closes 16:20 local = 14:20 UTC in summer time.
    assert last_closed_session(pd.Timestamp("2026-10-12 12:00", tz="UTC")) == date(2026, 10, 9)
    assert last_closed_session(pd.Timestamp("2026-10-12 15:00", tz="UTC")) == date(2026, 10, 12)
    assert last_closed_session(pd.Timestamp("2026-10-11 12:00", tz="UTC")) == date(2026, 10, 9)
