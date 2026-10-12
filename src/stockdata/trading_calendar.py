"""Oslo Børs trading calendar (exchange_calendars XOSL)."""

from datetime import date
from functools import lru_cache

import exchange_calendars as xcals
import pandas as pd

from stockdata.config import get_settings


@lru_cache
def _calendar() -> xcals.ExchangeCalendar:
    return xcals.get_calendar(get_settings().exchange_calendar)


def is_trading_day(day: date) -> bool:
    return _calendar().is_session(pd.Timestamp(day))


def trading_days(start: date, end: date) -> list[date]:
    """Trading sessions in [start, end], inclusive."""
    sessions = _calendar().sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
    return [s.date() for s in sessions]


def last_closed_session(now: pd.Timestamp | None = None) -> date:
    """Most recent session whose close is at or before `now` (UTC if naive).

    Rows dated after this are intraday/partial and must not be stored (§7).
    """
    cal = _calendar()
    now = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    session = cal.date_to_session(now.tz_convert(None).normalize(), direction="previous")
    while cal.session_close(session) > now:
        session = cal.previous_session(session)
    return session.date()
