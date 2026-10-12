"""yfinance wrapper: batching, pacing, retries. Always auto_adjust=False (§7, §8)."""

from datetime import date

import pandas as pd
import yfinance as yf

from stockdata.models import PriceRow

_PRICE_COLUMNS = ["open", "high", "low", "close"]


def normalize_history(raw: pd.DataFrame, last_closed: date) -> pd.DataFrame:
    """yfinance history (auto_adjust=False, actions=True) -> normalized frame.

    Lowercase snake_case columns, tz-naive trade-date index (exchange local date).
    Drops rows with no price at all (Yahoo holiday rows) and rows after `last_closed`
    (intraday, not final).
    """
    df = raw.rename(columns=lambda c: c.lower().replace(" ", "_"))
    df.index = pd.DatetimeIndex(df.index).tz_localize(None).normalize()
    df = df[df[_PRICE_COLUMNS].notna().any(axis=1)]
    return df[df.index <= pd.Timestamp(last_closed)]


def _num(v) -> float | None:
    return None if pd.isna(v) else float(v)


def to_price_rows(instrument_id: int, df: pd.DataFrame) -> list[PriceRow]:
    """Normalized frame -> PriceRow list with plain Python types (None for missing)."""
    return [
        PriceRow(
            instrument_id=instrument_id,
            trade_date=ts.date(),
            open=_num(r.open),
            high=_num(r.high),
            low=_num(r.low),
            close=_num(r.close),
            adj_close=_num(r.adj_close),
            volume=None if pd.isna(r.volume) else int(r.volume),
        )
        for ts, r in zip(df.index, df.itertuples(index=False), strict=True)
    ]


def download_prices(
    symbols: list[str], start: date, end: date | None = None
) -> dict[str, pd.DataFrame]:
    """Daily OHLCV + Adj Close + Dividends/Stock Splits per symbol.

    One yf.download call per batch (one start date per call). A failing symbol must not
    fail the batch; 429/5xx are retried with backoff, then reported.
    """
    raise NotImplementedError


def verify_symbol(symbol: str, ticker_factory=yf.Ticker) -> tuple[bool, str | None]:
    """(has_data, currency) for a candidate Yahoo symbol, from one month of history.

    Unknown symbols come back as empty history. Request errors (e.g. rate limiting) are
    raised, never reported as "no data", so they cannot push a symbol into mapping_review.
    """
    ticker = ticker_factory(symbol)
    history = ticker.history(period="1mo", auto_adjust=False)
    if history.empty:
        return False, None
    return True, ticker.history_metadata.get("currency")
