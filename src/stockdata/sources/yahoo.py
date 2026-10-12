"""yfinance wrapper: batching, pacing, retries. Always auto_adjust=False (§7, §8)."""

from datetime import date

import pandas as pd


def download_prices(
    symbols: list[str], start: date, end: date | None = None
) -> dict[str, pd.DataFrame]:
    """Daily OHLCV + Adj Close + Dividends/Stock Splits per symbol.

    One yf.download call per batch (one start date per call). A failing symbol must not
    fail the batch; 429/5xx are retried with backoff, then reported.
    """
    raise NotImplementedError


def verify_symbol(symbol: str) -> tuple[bool, str | None]:
    """(has_data, currency) for a candidate Yahoo symbol."""
    raise NotImplementedError
