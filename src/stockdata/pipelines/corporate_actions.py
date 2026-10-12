"""Split/dividend extraction and full-refetch triggering (§5, §7)."""

import pandas as pd

from stockdata.models import CorporateAction


def extract_actions(instrument_id: int, df: pd.DataFrame) -> list[CorporateAction]:
    """Corporate actions from yfinance 'Stock Splits' / 'Dividends' columns (non-zero rows)."""
    raise NotImplementedError
