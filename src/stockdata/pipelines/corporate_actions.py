"""Split/dividend extraction and full-refetch triggering (§5, §7)."""

import pandas as pd

from stockdata.models import ActionType, CorporateAction

_COLUMNS = (("dividends", ActionType.DIVIDEND), ("stock_splits", ActionType.SPLIT))


def extract_actions(instrument_id: int, df: pd.DataFrame) -> list[CorporateAction]:
    """Corporate actions from a normalized history frame's dividends / stock_splits columns
    (non-zero rows), in date order."""
    actions = []
    for ts in df.index:
        for column, action_type in _COLUMNS:
            value = df.at[ts, column] if column in df.columns else 0
            if value and not pd.isna(value):
                actions.append(CorporateAction(instrument_id, ts.date(), action_type, float(value)))
    return actions
