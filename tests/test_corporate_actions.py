from datetime import date

import pandas as pd

from stockdata.models import ActionType, CorporateAction
from stockdata.pipelines.corporate_actions import extract_actions


def test_extracts_non_zero_splits_and_dividends():
    df = pd.DataFrame(
        {"dividends": [0.0, 0.25, 0.0], "stock_splits": [0.0, 0.0, 5.0]},
        index=pd.to_datetime(["2025-05-28", "2025-05-29", "2025-06-03"]),
    )
    assert extract_actions(3, df) == [
        CorporateAction(3, date(2025, 5, 29), ActionType.DIVIDEND, 0.25),
        CorporateAction(3, date(2025, 6, 3), ActionType.SPLIT, 5.0),
    ]


def test_no_actions_columns_means_no_actions():
    df = pd.DataFrame({"close": [1.0]}, index=pd.to_datetime(["2025-06-03"]))
    assert extract_actions(3, df) == []
