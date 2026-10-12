from datetime import date

import pandas as pd

from stockdata.models import ActionType, CorporateAction, DataGap
from stockdata.quality import detect_gaps, detect_split_anomalies, invalid_ohlc_mask


def frame(rows: dict[str, tuple]) -> pd.DataFrame:
    """rows: {'YYYY-MM-DD': (open, high, low, close, volume)}"""
    df = pd.DataFrame.from_dict(
        rows, orient="index", columns=["open", "high", "low", "close", "volume"]
    )
    df.index = pd.to_datetime(df.index)
    return df


# --- invalid_ohlc_mask -------------------------------------------------------


def test_consistent_row_is_valid():
    df = frame({"2026-10-12": (10.0, 12.0, 9.0, 11.0, 1000)})
    assert invalid_ohlc_mask(df).tolist() == [False]


def test_low_above_open_or_close_is_invalid():
    df = frame({"2026-10-12": (10.0, 12.0, 10.5, 11.0, 1000)})
    assert invalid_ohlc_mask(df).tolist() == [True]


def test_high_below_open_or_close_is_invalid():
    df = frame({"2026-10-12": (10.0, 10.8, 9.0, 11.0, 1000)})
    assert invalid_ohlc_mask(df).tolist() == [True]


def test_row_with_missing_price_is_not_checked_for_ohlc():
    df = frame({"2026-10-12": (None, 12.0, 13.0, 11.0, 1000)})
    assert invalid_ohlc_mask(df).tolist() == [False]


def test_negative_volume_is_invalid_even_without_prices():
    df = frame({"2026-10-12": (None, None, None, None, -1)})
    assert invalid_ohlc_mask(df).tolist() == [True]


def test_mask_is_aligned_to_input_index():
    df = frame(
        {
            "2026-10-09": (10.0, 12.0, 9.0, 11.0, 1000),
            "2026-10-12": (10.0, 10.8, 9.0, 11.0, 1000),
        }
    )
    mask = invalid_ohlc_mask(df)
    assert mask.index.equals(df.index)
    assert mask.tolist() == [False, True]


# --- detect_split_anomalies --------------------------------------------------


def closes(values: dict[str, float]) -> pd.DataFrame:
    df = pd.DataFrame({"close": list(values.values())}, index=pd.to_datetime(list(values)))
    return df


def split(day: str, ratio: float) -> CorporateAction:
    return CorporateAction(
        instrument_id=1,
        action_date=date.fromisoformat(day),
        action_type=ActionType.SPLIT,
        value=ratio,
    )


# BORR.OL as returned by Yahoo (auto_adjust=False); 2021-12-13 was left unadjusted.
BORR_2021_12 = {
    "2021-12-08": 15.40,
    "2021-12-09": 15.60,
    "2021-12-10": 14.84,
    "2021-12-13": 29.76,
    "2021-12-14": 14.99,
    "2021-12-15": 15.20,
    "2021-12-16": 16.31,
    "2021-12-17": 16.05,
}


def test_borr_unadjusted_day_before_reverse_split_is_flagged():
    found = detect_split_anomalies(
        closes(BORR_2021_12), [split("2021-12-14", 0.5)], window_days=3, tolerance=0.15
    )
    assert found == [date(2021, 12, 13)]


def test_correctly_adjusted_series_has_no_anomaly():
    data = dict(BORR_2021_12, **{"2021-12-13": 15.10})
    found = detect_split_anomalies(
        closes(data), [split("2021-12-14", 0.5)], window_days=3, tolerance=0.15
    )
    assert found == []


def test_unadjusted_day_after_forward_split_is_flagged():
    # 5:1 split; the split day itself still shows the pre-split price level.
    data = {
        "2025-05-28": 300.0,
        "2025-05-29": 302.0,
        "2025-06-02": 298.0,
        "2025-06-03": 1495.0,
        "2025-06-04": 301.0,
        "2025-06-05": 303.0,
    }
    found = detect_split_anomalies(
        closes(data), [split("2025-06-03", 5.0)], window_days=3, tolerance=0.15
    )
    assert found == [date(2025, 6, 3)]


def test_jump_outside_window_is_not_attributed_to_split():
    data = {
        "2025-05-26": 600.0,  # 5 rows before the split: outside a ±3 window
        "2025-05-27": 300.0,
        "2025-05-28": 300.0,
        "2025-05-29": 302.0,
        "2025-06-02": 298.0,
        "2025-06-03": 299.0,
        "2025-06-04": 301.0,
        "2025-06-05": 303.0,
    }
    found = detect_split_anomalies(
        closes(data), [split("2025-06-03", 2.0)], window_days=3, tolerance=0.15
    )
    assert found == []


def test_jump_beyond_tolerance_of_ratio_is_not_flagged():
    # ratio 2.0, observed jump 1.6x: 20% off the ratio, outside 15% tolerance
    data = dict(BORR_2021_12, **{"2021-12-13": 24.0})
    found = detect_split_anomalies(
        closes(data), [split("2021-12-14", 0.5)], window_days=3, tolerance=0.15
    )
    assert found == []


def test_ratio_too_close_to_one_is_not_checked():
    # 1.1 split: a normal +10% day would otherwise look like an unadjusted day
    data = {
        "2025-06-02": 100.0,
        "2025-06-03": 110.0,
        "2025-06-04": 100.0,
        "2025-06-05": 101.0,
    }
    found = detect_split_anomalies(
        closes(data), [split("2025-06-03", 1.1)], window_days=3, tolerance=0.15
    )
    assert found == []


def test_dividends_are_ignored():
    dividend = CorporateAction(
        instrument_id=1,
        action_date=date(2021, 12, 14),
        action_type=ActionType.DIVIDEND,
        value=0.5,
    )
    found = detect_split_anomalies(closes(BORR_2021_12), [dividend], window_days=3, tolerance=0.15)
    assert found == []


def test_split_on_non_trading_day_uses_next_session():
    # 2021-12-12 is a Sunday; the window is centred on 2021-12-13
    found = detect_split_anomalies(
        closes(BORR_2021_12), [split("2021-12-12", 0.5)], window_days=3, tolerance=0.15
    )
    assert found == [date(2021, 12, 13)]


# --- detect_gaps ---------------------------------------------------------------

# Ten consecutive trading days (XOSL): Mon 2026-09-28 .. Fri 2026-10-09
SESSIONS = [
    date(2026, 9, 28),
    date(2026, 9, 29),
    date(2026, 9, 30),
    date(2026, 10, 1),
    date(2026, 10, 2),
    date(2026, 10, 5),
    date(2026, 10, 6),
    date(2026, 10, 7),
    date(2026, 10, 8),
    date(2026, 10, 9),
]


def without(*missing: date) -> list[date]:
    return [d for d in SESSIONS if d not in missing]


def test_complete_history_has_no_gaps():
    assert detect_gaps(7, SESSIONS, SESSIONS, min_trading_days=3) == []


def test_run_of_exactly_min_days_is_a_gap():
    have = without(date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5))
    assert detect_gaps(7, have, SESSIONS, min_trading_days=3) == [
        DataGap(instrument_id=7, start=date(2026, 10, 1), end=date(2026, 10, 5))
    ]


def test_run_shorter_than_min_is_ignored():
    have = without(date(2026, 10, 1), date(2026, 10, 2))
    assert detect_gaps(7, have, SESSIONS, min_trading_days=3) == []


def test_separate_runs_are_reported_separately():
    have = without(
        date(2026, 9, 29),
        date(2026, 9, 30),
        date(2026, 10, 1),  # run of 3
        date(2026, 10, 6),
        date(2026, 10, 7),
        date(2026, 10, 8),  # run of 3
    )
    assert detect_gaps(7, have, SESSIONS, min_trading_days=3) == [
        DataGap(instrument_id=7, start=date(2026, 9, 29), end=date(2026, 10, 1)),
        DataGap(instrument_id=7, start=date(2026, 10, 6), end=date(2026, 10, 8)),
    ]


def test_gap_running_to_the_end_is_reported():
    have = without(date(2026, 10, 7), date(2026, 10, 8), date(2026, 10, 9))
    assert detect_gaps(7, have, SESSIONS, min_trading_days=3) == [
        DataGap(instrument_id=7, start=date(2026, 10, 7), end=date(2026, 10, 9))
    ]


def test_rows_on_non_trading_days_do_not_break_a_gap():
    # Yahoo sometimes returns a row on a holiday/weekend; it is not an expected session
    have = without(date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5)) + [date(2026, 10, 3)]
    assert detect_gaps(7, sorted(have), SESSIONS, min_trading_days=3) == [
        DataGap(instrument_id=7, start=date(2026, 10, 1), end=date(2026, 10, 5))
    ]
