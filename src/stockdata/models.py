"""Domain types shared by sources, pipelines and the database layer (schema: sql/001_init.sql)."""

from dataclasses import dataclass
from datetime import date
from enum import StrEnum


class InstrumentStatus(StrEnum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    INACTIVE_CANDIDATE = "inactive_candidate"
    INACTIVE = "inactive"
    MAPPING_REVIEW = "mapping_review"


class BackfillStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    NO_DATA = "no_data"


class ErrorType(StrEnum):
    RATE_LIMITED = "rate_limited"
    HTTP_ERROR = "http_error"
    EMPTY_RESULT = "empty_result"
    MAPPING_REVIEW = "mapping_review"
    CURRENCY_MISMATCH = "currency_mismatch"
    QUALITY_CHECK = "quality_check"
    SPLIT_ANOMALY = "split_anomaly"
    DATA_GAP = "data_gap"
    SYNC_FAILED = "sync_failed"


class GapKind(StrEnum):
    SUSPENDED = "suspended"
    DELISTED = "delisted"
    SOURCE_GAP = "source_gap"


class ActionType(StrEnum):
    SPLIT = "split"
    DIVIDEND = "dividend"


SOURCE_YAHOO = 1  # daily_prices.source_id / corporate_actions.source_id


@dataclass(frozen=True)
class Instrument:
    isin: str | None
    exchange_symbol: str
    name: str
    market: str
    currency: str
    instrument_type: str = "common_stock"
    instrument_id: int | None = None


@dataclass(frozen=True)
class PriceRow:
    instrument_id: int
    trade_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None  # Yahoo Close: split-adjusted, not dividend-adjusted
    adj_close: float | None  # Yahoo Adj Close: split + dividend adjusted
    volume: int | None
    source_id: int = SOURCE_YAHOO


@dataclass(frozen=True)
class CorporateAction:
    instrument_id: int
    action_date: date
    action_type: ActionType
    value: float  # split ratio, or dividend per share in quote currency
    source_id: int = SOURCE_YAHOO


@dataclass(frozen=True)
class DataGap:
    instrument_id: int
    start: date
    end: date
    kind: GapKind | None = None  # None until classified
