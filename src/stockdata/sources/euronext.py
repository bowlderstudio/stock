"""Euronext Oslo instrument list (Oslo Børs, Euronext Expand Oslo, Euronext Growth Oslo).

Euronext's terms of use forbid automated access and systematic retrieval, so the list is
downloaded manually from https://live.euronext.com/en/markets/oslo/equities/list
("Download", Excel) and imported with scripts/import_euronext_list.py. Keep downloads in
data/ (gitignored): personal use only, not for redistribution.
"""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from stockdata.models import Instrument

MARKET_MIC = {
    "Oslo Børs": "XOSL",
    "Euronext Expand Oslo": "XOAS",
    "Euronext Growth Oslo": "MERK",
}

_COLUMNS = ["Name", "ISIN", "Symbol", "Market", "Currency"]


@dataclass(frozen=True)
class EuronextList:
    as_of: date
    instruments: list[Instrument]


def parse_list(path: Path) -> EuronextList:
    """Parse the Euronext equities 'Download' xlsx: a header row, three metadata rows
    (title, list date such as '12 Oct 2026', disclaimer), then one row per instrument."""
    raw = pd.read_excel(path, header=None, dtype=str)
    header = [str(c).strip() for c in raw.iloc[0]]
    missing = [c for c in _COLUMNS if c not in header]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")
    as_of = datetime.strptime(str(raw.iloc[2, 0]).strip(), "%d %b %Y").date()

    body = raw.iloc[4:].copy()
    body.columns = header
    body = body[_COLUMNS].dropna(subset=["ISIN", "Symbol"])
    body = body.apply(lambda col: col.str.strip())

    unknown = sorted(set(body["Market"]) - set(MARKET_MIC))
    if unknown:
        raise ValueError(f"{path}: unknown market(s) {unknown}")

    instruments = [
        Instrument(
            isin=r.ISIN,
            exchange_symbol=r.Symbol,
            name=r.Name,
            market=MARKET_MIC[r.Market],
            currency=r.Currency,
        )
        for r in body.itertuples(index=False)
    ]
    return EuronextList(as_of=as_of, instruments=instruments)


def candidate_yahoo_symbol(exchange_symbol: str) -> str:
    """Best-guess Yahoo symbol, '<symbol>.OL'. Must be verified before use."""
    return f"{exchange_symbol}.OL"
