"""Euronext Oslo instrument list (Oslo Børs, Euronext Expand Oslo, Euronext Growth Oslo).

Automated download is only allowed if the site terms permit it; a manually downloaded CSV
(scripts/import_euronext_csv.py) is the fallback. Keep the raw snapshot and its fetch time.
"""

from pathlib import Path

from stockdata.models import Instrument


def parse_csv(path: Path) -> list[Instrument]:
    """Parse an Euronext equities CSV export into instruments (currency taken from the file)."""
    raise NotImplementedError


def candidate_yahoo_symbol(exchange_symbol: str) -> str:
    """Best-guess Yahoo symbol, usually '<symbol>.OL'. Must be verified before use."""
    raise NotImplementedError
