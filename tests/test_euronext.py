from datetime import date

import pytest
from openpyxl import Workbook

from stockdata.models import Instrument
from stockdata.sources.euronext import candidate_yahoo_symbol, parse_list

HEADER = [
    "Name", "ISIN", "Symbol", "Market", "Currency", "Open Price", "High Price", "low Price",
    "last Price", "last Trade MIC Time", "Time Zone", "Volume", "Turnover", "Closing Price",
    "Closing Price DateTime",
]  # fmt: skip


def write_export(path, rows, as_of="12 Oct 2026"):
    """Same layout as the Euronext 'Download' xlsx: header, 3 metadata rows, data rows."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Simple"
    ws.append(HEADER)
    ws.append(["European Equities"])
    ws.append([as_of])
    ws.append(["All datapoints provided as of end of last active trading day."])
    for name, isin, symbol, market, currency in rows:
        ws.append([name, isin, symbol, market, currency, "1", "1", "1", "1",
                   "09/10/2026 16:25", "CET", "100", "100", "1", "09/10/2026"])  # fmt: skip
    wb.save(path)
    return path


def test_parses_instruments_with_market_mic(tmp_path):
    f = write_export(
        tmp_path / "list.xlsx",
        [
            ("ALPHA SHIPPING", "NO0000000001", "ALPHA", "Oslo Børs", "NOK"),
            ("BETA TECH", "SE0000000002", "BETA", "Euronext Expand Oslo", "NOK"),
            ("GAMMA SPAREBANK", "NO0000000003", "GAMMA", "Euronext Growth Oslo", "EUR"),
        ],
    )
    result = parse_list(f)
    assert result.instruments == [
        Instrument("NO0000000001", "ALPHA", "ALPHA SHIPPING", "XOSL", "NOK"),
        Instrument("SE0000000002", "BETA", "BETA TECH", "XOAS", "NOK"),
        Instrument("NO0000000003", "GAMMA", "GAMMA SPAREBANK", "MERK", "EUR"),
    ]


def test_reads_list_date_from_metadata_row(tmp_path):
    f = write_export(tmp_path / "list.xlsx", [("A", "NO0000000001", "A", "Oslo Børs", "NOK")])
    assert parse_list(f).as_of == date(2026, 10, 12)


def test_strips_whitespace(tmp_path):
    f = write_export(
        tmp_path / "list.xlsx", [(" ALPHA ", " NO0000000001", "ALPHA ", "Oslo Børs ", " NOK")]
    )
    assert parse_list(f).instruments == [
        Instrument("NO0000000001", "ALPHA", "ALPHA", "XOSL", "NOK")
    ]


def test_unknown_market_is_an_error(tmp_path):
    f = write_export(tmp_path / "list.xlsx", [("A", "FR0000000001", "A", "Euronext Paris", "EUR")])
    with pytest.raises(ValueError, match="Euronext Paris"):
        parse_list(f)


def test_yahoo_candidate_uses_oslo_suffix():
    assert candidate_yahoo_symbol("EQNR") == "EQNR.OL"
