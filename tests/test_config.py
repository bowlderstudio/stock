from stockdata.config import Settings


def test_defaults_match_design_doc():
    s = Settings(_env_file=None)
    assert s.batch_size == 25
    assert s.split_anomaly_tolerance == 0.15
    assert s.data_gap_trading_days == 10
    assert s.exchange_calendar == "XOSL"


def test_env_override(monkeypatch):
    monkeypatch.setenv("STOCKDATA_BATCH_SIZE", "10")
    monkeypatch.setenv("STOCKDATA_DB_URL", "postgresql://u:secret@h/db")
    s = Settings(_env_file=None)
    assert s.batch_size == 10
    assert "secret" not in repr(s)
