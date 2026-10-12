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


def test_database_url_is_accepted_and_normalized_for_psycopg(monkeypatch):
    monkeypatch.delenv("STOCKDATA_DB_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h.neon.tech/stocks?ssl=require")
    s = Settings(_env_file=None)
    assert s.db_url.get_secret_value() == "postgresql://u:p@h.neon.tech/stocks?sslmode=require"


def test_stockdata_db_url_takes_precedence(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://other@h/db")
    monkeypatch.setenv("STOCKDATA_DB_URL", "postgresql://mine@h/db")
    s = Settings(_env_file=None)
    assert s.db_url.get_secret_value() == "postgresql://mine@h/db"
