import os
from pathlib import Path

import psycopg
import pytest

# Local disposable database only — never point this at Neon/production.
# docker run -d --name stockdata-test-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=stockdata_test \
#   -p 127.0.0.1:55432:5432 postgres:17
TEST_DB_URL = os.environ.get(
    "STOCKDATA_TEST_DB_URL", "postgresql://postgres:test@127.0.0.1:55432/stockdata_test"
)
SCHEMA = Path(__file__).parent.parent / "sql" / "001_init.sql"


@pytest.fixture
def conn():
    try:
        c = psycopg.connect(TEST_DB_URL, prepare_threshold=None, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("test PostgreSQL not available (see tests/conftest.py)")
    with c:
        c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        c.execute(SCHEMA.read_text())
        c.commit()
        yield c
