"""Apply SQL files to the configured database (STOCKDATA_DB_URL / DATABASE_URL) in one transaction.

uv run python scripts/apply_sql.py sql/001_init.sql
"""

import argparse
from pathlib import Path

from stockdata import db


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()
    with db.connect() as conn:
        for f in args.files:
            conn.execute(f.read_text())
            print(f"applied {f}")
        conn.commit()
        tables = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY 1"
        ).fetchall()
        print("tables:", ", ".join(t[0] for t in tables))


if __name__ == "__main__":
    main()
