"""One-off load of Yahoo daily history for a list of Oslo symbols into the database.

Interim stand-in for Flow C (backfill_history) until the Euronext instrument list (Flow A)
exists: instruments are created from Yahoo metadata with market 'XOSL' as a placeholder
segment and no ISIN. All rows are written; quality findings are logged to ingestion_errors.

    uv run python scripts/load_yahoo_history.py                 # default 21 symbols, 10y
    uv run python scripts/load_yahoo_history.py --tickers EQNR.OL,DNB.OL --period 1y
"""

import argparse
import random
import time
import uuid

import pandas as pd
import yfinance as yf

from stockdata import db
from stockdata.config import get_settings
from stockdata.models import ActionType, BackfillStatus, ErrorType, Instrument
from stockdata.pipelines.corporate_actions import extract_actions
from stockdata.quality import detect_gaps, detect_split_anomalies, invalid_ohlc_mask
from stockdata.sources.yahoo import normalize_history, to_price_rows
from stockdata.trading_calendar import last_closed_session, trading_days

DEFAULT_TICKERS = [
    "EQNR.OL", "DNB.OL", "NHY.OL", "TEL.OL", "ORK.OL",
    "MOWI.OL", "YAR.OL", "AKRBP.OL", "FRO.OL", "KOG.OL",
    "SALM.OL", "STB.OL", "GJF.OL", "SUBC.OL", "TOM.OL",
    "NAS.OL", "SCATC.OL", "BAKKA.OL", "AUSS.OL", "VAR.OL",
    "BORR.OL",
]  # fmt: skip


def load_symbol(conn, run_id, symbol: str, period: str) -> tuple[int, int]:
    """Fetch and store one symbol; return (rows_upserted, quality_findings)."""
    settings = get_settings()
    ticker = yf.Ticker(symbol)
    raw = ticker.history(period=period, auto_adjust=False, actions=True)
    if raw.empty:
        raise ValueError("empty result")
    info = ticker.info
    currency = info.get("currency")
    if not currency:
        raise ValueError("no currency in Yahoo metadata")
    name = info.get("longName") or info.get("shortName") or symbol

    iid = db.upsert_instrument(
        conn,
        Instrument(
            isin=None,
            exchange_symbol=symbol.removesuffix(".OL"),
            name=name,
            market="XOSL",
            currency=currency,
        ),
    )
    db.set_provider_symbol(conn, iid, "yahoo", symbol, verified=True)

    df = normalize_history(raw, last_closed=last_closed_session())
    rows = db.upsert_prices(conn, to_price_rows(iid, df))
    actions = extract_actions(iid, df)
    db.upsert_corporate_actions(conn, actions)

    findings = 0
    for ts in df.index[invalid_ohlc_mask(df)]:
        r = df.loc[ts]
        db.log_error(
            conn,
            run_id,
            iid,
            ErrorType.QUALITY_CHECK,
            f"{ts.date()} OHLC inconsistent: o={r.open} h={r.high} l={r.low} "
            f"c={r.close} v={r.volume}",
        )
        findings += 1
    splits = [a for a in actions if a.action_type == ActionType.SPLIT]
    for day in detect_split_anomalies(
        df, splits, settings.split_anomaly_window_days, settings.split_anomaly_tolerance
    ):
        db.log_error(
            conn,
            run_id,
            iid,
            ErrorType.SPLIT_ANOMALY,
            f"{day} close off by ~split ratio vs. neighbours",
        )
        findings += 1
    have = [ts.date() for ts in df.index]
    expected = trading_days(have[0], have[-1])
    for gap in detect_gaps(iid, have, expected, settings.data_gap_trading_days):
        db.log_error(
            conn,
            run_id,
            iid,
            ErrorType.DATA_GAP,
            f"no rows {gap.start} .. {gap.end} (unclassified)",
        )
        findings += 1

    db.update_sync_state(
        conn,
        iid,
        backfill_status=BackfillStatus.DONE,
        backfilled_from=have[0],
        last_price_date=have[-1],
        last_success_at=pd.Timestamp.now(tz="UTC"),
        consecutive_failures=0,
        needs_full_refetch=False,
    )
    return rows, findings


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--tickers", default=",".join(DEFAULT_TICKERS))
    parser.add_argument("--period", default="10y")
    args = parser.parse_args()
    symbols = [t.strip() for t in args.tickers.split(",") if t.strip()]
    s = get_settings()

    run_id = uuid.uuid4()
    ok = total_rows = errors = 0
    with db.connect() as conn:
        db.start_run(conn, run_id, "backfill_history:load_yahoo_history")
        conn.commit()
        for i, symbol in enumerate(symbols):
            if i:
                time.sleep(random.uniform(s.batch_delay_min_s, s.batch_delay_max_s))
            try:
                rows, findings = load_symbol(conn, run_id, symbol, args.period)
                conn.commit()
                ok += 1
                total_rows += rows
                errors += findings
                print(f"{symbol:10} rows={rows:5}  quality_findings={findings}", flush=True)
            except Exception as exc:  # noqa: BLE001 - one symbol must not stop the run
                conn.rollback()
                db.log_error(
                    conn,
                    run_id,
                    None,
                    ErrorType.EMPTY_RESULT if isinstance(exc, ValueError) else ErrorType.HTTP_ERROR,
                    f"{symbol}: {type(exc).__name__}: {exc}"[:500],
                )
                conn.commit()
                errors += 1
                print(f"{symbol:10} FAILED {type(exc).__name__}: {exc}", flush=True)
        status = "completed" if ok == len(symbols) else "completed_with_failures"
        db.finish_run(
            conn,
            run_id,
            status,
            total_symbols=len(symbols),
            successful_symbols=ok,
            rows_upserted=total_rows,
            error_count=errors,
        )
        conn.commit()
    print(
        f"\nrun {run_id}: {ok}/{len(symbols)} symbols, {total_rows} rows, {errors} errors/findings"
    )
    return 0 if ok == len(symbols) else 1


if __name__ == "__main__":
    raise SystemExit(main())
