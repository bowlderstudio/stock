"""Check whether Yahoo Finance serves data to the current network (e.g. a cloud runner IP).

Runs three probes and prints a summary (also appended to $GITHUB_STEP_SUMMARY if set):
  1. single-ticker 1-year history for each ticker, with splits/dividends
  2. one batched yf.download call for all tickers
  3. 10-year history for stocks with known splits/reverse splits (simulates backfill
     and checks that Yahoo's Close is split-adjusted, i.e. continuous across split dates)

Exits non-zero when the single-ticker success rate is below --min-success-rate.
"""

import argparse
import math
import os
import random
import sys
import time

import yfinance as yf

DEFAULT_TICKERS = [
    "EQNR.OL", "DNB.OL", "NHY.OL", "TEL.OL", "ORK.OL",
    "MOWI.OL", "YAR.OL", "AKRBP.OL", "FRO.OL", "KOG.OL",
    "SALM.OL", "STB.OL", "GJF.OL", "SUBC.OL", "TOM.OL",
    "NAS.OL", "SCATC.OL", "BAKKA.OL", "AUSS.OL", "VAR.OL",
    "BORR.OL",
]

# Known corporate actions (per Yahoo): KOG 5:1 split 2025-06-03, TOM 2:1 split 2022-05-27,
# BORR reverse splits 2019-06-26 (0.2) and 2021-12-14 (0.5). EQNR has none (control).
DEFAULT_BACKFILL_TICKERS = ["EQNR.OL", "KOG.OL", "TOM.OL", "BORR.OL"]


def split_adjusted(df) -> str:
    """'yes' if Close shows no jump of the split ratio's size on any split date."""
    if "Stock Splits" not in df:
        return "-"
    checks = []
    for i, ratio in enumerate(df["Stock Splits"]):
        if ratio == 0 or i == 0:
            continue
        jump = df["Close"].iloc[i] / df["Close"].iloc[i - 1]
        checks.append(abs(math.log(jump)) < abs(math.log(ratio)) / 2)
    if not checks:
        return "-"
    return "yes" if all(checks) else "NO"


def probe_single(ticker: str, period: str) -> dict:
    start = time.monotonic()
    try:
        df = yf.Ticker(ticker).history(period=period, auto_adjust=False, actions=True)
        rows = len(df)
        result = {
            "ok": rows > 0,
            "rows": rows,
            "splits": int((df["Stock Splits"] != 0).sum()) if "Stock Splits" in df else 0,
            "dividends": int((df["Dividends"] != 0).sum()) if "Dividends" in df else 0,
            "split_adjusted": split_adjusted(df),
            "error": "" if rows > 0 else "empty result",
        }
    except Exception as exc:  # yfinance raises various types, incl. rate-limit errors
        result = {"ok": False, "rows": 0, "splits": 0, "dividends": 0, "split_adjusted": "-",
                  "error": f"{type(exc).__name__}: {exc}"[:200]}
    result["seconds"] = round(time.monotonic() - start, 2)
    return result


def probe_batch(tickers: list[str]) -> dict:
    start = time.monotonic()
    try:
        df = yf.download(tickers, period="1mo", auto_adjust=False,
                         group_by="ticker", progress=False, threads=False)
        got = [t for t in tickers if t in df.columns.get_level_values(0)
               and df[t]["Close"].notna().any()]
        result = {"ok": len(got), "total": len(tickers),
                  "missing": sorted(set(tickers) - set(got)), "error": ""}
    except Exception as exc:
        result = {"ok": 0, "total": len(tickers), "missing": tickers,
                  "error": f"{type(exc).__name__}: {exc}"[:200]}
    result["seconds"] = round(time.monotonic() - start, 2)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", default=",".join(DEFAULT_TICKERS),
                        help="comma-separated Yahoo symbols")
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=5.0)
    parser.add_argument("--backfill-tickers", default=",".join(DEFAULT_BACKFILL_TICKERS),
                        help="comma-separated Yahoo symbols to fetch 10y history for")
    parser.add_argument("--min-success-rate", type=float, default=0.95)
    args = parser.parse_args()

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    lines = [f"## Yahoo Finance reachability test (yfinance {yf.__version__})", ""]

    lines += ["### 1. Single-ticker, 1y, actions=True", "",
              "| ticker | ok | rows | splits | dividends | sec | error |",
              "|---|---|---|---|---|---|---|"]
    single = {}
    for i, t in enumerate(tickers):
        if i:
            time.sleep(random.uniform(args.delay_min, args.delay_max))
        r = probe_single(t, "1y")
        single[t] = r
        lines.append(f"| {t} | {'✅' if r['ok'] else '❌'} | {r['rows']} | {r['splits']} "
                     f"| {r['dividends']} | {r['seconds']} | {r['error']} |")
        print(t, r, flush=True)
    ok_count = sum(r["ok"] for r in single.values())
    rate = ok_count / len(tickers) if tickers else 0.0
    lines += ["", f"Success: **{ok_count}/{len(tickers)} ({rate:.0%})**", ""]

    time.sleep(random.uniform(args.delay_min, args.delay_max))
    b = probe_batch(tickers)
    print("batch", b, flush=True)
    lines += ["### 2. Batched yf.download, 1mo", "",
              f"Got **{b['ok']}/{b['total']}** in {b['seconds']}s. "
              f"Missing: {', '.join(b['missing']) or 'none'}. {b['error']}", ""]

    backfill = [t.strip() for t in args.backfill_tickers.split(",") if t.strip()]
    lines += ["### 3. 10y history (backfill simulation + split adjustment check)", "",
              "| ticker | ok | rows | splits | dividends | Close split-adjusted | sec | error |",
              "|---|---|---|---|---|---|---|---|"]
    for t in backfill:
        time.sleep(random.uniform(args.delay_min, args.delay_max))
        r = probe_single(t, "10y")
        print(t, "10y", r, flush=True)
        lines.append(f"| {t} | {'✅' if r['ok'] else '❌'} | {r['rows']} | {r['splits']} "
                     f"| {r['dividends']} | {r['split_adjusted']} | {r['seconds']} | {r['error']} |")

    report = "\n".join(lines) + "\n"
    print("\n" + report)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(report)

    return 0 if rate >= args.min_success_rate else 1


if __name__ == "__main__":
    sys.exit(main())
