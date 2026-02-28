# scripts/backfill_chunk.py
import argparse
import datetime as dt
from pathlib import Path

import pandas as pd

# ✅ 너가 이미 만든 investor fetch 모듈을 import
# 예: scripts/krx_fetch_investor.py에 fetch_investor_flow가 있다면 이렇게
from scripts.krx_fetch_investor import fetch_investor_flow

ROOT = Path(__file__).resolve().parents[1]
HIST = ROOT / "data" / "history"
HIST.mkdir(parents=True, exist_ok=True)

SCHEMA_COLS = [
    "date","market",
    "turnover",
    "retail_net","foreign_net","institution_net",
    "advancers","decliners",
    "top10_turnover_share"
]

def daterange(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)

# ⏳ 아직 bld 못 잡은 항목은 일단 None
def fetch_turnover(date: dt.date, market: str) -> dict | None:
    return None

def fetch_breadth(date: dt.date, market: str) -> dict | None:
    return None

def fetch_top10_share(date: dt.date, market: str) -> dict | None:
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--market", default="BOTH")  # KOSPI/KOSDAQ/BOTH
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    markets = ["KOSPI","KOSDAQ"] if args.market == "BOTH" else [args.market]

    rows = []
    quality = []

    for d in daterange(start, end):
        for m in markets:
            row = {"date": d.isoformat(), "market": m}
            errors = []

            # 1) turnover
            try:
                out = fetch_turnover(d, m)
                if out:
                    row.update(out)
                else:
                    errors.append("turnover:empty")
            except Exception as e:
                errors.append(f"turnover:{type(e).__name__}")

            # 2) investor flow ✅
            try:
                out = fetch_investor_flow(d, m)
                if out:
                    row.update(out)
                else:
                    errors.append("investor:empty")
            except Exception as e:
                errors.append(f"investor:{type(e).__name__}")

            # 3) breadth
            try:
                out = fetch_breadth(d, m)
                if out:
                    row.update(out)
                else:
                    errors.append("breadth:empty")
            except Exception as e:
                errors.append(f"breadth:{type(e).__name__}")

            # 4) top10 share
            try:
                out = fetch_top10_share(d, m)
                if out:
                    row.update(out)
                else:
                    errors.append("top10:empty")
            except Exception as e:
                errors.append(f"top10:{type(e).__name__}")

            rows.append(row)
            quality.append({
                "date": d.isoformat(),
                "market": m,
                "errors": "|".join(errors) if errors else "",
                "missing_fields": sum(1 for c in SCHEMA_COLS if c not in row or pd.isna(row.get(c))),
            })

    df = pd.DataFrame(rows)
    for c in SCHEMA_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[SCHEMA_COLS].sort_values(["date","market"])

    qf = pd.DataFrame(quality).sort_values(["date","market"])

    out_csv = HIST / f"flow_{args.start}_{args.end}_{args.market}.csv"
    out_q = HIST / f"quality_{args.start}_{args.end}_{args.market}.csv"
    df.to_csv(out_csv, index=False)
    qf.to_csv(out_q, index=False)

    print("Wrote:", out_csv)
    print("Quality:", out_q)
    print("Missing rate:", (qf["missing_fields"] > 0).mean())

if __name__ == "__main__":
    main()
