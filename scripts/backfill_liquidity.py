import argparse
import datetime as dt
from pathlib import Path
import sys

import pandas as pd

# scripts 폴더 import 안정화
sys.path.append(str(Path(__file__).resolve().parent))

from liquidity_fetch import fetch_liquidity_range


ROOT = Path(__file__).resolve().parents[1]
HIST = ROOT / "data" / "history"
HIST.mkdir(parents=True, exist_ok=True)

OUT_FILE = HIST / "liquidity_daily.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)  # YYYY-MM-DD
    ap.add_argument("--market", default="BOTH")  # KOSPI/KOSDAQ/BOTH
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)

    markets = ["KOSPI", "KOSDAQ"] if args.market == "BOTH" else [args.market]

    frames = [fetch_liquidity_range(start, end, m) for m in markets]
    new_df = pd.concat(frames, ignore_index=True).sort_values(["date", "market"])

    if OUT_FILE.exists():
        old_df = pd.read_csv(OUT_FILE)
        df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        df = new_df

    df = (
        df.drop_duplicates(subset=["date", "market"], keep="last")
        .sort_values(["date", "market"])
        .reset_index(drop=True)
    )

    df.to_csv(OUT_FILE, index=False)
    print("Saved:", OUT_FILE, "rows=", len(df))


if __name__ == "__main__":
    main()
