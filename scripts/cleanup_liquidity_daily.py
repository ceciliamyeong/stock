# scripts/cleanup_liquidity_daily.py
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
P = ROOT / "data" / "history" / "liquidity_daily.csv"

KEEP = [
    "date","market","turnover_krw","close",
    "individual_net","foreign_net","institution_net",
    "individual_ratio","foreign_ratio","institution_ratio",
]

def main():
    df = pd.read_csv(P)

    # 1) _x/_y 제거
    df = df.loc[:, ~df.columns.str.endswith("_x")]
    df = df.loc[:, ~df.columns.str.endswith("_y")]

    # 2) 중복 컬럼 있을 수 있으니, keep에 있는 것만 남김(있는 것만)
    cols = [c for c in KEEP if c in df.columns]
    df = df[cols].copy()

    # 3) 정렬/중복 제거
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    df.to_csv(P, index=False)
    print("Cleaned:", P, "cols=", list(df.columns), "rows=", len(df))

if __name__ == "__main__":
    main()
