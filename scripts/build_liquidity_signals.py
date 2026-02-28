import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[0]  # 루트에 파일이 있으면 0, scripts면 parents[1]로 조정
DATA_IN = ROOT / "data" / "history" / "liquidity_daily.csv"
OUT_DIR = ROOT / "data" / "derived"
OUT_CSV = OUT_DIR / "liquidity_signals_daily.csv"
OUT_SUMMARY = OUT_DIR / "latest_summary.json"


def compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["market", "date"]).reset_index(drop=True)

    # group by market
    out = []
    for m, g in df.groupby("market", sort=False):
        g = g.sort_values("date").reset_index(drop=True)

        g["turnover_ma20"] = g["turnover_krw"].rolling(20, min_periods=5).mean()
        g["turnover_std20"] = g["turnover_krw"].rolling(20, min_periods=5).std()

        g["turnover_ratio20"] = g["turnover_krw"] / g["turnover_ma20"]
        g["turnover_z20"] = (g["turnover_krw"] - g["turnover_ma20"]) / g["turnover_std20"]

        g["turnover_chg_5d"] = g["turnover_krw"].pct_change(5)
        g["turnover_chg_20d"] = g["turnover_krw"].pct_change(20)

        g["ret_1d"] = g["close"].pct_change(1)
        g["ret_5d"] = g["close"].pct_change(5)
        g["ret_20d"] = g["close"].pct_change(20)

        out.append(g)

    out_df = pd.concat(out, ignore_index=True)
    out_df = out_df.sort_values(["date", "market"]).reset_index(drop=True)
    return out_df


def build_latest_summary(sig: pd.DataFrame) -> dict:
    latest_date = sig["date"].max()
    latest = sig[sig["date"] == latest_date].copy()

    def pack(row):
        return {
            "turnover_krw": float(row["turnover_krw"]),
            "turnover_ratio20": None if pd.isna(row["turnover_ratio20"]) else float(row["turnover_ratio20"]),
            "turnover_z20": None if pd.isna(row["turnover_z20"]) else float(row["turnover_z20"]),
            "turnover_chg_5d": None if pd.isna(row["turnover_chg_5d"]) else float(row["turnover_chg_5d"]),
            "ret_1d": None if pd.isna(row["ret_1d"]) else float(row["ret_1d"]),
        }

    res = {"date": latest_date.strftime("%Y-%m-%d")}
    for _, r in latest.iterrows():
        res[r["market"]] = pack(r)
    return res


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(DATA_IN)
    sig = compute_signals(df)

    sig.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    summary = build_latest_summary(sig)
    OUT_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Saved:", OUT_CSV)
    print("Saved:", OUT_SUMMARY)


if __name__ == "__main__":
    main()
