# scripts/build_liquidity_signals.py
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
HIST = ROOT / "data" / "history"
DERIVED = ROOT / "data" / "derived"
DERIVED.mkdir(parents=True, exist_ok=True)

IN_FILE = HIST / "liquidity_daily.csv"
OUT_FILE = DERIVED / "liquidity_signals.csv"


def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    # percentile rank of the last value within rolling window
    def _pct(x):
        if len(x) == 0 or pd.isna(x.iloc[-1]):
            return np.nan
        return pd.Series(x).rank(pct=True).iloc[-1] * 100.0
    return series.rolling(window, min_periods=max(10, window // 3)).apply(_pct, raw=False)


def main():
    df = pd.read_csv(IN_FILE)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["market", "date"])

    # 1D return from close
    df["ret_1d"] = df.groupby("market")["close"].pct_change()

    # turnover ratio vs 20D average
    ma20 = df.groupby("market")["turnover"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    df["turnover_ratio"] = df["turnover"] / ma20

    # 60D percentile of turnover_ratio
    df["turnover_ratio_pctl60"] = df.groupby("market")["turnover_ratio"].transform(lambda s: rolling_percentile(s, 60))

    df_out = df[["date", "market", "close", "turnover", "ret_1d", "turnover_ratio", "turnover_ratio_pctl60"]].copy()
    df_out["date"] = df_out["date"].dt.date.astype(str)

    df_out.to_csv(OUT_FILE, index=False)
    print("Saved:", OUT_FILE, "rows=", len(df_out))


if __name__ == "__main__":
    main()
