#!/usr/bin/env python3
"""
scripts/make_liquidity_charts.py

Dual-axis charts:
- bar: turnover_krw
- line: close

Input:
  data/derived/liquidity_daily_complete.csv  (date, market, turnover_krw, close)

Output:
  data/derived/charts/kospi_turnover_close.png
  data/derived/charts/kosdaq_turnover_close.png
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def _plot_one(df: pd.DataFrame, market: str, out_path: Path):
    d = df[df["market"] == market].copy()
    d["date"] = pd.to_datetime(d["date"])
    d = d.sort_values("date")

    fig, ax1 = plt.subplots(figsize=(12, 4.2))
    ax1.bar(d["date"], d["turnover_krw"].astype(float), width=1.0)
    ax1.set_ylabel("Turnover (KRW)")
    ax1.set_title(f"{market}: Turnover (bar) vs Index Close (line)")

    ax2 = ax1.twinx()
    ax2.plot(d["date"], d["close"].astype(float))
    ax2.set_ylabel("Index Close")

    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def main():
    root = Path(__file__).resolve().parents[1]
    inp = root / "data" / "derived" / "liquidity_daily_complete.csv"
    if not inp.exists():
        raise FileNotFoundError(f"missing {inp}")

    df = pd.read_csv(inp)

    charts = root / "data" / "derived" / "charts"
    charts.mkdir(parents=True, exist_ok=True)

    _plot_one(df, "KOSPI", charts / "kospi_turnover_close.png")
    _plot_one(df, "KOSDAQ", charts / "kosdaq_turnover_close.png")
    print("Saved charts to", charts)


if __name__ == "__main__":
    main()
