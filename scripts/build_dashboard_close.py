from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
from pykrx import stock
import matplotlib.pyplot as plt
import squarify


ROOT = Path(__file__).resolve().parents[1]

HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
INV_CSV = ROOT / "data" / "derived" / "investor_flow_daily.csv"

OUT_BASE = ROOT / "data" / "derived" / "dashboard"
OUT_ARCHIVE = OUT_BASE / "archive"
OUT_CHART = ROOT / "data" / "derived" / "charts"

TARGET_DATE = "2026-02-27"  # 필요하면 run_daily에서 자동화 가능


# ------------------------
# Utils
# ------------------------

def ensure_dirs():
    OUT_BASE.mkdir(parents=True, exist_ok=True)
    OUT_ARCHIVE.mkdir(parents=True, exist_ok=True)
    OUT_CHART.mkdir(parents=True, exist_ok=True)


def krw_readable(x: Optional[float]) -> Optional[str]:
    if x is None:
        return None
    x = float(x)
    a = abs(x)
    if a >= 1e12:
        return f"{x/1e12:.2f}조"
    if a >= 1e8:
        return f"{x/1e8:.0f}억"
    return f"{x:.0f}"


def unit_mult(raw_hint: str) -> float:
    if "(십억원)" in str(raw_hint):
        return 1e10
    if "(억원)" in str(raw_hint):
        return 1e8
    return 1.0


def norm_inv(x: str) -> str:
    base = str(x).split("(")[0].strip()
    if base == "기관" or x == "institution_total":
        return "institution"
    if base == "개인":
        return "individual"
    if base == "외국인":
        return "foreign"
    return x


# ------------------------
# Core: Index + Flow
# ------------------------

def load_index_and_flow(date_str: str) -> Dict[str, Any]:

    liq = pd.read_csv(HIST_LIQ)
    liq["date"] = pd.to_datetime(liq["date"]).dt.date.astype(str)
    liq = liq[liq["date"] == date_str]

    inv = pd.read_csv(INV_CSV)
    inv["date"] = pd.to_datetime(inv["date"]).dt.date.astype(str)
    inv = inv[inv["date"] == date_str]

    inv["investor_type"] = inv["investor_type"].map(norm_inv)
    inv["net_krw"] = pd.to_numeric(inv["net_raw"], errors="coerce") * inv["raw_unit_hint"].map(unit_mult)

    out = {}

    for _, row in liq.iterrows():
        mk = row["market"]
        turnover = float(row["turnover_krw"])
        close = float(row["close"])

        sub = inv[inv["market"] == mk]

        flows = {}
        ratios = {}

        for t in ["foreign", "institution", "individual"]:
            val = sub[sub["investor_type"] == t]["net_krw"].sum()
            if pd.isna(val):
                val = None
            flows[t] = val
            ratios[t] = None if val is None else val / turnover

        out[mk] = {
            "close": close,
            "turnover_krw": turnover,
            "turnover_readable": krw_readable(turnover),
            "investor_net_krw": flows,
            "investor_net_readable": {k: krw_readable(v) for k, v in flows.items()},
            "investor_ratio": ratios,
        }

    return out


# ------------------------
# Top10 Treemap
# ------------------------

def fetch_top10(date_str: str, market: str) -> pd.DataFrame:
    ymd = date_str.replace("-", "")

    mcap = stock.get_market_cap_by_ticker(ymd, market=market)
    ohlcv = stock.get_market_ohlcv_by_ticker(ymd, market=market)

    mcap_col = "시가총액" if "시가총액" in mcap.columns else mcap.columns[0]
    ret_col = "등락률" if "등락률" in ohlcv.columns else ohlcv.columns[0]

    df = (
        mcap[[mcap_col]]
        .rename(columns={mcap_col: "mcap"})
        .join(ohlcv[[ret_col]].rename(columns={ret_col: "return_pct"}))
        .reset_index()
        .rename(columns={"티커": "ticker"})
    )

    df["mcap"] = pd.to_numeric(df["mcap"])
    df["return_pct"] = pd.to_numeric(df["return_pct"])

    df = df.sort_values("mcap", ascending=False).head(10)
    df["name"] = df["ticker"].map(stock.get_market_ticker_name)

    return df[["ticker", "name", "mcap", "return_pct"]]


def make_treemap(df: pd.DataFrame, title: str, path: Path):

    sizes = df["mcap"].tolist()
    labels = [f"{n}\n{r:+.2f}%" for n, r in zip(df["name"], df["return_pct"])]

    colors = [
        "#2E7D32" if r > 0 else "#C62828" if r < 0 else "#9E9E9E"
        for r in df["return_pct"]
    ]

    plt.figure(figsize=(12, 7))
    plt.axis("off")
    plt.title(title)

    squarify.plot(sizes=sizes, label=labels, color=colors, alpha=0.9)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


# ------------------------
# Main
# ------------------------

def main():

    ensure_dirs()

    dashboard = {
        "date": TARGET_DATE,
        "version": "1.0",
        "markets": load_index_and_flow(TARGET_DATE),
        "top10": {},
    }

    for mk in ["KOSPI", "KOSDAQ"]:
        df = fetch_top10(TARGET_DATE, mk)
        dashboard["top10"][mk] = df.to_dict(orient="records")

        make_treemap(
            df,
            f"{mk} TOP10",
            OUT_CHART / f"treemap_{mk.lower()}_top10_latest.png",
        )

    # archive
    archive_path = OUT_ARCHIVE / f"{TARGET_DATE}.json"
    archive_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2))

    # latest
    latest_path = OUT_BASE / "latest.json"
    latest_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2))

    print("Dashboard built.")
    print("Archive:", archive_path)
    print("Latest :", latest_path)


if __name__ == "__main__":
    main()
