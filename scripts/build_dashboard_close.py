from __future__ import annotations

print("RUNNING FILE:", __file__)
print("VERSION: build_dashboard_close-final-2026-03-01")

import json
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd

# deps
from pykrx import stock

# IMPORTANT: headless backend for CI BEFORE importing pyplot
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import squarify


ROOT = Path(__file__).resolve().parents[1]

HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
INV_CSV = ROOT / "data" / "derived" / "investor_flow_daily.csv"

OUT_BASE = ROOT / "data" / "derived" / "dashboard"
OUT_ARCHIVE = OUT_BASE / "archive"
OUT_CHART = ROOT / "data" / "derived" / "charts"


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
    try:
        v = float(x)
    except Exception:
        return None
    a = abs(v)
    if a >= 1e12:
        return f"{v/1e12:+.2f}조"
    if a >= 1e8:
        return f"{v/1e8:+.0f}억"
    return f"{v:+.0f}"


def unit_mult(raw_hint: str) -> float:
    s = str(raw_hint)
    if "(십억원)" in s:
        return 1e10
    if "(억원)" in s:
        return 1e8
    if "(백만원)" in s:
        return 1e6
    if "(천원)" in s:
        return 1e3
    return 1.0


def norm_inv(x: str) -> str:
    """
    Normalize investor_type to: foreign / institution / individual
    Handles:
      - 'institution_total'
      - '개인(십억원)' / '외국인(십억원)' / '기관(십억원)' ...
      - '기관합계', '기관계' ...
    """
    s = str(x).strip()
    if not s:
        return s

    # drop '(...)' suffix
    base = s.split("(")[0].strip()

    # english/internal keys
    if base in ["foreign", "foreigner", "foreign_total"]:
        return "foreign"
    if base in ["institution", "institution_total", "institutions"]:
        return "institution"
    if base in ["individual", "individual_total"]:
        return "individual"

    # korean-ish
    if "외국" in base:
        return "foreign"
    if "기관" in base:
        return "institution"
    if "개인" in base:
        return "individual"

    return base


def signal_label(ratio: Optional[float], strong: float = 0.05, normal: float = 0.02) -> Optional[str]:
    if ratio is None:
        return None
    a = abs(float(ratio))
    if a >= strong:
        return "STRONG"
    if a >= normal:
        return "NORMAL"
    return "WEAK"


def to_krx_date(s: str) -> str:
    return str(s).replace("-", "")  # 'YYYY-MM-DD' -> 'YYYYMMDD'


def to_dash_date(s: str) -> str:
    s = str(s)
    if "-" not in s and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"None of candidates found: {candidates} / got={df.columns.tolist()}")


def prev_business_day(date_str: str) -> str:
    """
    date_str: 'YYYY-MM-DD'
    """
    from datetime import datetime, timedelta

    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    start = (d - timedelta(days=14)).strftime("%Y-%m-%d")

    days = stock.get_previous_business_days(
        fromdate=to_krx_date(start),
        todate=to_krx_date(date_str),
    )
    if not days or len(days) < 2:
        raise RuntimeError(f"Cannot find previous business day for {date_str}")

    return to_dash_date(days[-2])


# ------------------------
# pykrx extras
# ------------------------

def fetch_top10_mcap_and_return(date_str: str, market: str) -> pd.DataFrame:
    prev_str = prev_business_day(date_str)

    cap = stock.get_market_cap_by_ticker(to_krx_date(date_str), market=market)
    if cap is None or cap.empty:
        raise RuntimeError(f"pykrx cap empty: date={date_str}, market={market}")

    prev_ohlcv = stock.get_market_ohlcv_by_ticker(to_krx_date(prev_str), market=market)
    if prev_ohlcv is None or prev_ohlcv.empty:
        raise RuntimeError(f"pykrx ohlcv empty: date={prev_str}, market={market}")

    close_col = _pick_col(cap, ["종가", "Close"])
    mcap_col = _pick_col(cap, ["시가총액", "Market Cap"])
    prev_close_col = _pick_col(prev_ohlcv, ["종가", "Close"])

    df = cap[[close_col, mcap_col]].copy()
    df = df.rename(columns={close_col: "close", mcap_col: "mcap"})
    df["ticker"] = df.index.astype(str)
    df["name"] = df["ticker"].map(stock.get_market_ticker_name)

    prev_close = prev_ohlcv[[prev_close_col]].copy().rename(columns={prev_close_col: "prev_close"})
    prev_close["ticker"] = prev_close.index.astype(str)

    df = df.merge(prev_close.reset_index(drop=True), on="ticker", how="left")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce")
    df["mcap"] = pd.to_numeric(df["mcap"], errors="coerce")

    df["return_1d"] = (df["close"] / df["prev_close"] - 1.0) * 100.0

    df = df.dropna(subset=["mcap"]).sort_values("mcap", ascending=False).head(10).reset_index(drop=True)
    return df[["ticker", "name", "close", "mcap", "return_1d"]]


def make_treemap_png(df_top10: pd.DataFrame, title: str, outpath: Path) -> None:
    outpath.parent.mkdir(parents=True, exist_ok=True)

    sizes = df_top10["mcap"].astype(float).tolist()
    labels = [f"{r['name']}\n{float(r['return_1d']):+.2f}%" for _, r in df_top10.iterrows()]

    plt.figure(figsize=(10, 6))
    squarify.plot(sizes=sizes, label=labels, alpha=0.9)
    plt.title(title)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(outpath, dpi=150)
    plt.close()


def fetch_volatility_top5(date_str: str, market: str) -> List[Dict[str, Any]]:
    prev_str = prev_business_day(date_str)

    today = stock.get_market_ohlcv_by_ticker(to_krx_date(date_str), market=market)
    prev = stock.get_market_ohlcv_by_ticker(to_krx_date(prev_str), market=market)
    if today is None or today.empty or prev is None or prev.empty:
        raise RuntimeError(f"pykrx ohlcv empty: date={date_str}/{prev_str}, market={market}")

    close_col = _pick_col(today, ["종가", "Close"])
    prev_close_col = _pick_col(prev, ["종가", "Close"])

    df = today[[close_col]].copy().rename(columns={close_col: "close"})
    df["ticker"] = df.index.astype(str)

    prev_df = prev[[prev_close_col]].copy().rename(columns={prev_close_col: "prev_close"})
    prev_df["ticker"] = prev_df.index.astype(str)

    df = df.merge(prev_df.reset_index(drop=True), on="ticker", how="left")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce")
    df["return_1d"] = (df["close"] / df["prev_close"] - 1.0) * 100.0
    df["abs_ret"] = df["return_1d"].abs()

    df = df.dropna(subset=["return_1d", "abs_ret"]).sort_values("abs_ret", ascending=False).head(5)
    df["name"] = df["ticker"].map(stock.get_market_ticker_name)

    return [
        {"ticker": str(r["ticker"]), "name": str(r["name"]), "return_1d": float(r["return_1d"]), "close": float(r["close"])}
        for _, r in df.iterrows()
    ]


def fetch_breadth(date_str: str, market: str) -> Dict[str, Any]:
    """
    Breadth = 상승/하락/보합 종목 수 + 비율
    """
    prev_str = prev_business_day(date_str)

    today = stock.get_market_ohlcv_by_ticker(to_krx_date(date_str), market=market)
    prev = stock.get_market_ohlcv_by_ticker(to_krx_date(prev_str), market=market)
    if today is None or today.empty or prev is None or prev.empty:
        raise RuntimeError(f"pykrx ohlcv empty: date={date_str}/{prev_str}, market={market}")

    close_col = _pick_col(today, ["종가", "Close"])
    prev_close_col = _pick_col(prev, ["종가", "Close"])

    df = today[[close_col]].copy().rename(columns={close_col: "close"})
    df["ticker"] = df.index.astype(str)

    prev_df = prev[[prev_close_col]].copy().rename(columns={prev_close_col: "prev_close"})
    prev_df["ticker"] = prev_df.index.astype(str)

    df = df.merge(prev_df.reset_index(drop=True), on="ticker", how="left")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["prev_close"] = pd.to_numeric(df["prev_close"], errors="coerce")
    df["return_1d"] = (df["close"] / df["prev_close"] - 1.0) * 100.0
    df = df.dropna(subset=["return_1d"])

    adv = int((df["return_1d"] > 0).sum())
    dec = int((df["return_1d"] < 0).sum())
    unch = int((df["return_1d"] == 0).sum())
    total = int(len(df))

    adv_ratio = float(adv / total) if total > 0 else 0.0
    dec_ratio = float(dec / total) if total > 0 else 0.0
    adv_dec_ratio = float(adv / dec) if dec > 0 else None

    return {
        "date": date_str,
        "market": market,
        "adv": adv,
        "dec": dec,
        "unch": unch,
        "total": total,
        "adv_ratio": adv_ratio,
        "dec_ratio": dec_ratio,
        "adv_dec_ratio": adv_dec_ratio,
    }


# ------------------------
# Latest date selection
# ------------------------

def load_liq_df() -> pd.DataFrame:
    if not HIST_LIQ.exists():
        raise FileNotFoundError(f"Missing {HIST_LIQ}")
    liq = pd.read_csv(HIST_LIQ)
    if "date" not in liq.columns:
        raise KeyError(f"{HIST_LIQ} missing date column")
    liq["date"] = pd.to_datetime(liq["date"], errors="coerce").dt.date.astype(str)
    liq["market"] = liq["market"].astype(str)
    liq["turnover_krw"] = pd.to_numeric(liq.get("turnover_krw"), errors="coerce")
    liq["close"] = pd.to_numeric(liq.get("close"), errors="coerce")
    liq = liq.dropna(subset=["date", "market"])
    return liq.sort_values(["date", "market"]).reset_index(drop=True)


def load_inv_df() -> pd.DataFrame:
    """
    investor_flow_daily.csv 형태가 이미 pivot(=date/market/foreign_net...)일 수도 있고,
    raw long-form(=date/market/investor_type/net_raw/raw_unit_hint)일 수도 있다.
    둘 다 지원한다.
    """
    if not INV_CSV.exists():
        raise FileNotFoundError(f"Missing {INV_CSV}")

    inv = pd.read_csv(INV_CSV)
    if inv.empty:
        return inv

    inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.date.astype(str)
    inv["market"] = inv["market"].astype(str)

    # case A) already pivoted
    if {"foreign_net", "institution_net", "individual_net"}.issubset(set(inv.columns)):
        for c in ["foreign_net", "institution_net", "individual_net"]:
            inv[c] = pd.to_numeric(inv[c], errors="coerce")
        return inv.sort_values(["date", "market"]).reset_index(drop=True)

    # case B) long-form -> pivot
    if "investor_type" not in inv.columns:
        return pd.DataFrame(columns=["date", "market", "foreign_net", "institution_net", "individual_net"])

    inv["investor_type"] = inv["investor_type"].map(norm_inv)

    # raw columns
    inv["net_raw"] = pd.to_numeric(inv.get("net_raw"), errors="coerce")
    inv["raw_unit_hint"] = inv.get("raw_unit_hint", "")
    inv["net_krw"] = inv["net_raw"] * inv["raw_unit_hint"].map(unit_mult)

    keep = inv[inv["investor_type"].isin(["foreign", "institution", "individual"])].copy()
    if keep.empty:
        return pd.DataFrame(columns=["date", "market", "foreign_net", "institution_net", "individual_net"])

    pivot = (
        keep.groupby(["date", "market", "investor_type"], as_index=False)["net_krw"]
        .sum(min_count=1)
        .pivot_table(index=["date", "market"], columns="investor_type", values="net_krw", aggfunc="sum")
        .reset_index()
        .rename(columns={"foreign": "foreign_net", "institution": "institution_net", "individual": "individual_net"})
    )

    for c in ["foreign_net", "institution_net", "individual_net"]:
        if c not in pivot.columns:
            pivot[c] = pd.NA

    return pivot.sort_values(["date", "market"]).reset_index(drop=True)


def pick_latest_trade_date(liq: pd.DataFrame, inv: pd.DataFrame) -> str:
    if liq.empty:
        raise RuntimeError("liquidity_daily.csv is empty")

    latest_liq = sorted(liq["date"].unique())[-1]
    if inv is None or inv.empty:
        return latest_liq

    def has_core(date_str: str) -> bool:
        sub = inv[inv["date"] == date_str]
        if sub.empty:
            return False
        types = set()
        if {"foreign_net", "institution_net", "individual_net"}.issubset(set(sub.columns)):
            # pivoted
            # if at least one not-null exists for each column
            ok = True
            for c in ["foreign_net", "institution_net", "individual_net"]:
                if sub[c].isna().all():
                    ok = False
            return ok
        return False

    if has_core(latest_liq):
        return latest_liq

    candidates = [d for d in sorted(inv["date"].unique()) if d <= latest_liq]
    if not candidates:
        return latest_liq

    return candidates[-1]


# ------------------------
# Core cards: index + flow
# ------------------------

def load_index_rows(liq: pd.DataFrame, date_str: str) -> pd.DataFrame:
    day = liq[liq["date"] == date_str].copy()
    if day.empty:
        raise RuntimeError(f"No liquidity rows for date={date_str}")
    return day.sort_values(["market"]).reset_index(drop=True)


def build_market_cards(liq_day: pd.DataFrame, inv_pivot: pd.DataFrame) -> Dict[str, Any]:
    # inv_pivot: date/market/foreign_net/institution_net/individual_net
    inv_map: Dict[str, dict] = {}
    if inv_pivot is not None and not inv_pivot.empty:
        for _, rr in inv_pivot.iterrows():
            inv_map[str(rr["market"])] = rr.to_dict()

    markets: Dict[str, Any] = {}
    for _, r in liq_day.iterrows():
        mk = str(r["market"])
        turnover = None if pd.isna(r.get("turnover_krw")) else float(r.get("turnover_krw"))
        close = None if pd.isna(r.get("close")) else float(r.get("close"))

        inv_row = inv_map.get(mk, {})
        foreign = inv_row.get("foreign_net")
        inst = inv_row.get("institution_net")
        indiv = inv_row.get("individual_net")

        foreign = None if foreign is None or pd.isna(foreign) else float(foreign)
        inst = None if inst is None or pd.isna(inst) else float(inst)
        indiv = None if indiv is None or pd.isna(indiv) else float(indiv)

        def ratio(v: Optional[float]) -> Optional[float]:
            if v is None or turnover is None or turnover == 0:
                return None
            return float(v) / float(turnover)

        ratios = {"foreign": ratio(foreign), "institution": ratio(inst), "individual": ratio(indiv)}

        markets[mk] = {
            "close": close,
            "turnover_krw": turnover,
            "turnover_readable": krw_readable(turnover),
            "investor_net_krw": {"foreign": foreign, "institution": inst, "individual": indiv},
            "investor_net_readable": {
                "foreign": krw_readable(foreign),
                "institution": krw_readable(inst),
                "individual": krw_readable(indiv),
            },
            "investor_ratio": ratios,
            "flow_signal": {
                "foreign": signal_label(ratios["foreign"]),
                "institution": signal_label(ratios["institution"]),
                "individual": signal_label(ratios["individual"]),
            },
        }

    return markets


# ------------------------
# Main
# ------------------------

def main():
    ensure_dirs()

    liq = load_liq_df()
    inv = load_inv_df()

    date_str = pick_latest_trade_date(liq, inv)

    liq_day = load_index_rows(liq, date_str)
    inv_pivot = inv[inv["date"] == date_str].copy() if inv is not None and not inv.empty else pd.DataFrame()

    # 프론트가 죽지 않도록 extras는 항상 기본 구조를 가진다
    dashboard: Dict[str, Any] = {
        "date": date_str,
        "version": "1.0",
        "markets": build_market_cards(liq_day, inv_pivot),
        "extras": {
            "top10_treemap": {"KOSPI": [], "KOSDAQ": []},
            "treemap_png": {
                "KOSPI": "data/derived/charts/treemap_kospi_top10_latest.png",
                "KOSDAQ": "data/derived/charts/treemap_kosdaq_top10_latest.png",
            },
            "volatility_top5": {"KOSPI": [], "KOSDAQ": []},
            "breadth": {"KOSPI": {}, "KOSDAQ": {}},
        },
    }

    treemap_png = dashboard["extras"]["treemap_png"]

    # Top10 treemap + data (실패해도 latest.json은 반드시 생성)
    try:
        for mk in ["KOSPI", "KOSDAQ"]:
            df_top10 = fetch_top10_mcap_and_return(date_str, mk)
            dashboard["extras"]["top10_treemap"][mk] = df_top10.to_dict(orient="records")

            make_treemap_png(
                df_top10,
                f"{mk} 시총 TOP10 — {date_str}",
                OUT_CHART / f"treemap_{mk.lower()}_top10_latest.png",
            )
    except Exception as e:
        dashboard["extras"]["top10_error"] = str(e)

    # Volatility top5
    try:
        dashboard["extras"]["volatility_top5"] = {
            "KOSPI": fetch_volatility_top5(date_str, "KOSPI"),
            "KOSDAQ": fetch_volatility_top5(date_str, "KOSDAQ"),
        }
    except Exception as e:
        dashboard["extras"]["volatility_top5"] = {"KOSPI": [], "KOSDAQ": []}
        dashboard["extras"]["volatility_error"] = str(e)

    # Breadth
    try:
        dashboard["extras"]["breadth"] = {
            "KOSPI": fetch_breadth(date_str, "KOSPI"),
            "KOSDAQ": fetch_breadth(date_str, "KOSDAQ"),
        }
    except Exception as e:
        dashboard["extras"]["breadth"] = {"KOSPI": {}, "KOSDAQ": {}}
        dashboard["extras"]["breadth_error"] = str(e)

    # archive + latest
    archive_path = OUT_ARCHIVE / f"{date_str}.json"
    latest_path = OUT_BASE / "latest.json"

    archive_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Built dashboard for:", date_str)
    print("Archive:", archive_path)
    print("Latest:", latest_path)
    print("Charts:", OUT_CHART)
    print("Treemap PNG paths:", treemap_png)


if __name__ == "__main__":
    main()
