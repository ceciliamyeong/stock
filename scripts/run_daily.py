from __future__ import annotations

print("RUNNING FILE:", __file__)
print("VERSION: run_daily-no-index-api-2026-03-01")

import json
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd
from pykrx import stock

ROOT = Path(__file__).resolve().parents[1]

HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
INVESTOR_LONG_CSV = ROOT / "data" / "derived" / "investor_flow_daily.csv"
INVESTOR_PIVOT_CSV = ROOT / "data" / "derived" / "investor_flow_pivot_daily.csv"
MERGED_CSV = ROOT / "data" / "derived" / "market_flow_daily.csv"
DERIVED_DIR = ROOT / "data" / "derived"
HISTORY_DIR = ROOT / "data" / "history"

# ✅ 최종 마감일 강제
FORCE_CLOSE_DATE = "2026-02-27"

# ✅ 거래소 수급 단위: (십억원)
RAW_UNIT_HINT = "(십억원)"

MARKETS = ["KOSPI", "KOSDAQ"]


def ensure_dirs():
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def to_krx_date(s: str) -> str:
    return str(s).replace("-", "")


def to_dash_date(s: str) -> str:
    s = str(s)
    if "-" not in s and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def latest_business_day() -> str:
    from datetime import datetime, timedelta
    today = datetime.utcnow().date()
    start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    days = stock.get_previous_business_days(fromdate=to_krx_date(start), todate=to_krx_date(end))
    if not days:
        raise RuntimeError("Cannot determine business days from pykrx")
    return to_dash_date(days[-1])


def prev_business_day_safe(date_str: str, lookback_days: int = 60) -> Optional[str]:
    from datetime import datetime, timedelta
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None
    start = (d - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    try:
        days = stock.get_previous_business_days(fromdate=to_krx_date(start), todate=to_krx_date(date_str))
    except Exception:
        return None

    if not days or len(days) < 2:
        return None

    return to_dash_date(days[-2])


def _unit_mult(hint: str) -> float:
    s = str(hint)
    if "(십억원)" in s:
        return 1e10
    if "(억원)" in s:
        return 1e8
    if "(백만원)" in s:
        return 1e6
    if "(천원)" in s:
        return 1e3
    return 1.0


def _norm_inv(t: str) -> str:
    s = str(t).strip()
    if not s:
        return s
    base = s.split("(")[0].strip()

    if base in ["individual", "individual_total"]:
        return "individual"
    if base in ["foreign", "foreigner", "foreign_total"]:
        return "foreign"
    if base in ["institution_total", "institution", "institutions"]:
        return "institution_total"

    if "개인" in base:
        return "individual"
    if "외국" in base:
        return "foreign"
    if "기관" in base:
        return "institution_total"

    return base


# ------------------------
# Liquidity: read from history CSV only (NO pykrx index API)
# ------------------------

def _load_liquidity_history() -> pd.DataFrame:
    cols = ["date", "market", "turnover_krw", "close"]
    if not HIST_LIQ.exists():
        return pd.DataFrame(columns=cols)

    df = pd.read_csv(HIST_LIQ)
    if df.empty:
        return pd.DataFrame(columns=cols)

    if "date" not in df.columns or "market" not in df.columns:
        return pd.DataFrame(columns=cols)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df["market"] = df["market"].astype(str)
    df["turnover_krw"] = pd.to_numeric(df.get("turnover_krw"), errors="coerce")
    df["close"] = pd.to_numeric(df.get("close"), errors="coerce")

    df = df.dropna(subset=["date", "market"])
    return df.sort_values(["date", "market"]).reset_index(drop=True)


def _liquidity_day_from_history(date_str: str) -> pd.DataFrame:
    """
    ✅ index API 호출 금지.
    liquidity_daily.csv에서 해당 date의 KOSPI/KOSDAQ rows를 꺼낸다.
    없으면 NaN row라도 만들어서 파이프라인을 살린다.
    """
    hist = _load_liquidity_history()
    day = hist[hist["date"] == date_str].copy()

    rows = []
    for mk in MARKETS:
        sub = day[day["market"] == mk]
        if not sub.empty:
            r = sub.iloc[-1].to_dict()
            rows.append({
                "date": date_str,
                "market": mk,
                "turnover_krw": r.get("turnover_krw"),
                "close": r.get("close"),
            })
        else:
            rows.append({"date": date_str, "market": mk, "turnover_krw": pd.NA, "close": pd.NA})

    out = pd.DataFrame(rows)
    out["turnover_krw"] = pd.to_numeric(out["turnover_krw"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    return out


# ------------------------
# Investor long-form (market level)
# ------------------------

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"None of candidates found: {candidates} / got={df.columns.tolist()}")


def _fetch_investor_long(date_str: str) -> pd.DataFrame:
    rows = []

    for mk in MARKETS:
        df = stock.get_market_trading_value_by_investor(to_krx_date(date_str), market=mk)
        if df is None or df.empty:
            raise RuntimeError(f"investor trading value empty: date={date_str}, market={mk}")

        buy_col = _pick_col(df, ["매수", "BUY", "buy", "매수금액"])
        sell_col = _pick_col(df, ["매도", "SELL", "sell", "매도금액"])
        net_col = _pick_col(df, ["순매수", "NET", "net", "순매수금액"])

        for inv_name in df.index.astype(str).tolist():
            bid_raw = pd.to_numeric(df.loc[inv_name, buy_col], errors="coerce")
            ask_raw = pd.to_numeric(df.loc[inv_name, sell_col], errors="coerce")
            net_raw = pd.to_numeric(df.loc[inv_name, net_col], errors="coerce")

            investor_type = f"{inv_name}{RAW_UNIT_HINT}"

            rows.append({
                "date": date_str,
                "market": mk,
                "investor_type": investor_type,
                "bid_raw": float(bid_raw) if pd.notna(bid_raw) else None,
                "ask_raw": float(ask_raw) if pd.notna(ask_raw) else None,
                "net_raw": float(net_raw) if pd.notna(net_raw) else None,
                "raw_unit_hint": RAW_UNIT_HINT,
            })

    return pd.DataFrame(rows)


def _load_investor_long() -> pd.DataFrame:
    cols = ["date", "market", "investor_type", "bid_raw", "ask_raw", "net_raw", "raw_unit_hint"]
    if not INVESTOR_LONG_CSV.exists():
        return pd.DataFrame(columns=cols)

    df = pd.read_csv(INVESTOR_LONG_CSV)
    if df.empty:
        return pd.DataFrame(columns=cols)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df["market"] = df["market"].astype(str)
    df["investor_type"] = df["investor_type"].astype(str)
    df["bid_raw"] = pd.to_numeric(df.get("bid_raw"), errors="coerce")
    df["ask_raw"] = pd.to_numeric(df.get("ask_raw"), errors="coerce")
    df["net_raw"] = pd.to_numeric(df.get("net_raw"), errors="coerce")
    df["raw_unit_hint"] = df.get("raw_unit_hint", RAW_UNIT_HINT)
    df = df.dropna(subset=["date", "market", "investor_type"])
    return df.sort_values(["date", "market", "investor_type"]).reset_index(drop=True)


def _upsert_investor_long(date_str: str) -> pd.DataFrame:
    hist = _load_investor_long()
    today = _fetch_investor_long(date_str)

    if not hist.empty:
        hist = hist[hist["date"] != date_str].copy()

    out = pd.concat([hist, today], ignore_index=True)
    out = out.sort_values(["date", "market", "investor_type"]).reset_index(drop=True)
    out.to_csv(INVESTOR_LONG_CSV, index=False)

    print("Saved investor long:", INVESTOR_LONG_CSV, "rows=", len(out))
    return out


def _read_investor_pivot_from_long(inv_long: pd.DataFrame) -> pd.DataFrame:
    cols = ["date", "market", "foreign_net", "institution_net", "individual_net"]
    if inv_long is None or inv_long.empty:
        return pd.DataFrame(columns=cols)

    inv = inv_long.copy()
    inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.date.astype(str)
    inv["market"] = inv["market"].astype(str)

    inv["investor_type_norm"] = inv["investor_type"].map(_norm_inv)

    inv["net_raw"] = pd.to_numeric(inv.get("net_raw"), errors="coerce")
    inv["raw_unit_hint"] = inv.get("raw_unit_hint", RAW_UNIT_HINT)
    inv["net_krw"] = inv["net_raw"] * inv["raw_unit_hint"].map(_unit_mult)

    keep = inv[inv["investor_type_norm"].isin(["individual", "foreign", "institution_total"])].copy()
    if keep.empty:
        return pd.DataFrame(columns=cols)

    pivot = (
        keep.groupby(["date", "market", "investor_type_norm"], as_index=False)["net_krw"]
        .sum(min_count=1)
        .pivot_table(index=["date", "market"], columns="investor_type_norm", values="net_krw", aggfunc="sum")
        .reset_index()
        .rename(columns={
            "individual": "individual_net",
            "foreign": "foreign_net",
            "institution_total": "institution_net",
        })
    )

    for c in ["individual_net", "foreign_net", "institution_net"]:
        if c not in pivot.columns:
            pivot[c] = pd.NA

    pivot = pivot.sort_values(["date", "market"]).reset_index(drop=True)
    return pivot[["date", "market", "foreign_net", "institution_net", "individual_net"]]


def _merge_investor(liquidity_day: pd.DataFrame, investor_pivot: pd.DataFrame) -> pd.DataFrame:
    if liquidity_day is None or liquidity_day.empty:
        return liquidity_day

    if investor_pivot is None:
        investor_pivot = pd.DataFrame(columns=["date", "market", "foreign_net", "institution_net", "individual_net"])

    net_cols = ["foreign_net", "institution_net", "individual_net"]

    # suffix 잔재 제거
    liq_suffix = [c for c in liquidity_day.columns if c.endswith("_x") or c.endswith("_y")]
    if liq_suffix:
        liquidity_day = liquidity_day.drop(columns=liq_suffix)

    inv_suffix = [c for c in investor_pivot.columns if c.endswith("_x") or c.endswith("_y")]
    if inv_suffix:
        investor_pivot = investor_pivot.drop(columns=inv_suffix)

    # liquidity쪽 net 제거
    liq_drop = [c for c in net_cols if c in liquidity_day.columns]
    if liq_drop:
        liquidity_day = liquidity_day.drop(columns=liq_drop)

    # investor net 임시 rename
    rename_map = {c: f"{c}__inv" for c in net_cols if c in investor_pivot.columns}
    if rename_map:
        investor_pivot = investor_pivot.rename(columns=rename_map)

    present = list(rename_map.values())
    if not investor_pivot.empty and {"date", "market"}.issubset(set(investor_pivot.columns)) and present:
        investor_pivot[present] = investor_pivot[present].apply(pd.to_numeric, errors="coerce")
        investor_pivot = investor_pivot.groupby(["date", "market"], as_index=False)[present].sum(min_count=1)

    out = liquidity_day.merge(investor_pivot, on=["date", "market"], how="left")

    back_map = {v: k for k, v in rename_map.items()}
    if back_map:
        out = out.rename(columns=back_map)

    drop_suffix_final = [c for c in out.columns if c.endswith("_x") or c.endswith("_y")]
    if drop_suffix_final:
        out = out.drop(columns=drop_suffix_final)

    denom = pd.to_numeric(out.get("turnover_krw", pd.Series([pd.NA] * len(out))), errors="coerce").replace({0: pd.NA})

    for c in net_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    if "individual_net" in out.columns:
        out["individual_ratio"] = out["individual_net"] / denom
    if "foreign_net" in out.columns:
        out["foreign_ratio"] = out["foreign_net"] / denom
    if "institution_net" in out.columns:
        out["institution_ratio"] = out["institution_net"] / denom

    return out.sort_values(["date", "market"]).reset_index(drop=True)


def main():
    ensure_dirs()

    date_str = FORCE_CLOSE_DATE if FORCE_CLOSE_DATE else latest_business_day()
    prev = prev_business_day_safe(date_str)

    print("Target date:", date_str, "(prev:", (prev if prev else "N/A"), ")")
    print("Investor unit:", RAW_UNIT_HINT)
    print("Liquidity source:", HIST_LIQ, "(NO pykrx index api)")

    # ✅ liquidity는 history에서만 읽어온다
    liq_day = _liquidity_day_from_history(date_str)

    inv_long_hist = _upsert_investor_long(date_str)

    inv_pivot = _read_investor_pivot_from_long(inv_long_hist)
    inv_pivot.to_csv(INVESTOR_PIVOT_CSV, index=False)
    print("Saved investor pivot:", INVESTOR_PIVOT_CSV, "rows=", len(inv_pivot))

    merged = _merge_investor(liq_day, inv_pivot)
    merged.to_csv(MERGED_CSV, index=False)
    print("Saved merged market flow:", MERGED_CSV, "rows=", len(merged))

    latest_snapshot: Dict[str, Any] = {"date": date_str, "markets": {}}
    for _, r in merged.iterrows():
        mk = str(r["market"])
        latest_snapshot["markets"][mk] = {
            "turnover_krw": None if pd.isna(r.get("turnover_krw")) else float(r.get("turnover_krw")),
            "close": None if pd.isna(r.get("close")) else float(r.get("close")),
            "foreign_net": None if pd.isna(r.get("foreign_net")) else float(r.get("foreign_net")),
            "institution_net": None if pd.isna(r.get("institution_net")) else float(r.get("institution_net")),
            "individual_net": None if pd.isna(r.get("individual_net")) else float(r.get("individual_net")),
            "foreign_ratio": None if pd.isna(r.get("foreign_ratio")) else float(r.get("foreign_ratio")),
            "institution_ratio": None if pd.isna(r.get("institution_ratio")) else float(r.get("institution_ratio")),
            "individual_ratio": None if pd.isna(r.get("individual_ratio")) else float(r.get("individual_ratio")),
        }

    snap_path = DERIVED_DIR / "latest_market_flow_snapshot.json"
    snap_path.write_text(json.dumps(latest_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Saved snapshot:", snap_path)


if __name__ == "__main__":
    main()
