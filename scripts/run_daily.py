from __future__ import annotations

print("RUNNING FILE:", __file__)
print("VERSION: run_daily-ultimate-fix-2026-03-01")

import json
import os
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd
from pykrx import stock

# 1. 경로 설정 (scripts 폴더 내부 실행 대응)
ROOT = Path(__file__).resolve().parents[1]

HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
INVESTOR_LONG_CSV = ROOT / "data" / "derived" / "investor_flow_daily.csv"
INVESTOR_PIVOT_CSV = ROOT / "data" / "derived" / "investor_flow_pivot_daily.csv"
MERGED_CSV = ROOT / "data" / "derived" / "market_flow_daily.csv"
DERIVED_DIR = ROOT / "data" / "derived"
HISTORY_DIR = ROOT / "data" / "history"

FORCE_CLOSE_DATE = "2026-02-27"
RAW_UNIT_HINT = "(십억원)"
MARKETS = ["KOSPI", "KOSDAQ"]

def ensure_dirs():
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

def to_krx_date(s: str) -> str:
    return str(s).replace("-", "")

def _unit_mult(hint: str) -> float:
    if "(십억원)" in hint: return 1e9
    return 1.0

def _norm_inv(t: str) -> str:
    s = str(t).strip()
    if "개인" in s or "individual" in s.lower(): return "individual"
    if "외국인" in s or "foreign" in s.lower(): return "foreign"
    if "기관합계" in s or "institution_total" in s.lower(): return "institution_total"
    return s

def _pick_col_safe(df: pd.DataFrame, candidates: List[str]) -> str:
    """존재하는 컬럼 중 하나를 안전하게 선택"""
    for c in candidates:
        if c in df.columns:
            return c
    # 후보가 없으면 사용 가능한 모든 컬럼 출력 후 에러 방지를 위해 첫 번째 컬럼 반환
    print(f"⚠️ Warning: Candidates {candidates} not found in {df.columns.tolist()}")
    return df.columns[0] if not df.empty else ""

def _call_trading_value_by_investor(date_str: str, mk: str) -> pd.DataFrame:
    d = to_krx_date(date_str)
    # 현재 환경의 pykrx 버전 요구사항 (시작일, 종료일, 시장)
    try:
        return stock.get_market_trading_value_by_investor(d, d, mk)
    except Exception as e:
        print(f"❌ pykrx call failed: {e}")
        return pd.DataFrame()

def _fetch_investor_long(date_str: str) -> pd.DataFrame:
    rows = []
    for mk in MARKETS:
        df = _call_trading_value_by_investor(date_str, mk)
        if df is None or df.empty:
            continue

        # ✅ KeyError: '거래대금' 방지를 위한 유연한 컬럼 선택
        # pykrx의 '거래대금'은 보통 '순매수' 컬럼을 의미함
        buy_col = _pick_col_safe(df, ["매수", "매수금액", "Buy", "BUY"])
        sell_col = _pick_col_safe(df, ["매도", "매도금액", "Sell", "SELL"])
        net_col = _pick_col_safe(df, ["순매수", "순매수금액", "Net", "NET"])

        for inv_name in df.index.astype(str).tolist():
            rows.append({
                "date": date_str,
                "market": mk,
                "investor_type": f"{inv_name}{RAW_UNIT_HINT}",
                "bid_raw": pd.to_numeric(df.loc[inv_name, buy_col], errors="coerce"),
                "ask_raw": pd.to_numeric(df.loc[inv_name, sell_col], errors="coerce"),
                "net_raw": pd.to_numeric(df.loc[inv_name, net_col], errors="coerce"),
                "raw_unit_hint": RAW_UNIT_HINT,
            })
    return pd.DataFrame(rows)

def main():
    ensure_dirs()
    date_str = FORCE_CLOSE_DATE
    print(f"🚀 Processing Date: {date_str}")

    # 1. 투자자별 상세 데이터 가져오기
    today_inv = _fetch_investor_long(date_str)
    if today_inv.empty:
        print("⚠️ No data fetched. Check pykrx status.")
        return

    # 2. 기존 데이터 로드 및 업데이트 (Upsert)
    if INVESTOR_LONG_CSV.exists():
        hist = pd.read_csv(INVESTOR_LONG_CSV)
        hist = hist[hist["date"] != date_str]
        today_inv = pd.concat([hist, today_inv], ignore_index=True)
    
    today_inv.to_csv(INVESTOR_LONG_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Saved Long Data: {len(today_inv)} rows")

    # 3. 피벗 및 지표 계산 (Individual/Foreign/Institution)
    today_inv["norm_type"] = today_inv["investor_type"].apply(_norm_inv)
    today_inv["net_krw"] = today_inv["net_raw"] * _unit_mult(RAW_UNIT_HINT)
    
    keep = today_inv[today_inv["norm_type"].isin(["individual", "foreign", "institution_total"])]
    pivot = keep.pivot_table(index=["date", "market"], columns="norm_type", values="net_krw", aggfunc="sum").reset_index()
    pivot.to_csv(INVESTOR_PIVOT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Saved Pivot Data")

if __name__ == "__main__":
    main()
