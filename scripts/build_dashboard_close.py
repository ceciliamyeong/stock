from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
from pykrx import stock

# GUI가 없는 서버 환경(CI)을 위한 설정
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import squarify

# ------------------------
# 1. 경로 및 날짜 설정
# ------------------------
ROOT = Path(__file__).resolve().parents[1]
# 데이터 소스 경로
HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
INV_PIVOT = ROOT / "data" / "derived" / "investor_flow_pivot_daily.csv"

# 결과물 저장 경로
OUT_BASE = ROOT / "data" / "derived" / "dashboard"
OUT_ARCHIVE = OUT_BASE / "archive"
OUT_CHART = ROOT / "data" / "derived" / "charts"

# ✅ 2월 27일 데이터 고정
FORCE_CLOSE_DATE = "2026-02-27"

# ------------------------
# 2. 유틸리티 함수 (방어 로직 포함)
# ------------------------

def ensure_dirs():
    """경로가 없으면 생성하여 FileNotFoundError 방지"""
    OUT_BASE.mkdir(parents=True, exist_ok=True)
    OUT_ARCHIVE.mkdir(parents=True, exist_ok=True)
    OUT_CHART.mkdir(parents=True, exist_ok=True)

def to_krx_date(s: str) -> str:
    return str(s).replace("-", "")

def to_dash_date(s: str) -> str:
    s = str(s)
    if "-" not in s and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    """한글/영문 컬럼명을 모두 찾아내어 KeyError를 원천 차단"""
    for c in candidates:
        if c in df.columns:
            return c
    return ""

def krw_readable(x: Optional[float]) -> str:
    """금액을 읽기 쉬운 단위로 변환 (조, 억)"""
    if x is None or pd.isna(x): return "0"
    v = float(x)
    a = abs(v)
    if a >= 1e12: return f"{v/1e12:+.2f}조"
    if a >= 1e8: return f"{v/1e8:+.0f}억"
    return f"{v:+.0f}"

def signal_label(ratio: Optional[float]) -> str:
    """수급 비중에 따른 강도 판별"""
    if ratio is None: return "WEAK"
    a = abs(float(ratio))
    if a >= 0.05: return "STRONG"
    if a >= 0.02: return "NORMAL"
    return "WEAK"

def prev_business_day(date_str: str) -> str:
    """수익률 계산을 위한 전 영업일 확보"""
    d = pd.to_datetime(date_str).date()
    start = (d - pd.Timedelta(days=20)).strftime("%Y%m%d")
    try:
        days = stock.get_previous_business_days(fromdate=start, todate=to_krx_date(date_str))
        if not days or len(days) < 2:
            return (d - pd.Timedelta(days=1 if d.weekday() != 0 else 3)).strftime("%Y-%m-%d")
        return to_dash_date(days[-2])
    except:
        return (d - pd.Timedelta(days=1 if d.weekday() != 0 else 3)).strftime("%Y-%m-%d")

# ------------------------
# 3. 데이터 수집 핵심 로직
# ------------------------

def fetch_top10_mcap_and_return(date_str: str, market: str) -> pd.DataFrame:
    """시총 상위 10개 종목 데이터 및 수익률 계산"""
    d = to_krx_date(date_str)
    prev_d = to_krx_date(prev_business_day(date_str))
    
    df = stock.get_market_cap_by_ticker(d, market=market)
    prev_ohlcv = stock.get_market_ohlcv_by_ticker(prev_d, prev_d, market=market)
    
    if df.empty: raise RuntimeError(f"{market} 시총 데이터가 없습니다.")

    mcap_col = _pick_col(df, ["시가총액", "Market Cap", "MCAP"])
    close_col = _pick_col(df, ["종가", "현재가", "Close"])
    prev_close_col = _pick_col(prev_ohlcv, ["종가", "Close"])

    df = df.sort_values(mcap_col, ascending=False).head(10).copy()
    df["ticker"] = df.index.astype(str)
    df["name"] = df["ticker"].map(stock.get_market_ticker_name)
    df["mcap"] = pd.to_numeric(df[mcap_col], errors="coerce")
    df["close"] = pd.to_numeric(df[close_col], errors="coerce")
    
    prev_close = prev_ohlcv[[prev_close_col]].copy().rename(columns={prev_close_col: "prev_close"})
    df = df.merge(prev_close, left_index=True, right_index=True, how="left")
    
    df["return_1d"] = (df["close"] / df["prev_close"] - 1.0) * 100.0
    return df.reset_index(drop=True)

def fetch_volatility_top5(date_str: str, market: str) -> List[Dict[str, Any]]:
    """변동성 상위 5개 종목 수집"""
    d = to_krx_date(date_str)
    df = stock.get_market_ohlcv_by_ticker(d, d, market=market)
    if df.empty: return []
    
    high_c = _pick_col(df, ["고가", "High"])
    low_c = _pick_col(df, ["저가", "Low"])
    close_c = _pick_col(df, ["종가", "Close"])
    
    df["vol"] = (df[high_c] - df[low_c]) / df[close_c] * 100
    df = df.sort_values("vol", ascending=False).head(5)
    return [{"name": stock.get_market_ticker_name(t), "vol": round(r["vol"], 2)} for t, r in df.iterrows()]

def fetch_breadth(date_str: str, market: str) -> Dict[str, Any]:
    """상승/하락 종목 수 계산"""
    d = to_krx_date(date_str)
    df = stock.get_market_ohlcv_by_ticker(d, d, market=market)
    if df.empty: return {}
    
    close_c = _pick_col(df, ["종가", "Close"])
    open_c = _pick_col(df, ["시가", "Open"])
    
    up = int((df[close_c] > df[open_c]).sum())
    down = int((df[close_c] < df[open_c]).sum())
    return {"up": up, "down": down, "same": len(df) - up - down}

def make_treemap_png(df: pd.DataFrame, title: str, outpath: Path):
    """트리맵 이미지 생성 및 수익률 컬러 적용"""
    plt.figure(figsize=(12, 7))
    sizes = df["mcap"].tolist()
    labels = [f"{r['name']}\n{r['return_1d']:+.2f}%" for _, r in df.iterrows()]
    # 수익률에 따른 색상: 상승(빨강), 하락(파랑), 보합(회색)
    colors = ['#ff9999' if r['return_1d'] > 0 else '#66b3ff' if r['return_1d'] < 0 else '#d3d3d3' for _, r in df.iterrows()]
    
    squarify.plot(sizes=sizes, label=labels, color=colors, alpha=0.8, text_kwargs={'fontsize':10, 'fontweight':'bold'})
    plt.title(title, fontsize=15, pad=20)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(outpath, dpi=150)
    plt.close()

# ------------------------
# 4. 실행부
# ------------------------

def main():
    ensure_dirs()
    date_str = FORCE_CLOSE_DATE
    print(f"🚀 {date_str} 통합 대시보드 생성 프로세스 시작")

    # A. 수급 및 지수 데이터 (markets 섹션) 재구성
    markets_section = {}
    if not HIST_LIQ.exists() or not INV_PIVOT.exists():
        print("⚠️ 필수 데이터 파일(CSV)이 누락되었습니다. 빈 구조를 생성합니다.")
    else:
        liq_df = pd.read_csv(HIST_LIQ)
        inv_df = pd.read_csv(INV_PIVOT)
        liq_df["date"] = liq_df["date"].astype(str)
        inv_df["date"] = inv_df["date"].astype(str)

        for mk in ["KOSPI", "KOSDAQ"]:
            try:
                l_row = liq_df[(liq_df["date"] == date_str) & (liq_df["market"] == mk)].iloc[0]
                i_row = inv_df[(inv_df["date"] == date_str) & (inv_df["market"] == mk)].iloc[0]
                
                turnover = float(l_row["turnover_krw"])
                f_net, inst_net, ind_net = float(i_row["foreign_net"]), float(i_row["institution_net"]), float(i_row["individual_net"])

                markets_section[mk] = {
                    "close": float(l_row["close"]),
                    "turnover_krw": turnover,
                    "turnover_readable": krw_readable(turnover),
                    "investor_net_krw": {"foreign": f_net, "institution": inst_net, "individual": ind_net},
                    "investor_net_readable": {k: krw_readable(v) for k, v in {"foreign": f_net, "institution": inst_net, "individual": ind_net}.items()},
                    "investor_ratio": {k: v/turnover if turnover != 0 else 0 for k, v in {"foreign": f_net, "institution": inst_net, "individual": ind_net}.items()},
                    "flow_signal": {k: signal_label(v/turnover if turnover != 0 else 0) for k, v in {"foreign": f_net, "institution": inst_net, "individual": ind_net}.items()}
                }
            except Exception as e:
                print(f"⚠️ {mk} 수급 카드 생성 실패: {e}")

    # B. 통합 대시보드 JSON 구조화
    dashboard = {
        "date": date_str,
        "version": "1.0",
        "markets": markets_section,
        "extras": {
            "top10_treemap": {}, "treemap_png": {}, "volatility_top5": {}, "breadth": {}
        }
    }

    # C. 상세 지표(extras) 수집 및 시각화
    for mk in ["KOSPI", "KOSDAQ"]:
        # 1. 시총 상위 10개 및 트리맵 생성
        try:
            df_top = fetch_top10_mcap_and_return(date_str, mk)
            dashboard["extras"]["top10_treemap"][mk] = df_top.to_dict(orient="records")
            
            img_name = f"treemap_{mk.lower()}_top10_latest.png"
            img_path = OUT_CHART / img_name
            make_treemap_png(df_top, f"{mk} MCAP TOP 10 ({date_str})", img_path)
            dashboard["extras"]["treemap_png"][mk] = f"data/derived/charts/{img_name}"
            print(f"✅ {mk} 트리맵 및 상위 데이터 완료")
        except Exception as e:
            dashboard["extras"][f"top10_error_{mk}"] = str(e)

        # 2. 변동성 및 등락 수집
        try:
            v5, br = fetch_volatility_top5(date_str, mk), fetch_breadth(date_str, mk)
            dashboard["extras"]["volatility_top5"][mk] = v5
            dashboard["extras"]["breadth"][mk] = br
            print(f"✅ {mk} 변동성/시장폭 수집 완료")
        except Exception as e:
            dashboard["extras"][f"ext_error_{mk}"] = str(e)

    # D. 파일 저장
    latest_path = OUT_BASE / "latest.json"
    archive_path = OUT_ARCHIVE / f"{date_str}.json"
    
    for path in [latest_path, archive_path]:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(dashboard, f, ensure_ascii=False, indent=2)

    print(f"🎉 모든 데이터가 꽉 채워진 대시보드가 생성되었습니다! ({latest_path})")

if __name__ == "__main__":
    main()
