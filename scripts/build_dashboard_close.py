from __future__ import annotations

print("RUNNING FILE:", __file__)
print("VERSION: build_dashboard_close-KRXTOP10-v2-UNITFIX-2026-03-01")

import json
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
import re
import requests
from pykrx import stock

# headless backend for CI
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import squarify

from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]

HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
# ✅ 우선 pivot을 읽고, 없으면 long-form을 읽어서 pivot 생성
INV_PIVOT = ROOT / "data" / "derived" / "investor_flow_pivot_daily.csv"
INV_LONG = ROOT / "data" / "derived" / "investor_flow_daily.csv"

OUT_BASE = ROOT / "data" / "derived" / "dashboard"
OUT_ARCHIVE = OUT_BASE / "archive"
OUT_CHART = ROOT / "data" / "derived" / "charts"

NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}


def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime(fmt)


def today_kst_date() -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")


def fetch_top10_from_naver(market: str) -> pd.DataFrame:
    import re
    import requests
    import pandas as pd
    from io import StringIO

    sosok = "0" if market.upper() == "KOSPI" else "1"
    url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page=1"
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      "Referer": "https://finance.naver.com/",
      "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()

    # 🔥 CI 인코딩 깨짐 방지
    r.encoding = "euc-kr"

    tables = pd.read_html(StringIO(r.text), match="종목명")
    if not tables:
        raise RuntimeError("Naver parse failed: no table matched '종목명'")

    df = tables[0].copy()
    df = df.dropna(subset=["종목명"]).copy()
    
    def to_num(x):
        s = str(x).replace(",", "").strip()
        s = re.sub(r"[^\d\.\-]", "", s)
        return pd.to_numeric(s, errors="coerce")

    close_col = "현재가" if "현재가" in df.columns else ("종가" if "종가" in df.columns else None)
    mcap_col  = next((c for c in df.columns if "시가총액" in str(c)), None)
    ret_col   = next((c for c in df.columns if "등락률" in str(c)), None)

    if close_col is None or mcap_col is None:
        raise RuntimeError(f"Naver table missing cols. columns={list(df.columns)}")

    out = pd.DataFrame()
    out["ticker"] = ""
    out["name"] = df["종목명"].astype(str)
    out["close"] = df[close_col].map(to_num)
    out["mcap"] = df[mcap_col].map(to_num) * 1e8
    out["return_1d"] = df[ret_col].map(to_num) if ret_col else 0.0

    out = out.dropna(subset=["mcap"]).sort_values("mcap", ascending=False).head(10).reset_index(drop=True)
    return out[["ticker", "name", "close", "mcap", "return_1d"]]

def fetch_index_and_turnover_from_naver(market: str) -> Dict[str, Any]:
    """
    네이버 지수 페이지에서 현재지수/거래대금(가능하면) 추출
    """
    code = "KOSPI" if market.upper() == "KOSPI" else "KOSDAQ"
    url = f"https://finance.naver.com/sise/sise_index.naver?code={code}"

    r = requests.get(url, headers=NAVER_HEADERS, timeout=20)
    r.raise_for_status()
    r.encoding = "euc-kr"
    html = r.text

    def to_float(s: str) -> Optional[float]:
        s = str(s).replace(",", "").strip()
        s = re.sub(r"[^\d\.\-]", "", s)
        try:
            return float(s)
        except:
            return None

    # 현재지수
    close = None
    for pat in [
        r"현재지수[^0-9]*([\d,]+\.\d+)",
        r"num[^>]*>\s*([\d,]+\.\d+)\s*<",
    ]:
        m = re.search(pat, html)
        if m:
            close = to_float(m.group(1))
            if close is not None:
                break

    # 거래대금(조/억)
    turnover_krw = None
    m = re.search(r"거래대금[^0-9]*([\d,\.]+)\s*([조억])", html)
    if m:
        v = to_float(m.group(1))
        unit = m.group(2)
        if v is not None:
            turnover_krw = v * (1e12 if unit == "조" else 1e8)

    return {"close": close, "turnover_krw": turnover_krw}


def fetch_investor_flow_from_naver(market: str) -> Dict[str, Optional[float]]:
    """
    네이버 투자자별 매매동향(장중)에서 개인/외국인/기관 순매수(원화) 추출
    반환 키를 프론트/카드와 동일하게: foreign/institution/individual
    """
    from io import StringIO

    sosok = "0" if market.upper() == "KOSPI" else "1"
    url = f"https://finance.naver.com/sise/sise_investor.naver?sosok={sosok}"

    r = requests.get(url, headers=NAVER_HEADERS, timeout=20)
    r.raise_for_status()
    r.encoding = "euc-kr"

    tables = pd.read_html(StringIO(r.text))
    if not tables:
        raise RuntimeError("Naver investor parse failed: no tables")

    df = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any("개인" in c for c in cols) and any("외국" in c for c in cols) and any("기관" in c for c in cols):
            df = t.copy()
            break

    if df is None or df.empty:
        raise RuntimeError("Naver investor table not found/empty")

    row = df.iloc[0].to_dict()

    def to_num(x) -> Optional[float]:
        s = str(x).replace(",", "").strip()
        s = re.sub(r"[^\d\.\-\+]", "", s)
        try:
            return float(s)
        except:
            return None

    def pick_key(keys, contains: str):
        for k in keys:
            if contains in str(k):
                return k
        return None

    keys = list(row.keys())
    k_ind = pick_key(keys, "개인")
    k_for = pick_key(keys, "외국")
    k_ins = pick_key(keys, "기관")

    # 네이버 표는 대체로 억원 단위로 노출 → 원화 환산(억=1e8)
    mul = 1e8
    individual = to_num(row.get(k_ind))
    foreign = to_num(row.get(k_for))
    institution = to_num(row.get(k_ins))

    return {
        "foreign": None if foreign is None else foreign * mul,
        "institution": None if institution is None else institution * mul,
        "individual": None if individual is None else individual * mul,
    }


def fetch_market_snapshot_from_naver(market: str) -> Dict[str, Any]:
    """
    지수/거래대금 + 투자자 수급을 한 번에 가져오는 통합 스냅샷
    투자자 수급 실패 시에도 지수/거래대금은 반환
    """
    idx = fetch_index_and_turnover_from_naver(market)
    try:
        flow = fetch_investor_flow_from_naver(market)
    except Exception as e:
        print(f"[Naver investor flow] {market} 실패 (지수/거래대금은 유지): {e}")
        flow = {"foreign": None, "institution": None, "individual": None}
    return {"close": idx.get("close"), "turnover_krw": idx.get("turnover_krw"), "flow": flow}


def fetch_market_snapshot_from_pykrx(market: str, date_str: str) -> Dict[str, Any]:
    """
    pykrx 기반 마켓 스냅샷 폴백 (Naver 실패 시 사용)
    - 지수 종가/거래대금: get_index_ohlcv_by_date
    - 투자자 순매수: get_market_trading_value_by_investor
    """
    d = to_krx_date(date_str)  # "YYYYMMDD"

    # 1. 지수 종가 & 거래대금
    ticker = "1001" if market.upper() == "KOSPI" else "2001"
    close = None
    turnover_krw = None
    try:
        df_idx = stock.get_index_ohlcv_by_date(d, d, ticker)
        if df_idx is not None and not df_idx.empty:
            close_col = _pick_col(df_idx, ["종가", "Close"])
            close = float(df_idx[close_col].iloc[-1])
            try:
                tv_col = _pick_col(df_idx, ["거래대금", "Turnover", "거래량"])
                turnover_krw = float(df_idx[tv_col].iloc[-1])
            except Exception:
                pass
    except Exception as e:
        print(f"[pykrx index] {market} {e}")

    # 2. 투자자별 순매수 (원화)
    flow: Dict[str, Optional[float]] = {"foreign": None, "institution": None, "individual": None}
    try:
        df_inv = stock.get_market_trading_value_by_investor(d, d, market)
        if df_inv is not None and not df_inv.empty:
            row = df_inv.iloc[-1]
            cols = list(df_inv.columns)
            for k_for in ["외국인합계", "외국인"]:
                if k_for in cols:
                    flow["foreign"] = float(row[k_for])
                    break
            for k_ins in ["기관합계", "기관"]:
                if k_ins in cols:
                    flow["institution"] = float(row[k_ins])
                    break
            for k_ind in ["개인"]:
                if k_ind in cols:
                    flow["individual"] = float(row[k_ind])
                    break
    except Exception as e:
        print(f"[pykrx investor] {market} {e}")

    return {"close": close, "turnover_krw": turnover_krw, "flow": flow}


# ------------------------
# Top10 treemap data collection (pykrx wrapper with column-safe logic)
# ------------------------

def fetch_top10_mcap_and_return(date_str: str, market: str) -> pd.DataFrame:
    try:
        d = to_krx_date(date_str)
        df = stock.get_market_cap_by_ticker(d, market=market)
        if df is None or df.empty:
            raise RuntimeError("pykrx empty")

        close_col = _pick_col(df, ["종가", "현재가", "Close"])
        mcap_col  = _pick_col(df, ["시가총액", "상장시가총액", "Market Cap"])
        ret_col   = next((c for c in df.columns if "등락률" in str(c)), None)

        df = df.sort_values(mcap_col, ascending=False).head(10).copy()
        df["ticker"] = df.index.astype(str)
        df["name"] = df["ticker"].map(stock.get_market_ticker_name)
        df["close"] = pd.to_numeric(df[close_col], errors="coerce")
        df["mcap"] = pd.to_numeric(df[mcap_col], errors="coerce")
        df["return_1d"] = pd.to_numeric(df[ret_col], errors="coerce") if ret_col else 0.0

        out = df[["ticker", "name", "close", "mcap", "return_1d"]]
        out = out.dropna(subset=["mcap"]).reset_index(drop=True)
        out["ticker"] = out["ticker"].fillna("").astype(str)

        if out.empty:
            raise RuntimeError("pykrx cleaned empty")

        return out

    except Exception as e:
        print(f"[Top10] pykrx failed -> fallback to Naver ({market})", e)
        return fetch_top10_from_naver(market)

def fetch_index_and_turnover_from_naver(market: str) -> Dict[str, Any]:
    """
    market: "KOSPI" or "KOSDAQ"
    return: {"close": float|None, "turnover_krw": float|None}
    """
    import re
    import requests

    code = "KOSPI" if market.upper() == "KOSPI" else "KOSDAQ"
    url = f"https://finance.naver.com/sise/sise_index.naver?code={code}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    r.encoding = "euc-kr"
    html = r.text

    def to_float(s: str) -> Optional[float]:
        s = str(s).replace(",", "").strip()
        s = re.sub(r"[^\d\.\-]", "", s)
        try:
            return float(s)
        except:
            return None

    # 지수 현재값 후보 (네이버 마크업이 조금씩 달라서 패턴 2개)
    close = None
    for pat in [
        r'현재지수[^0-9]*([\d,]+\.\d+)',
        r'num[^>]*>\s*([\d,]+\.\d+)\s*<',
    ]:
        m = re.search(pat, html)
        if m:
            close = to_float(m.group(1))
            if close is not None:
                break

    # 거래대금(조/억 단위 텍스트)
    turnover_krw = None
    m = re.search(r'거래대금[^0-9]*([\d,\.]+)\s*([조억])', html)
    if m:
        v = to_float(m.group(1))
        unit = m.group(2)
        if v is not None:
            turnover_krw = v * (1e12 if unit == "조" else 1e8)

    return {"close": close, "turnover_krw": turnover_krw}


def fetch_investor_net_from_naver(market: str) -> Dict[str, Any]:
    """
    네이버 투자자별 매매동향(장중)에서 개인/외국인/기관 순매수(원화) 추출
    return: {"foreign_net": float|None, "institution_net": float|None, "individual_net": float|None}
    """
    import pandas as pd
    import re
    from io import StringIO
    import requests

    sosok = "0" if market.upper() == "KOSPI" else "1"
    url = f"https://finance.naver.com/sise/sise_investor.naver?sosok={sosok}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    r.encoding = "euc-kr"

    tables = pd.read_html(StringIO(r.text))
    if not tables:
        raise RuntimeError("Naver investor parse failed: no tables")

    df = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any("개인" in c for c in cols) and any("외국" in c for c in cols) and any("기관" in c for c in cols):
            df = t.copy()
            break
    if df is None or df.empty:
        raise RuntimeError("Naver investor table not found/empty")

    row = df.iloc[0].to_dict()

    def to_num(x) -> Optional[float]:
        s = str(x).replace(",", "").strip()
        s = re.sub(r"[^\d\.\-\+]", "", s)
        try:
            return float(s)
        except:
            return None

    def pick_key(keys, contains: str):
        for k in keys:
            if contains in str(k):
                return k
        return None

    keys = list(row.keys())
    k_ind = pick_key(keys, "개인")
    k_for = pick_key(keys, "외국")
    k_ins = pick_key(keys, "기관")

    individual = to_num(row.get(k_ind))
    foreign = to_num(row.get(k_for))
    inst = to_num(row.get(k_ins))

    # 네이버 투자자 표는 보통 '억원' 단위로 많이 노출됨 → 원화로 환산
    mul = 1e8

    return {
        "foreign_net": None if foreign is None else foreign * mul,
        "institution_net": None if inst is None else inst * mul,
        "individual_net": None if individual is None else individual * mul,
    }

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

def signal_label(ratio: Optional[float], strong: float = 0.05, normal: float = 0.02) -> Optional[str]:
    if ratio is None:
        return None
    r = float(ratio)
    a = abs(r)

    if a < normal:
        return "WEAK_BUY" if r > 0 else ("WEAK_SELL" if r < 0 else "WEAK")

    if a < strong:
        return "NORMAL_BUY" if r > 0 else "NORMAL_SELL"

    return "STRONG_BUY" if r > 0 else "STRONG_SELL"



def to_krx_date(s: str) -> str:
    return str(s).replace("-", "")


def to_dash_date(s: str) -> str:
    s = str(s)
    if "-" not in s and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    cols = list(df.columns)

    # 1. exact match
    for c in candidates:
        if c in cols:
            return c

    # 2. case-insensitive match
    lower_map = {str(col).lower(): col for col in cols}
    for c in candidates:
        key = str(c).lower()
        if key in lower_map:
            return lower_map[key]

    # 3. substring match (case-insensitive)
    for col in cols:
        col_l = str(col).lower()
        for c in candidates:
            if str(c).lower() in col_l:
                return col

    raise KeyError(f"필요한 컬럼을 찾을 수 없습니다: {candidates} / available={cols}")

def signal_label(ratio: Optional[float], strong: float = 0.05, normal: float = 0.02) -> Optional[str]:
    if ratio is None:
        return None
    r = float(ratio)
    a = abs(r)

    if a < normal:
        return "WEAK_BUY" if r > 0 else ("WEAK_SELL" if r < 0 else "WEAK")

    if a < strong:
        return "NORMAL_BUY" if r > 0 else "NORMAL_SELL"

    return "STRONG_BUY" if r > 0 else "STRONG_SELL"


def unit_mult(raw_hint: str) -> float:
    s = str(raw_hint)
    if "(십억원)" in s:
        return 1e9
    if "(억원)" in s:
        return 1e8
    if "(백만원)" in s:
        return 1e6
    if "(천원)" in s:
        return 1e3
    return 1.0


def norm_inv(x: str) -> str:
    """
    Normalize long-form investor_type -> foreign / institution / individual
    """
    s = str(x).strip()
    if not s:
        return s
    base = s.split("(")[0].strip()
    if "외국" in base:
        return "foreign"
    if "기관" in base or base in ["institution_total", "institution"]:
        return "institution"
    if "개인" in base:
        return "individual"
    if base in ["foreign", "foreigner", "foreign_total"]:
        return "foreign"
    if base in ["individual", "individual_total"]:
        return "individual"
    return base


def prev_business_day(date_str: str) -> str:
    from datetime import datetime, timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    # 넉넉하게 20일치 영업일을 조회해서 에러 방지
    start = (d - timedelta(days=20)).strftime("%Y-%m-%d")
    try:
        days = stock.get_previous_business_days(fromdate=to_krx_date(start), todate=to_krx_date(date_str))
        if not days or len(days) < 2:
            # 만약 pykrx가 실패하면 강제로 하루 전 평일 계산 (비상용)
            return (d - timedelta(days=1 if d.weekday() != 0 else 3)).strftime("%Y-%m-%d")
        return to_dash_date(days[-2])
    except:
        # 에러 발생 시 수동 계산 결과 반환
        return (d - timedelta(days=1 if d.weekday() != 0 else 3)).strftime("%Y-%m-%d")


def fetch_upjong_top_bottom3_from_naver() -> Dict[str, List[Dict[str, Any]]]:
    """
    네이버 업종별 시세에서 업종 등락률 상위/하위 3개를 가져온다.
    return: {"top": [{"name":..., "return_pct":...}, ...], "bottom": [...]}
    """
    import pandas as pd
    import re
    from io import StringIO

    url = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    r.encoding = "euc-kr"  # ✅ 한글 깨짐 방지

    tables = pd.read_html(StringIO(r.text))
    if not tables:
        raise RuntimeError("Naver upjong parse failed: no table matched '업종명'")

    df = tables[0].copy()
    
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            (str(a).strip() if "Unnamed" not in str(a) else str(b).strip())
            if (str(b).strip() == "" or "Unnamed" in str(b))
            else str(b).strip()
            for a, b in df.columns.to_list()
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]
    
    # ✅ 업종명 컬럼 자동 탐색 후 dropna
    name_col = next((c for c in df.columns if "업종" in str(c)), None)
    if name_col is None:
        raise RuntimeError(f"Naver upjong: name col not found. columns={list(df.columns)}")
    
    df = df.dropna(subset=[name_col]).copy()
    df.rename(columns={name_col: "업종명"}, inplace=True)

    # 등락률 컬럼 찾기(보통 '전일대비')
    ret_col = None
    for c in df.columns:
        if "전일대비" in str(c) or "등락률" in str(c):
            ret_col = c
            break
    if ret_col is None:
        raise RuntimeError(f"Naver upjong table missing return col. columns={list(df.columns)}")

    def to_pct(x):
        s = str(x).strip().replace("%", "")
        s = re.sub(r"[^\d\.\-\+]", "", s)
        return pd.to_numeric(s, errors="coerce")

    df["return_pct"] = df[ret_col].map(to_pct)
    df = df.dropna(subset=["return_pct"]).copy()

    top = df.sort_values("return_pct", ascending=False).head(3)
    bottom = df.sort_values("return_pct", ascending=True).head(3)

    def pack(dd):
        return [{"name": str(r["업종명"]), "return_pct": float(r["return_pct"])} for _, r in dd.iterrows()]

    return {"top": pack(top), "bottom": pack(bottom)}

# ------------------------
# Extras: Top10 / Vol / Breadth
# ------------------------



def make_treemap_png(df_top10: pd.DataFrame, title: str, outpath: Path, market: str = "") -> None:
    """
    - 상승=빨강, 하락=파랑(한국식)
    - |등락률| 클수록 진하게
    - vmax 자동 스케일(당일 top10 변동폭 기반)
    - KOSPI/KOSDAQ 색조 살짝 다르게
    - 범례(legend) 추가
    """
    import matplotlib.colors as mcolors

    outpath.parent.mkdir(parents=True, exist_ok=True)

    if df_top10 is None or df_top10.empty:
        raise RuntimeError("Top10 DataFrame is empty: cannot draw treemap")

    df_top10 = df_top10.copy()
    df_top10["mcap"] = pd.to_numeric(df_top10["mcap"], errors="coerce")
    df_top10["return_1d"] = pd.to_numeric(df_top10.get("return_1d"), errors="coerce")
    df_top10 = df_top10.dropna(subset=["mcap"])
    df_top10 = df_top10[df_top10["mcap"] > 0]

    if df_top10.empty:
        raise RuntimeError("Top10 DataFrame has no positive mcap rows: cannot draw treemap")

    sizes = df_top10["mcap"].astype(float).tolist()
    rets = df_top10["return_1d"].fillna(0.0).astype(float).tolist()
    labels = [f"{r['name']}\n{float(r['return_1d']):+.2f}%" for _, r in df_top10.iterrows()]

    # -----------------------------
    # 1) vmax 자동 스케일링
    #   - top10의 |등락률|에서 85퍼센타일을 기준으로
    #   - 너무 작거나 큰 값은 클램프(2~12%)
    # -----------------------------
    abs_rets = pd.Series([abs(x) for x in rets if pd.notna(x)])
    if abs_rets.empty:
        vmax = 7.0
    else:
        vmax = float(abs_rets.quantile(0.85))
        vmax = max(2.0, min(vmax, 12.0))

    # -----------------------------
    # 2) 시장별 색조(살짝만)
    # -----------------------------
    mk = str(market).upper()
    if mk == "KOSDAQ":
        # KOSDAQ: 조금 더 쨍한 톤
        red_light, red_dark = "#FFE0E0", "#C4001A"
        blue_light, blue_dark = "#E0ECFF", "#003BB3"
    else:
        # KOSPI(기본): 조금 더 차분한 톤
        red_light, red_dark = "#FFD6D6", "#B00020"
        blue_light, blue_dark = "#D6E4FF", "#0033A0"

    neutral = "#F2F2F2"

    def lerp(c1, c2, t):
        a = mcolors.to_rgb(c1)
        b = mcolors.to_rgb(c2)
        return tuple(a[i] * (1 - t) + b[i] * t for i in range(3))

    def ret_to_color(ret):
        if ret is None or pd.isna(ret):
            return "#DDDDDD"
        r = float(ret)
        t = min(abs(r) / vmax, 1.0)  # 0~1
        if r > 0:
            return lerp(red_light, red_dark, t)
        if r < 0:
            return lerp(blue_light, blue_dark, t)
        return neutral

    colors = [ret_to_color(x) for x in rets]

    # -----------------------------
    # 3) Draw
    # -----------------------------
    plt.figure(figsize=(10, 6))
    squarify.plot(
        sizes=sizes,
        label=labels,
        color=colors,
        alpha=0.95,
        # text_kwargs={"fontsize": 10}  # 필요하면 켜
    )

    plt.title(f"{title} (색상기준 ±{vmax:.1f}%)")
    plt.axis("off")

    # -----------------------------
    # 4) Legend(범례) 추가: 좌하단
    # -----------------------------
    # -3단계 예시(약/중/강). vmax 기반으로 퍼센트 표시
    levels = [0.33, 0.66, 1.0]
    pos_pcts = [vmax * lv for lv in levels]
    neg_pcts = [-vmax * lv for lv in levels]

    legend_handles = []
    legend_labels = []

    # 상승(빨강)
    for p in pos_pcts:
        legend_handles.append(plt.Line2D([0], [0], marker='s', linestyle='', markersize=10,
                                         markerfacecolor=ret_to_color(p), markeredgecolor='none'))
        legend_labels.append(f"+{p:.1f}%")

    # 하락(파랑)
    for p in neg_pcts:
        legend_handles.append(plt.Line2D([0], [0], marker='s', linestyle='', markersize=10,
                                         markerfacecolor=ret_to_color(p), markeredgecolor='none'))
        legend_labels.append(f"{p:.1f}%")

    plt.legend(
        legend_handles,
        legend_labels,
        loc="lower left",
        frameon=True,
        fontsize=9,
        title="등락률 강도",
        title_fontsize=9
    )

    plt.tight_layout()
    plt.savefig(outpath, dpi=150)
    plt.close()

def fetch_volatility_top5(date_str: str, market: str) -> List[Dict[str, Any]]:
    prev_str = prev_business_day(date_str)
    today = stock.get_market_ohlcv_by_ticker(to_krx_date(date_str), market=market)
    prev = stock.get_market_ohlcv_by_ticker(to_krx_date(prev_str), market=market)
    if today is None or today.empty or prev is None or prev.empty:
        raise RuntimeError(f"pykrx ohlcv empty: date={date_str}/{prev_str}, market={market}")

    close_col = _pick_col(today, ["종가", "Close", "CLOSE", "close"])
    prev_close_col = _pick_col(prev, ["종가", "Close", "CLOSE", "close"])

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
# Load base data
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
    우선 pivot 파일을 사용.
    없으면 long-form을 읽어서 pivot 생성.
    """
    # pivot first
    if INV_PIVOT.exists():
        inv = pd.read_csv(INV_PIVOT)
        if not inv.empty:
            inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.date.astype(str)
            inv["market"] = inv["market"].astype(str)
            for c in ["foreign_net", "institution_net", "individual_net"]:
                inv[c] = pd.to_numeric(inv.get(c), errors="coerce")
            return inv.sort_values(["date", "market"]).reset_index(drop=True)

    # fallback: long-form -> pivot
    if not INV_LONG.exists():
        return pd.DataFrame(columns=["date", "market", "foreign_net", "institution_net", "individual_net"])

    inv = pd.read_csv(INV_LONG)
    if inv.empty:
        return pd.DataFrame(columns=["date", "market", "foreign_net", "institution_net", "individual_net"])

    inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.date.astype(str)
    inv["market"] = inv["market"].astype(str)
    inv["investor_type"] = inv["investor_type"].map(norm_inv)

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


# ------------------------
# Core: market cards
# ------------------------

def load_index_rows(liq: pd.DataFrame, date_str: str) -> pd.DataFrame:
    day = liq[liq["date"] == date_str].copy()
    if day.empty:
        raise RuntimeError(f"No liquidity rows for date={date_str}")
    return day.sort_values(["market"]).reset_index(drop=True)


def build_market_cards(liq_day: pd.DataFrame, inv_day: pd.DataFrame) -> Dict[str, Any]:
    inv_map: Dict[str, dict] = {}
    if inv_day is not None and not inv_day.empty:
        for _, rr in inv_day.iterrows():
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

    # ✅ 대시보드 표기 기준일: 오늘(KST) — Top10 기준과 동일
    date_str = today_kst_date()
    print("Dashboard date (KST):", date_str)

    dashboard: Dict[str, Any] = {
        "date": date_str,
        "version": "1.0",
        "markets": {},  # ✅ 여기부터 네이버로 채움
        "extras": {
            "top10_treemap": {"KOSPI": [], "KOSDAQ": []},
            "treemap_png": {
                "KOSPI": "data/derived/charts/treemap_kospi_top10_latest.png",
                "KOSDAQ": "data/derived/charts/treemap_kosdaq_top10_latest.png",
            },
            "upjong": {"top": [], "bottom": []},
        },
    }

    # ✅ as-of 시간(장중 갱신 확인용)
    dashboard["extras"]["naver_asof_kst"] = now_kst_str()

    # 1) markets: 네이버 스냅샷 → 실패 시 pykrx 폴백
    sources_used: list = []
    for mk in ["KOSPI", "KOSDAQ"]:
        snap = None
        try:
            snap = fetch_market_snapshot_from_naver(mk)
            sources_used.append("naver")
        except Exception as e_naver:
            print(f"[Naver] {mk} snapshot failed: {e_naver}, pykrx 폴백 시도")
            dashboard["extras"][f"naver_{mk.lower()}_error"] = str(e_naver)
            try:
                snap = fetch_market_snapshot_from_pykrx(mk, date_str)
                sources_used.append("pykrx")
            except Exception as e_pkrx:
                print(f"[pykrx] {mk} snapshot도 실패: {e_pkrx}")
                dashboard["extras"][f"pykrx_{mk.lower()}_error"] = str(e_pkrx)
                sources_used.append("failed")
                continue

        if snap is None:
            continue

        close = snap.get("close")
        turnover = snap.get("turnover_krw")

        flow = snap.get("flow", {}) or {}
        foreign = flow.get("foreign")
        inst = flow.get("institution")
        indiv = flow.get("individual")

        def _ratio(v: Optional[float], tv=turnover) -> Optional[float]:
            if v is None or tv is None or tv == 0:
                return None
            return float(v) / float(tv)

        ratios = {
            "foreign": _ratio(foreign),
            "institution": _ratio(inst),
            "individual": _ratio(indiv),
        }

        dashboard["markets"][mk] = {
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

    dashboard["extras"]["market_snapshot_source"] = ",".join(sources_used) if sources_used else "failed"

    # 2) Upjong (업종 상/하위) — 네이버 기반인데 너 코드에서 lxml 없으면 깨질 수 있음
    #    (workflow에 pip install lxml html5lib 해주는 건 별도)
    try:
        # 기존 함수가 파일에 남아있다면 그대로 호출 가능.
        # 여기서는 너가 이미 구현한 fetch_upjong_top_bottom3_from_naver()를 그대로 쓴다는 전제.
        from io import StringIO  # noqa
        # ---- 아래 함수가 기존 파일에 있다면 그대로 동작 ----
        # dashboard["extras"]["upjong"] = fetch_upjong_top_bottom3_from_naver()
        # --------------------------------------------------
        # 이 복붙 버전에서는 upjong 함수가 "아래에 포함돼있지 않으면" 에러 나니까,
        # 너 파일에 upjong 함수가 이미 있는 상태라는 전제하에 호출만 열어둠.
        dashboard["extras"]["upjong"] = fetch_upjong_top_bottom3_from_naver()
    except Exception as e:
        dashboard["extras"]["upjong"] = {"top": [], "bottom": []}
        dashboard["extras"]["upjong_error"] = str(e)

    # 3) Top10 treemap: 네이버 TOP10으로 통일 (기준 일치)
    try:
        TOP_N = 10
        for mk in ["KOSPI", "KOSDAQ"]:
            df_top = fetch_top10_from_naver(mk)
            dashboard["extras"]["top10_treemap"][mk] = df_top.to_dict(orient="records")

            make_treemap_png(
                df_top,
                f"{mk} 시총 TOP{TOP_N} — LIVE {dashboard['extras']['naver_asof_kst']}",
                OUT_CHART / f"treemap_{mk.lower()}_top10_latest.png",
                market=mk,
            )
    except Exception as e:
        dashboard["extras"]["top10_error"] = str(e)

    # 4) 저장
    archive_path = OUT_ARCHIVE / f"{date_str}.json"
    latest_path = OUT_BASE / "latest.json"

    import math

    def sanitize_for_json(obj):
        if isinstance(obj, float) and math.isnan(obj):
            return None
        if isinstance(obj, dict):
            return {k: sanitize_for_json(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [sanitize_for_json(v) for v in obj]
        return obj

    dashboard = sanitize_for_json(dashboard)

    archive_path.write_text(
        json.dumps(dashboard, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    latest_path.write_text(
        json.dumps(dashboard, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )

    print("Built dashboard for (KST):", date_str)
    print("Archive:", archive_path)
    print("Latest:", latest_path)
    print("Charts:", OUT_CHART)


if __name__ == "__main__":
    main()
