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


ROOT = Path(__file__).resolve().parents[1]


HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"

# ✅ 우선 pivot을 읽고, 없으면 long-form을 읽어서 pivot 생성
INV_PIVOT = ROOT / "data" / "derived" / "investor_flow_pivot_daily.csv"
INV_LONG = ROOT / "data" / "derived" / "investor_flow_daily.csv"

OUT_BASE = ROOT / "data" / "derived" / "dashboard"
OUT_ARCHIVE = OUT_BASE / "archive"
OUT_CHART = ROOT / "data" / "derived" / "charts"


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

        if out.empty:
            raise RuntimeError("pykrx cleaned empty")

        return out

    except Exception as e:
        print(f"[Top10] pykrx failed -> fallback to Naver ({market})", e)
        return fetch_top10_from_naver(market)


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

    liq = load_liq_df()
    inv = load_inv_df()

    date_str = sorted(liq["date"].unique())[-1]   # 가장 최근 데이터 자동 선택
    print("Dashboard date:", date_str)

    liq_day = load_index_rows(liq, date_str)
    inv_day = inv[inv["date"] == date_str].copy() if inv is not None and not inv.empty else pd.DataFrame()

    # 프론트가 죽지 않도록 extras 기본 구조 보장
    dashboard: Dict[str, Any] = {
        "date": date_str,
        "version": "1.0",
        "markets": build_market_cards(liq_day, inv_day),
        "extras": {
            "top10_treemap": {"KOSPI": [], "KOSDAQ": []},
            "treemap_png": {
                "KOSPI": "data/derived/charts/treemap_kospi_top10_latest.png",
                "KOSDAQ": "data/derived/charts/treemap_kosdaq_top10_latest.png",
            },
            "volatility_top5": {"KOSPI": [], "KOSDAQ": []},
            "breadth": {"KOSPI": {}, "KOSDAQ": {}},
            "upjong": {"top": [], "bottom": []}, 
        },
    }


        # Upjong (업종 상/하위)
    try:
        dashboard["extras"]["upjong"] = fetch_upjong_top_bottom3_from_naver()
    except Exception as e:
        dashboard["extras"]["upjong"] = {"top": [], "bottom": []}
        dashboard["extras"]["upjong_error"] = str(e)
    
  
    # Top10 treemap + data (실패해도 latest.json 생성)
    try:
        TOP_N = 10
        for mk in ["KOSPI", "KOSDAQ"]:
            # 함수 이름을 정의된 이름과 일치시켰습니다.
            df_top = fetch_top10_mcap_and_return(date_str, mk) 
            
            # 프론트 호환을 위해 key/파일명은 top10 그대로 유지
            dashboard["extras"]["top10_treemap"][mk] = df_top.to_dict(orient="records")

            make_treemap_png(
                df_top,
                f"{mk} 시총 TOP{TOP_N} — {date_str}",
                OUT_CHART / f"treemap_{mk.lower()}_top10_latest.png",
                market=mk
            )
            
    except Exception as e:
        dashboard["extras"]["top10_error"] = str(e)
        

    # Volatility
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

    archive_path = OUT_ARCHIVE / f"{date_str}.json"
    latest_path = OUT_BASE / "latest.json"

    archive_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Built dashboard for:", date_str)
    print("Archive:", archive_path)
    print("Latest:", latest_path)
    print("Charts:", OUT_CHART)


if __name__ == "__main__":
    main()

