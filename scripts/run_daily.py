# scripts/run_daily.py
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
HIST_DIR = ROOT / "data" / "history"
DERIVED_DIR = ROOT / "data" / "derived"
CHARTS_DIR = DERIVED_DIR / "charts"

LIQUIDITY_CSV = HIST_DIR / "liquidity_daily.csv"

# KRX endpoint (네가 캡쳐한 것)
KRX_JSON_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
KRX_BLD_INVESTOR_FLOW = "dbms/MDC/MAIN/MDCMAIN00103"  # 투자자별 매매동향(코스피/코스닥 탭쪽)

SESSION_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://data.krx.co.kr",
    "Referer": "https://data.krx.co.kr/",
    "X-Requested-With": "XMLHttpRequest",
}


def _ensure_dirs():
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)


def _read_liquidity() -> pd.DataFrame:
    if not LIQUIDITY_CSV.exists():
        raise FileNotFoundError(f"Missing {LIQUIDITY_CSV}. 먼저 백필 CSV를 업로드/커밋해줘.")
    df = pd.read_csv(LIQUIDITY_CSV)

    # required columns
    for c in ["date", "market", "turnover_krw", "close"]:
        if c not in df.columns:
            raise KeyError(f"{LIQUIDITY_CSV} missing column '{c}'. got={list(df.columns)}")

    # normalize types
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df["market"] = df["market"].astype(str)
    df["turnover_krw"] = pd.to_numeric(df["turnover_krw"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.sort_values(["date", "market"]).reset_index(drop=True)
    return df


def _yyyymmdd(date_str: str) -> str:
    # "YYYY-MM-DD" -> "YYYYMMDD"
    return date_str.replace("-", "")


def _parse_intish(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def _krx_post(payload: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    """
    KRX는 세션/쿠키를 요구하는 경우도 있어서 requests.Session()으로 호출.
    """
    with requests.Session() as s:
        r = s.post(KRX_JSON_URL, data=payload, headers=SESSION_HEADERS, timeout=timeout)
        r.raise_for_status()

        # 가끔 text/html로 와도 JSON 문자열이 들어있는 경우가 있어서 그냥 json() 시도
        try:
            return r.json()
        except Exception:
            # fallback
            txt = r.text.strip()
            return json.loads(txt)


def fetch_investor_flow_for_date(date_str: str) -> pd.DataFrame:
    """
    투자자별 매매동향을 특정 일자에 대해 가져와서
    KOSPI/KOSDAQ 시장별 + 투자자유형별 net을 뽑는다.

    반환 DF columns:
      date, market,
      individual_net, foreign_net, institution_net  (KRW)
    """
    ymd = _yyyymmdd(date_str)

    # KRX는 화면마다 날짜 키가 바뀌는 편이라 후보를 여러 개로 시도
    date_keys = ["trdDd", "trdDd1", "trdDd2", "strtDd", "endDd"]

    # 시장 키도 화면에 따라 바뀌어서 후보를 준비
    # 네가 캡쳐한 payload는 mktId=STK 였음.
    # 실제로는 KOSPI/KOSDAQ를 "코스피/코스닥 탭"이 내부적으로 분기하는데,
    # 응답 row에 KOSPI/KOSDAQ가 포함되는 케이스도 있고, 아니면 한 시장만 나오는 케이스도 있음.
    base_payload = {
        "bld": KRX_BLD_INVESTOR_FLOW,
        "mktId": "STK",
    }

    last_err = None
    data = None

    # 1) 가장 흔한 형태: trdDd=YYYYMMDD
    for k in date_keys:
        try_payload = dict(base_payload)
        try_payload[k] = ymd

        # strtDd/endDd 조합도 시도
        if k in ("strtDd", "endDd"):
            try_payload["strtDd"] = ymd
            try_payload["endDd"] = ymd

        try:
            data = _krx_post(try_payload)
            if isinstance(data, dict) and "output" in data and data["output"]:
                break
        except Exception as e:
            last_err = e
            data = None

    if not (isinstance(data, dict) and "output" in data and data["output"]):
        # 날짜 없는 호출(최신)도 한번 시도
        try:
            data = _krx_post(base_payload)
        except Exception as e:
            last_err = e
            data = None

    if not (isinstance(data, dict) and "output" in data and data["output"]):
        raise RuntimeError(f"KRX investor flow fetch failed for {date_str}. last_err={last_err}")

    rows = data["output"]
    tmp = pd.DataFrame(rows)

    # 기대 컬럼: TRD_DD, INVST_TP, NETBID_TRDVAL (or similar)
    # 네 캡쳐는 "INVST_TP":"기관(십억원)" "NETBID_TRDVAL":"567" 형태.
    # 단위가 (십억원)일 수도 있고, 원/천원/백만원일 수도 있어서
    # 문자열에 단위가 있으면 추정해서 KRW로 환산.
    if "TRD_DD" in tmp.columns:
        tmp["date"] = tmp["TRD_DD"].astype(str).str.replace(".", "", regex=False)
        tmp["date"] = tmp["date"].str.replace("/", "", regex=False)
        # 20260227 -> 2026-02-27
        tmp["date"] = pd.to_datetime(tmp["date"], format="%Y%m%d", errors="coerce").dt.date.astype(str)
    else:
        tmp["date"] = date_str

    # net value col 후보
    net_cols = [c for c in tmp.columns if "NET" in c and "TRDVAL" in c]
    if not net_cols:
        # 가끔 NETBID_TRDVAL 말고 netBID... 이런 케이스
        net_cols = [c for c in tmp.columns if "NET" in c]
    if not net_cols:
        raise KeyError(f"KRX investor output has no net column. cols={list(tmp.columns)}")

    net_col = net_cols[0]

    if "INVST_TP" not in tmp.columns:
        raise KeyError(f"KRX investor output missing INVST_TP. cols={list(tmp.columns)}")

    # 단위 추정: INVST_TP에 (십억원)/(억원)/(백만원)/(천원)/(원) 등이 붙는 케이스
    # 예: "기관(십억원)" -> 숫자 * 1e10 KRW
    def unit_multiplier(invst_tp: str) -> float:
        s = str(invst_tp)
        if "(십억원)" in s:
            return 1e10
        if "(억원)" in s:
            return 1e8
        if "(백만원)" in s:
            return 1e6
        if "(천원)" in s:
            return 1e3
        # 단위가 없으면 원 단위로 가정(보수적으로)
        return 1.0

    tmp["mult"] = tmp["INVST_TP"].map(unit_multiplier)
    tmp["net_krw"] = tmp[net_col].map(_parse_intish) * tmp["mult"]

    # 투자자 타입 정규화: 개인/외국인/기관 정도만 뽑기
    def norm_investor(invst_tp: str) -> str:
        s = str(invst_tp)
        # 단위 표기를 제거
        s = s.split("(")[0]
        s = s.strip()
        if "개인" in s:
            return "individual"
        if "외국" in s:
            return "foreign"
        if "기관" in s:
            return "institution"
        return "other"

    tmp["inv_norm"] = tmp["INVST_TP"].map(norm_investor)

    # 시장 식별: 응답에 시장이 없으면 "BOTH를 한 번에 준다"는 가정이 깨질 수 있음.
    # 다행히 네가 보여준 json은 그 화면이 코스피 탭이었는데도 output이 나왔고,
    # 실제로는 코스피/코스닥 각각 호출이 따로일 가능성도 있음.
    # 여기서는 "market" 컬럼 후보를 찾아보고 없으면 'UNKNOWN'으로 둔 뒤,
    # 아래에서 분리 저장은 하지 않고 run_daily 단계에서 KOSPI/KOSDAQ 날짜에 공통 merge(비추)하지 않도록 막는다.
    market_col_candidates = ["MKT_NM", "mktNm", "MKT_ID", "mktId", "IDX_NM", "idxNm"]
    mcol = next((c for c in market_col_candidates if c in tmp.columns), None)

    if mcol:
        # 여기 매핑은 케이스별로 다를 수 있어서 보수적으로:
        # 문자열에 KOSPI/KOSDAQ가 들어가면 그걸 market으로 사용
        def map_market(x: Any) -> str:
            s = str(x).upper()
            if "KOSPI" in s:
                return "KOSPI"
            if "KOSDAQ" in s:
                return "KOSDAQ"
            return "UNKNOWN"
        tmp["market"] = tmp[mcol].map(map_market)
    else:
        tmp["market"] = "UNKNOWN"

    # 우리가 원하는 형태로 pivot
    out = (
        tmp[tmp["inv_norm"].isin(["individual", "foreign", "institution"])]
        .groupby(["date", "market", "inv_norm"], as_index=False)["net_krw"]
        .sum()
        .pivot_table(index=["date", "market"], columns="inv_norm", values="net_krw", aggfunc="sum")
        .reset_index()
        .rename(columns={
            "individual": "individual_net",
            "foreign": "foreign_net",
            "institution": "institution_net",
        })
    )

    # 만약 시장이 UNKNOWN만 있으면, 이 데이터는 "탭 단위로 이미 시장이 정해진 호출"일 가능성이 큼.
    # 그래서 여기선 UNKNOWN을 그대로 두고, run_daily에서 market별로 “정확히 매칭되는 것만 merge”하게 한다.
    return out


def merge_investor_into_liquidity(df: pd.DataFrame, lookback_days: int = 15) -> pd.DataFrame:
    """
    liquidity DF에 투자자 net/ratio 컬럼을 붙인다.
    - 이미 컬럼이 있으면 누락된 날짜만 채우려 시도.
    - KRX 호출이 실패하면 조용히 스킵(데일리 파이프라인이 멈추지 않게).
    """
    need_cols = {"individual_net", "foreign_net", "institution_net"}
    has_any = any(c in df.columns for c in need_cols)

    # target dates: 최근 N일 중, investor 컬럼이 비어있는 날짜를 채운다
    df_dates = sorted(df["date"].unique())
    if not df_dates:
        return df

    max_date = dt.date.fromisoformat(df_dates[-1])
    cutoff = max_date - dt.timedelta(days=lookback_days)

    target_dates = [d for d in df_dates if dt.date.fromisoformat(d) >= cutoff]

    # 이미 investor 값이 있으면 스킵하는 로직
    def _needs_fill(date_str: str) -> bool:
        if not has_any:
            return True
        sub = df[df["date"] == date_str]
        # 컬럼이 있어도 전부 NA면 fill
        cols_present = [c for c in need_cols if c in df.columns]
        if not cols_present:
            return True
        return sub[cols_present].isna().all().all()

    target_dates = [d for d in target_dates if _needs_fill(d)]
    if not target_dates:
        return df

    frames: List[pd.DataFrame] = []
    for date_str in target_dates:
        try:
            inv = fetch_investor_flow_for_date(date_str)

            # inv.market이 UNKNOWN뿐이면 코스피/코스닥을 분리 못한 상태일 수 있음.
            # 이런 경우는 "시장 탭 단위로 호출"을 따로 구현해야 하는데,
            # 네가 지금 캡쳐한 건 일단 응답이 나오는 단계라서,
            # 여기서는 UNKNOWN row는 merge하지 않고 버린다(잘못 섞일 위험이 커서).
            inv = inv[inv["market"].isin(["KOSPI", "KOSDAQ"])].copy()
            if len(inv) == 0:
                continue

            frames.append(inv)

        except Exception:
            # 데일리 파이프라인을 멈추지 않음
            continue

    if not frames:
        return df

    investor_df = pd.concat(frames, ignore_index=True)
    investor_df["date"] = investor_df["date"].astype(str)
    investor_df["market"] = investor_df["market"].astype(str)

    # merge
    out = df.merge(investor_df, on=["date", "market"], how="left")

    # ratio 계산 (0 나눗셈 방지)
    denom = out["turnover_krw"].replace({0: pd.NA})

    if "individual_net" in out.columns:
        out["individual_ratio"] = out["individual_net"] / denom
    if "foreign_net" in out.columns:
        out["foreign_ratio"] = out["foreign_net"] / denom
    if "institution_net" in out.columns:
        out["institution_ratio"] = out["institution_net"] / denom

    out = out.sort_values(["date", "market"]).reset_index(drop=True)
    return out


def save_liquidity(df: pd.DataFrame):
    df.to_csv(LIQUIDITY_CSV, index=False)


def run_build_charts():
    # 같은 repo 안에 있는 build script 실행
    import subprocess
    subprocess.run(["python", "scripts/build_liquidity_charts.py"], check=False)


def main():
    _ensure_dirs()

    df = _read_liquidity()

    # 투자자 데이터: 최근 N일만 시도 (너무 과거는 KRX가 막힐 때가 많고 비용도 큼)
    df2 = merge_investor_into_liquidity(df, lookback_days=15)

    save_liquidity(df2)

    # 파생/대시보드용 최신 JSON(선택)
    latest_date = df2["date"].max()
    latest = df2[df2["date"] == latest_date].copy()
    out_json = DERIVED_DIR / "latest_liquidity.json"
    payload = {"date": latest_date}
    for _, r in latest.iterrows():
        mk = r["market"]
        payload[mk] = {k: (None if pd.isna(r[k]) else float(r[k])) for k in latest.columns if k not in ["date", "market"]}
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # 차트 생성
    run_build_charts()

    print("Saved:", LIQUIDITY_CSV)
    print("Saved:", out_json)


if __name__ == "__main__":
    main()
