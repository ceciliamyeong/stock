# scripts/krx_fetch_investor.py
import argparse
import datetime as dt
from io import BytesIO
import time
import pandas as pd
import requests

GEN_OTP_URL = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
DOWN_URL = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
PRELOAD_URL = "https://data.krx.co.kr/main/main.jsp"

# [12008] 투자자별 거래실적
BLD_INVESTOR = "dbms/MDC/STAT/standard/MDCSTAT02201"

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

def _to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def _mk_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": UA,
            "Referer": "https://data.krx.co.kr/",
            "Origin": "https://data.krx.co.kr",
        }
    )
    # 쿠키/세션 세팅(중요)
    s.get(PRELOAD_URL, timeout=30)
    return s

def _krx_download_csv(sess: requests.Session, form: dict, retry: int = 3) -> bytes:
    """
    1) Generate OTP
    2) Download CSV
    """
    last_err = None
    for _ in range(retry):
        try:
            r = sess.post(GEN_OTP_URL, data=form, timeout=60)
            r.raise_for_status()
            otp = r.text.strip()

            # 가끔 공백/개행만 오는 케이스 방지
            if not otp or len(otp) < 5:
                raise RuntimeError(f"OTP looks empty: {repr(otp)}")

            d = sess.post(DOWN_URL, data={"code": otp}, timeout=60)
            d.raise_for_status()
            return d.content
        except Exception as e:
            last_err = e
            time.sleep(1.5)
    raise RuntimeError(f"KRX download failed: {last_err}")

def _read_csv_bytes(b: bytes) -> pd.DataFrame:
    """
    KRX CSV는 EUC-KR 인코딩이 흔함.
    또, 상단에 불필요한 라인 들어오는 경우가 있어 유연하게 처리.
    """
    # 1) encoding 우선순위
    for enc in ("euc-kr", "cp949", "utf-8-sig", "utf-8"):
        try:
            df = pd.read_csv(BytesIO(b), encoding=enc)
            if len(df.columns) >= 2:
                return df
        except Exception:
            pass

    # 2) 최후: raw decode 후 다시 시도
    text = b.decode("cp949", errors="ignore")
    df = pd.read_csv(BytesIO(text.encode("utf-8")))
    return df

def _normalize_investor_df(raw: pd.DataFrame, market: str) -> pd.DataFrame:
    """
    기대 형태(일별추이):
      - 일자(또는 날짜)
      - 투자자구분
      - 매수 / 매도 (거래대금 기준)
    여기서 개인/외국인/기관계 net(매수-매도) 산출
    """
    cols = list(raw.columns)

    # 날짜 컬럼
    date_col = None
    for c in ["일자", "날짜", "DATE", "Date"]:
        if c in cols:
            date_col = c
            break
    if date_col is None:
        raise KeyError(f"date column not found. cols={cols}")

    # 투자자 구분 컬럼
    inv_col = None
    for c in ["투자자구분", "투자자", "INVESTOR", "Investor"]:
        if c in cols:
            inv_col = c
            break
    if inv_col is None:
        raise KeyError(f"investor column not found. cols={cols}")

    # 매수/매도 컬럼 (거래대금 기준)
    buy_col = None
    sell_col = None
    for c in ["매수", "BUY", "Buy"]:
        if c in cols:
            buy_col = c
            break
    for c in ["매도", "SELL", "Sell"]:
        if c in cols:
            sell_col = c
            break

    if buy_col is None or sell_col is None:
        raise KeyError(f"buy/sell columns not found. cols={cols}")

    df = raw[[date_col, inv_col, buy_col, sell_col]].copy()
    df[date_col] = pd.to_datetime(df[date_col]).dt.date.astype(str)
    df[buy_col] = pd.to_numeric(df[buy_col], errors="coerce")
    df[sell_col] = pd.to_numeric(df[sell_col], errors="coerce")

    # 투자자 명 normalize (KRX 표기 케이스 대응)
    def _map_inv(x: str) -> str:
        x = str(x).strip()
        if x == "개인":
            return "individual"
        if x == "외국인":
            return "foreign"
        # 기관계 / 기관합계 등
        if "기관" in x:
            return "institution"
        return "other"

    df["inv_key"] = df[inv_col].map(_map_inv)
    df["net"] = df[buy_col] - df[sell_col]

    piv = (
        df[df["inv_key"].isin(["individual", "foreign", "institution"])]
        .pivot_table(index=date_col, columns="inv_key", values="net", aggfunc="sum")
        .reset_index()
        .rename(columns={date_col: "date"})
    )
    piv["market"] = market
    piv = piv.rename(
        columns={
            "individual": "individual_net",
            "foreign": "foreign_net",
            "institution": "institution_net",
        }
    )

    # 컬럼 없으면 0/NA 처리
    for c in ["individual_net", "foreign_net", "institution_net"]:
        if c not in piv.columns:
            piv[c] = pd.NA

    return piv[["date", "market", "individual_net", "foreign_net", "institution_net"]]

def fetch_investor_flow(start: dt.date, end: dt.date, market: str, daily: bool = True) -> pd.DataFrame:
    """
    market: 'KOSPI' or 'KOSDAQ'
    daily=True면 일별추이, False면 기간합계
    """
    sess = _mk_session()

    # ✅ 핵심 파라미터
    # inqTpCd: 조회구분 (KRX에서 기간합계/일별추이 토글)
    # - 네 스샷은 1(기간합계)였는데, 우리는 '데일리'가 목적이라 2(일별추이)로 둠
    inqTpCd = "2" if daily else "1"

    # mktId: 시장구분 (KRX 페이지에서 ALL/KOSPI/KOSDAQ 토글에 해당)
    # - 환경/메뉴에 따라 값이 달라질 수 있어서, 가장 흔한 값들을 순차 fallback
    mkt_candidates = []
    if market == "KOSPI":
        mkt_candidates = ["STK", "KOSPI", "1"]
    elif market == "KOSDAQ":
        mkt_candidates = ["KSQ", "KOSDAQ", "2"]
    else:
        raise ValueError("market must be KOSPI or KOSDAQ")

    form_base = {
        "bld": BLD_INVESTOR,
        "locale": "ko_KR",
        "inqTpCd": inqTpCd,
        "trdVolVal": "2",   # 2=거래대금(많이 쓰는 값). 필요시 KRX UI 값에 맞춰 조정
        "askBid": "3",
        "strtDd": _to_ymd(start),
        "endDd": _to_ymd(end),
        "share": "2",
        "money": "3",
        "csvxls_isNo": "false",
    }

    last_err = None
    for mktId in mkt_candidates:
        try:
            form = dict(form_base)
            form["mktId"] = mktId
            b = _krx_download_csv(sess, form)
            raw = _read_csv_bytes(b)
            return _normalize_investor_df(raw, market=market)
        except Exception as e:
            last_err = e
            time.sleep(1.0)

    raise RuntimeError(f"Investor fetch failed for {market}. last_err={last_err}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)    # YYYY-MM-DD
    ap.add_argument("--market", default="BOTH", choices=["KOSPI", "KOSDAQ", "BOTH"])
    ap.add_argument("--mode", default="daily", choices=["daily", "sum"])
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    daily = (args.mode == "daily")

    markets = ["KOSPI", "KOSDAQ"] if args.market == "BOTH" else [args.market]
    frames = [fetch_investor_flow(start, end, m, daily=daily) for m in markets]
    out = pd.concat(frames, ignore_index=True).sort_values(["date", "market"]).reset_index(drop=True)
    print(out.tail(20).to_string(index=False))

if __name__ == "__main__":
    main()
