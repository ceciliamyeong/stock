import datetime as dt
import time
import random
import pandas as pd
import requests

BASE = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/",
    "Origin": "https://data.krx.co.kr",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}

MKT_MAP = {
    "KOSPI": "STK",
    "KOSDAQ": "KSQ",
}


def _get_session():
    s = requests.Session()
    s.get("https://data.krx.co.kr", headers=HEADERS, timeout=30)
    time.sleep(0.6 + random.random() * 0.8)
    return s


def fetch_investor_flow_range(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    session = _get_session()

    payload = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT02201",
        "locale": "ko_KR",
        "inqTpCd": "1",
        "trdVolVal": "2",
        "askBid": "3",
        "mktId": MKT_MAP[market],
        "strtDd": start.strftime("%Y%m%d"),
        "endDd": end.strftime("%Y%m%d"),
        "share": "2",
        "money": "3",
        "csvxls_isNo": "false",
    }

    r = session.post(BASE, headers=HEADERS, data=payload, timeout=30)

    if r.status_code != 200:
        raise RuntimeError(f"KRX HTTP {r.status_code} body[:200]={r.text[:200]}")

    js = r.json()
    df = pd.DataFrame(js.get("OutBlock_1", []))

    if df.empty:
        return pd.DataFrame(columns=[
            "date", "market",
            "retail_net", "foreign_net", "institution_net"
        ])

    df = df.rename(columns={
        "TRD_DD": "date",
        "INVST_TP_NM": "investor",
        "NET_BUY_AMT": "net_buy_amt",
    })

    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df["net_buy_amt"] = (
        df["net_buy_amt"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    pivot = (
        df.pivot_table(
            index="date",
            columns="investor",
            values="net_buy_amt",
            aggfunc="sum"
        )
        .reset_index()
    )

    out = pd.DataFrame({
        "date": pivot["date"],
        "market": market,
        "retail_net": pivot.get("개인"),
        "foreign_net": pivot.get("외국인"),
        "institution_net": pivot.get("기관합계"),
    })

    return out
