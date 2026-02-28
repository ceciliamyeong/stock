import requests
import datetime as dt
import pandas as pd

BASE = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/",
    "Origin": "https://data.krx.co.kr",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}

def fetch_investor_flow(date: dt.date, market: str):
    mkt_map = {
        "KOSPI": "STK",
        "KOSDAQ": "KSQ"
    }

    payload = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT02201",
        "locale": "ko_KR",
        "inqTpCd": "1",
        "trdVolVal": "2",
        "askBid": "3",
        "mktId": mkt_map[market],
        "strtDd": date.strftime("%Y%m%d"),
        "endDd": date.strftime("%Y%m%d"),
        "share": "2",
        "money": "3",
        "csvxls_isNo": "false"
    }

    r = requests.post(BASE, headers=HEADERS, data=payload)
    r.raise_for_status()
    js = r.json()

    df = pd.DataFrame(js["OutBlock_1"])

    # 개인 / 외국인 / 기관 합계 추출
    retail = df.loc[df["INVST_TP_NM"] == "개인", "NET_BUY_AMT"].values
    foreign = df.loc[df["INVST_TP_NM"] == "외국인", "NET_BUY_AMT"].values
    inst = df.loc[df["INVST_TP_NM"] == "기관합계", "NET_BUY_AMT"].values

    return {
        "date": date.isoformat(),
        "market": market,
        "retail_net": float(retail[0]) if len(retail) else None,
        "foreign_net": float(foreign[0]) if len(foreign) else None,
        "institution_net": float(inst[0]) if len(inst) else None,
    }
