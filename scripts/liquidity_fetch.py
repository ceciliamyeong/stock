# scripts/liquidity_fetch.py
import ast
import datetime as dt
import time
import random

import pandas as pd
import requests
from pykrx import stock


NAVER_API = "https://api.finance.naver.com/siseJson.naver"
NAVER_SYMBOL = {
    "KOSPI": "KOSPI",
    "KOSDAQ": "KOSDAQ",
}


def _to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _naver_fetch_index_close(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    params = {
        "symbol": NAVER_SYMBOL[market],
        "requestType": "1",
        "startTime": _to_ymd(start),
        "endTime": _to_ymd(end),
        "timeframe": "day",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://finance.naver.com/",
    }

    r = requests.get(NAVER_API, params=params, headers=headers, timeout=30)
    r.raise_for_status()

    text = r.text.strip().replace("\n", "").replace("\t", "")
    data = ast.literal_eval(text)

    if not data or len(data) < 2:
        return pd.DataFrame(columns=["date", "market", "close"])

    header = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=header)

    date_col = "날짜" if "날짜" in df.columns else df.columns[0]
    close_col = "종가" if "종가" in df.columns else df.columns[4]

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col], format="%Y%m%d").dt.date.astype(str),
            "market": market,
            "close": pd.to_numeric(df[close_col], errors="coerce"),
        }
    )
    return out


def _pykrx_fetch_market_turnover(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    s = _to_ymd(start)
    e = _to_ymd(end)

    time.sleep(0.3 + random.random() * 0.4)

    # ✅ 버전 호환: market은 키워드가 아니라 위치 인자로 전달
    tv = stock.get_market_trading_value_by_date(s, e, market).reset_index()

    date_col = "날짜" if "날짜" in tv.columns else tv.columns[0]

    turnover_col = None
    for c in ["거래대금", "거래대금(원)", "거래대금합계", "TRADING_VALUE", "trading_value", "turnover"]:
        if c in tv.columns:
            turnover_col = c
            break

    if turnover_col is None:
        raise KeyError(f"turnover column not found. cols={list(tv.columns)}")

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(tv[date_col]).dt.date.astype(str),
            "market": market,
            "turnover": pd.to_numeric(tv[turnover_col], errors="coerce"),
        }
    )
    return out


def fetch_liquidity_range(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    close_df = _naver_fetch_index_close(start, end, market)
    turn_df = _pykrx_fetch_market_turnover(start, end, market)

    out = (
        close_df.merge(turn_df, on=["date", "market"], how="outer")
        .sort_values(["date", "market"])
        .reset_index(drop=True)
    )
    return out
