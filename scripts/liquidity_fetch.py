# scripts/liquidity_fetch.py  (끝부분 merge 안전화 포함 완성본)
import ast
import datetime as dt
import io
import random
import time
from typing import Optional

import pandas as pd
import requests
from pykrx import stock

NAVER_API = "https://api.finance.naver.com/siseJson.naver"
NAVER_SYMBOL = {"KOSPI": "KOSPI", "KOSDAQ": "KOSDAQ"}


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
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"}
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

    return pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col], format="%Y%m%d").dt.date.astype(str),
            "market": market,
            "close": pd.to_numeric(df[close_col], errors="coerce"),
        }
    )


def _pick_turnover_col(cols: list[str]) -> Optional[str]:
    for c in ["거래대금", "거래대금(원)", "거래대금합계", "TRADING_VALUE", "trading_value", "turnover"]:
        if c in cols:
            return c
    return None


def _pykrx_fetch_market_turnover_once(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    s = _to_ymd(start)
    e = _to_ymd(end)

    tv = stock.get_market_trading_value_by_date(s, e, market)

    if isinstance(tv, pd.Series):
        tv = tv.to_frame().T
    if not isinstance(tv, pd.DataFrame):
        return pd.DataFrame()

    tv = tv.reset_index()

    if tv.shape[1] <= 1:
        return pd.DataFrame()

    date_col = "날짜" if "날짜" in tv.columns else tv.columns[0]
    turnover_col = _pick_turnover_col(list(tv.columns))
    if turnover_col is None:
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(tv[date_col]).dt.date.astype(str),
            "market": market,
            "turnover": pd.to_numeric(tv[turnover_col], errors="coerce"),
        }
    )
    if out["turnover"].notna().sum() == 0:
        return pd.DataFrame()

    return out


def _pykrx_fetch_market_turnover(start: dt.date, end: dt.date, market: str, retries: int = 4) -> pd.DataFrame:
    for _ in range(retries):
        time.sleep(0.4 + random.random() * 0.6)
        out = _pykrx_fetch_market_turnover_once(start, end, market)
        if not out.empty:
            return out
    return pd.DataFrame()


# (KRX fallback은 아직 비워둬도 됨. 여기선 파이프라인 안 죽게가 목표)
def _krx_fetch_turnover_fallback(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    return pd.DataFrame()


def fetch_liquidity_range(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    close_df = _naver_fetch_index_close(start, end, market)

    turn_df = _pykrx_fetch_market_turnover(start, end, market)
    if turn_df.empty:
        turn_df = _krx_fetch_turnover_fallback(start, end, market)

    # ✅ merge 안전장치: turn_df가 비었거나 key 컬럼 없으면 turnover만 NaN으로 추가
    if (
        turn_df is None
        or turn_df.empty
        or ("date" not in turn_df.columns)
        or ("market" not in turn_df.columns)
    ):
        out = close_df.copy()
        out["turnover"] = pd.NA
        return out.sort_values(["date", "market"]).reset_index(drop=True)

    out = (
        close_df.merge(turn_df, on=["date", "market"], how="left")
        .sort_values(["date", "market"])
        .reset_index(drop=True)
    )
    return out
