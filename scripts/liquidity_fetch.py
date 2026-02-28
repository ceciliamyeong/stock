# scripts/liquidity_fetch.py
import datetime as dt

import pandas as pd
from pykrx import stock

# --- pykrx hotfix: '지수명' 컬럼명이 바뀌거나 누락된 경우를 대비 ---
def _patch_pykrx_index_ticker():
    try:
        from pykrx.website.krx.market.ticker import IndexTicker

        def _safe_get_name(self, ticker):
            # 원래 기대: self.df.loc[ticker, "지수명"]
            if "지수명" in self.df.columns:
                return self.df.loc[ticker, "지수명"]

            # 1) '지수명'을 포함한 컬럼 찾기 (공백/변형 대응)
            for c in self.df.columns:
                if "지수명" in str(c):
                    return self.df.loc[ticker, c]

            # 2) '지수' 포함 컬럼 찾기
            for c in self.df.columns:
                if "지수" in str(c):
                    return self.df.loc[ticker, c]

            # 3) 최후: 첫 번째 컬럼
            return self.df.loc[ticker, self.df.columns[0]]

        IndexTicker.get_name = _safe_get_name

    except Exception:
        # 패치 실패해도 나머지 로직(거래대금 등)은 계속 돌 수 있게
        pass


_patch_pykrx_index_ticker()


INDEX_TICKER = {
    "KOSPI": "1001",
    "KOSDAQ": "2001",
}


def _to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _pick_any_col(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"missing columns: {candidates} / got: {list(df.columns)}")


def fetch_liquidity_range(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    """
    market: 'KOSPI' or 'KOSDAQ'
    output: date, market, close, turnover
    """
    s = _to_ymd(start)
    e = _to_ymd(end)

    # 1) Index close (ticker 기반)
    ticker = INDEX_TICKER[market]
    idx = stock.get_index_ohlcv_by_date(s, e, ticker).reset_index()

    date_col = _pick_any_col(idx, ["날짜", "Date", "date"])
    close_col = _pick_any_col(idx, ["종가", "Close", "close"])

    idx_out = pd.DataFrame(
        {
            "date": pd.to_datetime(idx[date_col]).dt.date.astype(str),
            "market": market,
            "close": pd.to_numeric(idx[close_col], errors="coerce"),
        }
    )

    # 2) Market trading value by date (turnover)
    tv = stock.get_market_trading_value_by_date(s, e, market=market).reset_index()

    tv_date_col = _pick_any_col(tv, ["날짜", "Date", "date"])
    turnover_col = _pick_any_col(
        tv,
        [
            "거래대금",
            "거래대금(원)",
            "거래대금합계",
            "TRADING_VALUE",
            "trading_value",
            "turnover",
        ],
    )

    tv_out = pd.DataFrame(
        {
            "date": pd.to_datetime(tv[tv_date_col]).dt.date.astype(str),
            "market": market,
            "turnover": pd.to_numeric(tv[turnover_col], errors="coerce"),
        }
    )

    out = (
        idx_out.merge(tv_out, on=["date", "market"], how="outer")
        .sort_values(["date", "market"])
        .reset_index(drop=True)
    )
    return out
