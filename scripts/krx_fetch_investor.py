import pandas as pd
from pykrx import stock
import datetime as dt

def fetch_market_investor(start: dt.date, end: dt.date, market: str):
    s = start.strftime("%Y%m%d")
    e = end.strftime("%Y%m%d")

    df = stock.get_market_trading_value_by_investor(s, e, market=market)
    df = df.reset_index()

    # 필요한 컬럼만 정리
    out = pd.DataFrame({
        "date": pd.to_datetime(df["날짜"]).dt.date.astype(str),
        "market": market,
        "individual_net": df["개인"],
        "foreign_net": df["외국인"],
        "institution_net": df["기관계"]
    })

    return out
