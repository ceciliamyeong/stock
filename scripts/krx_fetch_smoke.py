import json
import datetime as dt
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "raw"
OUT.mkdir(parents=True, exist_ok=True)

BASE = "https://data.krx.co.kr"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/",
    "Origin": "https://data.krx.co.kr",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}

def post(bld: str, payload: dict) -> dict:
    """
    KRX JSON endpoint helper.
    Many KRX endpoints use /comm/bldAttendant/getJsonData.cmd with 'bld' identifying a dataset.
    """
    url = f"{BASE}/comm/bldAttendant/getJsonData.cmd"
    data = {"bld": bld, **payload}
    r = requests.post(url, headers=HEADERS, data=data, timeout=30)
    r.raise_for_status()
    return r.json()

def save_json(obj: dict, name: str):
    (OUT / name).write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    # 테스트 날짜: 2022-01-03 (첫 거래일 근처)
    d = dt.date(2022, 1, 3)
    date_str = d.strftime("%Y%m%d")

    # 1) KOSPI market summary (bld는 예시; 환경에 따라 바뀔 수 있음)
    # 목적: "요청이 성공하는지" 확인
    # 만약 여기서 KeyError/Empty 나오면 bld를 우리가 찾는 과정으로 넘어가면 됨.
    try:
        js = post(
            bld="dbms/MDC/STAT/standard/MDCSTAT01501",  # (예시) 시장지표/거래대금 계열로 많이 쓰이는 패턴
            payload={
                "mktId": "STK",        # 코스피
                "trdDd": date_str,     # 거래일
                "share": "1",
                "money": "1",
                "csvxls_isNo": "false"
            },
        )
        save_json(js, f"smoke_kospi_{date_str}.json")
        print("KOSPI smoke OK:", date_str, "keys:", list(js.keys()))
    except Exception as e:
        print("KOSPI smoke FAILED:", e)

    try:
        js = post(
            bld="dbms/MDC/STAT/standard/MDCSTAT01501",  # 동일 bld로 KOSDAQ
            payload={
                "mktId": "KSQ",        # 코스닥
                "trdDd": date_str,
                "share": "1",
                "money": "1",
                "csvxls_isNo": "false"
            },
        )
        save_json(js, f"smoke_kosdaq_{date_str}.json")
        print("KOSDAQ smoke OK:", date_str, "keys:", list(js.keys()))
    except Exception as e:
        print("KOSDAQ smoke FAILED:", e)

if __name__ == "__main__":
    main()
