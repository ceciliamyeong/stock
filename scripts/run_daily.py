# scripts/run_daily.py
from __future__ import annotations

import datetime as dt
import json
import subprocess
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
HIST_DIR = ROOT / "data" / "history"
DERIVED_DIR = ROOT / "data" / "derived"
CHARTS_DIR = DERIVED_DIR / "charts"

LIQUIDITY_CSV = HIST_DIR / "liquidity_daily.csv"
INVESTOR_CSV = DERIVED_DIR / "investor_flow_daily.csv"


def _ensure_dirs():
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)


def _read_liquidity() -> pd.DataFrame:
    if not LIQUIDITY_CSV.exists():
        raise FileNotFoundError(f"Missing {LIQUIDITY_CSV}. (history 백필 파일이 필요)")

    df = pd.read_csv(LIQUIDITY_CSV)

    required = ["date", "market", "turnover_krw", "close"]
    for c in required:
        if c not in df.columns:
            raise KeyError(f"{LIQUIDITY_CSV} missing column '{c}'. got={list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df["market"] = df["market"].astype(str)
    df["turnover_krw"] = pd.to_numeric(df["turnover_krw"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.sort_values(["date", "market"]).reset_index(drop=True)
    return df


def _run_investor_fetch(start: str, end: str) -> None:
    """
    안정적인 시장별 호출 스크립트(krx_fetch_investor.py)를 실행해
    data/derived/investor_flow_daily.csv 를 누적 갱신한다.
    """
    cmd = [
        "python",
        "scripts/krx_fetch_investor.py",
        "--start",
        start,
        "--end",
        end,
        "--market",
        "BOTH",
        "--mode",
        "daily",
    ]
    # 투자자 데이터 실패가 전체 파이프라인을 죽이지 않도록 check=False
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        # Actions 로그에서 보이게 stderr를 출력
        print("[WARN] Investor fetch failed (non-fatal). stderr:")
        print(proc.stderr.strip())
    else:
        print(proc.stdout.strip())


def _read_investor_daily() -> pd.DataFrame:
    """
    investor_flow_daily.csv 형태:
      date, market, investor_type, bid_raw, ask_raw, net_raw, raw_unit_hint
    여기서 individual/foreign/institution_total net을 뽑아서
      date, market, individual_net, foreign_net, institution_net (KRW)
    로 만든다.

    IMPORTANT:
    raw_unit_hint에 '(십억원)' 같은 단위 힌트가 들어올 수 있어 환산한다.
    힌트가 없으면 '원' 단위로 가정한다(보수적).
    """
    if not INVESTOR_CSV.exists():
        return pd.DataFrame(columns=["date", "market", "individual_net", "foreign_net", "institution_net"])

    inv = pd.read_csv(INVESTOR_CSV)
    if inv.empty:
        return pd.DataFrame(columns=["date", "market", "individual_net", "foreign_net", "institution_net"])

    inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.date.astype(str)
    inv["market"] = inv["market"].astype(str)
    inv["investor_type"] = inv["investor_type"].astype(str)

    def _unit_mult(hint: str) -> float:
        s = str(hint)
        if "(십억원)" in s:
            return 1e10
        if "(억원)" in s:
            return 1e8
        if "(백만원)" in s:
            return 1e6
        if "(천원)" in s:
            return 1e3
        return 1.0

    inv["net_raw"] = pd.to_numeric(inv["net_raw"], errors="coerce")
    inv["mult"] = inv["raw_unit_hint"].map(_unit_mult)
    inv["net_krw"] = inv["net_raw"] * inv["mult"]

    # 우리가 원하는 3종만
    keep = inv[inv["investor_type"].isin(["individual", "foreign", "institution_total"])].copy()
    if keep.empty:
        return pd.DataFrame(columns=["date", "market", "individual_net", "foreign_net", "institution_net"])

    pivot = (
        keep.groupby(["date", "market", "investor_type"], as_index=False)["net_krw"]
        .sum()
        .pivot_table(index=["date", "market"], columns="investor_type", values="net_krw", aggfunc="sum")
        .reset_index()
        .rename(
            columns={
                "individual": "individual_net",
                "foreign": "foreign_net",
                "institution_total": "institution_net",
            }
        )
    )

    for c in ["individual_net", "foreign_net", "institution_net"]:
        if c not in pivot.columns:
            pivot[c] = pd.NA

    pivot = pivot.sort_values(["date", "market"]).reset_index(drop=True)
    return pivot


def _merge_investor(liquidity: pd.DataFrame, investor: pd.DataFrame) -> pd.DataFrame:
    """
    liquidity(date, market)에 investor net을 left merge.
    ratio = net / turnover_krw 추가.
    """
    out = liquidity.merge(investor, on=["date", "market"], how="left")

    denom = out["turnover_krw"].replace({0: pd.NA})

    if "individual_net" in out.columns:
        out["individual_ratio"] = out["individual_net"] / denom
    if "foreign_net" in out.columns:
        out["foreign_ratio"] = out["foreign_net"] / denom
    if "institution_net" in out.columns:
        out["institution_ratio"] = out["institution_net"] / denom

    out = out.sort_values(["date", "market"]).reset_index(drop=True)
    return out


def _save_liquidity(df: pd.DataFrame):
    df.to_csv(LIQUIDITY_CSV, index=False)


def _write_latest_json(df: pd.DataFrame):
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()

    out_json = DERIVED_DIR / "latest_liquidity.json"
    payload = {"date": latest_date}

    for _, r in latest.iterrows():
        mk = r["market"]
        payload[mk] = {
            k: (None if pd.isna(r.get(k)) else float(r.get(k)))
            for k in df.columns
            if k not in ["date", "market"]
        }

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Saved:", out_json)


def _run_build_charts():
    # 기존 차트 스크립트는 investor 컬럼이 있으면 자동으로 투자자 차트도 생성함 :contentReference[oaicite:1]{index=1}
    subprocess.run(["python", "scripts/build_liquidity_charts.py"], check=False)


def main():
    _ensure_dirs()

    liq = _read_liquidity()

    # 최근 30 영업일 정도만 투자자 갱신(너무 길게 잡으면 KRX throttling/지연 리스크)
    max_date = dt.date.fromisoformat(liq["date"].max())
    start = (max_date - dt.timedelta(days=45)).isoformat()  # 달력 기준 45일(영업일 ~30)
    end = max_date.isoformat()

    # 1) investor csv 갱신
    _run_investor_fetch(start, end)

    # 2) investor 읽고 pivot
    inv3 = _read_investor_daily()

    # 3) merge + ratio
    merged = _merge_investor(liq, inv3)

    # 4) save liquidity (history 유지하면서 investor 컬럼 추가)
    _save_liquidity(merged)
    print("Saved:", LIQUIDITY_CSV)

    # 5) latest json
    _write_latest_json(merged)

    # 6) charts
    _run_build_charts()


if __name__ == "__main__":
    main()
