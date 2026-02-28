from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

ROOT = Path(__file__).resolve().parents[1]

# 우선순위: complete/merged 파일이 있으면 그걸 사용 (없으면 기존 liquidity_daily.csv)
CANDIDATES = [
    ROOT / "data" / "history" / "liquidity_daily_complete.csv",
    ROOT / "data" / "history" / "liquidity_daily_merged.csv",
    ROOT / "data" / "history" / "liquidity_daily.csv",
]
IN_CSV = next((p for p in CANDIDATES if p.exists()), None)
if IN_CSV is None:
    raise FileNotFoundError("No input CSV found in data/history/ (liquidity_daily*.csv)")

OUT_DIR = ROOT / "data" / "derived" / "charts"

def _has_cols(df: pd.DataFrame, cols: list[str]) -> bool:
    return all(c in df.columns for c in cols)

def _safe_div(a, b):
    return a / b.replace({0: pd.NA})

def _prep_market(df: pd.DataFrame, market: str) -> pd.DataFrame:
    d = df[df["market"] == market].copy()
    d["date"] = pd.to_datetime(d["date"])
    d = d.sort_values("date").reset_index(drop=True)
    return d

def _apply_window(d: pd.DataFrame, window_days: int | None):
    if window_days is None:
        return d
    if len(d) == 0:
        return d
    cutoff = d["date"].max() - pd.Timedelta(days=window_days)
    return d[d["date"] >= cutoff].copy()

def plot_close_vs_turnover(d: pd.DataFrame, market: str, window_days: int | None = 365):
    """
    Close = line (left axis)
    Turnover = bar (right axis)
    turnover_krw is assumed to be KRW (원).
    """
    d = _apply_window(d, window_days)
    if len(d) == 0:
        return None

    # Turnover in trillion KRW for readability
    d["turnover_trn"] = pd.to_numeric(d["turnover_krw"], errors="coerce") / 1e12

    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Close line
    ax1.plot(d["date"], pd.to_numeric(d["close"], errors="coerce"))
    ax1.set_ylabel(f"{market} Index (Close)")
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_major_formatter(
        mdates.ConciseDateFormatter(ax1.xaxis.get_major_locator())
    )

    # Turnover bars
    ax2 = ax1.twinx()
    ax2.bar(d["date"], d["turnover_trn"], width=1.0)
    ax2.set_ylabel("Turnover (KRW, Trillion)")

    title = f"{market}: Close (Line) vs Turnover (Bar)"
    if window_days:
        title += f" — last {window_days}d"
    ax1.set_title(title)

    fig.tight_layout()
    return fig

def plot_investor_net_and_ratio(d: pd.DataFrame, market: str, window_days: int | None = 365):
    """
    Net flow (bars, left axis): individual_net / foreign_net / institution_net  (KRW)
    Ratio (lines, right axis): individual_ratio / foreign_ratio (= net / turnover)
    """
    # 필요한 컬럼이 있어야 그린다
    need_any = ["individual_net", "foreign_net"]
    if not any(c in d.columns for c in need_any):
        return None

    d = _apply_window(d, window_days)
    if len(d) == 0:
        return None

    # 숫자 변환
    if "turnover_krw" in d.columns:
        d["turnover_krw"] = pd.to_numeric(d["turnover_krw"], errors="coerce")

    for c in ["individual_net", "foreign_net", "institution_net"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    # ratio 자동 생성(없으면)
    if "turnover_krw" in d.columns:
        if "individual_net" in d.columns and "individual_ratio" not in d.columns:
            d["individual_ratio"] = _safe_div(d["individual_net"], d["turnover_krw"])
        if "foreign_net" in d.columns and "foreign_ratio" not in d.columns:
            d["foreign_ratio"] = _safe_div(d["foreign_net"], d["turnover_krw"])

    # net을 "십억원(=1e9 KRW)" 단위로 축약해서 표시
    scale = 1e9
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # bar 폭/오프셋 (겹치지 않게)
    width = 0.8
    # 날짜를 bar로 그릴 때는 그냥 같은 x에 여러 bar가 겹치기 쉬우니 살짝 투명 처리
    # (깔끔 우선: 2개만 그리거나, 3개면 더 투명하게)
    bar_alpha = 0.6

    # individual/foreign/institution net bars
    if "individual_net" in d.columns:
        ax1.bar(d["date"], d["individual_net"] / scale, width=width, alpha=bar_alpha, label="Individual net (₩bn)")
    if "foreign_net" in d.columns:
        ax1.bar(d["date"], d["foreign_net"] / scale, width=width, alpha=bar_alpha, label="Foreign net (₩bn)")
    if "institution_net" in d.columns:
        ax1.bar(d["date"], d["institution_net"] / scale, width=width, alpha=bar_alpha, label="Institution net (₩bn)")

    ax1.set_ylabel("Net flow (KRW, Billion)")
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_major_formatter(
        mdates.ConciseDateFormatter(ax1.xaxis.get_major_locator())
    )

    # ratio lines (secondary)
    ax2 = ax1.twinx()
    if "individual_ratio" in d.columns:
        ax2.plot(d["date"], pd.to_numeric(d["individual_ratio"], errors="coerce"), label="Individual / Turnover")
    if "foreign_ratio" in d.columns:
        ax2.plot(d["date"], pd.to_numeric(d["foreign_ratio"], errors="coerce"), label="Foreign / Turnover")
    ax2.set_ylabel("Net / Turnover (ratio)")

    title = f"{market}: Investor Net (Bar) & Net/Turnover (Line)"
    if window_days:
        title += f" — last {window_days}d"
    ax1.set_title(title)

    # 범례(두 축 합치기)
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    if handles1 or handles2:
        ax1.legend(handles1 + handles2, labels1 + labels2, loc="upper left")

    fig.tight_layout()
    return fig

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(IN_CSV)

    # 필수 컬럼 체크
    for col in ["date", "market"]:
        if col not in df.columns:
            raise KeyError(f"Missing required column: {col} in {IN_CSV}")

    # Close/turnover는 없으면 차트를 못 그림
    if not _has_cols(df, ["turnover_krw", "close"]):
        raise KeyError(f"Need columns turnover_krw and close in {IN_CSV}. Got: {list(df.columns)}")

    for market in ["KOSPI", "KOSDAQ"]:
        d = _prep_market(df, market)

        # 1) Close vs Turnover
        fig_1y = plot_close_vs_turnover(d, market, window_days=365)
        if fig_1y is not None:
            out_1y = OUT_DIR / f"{market.lower()}_close_vs_turnover_1y.png"
            fig_1y.savefig(out_1y, dpi=160)
            plt.close(fig_1y)

        fig_all = plot_close_vs_turnover(d, market, window_days=None)
        if fig_all is not None:
            out_all = OUT_DIR / f"{market.lower()}_close_vs_turnover_all.png"
            fig_all.savefig(out_all, dpi=160)
            plt.close(fig_all)

        # 2) Investor net + ratio (있을 때만)
        fig_inv_1y = plot_investor_net_and_ratio(d, market, window_days=365)
        if fig_inv_1y is not None:
            out_inv_1y = OUT_DIR / f"{market.lower()}_investor_net_ratio_1y.png"
            fig_inv_1y.savefig(out_inv_1y, dpi=160)
            plt.close(fig_inv_1y)

        fig_inv_all = plot_investor_net_and_ratio(d, market, window_days=None)
        if fig_inv_all is not None:
            out_inv_all = OUT_DIR / f"{market.lower()}_investor_net_ratio_all.png"
            fig_inv_all.savefig(out_inv_all, dpi=160)
            plt.close(fig_inv_all)

    print("Input:", IN_CSV)
    print("Saved charts into:", OUT_DIR)

if __name__ == "__main__":
    main()
