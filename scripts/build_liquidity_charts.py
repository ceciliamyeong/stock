# scripts/build_liquidity_charts.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


ROOT = Path(__file__).resolve().parents[1]
IN_CSV = ROOT / "data" / "history" / "liquidity_daily.csv"
OUT_DIR = ROOT / "data" / "derived" / "charts"


def _has_cols(df: pd.DataFrame, cols: list[str]) -> bool:
    return all(c in df.columns for c in cols)


def _safe_num(s):
    return pd.to_numeric(s, errors="coerce")


def _prep_market(df: pd.DataFrame, market: str) -> pd.DataFrame:
    d = df[df["market"] == market].copy()
    d["date"] = pd.to_datetime(d["date"])
    d = d.sort_values("date").reset_index(drop=True)
    return d


def _apply_window(d: pd.DataFrame, window_days: int | None):
    if window_days is None or len(d) == 0:
        return d
    cutoff = d["date"].max() - pd.Timedelta(days=window_days)
    return d[d["date"] >= cutoff].copy()


def plot_close_vs_turnover(d: pd.DataFrame, market: str, window_days: int | None = 365):
    d = _apply_window(d, window_days)
    if len(d) == 0:
        return None

    d["turnover_trn"] = _safe_num(d["turnover_krw"]) / 1e12  # 조원
    d["close"] = _safe_num(d["close"])

    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Close line (left axis)
    ax1.plot(d["date"], d["close"])
    ax1.set_ylabel(f"{market} Index (Close)")
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax1.xaxis.get_major_locator()))

    # Turnover bars (right axis)
    ax2 = ax1.twinx()
    ax2.bar(d["date"], d["turnover_trn"], width=1.0)
    ax2.set_ylabel("Turnover (KRW, Trillion)")

    title = f"{market}: Close (Line) vs Turnover (Bar)"
    if window_days is not None:
        title += f" — last {window_days}d"
    ax1.set_title(title)

    fig.tight_layout()
    return fig


def plot_investor_net_and_ratio(
    d: pd.DataFrame,
    market: str,
    window_days: int | None = 60,   # <- 추천: 기본 60
    z_window: int = 60,             # 통계 창
    use_institution_bars: bool = False,  # 기관 막대는 기본 끄기
):
    """
    Top panel: Net flow (bars) — Individual + Foreign (optionally Institution)
    Bottom panel: Foreign ratio z-score (rolling z-window) with +/-2 overheat shading
    """
    need_any = ["individual_net", "foreign_net", "institution_net"]
    if not any(c in d.columns for c in need_any):
        return None

    d = _apply_window(d, window_days)
    if len(d) == 0:
        return None

    # numeric
    for c in ["turnover_krw", "individual_net", "foreign_net", "institution_net",
              "individual_ratio", "foreign_ratio", "institution_ratio"]:
        if c in d.columns:
            d[c] = _safe_num(d[c])

    # ratio 자동 생성(없으면)
    if "turnover_krw" in d.columns:
        denom = d["turnover_krw"].replace({0: pd.NA})
    else:
        denom = pd.Series([pd.NA] * len(d), index=d.index)

    if "individual_net" in d.columns and "individual_ratio" not in d.columns and "turnover_krw" in d.columns:
        d["individual_ratio"] = d["individual_net"] / denom
    if "foreign_net" in d.columns and "foreign_ratio" not in d.columns and "turnover_krw" in d.columns:
        d["foreign_ratio"] = d["foreign_net"] / denom
    if "institution_net" in d.columns and "institution_ratio" not in d.columns and "turnover_krw" in d.columns:
        d["institution_ratio"] = d["institution_net"] / denom

    # --- figure: 2 panels ---
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 8), sharex=True,
        gridspec_kw={"height_ratios": [2, 1]}
    )

    # --- Top: net bars (KRW bn) ---
    scale = 1e9
    width = 0.9
    alpha = 0.55

    if "individual_net" in d.columns:
        ax1.bar(d["date"], d["individual_net"] / scale, width=width, alpha=alpha, label="Individual net (₩bn)")
    if "foreign_net" in d.columns:
        ax1.bar(d["date"], d["foreign_net"] / scale, width=width, alpha=alpha, label="Foreign net (₩bn)")
    if use_institution_bars and "institution_net" in d.columns:
        ax1.bar(d["date"], d["institution_net"] / scale, width=width, alpha=alpha, label="Institution net (₩bn)")

    ax1.axhline(0, linewidth=1)
    ax1.set_ylabel("Net flow (KRW, Billion)")

    # --- Bottom: foreign ratio z-score with +/-2 shading ---
    if "foreign_ratio" in d.columns:
        fr = d["foreign_ratio"].copy()
        # rolling stats (z_window)
        mu = fr.rolling(z_window, min_periods=max(20, z_window // 2)).mean()
        sd = fr.rolling(z_window, min_periods=max(20, z_window // 2)).std()
        z = (fr - mu) / sd

        ax2.plot(d["date"], z, linewidth=2, label=f"Foreign ratio z-score ({z_window}D)")
        ax2.axhline(0, linewidth=1)
        ax2.axhline(2, linestyle="--")
        ax2.axhline(-2, linestyle="--")

        # 양방향 과열 음영: z >= 2, z <= -2
        ax2.fill_between(d["date"], 2, z, where=(z >= 2), alpha=0.15, interpolate=True, label="Overheat inflow (z≥2)")
        ax2.fill_between(d["date"], -2, z, where=(z <= -2), alpha=0.15, interpolate=True, label="Overheat outflow (z≤-2)")

        ax2.set_ylabel("Foreign net/turnover (z)")
    else:
        # foreign_ratio가 없으면 아래패널 생략 느낌으로 안내
        ax2.text(0.5, 0.5, "foreign_ratio not available", transform=ax2.transAxes,
                 ha="center", va="center")
        ax2.set_axis_off()

    # x-axis formatting
    ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax2.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax2.xaxis.get_major_locator()))

    # title
    title = f"{market}: Investor Net (bars) + Foreign Overheat (z-score)"
    if window_days is not None:
        title += f" — last {window_days}d"
    ax1.set_title(title)

    # legends: keep separate so it doesn't explode
    h1, l1 = ax1.get_legend_handles_labels()
    if h1:
        ax1.legend(h1, l1, loc="upper left")

    h2, l2 = ax2.get_legend_handles_labels()
    if h2:
        ax2.legend(h2, l2, loc="upper left")

    fig.tight_layout()
    return fig


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not IN_CSV.exists():
        raise FileNotFoundError(f"Missing {IN_CSV}")

    df = pd.read_csv(IN_CSV)

    for c in ["date", "market", "turnover_krw", "close"]:
        if c not in df.columns:
            raise KeyError(f"Need column '{c}' in {IN_CSV}. got={list(df.columns)}")

    for market in ["KOSPI", "KOSDAQ"]:
        d = _prep_market(df, market)

        # 1) Close vs Turnover
        fig_1y = plot_close_vs_turnover(d, market, window_days=365)
        if fig_1y is not None:
            p = OUT_DIR / f"{market.lower()}_close_vs_turnover_1y.png"
            fig_1y.savefig(p, dpi=160)
            plt.close(fig_1y)

        fig_all = plot_close_vs_turnover(d, market, window_days=None)
        if fig_all is not None:
            p = OUT_DIR / f"{market.lower()}_close_vs_turnover_all.png"
            fig_all.savefig(p, dpi=160)
            plt.close(fig_all)

        # 2) Investor net + ratio (if available)
        fig_inv_1y = plot_investor_net_and_ratio(d, market, window_days=365)
        if fig_inv_1y is not None:
            p = OUT_DIR / f"{market.lower()}_investor_net_ratio_1y.png"
            fig_inv_1y.savefig(p, dpi=160)
            plt.close(fig_inv_1y)

        fig_inv_all = plot_investor_net_and_ratio(d, market, window_days=None)
        if fig_inv_all is not None:
            p = OUT_DIR / f"{market.lower()}_investor_net_ratio_all.png"
            fig_inv_all.savefig(p, dpi=160)
            plt.close(fig_inv_all)

    print("Input:", IN_CSV)
    print("Saved charts into:", OUT_DIR)


if __name__ == "__main__":
    main()
