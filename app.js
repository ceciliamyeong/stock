let dashboardData = null;
let currentMarket = "KOSPI";

// 🔥 GitHub Pages 하위경로 대응용 base path
const BASE = location.pathname.split('/')[1]
  ? `/${location.pathname.split('/')[1]}`
  : '';

fetch(`${BASE}/data/derived/dashboard/latest.json`)
  .then(res => {
    if (!res.ok) {
      throw new Error("JSON load failed: " + res.status);
    }
    return res.json();
  })
  .then(data => {
    dashboardData = data;
    document.getElementById("date").innerText =
      data.date + " 장마감";
    render();
  })
  .catch(err => {
    console.error(err);
    document.getElementById("date").innerText =
      "데이터 로딩 실패";
  });

function setMarket(market) {
  currentMarket = market;
  render();
}

function formatKRW(x) {
  if (!x) return "-";
  const abs = Math.abs(x);
  if (abs >= 1e12) return (x / 1e12).toFixed(2) + "조";
  if (abs >= 1e8) return (x / 1e8).toFixed(0) + "억";
  return x;
}

function render() {
  const m = dashboardData.markets[currentMarket];

  document.getElementById("market-name").innerText = currentMarket;
  document.getElementById("close").innerText = m.close;
  document.getElementById("turnover").innerText =
    "거래대금: " + m.turnover_readable;

  const setFlow = (id, val, label) => {
    const el = document.getElementById(id);
    if (!val) {
      el.innerText = label + ": -";
      return;
    }
    const cls = val > 0 ? "positive" : "negative";
    el.className = cls;
    el.innerText = label + ": " + formatKRW(val);
  };


    const up = dashboardData.extras?.upjong;
    if (up) {
      const topText = (up.top || [])
        .map(x => `${x.name} ${(x.return_pct > 0 ? "+" : "")}${x.return_pct.toFixed(2)}%`)
        .join(" / ");
      const botText = (up.bottom || [])
        .map(x => `${x.name} ${(x.return_pct > 0 ? "+" : "")}${x.return_pct.toFixed(2)}%`)
        .join(" / ");
    
      document.getElementById("upjong-top").innerText = "업종 상위: " + (topText || "-");
      document.getElementById("upjong-bottom").innerText = "업종 하위: " + (botText || "-");
    }

    

  setFlow("foreign", m.investor_net_krw.foreign, "외국인");
  setFlow("institution", m.investor_net_krw.institution, "기관");
  setFlow("individual", m.investor_net_krw.individual, "개인");

  document.getElementById("treemap").src =
  `${BASE}/data/derived/charts/treemap_${currentMarket.toLowerCase()}_top10_latest.png`;
}
