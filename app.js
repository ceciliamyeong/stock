// Simple dashboard renderer (no framework)

let STATE = {
  market: "KOSPI",
  range: "1y", // 1y | all
  data: null,
};

function $(id) { return document.getElementById(id); }

function setTheme(theme) {
  const html = document.documentElement;
  html.setAttribute("data-theme", theme);
  localStorage.setItem("theme", theme);

  const isLight = theme === "light";
  $("theme-icon").textContent = isLight ? "☀️" : "🌙";
  $("theme-text").textContent = isLight ? "Light" : "Dark";
}

function toggleTheme() {
  const cur = document.documentElement.getAttribute("data-theme") || "dark";
  setTheme(cur === "dark" ? "light" : "dark");
}

function setMarket(market) {
  STATE.market = market;

  $("tab-kospi").setAttribute("aria-pressed", market === "KOSPI" ? "true" : "false");
  $("tab-kosdaq").setAttribute("aria-pressed", market === "KOSDAQ" ? "true" : "false");

  render();
}

function setRange(range) {
  STATE.range = range;

  $("range-1y").setAttribute("aria-pressed", range === "1y" ? "true" : "false");
  $("range-all").setAttribute("aria-pressed", range === "all" ? "true" : "false");

  renderCharts();
}

function fmtPct(x) {
  if (x === null || x === undefined || Number.isNaN(Number(x))) return "—";
  const v = Number(x);
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(2)}%`;
}

function fmtNum(x) {
  if (x === null || x === undefined || Number.isNaN(Number(x))) return "—";
  return Number(x).toLocaleString("ko-KR");
}

function fmtKrwReadable(krw) {
  if (krw === null || krw === undefined || Number.isNaN(Number(krw))) return "—";
  const v = Number(krw);
  const a = Math.abs(v);
  const sign = v > 0 ? "+" : v < 0 ? "-" : "";
  const n = Math.abs(v);

  if (a >= 1e12) return `${sign}${(n / 1e12).toFixed(2)}조`;
  if (a >= 1e8) return `${sign}${Math.round(n / 1e8).toLocaleString("ko-KR")}억`;
  return `${sign}${Math.round(n).toLocaleString("ko-KR")}`;
}

function fmtMcap(krw) {
  if (krw === null || krw === undefined || Number.isNaN(Number(krw))) return "—";
  const v = Number(krw);
  const a = Math.abs(v);
  if (a >= 1e12) return `${(v / 1e12).toFixed(2)}조`;
  if (a >= 1e8) return `${(v / 1e8).toFixed(0)}억`;
  return fmtNum(v);
}

function clsBySign(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "neu";
  if (Number(v) > 0) return "pos";
  if (Number(v) < 0) return "neg";
  return "neu";
}

function setImgWithFallback(imgEl, src, noteEl, noteWhenMissing) {
  // bust cache (GitHub Pages can be aggressive)
  const withBuster = `${src}?v=${Date.now()}`;

  imgEl.onerror = () => {
    imgEl.style.display = "none";
    if (noteEl) noteEl.textContent = noteWhenMissing;
  };

  imgEl.onload = () => {
    imgEl.style.display = "block";
    if (noteEl) noteEl.textContent = "";
  };

  imgEl.src = withBuster;
}

async function loadData() {
  const res = await fetch("./data/derived/dashboard/latest.json?v=" + Date.now());
  if (!res.ok) throw new Error("failed to load latest.json");
  STATE.data = await res.json();
}

function renderHeader() {
  const d = STATE.data?.date || "";
  $("title-date").textContent = `장마감 스냅샷 · ${d}`;
  $("subtitle").textContent = `마켓: ${STATE.market} · range: ${STATE.range.toUpperCase()}`;
}

function renderSnapshot() {
  const mk = STATE.market;
  const m = STATE.data?.markets?.[mk];
  if (!m) return;

  $("market-name").textContent = mk;

  $("close").textContent = m.close !== null && m.close !== undefined ? fmtNum(m.close) : "—";
  $("turnover").textContent = `거래대금: ${m.turnover_readable || fmtKrwReadable(m.turnover_krw)}`;

  $("foreign").textContent = m.investor_net_readable?.foreign ?? fmtKrwReadable(m.investor_net_krw?.foreign);
  $("institution").textContent = m.investor_net_readable?.institution ?? fmtKrwReadable(m.investor_net_krw?.institution);
  $("individual").textContent = m.investor_net_readable?.individual ?? fmtKrwReadable(m.investor_net_krw?.individual);

  $("sig-foreign").textContent = m.flow_signal?.foreign ?? "—";
  $("sig-institution").textContent = m.flow_signal?.institution ?? "—";
  $("sig-individual").textContent = m.flow_signal?.individual ?? "—";
}

function renderIntraday() {
  // ✅ 파일명 규칙(추후 생성하도록): data/derived/charts/kospi_intraday_latest.png
  const mk = STATE.market.toLowerCase();
  const img = $("intraday-img");
  const note = $("intraday-note");

  const src = `./data/derived/charts/${mk}_intraday_latest.png`;
  setImgWithFallback(
    img,
    src,
    note,
    "intraday 차트 이미지가 아직 없습니다. (예: data/derived/charts/kospi_intraday_latest.png 생성 시 자동 표시)"
  );
}

function renderTreemap() {
  const mk = STATE.market;
  $("treemap-title").textContent = `시총 TOP10 등락 (Treemap) · ${mk}`;

  const mkLower = mk.toLowerCase();
  const img = $("treemap-img");
  const src = `./data/derived/charts/treemap_${mkLower}_top10_latest.png`;

  setImgWithFallback(
    img,
    src,
    null,
    ""
  );
}

function renderTop10List() {
  const mk = STATE.market;
  const list = $("top10-list");
  list.innerHTML = "";

  const rows = STATE.data?.extras?.top10_treemap?.[mk] || [];
  if (!rows.length) {
    list.innerHTML = `<div class="muted">TOP10 데이터가 없습니다.</div>`;
    return;
  }

  rows.forEach((r) => {
    const ret = Number(r.return_1d);
    const cls = clsBySign(ret);

    const el = document.createElement("div");
    el.className = "listItem";
    el.innerHTML = `
      <div class="liLeft">
        <div class="liName">${r.name ?? ""}</div>
        <div class="liMeta">시총 ${fmtMcap(r.mcap)} · 종가 ${fmtNum(r.close)}</div>
      </div>
      <div class="badge ${cls}">${fmtPct(ret)}</div>
    `;
    list.appendChild(el);
  });
}

function renderUpjong() {
  const topBox = $("upjong-top");
  const botBox = $("upjong-bottom");
  const note = $("upjong-note");

  topBox.innerHTML = "";
  botBox.innerHTML = "";
  note.textContent = "";

  const up = STATE.data?.extras?.upjong;
  if (!up || (!up.top?.length && !up.bottom?.length)) {
    const err = STATE.data?.extras?.upjong_error;
    note.textContent = err ? `업종 파싱 오류: ${err}` : "업종 데이터가 없습니다.";
    return;
  }

  (up.top || []).forEach((r) => {
    const el = document.createElement("div");
    el.className = "listItem";
    el.innerHTML = `
      <div class="liLeft">
        <div class="liName">${r.name}</div>
      </div>
      <div class="badge pos">${fmtPct(r.return_pct)}</div>
    `;
    topBox.appendChild(el);
  });

  (up.bottom || []).forEach((r) => {
    const el = document.createElement("div");
    el.className = "listItem";
    el.innerHTML = `
      <div class="liLeft">
        <div class="liName">${r.name}</div>
      </div>
      <div class="badge neg">${fmtPct(r.return_pct)}</div>
    `;
    botBox.appendChild(el);
  });
}

function renderCharts() {
  const mk = STATE.market.toLowerCase();
  const range = STATE.range;

  const turnoverImg = $("chart-turnover");
  const investorImg = $("chart-investor");

  const turnoverNote = $("chart-turnover-note");
  const investorNote = $("chart-investor-note");

  const turnoverSrc = `./data/derived/charts/${mk}_close_vs_turnover_${range}.png`;
  const investorSrc = `./data/derived/charts/${mk}_investor_net_ratio_${range}.png`;

  setImgWithFallback(
    turnoverImg,
    turnoverSrc,
    turnoverNote,
    `차트가 없습니다: data/derived/charts/${mk}_close_vs_turnover_${range}.png`
  );

  setImgWithFallback(
    investorImg,
    investorSrc,
    investorNote,
    `차트가 없습니다: data/derived/charts/${mk}_investor_net_ratio_${range}.png`
  );
}

function render() {
  if (!STATE.data) return;

  renderHeader();
  renderSnapshot();
  renderIntraday();
  renderTreemap();
  renderTop10List();
  renderUpjong();
  renderCharts();
}

async function boot() {
  // theme
  const savedTheme = localStorage.getItem("theme");
  setTheme(savedTheme || "dark");

  $("theme-toggle").addEventListener("click", toggleTheme);

  $("tab-kospi").addEventListener("click", () => setMarket("KOSPI"));
  $("tab-kosdaq").addEventListener("click", () => setMarket("KOSDAQ"));

  $("range-1y").addEventListener("click", () => setRange("1y"));
  $("range-all").addEventListener("click", () => setRange("all"));

  await loadData();
  render();
}

window.addEventListener("DOMContentLoaded", () => {
  boot().catch((e) => {
    console.error(e);
    const sub = document.getElementById("subtitle");
    if (sub) sub.textContent = "데이터 로딩 실패: " + String(e);
  });
});
