const SPOT_HOSTS = [
  { kind: "spot", api: "/api/v3", rest: "https://data-api.binance.vision", ws: "wss://data-stream.binance.vision" },
  { kind: "spot", api: "/api/v3", rest: "https://api.binance.com", ws: "wss://stream.binance.com:9443" },
  { kind: "spot", api: "/api/v3", rest: "https://api.binance.me", ws: "wss://stream.binance.me:9443" },
];

const FUTURES_HOSTS = [
  { kind: "futures", api: "/fapi/v1", rest: "https://fapi.binance.com", ws: "wss://fstream.binance.com" },
  { kind: "futures", api: "/fapi/v1", rest: "https://fapi1.binance.com", ws: "wss://fstream.binance.com" },
  { kind: "futures", api: "/fapi/v1", rest: "https://fapi2.binance.com", ws: "wss://fstream.binance.com" },
];
const FUTURES_SYMBOLS = new Set(["xauusdt", "xagusdt"]);
const hostsFor = (symbol) => (FUTURES_SYMBOLS.has(String(symbol).toLowerCase()) ? FUTURES_HOSTS : SPOT_HOSTS);

const el = (id) => document.getElementById(id);
const dom = {
  symbolSelect: el("symbolSelect"),
  customSymbol: el("customSymbol"),
  loadButton: el("loadSymbol"),
  intervalSelect: el("intervalSelect"),
  status: el("connectionStatus"),
  statLastPrice: el("statLastPrice"),
  statSpread: el("statSpread"),
  statVolume: el("statVolume"),
  statBuyPressure: el("statBuyPressure"),
  statLatency: el("statLatency"),
  orderBookList: el("orderBookList"),
  tapeList: el("tapeList"),
  volumeList: el("volumeList"),
  bookmapCanvas: el("bookmapCanvas"),
  candleTimer: el("candleTimer"),
  scaleToggle: el("scaleToggle"),
  microToggle: el("microToggle"),
  cutSlider: el("cutSlider"),
  cutValue: el("cutValue"),
  legendMin: el("legendMin"),
  legendMax: el("legendMax"),
  legendBar: el("legendBar"),
  mt5Price: el("mt5Price"),
  calibrate: el("calibrate"),
  resetOffset: el("resetOffset"),
  calibInfo: el("calibInfo"),
  brokerDigits: el("brokerDigits"),
};

function formatNumber(value, decimals) {
  return Number(value).toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function decimalPlacesForPrice(price) {
  const p = +price;
  if (p >= 1000) return 2;
  if (p >= 1) return 3;
  if (p >= 0.1) return 4;
  return 6;
}

let currentSymbol = "xauusdt";
let currentInterval = "1m";
let activeHost = hostsFor(currentSymbol)[0];
let sockets = [];
let reconnectTimer = null;

let candles = [];
let orderBookBids = [];
let orderBookAsks = [];
let tradeTape = [];
let tradeHistory = [];
let tapeFlow = { buy: 0, sell: 0 };
let heatmapColumns = [];
let priceDecimals = 2;

let priceOffset = 0;
let offsetSetAt = 0;
const adj = (price) => +price + priceOffset;
let brokerDigits = 3;
try { brokerDigits = +localStorage.getItem("brokerDigits") === 2 ? 2 : 3; } catch (e) {}
dom.brokerDigits.value = String(brokerDigits);
dom.mt5Price.step = String(10 ** -brokerDigits);
const shownDecimals = () => (offsetSetAt ? brokerDigits : priceDecimals);
const fmtAdj = (price) => formatNumber(adj(price), shownDecimals());
const rawLastPrice = () => (tradeTape[0] ? tradeTape[0].price : candles.length ? candles.at(-1).close : 0);

const MAX_HEATMAP_COLUMNS = 900;
const HEATMAP_ROWS = 80;

const dirty = { book: false, tape: false, chart: false };

async function fetchKlinesWithFallback(symbol, interval) {
  let lastError;
  for (const host of hostsFor(symbol)) {
    try {
      const response = await fetch(
        `${host.rest}${host.api}/klines?symbol=${symbol.toUpperCase()}&interval=${interval}&limit=80`
      );
      const payload = await response.json();
      if (!Array.isArray(payload)) throw new Error("simbol tidak valid");
      return { payload, host };
    } catch (error) {
      lastError = error;
      if (error.message === "simbol tidak valid") break;
    }
  }
  const message =
    lastError && lastError.message === "Failed to fetch"
      ? "semua server market data tidak terjangkau (kemungkinan diblokir jaringan)"
      : lastError?.message ?? "gagal memuat data";
  throw new Error(message);
}

async function connectMarket(isReconnect = false) {
  clearTimeout(reconnectTimer);
  sockets.forEach((ws) => { ws.onclose = null; ws.close(); });
  sockets = [];
  orderBookBids = [];
  orderBookAsks = [];
  if (!isReconnect) {
    candles = [];
    tradeTape = [];
    tradeHistory = [];
    tapeFlow = { buy: 0, sell: 0 };
    heatmapColumns = [];
    deepBook = null;
    dirty.tape = true;
  }

  dom.status.className = "status status-off";
  dom.status.textContent = "menghubungkan";

  try {
    const { payload, host } = await fetchKlinesWithFallback(currentSymbol, currentInterval);
    activeHost = host;
    pollDeep();
    syncClock();
    candles = payload.map((k) => ({
      time: k[0], open: +k[1], high: +k[2], low: +k[3], close: +k[4], volume: +k[5],
    }));
    priceDecimals = decimalPlacesForPrice(candles.at(-1).close);
  } catch (error) {
    dom.status.textContent = "gagal: " + error.message;
    return;
  }

  const sym = currentSymbol;
  let urls;
  if (activeHost.kind === "futures") {

    urls = [
      `${activeHost.ws}/public/stream?streams=${sym}@depth20@100ms`,
      `${activeHost.ws}/market/stream?streams=${sym}@aggTrade/${sym}@kline_${currentInterval}`,
    ];
  } else {
    urls = [`${activeHost.ws}/stream?streams=${sym}@depth20@100ms/${sym}@trade/${sym}@kline_${currentInterval}`];
  }

  let openCount = 0;
  sockets = urls.map((url) => {
    const ws = new WebSocket(url);
    ws.onopen = () => {
      openCount++;
      if (openCount === urls.length) {
        dom.status.className = "status status-on";
        dom.status.textContent = "langsung";
      }
    };
    ws.onclose = () => {
      dom.status.className = "status status-off";
      dom.status.textContent = "terputus, mencoba lagi";
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(() => connectMarket(true), 3000);
    };
    ws.onmessage = handleSocketMessage;
    return ws;
  });
}

function handleSocketMessage(event) {
  const message = JSON.parse(event.data);
  const payload = message.data;
  if (message.stream.includes("@depth")) {
    orderBookBids = payload.bids ?? payload.b ?? [];
    orderBookAsks = payload.asks ?? payload.a ?? [];
    dirty.book = true;
  } else if (message.stream.includes("@trade") || message.stream.includes("@aggTrade")) {
    const quantity = +payload.q;
    const isSell = payload.m;
    tradeTape.unshift({ time: payload.T, price: +payload.p, quantity, isSell });
    tradeHistory.push({ time: payload.T, price: +payload.p, quantity, isSell });
    if (tradeHistory.length > 60000) tradeHistory.splice(0, 6000);
    if (tradeTape.length > 60) tradeTape.pop();
    isSell ? (tapeFlow.sell += quantity) : (tapeFlow.buy += quantity);
    dom.statLatency.textContent = Math.max(0, nowMs() - payload.E) + " ms";
    dirty.tape = true;
  } else {
    const k = payload.k;
    const updated = { time: k.t, open: +k.o, high: +k.h, low: +k.l, close: +k.c, volume: +k.v };
    const last = candles.at(-1);
    if (last && last.time === updated.time) {
      candles[candles.length - 1] = updated;
    } else {
      candles.push(updated);
      if (candles.length > 80) candles.shift();
    }
    dirty.chart = true;
  }
}

let serverOffset = 0;
const nowMs = () => Date.now() + serverOffset;
async function syncClock() {
  try {
    const t0 = Date.now();
    const response = await fetch(`${activeHost.rest}${activeHost.api}/time`);
    const data = await response.json();
    const t1 = Date.now();
    if (data.serverTime) serverOffset = data.serverTime + (t1 - t0) / 2 - t1;
  } catch (error) {}
}
setInterval(syncClock, 60000);

let deepBook = null;
const flatBook = (rows) => {
  const out = new Float64Array(rows.length * 2);
  rows.forEach(([p, q], i) => { out[2 * i] = +p; out[2 * i + 1] = +q; });
  return out;
};
async function pollDeep() {
  const symbol = currentSymbol;
  try {
    const response = await fetch(`${activeHost.rest}${activeHost.api}/depth?symbol=${symbol.toUpperCase()}&limit=500`);
    const data = await response.json();
    if (symbol === currentSymbol && data.bids && data.asks) deepBook = { bids: data.bids, asks: data.asks };
  } catch (error) {}
}
setInterval(pollDeep, 2000);
setInterval(() => {
  const src = deepBook || { bids: orderBookBids, asks: orderBookAsks };
  if (!src.bids.length || !src.asks.length) return;
  heatmapColumns.push({
    time: nowMs(),
    mid: (+src.bids[0][0] + +src.asks[0][0]) / 2,
    bids: flatBook(src.bids),
    asks: flatBook(src.asks),
  });
  if (heatmapColumns.length > MAX_HEATMAP_COLUMNS) heatmapColumns.shift();
  dirty.chart = true;
}, 2000);

function renderOrderBook() {
  const asksTop = orderBookAsks.slice(0, 10).reverse();
  const bidsTop = orderBookBids.slice(0, 10);
  const maxQty = Math.max(
    ...orderBookAsks.slice(0, 10).map((x) => +x[1]),
    ...bidsTop.map((x) => +x[1]),
    1e-9
  );
  const row = (entry, side) =>
    `<div class="book-row ${side} mono">` +
    `<span class="depth-bar" style="width:${((+entry[1] / maxQty) * 100).toFixed(1)}%"></span>` +
    `<span>${fmtAdj(entry[0])}</span>` +
    `<span>${formatNumber(entry[1], 4)}</span></div>`;

  const bestBid = +(bidsTop[0] || [0])[0];
  const bestAsk = +(orderBookAsks[0] || [0])[0];
  dom.orderBookList.innerHTML =
    asksTop.map((e) => row(e, "ask")).join("") +
    `<div class="book-mid mono"><b>${candles.length ? fmtAdj(candles.at(-1).close) : "0"}</b></div>` +
    bidsTop.map((e) => row(e, "bid")).join("");
  dom.statSpread.textContent = bestAsk && bestBid ? formatNumber(bestAsk - bestBid, priceDecimals) : "0";
}

function renderTape() {
  dom.tapeList.innerHTML = tradeTape
    .slice(0, 22)
    .map((t) => {
      const time = new Date(t.time).toLocaleTimeString("id-ID", { hour12: false });
      return `<div class="tape-row mono ${t.isSell ? "sell" : "buy"}">` +
        `<span>${time}</span><span>${fmtAdj(t.price)}</span>` +
        `<span>${formatNumber(t.quantity, 4)}</span></div>`;
    })
    .join("");
  const total = tapeFlow.buy + tapeFlow.sell;
  dom.statBuyPressure.textContent = total ? Math.round((tapeFlow.buy / total) * 100) + "%" : "0%";
  if (tradeTape[0]) dom.statLastPrice.textContent = fmtAdj(tradeTape[0].price);
}

function renderVolumeList() {
  dom.volumeList.innerHTML = candles
    .slice(-14)
    .reverse()
    .map((c) => {
      const up = c.close >= c.open;
      const time = new Date(c.time).toLocaleTimeString("id-ID", { hour12: false, hour: "2-digit", minute: "2-digit" });
      return `<div class="volume-row mono ${up ? "buy" : "sell"}">` +
        `<span>${time}</span><span>${fmtAdj(c.close)}</span>` +
        `<span>${formatNumber(c.volume, 2)}</span></div>`;
    })
    .join("");
  if (candles.length) dom.statVolume.textContent = formatNumber(candles.at(-1).volume, 2);
}

const HEAT_STOPS = [
  [0.0, [10, 14, 25]],
  [0.18, [24, 44, 95]],
  [0.4, [10, 120, 180]],
  [0.6, [30, 190, 150]],
  [0.78, [225, 195, 45]],
  [1.0, [225, 70, 50]],
];
const heatAlpha = (t) => Math.min(1, 0.15 + 1.2 * t);
function heatColor(t) {
  for (let i = 0; i < HEAT_STOPS.length - 1; i++) {
    const [t0, c0] = HEAT_STOPS[i];
    const [t1, c1] = HEAT_STOPS[i + 1];
    if (t >= t0 && t <= t1) {
      const f = (t - t0) / (t1 - t0 || 1);
      const r = Math.round(c0[0] + (c1[0] - c0[0]) * f);
      const g = Math.round(c0[1] + (c1[1] - c0[1]) * f);
      const b = Math.round(c0[2] + (c1[2] - c0[2]) * f);
      return `rgb(${r},${g},${b})`;
    }
  }
  const last = HEAT_STOPS.at(-1)[1];
  return `rgb(${last.join(",")})`;
}

function candleRemaining() {
  if (candles.length < 2) return null;
  const interval = candles[1].time - candles[0].time;
  return Math.max(0, candles.at(-1).time + interval - nowMs());
}
function fmtClock(ms) {
  const t = Math.ceil(ms / 1000);
  const h = Math.floor(t / 3600);
  const m = Math.floor((t % 3600) / 60);
  return (h ? h + ":" : "") + String(m).padStart(2, "0") + ":" + String(t % 60).padStart(2, "0");
}

const layers = { bubbles: true, cob: true, svp: true, cvd: true };
try { Object.assign(layers, JSON.parse(localStorage.getItem("layers") || "{}")); } catch (e) {}
document.querySelectorAll("#layerChips button").forEach((btn) => {
  const key = btn.dataset.layer;
  btn.classList.toggle("on", !!layers[key]);
  btn.onclick = () => {
    layers[key] = !layers[key];
    btn.classList.toggle("on", layers[key]);
    try { localStorage.setItem("layers", JSON.stringify(layers)); } catch (e) {}
    dirty.chart = true;
  };
});

const heatPrefs = { linear: false, cut: 0, micro: false };
try { Object.assign(heatPrefs, JSON.parse(localStorage.getItem("heatPrefs") || "{}")); } catch (e) {}
const saveHeatPrefs = () => { try { localStorage.setItem("heatPrefs", JSON.stringify(heatPrefs)); } catch (e) {} };
function syncHeatControls() {
  dom.scaleToggle.textContent = "Skala: " + (heatPrefs.linear ? "Linear" : "Log");
  dom.microToggle.textContent = "Candle: " + (heatPrefs.micro ? "Mikro" : "Asli");
  dom.cutSlider.value = String(heatPrefs.cut);
  dom.cutValue.textContent = heatPrefs.cut + "%";
}
dom.scaleToggle.onclick = () => { heatPrefs.linear = !heatPrefs.linear; syncHeatControls(); saveHeatPrefs(); dirty.chart = true; };
dom.microToggle.onclick = () => { heatPrefs.micro = !heatPrefs.micro; syncHeatControls(); saveHeatPrefs(); dirty.chart = true; };
dom.cutSlider.oninput = () => { heatPrefs.cut = +dom.cutSlider.value; syncHeatControls(); saveHeatPrefs(); dirty.chart = true; };
syncHeatControls();
dom.legendBar.style.background = `linear-gradient(90deg, ${HEAT_STOPS.map(([t, c]) => `rgba(${c.join(",")},${heatAlpha(t).toFixed(2)}) ${t * 100}%`).join(", ")})`;

let hover = null;
const canvasPoint = (e) => { const r = dom.bookmapCanvas.getBoundingClientRect(); return { x: e.clientX - r.left, y: e.clientY - r.top }; };
dom.bookmapCanvas.addEventListener("pointermove", (e) => { if (e.pointerType !== "mouse") return; hover = canvasPoint(e); dirty.chart = true; });
dom.bookmapCanvas.addEventListener("pointerleave", (e) => { if (e.pointerType === "mouse") { hover = null; dirty.chart = true; } });
dom.bookmapCanvas.addEventListener("click", (e) => {
  const p = canvasPoint(e);
  hover = hover && Math.hypot(hover.x - p.x, hover.y - p.y) < 14 ? null : p;
  dirty.chart = true;
});

const clockLabel = (t) => new Date(t).toLocaleTimeString("id-ID", { hour12: false, hour: "2-digit", minute: "2-digit" });
const compact = (v) => {
  const a = Math.abs(v);
  if (a >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (a >= 1e3) return (v / 1e3).toFixed(1) + "k";
  return a >= 100 ? v.toFixed(0) : v.toFixed(1);
};

function renderBookmap() {
  const canvas = dom.bookmapCanvas;
  const ratio = devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  const style = getComputedStyle(document.documentElement);
  const bidColor = style.getPropertyValue("--bid").trim();
  const askColor = style.getPropertyValue("--ask").trim();
  const mutedColor = style.getPropertyValue("--muted").trim();
  ctx.fillStyle = "#0a0e16";
  ctx.fillRect(0, 0, width, height);
  ctx.font = "11px ui-monospace, monospace";
  if (!candles.length) {
    ctx.fillStyle = mutedColor;
    ctx.fillText("memuat data...", 10, 20);
    return;
  }

  const axisW = 62;
  const sideW = Math.max(34, Math.min(64, width * 0.1));
  const cobW = layers.cob ? sideW : 0;
  const svpW = layers.svp ? sideW : 0;
  const plotW = width - axisW - cobW - svpW;
  const cobX = plotW + axisW;
  const svpX = cobX + cobW;
  const timeH = 14;
  const volH = Math.round(height * 0.09);
  const cvdH = layers.cvd ? Math.round(height * 0.17) : 0;
  const chartH = height - volH - timeH - cvdH - (layers.cvd ? 4 : 0);
  const volTop = chartH;
  const timeTop = chartH + volH;
  const cvdTop = timeTop + timeH + 4;

  const interval = candles.length > 1 ? candles[1].time - candles[0].time : 60000;
  const cols = heatmapColumns;
  const now = nowMs();
  const histSpan = cols.length ? now - cols[0].time : 0;

  const windowSpan = Math.min(30 * 60000, Math.max(3 * 60000, histSpan * 1.04));
  const windowEnd = now + windowSpan * 0.03;
  const windowStart = windowEnd - windowSpan;
  const xOf = (t) => ((t - windowStart) / windowSpan) * plotW;

  const visCols = cols.filter((c) => c.time >= windowStart);
  let startIdx = tradeHistory.length;
  while (startIdx > 0 && tradeHistory[startIdx - 1].time >= windowStart) startIdx--;
  const winTrades = tradeHistory.slice(startIdx);

  const adjustCandle = (c) => ({ ...c, open: adj(c.open), high: adj(c.high), low: adj(c.low), close: adj(c.close) });
  const micro = heatPrefs.micro && windowSpan < interval * 10 && winTrades.length > 1;
  const stepOptions = [1000, 2000, 5000, 10000, 15000, 30000, 60000];
  const step = micro ? stepOptions.find((o) => o >= windowSpan / 40) || 60000 : interval;
  let vis;
  if (micro) {
    const buckets = new Map();
    for (const t of winTrades) {
      const key = Math.floor(t.time / step) * step;
      const m = buckets.get(key);
      if (!m) buckets.set(key, { time: key, open: t.price, high: t.price, low: t.price, close: t.price, volume: t.quantity });
      else {
        m.high = Math.max(m.high, t.price);
        m.low = Math.min(m.low, t.price);
        m.close = t.price;
        m.volume += t.quantity;
      }
    }
    vis = [...buckets.values()].map(adjustCandle);
  } else {
    vis = candles.filter((c) => c.time + interval > windowStart).map(adjustCandle);
  }

  let lo = Infinity;
  let hi = -Infinity;
  const upd = (p) => { if (p < lo) lo = p; if (p > hi) hi = p; };
  if (step <= windowSpan * 1.5) vis.forEach((c) => { upd(c.low); upd(c.high); });
  winTrades.forEach((t) => upd(adj(t.price)));
  visCols.forEach((c) => upd(adj(c.mid)));
  if (!isFinite(lo)) { const c = vis.at(-1) || { low: adj(candles.at(-1).close), high: adj(candles.at(-1).close) }; upd(c.low); upd(c.high); }
  const center = (hi + lo) / 2;
  const half = Math.max(((hi - lo) / 2) * 1.25, center * 0.0004);
  const pLow = center - half;
  const pHigh = center + half;
  const toY = (p) => chartH - ((p - pLow) / (pHigh - pLow)) * chartH;
  const ROWS = Math.max(40, Math.floor(chartH / 4));
  const rowH = chartH / ROWS;
  const rowOf = (p) => Math.floor(((p - pLow) / (pHigh - pLow)) * ROWS);
  const rowY = (r) => chartH - (r + 0.5) * rowH;

  ctx.save();
  ctx.beginPath();
  ctx.rect(0, 0, plotW, chartH);
  ctx.clip();
  let heatGrid = null;
  let heatMax = 1;
  if (visCols.length) {
    const colW = (2000 / windowSpan) * plotW + 1;
    const grid = visCols.map((col) => {
      const rows = new Float32Array(ROWS);
      for (const book of [col.bids, col.asks]) {
        for (let i = 0; i < book.length; i += 2) {
          const p = adj(book[i]);
          if (p < pLow || p >= pHigh) continue;
          rows[rowOf(p)] += book[i + 1];
        }
      }
      return rows;
    });
    const values = [];
    grid.forEach((rows) => rows.forEach((v) => { if (v > 0) values.push(v); }));
    values.sort((x, y) => x - y);
    const maxQty = values.length ? values[Math.floor(values.length * 0.985)] || values.at(-1) : 1;
    const logMax = Math.log1p(maxQty);
    const cut = heatPrefs.cut / 100;
    heatGrid = grid;
    heatMax = maxQty;
    dom.legendMin.textContent = compact(maxQty * cut);
    dom.legendMax.textContent = compact(maxQty) + "+ | " + (heatPrefs.linear ? "linear" : "log");
    grid.forEach((rows, ci) => {
      const x = xOf(visCols[ci].time);
      rows.forEach((qty, r) => {
        if (qty <= 0 || qty < maxQty * cut) return;
        const t = heatPrefs.linear
          ? Math.min(1, qty / maxQty)
          : Math.pow(Math.min(1, Math.log1p(qty) / logMax), 1.5);
        ctx.globalAlpha = heatAlpha(t);
        ctx.fillStyle = heatColor(t);
        ctx.fillRect(x, chartH - (r + 1) * rowH, colW, rowH + 1);
      });
    });
    ctx.globalAlpha = 1;
  }

  const stepsMin = [1, 2, 5, 10, 15, 30];
  const targetStep = windowSpan / 60000 / 4;
  const stepMs = (stepsMin.find((m) => m >= targetStep) || 30) * 60000;
  const ticks = [];
  for (let t = Math.ceil(windowStart / stepMs) * stepMs; t < windowEnd; t += stepMs) ticks.push(t);
  ctx.save();
  ctx.setLineDash([3, 4]);
  ctx.strokeStyle = "rgba(160,180,200,0.22)";
  ctx.lineWidth = 1;
  ticks.forEach((t) => { const x = Math.round(xOf(t)) + 0.5; ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, chartH); ctx.stroke(); });
  ctx.restore();

  const volMax = Math.max(...vis.map((c) => c.volume), 1e-9);
  const candleXs = [];
  vis.forEach((c) => {
    const candleW = (Math.max(0, Math.min(step, now - c.time)) / windowSpan) * plotW;
    const bodyW = Math.max(2, Math.min(candleW * 0.7, 60));
    const x = xOf(c.time) + candleW / 2;
    const color = c.close >= c.open ? bidColor : askColor;
    candleXs.push({ x, bodyW, color, c });
    ctx.globalAlpha = 0.9;
    ctx.strokeStyle = color;
    ctx.beginPath();
    ctx.moveTo(x, toY(c.high));
    ctx.lineTo(x, toY(c.low));
    ctx.stroke();
    const top = toY(Math.max(c.open, c.close));
    const bottom = toY(Math.min(c.open, c.close));
    ctx.fillStyle = color;
    ctx.globalAlpha = 0.55;
    ctx.fillRect(x - bodyW / 2, top, bodyW, Math.max(1, bottom - top));
    ctx.globalAlpha = 1;
    ctx.strokeRect(x - bodyW / 2 + 0.5, top + 0.5, bodyW - 1, Math.max(1, bottom - top) - 1);
  });

  if (layers.bubbles && winTrades.length) {
    const buckets = new Map();
    for (const t of winTrades) {
      const row = rowOf(adj(t.price));
      if (row < 0 || row >= ROWS) continue;
      const key = Math.floor(t.time / 1000) * 100000 + row * 2 + (t.isSell ? 1 : 0);
      buckets.set(key, (buckets.get(key) || 0) + t.quantity);
    }
    let maxB = 1e-9;
    buckets.forEach((q) => { if (q > maxB) maxB = q; });
    const maxR = Math.min(18, Math.max(9, plotW / 22));
    const bubbles = [];
    buckets.forEach((qty, key) => {
      const isSell = key % 2 === 1;
      const row = Math.floor((key % 100000) / 2);
      const sec = Math.floor(key / 100000);
      bubbles.push({ x: xOf(sec * 1000 + 500), y: rowY(row), r: 2 + (maxR - 2) * Math.sqrt(qty / maxB), isSell });
    });
    bubbles.sort((a, b) => b.r - a.r);
    bubbles.forEach((b) => {
      ctx.beginPath();
      ctx.arc(b.x, b.y, b.r, 0, Math.PI * 2);
      ctx.fillStyle = b.isSell ? "rgba(227,86,79,0.62)" : "rgba(40,199,154,0.62)";
      ctx.fill();
      ctx.lineWidth = 1;
      ctx.strokeStyle = b.isSell ? "rgba(255,150,140,0.9)" : "rgba(130,255,215,0.9)";
      ctx.stroke();
    });
  }
  ctx.restore();

  ctx.fillStyle = "#0d121b";
  ctx.fillRect(plotW, 0, axisW, chartH);
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const price = pLow + ((pHigh - pLow) * i) / 4;
    const y = toY(price);
    ctx.globalAlpha = 0.18;
    ctx.strokeStyle = mutedColor;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(plotW, y);
    ctx.stroke();
    ctx.globalAlpha = 1;
    ctx.fillStyle = mutedColor;
    ctx.fillText(formatNumber(price, shownDecimals()), plotW + 4, Math.min(chartH - 2, Math.max(11, y + 4)));
  }

  if (layers.cob) {
    const src = deepBook || { bids: orderBookBids, asks: orderBookAsks };
    const bidRows = new Float32Array(ROWS);
    const askRows = new Float32Array(ROWS);
    src.bids.forEach(([p, q]) => { const a = adj(p); if (a >= pLow && a < pHigh) bidRows[rowOf(a)] += +q; });
    src.asks.forEach(([p, q]) => { const a = adj(p); if (a >= pLow && a < pHigh) askRows[rowOf(a)] += +q; });
    let m = 1e-9;
    for (let r = 0; r < ROWS; r++) m = Math.max(m, bidRows[r], askRows[r]);
    ctx.fillStyle = "#0d121b";
    ctx.fillRect(cobX, 0, cobW, chartH);
    for (let r = 0; r < ROWS; r++) {
      const y = chartH - (r + 1) * rowH;
      if (bidRows[r] > 0) { ctx.fillStyle = "rgba(40,199,154,0.75)"; ctx.fillRect(cobX, y, (bidRows[r] / m) * (cobW - 2), rowH); }
      if (askRows[r] > 0) { ctx.fillStyle = "rgba(227,86,79,0.75)"; ctx.fillRect(cobX, y, (askRows[r] / m) * (cobW - 2), rowH); }
    }
    ctx.fillStyle = mutedColor;
    ctx.fillText("COB", cobX + 3, 12);
  }

  if (layers.svp) {
    const buyRows = new Float32Array(ROWS);
    const sellRows = new Float32Array(ROWS);
    winTrades.forEach((t) => {
      const a = adj(t.price);
      if (a < pLow || a >= pHigh) return;
      (t.isSell ? sellRows : buyRows)[rowOf(a)] += t.quantity;
    });
    let m = 1e-9;
    let poc = -1;
    for (let r = 0; r < ROWS; r++) {
      const tot = buyRows[r] + sellRows[r];
      if (tot > m) { m = tot; poc = r; }
    }
    ctx.fillStyle = "#0d121b";
    ctx.fillRect(svpX, 0, svpW, chartH);
    for (let r = 0; r < ROWS; r++) {
      const y = chartH - (r + 1) * rowH;
      const sw = (sellRows[r] / m) * (svpW - 2);
      const bw = (buyRows[r] / m) * (svpW - 2);
      if (sw > 0) { ctx.fillStyle = "rgba(227,86,79,0.8)"; ctx.fillRect(svpX, y, sw, rowH); }
      if (bw > 0) { ctx.fillStyle = "rgba(40,199,154,0.8)"; ctx.fillRect(svpX + sw, y, bw, rowH); }
    }
    if (poc >= 0) {
      ctx.strokeStyle = "rgba(255,210,70,0.9)";
      ctx.strokeRect(svpX + 0.5, chartH - (poc + 1) * rowH, svpW - 1, rowH);
    }
    ctx.fillStyle = mutedColor;
    ctx.fillText("SVP", svpX + 3, 12);
  }

  const last = vis.at(-1);
  const lastY = toY(last.close);
  const priceColor = last.close >= last.open ? bidColor : askColor;
  ctx.save();
  ctx.setLineDash([4, 3]);
  ctx.strokeStyle = priceColor;
  ctx.beginPath();
  ctx.moveTo(0, lastY);
  ctx.lineTo(plotW + axisW, lastY);
  ctx.stroke();
  ctx.restore();
  const remaining = candleRemaining();
  dom.candleTimer.textContent = remaining === null ? "" : "candle baru " + fmtClock(remaining);
  const boxH = remaining === null ? 16 : 30;
  const boxY = Math.min(chartH - boxH, Math.max(0, lastY - 8));
  ctx.fillStyle = priceColor;
  ctx.fillRect(plotW + 1, boxY, axisW - 1, boxH);
  ctx.fillStyle = "#06121f";
  ctx.font = "bold 11px ui-monospace, monospace";
  ctx.fillText(formatNumber(last.close, shownDecimals()), plotW + 4, boxY + 12);
  if (remaining !== null) ctx.fillText(fmtClock(remaining), plotW + 4, boxY + 25);
  ctx.font = "11px ui-monospace, monospace";

  ctx.fillStyle = "#0c1119";
  ctx.fillRect(0, volTop, plotW, volH);
  candleXs.forEach(({ x, bodyW, color, c }) => {
    const barH = (c.volume / volMax) * (volH - 4);
    ctx.globalAlpha = 0.65;
    ctx.fillStyle = color;
    ctx.fillRect(x - bodyW / 2, volTop + volH - barH, bodyW, barH);
    ctx.globalAlpha = 1;
  });

  ctx.fillStyle = mutedColor;
  ticks.forEach((t) => {
    const x = xOf(t);
    const label = clockLabel(t);
    ctx.fillText(label, Math.min(plotW - 30, Math.max(0, x - 15)), timeTop + 11);
  });
  ctx.fillText(`${Math.round(windowSpan / 60000)} mnt | rekam ${fmtClock(histSpan)} | candle ${micro ? step / 1000 + "d" : currentInterval}`, 6, 14);

  if (layers.cvd) {
    ctx.fillStyle = "#0c1119";
    ctx.fillRect(0, cvdTop, plotW + axisW, cvdH);
    const pts = [];
    let acc = 0;
    let lastSec = -1;
    for (const t of winTrades) {
      acc += t.isSell ? -t.quantity : t.quantity;
      const sec = Math.floor(t.time / 1000);
      if (sec === lastSec) pts[pts.length - 1][1] = acc;
      else { pts.push([t.time, acc]); lastSec = sec; }
    }
    ctx.fillStyle = mutedColor;
    ctx.fillText("CVD", 6, cvdTop + 12);
    if (pts.length > 1) {
      let mn = 0;
      let mx = 0;
      pts.forEach(([, v]) => { mn = Math.min(mn, v); mx = Math.max(mx, v); });
      const span = mx - mn || 1;
      const yC = (v) => cvdTop + 4 + (1 - (v - mn) / span) * (cvdH - 8);
      const zeroY = yC(0);
      const up = acc >= 0;
      ctx.strokeStyle = "rgba(160,180,200,0.3)";
      ctx.beginPath(); ctx.moveTo(0, zeroY); ctx.lineTo(plotW, zeroY); ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(xOf(pts[0][0]), zeroY);
      pts.forEach(([t, v]) => ctx.lineTo(xOf(t), yC(v)));
      ctx.lineTo(xOf(pts.at(-1)[0]), zeroY);
      ctx.closePath();
      ctx.fillStyle = up ? "rgba(40,199,154,0.22)" : "rgba(227,86,79,0.22)";
      ctx.fill();
      ctx.beginPath();
      pts.forEach(([t, v], i) => (i ? ctx.lineTo(xOf(t), yC(v)) : ctx.moveTo(xOf(t), yC(v))));
      ctx.strokeStyle = up ? bidColor : askColor;
      ctx.lineWidth = 1.5;
      ctx.stroke();
      ctx.lineWidth = 1;
      ctx.fillStyle = up ? bidColor : askColor;
      ctx.fillText((acc >= 0 ? "+" : "") + compact(acc), plotW + 4, Math.min(cvdTop + cvdH - 4, Math.max(cvdTop + 12, yC(acc) + 4)));
    }
  }

  if (hover && hover.x >= 0 && hover.x < plotW && hover.y >= 0 && hover.y < chartH) {
    const r = Math.min(ROWS - 1, Math.max(0, Math.floor((chartH - hover.y) / rowH)));
    const t = windowStart + (hover.x / plotW) * windowSpan;
    const step = (pHigh - pLow) / ROWS;
    const rowLow = pLow + r * step;
    ctx.save();
    ctx.setLineDash([3, 3]);
    ctx.strokeStyle = "rgba(255,255,255,0.55)";
    ctx.beginPath();
    ctx.moveTo(hover.x, 0);
    ctx.lineTo(hover.x, chartH);
    ctx.moveTo(0, hover.y);
    ctx.lineTo(plotW, hover.y);
    ctx.stroke();
    ctx.restore();
    let ci = visCols.length - 1;
    while (ci >= 0 && visCols[ci].time > t) ci--;
    const hasData = heatGrid && ci >= 0 && t - visCols[ci].time <= 2500;
    const qty = hasData ? heatGrid[ci][r] : 0;
    const lines = [
      `${formatNumber(rowLow, shownDecimals())} - ${formatNumber(rowLow + step, shownDecimals())}`,
      new Date(t).toLocaleTimeString("id-ID", { hour12: false }),
    ];
    if (!hasData) lines.push("belum ada data heatmap");
    else if (qty <= 0) lines.push("tidak ada order");
    else {
      const side = rowLow + step / 2 < adj(visCols[ci].mid) ? "bid (beli)" : "ask (jual)";
      lines.push(`tebal ${formatNumber(qty, 2)} ${side}`);
      lines.push(`${Math.round((qty / heatMax) * 100)}% dari skala warna${qty < (heatMax * heatPrefs.cut) / 100 ? " (tersembunyi)" : ""}`);
    }
    ctx.font = "11px ui-monospace, monospace";
    const boxW = Math.max(...lines.map((l) => ctx.measureText(l).width)) + 12;
    const boxHt = lines.length * 14 + 8;
    const bx = Math.min(plotW - boxW - 2, Math.max(2, hover.x + 12));
    const by = Math.min(chartH - boxHt - 2, Math.max(2, hover.y - boxHt - 12));
    ctx.fillStyle = "rgba(8,12,20,0.92)";
    ctx.fillRect(bx, by, boxW, boxHt);
    ctx.strokeStyle = "rgba(160,180,200,0.5)";
    ctx.strokeRect(bx + 0.5, by + 0.5, boxW - 1, boxHt - 1);
    ctx.fillStyle = "#dde6ef";
    lines.forEach((l, i) => ctx.fillText(l, bx + 6, by + 14 + i * 14));
  }
}

setInterval(() => {
  if (dirty.book) { renderOrderBook(); dirty.book = false; }
  if (dirty.tape) { renderTape(); dirty.tape = false; }
  if (dirty.chart) { renderBookmap(); renderVolumeList(); dirty.chart = false; }
}, 150);

setInterval(() => { dirty.chart = true; }, 1000);
addEventListener("resize", () => { dirty.chart = true; });

function refreshCalibInfo() {
  if (!offsetSetAt) {
    dom.calibInfo.className = "calib-info mono";
    dom.calibInfo.textContent = "belum dikalibrasi";
    return;
  }
  const sign = priceOffset >= 0 ? "+" : "-";
  const abs = Math.abs(priceOffset);
  dom.calibInfo.className = "calib-info mono fresh";
  dom.calibInfo.textContent =
    `selisih ${sign}${abs.toFixed(brokerDigits)} (${sign}${Math.round(abs * 10 ** brokerDigits)} poin)`;
}

function resetCalibration() {
  priceOffset = 0;
  offsetSetAt = 0;
  refreshCalibInfo();
  dirty.book = dirty.tape = dirty.chart = true;
}

dom.calibrate.onclick = () => {
  const mt5 = parseFloat(dom.mt5Price.value);
  const raw = rawLastPrice();
  if (!isFinite(mt5) || !raw) return;
  priceOffset = mt5 - raw;
  offsetSetAt = Date.now();
  refreshCalibInfo();
  dirty.book = dirty.tape = dirty.chart = true;
};
dom.resetOffset.onclick = resetCalibration;
dom.brokerDigits.onchange = (e) => {
  brokerDigits = +e.target.value;
  dom.mt5Price.step = String(10 ** -brokerDigits);
  try { localStorage.setItem("brokerDigits", String(brokerDigits)); } catch (err) {}
  refreshCalibInfo();
  dirty.book = dirty.tape = dirty.chart = true;
};
setInterval(refreshCalibInfo, 1000);

dom.symbolSelect.onchange = (e) => { currentSymbol = e.target.value; resetCalibration(); connectMarket(); };
dom.intervalSelect.onchange = (e) => { currentInterval = e.target.value; connectMarket(); };
dom.loadButton.onclick = () => {
  const value = dom.customSymbol.value.trim().toLowerCase();
  if (value) { currentSymbol = value; resetCalibration(); connectMarket(); }
};

connectMarket();
