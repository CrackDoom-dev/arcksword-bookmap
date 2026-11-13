// Arcksword Bookmap Free
// Sumber data: market data publik Binance (REST + WebSocket), tanpa delay buatan.

const MARKET_HOSTS = [
  { rest: "https://data-api.binance.vision", ws: "wss://data-stream.binance.vision" },
  { rest: "https://api.binance.com", ws: "wss://stream.binance.com:9443" },
  { rest: "https://api.binance.me", ws: "wss://stream.binance.me:9443" },
];

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

// ---- State ----
let currentSymbol = "paxgusdt";
let currentInterval = "1m";
let activeHost = MARKET_HOSTS[0];
let socket = null;

let candles = [];
let orderBookBids = [];
let orderBookAsks = [];
let tradeTape = [];
let tapeFlow = { buy: 0, sell: 0 };
let heatmapColumns = []; // { time, bids:[[price,qty]], asks:[[price,qty]] }
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

const MAX_HEATMAP_COLUMNS = 450;
const HEATMAP_ROWS = 80;

const dirty = { book: false, tape: false, chart: false };

// ---- Networking ----
async function fetchKlinesWithFallback(symbol, interval) {
  let lastError;
  for (const host of MARKET_HOSTS) {
    try {
      const response = await fetch(
        `${host.rest}/api/v3/klines?symbol=${symbol.toUpperCase()}&interval=${interval}&limit=80`
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
  if (socket) {
    socket.onclose = null;
    socket.close();
  }
  orderBookBids = [];
  orderBookAsks = [];
  if (!isReconnect) {
    candles = [];
    tradeTape = [];
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

  const streams = `${currentSymbol}@depth20@100ms/${currentSymbol}@trade/${currentSymbol}@kline_${currentInterval}`;
  socket = new WebSocket(`${activeHost.ws}/stream?streams=${streams}`);

  socket.onopen = () => {
    dom.status.className = "status status-on";
    dom.status.textContent = "langsung";
  };
  socket.onclose = () => {
    dom.status.className = "status status-off";
    dom.status.textContent = "terputus, mencoba lagi";
    setTimeout(() => connectMarket(true), 3000);
  };
  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    const payload = message.data;
    if (message.stream.includes("@depth")) {
      orderBookBids = payload.bids;
      orderBookAsks = payload.asks;
      dirty.book = true;
    } else if (message.stream.includes("@trade")) {
      const quantity = +payload.q;
      const isSell = payload.m;
      tradeTape.unshift({ time: payload.T, price: +payload.p, quantity, isSell });
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
  };
}

// ---- Heatmap sampling: order book 500 level, diambil tiap 2 detik ----
let serverOffset = 0;
const nowMs = () => Date.now() + serverOffset;
async function syncClock() {
  try {
    const t0 = Date.now();
    const response = await fetch(`${activeHost.rest}/api/v3/time`);
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
    const response = await fetch(`${activeHost.rest}/api/v3/depth?symbol=${symbol.toUpperCase()}&limit=500`);
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

// ---- Rendering ----
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
  const lineColor = style.getPropertyValue("--line").trim();
  ctx.fillStyle = "#0a0e16";
  ctx.fillRect(0, 0, width, height);
  ctx.font = "11px ui-monospace, monospace";
  if (!candles.length) {
    ctx.fillStyle = mutedColor;
    ctx.fillText("memuat data...", 10, 20);
    return;
  }

  const interval = candles.length > 1 ? candles[1].time - candles[0].time : 60000;
  const cols = heatmapColumns;
  const now = nowMs();
  const histSpan = cols.length ? now - cols[0].time : 0;
  const windowSpan = Math.min(candles.length * interval, Math.max(histSpan + 2 * interval, 8 * interval));
  const windowEnd = now + windowSpan * 0.03;
  const windowStart = windowEnd - windowSpan;
  const plotW = width - 66;
  const chartH = height * 0.8;
  const xOf = (t) => ((t - windowStart) / windowSpan) * plotW;

  const vis = candles
    .filter((c) => c.time + interval > windowStart)
    .map((c) => ({ ...c, open: adj(c.open), high: adj(c.high), low: adj(c.low), close: adj(c.close) }));
  const lo = Math.min(...vis.map((c) => c.low));
  const hi = Math.max(...vis.map((c) => c.high));
  const center = (hi + lo) / 2;
  const half = Math.max(((hi - lo) / 2) * 1.1, center * 0.0006);
  const pLow = center - half;
  const pHigh = center + half;
  const toY = (p) => chartH - ((p - pLow) / (pHigh - pLow)) * chartH;
  const ROWS = HEATMAP_ROWS;
  const rowH = chartH / ROWS;

  const visCols = cols.filter((c) => c.time >= windowStart);
  if (visCols.length) {
    const colW = (2000 / windowSpan) * plotW + 1;
    const grid = visCols.map((col) => {
      const rows = new Float32Array(ROWS);
      for (const book of [col.bids, col.asks]) {
        for (let i = 0; i < book.length; i += 2) {
          const p = adj(book[i]);
          if (p < pLow || p >= pHigh) continue;
          rows[Math.floor(((p - pLow) / (pHigh - pLow)) * ROWS)] += book[i + 1];
        }
      }
      return rows;
    });
    const values = [];
    grid.forEach((rows) => rows.forEach((v) => { if (v > 0) values.push(v); }));
    values.sort((x, y) => x - y);
    const maxQty = values.length ? values[Math.floor(values.length * 0.97)] || values.at(-1) : 1;
    grid.forEach((rows, ci) => {
      const x = xOf(visCols[ci].time);
      rows.forEach((qty, r) => {
        if (qty <= 0) return;
        ctx.fillStyle = heatColor(Math.min(1, Math.log1p(qty) / Math.log1p(maxQty)));
        ctx.fillRect(x, chartH - (r + 1) * rowH, colW, rowH + 1);
      });
    });
  }

  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const price = pLow + ((pHigh - pLow) * i) / 4;
    const y = toY(price);
    ctx.globalAlpha = 0.3;
    ctx.strokeStyle = mutedColor;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(plotW, y);
    ctx.stroke();
    ctx.globalAlpha = 1;
    ctx.fillStyle = mutedColor;
    ctx.fillText(formatNumber(price, shownDecimals()), plotW + 4, Math.min(chartH - 2, Math.max(11, y + 4)));
  }

  const volMax = Math.max(...vis.map((c) => c.volume), 1e-9);
  vis.forEach((c) => {
    const candleW = (Math.max(0, Math.min(interval, now - c.time)) / windowSpan) * plotW;
    const bodyW = Math.max(2, candleW * 0.7);
    const x = xOf(c.time) + candleW / 2;
    const color = c.close >= c.open ? bidColor : askColor;
    const barH = (c.volume / volMax) * (height - chartH - 8);
    ctx.globalAlpha = 0.55;
    ctx.fillStyle = color;
    ctx.fillRect(x - bodyW / 2, height - barH, bodyW, barH);
    ctx.globalAlpha = 1;
    ctx.strokeStyle = color;
    ctx.beginPath();
    ctx.moveTo(x, toY(c.high));
    ctx.lineTo(x, toY(c.low));
    ctx.stroke();
    const top = toY(Math.max(c.open, c.close));
    const bottom = toY(Math.min(c.open, c.close));
    ctx.fillStyle = color;
    ctx.fillRect(x - bodyW / 2, top, bodyW, Math.max(1, bottom - top));
  });

  const last = vis.at(-1);
  const lastY = toY(last.close);
  const priceColor = last.close >= last.open ? bidColor : askColor;
  ctx.save();
  ctx.setLineDash([4, 3]);
  ctx.strokeStyle = priceColor;
  ctx.beginPath();
  ctx.moveTo(0, lastY);
  ctx.lineTo(plotW, lastY);
  ctx.stroke();
  ctx.restore();
  const remaining = candleRemaining();
  dom.candleTimer.textContent = remaining === null ? "" : "candle baru " + fmtClock(remaining);
  const boxH = remaining === null ? 16 : 30;
  const boxY = Math.min(chartH - boxH, Math.max(0, lastY - 8));
  ctx.fillStyle = priceColor;
  ctx.fillRect(plotW + 1, boxY, 65, boxH);
  ctx.fillStyle = "#06121f";
  ctx.font = "bold 11px ui-monospace, monospace";
  ctx.fillText(formatNumber(last.close, shownDecimals()), plotW + 4, boxY + 12);
  if (remaining !== null) ctx.fillText(fmtClock(remaining), plotW + 4, boxY + 25);

  ctx.font = "11px ui-monospace, monospace";
  ctx.fillStyle = mutedColor;
  ctx.fillText(`${Math.round(windowSpan / 60000)} mnt`, 6, 14);
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
  const age = Math.round((Date.now() - offsetSetAt) / 1000);
  const sign = priceOffset >= 0 ? "+" : "-";
  const abs = Math.abs(priceOffset);
  dom.calibInfo.className = "calib-info mono " + (age > 120 ? "stale" : "fresh");
  dom.calibInfo.textContent =
    `selisih ${sign}${abs.toFixed(brokerDigits)} (${sign}${Math.round(abs * 10 ** brokerDigits)} poin) | ${age} dtk lalu` +
    (age > 120 ? " - kalibrasi ulang" : "");
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
