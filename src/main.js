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
  candleCanvas: el("candleCanvas"),
  bookmapCanvas: el("bookmapCanvas"),
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
      dom.statLatency.textContent = Math.max(0, Date.now() - payload.E) + " ms";
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
    time: Date.now(),
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
  const mutedColor = style.getPropertyValue("--muted");
  ctx.fillStyle = "#0a0e16";
  ctx.fillRect(0, 0, width, height);
  ctx.font = "11px ui-monospace, monospace";
  ctx.fillStyle = mutedColor;
  const cols = heatmapColumns;
  if (cols.length < 2) {
    ctx.fillText("mengumpulkan data order book...", 10, 20);
    return;
  }

  const plotW = width - 66;
  const mids = cols.map((c) => adj(c.mid));
  const lo = Math.min(...mids);
  const hi = Math.max(...mids);
  const midNow = mids.at(-1);
  const half = Math.max((hi - lo) / 2 + midNow * 0.0002, midNow * 0.0006);
  const pLow = (hi + lo) / 2 - half;
  const pHigh = (hi + lo) / 2 + half;
  const ROWS = HEATMAP_ROWS;
  const rowH = height / ROWS;
  const toY = (p) => height - ((p - pLow) / (pHigh - pLow)) * height;
  const t0 = cols[0].time;
  const span = Math.max(60000, cols.at(-1).time - t0 + 2000);
  const colW = (2000 / span) * plotW + 1;

  const grid = cols.map((col) => {
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
    const x = ((cols[ci].time - t0) / span) * plotW;
    rows.forEach((qty, r) => {
      if (qty <= 0) return;
      const intensity = Math.min(1, Math.log1p(qty) / Math.log1p(maxQty));
      ctx.fillStyle = heatColor(intensity);
      ctx.fillRect(x, height - (r + 1) * rowH, colW, rowH + 1);
    });
  });

  ctx.strokeStyle = "#ffffff";
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  cols.forEach((col, i) => {
    const x = ((col.time - t0) / span) * plotW + colW / 2;
    const y = toY(mids[i]);
    i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
  });
  ctx.stroke();

  ctx.fillStyle = mutedColor;
  for (let i = 0; i <= 4; i++) {
    const price = pLow + ((pHigh - pLow) * i) / 4;
    ctx.fillText(formatNumber(price, shownDecimals()), plotW + 4, Math.min(height - 3, Math.max(11, toY(price) + 4)));
  }
  ctx.fillStyle = "#ffffff";
  ctx.fillText(formatNumber(midNow, shownDecimals()), plotW + 4, Math.min(height - 3, Math.max(11, toY(midNow) + 4)));
  ctx.fillStyle = mutedColor;
  ctx.fillText(`${Math.round(span / 1000)} dtk`, 6, 14);
}

function renderChart() {
  const canvas = dom.candleCanvas;
  const ratio = devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);

  const style = getComputedStyle(document.documentElement);
  const bidColor = style.getPropertyValue("--bid");
  const askColor = style.getPropertyValue("--ask");
  const lineColor = style.getPropertyValue("--line");
  const mutedColor = style.getPropertyValue("--muted");

  const visibleCount = Math.min(candles.length, Math.floor(width / 8));
  const visibleCandles = candles.slice(-visibleCount).map((c) => ({
    ...c,
    open: adj(c.open),
    high: adj(c.high),
    low: adj(c.low),
    close: adj(c.close),
  }));
  if (!visibleCandles.length) return;

  const priceHigh = Math.max(...visibleCandles.map((c) => c.high));
  const priceLow = Math.min(...visibleCandles.map((c) => c.low));
  const volumeMax = Math.max(...visibleCandles.map((c) => c.volume));
  const chartHeight = height * 0.72;
  const rightPad = 66;
  const columnWidth = (width - rightPad) / visibleCount;
  const priceToY = (price) =>
    chartHeight - ((price - priceLow) / (priceHigh - priceLow || 1)) * (chartHeight - 8) + 4;

  // --- price grid labels ---
  ctx.strokeStyle = lineColor;
  ctx.fillStyle = mutedColor;
  ctx.font = "11px ui-monospace, monospace";
  for (let i = 0; i <= 4; i++) {
    const price = priceLow + ((priceHigh - priceLow) * i) / 4;
    const y = priceToY(price);
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(width - rightPad, y);
    ctx.stroke();
    ctx.fillText(formatNumber(price, shownDecimals()), width - rightPad + 4, y + 4);
  }

  // --- candle outlines on top of the heatmap ---
  visibleCandles.forEach((candle, index) => {
    const x = index * columnWidth + columnWidth / 2;
    const color = candle.close >= candle.open ? bidColor : askColor;
    ctx.strokeStyle = color;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(x, priceToY(candle.high));
    ctx.lineTo(x, priceToY(candle.low));
    ctx.stroke();
    const top = priceToY(Math.max(candle.open, candle.close));
    const bottom = priceToY(Math.min(candle.open, candle.close));
    ctx.strokeRect(x - columnWidth * 0.35, top, columnWidth * 0.7, Math.max(1, bottom - top));

    const volumeBarHeight = (candle.volume / volumeMax) * (height - chartHeight - 6);
    ctx.globalAlpha = 0.5;
    ctx.fillStyle = color;
    ctx.fillRect(x - columnWidth * 0.35, height - volumeBarHeight, columnWidth * 0.7, volumeBarHeight);
    ctx.globalAlpha = 1;
  });
}

setInterval(() => {
  if (dirty.book) { renderOrderBook(); dirty.book = false; }
  if (dirty.tape) { renderTape(); dirty.tape = false; }
  if (dirty.chart) { renderBookmap(); renderChart(); renderVolumeList(); dirty.chart = false; }
}, 150);

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
