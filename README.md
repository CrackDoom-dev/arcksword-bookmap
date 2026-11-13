# Arcksword Bookmap Free

Viewer order book, time and sales, dan heatmap likuiditas berbasis data pasar publik Binance.
Dijalankan sepenuhnya di komputer/HP sendiri lewat npm, tanpa server pihak ketiga.

## Menjalankan

Butuh Node.js 18 ke atas (di Termux: `pkg install nodejs`).

```
npm install
npm run dev
```

Vite akan menampilkan alamat seperti `http://localhost:5173`, buka di browser.

## Catatan

- Data diambil langsung dari market data publik Binance (REST untuk candle awal,
  WebSocket untuk order book, transaksi, dan update candle). Tidak ada delay buatan.
- Simbol default XAUUSDT (kontrak perpetual emas di Binance Futures, harganya mengikuti emas spot
  sehingga paling dekat dengan XAUUSD di MT5). PAXG/USDT tetap ada di dropdown. Simbol futures lain
  (mis. xagusdt) bisa dimuat lewat kolom "simbol lain" setelah ditambahkan ke FUTURES_SYMBOLS di src/main.js.
- Untuk simbol futures, aplikasi memakai dua WebSocket (/public untuk depth, /market untuk trade dan candle)
  sesuai aturan URL baru Binance Futures.
- Selisih kecil dengan harga MT5 tetap mungkin ada (funding, basis, kuotasi broker). Pakai kolom "Harga di MT5"
  untuk kalibrasi offset.
- Heatmap menunjukkan ketebalan order resting (bid + ask) di tiap level harga seiring
  waktu, disampel tiap 1 detik dari snapshot order book 20 level. Karena hanya 20 level
  per sisi yang tersedia dari API publik, heatmap paling informatif untuk area harga
  dekat best bid/ask, bukan seluruh depth pasar.
- Nama "Arcksword Bookmap Free" adalah penamaan proyek pribadi, tidak berafiliasi
  dengan produk komersial manapun yang memakai nama serupa.

## akses here 👉 https://crackdoom-dev.github.io/arcksword-bookmap/
