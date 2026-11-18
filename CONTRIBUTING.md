# Ikut berkontribusi

Terima kasih sudah mau ikut. Bug kecil, typo, sampai fitur baru semuanya boleh.

## Lapor bug atau request

Buka tab [Issues](https://github.com/CrackDoom-dev/arcksword-bookmap/issues/new/choose) lalu pilih template:

- **Laporan bug** kalau ada yang rusak
- **Request fitur** kalau punya ide
- **Lainnya** untuk pertanyaan atau saran

Cari dulu di issue yang sudah ada supaya tidak dobel.

## Kirim perubahan

1. Fork repo ini.
2. Clone hasil fork, lalu buat branch dari `main`:

   ```bash
   git checkout -b perbaiki-bubble-kecil
   ```

3. Pasang dan jalankan:

   ```bash
   npm install
   npm run dev
   ```

4. Ubah kodenya, lalu pastikan build lolos:

   ```bash
   npm run build
   ```

5. Commit dan push ke fork kamu:

   ```bash
   git add .
   git commit -m "perbaiki ukuran bubble di transaksi kecil"
   git push -u origin perbaiki-bubble-kecil
   ```

6. Buka pull request ke `main`. Isi template yang muncul.

Lewat Termux dengan `gh`:

```bash
gh repo fork CrackDoom-dev/arcksword-bookmap --clone
cd arcksword-bookmap
git checkout -b nama-branch
git push -u origin nama-branch
gh pr create --fill
```

## Aturan singkat

- Satu pull request, satu perubahan. Jangan campur banyak hal.
- Ikuti gaya kode yang sudah ada. Proyek ini JavaScript biasa tanpa framework, jangan tambah dependensi kalau tidak perlu.
- Pesan commit pendek dan jelas, huruf kecil di awal.
- Perubahan besar atau yang mengubah tampilan, buka issue dulu supaya kita sepakat arahnya.
- Jangan masukkan kunci API, token, atau data pribadi.

## Supaya terdaftar sebagai kontributor

GitHub mencatat kamu otomatis di daftar kontributor setelah commit kamu masuk ke `main`. Syaratnya:

- pull request kamu sudah di-merge
- email di commit terhubung ke akun GitHub kamu

Cek email commit dengan `git config user.email`. Kalau tidak mau membuka email asli, pakai email noreply dari GitHub di Settings > Emails, lalu:

```bash
git config --global user.email "ID+username@users.noreply.github.com"
```

Daftarnya bisa dilihat di tab [Contributors](https://github.com/CrackDoom-dev/arcksword-bookmap/graphs/contributors) dan ikut tampil di README.
