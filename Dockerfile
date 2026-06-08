# 1. Gunakan versi Node.js 20 (Sesuai syarat Supabase terbaru)
FROM node:20-alpine

# 2. Tentukan direktori kerja di dalam container
WORKDIR /app

# 3. Salin file konfigurasi package
COPY package*.json ./

# 4. Install semua dependensi
RUN npm install --production

# 5. Salin seluruh sisa kode aplikasi
COPY . .

# 6. Ekspos port
EXPOSE 3000

# 7. Perintah wajib untuk menjalankan server
CMD ["node", "index.js"]