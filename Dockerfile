# 1. Gunakan versi Node.js yang ringan (Alpine Linux)
FROM node:18-alpine

# 2. Tentukan direktori kerja di dalam container
WORKDIR /app

# 3. Salin file konfigurasi package (package.json & package-lock.json)
COPY package*.json ./

# 4. Install semua dependensi (Express, ws, amqplib, supabase-js)
RUN npm install --production

# 5. Salin seluruh sisa kode aplikasi kamu ke dalam container
COPY . .

# 6. Ekspos port (Default Back4app / yang kita pakai di index.js)
EXPOSE 3000

# 7. Perintah wajib untuk menjalankan server
CMD ["node", "index.js"]