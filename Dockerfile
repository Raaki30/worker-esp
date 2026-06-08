FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
# Hapus --production agar semua dependencies pasti ter-install
RUN npm install
COPY . .
EXPOSE 3000
CMD ["node", "index.js"]