require("dotenv").config();

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const amqp = require("amqplib");
const { createClient } = require("@supabase/supabase-js");

// Nama antrean sesuai instruksi
const QUEUE = "parking_queue"; 

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Inisialisasi Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// Middleware untuk memantau traffic HTTP yang masuk (Healthcheck Railway)
app.use((req, res, next) => {
  console.log(`[HTTP] Request masuk: ${req.method} ${req.url}`);
  next();
});

// Endpoint Utama (Wajib merespons 200 OK agar container tidak di-stop)
app.get("/", (req, res) => {
  res.status(200).send("Smart Parking WebSockets & Worker Service is Running!");
});

// ======================================================
// LOGIKA UTAMA: RABBITMQ & WEBSOCKET
// ======================================================
async function startRabbitMQ() {
  try {
    console.log("[RabbitMQ] Mencoba menghubungkan ke broker...");
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    
    // Penangkap error internal pada koneksi RabbitMQ agar tidak memicu fatal crash
    connection.on("error", (err) => {
      console.error("🔥 [RabbitMQ] Connection Error:", err.message);
    });
    
    connection.on("close", () => {
      console.warn("⚠️ [RabbitMQ] Koneksi terputus! Mencoba menghubungkan ulang dalam 5 detik...");
      setTimeout(startRabbitMQ, 5000);
    });

    const channel = await connection.createChannel();
    await channel.assertQueue(QUEUE, { durable: true });
    console.log(`✅ [RabbitMQ] Berhasil terhubung ke queue: ${QUEUE}`);

    // --- BAGIAN A: MENERIMA DATA DARI ESP32 (WEBSOCKET INBOUND) ---
    wss.on("connection", (ws, req) => {
      const ip = req.socket.remoteAddress;
      console.log(`✅ [WebSocket] ESP32 terhubung dari IP: ${ip}`);

      ws.on("message", (message) => {
        try {
          const data = JSON.parse(message.toString());
          console.log(`📥 [WS] Data diterima. Jumlah slot: ${data.slots ? data.slots.length : 0}`);

          // Teruskan payload mentah dari ESP32 ke dalam antrean RabbitMQ
          channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(data)), {
            persistent: true,
          });
          console.log(`📤 [RabbitMQ] Payload berhasil dimasukkan ke queue [${QUEUE}]`);

        } catch (err) {
          console.error("❌ [WebSocket] Gagal memproses JSON dari ESP32:", err.message);
        }
      });

      ws.on("close", () => {
        console.log("❌ [WebSocket] ESP32 terputus (Koneksi tertutup)");
      });

      ws.on("error", (error) => {
        console.error("⚠️ [WebSocket] Terjadi error pada socket:", error.message);
      });
    });

    // --- BAGIAN B: WORKER CONSUMER (FORWARD DATA KE SUPABASE) ---
    console.log(`👷 [Worker] Standby mendengarkan antrean: ${QUEUE}...`);
    
    channel.consume(QUEUE, async (msg) => {
      if (!msg) return;

      try {
        const data = JSON.parse(msg.content.toString());
        
        if (data.slots && Array.isArray(data.slots)) {
          // Mapping data array untuk Bulk Upsert ke Supabase
          const payloadToDb = data.slots.map((slot) => ({
            parking_id: slot.parking_id,
            area_id: slot.area_id,
            level_id: slot.level_id,
            zone_id: slot.zone_id,
            slot_number: slot.slot_number,
            is_filled: slot.is_filled,
            updated_at: new Date(),
          }));

          // Eksekusi Bulk Upsert ke tabel parking_slots
          const { error } = await supabase
            .from("parking_slots")
            .upsert(payloadToDb, {
              onConflict: "parking_id,area_id,level_id,zone_id,slot_number",
            });

          if (error) {
            console.error("❌ [Supabase] Gagal menyimpan data:", error.message);
          } else {
            console.log(`✅ [Supabase] Database berhasil diperbarui: ${payloadToDb.length} slot`);
          }
        } else {
          console.warn("⚠️ [Worker] Format payload tidak valid. Array 'slots' tidak ditemukan.");
        }

        // Acknowledge pesan agar dihapus dari antrean RabbitMQ
        channel.ack(msg);

      } catch (err) {
        console.error("❌ [Worker] Gagal memproses pesan antrean:", err.message);
        // Tetap di-ack jika JSON corrupt agar antrean tidak macet bergulung terus
        channel.ack(msg); 
      }
    });

  } catch (err) {
    console.error("🔥 [RabbitMQ] Gagal inisialisasi awal:", err.message);
    // Coba hubungkan ulang jika setup awal gagal
    setTimeout(startRabbitMQ, 5000);
  }
}

// ======================================================
// JARING PENGAMAN GLOBAL (ANTI-STOPPING CONTAINER)
// ======================================================
process.on("uncaughtException", (err) => {
  console.error("🔥 [CRITICAL_EXC] Uncaught Exception Terdeteksi:", err.message);
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("🔥 [CRITICAL_REJ] Unhandled Rejection Terdeteksi:", reason);
});

// ======================================================
// EKSEKUSI SERVER HTTP
// ======================================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Server HTTP berhasil berjalan di port ${PORT}`);
  
  // Menjalankan subsistem RabbitMQ setelah port HTTP dipastikan terbuka
  startRabbitMQ();
});