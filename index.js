require("dotenv").config();

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const amqp = require("amqplib");
const { createClient } = require("@supabase/supabase-js");

// NAMA QUEUE SUDAH DISESUAIKAN
const QUEUE = "parking_queue"; 

// 1. Inisialisasi Express & WebSocket
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// Endpoint Health Check (Sangat penting agar Railway tidak mematikan container)
app.get("/", (req, res) => {
  res.send("Smart Parking WebSockets & Worker Service is Running!");
});

// ======================================================
// FUNGSI KONEKSI RABBITMQ & WORKER (JALAN DI BACKGROUND)
// ======================================================
async function startRabbitMQ() {
  try {
    console.log("Menghubungkan ke RabbitMQ...");
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE, { durable: true });
    console.log(`✅ RabbitMQ Connected to queue: ${QUEUE}`);

    // --- WEBSOCKET LISTENER (Menerima dari ESP32) ---
    wss.on("connection", (ws, req) => {
      console.log(`✅ [WebSocket] ESP32 Connected from ${req.socket.remoteAddress}`);

      ws.on("message", (message) => {
        try {
          const data = JSON.parse(message.toString());
          console.log(`📥 [WS] Data received. Slots: ${data.slots ? data.slots.length : 0}`);

          channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(data)), {
            persistent: true,
          });
        } catch (err) {
          console.error("❌ [WebSocket] Invalid JSON:", err.message);
        }
      });

      ws.on("close", () => console.log("❌ [WebSocket] ESP32 Disconnected"));
      ws.on("error", (error) => console.error("⚠️ [WebSocket] Error:", error));
    });

    // --- WORKER CONSUMER (Menyimpan ke Supabase) ---
    console.log(`👷 Worker siap mendengarkan antrean: ${QUEUE}...`);
    channel.consume(QUEUE, async (msg) => {
      if (!msg) return;

      try {
        const data = JSON.parse(msg.content.toString());
        
        if (data.slots && Array.isArray(data.slots)) {
          const payloadToDb = data.slots.map((slot) => ({
            parking_id: slot.parking_id,
            area_id: slot.area_id,
            level_id: slot.level_id,
            zone_id: slot.zone_id,
            slot_number: slot.slot_number,
            is_filled: slot.is_filled,
            updated_at: new Date(),
          }));

          const { error } = await supabase
            .from("parking_slots")
            .upsert(payloadToDb, {
              onConflict: "parking_id,area_id,level_id,zone_id,slot_number",
            });

          if (error) {
            console.error("❌ [Supabase] Error:", error.message);
          } else {
            console.log(`✅ [Supabase] Updated ${payloadToDb.length} slots`);
          }
        }
        channel.ack(msg);
      } catch (err) {
        console.error("❌ [Worker] Error:", err.message);
        channel.ack(msg); 
      }
    });

  } catch (err) {
    console.error("🔥 RabbitMQ Connection Error:", err.message);
    // Coba ulang koneksi RabbitMQ setelah 5 detik jika terputus
    setTimeout(startRabbitMQ, 5000);
  }
}

// ======================================================
// JALANKAN SERVER EXPRESS TERLEBIH DAHULU!
// ======================================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Server HTTP berjalan di port ${PORT}`);
  
  // Setelah server HTTP berhasil jalan, baru kita panggil RabbitMQ
  startRabbitMQ();
});

// ======================================================
// JARING PENGAMAN ANTI-CRASH (GLOBAL ERROR HANDLER)
// ======================================================
process.on("uncaughtException", (err) => {
  console.error("🔥 [CRITICAL] Uncaught Exception:", err.message);
  // Jangan biarkan aplikasi mati
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("🔥 [CRITICAL] Unhandled Rejection:", reason);
  // Jangan biarkan aplikasi mati
});