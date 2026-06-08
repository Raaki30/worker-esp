require("dotenv").config();

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const WebSocket = require("ws"); // Wajib di-require spesifik untuk Supabase
const amqp = require("amqplib");
const { createClient } = require("@supabase/supabase-js");

// PENCEGAHAN CRASH 1: Validasi Environment Variables
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
  console.error("🔥 FATAL ERROR: SUPABASE_URL atau SUPABASE_KEY belum diisi di Railway!");
}
if (!process.env.RABBITMQ_URL) {
  console.error("🔥 FATAL ERROR: RABBITMQ_URL belum diisi di Railway!");
}

const QUEUE = "parking_queue"; 

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// PENCEGAHAN CRASH 2: Konfigurasi Supabase dengan injeksi WebSocket
// (Railway menggunakan Node 20+, sehingga butuh injeksi ini agar tidak crash)
const supabase = createClient(
  process.env.SUPABASE_URL || "https://dummy.supabase.co", 
  process.env.SUPABASE_KEY || "dummy_key",
  {
    auth: {
      persistSession: false 
    },
    realtime: {
      transport: WebSocket 
    }
  }
);

// Endpoint Healthcheck (Penting: Ini yang dicek oleh bot Railway agar container tidak di-stop)
app.get("/", (req, res) => {
  res.status(200).send("Smart Parking Service is LIVE on Railway!");
});

// ======================================================
// MANAJEMEN JALUR DATA: RABBITMQ & WEBSOCKET
// ======================================================
async function startRabbitMQ() {
  try {
    console.log("[RabbitMQ] Menghubungkan ke broker...");
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    
    connection.on("error", (err) => console.error("🔥 [RabbitMQ] Error:", err.message));
    connection.on("close", () => {
      console.warn("⚠️ [RabbitMQ] Terputus! Reconnect dalam 5 detik...");
      setTimeout(startRabbitMQ, 5000);
    });

    const channel = await connection.createChannel();
    await channel.assertQueue(QUEUE, { durable: true });
    console.log(`✅ [RabbitMQ] Terhubung ke antrean: ${QUEUE}`);

    // --- WEBSOCKET INBOUND (Terima dari ESP32) ---
    wss.on("connection", (ws, req) => {
      console.log(`✅ [WebSocket] ESP32 Terhubung dari IP: ${req.socket.remoteAddress}`);

      ws.on("message", (message) => {
        try {
          const data = JSON.parse(message.toString());
          console.log(`📥 [WS] Data masuk. Slot yang dikirim: ${data.slots ? data.slots.length : 0}`);
          
          // Lempar data mentah ke RabbitMQ
          channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(data)), { persistent: true });
        } catch (err) {
          console.error("❌ [WS] Invalid JSON dari ESP32:", err.message);
        }
      });

      ws.on("close", () => console.log("❌ [WebSocket] ESP32 Terputus"));
      ws.on("error", (err) => console.error("⚠️ [WS] Error pada soket:", err.message));
    });

    // --- WORKER CONSUMER (Kirim ke Supabase) ---
    console.log(`👷 [Worker] Standby memproses antrean di latar belakang...`);
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

          const { error } = await supabase.from("parking_slots").upsert(payloadToDb, {
            onConflict: "parking_id,area_id,level_id,zone_id,slot_number",
          });

          if (error) {
            console.error("❌ [Supabase] Gagal menyimpan data:", error.message);
          } else {
            console.log(`✅ [Supabase] Berhasil memperbarui ${payloadToDb.length} slot di database`);
          }
        }
        
        channel.ack(msg);
      } catch (err) {
        console.error("❌ [Worker] Gagal memproses pesan:", err.message);
        channel.ack(msg); 
      }
    });

  } catch (err) {
    console.error("🔥 [RabbitMQ] Gagal Konek (Inisialisasi):", err.message);
    setTimeout(startRabbitMQ, 5000);
  }
}

// ======================================================
// JARING PENGAMAN GLOBAL (ANTI-CRASH)
// ======================================================
process.on("uncaughtException", (err) => console.error("🔥 [CRITICAL] Uncaught Exception:", err.message));
process.on("unhandledRejection", (reason) => console.error("🔥 [CRITICAL] Unhandled Rejection:", reason));

// ======================================================
// EKSEKUSI SERVER
// ======================================================
// Railway secara otomatis akan memasukkan port ke dalam process.env.PORT
const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Server HTTP berjalan stabil di port ${PORT}`);
  startRabbitMQ();
});