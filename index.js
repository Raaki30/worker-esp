require("dotenv").config();

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const WebSocket = require("ws"); // Di-require secara spesifik untuk injeksi ke Supabase
const amqp = require("amqplib");
const { createClient } = require("@supabase/supabase-js");

// PENCEGAHAN CRASH 1: Cek apakah variabel dari Back4App sudah masuk
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
  console.error("🔥 FATAL ERROR: SUPABASE_URL atau SUPABASE_KEY belum terbaca dari server!");
}
if (!process.env.RABBITMQ_URL) {
  console.error("🔥 FATAL ERROR: RABBITMQ_URL belum terbaca dari server!");
}

const QUEUE = "parking_queue"; 

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// PENCEGAHAN CRASH 2: Konfigurasi Supabase khusus untuk Node.js 20+
const supabase = createClient(
  process.env.SUPABASE_URL || "https://dummy.supabase.co", 
  process.env.SUPABASE_KEY || "dummy_key",
  {
    auth: {
      persistSession: false // Wajib false untuk lingkungan server/backend
    },
    realtime: {
      transport: WebSocket // Solusi jitu untuk error "Node.js 20 without native WebSocket"
    }
  }
);

// Healthcheck endpoint (Wajib untuk Back4App)
app.get("/", (req, res) => {
  res.status(200).send("Smart Parking Service is LIVE on Back4App!");
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
          
          // Melempar payload ESP32 langsung ke RabbitMQ
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
          // Mapping data dari payload JSON
          const payloadToDb = data.slots.map((slot) => ({
            parking_id: slot.parking_id,
            area_id: slot.area_id,
            level_id: slot.level_id,
            zone_id: slot.zone_id,
            slot_number: slot.slot_number,
            is_filled: slot.is_filled,
            updated_at: new Date(),
          }));

          // Eksekusi Bulk Upsert ke Supabase
          const { error } = await supabase.from("parking_slots").upsert(payloadToDb, {
            onConflict: "parking_id,area_id,level_id,zone_id,slot_number",
          });

          if (error) {
            console.error("❌ [Supabase] Gagal menyimpan data:", error.message);
          } else {
            console.log(`✅ [Supabase] Berhasil memperbarui ${payloadToDb.length} slot di database`);
          }
        }
        
        // Tandai pesan sudah diproses agar dihapus dari antrean
        channel.ack(msg);
      } catch (err) {
        console.error("❌ [Worker] Gagal memproses pesan:", err.message);
        channel.ack(msg); // Tetap di-ack agar antrean tidak macet/looping
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
// Back4App merekomendasikan penggunaan variabel environment PORT (Biasanya 3000)
const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Server HTTP berjalan aman di port ${PORT}`);
  startRabbitMQ();
});