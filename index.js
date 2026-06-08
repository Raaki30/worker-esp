require("dotenv").config();

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const amqp = require("amqplib");
const { createClient } = require("@supabase/supabase-js");

const QUEUE = "parking_queue"; 

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// ======================================================
// FUNGSI KONEKSI RABBITMQ & WORKER
// ======================================================
async function startRabbitMQ() {
  try {
    console.log("[RabbitMQ] Menghubungkan ke CloudAMQP...");
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    
    connection.on("error", (err) => console.error("🔥 [RabbitMQ] Error:", err.message));
    connection.on("close", () => {
      console.warn("⚠️ [RabbitMQ] Terputus! Reconnect dalam 5 detik...");
      setTimeout(startRabbitMQ, 5000);
    });

    const channel = await connection.createChannel();
    await channel.assertQueue(QUEUE, { durable: true });
    console.log(`✅ [RabbitMQ] Terhubung ke queue: ${QUEUE}`);

    // --- WEBSOCKET SERVER (TERIMA DARI ESP32) ---
    wss.on("connection", (ws, req) => {
      console.log(`✅ [WebSocket] ESP32 Terhubung dari IP Lokal: ${req.socket.remoteAddress}`);

      ws.on("message", (message) => {
        try {
          const data = JSON.parse(message.toString());
          console.log(`📥 [WS] Data diterima. Slot ter-update: ${data.slots ? data.slots.length : 0}`);

          channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(data)), { persistent: true });
        } catch (err) {
          console.error("❌ [WebSocket] Invalid JSON:", err.message);
        }
      });

      ws.on("close", () => console.log("❌ [WebSocket] ESP32 Terputus"));
      ws.on("error", (error) => console.error("⚠️ [WebSocket] Error:", error.message));
    });

    // --- WORKER CONSUMER (KIRIM KE SUPABASE) ---
    console.log(`👷 [Worker] Standby memproses antrean...`);
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
            .upsert(payloadToDb, { onConflict: "parking_id,area_id,level_id,zone_id,slot_number" });

          if (error) {
            console.error("❌ [Supabase] Error:", error.message);
          } else {
            console.log(`✅ [Supabase] Updated ${payloadToDb.length} slot di Database!`);
          }
        }
        channel.ack(msg);
      } catch (err) {
        console.error("❌ [Worker] Error:", err.message);
        channel.ack(msg); 
      }
    });

  } catch (err) {
    console.error("🔥 [RabbitMQ] Gagal Konek:", err.message);
    setTimeout(startRabbitMQ, 5000);
  }
}

// ======================================================
// JALANKAN SERVER LOKAL DI PORT 3000
// ======================================================
const PORT = 3000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 [LOCAL SERVER] Berjalan di port ${PORT}`);
  console.log(`👉 Pastikan ESP32 diarahkan ke IP IPv4 Laptop kamu!`);
  startRabbitMQ();
});