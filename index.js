require("dotenv").config();

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const WebSocket = require("ws"); 
const amqp = require("amqplib");
const { createClient } = require("@supabase/supabase-js");

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
  console.error("🔥 FATAL ERROR: SUPABASE_URL atau SUPABASE_KEY belum disetel!");
}
if (!process.env.RABBITMQ_URL) {
  console.error("🔥 FATAL ERROR: RABBITMQ_URL belum disetel!");
}

const QUEUE = "parking_queue"; 

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const supabase = createClient(
  process.env.SUPABASE_URL || "https://dummy.supabase.co", 
  process.env.SUPABASE_KEY || "dummy_key",
  {
    auth: { persistSession: false },
    realtime: { transport: WebSocket }
  }
);

// Healthcheck endpoint (Dibutuhkan oleh mesin Fly.io)
app.get("/", (req, res) => {
  res.status(200).send("Smart Parking Service is LIVE on Fly.io!");
});

async function startRabbitMQ() {
  try {
    console.log("[RabbitMQ] Menghubungkan...");
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    
    connection.on("error", (err) => console.error("🔥 [RabbitMQ] Error:", err.message));
    connection.on("close", () => {
      console.warn("⚠️ [RabbitMQ] Terputus! Reconnect dalam 5 detik...");
      setTimeout(startRabbitMQ, 5000);
    });

    const channel = await connection.createChannel();
    await channel.assertQueue(QUEUE, { durable: true });
    console.log(`✅ [RabbitMQ] Terhubung ke antrean: ${QUEUE}`);

    wss.on("connection", (ws, req) => {
      console.log(`✅ [WebSocket] ESP32 Terhubung dari IP: ${req.socket.remoteAddress}`);

      ws.on("message", (message) => {
        try {
          const data = JSON.parse(message.toString());
          channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(data)), { persistent: true });
        } catch (err) {
          console.error("❌ [WS] Invalid JSON dari ESP32:", err.message);
        }
      });

      ws.on("close", () => console.log("❌ [WebSocket] ESP32 Terputus"));
      ws.on("error", (err) => console.error("⚠️ [WS] Error pada soket:", err.message));
    });

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

          const { error } = await supabase.from("parking_slots").upsert(payloadToDb, {
            onConflict: "parking_id,area_id,level_id,zone_id,slot_number",
          });

          if (error) console.error("❌ [Supabase] Error:", error.message);
          else console.log(`✅ [Supabase] Updated ${payloadToDb.length} slot`);
        }
        channel.ack(msg);
      } catch (err) {
        console.error("❌ [Worker] Gagal memproses:", err.message);
        channel.ack(msg); 
      }
    });

  } catch (err) {
    console.error("🔥 [RabbitMQ] Gagal inisialisasi:", err.message);
    setTimeout(startRabbitMQ, 5000);
  }
}

process.on("uncaughtException", (err) => console.error("🔥 Exception:", err.message));
process.on("unhandledRejection", (reason) => console.error("🔥 Rejection:", reason));

// Fly.io otomatis menyuntikkan port (biasanya 3000 atau 8080)
const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Server berjalan di port ${PORT}`);
  startRabbitMQ();
});