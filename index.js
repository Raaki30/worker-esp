require("dotenv").config();

const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const amqp = require("amqplib");
const { createClient } = require("@supabase/supabase-js");

// Nama antrean RabbitMQ
const QUEUE = "parking_queue"; 

// Inisialisasi Express & WebSocket Server (Agar Railway meng-expose port)
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

async function startService() {
  try {
    console.log("Connecting to RabbitMQ...");

    // 1. Connect RabbitMQ
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    const channel = await connection.createChannel();

    // Create queue if not exists
    await channel.assertQueue(QUEUE, { durable: true });
    console.log("✅ RabbitMQ Connected!");

    // ======================================================
    // BAGIAN 1: WEBSOCKET SERVER (MENERIMA DARI ESP32)
    // ======================================================
    wss.on("connection", (ws, req) => {
      console.log(`✅ [WebSocket] ESP32 Connected from ${req.socket.remoteAddress}`);

      ws.on("message", (message) => {
        try {
          const data = JSON.parse(message.toString());
          console.log(`📥 [WS] Data received from ESP32. Slots: ${data.slots ? data.slots.length : 0}`);

          // Publish data mentah dari ESP32 langsung ke RabbitMQ
          channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(data)), {
            persistent: true,
          });
          console.log("📤 [RabbitMQ] Data pushed to queue.");

        } catch (err) {
          console.error("❌ [WebSocket] Invalid JSON from ESP32:", err.message);
        }
      });

      ws.on("close", () => {
        console.log("❌ [WebSocket] ESP32 Disconnected");
      });
      
      ws.on("error", (error) => {
        console.error("⚠️ [WebSocket] Error:", error);
      });
    });

    // ======================================================
    // BAGIAN 2: WORKER (MENGAMBIL DARI RABBITMQ KE SUPABASE)
    // ======================================================
    console.log(`👷 Worker is waiting for messages in queue: ${QUEUE}...`);
    channel.consume(QUEUE, async (msg) => {
      if (!msg) return;

      try {
        const data = JSON.parse(msg.content.toString());
        
        if (data.slots && Array.isArray(data.slots)) {
          // Mapping data array untuk Bulk Upsert
          const payloadToDb = data.slots.map((slot) => ({
            parking_id: slot.parking_id,
            area_id: slot.area_id,
            level_id: slot.level_id,
            zone_id: slot.zone_id,
            slot_number: slot.slot_number,
            is_filled: slot.is_filled,
            updated_at: new Date(),
          }));

          // Insert / update database sekaligus (Bulk Upsert)
          const { error } = await supabase
            .from("parking_slots")
            .upsert(payloadToDb, {
              onConflict: "parking_id,area_id,level_id,zone_id,slot_number",
            });

          if (error) {
            console.error("❌ [Supabase] Insert Error:", error);
          } else {
            console.log(`✅ [Supabase] Database updated successfully: ${payloadToDb.length} slots`);
          }
        } else {
          console.warn("⚠️ Invalid payload format. 'slots' array is missing.");
        }

        // Acknowledge message agar dihapus dari antrean
        channel.ack(msg);

      } catch (err) {
        console.error("❌ [Worker] Processing error:", err);
        channel.ack(msg); // Tetap ack jika error agar antrean tidak macet/looping
      }
    });

    // ======================================================
    // BAGIAN 3: START HTTP SERVER (UNTUK PORT RAILWAY)
    // ======================================================
    // Health Check Endpoint (Penting agar Railway tidak mematikan service)
    app.get("/", (req, res) => {
      res.send("Smart Parking WebSockets & Worker Service is Running!");
    });

    const PORT = process.env.PORT || 3000;
    server.listen(PORT, "0.0.0.0", () => {
      console.log(`🚀 Server and WebSocket listening on port ${PORT}`);
    });

  } catch (err) {
    console.error("🔥 Critical Error:", err);
    // Retry reconnect setelah 5 detik
    setTimeout(startService, 5000);
  }
}

// Jalankan Service
startService();