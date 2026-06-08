require("dotenv").config();

const amqp = require("amqplib");
const { createClient } = require("@supabase/supabase-js");

// NAMA QUEUE DISAMAKAN DENGAN WEBSOCKET SERVER
const QUEUE = "parking_queue"; 

// Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

async function startWorker() {
  try {
    console.log("Connecting to RabbitMQ...");

    // Connect RabbitMQ
    const connection = await amqp.connect(
      process.env.RABBITMQ_URL
    );

    const channel = await connection.createChannel();

    // Create queue if not exists
    await channel.assertQueue(QUEUE, {
      durable: true,
    });

    console.log("Worker running...");
    console.log(`Waiting for messages in queue: ${QUEUE}`);

    // Consume queue
    channel.consume(QUEUE, async (msg) => {
      if (!msg) return;

      try {
        // Parse incoming message
        const data = JSON.parse(msg.content.toString());
        console.log("Incoming message slots count:", data.slots ? data.slots.length : 0);

        // Pastikan format JSON dari ESP32 benar (memiliki array 'slots')
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
            console.error("❌ Supabase error:", error);
          } else {
            console.log(`✅ Database updated successfully: ${payloadToDb.length} slots`);
          }
          
        } else {
          console.warn("⚠️ Invalid payload format. 'slots' array is missing.");
        }

        // Acknowledge message agar dihapus dari antrean RabbitMQ
        channel.ack(msg);

      } catch (err) {
        console.error("Worker processing error:", err);
        // Tetap acknowledge jika JSON corrupt agar antrean tidak macet
        channel.ack(msg); 
      }
    });

  } catch (err) {
    console.error("Connection error:", err);

    // Retry reconnect
    setTimeout(startWorker, 5000);
  }
}

// Start worker
startWorker();