require("dotenv").config();

const amqp = require("amqplib");
const { createClient } = require("@supabase/supabase-js");

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

        console.log("Incoming message:", data);

        // Insert / update database
        const { error } = await supabase
          .from("parking_slots")
          .upsert(
            {
              parking_id: data.parking_id,
              area_id: data.area_id,
              level_id: data.level_id,
              zone_id: data.zone_id,
              slot_number: data.slot_number,
              is_filled: data.is_filled,
              updated_at: new Date(),
            },
            {
              onConflict:
                "parking_id,area_id,level_id,zone_id,slot_number",
            }
          );

        if (error) {
          console.error("Supabase error:", error);
        } else {
          console.log("Database updated successfully");
        }

        // Acknowledge message
        channel.ack(msg);

      } catch (err) {
        console.error("Worker processing error:", err);
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