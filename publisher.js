require("dotenv").config();

const amqp = require("amqplib");

const QUEUE = "parking_queue";

async function publish() {
  const connection = await amqp.connect(
    process.env.RABBITMQ_URL
  );

  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE, {
    durable: true,
  });

  const message = {
    parking_id: "L2-A4",
    area_id: "MK",
    level_id: 2,
    zone_id: "kiri",
    slot_number: 4,
    is_filled: true
  };

  channel.sendToQueue(
    QUEUE,
    Buffer.from(JSON.stringify(message))
  );

  console.log("Message sent");

  setTimeout(() => {
    connection.close();
  }, 500);
}

publish();