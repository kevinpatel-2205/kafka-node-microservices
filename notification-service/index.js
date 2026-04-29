const kafka = require("./kafka");

const consumer = kafka.consumer({ groupId: "notification-group" });

const start = async () => {
  console.log("🔵 [Notification Service] Starting...");

  await consumer.connect();
  console.log("🟢 [Notification Service] Kafka consumer connected");

  await consumer.subscribe({
    topic: "payment-success",
    fromBeginning: false,
  });

  console.log("👂 [Notification Service] Listening topic: payment-success");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("\n📥 [Notification Service] New Kafka message received");
      console.log("📌 Topic:", topic);
      console.log("📌 Partition:", partition);
      console.log("📌 Offset:", message.offset);

      const order = JSON.parse(message.value.toString());

      console.log("📦 [Notification Service] Order data:", order);
      console.log("✉️ [Notification Service] Preparing confirmation email...");

      console.log(
        `✅ [Notification Service] Email sent: Your order ${order.id} for ${order.item} is confirmed`,
      );

      console.log(
        "📦 [Notification Service] Flow: payment-success → notification\n",
      );
    },
  });
};

start();
