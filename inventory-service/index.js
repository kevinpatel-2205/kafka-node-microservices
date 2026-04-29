const kafka = require("./kafka");

const consumer = kafka.consumer({ groupId: "inventory-group" });

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const start = async () => {
  console.log("🔵 [Inventory Service] Starting...");

  await consumer.connect();
  console.log("🟢 [Inventory Service] Kafka consumer connected");

  await consumer.subscribe({
    topic: "order-created",
    fromBeginning: false,
  });

  console.log("👂 [Inventory Service] Listening topic: order-created");
  console.log("👥 [Inventory Service] Consumer Group: inventory-group");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("\n📥 [Inventory Service] New Kafka message received");
      console.log("📌 Topic:", topic);
      console.log("📌 Partition:", partition);
      console.log("📌 Offset:", message.offset);

      const order = JSON.parse(message.value.toString());

      console.log("📦 [Inventory Service] Order data:", order);
      console.log("🏬 [Inventory Service] Checking stock...");
      await wait(3000);

      console.log(`✅ [Inventory Service] Stock reserved for ${order.item}`);
      console.log(
        "📦 [Inventory Service] Flow: order-created → inventory reserved\n",
      );
    },
  });
};

start();
