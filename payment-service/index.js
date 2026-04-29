const { Partitioners } = require("kafkajs");
const kafka = require("./kafka");

const orderConsumer = kafka.consumer({ groupId: "payment-order-group" });
const retryConsumer = kafka.consumer({ groupId: "payment-retry-group" });

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function processPayment(order, sourceTopic) {
  console.log("\n💳 [Payment Service] Payment processing started");
  console.log("📍 [Payment Service] Source topic:", sourceTopic);
  console.log("📦 [Payment Service] Order data:", order);

  console.log("⏳ [Payment Service] Simulating payment work for 5 seconds...");
  await wait(5000);

  const failed = Math.random() < 0.5;
  console.log(
    "🎲 [Payment Service] Random failure result:",
    failed ? "FAILED" : "SUCCESS",
  );

  if (failed) {
    throw new Error("Payment failed randomly");
  }

  console.log("✅ [Payment Service] Payment successful");
  console.log("📤 [Payment Service] Sending message to topic: payment-success");

  await producer.send({
    topic: "payment-success",
    messages: [{ value: JSON.stringify(order) }],
  });

  console.log("📨 [Payment Service] payment-success event published");
}

async function sendToRetry(order) {
  const retryCount = order.retryCount || 0;

  console.log("🔁 [Payment Service] Current retry count:", retryCount);

  if (retryCount >= 3) {
    console.log("❌ [Payment Service] Max retries reached");
    console.log("🛑 [Payment Service] Payment permanently failed:", order);
    return;
  }

  const retryOrder = {
    ...order,
    retryCount: retryCount + 1,
  };

  console.log("⏲️ [Payment Service] Waiting 3 seconds before retry...");
  await wait(3000);

  console.log("📤 [Payment Service] Sending message to topic: payment-retry");
  console.log("🔁 [Payment Service] New retry count:", retryOrder.retryCount);

  await producer.send({
    topic: "payment-retry",
    messages: [{ value: JSON.stringify(retryOrder) }],
  });

  console.log("📨 [Payment Service] payment-retry event published");
}

const start = async () => {
  console.log("🔵 [Payment Service] Starting...");

  await orderConsumer.connect();
  console.log("🟢 [Payment Service] order-created consumer connected");

  await retryConsumer.connect();
  console.log("🟢 [Payment Service] payment-retry consumer connected");

  await producer.connect();
  console.log("🟢 [Payment Service] Kafka producer connected");

  await orderConsumer.subscribe({
    topic: "order-created",
    fromBeginning: false,
  });

  console.log("👂 [Payment Service] Listening topic: order-created");

  await retryConsumer.subscribe({
    topic: "payment-retry",
    fromBeginning: true,
  });

  console.log("👂 [Payment Service] Listening topic: payment-retry");

  await orderConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("\n📥 [Payment Service] New Kafka message received");
      console.log("📌 Topic:", topic);
      console.log("📌 Partition:", partition);
      console.log("📌 Offset:", message.offset);

      const order = JSON.parse(message.value.toString());

      try {
        await processPayment(order, topic);
      } catch (error) {
        console.log("⚠️ [Payment Service] Error:", error.message);
        await sendToRetry(order);
      }
    },
  });

  await retryConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("\n📥 [Payment Service] Retry Kafka message received");
      console.log("📌 Topic:", topic);
      console.log("📌 Partition:", partition);
      console.log("📌 Offset:", message.offset);

      const order = JSON.parse(message.value.toString());

      try {
        await processPayment(order, topic);
      } catch (error) {
        console.log("⚠️ [Payment Service] Retry failed:", error.message);
        await sendToRetry(order);
      }
    },
  });
};

start();
