const express = require("express");
const kafka = require("./kafka");
const { Partitioners } = require("kafkajs");

const app = express();
app.use(express.json());


const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

const start = async () => {
  console.log("🔵 [Order Service] Starting...");

  await producer.connect();
  console.log("🟢 [Order Service] Kafka Producer Connected");

  app.post("/order", async (req, res) => {
    console.log("\n📥 [Order Service] API called /order");

    const order = {
      id: Date.now(),
      item: req.body.item,
    };

    console.log("🧾 [Order Service] Created Order Object:", order);

    console.log(
      "📤 [Order Service] Sending message to Kafka topic: order-created",
    );

    await producer.send({
      topic: "order-created",
      messages: [{ value: JSON.stringify(order) }],
    });

    console.log("✅ [Order Service] Message sent to Kafka successfully");
    console.log("📦 [Order Service] Flow: API → Kafka → order-created\n");

    res.send({ status: "Order placed", order });
  });

  app.listen(3000, () => {
    console.log("🚀 [Order Service] Running on port 3000\n");
  });
};

start();
