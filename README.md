Here’s a clean README.md you can use for your Kafka project (only Kafka, no Redis).
📦 Kafka Node.js Microservices
A simple event-driven microservices system built using
Apache Kafka and Node.js.
🚀 Architecture
Order Service → Kafka → Payment Service → Kafka → Notification Service
                      ↘ retry ↗
🧩 Services1. Order Service
REST API (/order)
Produces messages to Kafka topic: order-created
2. Payment Service
Consumes from order-created
Simulates 5-second processing
Random failure simulation
Retry mechanism using payment-retry
Produces:
payment-success
payment-retry
3. Notification Service
Consumes from payment-success
Simulates sending email/notification
⚙️ Tech Stack
Node.js
Apache Kafka
KafkaJS
Docker
🐳 Kafka SetupStart Kafka
docker compose up -d
Check running containers
docker ps
▶️ Run Services
Open 3 terminals:
Order Service
cd order-service
node index.js
Payment Service
cd payment-service
node index.js
Notification Service
cd notification-service
node index.js
🧪 Test APISingle request
curl -X POST http://localhost:3000/order -H "Content-Type: application/json" -d "{\"item\":\"phone\"}"
Multiple requests (Windows CMD)
for /L %i in (1,1,50) do curl -X POST http://localhost:3000/order -H "Content-Type: application/json" -d "{\"item\":\"product-%i\"}"
🔄 Flow Explanation
1. User hits /order API
2. Order Service sends message → Kafka (order-created)
3. Payment Service consumes message
4. Waits 5 seconds (simulate processing)
5. If success → send payment-success
6. If fail → send payment-retry
7. Retry happens with delay
8. Notification Service consumes payment-success
🔁 Retry Logic
Retry topic: payment-retry
Retry delay: 3 seconds
Max retries: 3
After max retries → logs failure
📊 Kafka Concepts UsedTopic
order-created
payment-success
payment-retry
Partition
Each topic stores messages in partitions
Your setup uses 1 partition
Offset
Each message has a position (offset)
Used to track consumption progress
Consumer Group
payment-order-group
payment-retry-group
notification-group
inventory-group
🔍 Kafka UI (Optional)
Run Kafka UI:
docker run -d -p 8080:8080 -e KAFKA_CLUSTERS_0_NAME=local -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092 provectuslabs/kafka-ui
Open:
http://localhost:8080
📈 What You Learn
Event-driven architecture
Producer / Consumer model
Retry handling with Kafka
Consumer groups
Offsets and message tracking
Async processing
⚠️ Notes
Kafka is not a traditional queue
Messages are not deleted after consumption
Consumers track progress using offsets
🚀 Future Improvements
Dead Letter Queue (payment-failed)
Multiple partitions
Horizontal scaling (multiple consumers)
Monitoring (lag, throughput)
Schema validation (Avro / JSON Schema)
🧠 Summary
Kafka acts as a central event bus.
Services communicate via events instead of direct calls.
This makes the system scalable, fault-tolerant, and asynchronous.
If you want, next I can:
convert this into a GitHub-ready README with badges
or add real production patterns (DLQ, scaling, partitions)
