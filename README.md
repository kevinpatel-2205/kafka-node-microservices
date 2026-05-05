# 📦 Kafka Node.js Microservices

A simple **event-driven microservices system** built using **Apache Kafka** and **Node.js**.  
This project demonstrates how services communicate asynchronously using Kafka topics.

---

## 🚀 Architecture

```
Order Service → Kafka → Payment Service → Kafka → Notification Service
                         ↘ retry ↗
```

---

## 🧩 Services

### 1️⃣ Order Service
- Exposes REST API: `/order`
- Produces messages to Kafka topic: `order-created`

---

### 2️⃣ Payment Service
- Consumes from: `order-created`
- Simulates:
  - 5-second processing delay
  - Random failures
- Implements retry mechanism using `payment-retry` topic

**Produces:**
- `payment-success`
- `payment-retry`

---

### 3️⃣ Notification Service
- Consumes from: `payment-success`
- Simulates sending email/notification

---

## ⚙️ Tech Stack

- Node.js
- Apache Kafka
- KafkaJS
- Docker

---

## 🐳 Kafka Setup

### Start Kafka
```bash
docker compose up -d
```

### Verify Containers
```bash
docker ps
```

---

## ▶️ Run Services

Open **3 separate terminals**:

### Order Service
```bash
cd order-service
node index.js
```

### Payment Service
```bash
cd payment-service
node index.js
```

### Notification Service
```bash
cd notification-service
node index.js
```

---

## 🧪 Test API

### Single Request
```bash
curl -X POST http://localhost:3000/order \
-H "Content-Type: application/json" \
-d "{\"item\":\"phone\"}"
```

### Multiple Requests (Windows CMD)
```bash
for /L %i in (1,1,50) do curl -X POST http://localhost:3000/order -H "Content-Type: application/json" -d "{\"item\":\"product-%i\"}"
```

---

## 🔄 Flow Explanation

1. User hits `/order` API  
2. Order Service sends message → Kafka (`order-created`)  
3. Payment Service consumes message  
4. Simulates processing (5 seconds)  
5. If success → sends `payment-success`  
6. If failure → sends `payment-retry`  
7. Retry mechanism triggers with delay  
8. Notification Service consumes `payment-success`  

---

## 🔁 Retry Logic

- Retry Topic: `payment-retry`
- Retry Delay: **3 seconds**
- Max Retries: **3 attempts**
- After max retries → logs failure

---

## 📊 Kafka Concepts Used

### 🧵 Topics
- `order-created`
- `payment-success`
- `payment-retry`

---

### 📦 Partition
- Each topic stores messages in partitions  
- Current setup uses **1 partition**

---

### 📍 Offset
- Each message has a unique position (offset)  
- Used to track consumer progress

---

### 👥 Consumer Groups
- `payment-order-group`
- `payment-retry-group`
- `notification-group`
- `inventory-group`

---

## 🔍 Kafka UI (Optional)

### Run Kafka UI
```bash
docker run -d -p 8080:8080 \
-e KAFKA_CLUSTERS_0_NAME=local \
-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092 \
provectuslabs/kafka-ui
```

### Open Dashboard
```
http://localhost:8080
```

---

## 📈 What You Learn

- Event-driven architecture  
- Producer / Consumer model  
- Retry handling with Kafka  
- Consumer groups  
- Offsets & message tracking  
- Asynchronous processing  

---

## ⚠️ Notes

- Kafka is **not a traditional queue**
- Messages are **not deleted after consumption**
- Consumers track progress using **offsets**

---

## 🚀 Future Improvements

- Dead Letter Queue (`payment-failed`)
- Multiple partitions
- Horizontal scaling (multiple consumers)
- Monitoring (consumer lag, throughput)
- Schema validation (Avro / JSON Schema)

---

## 🧠 Summary

Kafka acts as a **central event bus**.  
Services communicate via events instead of direct API calls.

✅ Scalable  
✅ Fault-tolerant  
✅ Asynchronous  
