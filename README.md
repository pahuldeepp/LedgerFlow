Perfect — here is a **clean, copy-paste ready README** and a **professional commit message** you can use immediately.

---

# ✅ README.md (Copy Everything Below)

````markdown
# LedgerFlow

LedgerFlow is a production-style financial ledger service built in Go.

It demonstrates real-world distributed systems patterns including:

- Double-entry accounting
- Idempotent transaction handling
- Transactional Outbox pattern
- Kafka event publishing
- Background worker with retries + DLQ
- Prometheus metrics
- Dockerized infrastructure (Postgres + Kafka)

---

## 🏗 Architecture Overview

Client  
  ↓  
REST API (Gin)  
  ↓  
PostgreSQL (ACID Transaction)  
  ↓  
Outbox Table (same transaction)  
  ↓  
Background Worker  
  ↓  
Kafka Topic (`ledger.events`)  
  ↓  
Downstream Consumers  

This ensures:

- No lost events
- Strong consistency
- Reliable event publishing
- Observability via metrics

---

## ⚙️ Tech Stack

- Go (Gin)
- PostgreSQL
- Kafka (Confluent)
- Prometheus
- Docker Compose

---

## 🚀 Running Locally

### 1. Start PostgreSQL

```bash
docker compose up -d
````

### 2. Start Kafka

```bash
docker compose -f docker-compose.kafka.yml up -d
```

### 3. Run the API

```bash
cd services/ledger-go
go run .
```

Server runs at:

```
http://localhost:8081
```

---

## 📌 API Endpoints

### Health Check

```
GET /health
```

---

### Create Transaction

```
POST /transactions
```

Example request:

```json
{
  "idempotency_key": "tx-1001",
  "entries": [
    { "account_id": "cash", "direction": "debit", "amount": 100 },
    { "account_id": "revenue", "direction": "credit", "amount": 100 }
  ]
}
```

Rules enforced:

* At least 2 entries required
* Debits must equal credits
* Idempotency key prevents duplicate processing

---

### Get Account Balance

```
GET /accounts/{id}/balance
```

---

### Metrics

```
GET /metrics
```

Exposes Prometheus metrics including:

* `outbox_pending_total`
* `outbox_published_total`
* `outbox_publish_failed_total`
* `outbox_dlq_total`

---

## 🔥 Key Concepts Implemented

### Double Entry Accounting

Every transaction must balance:

* Total Debits = Total Credits
* Executed inside a single DB transaction

### Idempotency

* Unique `idempotency_key`
* Prevents duplicate financial operations
* Safe retry behavior

### Transactional Outbox Pattern

* Events written to `outbox` table inside same DB transaction
* Background worker publishes to Kafka
* Guarantees no lost events

### Retry + Dead Letter Queue

* Failed publishes increment attempt counter
* After max attempts → moved to DLQ
* Fully observable via metrics

### Observability

* Prometheus metrics
* Go runtime metrics
* Publish success/failure counters

---

## 📊 Production Concepts Demonstrated

* ACID guarantees
* Event-driven architecture
* Exactly-once semantic simulation
* Resilience patterns
* Background processing
* Observability
* Durable message publishing

---

## 📌 Author

Built as a distributed systems and event-driven architecture learning project.

```

---

```
