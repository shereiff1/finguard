<p align="center">
  <h1 align="center">FinGuard</h1>
  <p align="center">
    Real-time fraud detection pipeline built with Go, Python, Java, and Apache Kafka.
  </p>
</p>

---

## Overview

FinGuard is a microservices-based financial transaction pipeline that ingests transactions in real time, scores them for fraud using an ONNX machine-learning model, and settles them in a double-entry ledger — all orchestrated through Apache Kafka.

```
Client ──WebSocket──▶ Go Ingestion ──Kafka──▶ Python ML ──Kafka──▶ Java Ledger ──▶ PostgreSQL
```

## Architecture

| Service | Language | Role |
|---|---|---|
| **finguard-ingestion** | Go | WebSocket gateway — accepts streaming transactions and publishes them to Kafka (`transactions.raw`) |
| **finguard-ml** | Python (FastAPI) | Consumes raw transactions, runs ONNX fraud inference, enriches each event with a `fraud_score`, and publishes to Kafka (`transactions.evaluated`) |
| **finguard-ledger** | Java (Spring Boot) | Consumes evaluated transactions, performs double-entry balance transfers, freezes accounts on high fraud scores, and persists everything to PostgreSQL |

### Data Flow

```
┌─────────────┐       ┌──────────────────┐       ┌──────────────────┐       ┌──────────────┐
│   Client    │──WS──▶│  Go Ingestion    │──────▶│   Python ML      │──────▶│  Java Ledger │
│  (scripts)  │       │  :8080           │       │   :8000           │       │  :8081       │
└─────────────┘       └──────────────────┘       └──────────────────┘       └──────────────┘
                              │                          │                         │
                              ▼                          ▼                         ▼
                        transactions.raw         transactions.evaluated       PostgreSQL
                          (Kafka topic)            (Kafka topic)              (accounts,
                                                       │                   ledger_transactions,
                                                       ▼                    fraud_alerts)
                                                    Redis
                                                 (feature cache)
```

### Infrastructure

| Component | Purpose |
|---|---|
| **Apache Kafka** (KRaft) | Event streaming backbone between all services |
| **PostgreSQL** | Persistent storage for accounts, transactions, and fraud alerts |
| **Redis** | Feature cache for ML user profiles and velocity data |

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & Docker Compose

### Run

```bash
docker compose up --build
```

This starts all five containers:

| Container | Port |
|---|---|
| Go Ingestion | `localhost:8080` |
| ML Service | `localhost:8000` |
| Ledger Service | `localhost:8081` |
| Kafka | `localhost:9092` |
| PostgreSQL | `localhost:5432` |
| Redis | `localhost:6379` |


## Project Structure

```
finguard/
├── docker-compose.yml          # Full stack orchestration
├── init.sql                    # PostgreSQL seed data
│
├── finguard-ingestion/         # Go — WebSocket → Kafka producer
│   ├── cmd/server/main.go      # Entry point, graceful shutdown
│   ├── internal/
│   │   ├── domain/             # Transaction model, validation, sync.Pool
│   │   ├── transport/          # WebSocket upgrade & stream handler
│   │   └── worker/             # Fan-out worker pool → Kafka writer
│   ├── Dockerfile
│   └── go.mod
│
├── finguard-ml/                # Python — Kafka consumer → ONNX → Kafka producer
│   ├── app/
│   │   ├── main.py             # FastAPI lifespan, health endpoint
│   │   ├── config.py           # Pydantic settings from env vars
│   │   ├── models/             # ONNX model + training data (gitignored)
│   │   └── services/
│   │       ├── inference.py    # ONNX Runtime inference engine
│   │       ├── cache.py        # Redis feature cache
│   │       └── kafka_worker.py # Async Kafka consume → score → produce
│   ├── Dockerfile
│   └── requirements.txt
│
├── finguard-ledger/            # Java — Kafka consumer → PostgreSQL ledger
│   ├── src/main/java/com/finguard/ledger/
│   │   ├── config/             # Kafka consumer & Jackson configs
│   │   ├── consumer/           # @KafkaListener for transactions.evaluated
│   │   ├── domain/             # JPA entities (Account, LedgerTransaction, FraudAlert)
│   │   ├── dto/                # EvaluatedTransaction DTO
│   │   ├── repository/         # Spring Data JPA repositories
│   │   └── service/            # LedgerService (transfers), FraudHandlerService (freeze)
│   ├── Dockerfile
│   └── pom.xml
```

## How It Works

### 1. Transaction Ingestion (Go)

The Go service accepts live transaction streams over WebSocket connections. Each message is:

- Deserialized using [jsoniter](https://github.com/json-iterator/go) for speed
- Enriched with a server-generated `correlation_id` (UUID) and UTC timestamp
- Memory-managed via `sync.Pool` to eliminate GC pressure
- Dispatched to a 50-worker fan-out pool that publishes to Kafka asynchronously

Backpressure is handled with a 10,000-slot buffered channel — if the channel fills up, incoming messages are dropped with a logged alert rather than crashing the server.

### 2. Fraud Scoring (Python)

The ML service consumes from `transactions.raw` and for each event:

1. Looks up the sender's risk profile from Redis (historical risk score, transaction velocity)
2. Constructs a feature vector: `[amount, historical_risk_score, velocity_1h]`
3. Runs inference through a pre-trained ONNX model
4. Attaches the `fraud_score` (0.0 – 1.0) to the payload
5. Publishes the enriched event to `transactions.evaluated`

### 3. Settlement & Fraud Handling (Java)

The Spring Boot ledger service consumes scored transactions and:

- **Normal transactions** (`fraud_score ≤ 0.85`): Executes a double-entry balance transfer with pessimistic row locks to prevent race conditions
- **Fraudulent transactions** (`fraud_score > 0.85`): Rolls back the transaction, freezes the sender's account, and creates a `fraud_alert` record
- **Insufficient funds**: Rolls back with `ROLLED_BACK` status

All operations run inside a `@Transactional` boundary for atomicity.

## Environment Variables

### Go Ingestion

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `kafka:29092` | Kafka broker address |
| `KAFKA_TOPIC` | `transactions.raw` | Output topic |
| `WS_PORT` | `8080` | WebSocket server port |

### Python ML

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `REDIS_URL` | `redis://redis:6379` | Redis connection URL |
| `MODEL_PATH` | `app/models/fraud_model.onnx` | Path to ONNX model file |




