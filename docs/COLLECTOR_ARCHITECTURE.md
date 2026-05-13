# Collector Service — Architecture & Design

## Table of Contents

1. [Overview](#overview)
2. [Responsibility & Role in the Pipeline](#1-responsibility--role-in-the-pipeline)
3. [Consumption Strategy](#2-consumption-strategy)
4. [Consumer Identification](#3-consumer-identification)
5. [Processing Model](#4-processing-model)
6. [Data Validation & Conversion](#5-data-validation--conversion)
7. [Persistence Strategy](#6-persistence-strategy)
8. [Database Schema](#7-database-schema)
9. [Idempotency & Deduplication](#8-idempotency--deduplication)
10. [Acknowledgment Policy](#9-acknowledgment-policy)
11. [Concurrency Architecture](#10-concurrency-architecture)
12. [Readiness & Life Cycle Management](#11-readiness--life-cycle-management)
13. [Observability & Metrics](#12-observability--metrics)
14. [Configuration](#13-configuration)

---

## Overview

The Collector (Kubernetes service: `collector`) is the **consumer** service in the GPU Telemetry Pipeline. Its primary role is to pull telemetry data from the Message Queue, process/validate it, and persist it into a permanent database (Postgres).

**What it does:**
- Consumes batches of messages from the Message Queue using HTTP Long Polling.
- Performs "defense-in-depth" validation and data type conversion (strings to floats/timestamps).
- Performs high-efficiency bulk inserts into Postgres.
- Acknowledges successfully stored messages to the queue to clear them.
- Exposes health, readiness, and metrics for monitoring and cluster management.

**What it is NOT:**
- It is not a real-time agent — it relies on the Message Queue for buffering.
- It is not a dashboarding tool — it strictly handles the ingestion and storage path.

**Kubernetes service name:** `collector`

```
┌──────────────────────────────────────────────────┐
│                  Collector Pod                   │
│                                                  │
│  ┌────────────────────────┐      ┌────────────┐  │
│  │   Consumption Loop     │◄─────│    MQ      │  │
│  │ (Long Poll → Validate  │      │  Service   │  │
│  │  → Bulk SQL → ACK)     │─────►│            │  │
│  └────────────────────────┘      └────────────┘  │
│              │                                   │
│              ▼                                   │
│  ┌────────────────────────┐                      │   
│  │      Postgres          │                      │   
│  │     (Database)         │                      │   
│  └────────────────────────┘                      │   
│                                                  │
│  ┌──────────────────────────────────────────┐    │
│  │  Gin Server (:8082)                      │    │
│  │  GET /health  GET /ready  GET /metrics   │    │
│  │  GET /docs/index.html                 │    │
│  └──────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
```

---

## 1. Responsibility & Role in the Pipeline

The Collector acts as the bridge between the transient Message Queue and the permanent Database.

```
┌──────────┐    POST /messages    ┌───────────────┐   GET /messages/consume  ┌───────────┐   SQL INSERT  ┌──────────┐
│ Streamer │ ──────────────────► │ Message Queue │ ──────────────────────► │ Collector │ ────────────► │    DB    │
└──────────┘                     └───────────────┘                          └───────────┘               └──────────┘
```

The Collector ensures that once data enters the pipeline, it is safely stored in a queryable format. It provides the final reliability link by only clearing messages from the queue *after* they are confirmed to be written to disk in the database.

---

## 2. Consumption Strategy

### Long Polling with Batching
The Collector uses a **Pull-based** long polling strategy to retrieve messages from the Message Queue.

- **Mechanism**: `GET /messages/consume?long_poll_timeout=30s&batch_size=10&consumer_id=<ID>`.
- **Long Polling**: If the queue is empty, the connection stays open for up to 30 seconds waiting for new data. This minimizes network overhead and "tight loop" CPU usage when data is sparse.
- **Batching**: It pulls up to 10 messages at once. Processing messages in batches allows for highly efficient bulk database operations.

### Response Handling
The Collector handles each HTTP status code from the queue distinctly:
- **HTTP 200**: A batch of messages was received. Process and persist the batch.
- **HTTP 204 No Content**: The queue is empty and the long-poll timed out. This is a non-error state — the Collector immediately initiates a new poll request.
- **HTTP 4xx / 5xx**: A genuine error occurred. The Collector logs the error, applies a short backoff (default 2 seconds) to avoid hammering a failing service, and then retries.

---

## 3. Consumer Identification

Every Collector pod must identify itself to the Message Queue to manage leases effectively.

- **Identification**: The Collector sends a `consumer_id` with every request.
- **Generation Logic**: 
    1. It attempts to use the **OS Hostname** (which is the Pod Name in Kubernetes).
    2. If the hostname is unavailable, it generates a **random UUID** at startup.
- **Why**: This ensures that even if multiple Collector pods are running (Horizontal Pod Autoscaling), the Message Queue can track which pod "owns" which message lease.

---

## 4. Processing Model

### Sequential Processing
Within a single batch of 10 messages, the Collector processes records **sequentially**. 

- **Rationale**: Given that network calls to the DB and MQ are the primary bottlenecks, sequential processing within a batch keeps the internal logic simple and avoids the overhead of complex local worker pools. 
- **Scaling**: Scaling is achieved by increasing the number of Collector **Pods** in Kubernetes, rather than adding complex concurrency inside a single pod.

### Error Boundaries
If a database write fails for the entire batch, the Collector logs the error and **skips the Acknowledgment**. The Message Queue will eventually timeout the lease and redeliver the messages to another healthy pod.

---

## 5. Data Validation & Conversion

The Collector performs a "defense-in-depth" validation step before sending data to the database.

- **Type Conversion**: Telemetry data arrives in the queue as strings. The Collector explicitly converts:
    - `Value` string → `float64` (Double Precision in SQL).
    - `Timestamp` string → `time.Time` (TIMESTAMPTZ in SQL).
- **Partial Batch Success**: If an individual row in a batch fails validation or conversion, it is skipped and logged as a `validation_error`. The Collector continues processing the remaining valid messages in the batch.

---

## 6. Persistence Strategy

### Vanilla Postgres
The service uses standard **Postgres** for storage.
- **Driver**: `jackc/pgx/v5` with **pgxpool**.
- **Connection Management**: A connection pool is used to manage concurrent access and handle transient network hiccups with the database.

### Bulk Write Strategy
Instead of one SQL statement per message, the Collector constructs a single **Bulk INSERT** statement for each batch. This drastically reduces network round-trips to the database and improves overall pipeline throughput.

---

## 7. Database Schema

The Collector is self-sufficient and handles its own schema management.

- **Auto-Migration**: On startup, the Collector executes `CREATE TABLE IF NOT EXISTS` to ensure the storage table is ready.
- **Table Structure**:
    - `ts`: TIMESTAMPTZ (Primary Key component)
    - `hostname`: TEXT (Primary Key component)
    - `gpu_id`: TEXT (Primary Key component)
    - `metric_name`: TEXT (Primary Key component)
    - `value`: DOUBLE PRECISION
    - `device`, `uuid`, `model_name`, `labels_raw`: TEXT (Metadata)
    - `message_id`: TEXT (Assigned by the Queue)

All 10 fields (the 9 original telemetry fields plus the `message_id` assigned by the queue) are stored to ensure complete auditability and no data loss.

---

## 8. Idempotency & Deduplication

Data reliability in the pipeline relies on **At-Least-Once** delivery, which can occasionally lead to duplicates (e.g., if a Collector crashes after writing to the DB but before sending the ACK).

- **Handling**: The Collector uses a **Composite Primary Key** on `(ts, hostname, gpu_id, metric_name)`.
- **SQL Logic**: `INSERT ... ON CONFLICT DO NOTHING`.
- **Result**: If a duplicate message is redelivered, Postgres silently ignores the secondary insert, ensuring the analytical data remains clean.

---

## 9. Acknowledgment Policy

### Bulk Acknowledgment
Acknowledgments are only sent **after** the data has been successfully committed to the database.

- **Payload**: The Collector sends a list of `DeliveryID`s (not `MessageID`s) to `POST /messages/ack`. A `DeliveryID` is unique per delivery *attempt* — the same message redelivered after a lease expiry gets a new `DeliveryID`.
- **Strict Filtering**: Only the `DeliveryID`s that were successfully converted and stored in the database are included in the ACK payload. Messages that failed validation or type conversion are silently skipped, leaving their leases to expire and trigger redelivery by the queue.

---

## 10. Concurrency Architecture

The Collector utilizes a **3-Goroutine Model** to maintain high availability and responsiveness:

1.  **Main Goroutine**: Coordinates startup, configuration loading, and blocking on OS signals.
2.  **Health Server Goroutine**: Runs the Gin HTTP server for Kubernetes probes and metrics.
3.  **Consumption Loop Goroutine**: The core "engine" that handles polling, processing, and persistence.

---

## 11. Readiness & Life Cycle Management

### Readiness Protocol
The Collector manages its readiness flag precisely to avoid data drops:
- **Startup**: The `/ready` probe only returns `200 OK` after:
    1.  A successful connection to the Postgres pool is verified.
    2.  The Auto-Migration/Schema check is complete.
    3.  The consumption loop has successfully started.
- **Shutdown (SIGTERM)**: Upon receiving a termination signal, the readiness flag is flipped to `false` (503) immediately. This tells the load balancer/orchestrator to stop considering the pod as an active target while it drains its final batch.

### Graceful Shutdown
The service allows a configurable grace period (default 30s) to:
- Finish the current long-poll.
- Finish the current DB bulk insert.
- Send the final ACK.
- Close the DB pool cleanly.

---

## 12. Observability & Metrics

Consistent with the rest of the pipeline, the Collector exposes a **JSON Snapshot** metrics endpoint.

- **Endpoint**: `GET /metrics`.
- **Implementation**: Uses `sync/atomic` counters for lock-free performance.

The Collector also exposes `GET /docs/index.html`, which serves the interactive Swagger UI for the Collector's internal OpenAPI spec (generated by `make generate-openapi`).
- **Key Metrics**:
    - `messages_consumed_total`: Cumulative messages pulled.
    - `db_writes_success_total`: Batches successfully committed to SQL.
    - `db_writes_error_total`: Database failures (triggers redelivery).
    - `validation_errors_total`: Messages with corrupt formatting.
    - `last_db_write_timestamp`: Success indicator for data flow.

---

## 13. Configuration

Configured via environment variables with sane production defaults:

| Variable | Default | Purpose |
|---|---|---|
| `COLLECTOR_PORT` | `8082` | Health/Metrics server port |
| `COLLECTOR_QUEUE_URL` | `http://message-queue:8080` | MQ Service coordinate |
| `COLLECTOR_DATABASE_URL` | (Required) | Postgres connection DSN |
| `COLLECTOR_BATCH_SIZE` | `10` | Max messages per batch |
| `COLLECTOR_LONG_POLL_TIMEOUT`| `30s` | MQ waiting duration |
| `COLLECTOR_DB_MAX_CONNS` | `5` | Postgres pool size |
| `COLLECTOR_DB_CONNECT_TIMEOUT`| `10s` | Initial startup timeout |
| `COLLECTOR_REQUEST_TIMEOUT` | `10s` | Deadline for ACK calls |
| `COLLECTOR_SHUTDOWN_GRACE` | `30s` | Final flush timeout |
