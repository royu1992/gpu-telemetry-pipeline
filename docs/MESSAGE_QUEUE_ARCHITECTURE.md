# Message Queue Service — Architecture & Design

## Table of Contents

1. [Overview](#overview)
2. [Queue Topology](#1-queue-topology)
3. [Delivery Semantics](#2-delivery-semantics)
4. [Consumer Coordination](#3-consumer-coordination)
5. [Backpressure](#4-backpressure)
6. [Service Discovery](#5-service-discovery)
7. [Communication Protocol](#6-communication-protocol)
8. [Message Ordering](#7-message-ordering)
9. [Message Serialization](#8-message-serialization)
10. [Graceful Shutdown](#9-graceful-shutdown)
11. [Health & Readiness](#10-health--readiness)
12. [Observability](#11-observability)
13. [Security](#12-security)
14. [API Reference](#13-api-reference)
15. [Configuration](#14-configuration)
16. [Concurrency Model](#15-concurrency-model)
17. [Scaling](#16-scaling)

---

## Overview

The message queue is the central nervous system of the GPU telemetry pipeline. It decouples the **Telemetry Streamers** (producers) from the **Telemetry Collectors** (consumers), allowing each side to scale and operate independently.

**What it does:**
- Accepts telemetry messages from one or more Streamer pods
- Buffers messages in memory using a ring buffer
- Delivers messages to one or more Collector pods on demand
- Guarantees that every message is delivered at least once, even if a Collector crashes mid-processing
- Applies backpressure to Streamers when Collectors fall behind

**What it is NOT:**
- It is not Kafka, RabbitMQ, or any existing message broker
- It is a custom implementation purpose-built for this pipeline
- It does not persist messages to disk — it is an in-memory buffer

**Kubernetes service name:** `message-queue`

```
┌─────────────┐     POST /messages      ┌───────────────────┐     GET /messages/consume     ┌─────────────┐
│  Streamer 1 │ ──────────────────────► │                   │ ◄──────────────────────────── │ Collector 1 │
│  Streamer 2 │ ──────────────────────► │   message-queue   │ ◄──────────────────────────── │ Collector 2 │
│  Streamer N │ ──────────────────────► │                   │ ◄──────────────────────────── │ Collector N │
└─────────────┘                         └───────────────────┘                               └─────────────┘
```

---

## 1. Queue Topology

### Design Decision: Single Shared Ring Buffer

The queue uses a **single shared ring buffer** — one contiguous block of memory, allocated once at startup, shared by all producers and consumers.

All Streamers write to the same buffer. All Collectors read from the same buffer. There is no partitioning, no topics, no sharding.

### What is a Ring Buffer?

A ring buffer is a fixed-size array where the end wraps around to the beginning, forming a logical circle. Two pointers track state:

- **`head`** — the next position to write a new message
- **`tail`** — the next position to read a message

```
Capacity = 8 slots

Empty:
[ ][ ][ ][ ][ ][ ][ ][ ]
 ^
head = tail = 0

After writing A, B, C:
[A][B][C][ ][ ][ ][ ][ ]
          ^
         head=3, tail=0

After reading A, B:
[A][B][C][ ][ ][ ][ ][ ]
       ^  ^
    tail=2  head=3

Buffer wraps around after reaching the end:
[H][I][C][D][E][F][G][ ]
    ^  ^
  head=2 (wrapped)  tail=2
```

Key properties:
- **Fixed memory** — allocated once, never grows, no garbage collection pressure
- **O(1) reads and writes** — just increment a pointer modulo capacity
- **Full condition** — `(head + 1) % capacity == tail`
- **Empty condition** — `head == tail`

### Why Not a Go Channel or a Slice?

| | Go Channel | Slice Queue | Ring Buffer |
|---|---|---|---|
| Thread-safe | Yes (built-in) | Manual | Manual |
| Bounded memory | Yes (fixed cap) | No — grows forever | Yes (fixed cap) |
| GC pressure | Low | High (frequent allocs) | None |
| Backpressure | Yes | No | Yes |
| Inspectable (depth, metrics) | Limited | Yes | Yes |
| Ack / redelivery support | No | Possible | Yes |
| Exposable over network | No | Yes | Yes |

A Go channel cannot be exposed over a network to other pods — the queue must be a networked service. At that point, a ring buffer is the natural internal storage model: bounded, inspectable, and ack-friendly.

A plain slice queue is dangerous in a streaming system because it grows unboundedly when consumers fall behind, risking OOM crashes.

---

## 2. Delivery Semantics

### Design Decision: At-Least-Once with Lease-Based Redelivery

The question delivery semantics answers is: **what happens to a message if the Collector crashes before it finishes processing?**

There are three standard models:

| Model | Behaviour | Verdict |
|---|---|---|
| At-most-once | Message removed immediately on delivery. Lost if consumer crashes. | Unacceptable — silent data loss |
| At-least-once | Message redelivered if not acknowledged in time. May be processed twice. | Correct choice |
| Exactly-once | Every message processed exactly once, guaranteed. | Requires distributed transactions — out of scope |

### How Lease-Based Redelivery Works

When a Collector receives a message, the queue does **not** remove it immediately. Instead, it stamps the message with a **lease** — a deadline by which the Collector must send an acknowledgment.

```
t=0s    Queue delivers M1 to Collector-1
        Lease deadline: t=30s
        Message status: IN_FLIGHT

t=5s    Collector-1 crashes 💥

t=30s   Lease expires. No ack received.
        Queue resets M1 status: PENDING
        M1 is now available for redelivery.

t=31s   Collector-2 polls. Gets M1 with a fresh lease.
        Lease deadline: t=61s

t=35s   Collector-2 persists M1, sends ACK ✓
        Queue frees the slot. M1 done. ✅
```

A background **lease reaper goroutine** runs on a configurable interval (default every 5 seconds) and scans for expired in-flight messages, resetting them to PENDING.

### Handling Duplicate Delivery

If a Collector is very slow (not crashed), it might persist the message and then send an ack — but the lease already expired and the message was redelivered to another Collector. The same message gets written to the database twice.

This is the "at-least-once" scenario. It is handled at the database layer with an **upsert**:

```sql
INSERT INTO telemetry (uuid, timestamp, metric_name, value, ...)
VALUES (...)
ON CONFLICT (uuid, timestamp, metric_name) DO NOTHING;
```

Writing the same telemetry row twice produces the same result as writing it once. No data corruption, no error.

### Handling Persistently Failing Messages

If a message fails delivery `MAX_DELIVERY_ATTEMPTS` times (default 3), it is **dropped and logged**. There is no dead-letter queue — for a CSV-sourced simulation pipeline, poison pill messages are extremely unlikely, and the added complexity of a DLQ is not justified. The drop is recorded in the metrics counter `total_dropped` and logged at ERROR level with the full message content for post-mortem inspection.

### Slot State Machine

Each slot in the ring buffer moves through a defined set of states. Understanding this state machine is essential for implementing the queue correctly.

```
                  Streamer publishes
  EMPTY ──────────────────────────────► PENDING
    ▲                                      │
    │                                      │ Collector consumes
    │                                      ▼
    │                                  IN_FLIGHT
    │                                  │       │
    │              ack received         │       │ lease expired
    └───────────────────────────────────┘       │ (redelivery)
    ▲                                           │
    │                                           ▼
    │  slot freed after logging             PENDING  (requeued)
    └──────────────────────────────────────────┘
    ▲
    │  max delivery attempts exceeded
    └── IN_FLIGHT ──► DROPPED ──► EMPTY
```

| Transition | Trigger | Result |
|---|---|---|
| `EMPTY → PENDING` | Streamer publishes a message | Slot holds new message, head advances |
| `PENDING → IN_FLIGHT` | Collector consumes the slot | `delivery_id` and `lease_expires` stamped, attempt count incremented |
| `IN_FLIGHT → PENDING` | Lease reaper finds expired lease | `delivery_id` cleared, slot available for redelivery |
| `IN_FLIGHT → EMPTY` | Collector sends valid ack | Slot cleared, `canPublish` signalled, tail advances |
| `IN_FLIGHT → DROPPED` | Delivery attempt count exceeds `MAX_DELIVERY_ATTEMPTS` | Message logged at ERROR, slot freed |
| `DROPPED → EMPTY` | Immediate after logging | Slot available for new messages |

---

## 3. Consumer Coordination

### Design Decision: Pull-Based with Server-Side Locking

With up to 10 Collector pods all requesting messages concurrently, the queue must ensure each message goes to exactly one Collector.

### How It Works

Collectors send a `GET /messages/consume` request to the queue. The queue's dispatch logic is protected by a **mutex** — finding and marking a batch of messages as `IN_FLIGHT` is one atomic operation. No two Collectors can receive the same message.

```
Collector-1 ──► GET /messages/consume ─┐
Collector-2 ──► GET /messages/consume ─┤──► Dispatch mutex acquired
Collector-3 ──► GET /messages/consume ─┘    One batch assigned atomically

Result:
  Collector-1 ◄── [M1, M2, M3]  (lease until t+30s)
  Collector-2 ◄── [M4, M5, M6]  (lease until t+30s)
  Collector-3 ◄── [M7, M8, M9]  (lease until t+30s)
```

### Why Pull-Based?

Pull-based (Collectors ask for work) is simpler and more resilient than push-based (queue pushes to Collectors):

- **Adding a new Collector** — it just starts polling. Zero reconfiguration.
- **Removing a Collector** — it stops polling. Its in-flight messages expire and are redelivered automatically.
- **Slow Collector** — it polls less frequently. Fast Collectors naturally get more work.
- **Queue has no health management burden** — it does not track which Collectors are alive.

Collectors are **fully stateless**. Kubernetes can scale them up or down freely with no coordination protocol between them.

### Alternative Considered: Partition Assignment (Kafka-style)

Partitions assigned to specific Collectors would enable true parallelism without a central dispatch bottleneck. However, this requires a partition assignment protocol, a leader election mechanism for rebalancing, and complex handling of scale events. At 10 Collectors maximum, the dispatch mutex is not a bottleneck, and this complexity is not justified.

---

## 4. Backpressure

### Design Decision: Blocking Publish on Full Buffer

Backpressure is the mechanism by which a system signals overload upstream, forcing producers to slow down rather than dropping data or crashing.

### The Problem Without Backpressure

If Collectors fall behind and the queue fills up, one of two things happens without backpressure:
- The queue grows forever → OOM crash
- New messages are silently dropped → data loss

Neither is acceptable.

### How Backpressure Works

The ring buffer is fixed in size. When it is full, the `POST /messages` handler **blocks** — it waits using `sync.Cond.Wait()` until a slot becomes available. The Streamer's HTTP request hangs until the queue accepts the message.

This naturally throttles the entire pipeline:

```
Timeline:

t=0   Buffer: [M1][M2][M3][M4][M5]  ← FULL (capacity=5)
t=0   Streamer attempts POST /messages → request blocks at server
t=1   Collector acks M1 → slot freed, sync.Cond signals
t=1   Streamer's blocked request unblocks → M6 written
t=1   POST /messages returns 201 to Streamer
```

### Streamer Behaviour Under Backpressure

The blocking has a configurable timeout. If the queue does not free a slot within the timeout, the server returns `429 Too Many Requests`. The Streamer then retries with **exponential backoff**:

```
attempt 1: wait 1s,  retry
attempt 2: wait 2s,  retry
attempt 3: wait 4s,  retry
attempt 4: wait 8s,  retry
...
max backoff: 30s
```

Only after exhausting retries does the Streamer log a warning and move on. This ensures no data is lost under normal overload — it is only dropped under extreme, sustained overload.

### Buffer Sizing

The buffer capacity is the key tuning parameter. A good rule of thumb:

> Buffer should hold approximately 30–60 seconds of message flow at peak rate.

At 10 Streamers each emitting one message per second, that is 300–600 messages per minute. A default capacity of **10,000 slots** provides a comfortable buffer. Each slot holds one telemetry row (~500 bytes of JSON), so 10,000 slots ≈ 5MB of memory.

Capacity is configurable via `QUEUE_CAPACITY` environment variable.

---

## 5. Service Discovery

### Design Decision: Kubernetes DNS with Environment Variable Configuration

Kubernetes provides built-in DNS-based service discovery. Every Service object gets a stable DNS name resolvable by all pods in the cluster.

### How It Works

The queue is deployed as a Kubernetes Service named `message-queue`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: message-queue
  namespace: telemetry
spec:
  selector:
    app: message-queue
  ports:
    - port: 8080
```

Kubernetes automatically creates the DNS entry:

```
message-queue.telemetry.svc.cluster.local:8080
```

Streamers and Collectors read the queue address from a single environment variable:

```go
queueAddr := os.Getenv("QUEUE_ADDR")
// "message-queue.telemetry.svc.cluster.local:8080"
```

The Helm chart sets this automatically in the Streamer and Collector Deployment templates. No hardcoded addresses anywhere.

### Queue Pod Restarts

When Kubernetes restarts the queue pod (crash, rolling update, node eviction), the DNS name remains stable. Clients experience a brief connection error window and must handle it with retry logic:

```
Streamer/Collector on connection error:
  attempt 1: wait 1s,  retry
  attempt 2: wait 2s,  retry
  attempt 3: wait 4s,  retry
  ...
  max backoff: 30s
```

The in-memory ring buffer is lost on restart. Streamers loop the CSV continuously — they will re-emit any messages that were in the buffer at crash time. This is acceptable given the simulation nature of the system.

---

## 6. Communication Protocol

### Design Decision: REST HTTP with Long Polling and Batching

#### Why Not gRPC?

gRPC with server-side streaming would eliminate polling entirely — the queue pushes messages down a persistent stream to each Collector. However:

- Requires `.proto` files and `protoc` code generation in the build pipeline
- Binary protocol — not debuggable with curl
- More complex test setup (mock gRPC servers)
- The efficiency gains are negligible at 10 Collectors maximum

#### Why Not Plain REST Polling?

Plain polling (Collector repeatedly hits `GET /messages/consume`, gets 204 when empty) wastes requests and adds latency:

```
Collector → GET /consume → 204 No Content  (queue empty)
Collector → GET /consume → 204 No Content  (still empty)
Collector → GET /consume → 204 No Content  (still empty)
Collector → GET /consume → 200 {message}   (finally, 3 wasted round trips)
```

#### Long Polling

With long polling, the server **holds the connection open** until a message arrives or the `long_poll_timeout` elapses. The Collector gets messages the instant they become available, with zero wasted requests:

```
Collector → GET /messages/consume?long_poll_timeout=30s

            [server blocks — queue is empty]
            [message arrives at t=4s]

Collector ← 200 { messages: [...] }   (responded after 4s)

Collector → GET /messages/consume?long_poll_timeout=30s  (immediately re-requests)
```

#### Batching

Instead of one message per request, the server returns up to `batch_size` messages. The Collector processes the batch and sends a single bulk acknowledgment:

```
GET /messages/consume?batch_size=10&long_poll_timeout=30s

→ Returns up to 10 messages in one response
→ Collector processes all 10
→ POST /messages/ack with all 10 delivery_ids
→ One round trip for 10 messages
```

Batching reduces HTTP overhead by an order of magnitude and enables bulk DB inserts on the Collector side.

---

## 7. Message Ordering

### Design Decision: No Global Ordering Guarantee

With multiple Streamers writing concurrently to the same ring buffer, strict global message ordering cannot be guaranteed. Streamer-1 and Streamer-2 interleave their writes non-deterministically.

This is **intentional and acceptable** because:

- The `timestamp` field in each telemetry record is the source-of-truth for time ordering
- The API Gateway sorts results with `ORDER BY timestamp` at query time
- The queue's job is delivery, not ordering

This design decision is documented here as a known, conscious tradeoff.

---

## 8. Message Serialization

### Design Decision: JSON over HTTP

All request and response bodies use `application/json`. JSON is human-readable, universally supported, and requires no code generation tooling.

### Telemetry Message Schema

This schema is the wire format for messages flowing through the queue. It maps directly to the CSV columns:

```json
{
  "message_id":  "550e8400-e29b-41d4-a716-446655440000",
  "timestamp":   "2025-07-18T20:42:34Z",
  "metric_name": "DCGM_FI_DEV_GPU_UTIL",
  "gpu_id":      "0",
  "device":      "nvidia0",
  "uuid":        "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
  "model_name":  "NVIDIA H100 80GB HBM3",
  "hostname":    "mtv5-dgx1-hgpu-031",
  "value":       "98",
  "labels_raw":  "DCGM_FI_DRIVER_VERSION=\"535.129.03\",..."
}
```

**Important:** `message_id` is generated by the queue service (UUID v4), not by the Streamer. The Streamer only provides the telemetry payload.

**`delivery_id` vs `message_id`:**
- `message_id` — identifies the message content. Stable across redeliveries.
- `delivery_id` — identifies a specific delivery attempt. Changes on each redelivery. This is the token used for acknowledgment.

---

## 9. Graceful Shutdown

### Design Decision: SIGTERM Handler with Connection Draining

Kubernetes sends `SIGTERM` to a pod before terminating it (rolling update, scale-down, node drain). Without handling this signal, the pod dies instantly — in-flight messages are lost and connected clients receive hard connection drops.

### Shutdown Sequence

```
SIGTERM received
    │
    ▼
1. Readiness probe begins returning 503
   (Kubernetes stops routing new traffic to this pod)
    │
    ▼
2. Stop accepting new POST /messages requests
   (return 503 Service Unavailable to new publishers)
    │
    ▼
3. Allow in-flight GET /messages/consume and POST /messages/ack
   requests to complete
    │
    ▼
4. Wait up to QUEUE_SHUTDOWN_GRACE_PERIOD for in-flight
   leases to be acknowledged
    │
    ▼
5. Close all connections cleanly
    │
    ▼
6. Exit with code 0
```

The liveness probe (`GET /health`) continues returning `200` throughout the drain period. This prevents Kubernetes from restarting the pod while it is gracefully shutting down.

Implementation uses Go's `signal.NotifyContext` to capture `SIGTERM` and `SIGINT`, and Go's `http.Server.Shutdown(ctx)` for connection draining.

---

## 10. Health & Readiness

Two probe endpoints are required for correct Kubernetes lifecycle management.

### `GET /health` — Liveness Probe

Kubernetes calls this to decide whether to **restart** the pod.

- Returns `200 OK` if the process is alive and functioning normally
- Returns `500` only if something is critically broken (e.g., background goroutines have panicked)
- Must be **very cheap** — no locks, no queue inspection, just return 200
- A full queue is NOT a reason to return unhealthy — it is expected behaviour

```json
200 OK
{ "status": "ok" }
```

### `GET /ready` — Readiness Probe

Kubernetes calls this to decide whether to **send traffic** to the pod.

- Returns `200 OK` when the ring buffer is initialised and the pod is ready to serve requests
- Returns `503 Service Unavailable` during startup (initialising) and during graceful shutdown (draining)

```json
200 OK
{
  "status": "ready",
  "queue_depth": 342,
  "capacity":    10000
}
```

```json
503 Service Unavailable
{
  "status":  "not ready",
  "reason":  "shutting down"
}
```

### `GET /docs/index.html` — Interactive API Documentation

Serves the Swagger UI browser interface for the Message Queue's internal OpenAPI spec. Generated by `make generate-openapi` and embedded via the `openapi_message_queue` swaggo instance.

Useful for exploring and manually testing the Message Queue API during development.
```

**Why two separate probes?**

During graceful shutdown, readiness returns `503` (stops new traffic) while liveness stays `200` (prevents premature restart). This ensures clean draining without Kubernetes interrupting the process.

---

## 11. Observability

### `GET /metrics` — Queue Statistics

Exposes internal queue state as JSON. Useful for debugging, dashboards, and capacity planning.

```json
200 OK
{
  "queue_depth":       342,
  "capacity":          10000,
  "in_flight":         18,
  "total_published":   10482,
  "total_acked":       10140,
  "total_redelivered": 3,
  "total_dropped":     0,
  "uptime_seconds":    3600
}
```

| Field | Description |
|---|---|
| `queue_depth` | Messages currently waiting to be consumed |
| `capacity` | Total ring buffer capacity |
| `in_flight` | Messages delivered to Collectors but not yet acked |
| `total_published` | Lifetime count of messages received from Streamers |
| `total_acked` | Lifetime count of successfully acknowledged messages |
| `total_redelivered` | Lifetime count of messages redelivered after lease expiry |
| `total_dropped` | Lifetime count of messages dropped after exceeding max delivery attempts |
| `uptime_seconds` | Seconds since the queue service started |

All counters are **monotonically increasing** and never reset during runtime — a standard observability convention that makes rate calculations straightforward.

---

## 12. Security

### Design Decision: Kubernetes NetworkPolicy — No Application-Level Authentication

Internal cluster services (Streamer → Queue, Collector → Queue) communicate without application-level authentication tokens. Instead, access is restricted at the network layer using Kubernetes **NetworkPolicy** objects, which ensure only pods with the correct labels (i.e., Streamer and Collector pods in the same namespace) can reach the queue service.

This is a known simplification. In a production system, mutual TLS (mTLS) between services would be appropriate.

---

## 13. API Reference

### `POST /messages` — Publish a Message

Called by Streamers to put a telemetry message onto the queue.

**Request body size limit: 64KB.** Requests exceeding this are rejected with `413 Request Entity Too Large`. This is enforced by Gin middleware at server startup and prevents memory abuse from malformed or malicious requests.

**Request:**
```
POST /messages
Content-Type: application/json

{
  "timestamp":   "2025-07-18T20:42:34Z",
  "metric_name": "DCGM_FI_DEV_GPU_UTIL",
  "gpu_id":      "0",
  "device":      "nvidia0",
  "uuid":        "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
  "model_name":  "NVIDIA H100 80GB HBM3",
  "hostname":    "mtv5-dgx1-hgpu-031",
  "value":       "98",
  "labels_raw":  "DCGM_FI_DRIVER_VERSION=\"535.129.03\"..."
}
```

**Required fields:** `uuid`, `timestamp`, `metric_name`, `value`. All others are optional but should be included when available.

**Responses:**

| Status | Meaning |
|---|---|
| `201 Created` | Message accepted. Body: `{ "message_id": "uuid" }` |
| `400 Bad Request` | Missing or invalid required fields |
| `413 Request Entity Too Large` | Request body exceeds 64KB limit |
| `429 Too Many Requests` | Queue full — Streamer should retry with backoff |
| `503 Service Unavailable` | Queue is shutting down |

---

### `GET /messages/consume` — Consume a Batch (Long Poll)

Called by Collectors to receive a batch of messages. The server blocks until at least one message is available or the `long_poll_timeout` elapses.

**Request:**
```
GET /messages/consume?batch_size=10&long_poll_timeout=30s&consumer_id=collector-pod-abc123
```

| Query Param | Default | Description |
|---|---|---|
| `batch_size` | `10` | Maximum number of messages to return |
| `long_poll_timeout` | `30s` | Maximum wait time if queue is empty |
| `consumer_id` | required | Unique identifier for this Collector instance |

**Responses:**

| Status | Meaning |
|---|---|
| `200 OK` | One or more messages returned |
| `204 No Content` | `long_poll_timeout` elapsed, queue was empty |
| `503 Service Unavailable` | Queue is shutting down |

```json
200 OK
{
  "messages": [
    {
      "delivery_id":   "dlv-550e8400-e29b",
      "lease_expires": "2026-05-07T10:00:30Z",
      "message": {
        "message_id":  "msg-uuid-001",
        "timestamp":   "2025-07-18T20:42:34Z",
        "metric_name": "DCGM_FI_DEV_GPU_UTIL",
        "gpu_id":      "0",
        "device":      "nvidia0",
        "uuid":        "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
        "model_name":  "NVIDIA H100 80GB HBM3",
        "hostname":    "mtv5-dgx1-hgpu-031",
        "value":       "98",
        "labels_raw":  "..."
      }
    }
  ]
}
```

`lease_expires` is returned so the Collector can set its own internal processing deadline accordingly.

**`consumer_id`** must be sent on every consume request. It is used to associate deliveries with a specific Collector instance, enabling the queue to validate that acks come from the correct consumer. The value should be the pod's hostname, which Kubernetes sets automatically via the `HOSTNAME` environment variable:

```go
consumerID := os.Getenv("HOSTNAME") // e.g. "collector-7d9f4b-xk2pl"
```

---

### `POST /messages/ack` — Acknowledge a Batch

Called by Collectors after successfully persisting a batch to the database.

**Request:**
```
POST /messages/ack
Content-Type: application/json

{
  "consumer_id":  "collector-pod-abc123",
  "delivery_ids": ["dlv-550e8400-e29b", "dlv-660e9500-f30c"]
}
```

**`consumer_id`** must match the `consumer_id` that was used in the original `GET /messages/consume` request. If it does not match, the ack is rejected with `207`. This prevents a Collector from accidentally acking messages it did not consume.

**Responses:**

| Status | Meaning |
|---|---|
| `200 OK` | All acks processed. Body: `{ "acked": 2, "rejected": 0 }` |
| `207 Multi-Status` | Some acks rejected (lease expired or consumer_id mismatch). Body includes `rejected_ids` and `reason` |
| `400 Bad Request` | Malformed request |

A `207` response means the lease expired on some messages before the ack arrived — they were already redelivered to another Collector. The Collector should ignore rejected acks; the message will be processed again by the Collector that received the redelivery.

**Unknown `delivery_id`:** If a Collector sends an ack for a `delivery_id` the queue does not recognise (most commonly after a queue pod restart, where Collectors still hold `delivery_id`s from the previous process lifetime), the queue treats it identically to an expired lease — rejected with `207` and reason `"unknown delivery_id"`. The Collector ignores it and moves on.

---

### Error Response Format

All error responses across every endpoint share a consistent JSON body structure:

```json
{
  "error": "human readable description of what went wrong"
}
```

Streamer and Collector clients should always attempt to parse this field from non-2xx responses for logging purposes. The HTTP status code is the authoritative signal for control flow; the `error` field is informational.

---

### `GET /health` — Liveness Probe

```
GET /health

200 OK
{ "status": "ok" }
```

---

### `GET /ready` — Readiness Probe

```
GET /ready

200 OK
{ "status": "ready", "queue_depth": 342, "capacity": 10000 }

503 Service Unavailable
{ "status": "not ready", "reason": "shutting down" }
```

---

### `GET /metrics` — Queue Statistics

```
GET /metrics

200 OK
{
  "queue_depth":       342,
  "capacity":          10000,
  "in_flight":         18,
  "total_published":   10482,
  "total_acked":       10140,
  "total_redelivered": 3,
  "total_dropped":     0,
  "uptime_seconds":    3600
}
```

---

### `GET /docs/index.html` — Interactive API Documentation

Serves the Swagger UI browser interface for the Message Queue's internal OpenAPI spec.

```
GET /docs/index.html

200 OK  (HTML)
```

---

## 14. Configuration

All configuration is via environment variables. The service works with zero configuration using the defaults below.

| Environment Variable | Default | Description |
|---|---|---|
| `QUEUE_PORT` | `8080` | HTTP server port |
| `QUEUE_CAPACITY` | `10000` | Ring buffer size (number of message slots) |
| `QUEUE_LEASE_DURATION` | `30s` | Time a Collector has to acknowledge a message |
| `QUEUE_LEASE_REAPER_INTERVAL` | `5s` | How often the background goroutine checks for expired leases |
| `QUEUE_MAX_DELIVERY_ATTEMPTS` | `3` | Maximum delivery attempts before a message is dropped |
| `QUEUE_BATCH_SIZE` | `10` | Maximum messages returned per consume request |
| `QUEUE_PUBLISH_TIMEOUT` | `10s` | Maximum time a publish request blocks waiting for a free slot before returning 429 |
| `QUEUE_LONG_POLL_TIMEOUT` | `30s` | Maximum wait time on an empty queue per consume request |
| `QUEUE_SHUTDOWN_GRACE_PERIOD` | `30s` | Time allowed for draining before forced exit |
| `QUEUE_MAX_REQUEST_BODY_SIZE` | `65536` | Maximum publish request body size in bytes (64KB) |
| `QUEUE_HTTP_READ_TIMEOUT` | `15s` | HTTP server read timeout |
| `QUEUE_HTTP_WRITE_TIMEOUT` | `40s` | HTTP server write timeout — must exceed `QUEUE_LONG_POLL_TIMEOUT` |
| `QUEUE_HTTP_IDLE_TIMEOUT` | `60s` | HTTP server idle connection timeout |

> **Important:** `QUEUE_HTTP_WRITE_TIMEOUT` must always be greater than `QUEUE_LONG_POLL_TIMEOUT`. If they are equal, the HTTP server will close the long-poll connection at exactly the same moment the handler tries to write the timeout response, causing a race. The default values (40s write, 30s poll) provide a 10-second buffer. If you increase `QUEUE_LONG_POLL_TIMEOUT`, increase `QUEUE_HTTP_WRITE_TIMEOUT` by the same amount.

---

## 15. Concurrency Model

### HTTP Framework: Gin

All services in this pipeline use the **Gin** HTTP framework for consistency. Gin handles routing, JSON binding, request logging, and panic recovery. Using the same framework across services reduces cognitive overhead and enables shared internal packages for common patterns (error responses, middleware, etc.).

### Internal Concurrency

The queue service is highly concurrent by nature — multiple Streamers and Collectors interact with the ring buffer simultaneously.

```
Gin HTTP Server
(each request runs in its own goroutine — Gin's default)
    │
    ├── POST /messages    goroutine-1 ──┐
    ├── POST /messages    goroutine-2 ──┤
    ├── GET  /consume     goroutine-3 ──┤──► Ring Buffer
    ├── GET  /consume     goroutine-4 ──┤    (mutex protected)
    └── POST /ack         goroutine-5 ──┘
                                         ▲
                          Lease Reaper ──┘
                          (background goroutine,
                           runs on ticker interval)
```

### Synchronisation

- The ring buffer is protected by a **single `sync.Mutex`** — all reads and writes acquire it
- **`sync.Cond`** is used for efficient blocking:
  - `canPublish` condition — publish goroutines wait here when the buffer is full; signalled when a slot is freed by an ack
  - `canConsume` condition — consume goroutines wait here when the buffer is empty; signalled when a new message is published
- This avoids busy-waiting — goroutines sleep until the condition changes, using zero CPU
- The lease reaper goroutine acquires the same mutex during its scan — no race conditions
- **Lease reaper holds the mutex only briefly per slot**, not for the full scan. It locks, inspects and resets one slot, unlocks, then moves to the next. This prevents a full 10,000-slot scan from blocking publish/consume goroutines for a noticeable duration on every reaper tick.

### Why a Single Mutex is Sufficient

At 10 Streamers + 10 Collectors maximum, a single mutex protecting the ring buffer is not a bottleneck. The critical section (finding and marking a slot) is microseconds. Lock contention only becomes a concern at hundreds or thousands of concurrent clients.

---

## 16. Scaling

### Queue: Fixed at 1 Replica

The message queue runs as a **single pod** and is not horizontally scaled. It is a stateful service — its entire content lives in the ring buffer in memory. Running two queue pods without shared state would silo messages:

```
Streamer-1 → Queue Pod A  (writes M1, M2, M3)
Streamer-2 → Queue Pod B  (writes M4, M5, M6)

Collector-1 → Queue Pod A  (gets M1, M2, M3 — never sees M4, M5, M6)
```

Solving this requires distributed consensus or partition assignment — complexity well beyond this project's scope.

Kubernetes ensures high availability of the single instance via automatic restarts (liveness probe) and clean traffic management during updates (readiness probe).

### Streamers and Collectors: Elastic (1–10 Replicas)

The **Telemetry Streamer** and **Telemetry Collector** are the elastic components. They are fully stateless and can be freely scaled:

```bash
kubectl scale deployment streamer   --replicas=5
kubectl scale deployment collector  --replicas=8
```

New pods discover the queue via Kubernetes DNS and begin working immediately. Removed pods have their in-flight messages reclaimed automatically by the lease reaper.

### Known Limitation

The single-instance queue is a recognised architectural constraint. In a production system at scale, this component would be replaced by a distributed, durable message broker. The custom queue implementation here is intentionally scoped for clarity, testability, and demonstrating the core design concepts.
