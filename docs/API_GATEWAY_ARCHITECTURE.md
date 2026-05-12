# API Gateway Service — Architecture & Design

## Table of Contents

1. [Overview](#overview)
2. [Responsibility & Role in the Pipeline](#1-responsibility--role-in-the-pipeline)
3. [Query Interface & API Contract](#2-query-interface--api-contract)
4. [GPU Identity — The `{id}` Design Decision](#3-gpu-identity--the-id-design-decision)
5. [Database Read Path](#4-database-read-path)
6. [Shared Store Package](#5-shared-store-package)
7. [Database Indexing Strategy](#6-database-indexing-strategy)
8. [JSON Payload Structure](#7-json-payload-structure)
9. [Request Safety — Timeouts, Limits & Validation](#8-request-safety--timeouts-limits--validation)
10. [GPU List Cache](#9-gpu-list-cache)
11. [Error Handling & HTTP Status Codes](#10-error-handling--http-status-codes)
12. [CORS](#11-cors)
13. [Database Security — Read-Only Role](#12-database-security--read-only-role)
14. [Concurrency Architecture](#13-concurrency-architecture)
15. [Readiness & Life Cycle Management](#14-readiness--life-cycle-management)
16. [Observability & Metrics](#15-observability--metrics)
17. [Configuration](#16-configuration)

---

## Overview

The API Gateway (Kubernetes service: `api-gateway`) is the **read-facing** service in the GPU Telemetry Pipeline. Its sole responsibility is to expose the telemetry data stored in Postgres via a clean, well-defined REST API that is safe, efficient, and easy to consume.

**What it does:**
- Exposes a REST API for listing all known GPUs in the cluster.
- Exposes a REST API for querying time-series telemetry for a specific GPU.
- Applies query safety measures (timeouts, row limits, input validation).
- Serves cached responses for stable, expensive list queries.
- Exposes health, readiness, and metrics endpoints for Kubernetes and monitoring systems.

**What it is NOT:**
- It is not a write path — it never inserts, updates, or deletes data.
- It is not an aggregation service — it returns raw telemetry rows, not aggregated statistics.
- It does not talk to the Message Queue or the Streamer — it is purely a database reader.

**Kubernetes service name:** `api-gateway`

```
┌──────────────────────────────────────────────────────────┐
│                    API Gateway Pod                       │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │  Gin HTTP Server (:8083)                         │    │
│  │                                                  │    │
│  │  GET /api/v1/gpus                                │    │
│  │  GET /api/v1/gpus/{id}/telemetry                 │    │
│  │  GET /health  GET /ready  GET /metrics           │    │
│  └───────────────────────┬──────────────────────────┘    │
│                          │                               │
│              ┌───────────▼───────────┐                   │
│              │   GPU List Cache      │                   │
│              │   (sync.RWMutex,      │                   │
│              │    1-min TTL)         │                   │
│              └───────────┬───────────┘                   │
│                          │ cache miss                    │
│              ┌───────────▼───────────┐                   │
│              │   Read Store          │                   │
│              │   (internal/store)    │                   │
│              └───────────┬───────────┘                   │
│                          │                               │
└──────────────────────────┼───────────────────────────────┘
                           │ SELECT (read-only)
                           ▼
                  ┌─────────────────┐
                  │    Postgres      │
                  │  (gpu_metrics)   │
                  └─────────────────┘
```

---

## 1. Responsibility & Role in the Pipeline

The API Gateway is the terminal consumer of the pipeline from a user's perspective. All other services are internal infrastructure; this service is what an operator, dashboard, or external system interacts with.

```
┌──────────┐    POST /messages    ┌───────────────┐   GET /messages/consume  ┌───────────┐   SQL INSERT  ┌──────────┐
│ Streamer │ ──────────────────► │ Message Queue │ ──────────────────────► │ Collector │ ────────────► │    DB    │
└──────────┘                     └───────────────┘                          └───────────┘               └──────────┘
                                                                                                              │
                                                                                                     SQL SELECT│
                                                                                                              ▼
                                                                                                    ┌──────────────────┐
                                                                                                    │  API Gateway     │◄── REST Client
                                                                                                    └──────────────────┘
```

The API Gateway is **strictly read-only**. It connects to the same Postgres database that the Collector writes to, but using a database user that has only `SELECT` privileges on the `gpu_metrics` table. This enforces the principle of least privilege — even if the Gateway is compromised, it cannot corrupt or delete stored telemetry.

---

## 2. Query Interface & API Contract

The API Gateway exposes the following endpoints as required by the project specification:

### `GET /api/v1/gpus`

Returns the list of all unique GPUs for which telemetry data is currently stored in the database.

- **No query parameters.**
- **Response**: `200 OK` with a JSON array of GPU summary objects.
- **Backed by**: A `SELECT DISTINCT uuid, hostname, gpu_id, model_name FROM gpu_metrics` query, results cached for 1 minute.

**Example Response:**
```json
[
  {
    "id": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
    "hostname": "mtv5-dgx1-hgpu-031",
    "gpu_id": "0",
    "model_name": "NVIDIA H100 80GB HBM3"
  },
  {
    "id": "GPU-bc7a12ab-4998-fdc5-0785-2678a929a142",
    "hostname": "mtv5-dgx1-hgpu-031",
    "gpu_id": "1",
    "model_name": "NVIDIA H100 80GB HBM3"
  }
]
```

---

### `GET /api/v1/gpus/{id}/telemetry`

Returns all telemetry entries for a specific GPU, ordered by time ascending.

**Path Parameter:**
- `{id}`: The hardware `uuid` of the GPU (e.g., `GPU-5fd4f087-86f3-7a43-b711-4771313afc50`). Callers discover this value from `GET /api/v1/gpus`.

**Optional Query Parameters:**
- `start_time`: RFC3339 timestamp (inclusive). Default: 1 hour before the current time.
- `end_time`: RFC3339 timestamp (inclusive). Default: current time.

**Example Requests:**
```
GET /api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry
GET /api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time=2025-07-18T20:00:00Z&end_time=2025-07-18T21:00:00Z
```

**Response**: `200 OK` with a wrapped JSON object (see Section 7).

---

### Observability Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Always returns `200 OK`. Used by Kubernetes liveness probe. |
| `GET /ready` | Returns `200 OK` only if the DB connection is alive. Used by Kubernetes readiness probe. |
| `GET /metrics` | Returns plain-text atomic counters (request totals, cache hits, DB errors). |

---

## 3. GPU Identity — The `{id}` Design Decision

### The Problem

The DCGM telemetry CSV (and thus the `gpu_metrics` table) contains two identity fields per GPU:

- **`gpu_id`**: An integer index local to the host, e.g., `"0"`, `"1"`, `"2"`. This is **not** globally unique — every host has a GPU `"0"`.
- **`uuid`**: A hardware identifier, e.g., `"GPU-5fd4f087-86f3-7a43-b711-4771313afc50"`. This **is** globally unique across the entire cluster.

### Why Not `gpu_id`?

If we used `gpu_id` as the API identifier, `GET /api/v1/gpus/0/telemetry` would return a mixed dataset of telemetry from **every node's first GPU** — a semantically incorrect result when the goal is querying "a specific GPU", as required by the project specification.

### Why `uuid`?

The `uuid` value is the only globally unique, source-native identifier available for a GPU. It is the same identifier used by NVIDIA management tools (`nvidia-smi`, `dcgm-exporter`) and is the industry standard for pinning telemetry to a specific piece of hardware.

### User Experience Concern

UUIDs are long and not human-writable. This is by design and acceptable because:

1. **Discoverability**: Callers first use `GET /api/v1/gpus` to get the full list, including each GPU's `uuid`, `hostname`, and `gpu_id`. The response is human-readable enough to understand which GPU is which.
2. **Machine Consumption**: The API is primarily consumed by dashboards (Grafana), CLIs, or scripts — not by humans typing URLs. These tools copy the `uuid` from the list response and use it programmatically.
3. **The `gpu_id` is still present**: The listing response includes `gpu_id` so a user can see "GPU 0 on node mtv5-dgx1-hgpu-031" and use the corresponding UUID.

---

## 4. Database Read Path

### Connection Pool (Read-Optimized)

The API Gateway opens its own `pgxpool.Pool` connection to Postgres, separate from the Collector's pool. The pool is configured conservatively to avoid starving the Collector's write connections.

- **Maximum connections**: Configurable via `GATEWAY_DB_MAX_CONNS` (default: `10`).
- **Minimum idle connections**: `2`.
- **The pool is shared** across all Gin goroutines — one pool per Gateway pod, not one pool per request.

### Query Structure

**List GPUs:**
```sql
SELECT DISTINCT uuid, hostname, gpu_id, model_name
FROM gpu_metrics
ORDER BY hostname, gpu_id;
```

**Get Telemetry:**
```sql
SELECT ts, metric_name, value, hostname, gpu_id, device, model_name
FROM gpu_metrics
WHERE uuid = $1
  AND ts >= $2
  AND ts <= $3
ORDER BY ts ASC
LIMIT $4;
```

All queries are parameterised (`$1`, `$2`, ...) to prevent SQL injection.

---

## 5. Shared Store Package

### Motivation

Previously, the Collector owned all database logic inside `internal/collector/store/store.go`. Since the API Gateway also needs to query the same `gpu_metrics` table, that logic needs to be accessible to both services without duplication.

### Solution: Refactor to `internal/store`

The store package is moved to `internal/store` and restructured into two responsibilities:

| File | Responsibility |
|---|---|
| `internal/store/store.go` | Shared connection management, the `dbPool` interface, and the `Row` struct. |
| `internal/store/writer.go` | Write operations used by the Collector: `Migrate()`, `BulkInsert()`. |
| `internal/store/reader.go` | Read operations used by the API Gateway: `ListGPUs()`, `GetTelemetry()`. |

This structure means:
- The **Collector** imports and uses `store.New()`, `Migrate()`, and `BulkInsert()`.
- The **API Gateway** imports and uses `store.New()`, `ListGPUs()`, and `GetTelemetry()`.
- The `dbPool` interface (which enables test mocking) remains shared and benefits both services.

---

## 6. Database Indexing Strategy

### Current Index (Ingestion-Optimized)

The Collector's migration creates the following primary key:
```sql
PRIMARY KEY (ts, hostname, gpu_id, metric_name)
```

In Postgres, a primary key creates a B-Tree index with `ts` as the **leading column**. This is optimal for inserts (which deduplicate by `ts`) but is suboptimal for the Gateway's queries, which filter by `uuid` first.

### New Index (Query-Optimized)

The `Migrate()` function will also create the following secondary index:
```sql
CREATE INDEX IF NOT EXISTS idx_gpu_metrics_uuid_ts
    ON gpu_metrics (uuid, ts DESC);
```

**Why this index works:**

- `uuid` is the leading column — Postgres can instantly locate all rows for a specific GPU without a full-table scan.
- `ts DESC` matches the ordering direction of the `ORDER BY ts ASC` query (Postgres can scan an index backwards) and places the most recent data at the top of the index leaf nodes, which is the most commonly queried range.

**Why the Collector owns this index:**

The `Migrate()` function runs at Collector startup and is responsible for all DDL (schema and index creation). Centralising all DDL here ensures the database schema is consistent regardless of how many Gateway pods are running, and avoids DDL race conditions if multiple Gateway pods start simultaneously.

---

## 7. JSON Payload Structure

### `GET /api/v1/gpus` Response

A flat JSON array of GPU summary objects. Each object includes enough context that a caller understands exactly which physical card the `id` refers to, without needing prior knowledge.

```json
[
  {
    "id": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
    "hostname": "mtv5-dgx1-hgpu-031",
    "gpu_id": "0",
    "model_name": "NVIDIA H100 80GB HBM3"
  }
]
```

### `GET /api/v1/gpus/{id}/telemetry` Response

A **wrapped object** (not a raw array) containing the GPU identifier, the count of returned rows, and the data array.

```json
{
  "id": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
  "count": 2,
  "data": [
    {
      "timestamp": "2025-07-18T20:42:34Z",
      "metric_name": "DCGM_FI_DEV_GPU_UTIL",
      "value": 98.5,
      "hostname": "mtv5-dgx1-hgpu-031",
      "gpu_id": "0",
      "model_name": "NVIDIA H100 80GB HBM3"
    },
    {
      "timestamp": "2025-07-18T20:42:35Z",
      "metric_name": "DCGM_FI_DEV_GPU_TEMP",
      "value": 72.0,
      "hostname": "mtv5-dgx1-hgpu-031",
      "gpu_id": "0",
      "model_name": "NVIDIA H100 80GB HBM3"
    }
  ]
}
```

### Why a Wrapped Object (Not a Raw Array)?

- **Extensibility**: Adding `next_page_token`, `start_time`, `end_time`, or `truncated: true` to the response in the future does not break existing callers.
- **Self-describing**: The `id` and `count` fields in the top level tell the caller exactly which GPU and how many rows were returned without inspecting the array.
- **Future-proofing**: If pagination is ever added, the wrapper is already in place.

---

## 8. Request Safety — Timeouts, Limits & Validation

### Per-Request Query Timeout

Every database query is wrapped in a `context.WithTimeout`:

```go
ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
defer cancel()
```

If the Postgres query takes longer than 5 seconds (e.g., due to DB overload or a very wide time range before indexes are built), the query is cancelled and the Gateway returns `504 Gateway Timeout`. This prevents slow queries from holding up Gin goroutines indefinitely.

The 5-second timeout is configurable via `GATEWAY_DB_QUERY_TIMEOUT`.

### Row Limit

Every telemetry query includes a hard `LIMIT` clause to prevent a single request from dumping millions of rows into a single JSON response, which would exhaust the Gateway's memory.

```sql
LIMIT $4  -- defaults to GATEWAY_MAX_RESPONSE_ROWS, default 1000
```

If the result is truncated by the limit, the response payload will still return `200 OK` with the capped rows. The caller should use the time window filters to narrow their query if they need more data.

### Default Time Window

When `start_time` and/or `end_time` are omitted, the Gateway applies safe defaults:
- **`start_time` omitted**: defaults to `time.Now().Add(-1 * time.Hour)` — the last 1 hour.
- **`end_time` omitted**: defaults to `time.Now()`.

### Input Validation

Before any database call is made, the Gateway validates all inputs:

| Input | Validation Rule | Error on Failure |
|---|---|---|
| `start_time` | Must be a valid RFC3339 timestamp | `400 Bad Request` |
| `end_time` | Must be a valid RFC3339 timestamp | `400 Bad Request` |
| `start_time` vs `end_time` | `start_time` must be before `end_time` | `400 Bad Request` |
| `{id}` (UUID) | Non-empty string; format is not pre-validated, DB query handles it | `404 Not Found` if not found |

**Error Response Shape (400):**
```json
{
  "error": "invalid start_time: cannot parse '2025-07-XX' as RFC3339"
}
```

---

## 9. GPU List Cache

### Why Cache?

The `SELECT DISTINCT uuid, hostname, gpu_id, model_name FROM gpu_metrics` query scans a potentially large table to find unique GPU identifiers. In a cluster with millions of rows, this can be slow. Critically, the set of GPUs in the cluster changes very rarely (only when hardware is added or removed), so re-running this query on every API request is wasteful.

### Cache Design

The cache is a struct stored in memory within each Gateway pod:

```go
type gpuListCache struct {
    mu        sync.RWMutex
    entries   []GPUSummary
    expiresAt time.Time
}
```

**Read path (cache hit):**
1. Acquire `RLock()` — allows multiple concurrent readers without blocking each other.
2. Check if `time.Now().Before(expiresAt)`. If true, return `entries`.
3. Release `RLock()`.

**Write path (cache miss or expiry):**
1. Acquire full `Lock()` — exclusive write, blocks all readers.
2. Re-check expiry ("double-checked locking") in case another goroutine already refreshed the cache while this goroutine was waiting for the lock.
3. If still expired, query the DB and update `entries` and `expiresAt`.
4. Release `Lock()`.

### TTL

The cache Time-To-Live (TTL) defaults to **1 minute** and is configurable via `GATEWAY_CACHE_TTL_GPUS`. On first request after startup, the cache is cold and will always hit the DB once.

### Observability

The cache exposes two counters in `/metrics`:
- `gpu_list_cache_hits_total`: Cache was valid; DB was not queried.
- `gpu_list_cache_misses_total`: Cache was expired or cold; DB was queried.

---

## 10. Error Handling & HTTP Status Codes

The Gateway maps all outcomes to explicit, well-defined HTTP status codes. Internal Postgres error details are **never** leaked to the HTTP response — doing so could expose schema names, query structure, or other internals.

| Situation | HTTP Status | Response Body |
|---|---|---|
| Success — data found | `200 OK` | Wrapped JSON payload |
| Success — GPU found, but no data in time range | `200 OK` | `{ "id": "...", "count": 0, "data": [] }` |
| GPU UUID not found in DB | `404 Not Found` | `{ "error": "GPU not found" }` |
| Invalid `start_time` / `end_time` format | `400 Bad Request` | `{ "error": "invalid start_time: ..." }` |
| `start_time` is after `end_time` | `400 Bad Request` | `{ "error": "start_time must be before end_time" }` |
| DB query timeout exceeded | `504 Gateway Timeout` | `{ "error": "query timed out" }` |
| DB connection unavailable / pool exhausted | `503 Service Unavailable` | `{ "error": "database unavailable" }` |
| Any other unexpected internal error | `500 Internal Server Error` | `{ "error": "internal server error" }` |

### GPU Not Found vs Empty Data

These are two distinct situations handled differently on purpose:

1. **GPU exists, no data in time range**: The UUID is valid (found in the DB), but no rows match the time window filter. Returns `200` with an empty `data` array. This is not an error — the GPU is real, just idle or not yet polled in that window.
2. **GPU does not exist**: The UUID is completely unknown to the database. Returns `404`. This signals to the caller that they have used an incorrect identifier.

The implementation does this with a two-step query: first a lightweight `SELECT 1 FROM gpu_metrics WHERE uuid = $1 LIMIT 1` to confirm existence, then the full telemetry query with time filters.

---

## 11. CORS

### Why CORS Matters

Browsers enforce the Same-Origin Policy (SOP). If a web-based dashboard hosted at `http://dashboard.company.com` makes a JavaScript `fetch()` call to `http://api-gateway:8083/api/v1/gpus`, the browser will block the response unless the API includes the appropriate `Access-Control-Allow-Origin` response header.

Without CORS support, the API Gateway is unusable from any browser-based frontend.

### Implementation

CORS is handled by the `github.com/gin-contrib/cors` middleware, registered globally on the Gin router. The middleware:

- Adds `Access-Control-Allow-Origin` to all responses.
- Handles `OPTIONS` preflight requests automatically (required for non-simple requests with custom headers or JSON bodies).
- Is configured before any route handlers are registered.

### Configuration

The list of allowed origins is controlled by the `GATEWAY_CORS_ORIGINS` environment variable.

- **Development default**: `*` (all origins) — permissive for ease of local development.
- **Production recommendation**: Set to the specific dashboard origin, e.g., `https://dashboard.company.com`, to prevent unauthorized cross-origin access.

---

## 12. Database Security — Read-Only Role

### Principle of Least Privilege

The API Gateway only ever reads data. It is therefore connected to Postgres using a dedicated role that has only `SELECT` privileges on the `gpu_metrics` table.

```sql
-- Collector role: can insert and read
GRANT INSERT, SELECT ON gpu_metrics TO collector_user;

-- Gateway role: can only read
GRANT SELECT ON gpu_metrics TO gateway_user;
```

### Why This Matters

If the API Gateway service is ever compromised (e.g., via a dependency vulnerability or a path traversal bug), the attacker cannot:
- Delete telemetry records.
- Truncate the `gpu_metrics` table.
- Insert fake telemetry data.
- Drop tables or modify the schema.

The Collector uses its own `DB_DSN` pointing to `collector_user`. The Gateway uses a separate `DB_DSN` pointing to `gateway_user`. Both are injected as Kubernetes secrets via the Helm chart.

### Schema Ownership

DDL operations (creating the table, creating indexes) require elevated permissions and remain the responsibility of the Collector's role, which is the service that calls `Migrate()` at startup. The Gateway role does not need (and does not have) `CREATE`, `ALTER`, or `DROP` privileges.

---

## 13. Concurrency Architecture

The API Gateway uses a **2-Goroutine Model**:

1.  **Main Goroutine**: Handles startup (config loading, DB pool creation, readiness check), runs the Gin HTTP server directly (blocking call), and handles shutdown after `SIGINT`/`SIGTERM`.
2.  **Shutdown Goroutine**: Waits on `os.Signal`, then triggers a graceful HTTP server shutdown via `srv.Shutdown(ctx)`.

Unlike the Collector and Streamer, the Gateway does not have a background processing loop. All work is driven by incoming HTTP requests, which Gin handles in per-request goroutines from its internal pool. The shared state (the GPU list cache and the DB pool) is protected by `sync.RWMutex` and `pgxpool`'s own internal locking respectively.

```
OS Signal (SIGTERM/SIGINT)
         │
         ▼
  Shutdown Goroutine ──► srv.Shutdown(ctx) ──► Drain in-flight requests
                                               (up to 10s timeout)
                                            ──► pool.Close()
                                            ──► Exit
```

---

## 14. Readiness & Life Cycle Management

### Startup Sequence

The Gateway follows a strict startup sequence before accepting traffic:

1. Load and validate all environment variables. Exit with a non-zero code if any required config is missing.
2. Establish the `pgxpool` connection to Postgres. If `DB.Ping()` fails within the connect timeout, exit.
3. Mark the service as ready (set `ready = true`).
4. Start the Gin HTTP server.

The `/ready` endpoint returns `200 OK` only after step 3 completes. Kubernetes will not send traffic to the pod until readiness is confirmed.

### Graceful Shutdown

On receiving `SIGINT` or `SIGTERM`:

1. Call `srv.Shutdown(ctx)` with a 10-second deadline. This stops accepting new connections and waits for in-flight requests to complete.
2. Once the server drains, close the Postgres connection pool (`pool.Close()`).
3. Exit with code `0`.

This ensures that:
- No in-flight requests are abruptly cut off.
- No database connections are leaked.
- Kubernetes rolling updates work correctly, as the old pod completes cleanly before being terminated.

### Health vs Readiness

| Probe | Endpoint | Returns 200 when... |
|---|---|---|
| Liveness | `GET /health` | Always (the process is running) |
| Readiness | `GET /ready` | DB ping is successful |

The liveness probe is intentionally simple — if the process is running, it is "alive". Kubernetes uses the liveness probe to decide whether to restart the container; it should not be made artificially strict.

The readiness probe is stricter — if the DB is unreachable, the Gateway cannot serve useful responses, so it should be taken out of rotation (readiness returns `503`).

---

## 15. Observability & Metrics

The `GET /metrics` endpoint returns plain-text key-value counters, consistent with the pattern used by the Streamer and Collector services. No external Prometheus library is required.

**Example Response:**
```
requests_total: 1482
requests_success_total: 1478
requests_error_total: 4
gpu_list_cache_hits_total: 1461
gpu_list_cache_misses_total: 21
db_query_errors_total: 2
```

| Metric | Description |
|---|---|
| `requests_total` | Total HTTP requests received (all endpoints) |
| `requests_success_total` | Requests that returned a `2xx` response |
| `requests_error_total` | Requests that returned a `4xx` or `5xx` response |
| `gpu_list_cache_hits_total` | Times `GET /api/v1/gpus` was served from the in-memory cache |
| `gpu_list_cache_misses_total` | Times `GET /api/v1/gpus` required a live DB query |
| `db_query_errors_total` | Total database errors encountered (timeouts + connection failures) |

All counters are `sync/atomic` integers, incremented without locking.

---

## 16. Configuration

All configuration is provided via environment variables. The Gateway has no configuration files on disk.

| Variable | Required | Default | Description |
|---|---|---|---|
| `GATEWAY_PORT` | No | `8083` | The TCP port the Gin HTTP server listens on |
| `DB_DSN` | **Yes** | — | Postgres connection string for the read-only gateway user (e.g., `postgres://gateway_user:pass@postgres:5432/telemetry`) |
| `GATEWAY_DB_MAX_CONNS` | No | `10` | Maximum connections in the read pool |
| `GATEWAY_DB_CONNECT_TIMEOUT` | No | `10s` | Timeout for the initial DB connection at startup |
| `GATEWAY_DB_QUERY_TIMEOUT` | No | `5s` | Per-request context timeout for all DB queries |
| `GATEWAY_MAX_RESPONSE_ROWS` | No | `1000` | Hard row cap on every telemetry query response |
| `GATEWAY_CACHE_TTL_GPUS` | No | `1m` | Time-To-Live for the in-memory GPU list cache |
| `GATEWAY_CORS_ORIGINS` | No | `*` | Comma-separated list of allowed CORS origins |
