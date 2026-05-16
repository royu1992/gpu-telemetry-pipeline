# GPU Telemetry Pipeline — Architecture

This document synthesises the per-service architecture docs and the project README to present a single, comprehensive reference for the full GPU Telemetry Pipeline. It summarises design goals, component responsibilities, data model, delivery semantics, deployment notes, operational procedures, and tuning guidance.

See the service-specific architecture docs for full detail:

- <a href="API_GATEWAY_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">API_GATEWAY_ARCHITECTURE.md</a>
- <a href="COLLECTOR_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">COLLECTOR_ARCHITECTURE.md</a>
- <a href="MESSAGE_QUEUE_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">MESSAGE_QUEUE_ARCHITECTURE.md</a>
- <a href="STREAMER_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">STREAMER_ARCHITECTURE.md</a>

OpenAPI specs and supporting artifacts are in `api/` (one OpenAPI spec per service):

- <a href="api/api-gateway/openapi_api_gateway_swagger.yaml" target="_blank" rel="noopener noreferrer">api/api-gateway/openapi_api_gateway_swagger.yaml</a>
- <a href="api/collector/openapi_collector_swagger.yaml" target="_blank" rel="noopener noreferrer">api/collector/openapi_collector_swagger.yaml</a>
- <a href="api/message-queue/openapi_message_queue_swagger.yaml" target="_blank" rel="noopener noreferrer">api/message-queue/openapi_message_queue_swagger.yaml</a>
- <a href="api/streamer/openapi_streamer_swagger.yaml" target="_blank" rel="noopener noreferrer">api/streamer/openapi_streamer_swagger.yaml</a>

Also consult the repository README for quickstart, running with `docker-compose`, and `kind` (local Kubernetes) instructions: <a href="../README.md" target="_blank" rel="noopener noreferrer">../README.md</a>.

**Scope & purpose**

- Provide an executive-level and operator-friendly description of how telemetry flows from CSV → queue → DB → API.
- Collate key design decisions, failure modes, observability points, and tuning guidance in a single place.

**High-level system diagram**

```
┌─────────────┐   HTTP POST   ┌───────────────┐   Long-poll   ┌─────────────┐
│  Streamer   │ ────────────► │ Message Queue │ ◄──────────── │  Collector  │
│  (1–10 pods)│               │  (1 pod)      │               │  (1–10 pods)│
└─────────────┘               └───────────────┘               └──────┬──────┘
                                                                 │ pgx bulk insert
                                                          ┌──────▼──────┐
                                                          │  PostgreSQL  │
                                                          └──────┬──────┘
                                                                 │ read-only
                                                          ┌──────▼──────┐
                                                          │ API Gateway  │
                                                          │  (1 pod)     │
                                                          └─────────────┘
```

**Core design principles**

- Decoupling: producers (Streamers) and consumers (Collectors) are decoupled via an in-memory ring-buffer queue.
- At-least-once delivery: delivered-by-lease semantics with idempotent DB writes.
- Simplicity & observability: REST + JSON wire format, small number of clear metrics per service.
- Scoped persistence: only Postgres persists data; the queue is intentionally in-memory and single-instance.

## Component summaries

### Message Queue (single replica)

- <a href="MESSAGE_QUEUE_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">MESSAGE_QUEUE_ARCHITECTURE.md</a>

- Role: central in-memory ring-buffer which accepts `POST /messages` from Streamers, and serves `GET /messages/consume` long-poll requests to Collectors.
- Key decisions:
  - Single shared ring buffer for bounded memory and O(1) operations.
  - Lease-based at-least-once delivery with a lease reaper to requeue expired deliveries.
  - Blocking publish when full (backpressure) with a publish timeout that returns `429`.
  - REST + JSON (human-readable, curl-friendly) with batching and long-polling.
- API surface (high-level): `POST /messages`, `GET /messages/consume?batch_size&long_poll_timeout&consumer_id`, `POST /messages/ack`, `GET /metrics`, `GET /health`, `GET /ready`, `GET /docs/index.html`.
- Concurrency: Gin per-request goroutines; a single `sync.Mutex` protects ring buffer transitions; `sync.Cond` used to block publishers/consumers efficiently.
- Failure modes:
  - If a collector crashes mid-processing, the lease expires and the message is redelivered to another collector.
  - Messages exceeding `QUEUE_MAX_DELIVERY_ATTEMPTS` are dropped and logged (no DLQ).
- Tuning knobs: `QUEUE_CAPACITY`, `QUEUE_LEASE_DURATION`, `QUEUE_LONG_POLL_TIMEOUT`, `QUEUE_HTTP_WRITE_TIMEOUT` (must be > long-poll timeout), `QUEUE_PUBLISH_TIMEOUT`.

### Streamer (producers)

- <a href="STREAMER_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">STREAMER_ARCHITECTURE.md</a>

- Role: reads DCGM CSV rows from a mounted file and publishes them to the queue (`POST /messages`).
- Behaviour:
  - One-row-per-request synchronous `POST` model (simple sequential loop). Generates `timestamp` at publish time.
  - Rewinds on EOF to simulate continuous telemetry; configurable `STREAMER_INTERVAL_MS` controls cadence.
  - Retry policy: bounded retries (default 3 attempts) with fixed delay, then skip and log.
  - Bad-row policy: skip a bad row and increment an error counter; exit after `STREAMER_MAX_CONSECUTIVE_ERRORS` consecutive bad rows.
- Observability: `streamer_rows_sent_total`, `streamer_errors_total`, `streamer_last_sent_timestamp_seconds`, `streamer_last_row_read_timestamp_seconds` exposed at `/metrics`.

### Collector (consumers)

- <a href="COLLECTOR_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">COLLECTOR_ARCHITECTURE.md</a>

- Role: long-polls the queue, validates/normalises messages, bulk-inserts into Postgres, then acknowledges delivery IDs.
- Behaviour:
  - Pull-based long-poll (`GET /messages/consume?consumer_id=...`) with configurable batch size and long-poll timeout.
  - Type conversion and validation; partial batch acceptance (skip invalid rows, ACK only persisted rows).
  - Bulk insert strategy using `pgx` and `SendBatch` / multi-row insert to minimise DB round-trips.
  - Runs `Migrate()` on startup (DDL + index creation) and sets readiness only after migration completes.
- Observability: `messages_consumed_total`, `db_writes_success_total`, `db_writes_error_total`, `validation_errors_total`, `last_db_write_timestamp`.

### API Gateway (read-only)

- <a href="API_GATEWAY_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">API_GATEWAY_ARCHITECTURE.md</a>

- Role: exposes read-only REST endpoints (list GPUs, get telemetry by `uuid`) backed by Postgres.
- Key points:
  - `uuid` is used as the canonical GPU identifier. `gpu_id` remains available in responses for human convenience.
  - GPU list cached in-memory (TTL default 1 minute) to avoid expensive `SELECT DISTINCT` scans.
  - DB pool is conservatively sized to avoid starving the Collector.
  - Query timeouts, `LIMIT` caps, and input validation to avoid resource exhaustion and slow queries.
- Observability: `requests_total`, `requests_success_total`, `requests_error_total`, `gpu_list_cache_hits_total`, `gpu_list_cache_misses_total`, `db_query_errors_total`.

### PostgreSQL (store)

- Single Postgres service persists `gpu_metrics` table with the following important characteristics:
  - Columns: `ts` (TIMESTAMPTZ), `hostname`, `gpu_id`, `metric_name`, `value`, `device`, `uuid`, `model_name`, `labels_raw`, `message_id`.
  - Composite primary key: `(ts, hostname, gpu_id, metric_name)` enabling `ON CONFLICT DO NOTHING` for idempotency under at-least-once delivery.
  - Secondary index: `idx_gpu_metrics_uuid_ts (uuid, ts DESC)` to accelerate Gateway queries by `uuid` and time-range scans.
  - Collector owns DDL/migrations at startup (centralises schema management).

## Wire format & data model

- Wire format: JSON over HTTP. `TelemetryMessage` maps directly to CSV columns. See sample CSV at <a href="dcgm_metrics_20250718_134233.csv" target="_blank" rel="noopener noreferrer">dcgm_metrics_20250718_134233.csv</a>.
- Key fields:
  - `timestamp` (RFC3339) — Streamer generates the ingest wall-clock timestamp on publish.
  - `uuid` — hardware UUID, globally unique and used as API `id`.
  - `message_id` — assigned by the queue when accepted (stable across redeliveries).
  - `delivery_id` — assigned per delivery attempt and used for ack.

SQL example used by Collector / Gateway:

```sql
INSERT INTO gpu_metrics (ts, hostname, gpu_id, metric_name, value, device, uuid, model_name, labels_raw, message_id)
VALUES (...) ON CONFLICT (ts, hostname, gpu_id, metric_name) DO NOTHING;

-- Index for gateway queries
CREATE INDEX IF NOT EXISTS idx_gpu_metrics_uuid_ts ON gpu_metrics (uuid, ts DESC);
```

## Delivery semantics, idempotency, and failures

- Semantics: At-least-once delivery via lease-based `IN_FLIGHT` state on the queue. If a consumer fails to ack before `lease_expires`, messages are requeued.
- Idempotency: handled at DB level via composite PK + `ON CONFLICT DO NOTHING`.
- Poison / bad messages: messages exceeding `QUEUE_MAX_DELIVERY_ATTEMPTS` are dropped and logged; the system records a `total_dropped` metric.
- Ack semantics: Collector sends `POST /messages/ack` with `delivery_id`s. The queue validates `consumer_id` and may return `207 Multi-Status` for some rejected delivery IDs.

## Scaling & operational constraints

- Message Queue is intentionally single-replica and stateful (ring buffer lives in memory). This simplifies design but means the queue is a single point of state; it should be made highly available via Kubernetes restart policies and careful rolling updates.
- Streamers and Collectors are stateless and horizontally scalable (typical range 1–10 replicas). New pods discover the queue via Kubernetes DNS and begin polling/producing immediately.
- Buffer sizing: tune `QUEUE_CAPACITY` to hold ~30–60 seconds of peak traffic; default is large (e.g., 10,000 slots) and memory cost is small.

## Deployment & runbook

- Local: `make compose-up` / `make compose-logs` / `make compose-down` (see <a href="../README.md" target="_blank" rel="noopener noreferrer">../README.md</a>).
- Kind (k8s) one-shot: `make k8s-up` loads images and installs Helm charts (charts exist under `charts/` for each component).
- Collector startup: runs schema migration (`Migrate()`) and only marks readiness after migration completes (prevents traffic before schema is present).
- Gateway: must use a read-only DB role (`gateway_user`) with only `SELECT` privileges; Collector uses a `collector_user` with `INSERT` and `MIGRATE` privileges.

## Observability & debugging

- Each service exposes `/metrics`, `/health`, and `/ready` as described in their dedicated docs. Use these endpoints for alerts and readiness checks.
- Key cross-system signals:
  - `streamer_rows_sent_total` drop → check queue readiness/`total_acked`.
  - `messages_consumed_total` lag → check collector logs or DB connectivity.
  - `total_redelivered` non-zero → indicates lease expiries and slow consumers.
  - `gpu_list_cache_misses_total` high → Gateway is frequently querying the DB (cache TTL tuning).

## Configuration and important invariants

- Always ensure `QUEUE_HTTP_WRITE_TIMEOUT` > `QUEUE_LONG_POLL_TIMEOUT` to avoid server write races.
- Keep Gateway DB pool conservative (default `GATEWAY_DB_MAX_CONNS=10`) so collectors have headroom for writes.
- Tune `COLLECTOR_BATCH_SIZE` and `COLLECTOR_LONG_POLL_TIMEOUT` to balance throughput vs. latency.

## Security model

- Service-to-service traffic is assumed to be internal to the Kubernetes cluster. The project uses Kubernetes `NetworkPolicy` to limit which pods can talk to each service rather than implementing application-level authentication.
- Database permissions follow the principle of least privilege: `collector_user` performs migrations and writes, `gateway_user` is read-only.

## Operational recommendations

- Monitor `total_dropped` and `total_redelivered` (queue) and `db_writes_error_total` (collector). Investigate persistent spikes.
- When performing rolling updates of the queue pod, accept that in-memory buffer contents are lost; either drain producers briefly or rely on the Streamer replay behaviour.
- For production at scale, replace the single in-memory queue with a distributed, durable broker (Kafka, RabbitMQ, etc.) if required by throughput or HA needs.

## File references (source material)

- Top-level README: <a href="../README.md" target="_blank" rel="noopener noreferrer">../README.md</a>
- Service architecture docs: <a href="API_GATEWAY_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">API_GATEWAY_ARCHITECTURE.md</a>, <a href="COLLECTOR_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">COLLECTOR_ARCHITECTURE.md</a>, <a href="MESSAGE_QUEUE_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">MESSAGE_QUEUE_ARCHITECTURE.md</a>, <a href="STREAMER_ARCHITECTURE.md" target="_blank" rel="noopener noreferrer">STREAMER_ARCHITECTURE.md</a>
- OpenAPI specs: <a href="api/api-gateway/openapi_api_gateway_swagger.yaml" target="_blank" rel="noopener noreferrer">api/api-gateway/openapi_api_gateway_swagger.yaml</a>, <a href="api/collector/openapi_collector_swagger.yaml" target="_blank" rel="noopener noreferrer">api/collector/openapi_collector_swagger.yaml</a>, <a href="api/message-queue/openapi_message_queue_swagger.yaml" target="_blank" rel="noopener noreferrer">api/message-queue/openapi_message_queue_swagger.yaml</a>, <a href="api/streamer/openapi_streamer_swagger.yaml" target="_blank" rel="noopener noreferrer">api/streamer/openapi_streamer_swagger.yaml</a>
- Sample CSV: <a href="dcgm_metrics_20250718_134233.csv" target="_blank" rel="noopener noreferrer">dcgm_metrics_20250718_134233.csv</a>
- Project specifications and requirements: <a href="GPU%20Telemetry%20Pipeline%20Message%20Queue.pdf" target="_blank" rel="noopener noreferrer">GPU Telemetry Pipeline Message Queue.pdf</a>

---

If you'd like, I can:

- open any specific section as a PR, or
- extract a one-page operational runbook, or
- add a short appendix with recommended Prometheus alert rules.
