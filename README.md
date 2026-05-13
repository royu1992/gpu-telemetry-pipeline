# GPU Telemetry Pipeline

Elastic, scalable telemetry pipeline for an AI cluster. Reads GPU metrics from a DCGM CSV export, streams them through a custom in-memory message queue, persists them in PostgreSQL, and exposes them via a REST API.

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
                                                               │  (1 pod)    │
                                                               └─────────────┘
```

---

## Table of Contents

1. [Architecture](#architecture)
2. [Repository layout](#repository-layout)
3. [Prerequisites](#prerequisites)
4. [Build instructions](#build-instructions)
5. [Running locally with docker-compose](#running-locally-with-docker-compose)
6. [Running on Kubernetes (kind)](#running-on-kubernetes-kind)
7. [API reference](#api-reference)
8. [Sample user workflow](#sample-user-workflow)
9. [Scaling streamers and collectors](#scaling-streamers-and-collectors)
10. [Code coverage](#code-coverage)
11. [OpenAPI spec generation](#openapi-spec-generation)
12. [AI assistance documentation](#ai-assistance-documentation)

---

## Architecture

### Components

| Component | Port | Description |
|---|---|---|
| **Message Queue** | 8080 | Custom in-memory ring-buffer queue. Exposes HTTP endpoints for publish, consume, and ack. |
| **Streamer** | 8081 | Reads DCGM CSV rows in a loop, batches them, publishes to the queue. Horizontally scalable (up to 10 pods). |
| **Collector** | 8082 | Long-polls the queue, parses messages, bulk-inserts into PostgreSQL. Runs the schema migration on startup. Horizontally scalable (up to 10 pods). |
| **API Gateway** | 9090 | Read-only REST API backed by PostgreSQL. GPU list is served from an in-memory TTL cache. |
| **PostgreSQL** | 5432 | Persistent store for all ingested metrics (`gpu_metrics` table). |

### Message Queue design

The queue is implemented as a fixed-capacity **ring buffer** of `Slot` structs. Each slot holds one message and a state machine (`free → occupied → in-flight → free`).

- **Publish** (`POST /messages`): a streamer acquires the next free slot, writes the message, and atomically transitions it to `occupied`.
- **Consume** (`GET /messages?n=N&timeout=T`): a collector long-polls for up to `T` seconds. When messages are available, up to `N` are leased (state → `in-flight`) and returned with their IDs.
- **Ack** (`POST /messages/ack`): after a successful insert, the collector acks each message ID, returning the slot to `free`.
- **Lease reaper**: a background goroutine periodically scans for slots whose lease has expired (the collector crashed or was slow) and requeues them, up to `MaxDeliveryAttempts`.

This design gives O(1) publish/consume with no allocations on the hot path, and bounded memory usage regardless of throughput.

### Telemetry data model

Each CSV row produces one `TelemetryMessage` (JSON over HTTP) and one row in `gpu_metrics`:

| Column | Type | Description |
|---|---|---|
| `ts` | TIMESTAMPTZ | Time the row was processed (wall-clock at ingest) |
| `hostname` | TEXT | Node hostname |
| `gpu_id` | TEXT | Ordinal GPU index on the node |
| `metric_name` | TEXT | DCGM metric name (e.g. `DCGM_FI_DEV_GPU_UTIL`) |
| `value` | DOUBLE PRECISION | Metric value |
| `device` | TEXT | OS device name (e.g. `nvidia0`) |
| `uuid` | TEXT | Hardware UUID (used as the API `{id}` parameter) |
| `model_name` | TEXT | GPU product name |
| `labels_raw` | TEXT | Raw Prometheus-style label string |
| `message_id` | TEXT | Queue-assigned message ID for deduplication |

**Primary key**: `(ts, hostname, gpu_id, metric_name)` — enables `ON CONFLICT DO NOTHING` deduplication during at-least-once redelivery.  
**Secondary index**: `(uuid, ts DESC)` — optimises the API Gateway's time-range query.

---

## Repository layout

```
.
├── build/                  # Dockerfiles (one per service)
│   ├── api-gateway/
│   ├── collector/
│   ├── message-queue/
│   ├── postgres/
│   └── streamer/
├── charts/                 # Helm charts (one per component)
│   ├── api-gateway/
│   ├── collector/
│   ├── message-queue/
│   ├── postgres/
│   └── streamer/
├── cmd/                    # main packages
│   ├── api-gateway/
│   ├── collector/
│   ├── message_queue/
│   └── streamer/
├── docs/                   # Architecture docs, DCGM CSV data file, and OpenAPI specs
│   ├── api/                # Generated OpenAPI specs + Go registration packages
│   │   ├── api-gateway/    #   spec + swaggo package for the API Gateway
│   │   ├── collector/      #   spec + swaggo package for the Collector
│   │   ├── message-queue/  #   spec + swaggo package for the Message Queue
│   │   └── streamer/       #   spec + swaggo package for the Streamer
│   ├── API_GATEWAY_ARCHITECTURE.md
│   ├── COLLECTOR_ARCHITECTURE.md
│   ├── MESSAGE_QUEUE_ARCHITECTURE.md
│   └── STREAMER_ARCHITECTURE.md
├── internal/               # Shared and service-specific packages
│   ├── api-gateway/
│   ├── collector/
│   ├── message_queue/
│   ├── model/              # Shared TelemetryMessage type
│   ├── store/              # PostgreSQL read/write layer
│   └── streamer/
├── docker-compose.yaml     # Full local stack
├── kind-config.yaml        # kind cluster topology
└── Makefile                # All build, test, and deploy targets
```

---

## Prerequisites

| Tool | Minimum version | Purpose |
|---|---|---|
| Go | 1.25 | Build and test |
| Docker + Docker Compose v2 | 24+ | Container builds and local stack |
| kind | 0.23+ | Local Kubernetes cluster |
| kubectl | 1.30+ | Cluster interaction |
| Helm | 3.14+ | Chart packaging and release management |
| swag *(optional)* | latest | OpenAPI spec generation (`make generate-openapi` auto-installs it) |

---

## Build instructions

```bash
# Compile all four binaries for the current host OS
make build-local

# Compile for linux/amd64 (what Docker images use)
make build

# Build all Docker images (tagged as localhost:5001/<name>:latest)
make docker-build

# Build a single image
make docker-build-api-gateway
```

---

## Running locally with docker-compose

This is the fastest way to get the full pipeline running end-to-end.

```bash
# 1. Start all services (builds images on first run, ~2 min)
make compose-up

# 2. Watch logs to confirm the pipeline is flowing
make compose-logs

# 3. Wait until the collector logs "database migration complete"
#    then query the API (see Sample user workflow below)

# 4. Tear everything down when done
make compose-down
```

Services exposed to the host:

| Service | URL |
|---|---|
| Message Queue | http://localhost:8080 |
| Streamer health | http://localhost:8081/health |
| Collector health | http://localhost:8082/health |
| **API Gateway** | **http://localhost:9090** |
| PostgreSQL | localhost:5432 |

---

## Running on Kubernetes (kind)

### One-shot setup

```bash
# Creates cluster, builds images, loads them, installs all Helm charts
make k8s-up
```

Behind the scenes this runs:

```
make kind-create   →  kind create cluster --config kind-config.yaml
make kind-load     →  docker build + kind load docker-image (×4)
make helm-install  →  kubectl create ns + helm upgrade --install (×5 charts)
```

The `kind-config.yaml` maps **host port 9090** → container port 30090 (the API Gateway NodePort), so the API is reachable at `http://localhost:9090` without any `kubectl port-forward`.

### Manual step-by-step

```bash
# 1. Create the kind cluster
kind create cluster --config kind-config.yaml --name gpu-telemetry

# 2. Build and load images
make docker-build
make kind-load

# 3. Create namespace and CSV ConfigMap
make k8s-create-csv-configmap

# 4. Install Helm charts in dependency order
helm upgrade --install postgres      charts/postgres      -n gpu-telemetry --wait
helm upgrade --install message-queue charts/message-queue -n gpu-telemetry --wait
helm upgrade --install collector     charts/collector     -n gpu-telemetry --wait
helm upgrade --install streamer      charts/streamer      -n gpu-telemetry --wait
helm upgrade --install api-gateway   charts/api-gateway   -n gpu-telemetry --wait

# 5. Verify pods are Running
kubectl get pods -n gpu-telemetry

# 6. Tear down
make k8s-down
```

### Scaling

Scaling is supported for both Streamers and Collectors via the Helm `autoscaling` values or `kubectl scale`:

```bash
# Scale to 3 streamer replicas
kubectl scale deployment streamer -n gpu-telemetry --replicas=3

# Enable HPA (CPU-based, 1–10 replicas) for the collector
helm upgrade collector charts/collector -n gpu-telemetry \
  --set autoscaling.enabled=true \
  --set autoscaling.minReplicas=2 \
  --set autoscaling.maxReplicas=10
```

---

## API reference

Each service exposes its own interactive Swagger UI. After starting the stack (see above), open a browser and navigate to:

| Service | Swagger UI URL |
|---|---|
| **API Gateway** | http://localhost:9090/docs/index.html |
| Message Queue | http://localhost:8080/docs/index.html |
| Collector | http://localhost:8082/docs/index.html |
| Streamer | http://localhost:8081/docs/index.html |

The underlying OpenAPI YAML specs are located in `docs/api/<service>/`.
Regenerate them at any time with `make generate-openapi`.

### Endpoints

#### `GET /gpus`

Returns all GPUs for which telemetry data is available.

```bash
curl http://localhost:9090/gpus
```

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

#### `GET /gpus/{id}/telemetry`

Returns telemetry for a specific GPU, ordered by timestamp.

| Query parameter | Type | Default | Description |
|---|---|---|---|
| `start_time` | RFC3339 | now − 1 hour | Inclusive lower time bound |
| `end_time` | RFC3339 | now | Inclusive upper time bound |

```bash
GPU_ID="GPU-5fd4f087-86f3-7a43-b711-4771313afc50"

# All data in the last hour (default window)
curl "http://localhost:9090/gpus/${GPU_ID}/telemetry"

# Specific time window
curl "http://localhost:9090/gpus/${GPU_ID}/telemetry?start_time=2025-07-18T20:00:00Z&end_time=2025-07-18T21:00:00Z"
```

```json
{
  "id": "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
  "count": 1,
  "data": [
    {
      "timestamp": "2025-07-18T20:42:34Z",
      "metric_name": "DCGM_FI_DEV_GPU_UTIL",
      "value": 0,
      "hostname": "mtv5-dgx1-hgpu-031",
      "gpu_id": "0",
      "model_name": "NVIDIA H100 80GB HBM3"
    }
  ]
}
```

---

## Sample user workflow

```bash
# Step 1: Start the pipeline (docker-compose example)
make compose-up

# Step 2: Wait ~10 seconds for data to flow through the pipeline
sleep 10

# Step 3: List available GPUs
curl -s http://localhost:9090/gpus | python3 -m json.tool

# Step 4: Copy a GPU UUID from the response and query its telemetry
GPU_ID="GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
curl -s "http://localhost:9090/gpus/${GPU_ID}/telemetry" | python3 -m json.tool

# Step 5: Query a specific time window
curl -s "http://localhost:9090/gpus/${GPU_ID}/telemetry?start_time=2025-07-18T20:00:00Z&end_time=2025-07-18T21:00:00Z" \
  | python3 -m json.tool

# Step 6: Check observability endpoints
curl http://localhost:8080/metrics   # message-queue counters
curl http://localhost:8082/metrics   # collector counters
curl http://localhost:9090/metrics   # api-gateway counters
```

---

## Scaling streamers and collectors

The docker-compose stack runs one instance of each. To test horizontal scaling locally with compose:

```bash
# Run 3 streamer replicas (they independently loop through the CSV)
docker compose up --scale streamer=3 -d

# Run 2 collector replicas (they compete for messages on the queue)
docker compose up --scale collector=2 -d
```

On Kubernetes:

```bash
kubectl scale deployment streamer   -n gpu-telemetry --replicas=5
kubectl scale deployment collector  -n gpu-telemetry --replicas=3
```

The message queue implements at-least-once delivery with a **lease reaper** that requeues messages leased by crashed consumers. Each slot is retried up to `QUEUE_MAX_DELIVERY_ATTEMPTS` times before being dropped and counted.

---

## Code coverage

```bash
# Run the full suite and print function-level coverage
make coverage

# Open the HTML report in a browser
open coverage.html   # macOS
xdg-open coverage.html  # Linux
start coverage.html  # Windows
```

The `coverage.out` profile is also compatible with CI tools such as Codecov and SonarQube.

---

## OpenAPI spec generation

Each service has its own scoped OpenAPI spec, generated from `@Summary`, `@Param`, `@Router`, and `@Success` annotations embedded in the service's source files. Output is written to `docs/api/<service>/`.

- `cmd/api-gateway/main.go` — general API metadata
- `internal/api-gateway/server/handler.go` — per-route annotations

To regenerate:

```bash
make generate-openapi
```

This installs [swag](https://github.com/swaggo/swag) on demand (via `go install`) then runs four scoped `swag init` invocations, one per service, for example:

```
swag init --generalInfo main.go \
  --dir cmd/api-gateway,internal/api-gateway/server,... \
  --output docs/api/api-gateway \
  --outputTypes go,yaml \
  --instanceName openapi
```

You can also regenerate a single service's spec:

```bash
make generate-openapi-gateway
make generate-openapi-message-queue
make generate-openapi-collector
make generate-openapi-streamer
```

---

## AI assistance documentation

This project was developed with extensive use of GitHub Copilot (Claude Sonnet 4.5 / 4.6). The following table summarises which aspects were AI-assisted and where manual intervention was required.

### Bootstrapping

| Aspect | AI-assisted? | Notes |
|---|---|---|
| Repo / directory structure | Yes | Prompted for a Go module layout with `cmd/`, `internal/`, `build/`, `charts/` |
| `go.mod` initial setup | Yes | AI suggested gin + pgx/v5 + google/uuid dependencies |
| `.gitignore` | Yes | Standard Go gitignore generated |

**Prompts used:**
- *"Bootstrap a Go 1.25 module for an elastic GPU telemetry pipeline with these four services: streamer, collector, message-queue, api-gateway. Use gin for HTTP and pgx/v5 for Postgres."*

### Source code

| Component | AI-assisted? | Manual intervention |
|---|---|---|
| Message Queue ring buffer | Mostly AI | Concurrency correctness (atomic state transitions) reviewed manually |
| Lease reaper | AI | Verified reaper does not starve the hot path |
| Streamer CSV reader + publisher | AI | Retry/backoff logic tuned by hand |
| Collector consumer loop | AI | Long-poll timeout interplay with HTTP client timeout required manual tuning |
| Store (Postgres read/write) | AI | ON CONFLICT deduplication key selection done manually |
| API Gateway handlers | AI | Time-range default window (now−1h) was a deliberate manual choice |
| Config loaders | AI | All env-var names and defaults finalised manually |

**Prompts used (selection):**
- *"Write a fixed-size in-memory ring buffer for a message queue in Go with atomic slot state transitions (free/occupied/in-flight). The buffer must support concurrent publish and consume without a global lock."*
- *"Implement a lease reaper goroutine that scans the ring buffer for expired in-flight slots and requeues them."*
- *"Write a Go HTTP long-poll consumer that pulls batches from the message queue and bulk-inserts them into PostgreSQL using pgx SendBatch."*
- *"Design a PostgreSQL schema for DCGM GPU metrics with a composite primary key for deduplication and a secondary index optimised for time-range queries by GPU UUID."*

### Tests

All unit tests were AI-generated with the following prompt pattern:

- *"Write table-driven unit tests for `<package>` covering all exported functions, using mocks/stubs for external dependencies (Postgres, HTTP). Include edge cases."*

Manual intervention was required to:
- Replace incorrect interface method signatures in mocks
- Fix test cases where the AI incorrectly assumed time.Now() determinism

### Build environment

| Deliverable | AI-assisted? | Manual notes |
|---|---|---|
| Dockerfiles (multi-stage) | Yes | Build context choice (repo root) was a manual decision |
| docker-compose.yaml | Yes | Healthcheck ordering (collector depends on postgres AND queue) refined manually |
| kind-config.yaml | Yes | NodePort / extraPortMapping required reading kind docs manually |
| Helm charts (all templates) | Yes | StatefulSet VolumeClaimTemplate for Postgres required iteration |
| Makefile | Yes | Phony targets, shell escaping, and `helm-install` order finalised manually |
| OpenAPI spec | Yes | Written manually then annotated for swag auto-generation |

**Prompts used:**
- *"Write a multi-stage Dockerfile for a Go 1.25 service. Stage 1 builds a static binary using golang:1.25-alpine. Stage 2 uses alpine:3.20 with wget installed for health checks."*
- *"Write a comprehensive Makefile for a Go project with targets for build, test, coverage, docker-build/push, docker-compose, kind cluster management, Helm chart install, and OpenAPI generation."*
- *"Write a Helm chart for a Kubernetes Deployment that mounts a ConfigMap as a volume for the streamer service's CSV data."*
- *"Write a Helm StatefulSet chart for PostgreSQL 16 with a PersistentVolumeClaim."*

### Where AI fell short / required manual fixes

1. **Concurrent ring-buffer correctness**: The initial AI-generated buffer used a mutex for all operations. This was replaced with atomic state transitions to avoid contention on the publish hot path.

2. **Long-poll timeout coordination**: The AI set `COLLECTOR_LONG_POLL_TIMEOUT` = `QUEUE_HTTP_WRITE_TIMEOUT`. In practice the queue write timeout must exceed the long-poll timeout to prevent the server closing the connection before the response is written.

3. **Kind NodePort mapping**: The initial AI-generated `kind-config.yaml` used an `Ingress` mapping that does not work without an ingress controller. Replaced with a direct `extraPortMappings` entry targeting the NodePort.

4. **swag annotation syntax**: The first AI-generated swag annotations used incorrect types for the `{array}` response. The correct format `{array} store.GPUSummary` required consultation with the swag documentation.
