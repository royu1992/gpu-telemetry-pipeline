# Streamer Service — Architecture & Design

## Table of Contents

1. [Overview](#overview)
2. [Responsibility & Role in the Pipeline](#1-responsibility--role-in-the-pipeline)
3. [Input Model — CSV File](#2-input-model--csv-file)
4. [CSV Parsing Strategy](#3-csv-parsing-strategy)
5. [Hostname Resolution](#4-hostname-resolution)
6. [Delivery Model](#5-delivery-model)
7. [Throughput Control](#6-throughput-control)
8. [Retry Policy](#7-retry-policy)
9. [Bad Row Policy](#8-bad-row-policy)
10. [EOF Rewind Behavior](#9-eof-rewind-behavior)
11. [Per-Request HTTP Timeout](#10-per-request-http-timeout)
12. [Concurrency Model](#11-concurrency-model)
13. [Graceful Shutdown](#12-graceful-shutdown)
14. [Observability](#13-observability)
15. [Health & Readiness](#14-health--readiness)
16. [Configuration](#15-configuration)

---

## Overview

The Streamer is a **producer** service in the GPU Telemetry Pipeline. Its sole job is to read GPU telemetry records from a CSV file and deliver them, one at a time, to the Message Queue service over HTTP.

**What it does:**
- Reads GPU telemetry rows from a mounted CSV file, line by line
- Converts each row into a structured `TelemetryMessage` JSON payload
- Sends each message to the Message Queue via a `POST /messages` request
- Loops back to the start of the file after reaching EOF, simulating a continuous telemetry feed
- Exposes health, readiness, and metrics endpoints for Kubernetes and monitoring systems

**What it is NOT:**
- It is not a real-time telemetry agent — it replays static CSV data
- It is not responsible for persistence, deduplication, or acknowledgment
- It does not fan out messages to multiple queues

**Kubernetes service name:** `streamer`

```
┌──────────────────────────────────────────────────┐
│                  Streamer Pod                    │
│                                                  │
│  ┌─────────────┐     ┌────────────────────────┐  │
│  │  CSV File   │────►│   Telemetry Loop       │  │
│  │  (mounted   │     │   (read → convert →    │──┼───► POST /messages ───► message-queue
│  │   volume)   │     │    POST → sleep)       │  │
│  └─────────────┘     └────────────────────────┘  │
│                                                  │
│  ┌──────────────────────────────────────────┐    │
│  │  Gin Server (:8081)                      │    │
│  │  GET /health  GET /ready  GET /metrics   │    │
│  │  GET /docs/index.html                 │    │
│  └──────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
```

---

## 1. Responsibility & Role in the Pipeline

The Streamer sits at the very start of the data pipeline, acting as the **source of telemetry data**.

```
┌──────────┐    POST /messages    ┌───────────────┐   GET /messages/consume  ┌───────────┐   SQL INSERT  ┌──────────┐
│ Streamer │ ──────────────────► │ Message Queue │ ──────────────────────► │ Collector │ ────────────► │    DB    │
└──────────┘                     └───────────────┘                          └───────────┘               └──────────┘
```

The Streamer is intentionally kept **simple and disposable**:
- It holds no persistent state of its own.
- Restarting a Streamer pod has no data loss implications on its own — it simply begins reading from the top of the CSV again.
- Its reliability guarantee is provided by the Message Queue's at-least-once delivery semantics: if a message was successfully posted to the queue, it will eventually reach a Collector.

---

## 2. Input Model — CSV File

### Design Decision: Mounted Volume, Not Baked Into the Image

The CSV file is provided to the Streamer through a **Kubernetes volume mount**, not embedded in the container image.

**Why mounted volume:**
- Updating or replacing the data file does not require rebuilding and redeploying the image.
- The same container image can be used across environments with different data files.
- Secrets or environment-specific datasets can be injected at runtime without code changes.

**Default mount path:** `/data/metrics.csv` (configurable via `STREAMER_CSV_PATH`).

### CSV Format

The expected CSV file is a DCGM telemetry export with the following header columns:

```
timestamp, metric_name, gpu_id, device, uuid, modelName, Hostname,
container, pod, namespace, value, labels_raw
```

The Streamer reads the CSV columns and maps them to the `TelemetryMessage` schema before sending to the queue. Note: the CSV `timestamp` column is ignored — the Streamer generates the `timestamp` value at publish time (UTC, RFC3339) so replayed rows are recorded with the ingest wall-clock time. All rows after the header are treated as data rows.

---

## 3. CSV Parsing Strategy

### Design Decision: `encoding/csv.Reader`, Not `bufio.Scanner`

The Streamer reads the file in a **streaming fashion** using Go's standard `encoding/csv.Reader`. It does **not** load the entire file into memory.

**Why `encoding/csv.Reader` over `bufio.Scanner`:**

| | `bufio.Scanner` | `encoding/csv.Reader` |
|---|---|---|
| Handles quoted fields | No | Yes |
| Handles embedded newlines | No | Yes |
| Token size limit (default 64KB) | Yes — can fail on large rows | No |
| CSV-aware parsing | No | Yes |
| Memory footprint | Very low | Very low |

A bare `bufio.Scanner` would fail silently or panic if any CSV row contained a quoted field with an embedded newline, or if a row's total byte length exceeded the scanner's internal buffer limit. `encoding/csv.Reader` handles all standard CSV edge cases correctly.

### Memory Profile

Because the file is streamed one row at a time, the Streamer's memory footprint remains constant regardless of the CSV file size. It will use less than **50MB of RAM** in all normal operating conditions.

---

## 4. Hostname Resolution

The `Hostname` column in the CSV identifies the physical GPU node that generated the telemetry. However, in practice, this field can be missing or malformed. The Streamer applies a deterministic three-step resolution process before every row is sent.

### Rule 6.1 — Whitespace Trimming

Before evaluating the hostname, the raw value from the CSV is trimmed of all leading and trailing whitespace.

```
" gpu-node-01 "  →  "gpu-node-01"
```

If the result after trimming is an empty string, the field is considered **blank** and the fallback logic is triggered.

**Rationale:** Without trimming, `"node-01"` and `" node-01 "` would be treated as two distinct hosts in the database, causing split time-series in dashboards and incorrect alerting.

### Rule 6.2 — Empty String Detection

Only a truly empty string (after trimming) triggers the fallback. The Streamer does **not** maintain a list of placeholder sentinel values like `"N/A"`, `"-"`, or `"null"`.

**Rationale:** The actual CSV data from this pipeline uses empty strings (`""`) for missing fields, not text-based sentinels. Adding a placeholder list would be premature complexity for data that does not require it.

### Rule 6.3 — Fallback Precedence

If the trimmed CSV hostname is blank, the Streamer resolves the hostname using the following priority order:

```
Priority 1: Trimmed value from CSV Hostname column
     │
     ▼ (if empty)
Priority 2: os.Hostname() — returns the pod name in Kubernetes
     │
     ▼ (if empty or error)
Priority 3: Static string "unknown-host"
```

In a Kubernetes environment, `os.Hostname()` returns the **pod name**, which is a meaningful and traceable identity for observability purposes. The static fallback `"unknown-host"` ensures that the service never panics or produces a message with a missing host field, while making it obvious in the database that the data has a provenance problem.

### Complete Resolution Example

```go
func resolveHostname(csvHostname string) string {
    // Rule 6.1: Trim whitespace
    h := strings.TrimSpace(csvHostname)

    // Rule 6.2 & 6.3: Fallback if empty
    if h == "" {
        if sysHost, err := os.Hostname(); err == nil && strings.TrimSpace(sysHost) != "" {
            h = strings.TrimSpace(sysHost)
        } else {
            h = "unknown-host"
        }
    }

    return h
}
```

---

## 5. Delivery Model

### Design Decision: One Row, One Synchronous HTTP POST

Each CSV row is converted into exactly one `TelemetryMessage` and sent to the Message Queue via a single synchronous `POST /messages` HTTP request. The Streamer waits for the response before reading the next row.

**Why not parallel sends?**

| | Sequential (Chosen) | Parallel |
|---|---|---|
| Backpressure | Natural — slow queue = slow reads | Must implement separately |
| Row ordering | Preserved | Scrambled |
| Resource usage | Predictable (1 connection) | Unpredictable (N connections) |
| Metric accuracy | Simple — one goroutine owns all metrics | Requires atomics/mutexes everywhere |
| Risk of queue overload | None | High — could flood queue with backlog |

The Message Queue already has backpressure built in: when its ring buffer is full, it returns `HTTP 503` to the Streamer. With a sequential model, this naturally slows down the Streamer without any additional coordination code.

### Request Payload

Each POST sends a JSON body matching the `PublishRequest` schema defined in `internal/message_queue/model/`.

Note: the Streamer now **generates the `timestamp` at publish time** (UTC, RFC3339) rather than using the historical timestamp embedded in the CSV file. This ensures replayed CSV rows are visible to the API Gateway's default recent-time queries.

Example payload (timestamp generated at publish time):

```json
{
  "timestamp":   "2026-05-14T06:48:45Z",
  "metric_name": "DCGM_FI_DEV_GPU_UTIL",
  "gpu_id":      "0",
  "device":      "nvidia0",
  "uuid":        "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
  "model_name":  "NVIDIA H100 80GB HBM3",
  "hostname":    "mtv5-dgx1-hgpu-031",
  "container":   "",
  "pod":         "",
  "namespace":   "",
  "value":       "0",
  "labels_raw":  "DCGM_FI_DRIVER_VERSION=\"535.129.03\"..."
}
```

---

## 6. Throughput Control

### Design Decision: Configurable Delay Between Sends

After each successful or failed send attempt, the Streamer sleeps for a configurable duration before reading the next row.

**Controlled by:** `STREAMER_INTERVAL_MS` (default: `100` milliseconds)

**Why a delay?**
- Without a delay, a fast machine reading a small CSV could flood the Message Queue at tens of thousands of rows per second.
- The delay allows operators to tune the pipeline throughput to match the processing capacity of downstream Collectors and the database.
- Even a 100ms interval produces a sustained 10 rows/second — sufficient for realistic telemetry simulation.

**Why `time.Sleep` over a `time.Ticker`?**

A ticker fires at a fixed wall-clock interval regardless of how long the send took. If a send takes 500ms and the ticker fires every 100ms, the Streamer immediately sends the next row with no pause — which is unexpected behavior when the queue is under load.

A post-send sleep means the interval is always measured *after* a send completes, which produces more predictable behavior under variable network latency.

---

## 7. Retry Policy

### Design Decision: Bounded Retry with Fixed Backoff, then Skip

When a POST request to the Message Queue fails — due to a network error, timeout, or `5xx` response — the Streamer does not immediately give up or block forever.

**Retry rules:**
- Maximum **3 attempts** per row (1 initial attempt + 2 retries).
- Fixed **2-second delay** between each attempt.
- Controlled by `STREAMER_RETRY_ATTEMPTS` and `STREAMER_RETRY_DELAY_SECONDS`.

```
Attempt 1 fails
  │
  ▼ wait 2s
Attempt 2 fails
  │
  ▼ wait 2s
Attempt 3 fails
  │
  ▼ Log error. Increment streamer_errors_total. Skip row. Move on.
```

If all retries are exhausted, the row is **skipped**. The Streamer logs the failure at ERROR level, increments `streamer_errors_total`, and advances to the next row.

**Why skip rather than block?**

The Message Queue already guarantees at-least-once delivery for messages it has accepted. It is the Streamer's job to get a message *into* the queue, not to guarantee that any single specific row from the CSV survives a network partition. If the queue is persistently down, the Streamer's consecutive-error monitoring will surface the problem.

**Why not exponential backoff?**

Exponential backoff is appropriate when request volume is high and many clients are retrying simultaneously. This Streamer sends sequentially and is the only producer, so a fixed 2-second delay is simple, predictable, and effective without risk of a "retry storm."

---

## 8. Bad Row Policy

### Design Decision: Skip with Warning, Halt on Consecutive Failures

A "bad row" is a CSV row that can be successfully read by the parser but cannot be converted into a valid `TelemetryMessage`. Examples include: a missing required column, an empty `metric_name`, or a value that cannot be parsed.

This is distinct from a **send failure** (network problem). The data itself is the problem, and retrying it will not help.

**Rules:**
1. Log a WARNING with the row number and the reason the row was rejected.
2. Increment `streamer_errors_total`.
3. Skip the row and advance to the next one.
4. Increment an internal **consecutive bad row counter**.
5. If the consecutive counter reaches `STREAMER_MAX_CONSECUTIVE_ERRORS` (default: `10`), the Streamer logs a FATAL error and exits. This signals that the mounted file is likely corrupt or the wrong file was deployed.
6. A successful row (either sent or sent-after-retry) **resets** the consecutive bad row counter to zero.

```
Row 801  → bad (missing metric_name)  consecutive_bad=1
Row 802  → bad (empty uuid)           consecutive_bad=2
Row 803  → OK, sent successfully      consecutive_bad=0  ← reset
Row 804  → bad                        consecutive_bad=1
...
Row 810  → bad (10th consecutive)     consecutive_bad=10 → FATAL EXIT
```

**Why halt at 10 consecutive failures?**

A single bad row is a data quality issue. Ten consecutive bad rows almost certainly mean the CSV file schema has changed, the wrong file was mounted, or the file is corrupted. Continuing to silently skip thousands of rows provides no value and hides a critical operational problem.

---

## 9. EOF Rewind Behavior

When the CSV parser reaches the end of the file, the Streamer does not exit. It rewinds to the beginning (seeking past the header row) and starts again, simulating an infinite telemetry stream.

**Rewind sequence:**
1. EOF is detected.
2. Streamer sleeps for **one full `STREAMER_INTERVAL_MS` tick** before rewinding.
3. The file reader is reset to the first data row (after the header).
4. The **consecutive bad row counter is reset to zero** (a fresh file loop is a clean slate).
5. Normal processing resumes.

**Why sleep before rewinding?**

Without a pause, the loop boundary would cause an immediate send after the last row of the file, effectively removing the interval between the last row and the first row of the next cycle. The sleep ensures the send cadence is consistent across the loop boundary — the first row of the new loop is treated identically to every other row.

---

## 10. Per-Request HTTP Timeout

Every HTTP POST to the Message Queue is wrapped in a `context.WithTimeout` with a configurable deadline.

**Default:** 10 seconds, controlled by `STREAMER_REQUEST_TIMEOUT_SECONDS`.

**Why 10 seconds?**
- Short enough to detect a hung queue before the retry window becomes unacceptably long.
- Long enough to avoid false timeouts on a queue that is momentarily under load (e.g., after GC or a backpressure event).

If a request times out, it is treated **identically to any other send failure** and enters the retry logic from Section 7. A timeout counts toward the 3 retry attempts.

This timeout is **separate from the shutdown grace period** defined in Section 12. The per-request timeout governs individual network calls during normal operation; the shutdown grace period governs the final in-flight request at termination time.

---

## 11. Concurrency Model

The Streamer uses a minimal, easy-to-reason-about concurrency model: **two goroutines plus main**.

```
main()
 │
 ├──► go telemetryLoop()   — reads CSV, sends HTTP POSTs, updates metrics
 │
 └──► go ginServer()       — serves /health, /ready, /metrics on :8081
```

### Why Only One Telemetry Loop Goroutine?

| Property | Single Loop | Multiple Workers |
|---|---|---|
| Backpressure | Automatic — blocked POST = no new reads | Must be designed separately |
| Row ordering | Guaranteed | Requires coordination |
| Memory usage | 1 row in flight at any time | N rows in flight |
| Metric updates | Single owner, no race conditions | Needs atomics or mutexes |
| Debugging | Simple stack traces | Complex goroutine interactions |

A second goroutine would only be justified if maximum throughput were the primary goal. For a telemetry simulation pipeline, **predictability and operational simplicity are more important than raw throughput**.

### Thread Safety

The telemetry loop is the **sole writer** of all four metrics (see Section 13). The Gin server is the **sole reader** of those metrics when serving `/metrics`. Because both operations are atomic gauge and counter updates (using the Prometheus client library's thread-safe operations), no additional mutex is needed.

---

## 12. Graceful Shutdown

The Streamer handles `SIGTERM` and `SIGINT` gracefully to avoid leaving partial data or orphaned goroutines.

### Shutdown Sequence

```
Signal received (SIGTERM or SIGINT)
       │
       ▼
Step 1: Stop the Telemetry Loop
        The loop checks a "stop" channel or context cancellation.
        No new rows are read after this point.
       │
       ▼
Step 2: In-Flight Grace Period (up to 5 seconds)
        If a POST request is currently in progress, it is allowed
        to complete naturally within a 5-second window.
        If it finishes in 1 second, shutdown proceeds immediately.
       │
       ▼
Step 3: Gin Server Shutdown
        The health/metrics server is shut down with a short deadline
        so Kubernetes stops sending traffic to the pod.
       │
       ▼
Step 4: Total Safety Valve (30 seconds)
        If all cleanup has not completed within 30 seconds of the
        original signal, the process calls os.Exit(1) and terminates.
       │
       ▼
Step 5: Clean Exit (os.Exit(0))
```

### Why a Safety Valve?

Without a total deadline, a hung goroutine (e.g., a stalled network connection) could prevent the pod from ever terminating. Kubernetes will eventually send `SIGKILL` after its own `terminationGracePeriodSeconds`, but that produces no application-level log message. The Streamer's internal 30-second limit ensures the process logs a clear FATAL message before dying, making the cause of the hang visible in the pod logs.

### Kubernetes Alignment

The Streamer's shutdown grace period should be less than the Kubernetes `terminationGracePeriodSeconds` defined in its `Deployment` spec. A recommended Kubernetes setting is **60 seconds**, which gives the Streamer's 30-second internal limit enough headroom to operate without risk of being preempted by the platform.

---

## 13. Observability

The Streamer exposes four metrics on the `/metrics` endpoint (Prometheus text format). The set is intentionally minimal — enough to determine health, detect stalls, and distinguish between input and output bottlenecks.

### Metrics

| Metric | Type | Description |
|---|---|---|
| `streamer_rows_sent_total` | Counter | Total rows successfully delivered to the Message Queue |
| `streamer_errors_total` | Counter | Total errors (send failures + bad row skips) |
| `streamer_last_sent_timestamp_seconds` | Gauge | Unix timestamp of the last successful POST |
| `streamer_last_row_read_timestamp_seconds` | Gauge | Unix timestamp of the last successful CSV row read |

### How to Use These Metrics for Alerting (Optional)

**Is the Streamer alive and sending?**
```
rate(streamer_rows_sent_total[1m]) > 0
```

**Is the Streamer failing silently?**
```
rate(streamer_errors_total[1m]) > 0
```

**Is the Streamer stuck — not sending?**
```
time() - streamer_last_sent_timestamp_seconds > 60
```
This alert fires when no row has been sent for over 60 seconds, even if no errors are being counted. This catches the "blocked on backpressure" scenario where the queue is full and the Streamer is hung waiting.

**Is the Streamer reading but not sending (output bottleneck)?**
```
# last_row_read is recent, but last_sent is stale
time() - streamer_last_sent_timestamp_seconds > 60
AND
time() - streamer_last_row_read_timestamp_seconds < 10
```
This combination means the Streamer is reading CSV rows fine, but POST requests to the queue are failing or timing out.

### Why Not More Metrics?

Metrics like per-row latency histograms, loop counters, or per-host cardinality labels were explicitly excluded because:
- They add operational noise without proportional observability value for a service this simple.
- Prometheus histograms carry significant cardinality cost when labels are added.
- The four metrics above are sufficient to answer every meaningful operational question about this service.

---

## 14. Health & Readiness

The Gin server on port `8081` exposes the following Kubernetes probe endpoints:

| Endpoint | Probe Type | Description |
|---|---|---|
| `GET /health` | Liveness | Returns `200 OK` if the process is alive |
| `GET /ready` | Readiness | Returns `200 OK` if the CSV file has been opened successfully and the telemetry loop is running |
| `GET /metrics` | Monitoring | Returns Prometheus-format metrics |
| `GET /docs/index.html` | Documentation | Serves the interactive Swagger UI for the Streamer's internal OpenAPI spec |

### Readiness vs. Liveness

- **Liveness (`/health`)** — always returns `200` while the process is running. Kubernetes uses this to decide whether to restart the pod.
- **Readiness (`/ready`)** — returns `200` only after the CSV file has been opened and the telemetry loop is actively running. Returns `503` during startup or if the file cannot be opened. Kubernetes uses this to decide whether to send traffic to the pod.

If the CSV file path is misconfigured or the file does not exist at the expected mount path, the Streamer will set the readiness probe to `503` and log a FATAL error. The pod will appear as `Running` but `0/1 READY` in `kubectl get pods`, making the misconfiguration immediately visible.

---

## 15. Configuration

All Streamer configuration is provided via environment variables. There are no configuration files.

| Env Var | Description | Default |
|---|---|---|
| `STREAMER_CSV_PATH` | Absolute path to the mounted CSV file | `/data/metrics.csv` |
| `STREAMER_QUEUE_URL` | Base URL of the Message Queue service | `http://message-queue:8080` |
| `STREAMER_INTERVAL_MS` | Milliseconds to sleep between row sends | `100` |
| `STREAMER_REQUEST_TIMEOUT_SECONDS` | Per-POST HTTP request deadline | `10` |
| `STREAMER_SHUTDOWN_GRACE_SECONDS` | Total process shutdown deadline | `30` |
| `STREAMER_MAX_CONSECUTIVE_ERRORS` | Bad row count before fatal exit | `10` |
| `STREAMER_RETRY_ATTEMPTS` | Max send attempts per row | `3` |
| `STREAMER_RETRY_DELAY_SECONDS` | Seconds between retry attempts | `2` |
| `STREAMER_PORT` | Port for the health/metrics Gin server | `8081` |

### Example Kubernetes Deployment Snippet

```yaml
env:
  - name: STREAMER_CSV_PATH
    value: "/data/metrics.csv"
  - name: STREAMER_QUEUE_URL
    value: "http://message-queue:8080"
  - name: STREAMER_INTERVAL_MS
    value: "100"
  - name: STREAMER_REQUEST_TIMEOUT_SECONDS
    value: "10"
  - name: STREAMER_SHUTDOWN_GRACE_SECONDS
    value: "30"
  - name: STREAMER_MAX_CONSECUTIVE_ERRORS
    value: "10"
  - name: STREAMER_RETRY_ATTEMPTS
    value: "3"
  - name: STREAMER_RETRY_DELAY_SECONDS
    value: "2"
  - name: STREAMER_PORT
    value: "8081"

volumeMounts:
  - name: telemetry-data
    mountPath: /data

volumes:
  - name: telemetry-data
    configMap:
      name: dcgm-metrics-csv
```
