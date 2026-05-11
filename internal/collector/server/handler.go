package server

import (
	"net/http"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/metrics"
)

// Handler registers the collector's three operational HTTP endpoints:
//   - GET /health  — liveness probe (always 200 while the process is alive)
//   - GET /ready   — readiness probe (200 only after DB connection, migration,
//     and consumption loop are all confirmed running)
//   - GET /metrics — current observability snapshot in JSON
//
// The Handler is safe for concurrent use: readyFlag is an atomic, and metrics
// values are read via atomic operations inside metrics.Snapshot().
type Handler struct {
	// metrics is the shared Metrics instance written by the consumption loop
	// goroutine and read here when serving GET /metrics.
	metrics *metrics.Metrics

	// readyFlag is 0 at construction, set to 1 by main once the DB connection,
	// migration, and loop are all verified, and reset to 0 immediately on SIGTERM
	// so the readiness probe returns 503 before shutdown begins.
	readyFlag atomic.Int32
}

// NewHandler creates a Handler backed by the provided Metrics instance.
// The handler starts in the "not ready" state; call SetReady(true) only
// after the Postgres pool is confirmed healthy, the schema migration has run,
// and the consumption loop goroutine is running.
func NewHandler(m *metrics.Metrics) *Handler {
	return &Handler{metrics: m}
}

// SetReady marks the handler as ready (true) or not ready (false).
// It is called by main during the startup sequence (true) and as the very
// first action upon receiving a SIGTERM (false) so Kubernetes stops routing
// new traffic to this pod before any shutdown logic runs.
func (h *Handler) SetReady(ready bool) {
	if ready {
		h.readyFlag.Store(1)
	} else {
		h.readyFlag.Store(0)
	}
}

// RegisterRoutes attaches all collector HTTP endpoints to the given Gin engine.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	// Liveness probe — Kubernetes restarts the pod if this returns non-2xx.
	r.GET("/health", h.handleHealth)
	// Readiness probe — Kubernetes stops sending traffic if this returns non-2xx.
	r.GET("/ready", h.handleReady)
	// JSON metrics snapshot for dashboards and alerting.
	r.GET("/metrics", h.handleMetrics)
}

// handleHealth handles GET /health.
// Always returns 200 OK with a minimal JSON body while the process is running.
// Kubernetes uses this as a liveness probe to detect deadlocked or crashed pods.
func (h *Handler) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// handleReady handles GET /ready.
// Returns 200 OK only when all three startup conditions are satisfied (Postgres
// pool healthy, schema migration complete, consumption loop running).
// Returns 503 Service Unavailable during startup and graceful shutdown.
func (h *Handler) handleReady(c *gin.Context) {
	if h.readyFlag.Load() == 1 {
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
		return
	}
	c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready"})
}

// handleMetrics handles GET /metrics.
// Returns a JSON snapshot of the collector's six observability counters.
// All values are point-in-time reads of atomic fields, so no locking is needed.
//
// Metric semantics:
//   - messages_consumed_total: cumulative messages pulled from the queue.
//   - db_writes_success_total: cumulative successful batch bulk-insert commits.
//   - db_writes_error_total: cumulative failed bulk-inserts (triggers redelivery).
//   - validation_errors_total: individual messages dropped due to bad formatting.
//   - last_db_write_timestamp_seconds: Unix timestamp of the last successful write.
//   - uptime_seconds: seconds elapsed since process start.
//
// A stale last_db_write_timestamp while messages_consumed_total grows indicates
// a DB write-path failure. Both values being stale indicates the loop has stalled.
func (h *Handler) handleMetrics(c *gin.Context) {
	// Snapshot() atomically collects all counter values and converts
	// the nanosecond timestamp to fractional seconds for easy consumption.
	snap := h.metrics.Snapshot()
	c.JSON(http.StatusOK, snap)
}
