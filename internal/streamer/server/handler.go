package server

import (
	"net/http"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/metrics"
)

// Handler registers the streamer's three operational HTTP endpoints:
//   - GET /health  — liveness probe (always 200 while the process is alive)
//   - GET /ready   — readiness probe (200 only when the loop is running)
//   - GET /metrics — current observability snapshot in JSON
//
// The Handler is safe for concurrent use: readyFlag is an atomic, and metrics
// values are read via atomic operations inside metrics.Snapshot().
type Handler struct {
	// metrics is the shared Metrics instance written by the telemetry loop and
	// read here when serving GET /metrics.
	metrics *metrics.Metrics

	// readyFlag is set to 1 by main once the telemetry loop is running, and
	// back to 0 during graceful shutdown so the readiness probe returns 503
	// before any active connections are severed.
	readyFlag atomic.Int32
}

// NewHandler creates a Handler that reads from the provided Metrics instance.
// The handler starts in the "not ready" state; call SetReady(true) once the
// telemetry loop goroutine is confirmed to be running.
func NewHandler(m *metrics.Metrics) *Handler {
	return &Handler{metrics: m}
}

// SetReady marks the handler as ready (true) or not ready (false).
// It is called by main after the loop goroutine starts, and again during
// graceful shutdown to stop Kubernetes from sending new traffic to the pod.
func (h *Handler) SetReady(ready bool) {
	if ready {
		h.readyFlag.Store(1)
	} else {
		h.readyFlag.Store(0)
	}
}

// RegisterRoutes attaches all streamer HTTP endpoints to the given Gin engine.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	// Liveness probe — Kubernetes restarts the pod if this returns non-2xx.
	r.GET("/health", h.handleHealth)
	// Readiness probe — Kubernetes stops sending traffic if this returns non-2xx.
	r.GET("/ready", h.handleReady)
	// Metrics snapshot consumed by Prometheus or a JSON dashboard.
	r.GET("/metrics", h.handleMetrics)
}

// handleHealth handles GET /health.
// Always returns 200 OK with a minimal JSON body while the process is running.
// Kubernetes uses this as a liveness probe to detect deadlocked or crashed pods.
func (h *Handler) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// handleReady handles GET /ready.
// Returns 200 OK when the telemetry loop is actively running, or 503 Service
// Unavailable during startup (before the loop is confirmed running) and during
// graceful shutdown (after SetReady(false) is called). Kubernetes uses this as
// a readiness probe to stop routing traffic to pods that are not yet ready or
// that are draining.
func (h *Handler) handleReady(c *gin.Context) {
	if h.readyFlag.Load() == 1 {
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
		return
	}
	c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready"})
}

// handleMetrics handles GET /metrics.
// Returns a JSON snapshot of the four observability indicators. Values are
// point-in-time reads of atomic counters and gauges maintained by the telemetry
// loop goroutine.
//
// Metric semantics:
//   - rows_sent_total: cumulative count of rows successfully delivered to the queue.
//   - errors_total: cumulative count of send failures and bad-row skips.
//   - last_sent_timestamp_seconds: Unix timestamp (float64) of the last successful POST.
//   - last_row_read_timestamp_seconds: Unix timestamp (float64) of the last successful read.
//
// A stale last_sent_timestamp while last_row_read_timestamp advances indicates
// an output bottleneck (queue unreachable or full). Both timestamps being stale
// indicates the loop is stuck reading or has exited.
func (h *Handler) handleMetrics(c *gin.Context) {
	// Snapshot() collects all four atomic values in a single call and converts
	// nanosecond timestamps to fractional seconds for easy consumption.
	snap := h.metrics.Snapshot()
	c.JSON(http.StatusOK, snap)
}
