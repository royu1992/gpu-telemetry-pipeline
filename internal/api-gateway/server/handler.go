package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/api-gateway/cache"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/api-gateway/metrics"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/store"
)

// ─── interfaces ───────────────────────────────────────────────────────────────

// TelemetryReader abstracts the store read-path for testability. In production
// it is implemented by *store.Store; in tests a simple stub is used.
type TelemetryReader interface {
	// GPUExists returns true if any telemetry rows are stored for uuid.
	GPUExists(ctx context.Context, uuid string) (bool, error)
	// GetTelemetry returns time-series data for uuid within the given filter.
	GetTelemetry(ctx context.Context, uuid string, f store.TelemetryFilter) ([]store.TelemetryEntry, error)
}

// ─── Handler ──────────────────────────────────────────────────────────────────

// Handler owns all dependencies needed by the API Gateway HTTP handlers and
// provides a single RegisterRoutes method that wires them to a Gin engine.
//
// All fields are read-only after construction so Handler is safe for concurrent
// use by multiple Gin goroutines.
type Handler struct {
	// store provides GPU-existence checks and time-series queries.
	store TelemetryReader

	// cache provides the cached GPU list with a TTL refresh strategy.
	cache *cache.Cache

	// metrics tracks observability counters for the gateway.
	metrics *metrics.Metrics

	// queryTimeout is the per-request deadline applied to every database query.
	queryTimeout time.Duration

	// maxRows caps the number of telemetry rows returned per request.
	maxRows int

	// ready is 1 when the service is ready to serve traffic (store connected),
	// 0 during startup or after a fatal error.
	ready atomic.Int32
}

// NewHandler creates a Handler with the given dependencies.
// Call SetReady(true) after successfully connecting to the database to allow
// the /ready probe to return HTTP 200.
func NewHandler(
	st TelemetryReader,
	c *cache.Cache,
	m *metrics.Metrics,
	queryTimeout time.Duration,
	maxRows int,
) *Handler {
	return &Handler{
		store:        st,
		cache:        c,
		metrics:      m,
		queryTimeout: queryTimeout,
		maxRows:      maxRows,
	}
}

// SetReady toggles the readiness state reported by the /ready endpoint.
// Call with true once the store is connected; call with false on shutdown.
func (h *Handler) SetReady(v bool) {
	// atomic.Int32 is used so concurrent readers (the /ready handler) never
	// observe a torn write.
	if v {
		h.ready.Store(1)
	} else {
		h.ready.Store(0)
	}
}

// RegisterRoutes attaches all API Gateway routes to the provided Gin engine.
// The route layout is:
//
//	GET /health                        – liveness probe (always 200 once started)
//	GET /ready                         – readiness probe (200 when store is connected)
//	GET /metrics                       – plain-text observability counters
//	GET /api/v1/gpus                   – list all distinct GPUs (cached)
//	GET /api/v1/gpus/:id/telemetry     – time-series data for a single GPU
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	// Probes and observability are served at the root level with no version prefix.
	r.GET("/health", h.handleHealth)
	r.GET("/ready", h.handleReady)
	r.GET("/metrics", h.handleMetrics)

	// All public API routes are versioned under /api/v1.
	v1 := r.Group("/api/v1")
	v1.GET("/gpus", h.handleListGPUs)
	v1.GET("/gpus/:id/telemetry", h.handleGetTelemetry)
}

// ─── Probe handlers ───────────────────────────────────────────────────────────

// handleHealth responds with HTTP 200 and a static JSON body. It is intended
// for liveness checks that simply verify the process is alive and able to
// accept connections. It never fails once the server is running.
//
// @Summary      Liveness probe
// @Description  Returns 200 while the api-gateway process is alive.
// @Tags         operations
// @Produce      json
// @Success      200  {object}  map[string]string
// @Router       /health [get]
func (h *Handler) handleHealth(c *gin.Context) {
	// Always OK — if the process is reachable, it is alive.
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// handleReady responds with HTTP 200 when the gateway is ready to serve API
// traffic (i.e., the database connection has been established), or HTTP 503
// while the store is still connecting or unavailable.
//
// @Summary      Readiness probe
// @Description  Returns 200 when the database connection is established. Returns 503 during startup or when the DB is unavailable.
// @Tags         operations
// @Produce      json
// @Success      200  {object}  map[string]string
// @Failure      503  {object}  map[string]string
// @Router       /ready [get]
func (h *Handler) handleReady(c *gin.Context) {
	// Read the atomic flag set by SetReady.
	if h.ready.Load() == 1 {
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
		return
	}
	// 503 tells load-balancers and Kubernetes to withhold traffic.
	c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready"})
}

// handleMetrics renders all gateway observability counters as plain text in
// the "key: value\n" format consistent with other services in the pipeline.
//
// @Summary      Gateway metrics
// @Description  Returns plain-text observability counters: requests total, success, error, cache hits/misses, and DB query errors.
// @Tags         operations
// @Produce      text/plain
// @Success      200  {string}  string
// @Router       /metrics [get]
func (h *Handler) handleMetrics(c *gin.Context) {
	// Format returns a pre-allocated string; no error path.
	c.String(http.StatusOK, h.metrics.Snapshot().Format())
}

// ─── API handlers ─────────────────────────────────────────────────────────────

// handleListGPUs returns the full set of distinct GPUs stored in the database.
// The result is served from an in-memory cache that refreshes on the first
// request after the configured TTL expires.
//
// Response: HTTP 200 with a JSON array of GPUSummary objects.
// The array is always present (may be empty); it is never null.
//
//	[{"id":"GPU-uuid","hostname":"node","gpu_id":"0","model_name":"NVIDIA H100"}]
//
// @Summary      List all GPUs
// @Description  Returns a list of all GPUs for which telemetry data is available, served from an in-memory cache.
// @Tags         gpus
// @Produce      json
// @Success      200  {array}   store.GPUSummary  "List of GPU summaries"
// @Failure      500  {object}  map[string]string "Internal server error"
// @Failure      503  {object}  map[string]string "Service unavailable"
// @Router       /gpus [get]
func (h *Handler) handleListGPUs(c *gin.Context) {
	// Count every inbound request for observability.
	h.metrics.IncRequests()

	// Apply the per-request query deadline. The parent context carries any
	// caller-supplied deadline; we only tighten it, never loosen it.
	ctx, cancel := context.WithTimeout(c.Request.Context(), h.queryTimeout)
	defer cancel()

	// Delegate to the cache. hit=true means no DB query was made.
	gpus, hit, err := h.cache.ListGPUs(ctx)
	if err != nil {
		// Classify the error for telemetry and return an appropriate status.
		h.metrics.IncRequestsError()
		h.metrics.IncDBQueryError()
		h.respondError(c, err)
		return
	}

	// Update cache observability counters.
	if hit {
		h.metrics.IncCacheHit()
	} else {
		h.metrics.IncCacheMiss()
	}

	// Always return a non-null JSON array; an empty cluster returns [].
	h.metrics.IncRequestsSuccess()
	c.JSON(http.StatusOK, gpus)
}

// handleGetTelemetry returns time-series telemetry for a single GPU identified
// by its hardware UUID (:id path parameter).
//
// Query parameters:
//
//	start_time  RFC3339 timestamp (inclusive lower bound). Default: now − 1 hour.
//	end_time    RFC3339 timestamp (inclusive upper bound). Default: now.
//
// Response: HTTP 200 with a JSON object:
//
//	{"id":"GPU-uuid","count":2,"data":[...]}
//
// Error responses:
//
//	400 – invalid start_time / end_time format, or start_time > end_time.
//	404 – uuid not present in the database.
//	504 – database query timed out.
//	503 – database unavailable.
//	500 – unexpected error.
//
// @Summary      Get GPU telemetry
// @Description  Returns telemetry entries for a specific GPU ordered by timestamp. Supports optional RFC3339 time window filters.
// @Tags         gpus
// @Produce      json
// @Param        id         path   string  true   "GPU hardware UUID (e.g. GPU-5fd4f087-86f3-7a43-b711-4771313afc50)"
// @Param        start_time query  string  false  "Start of the time window, RFC3339 format, inclusive. Default: 1 hour ago."
// @Param        end_time   query  string  false  "End of the time window, RFC3339 format, inclusive. Default: now."
// @Success      200  {object}  map[string]interface{}  "Telemetry response with id, count, and data array"
// @Failure      400  {object}  map[string]string       "Bad request – invalid time format or start > end"
// @Failure      404  {object}  map[string]string       "GPU not found"
// @Failure      500  {object}  map[string]string       "Internal server error"
// @Failure      503  {object}  map[string]string       "Service unavailable"
// @Failure      504  {object}  map[string]string       "Gateway timeout"
// @Router       /gpus/{id}/telemetry [get]
func (h *Handler) handleGetTelemetry(c *gin.Context) {
	// Count every inbound request for observability.
	h.metrics.IncRequests()

	// Extract and validate the UUID path parameter. Gin guarantees :id is
	// present when this handler is invoked; we still validate it is non-empty
	// to guard against edge cases from future route changes.
	uuid := c.Param("id")
	if uuid == "" {
		h.metrics.IncRequestsError()
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing GPU id"})
		return
	}

	// Parse optional start_time query parameter (RFC3339). Default: now − 1h.
	now := time.Now().UTC()
	startTime, err := parseTimeParam(c.Query("start_time"), now.Add(-1*time.Hour))
	if err != nil {
		h.metrics.IncRequestsError()
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid start_time: %s", err)})
		return
	}

	// Parse optional end_time query parameter (RFC3339). Default: now.
	endTime, err := parseTimeParam(c.Query("end_time"), now)
	if err != nil {
		h.metrics.IncRequestsError()
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid end_time: %s", err)})
		return
	}

	// Validate time ordering. A reversed window is always a caller mistake.
	if startTime.After(endTime) {
		h.metrics.IncRequestsError()
		c.JSON(http.StatusBadRequest, gin.H{"error": "start_time must not be after end_time"})
		return
	}

	// Apply the per-request query deadline before touching the database.
	ctx, cancel := context.WithTimeout(c.Request.Context(), h.queryTimeout)
	defer cancel()

	// Probe the database to distinguish "unknown UUID" (→ 404) from
	// "UUID exists but empty window" (→ 200 with empty data array).
	exists, err := h.store.GPUExists(ctx, uuid)
	if err != nil {
		h.metrics.IncRequestsError()
		h.metrics.IncDBQueryError()
		h.respondError(c, err)
		return
	}
	if !exists {
		// The uuid is not in the database at all; surface a 404 with context.
		h.metrics.IncRequestsError()
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("GPU %q not found", uuid)})
		return
	}

	// Build the filter with the operator-configured row cap.
	filter := store.TelemetryFilter{
		StartTime: startTime,
		EndTime:   endTime,
		Limit:     h.maxRows,
	}

	// Fetch telemetry rows from the database.
	entries, err := h.store.GetTelemetry(ctx, uuid, filter)
	if err != nil {
		h.metrics.IncRequestsError()
		h.metrics.IncDBQueryError()
		h.respondError(c, err)
		return
	}

	// Wrap the result in a JSON envelope with the requested id and row count.
	// The data field is always an array (never null), even when empty.
	h.metrics.IncRequestsSuccess()
	c.JSON(http.StatusOK, gin.H{
		"id":    uuid,
		"count": len(entries),
		"data":  entries,
	})
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// parseTimeParam parses an RFC3339 timestamp string into a UTC time.Time.
// If raw is empty, defaultTime is returned unchanged. This lets callers
// provide optional query parameters with sensible defaults.
func parseTimeParam(raw string, defaultTime time.Time) (time.Time, error) {
	if raw == "" {
		// No parameter supplied; use the caller-provided default.
		return defaultTime, nil
	}

	// time.RFC3339 is the only accepted format to keep the API unambiguous.
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is not a valid RFC3339 timestamp", raw)
	}

	// Normalise to UTC so comparisons and log messages are consistent.
	return t.UTC(), nil
}

// respondError maps common infrastructure errors to appropriate HTTP status
// codes and writes a JSON error body. Unknown errors fall through to 500.
func (h *Handler) respondError(c *gin.Context, err error) {
	// context.DeadlineExceeded means the database query hit the per-request
	// timeout; report it as a 504 Gateway Timeout.
	if errors.Is(err, context.DeadlineExceeded) {
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "database query timed out"})
		return
	}

	// context.Canceled typically means the client disconnected. We still
	// log it as a 503 since from the perspective of the caller the request
	// was not completed successfully.
	if errors.Is(err, context.Canceled) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "request cancelled"})
		return
	}

	// All other errors are unexpected; return 500 without leaking internals.
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
}
