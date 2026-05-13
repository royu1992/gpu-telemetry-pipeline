package metrics

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Metrics holds the observability counters for the api-gateway service.
// All fields use atomic operations so the Gin goroutines can write to them
// concurrently without a mutex.
type Metrics struct {
	// requestsTotal counts every HTTP request received, across all endpoints.
	requestsTotal atomic.Int64

	// requestsSuccessTotal counts requests that returned a 2xx status code.
	requestsSuccessTotal atomic.Int64

	// requestsErrorTotal counts requests that returned a 4xx or 5xx status code.
	requestsErrorTotal atomic.Int64

	// gpuListCacheHitsTotal counts times GET /gpus was served from
	// the in-memory cache without querying the database.
	gpuListCacheHitsTotal atomic.Int64

	// gpuListCacheMissesTotal counts times GET /gpus required a
	// live database query because the cache was cold or expired.
	gpuListCacheMissesTotal atomic.Int64

	// dbQueryErrorsTotal counts all database errors encountered across all
	// query types (timeouts, connection failures, scan errors).
	dbQueryErrorsTotal atomic.Int64

	// startTime is captured at construction and used to compute uptime_seconds
	// in the snapshot without storing a separate elapsed counter.
	startTime time.Time
}

// Snapshot is a point-in-time copy of all metrics values, ready for plain-text
// serialisation and HTTP delivery via GET /metrics.
type Snapshot struct {
	RequestsTotal           int64 `json:"requests_total"`
	RequestsSuccessTotal    int64 `json:"requests_success_total"`
	RequestsErrorTotal      int64 `json:"requests_error_total"`
	GPUListCacheHitsTotal   int64 `json:"gpu_list_cache_hits_total"`
	GPUListCacheMissesTotal int64 `json:"gpu_list_cache_misses_total"`
	DBQueryErrorsTotal      int64 `json:"db_query_errors_total"`
	UptimeSeconds           int64 `json:"uptime_seconds"`
}

// New creates an initialised Metrics instance with all counters at zero and
// the start time recorded at the moment of construction.
func New() *Metrics {
	return &Metrics{startTime: time.Now()}
}

// IncRequests increments the total request counter by one.
// Called at the start of every handler invocation.
func (m *Metrics) IncRequests() {
	m.requestsTotal.Add(1)
}

// IncRequestsSuccess increments the success counter by one.
// Called when a handler returns a 2xx status code.
func (m *Metrics) IncRequestsSuccess() {
	m.requestsSuccessTotal.Add(1)
}

// IncRequestsError increments the error counter by one.
// Called when a handler returns a 4xx or 5xx status code.
func (m *Metrics) IncRequestsError() {
	m.requestsErrorTotal.Add(1)
}

// IncCacheHit increments the GPU list cache-hit counter by one.
// Called when GET /gpus is served from the in-memory cache.
func (m *Metrics) IncCacheHit() {
	m.gpuListCacheHitsTotal.Add(1)
}

// IncCacheMiss increments the GPU list cache-miss counter by one.
// Called when GET /gpus must query the database.
func (m *Metrics) IncCacheMiss() {
	m.gpuListCacheMissesTotal.Add(1)
}

// IncDBQueryError increments the database-error counter by one.
// Called whenever a store query returns a non-nil error.
func (m *Metrics) IncDBQueryError() {
	m.dbQueryErrorsTotal.Add(1)
}

// Snapshot atomically reads all counters and returns them in a Snapshot struct.
// The uptime is computed from the startTime captured at construction.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		RequestsTotal:           m.requestsTotal.Load(),
		RequestsSuccessTotal:    m.requestsSuccessTotal.Load(),
		RequestsErrorTotal:      m.requestsErrorTotal.Load(),
		GPUListCacheHitsTotal:   m.gpuListCacheHitsTotal.Load(),
		GPUListCacheMissesTotal: m.gpuListCacheMissesTotal.Load(),
		DBQueryErrorsTotal:      m.dbQueryErrorsTotal.Load(),
		UptimeSeconds:           int64(time.Since(m.startTime).Seconds()),
	}
}

// Format returns the plain-text representation of the snapshot, consistent
// with the key: value format used by the Streamer and Collector services.
func (s Snapshot) Format() string {
	return fmt.Sprintf(
		"requests_total: %d\n"+
			"requests_success_total: %d\n"+
			"requests_error_total: %d\n"+
			"gpu_list_cache_hits_total: %d\n"+
			"gpu_list_cache_misses_total: %d\n"+
			"db_query_errors_total: %d\n"+
			"uptime_seconds: %d\n",
		s.RequestsTotal,
		s.RequestsSuccessTotal,
		s.RequestsErrorTotal,
		s.GPUListCacheHitsTotal,
		s.GPUListCacheMissesTotal,
		s.DBQueryErrorsTotal,
		s.UptimeSeconds,
	)
}
