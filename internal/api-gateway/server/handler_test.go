package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/api-gateway/cache"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/api-gateway/metrics"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/store"
)

func init() {
	// Suppress Gin debug output from all tests.
	gin.SetMode(gin.TestMode)
}

// ─── mock TelemetryReader ─────────────────────────────────────────────────────

// mockReader is a test double for TelemetryReader.
type mockReader struct {
	exists    bool
	existsErr error
	entries   []store.TelemetryEntry
	teleErr   error
}

// GPUExists satisfies TelemetryReader.
func (m *mockReader) GPUExists(_ context.Context, _ string) (bool, error) {
	return m.exists, m.existsErr
}

// GetTelemetry satisfies TelemetryReader.
func (m *mockReader) GetTelemetry(_ context.Context, _ string, _ store.TelemetryFilter) ([]store.TelemetryEntry, error) {
	return m.entries, m.teleErr
}

// ─── mock GPULister (for cache) ───────────────────────────────────────────────

// mockListerForHandler satisfies cache.GPULister.
type mockListerForHandler struct {
	gpus []store.GPUSummary
	err  error
}

// ListGPUs satisfies cache.GPULister.
func (m *mockListerForHandler) ListGPUs(_ context.Context) ([]store.GPUSummary, error) {
	return m.gpus, m.err
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// newTestHandler builds a Handler wired to the given mocks and registers its
// routes on a fresh test-mode Gin engine. The returned *httptest.Server is
// already started; callers must call ts.Close() when done.
func newTestHandler(t *testing.T, reader TelemetryReader, lister cache.GPULister) (*Handler, *gin.Engine) {
	t.Helper()
	m := metrics.New()
	c := cache.New(lister, 1*time.Minute)
	h := NewHandler(reader, c, m, 5*time.Second, 1000)
	engine := New("*", 1<<20)
	h.RegisterRoutes(engine)
	return h, engine
}

// doRequest performs an HTTP GET against the Gin engine and returns the recorder.
func doRequest(engine *gin.Engine, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	engine.ServeHTTP(w, req)
	return w
}

// ─── NewHandler ───────────────────────────────────────────────────────────────

// TestNewHandler_NonNil verifies that NewHandler returns a non-nil Handler.
func TestNewHandler_NonNil(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
	h, _ := newTestHandler(t, reader, lister)
	if h == nil {
		t.Fatal("NewHandler() returned nil")
	}
}

// ─── SetReady ─────────────────────────────────────────────────────────────────

// TestSetReady_TogglesState exercises both true and false transitions.
func TestSetReady_TogglesState(t *testing.T) {
	tests := []struct {
		name     string
		set      bool
		wantCode int
	}{
		{"not_ready", false, http.StatusServiceUnavailable},
		{"ready", true, http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &mockReader{}
			lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
			h, engine := newTestHandler(t, reader, lister)
			h.SetReady(tc.set)
			w := doRequest(engine, "/ready")
			if w.Code != tc.wantCode {
				t.Errorf("SetReady(%v) → /ready: got %d, want %d", tc.set, w.Code, tc.wantCode)
			}
		})
	}
}

// ─── /health ─────────────────────────────────────────────────────────────────

// TestHandleHealth verifies the liveness probe always returns 200.
func TestHandleHealth(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
	_, engine := newTestHandler(t, reader, lister)

	w := doRequest(engine, "/health")

	if w.Code != http.StatusOK {
		t.Errorf("/health: got %d, want 200", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf(`body["status"] = %q, want "ok"`, body["status"])
	}
}

// ─── /ready ──────────────────────────────────────────────────────────────────

// TestHandleReady_NotReady verifies that before SetReady(true) the probe returns 503.
func TestHandleReady_NotReady(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
	_, engine := newTestHandler(t, reader, lister)

	// SetReady is NOT called, so ready flag is 0.
	w := doRequest(engine, "/ready")
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("/ready (not ready): got %d, want 503", w.Code)
	}
}

// TestHandleReady_Ready verifies that after SetReady(true) the probe returns 200.
func TestHandleReady_Ready(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
	h, engine := newTestHandler(t, reader, lister)
	h.SetReady(true)

	w := doRequest(engine, "/ready")
	if w.Code != http.StatusOK {
		t.Errorf("/ready (ready): got %d, want 200", w.Code)
	}
}

// ─── /metrics ─────────────────────────────────────────────────────────────────

// TestHandleMetrics_OK verifies the metrics endpoint returns 200 and plain text.
func TestHandleMetrics_OK(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
	_, engine := newTestHandler(t, reader, lister)

	w := doRequest(engine, "/metrics")

	if w.Code != http.StatusOK {
		t.Errorf("/metrics: got %d, want 200", w.Code)
	}
	if w.Body.Len() == 0 {
		t.Error("/metrics: empty body")
	}
}

// ─── GET /gpus ────────────────────────────────────────────────────────

// TestHandleListGPUs_Success verifies that a successful cache/DB call returns
// HTTP 200 with the GPU list as a JSON array.
func TestHandleListGPUs_Success(t *testing.T) {
	gpuList := []store.GPUSummary{
		{ID: "GPU-abc", Hostname: "node1", GpuID: "0", ModelName: "H100"},
	}
	reader := &mockReader{}
	lister := &mockListerForHandler{gpus: gpuList}
	_, engine := newTestHandler(t, reader, lister)

	w := doRequest(engine, "/gpus")

	if w.Code != http.StatusOK {
		t.Errorf("/gpus: got %d, want 200", w.Code)
	}
	var result []store.GPUSummary
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if len(result) != 1 || result[0].ID != "GPU-abc" {
		t.Errorf("unexpected GPU list: %+v", result)
	}
}

// TestHandleListGPUs_DBError verifies that a store error surfaces as 500.
func TestHandleListGPUs_DBError(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{err: errors.New("db down")}
	_, engine := newTestHandler(t, reader, lister)

	w := doRequest(engine, "/gpus")

	if w.Code != http.StatusInternalServerError {
		t.Errorf("/gpus (error): got %d, want 500", w.Code)
	}
}

// TestHandleListGPUs_DBTimeout verifies that a context.DeadlineExceeded from
// the cache/store is mapped to HTTP 504.
func TestHandleListGPUs_DBTimeout(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{err: context.DeadlineExceeded}
	_, engine := newTestHandler(t, reader, lister)

	w := doRequest(engine, "/gpus")

	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("/gpus (timeout): got %d, want 504", w.Code)
	}
}

// TestHandleListGPUs_CacheMissIncrementsCacheMiss verifies cache-miss counters
// are incremented when the cache is cold.
func TestHandleListGPUs_CacheMissIncrementsCacheMiss(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
	m := metrics.New()
	c := cache.New(lister, 1*time.Minute)
	h := NewHandler(reader, c, m, 5*time.Second, 1000)
	engine := New("*", 1<<20)
	h.RegisterRoutes(engine)

	// Cold cache → miss.
	doRequest(engine, "/gpus")
	snap := m.Snapshot()
	if snap.GPUListCacheMissesTotal != 1 {
		t.Errorf("cache miss counter: got %d, want 1", snap.GPUListCacheMissesTotal)
	}
}

// TestHandleListGPUs_CacheHitIncrementsHit verifies cache-hit counters are
// incremented on the second request within the TTL.
func TestHandleListGPUs_CacheHitIncrementsHit(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
	m := metrics.New()
	c := cache.New(lister, 1*time.Minute)
	h := NewHandler(reader, c, m, 5*time.Second, 1000)
	engine := New("*", 1<<20)
	h.RegisterRoutes(engine)

	// First call: cold cache miss.
	doRequest(engine, "/gpus")
	// Second call: warm cache hit.
	doRequest(engine, "/gpus")

	snap := m.Snapshot()
	if snap.GPUListCacheHitsTotal != 1 {
		t.Errorf("cache hit counter: got %d, want 1", snap.GPUListCacheHitsTotal)
	}
}

// ─── GET /gpus/:id/telemetry ──────────────────────────────────────────

// telemetryTestCase is a table-driven row for handleGetTelemetry tests.
type telemetryTestCase struct {
	name     string
	path     string
	reader   *mockReader
	wantCode int
}

// TestHandleGetTelemetry_EmptyID exercises the uuid == "" guard in
// handleGetTelemetry. Gin normally guarantees :id is non-empty, but the guard
// exists as a defence-in-depth check; we call the handler method directly to
// reach that branch.
func TestHandleGetTelemetry_EmptyID(t *testing.T) {
	reader := &mockReader{}
	lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
	h, _ := newTestHandler(t, reader, lister)

	// Build a Gin context with an empty id param by calling the handler directly.
	w := httptest.NewRecorder()
	gc, _ := gin.CreateTestContext(w)
	gc.Request = httptest.NewRequest(http.MethodGet, "/gpus//telemetry", nil)
	// Do not set the "id" param so c.Param("id") returns "".
	h.handleGetTelemetry(gc)

	if w.Code != http.StatusBadRequest {
		t.Errorf("empty id: got %d, want 400", w.Code)
	}
}

// TestHandleGetTelemetry_Table exercises the main handler paths via a table.
func TestHandleGetTelemetry_Table(t *testing.T) {
	ts := time.Now().UTC()
	sampleEntries := []store.TelemetryEntry{
		{Timestamp: ts, MetricName: "DCGM_FI_DEV_GPU_UTIL", Value: 98.5,
			Hostname: "node1", GpuID: "0", ModelName: "H100"},
	}

	cases := []telemetryTestCase{
		{
			name: "success_default_window",
			path: "/gpus/GPU-abc/telemetry",
			reader: &mockReader{
				exists:  true,
				entries: sampleEntries,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "success_explicit_window",
			path: "/gpus/GPU-abc/telemetry?start_time=2025-01-01T00:00:00Z&end_time=2025-01-01T01:00:00Z",
			reader: &mockReader{
				exists:  true,
				entries: sampleEntries,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "success_empty_data_within_window",
			path: "/gpus/GPU-abc/telemetry",
			reader: &mockReader{
				exists:  true,
				entries: []store.TelemetryEntry{},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "gpu_not_found",
			path:     "/gpus/GPU-unknown/telemetry",
			reader:   &mockReader{exists: false},
			wantCode: http.StatusNotFound,
		},
		{
			name: "exists_check_db_error_returns_500",
			path: "/gpus/GPU-abc/telemetry",
			reader: &mockReader{
				existsErr: errors.New("connection refused"),
			},
			wantCode: http.StatusInternalServerError,
		},
		{
			name: "exists_check_timeout_returns_504",
			path: "/gpus/GPU-abc/telemetry",
			reader: &mockReader{
				existsErr: context.DeadlineExceeded,
			},
			wantCode: http.StatusGatewayTimeout,
		},
		{
			name: "telemetry_query_error_returns_500",
			path: "/gpus/GPU-abc/telemetry",
			reader: &mockReader{
				exists:  true,
				teleErr: errors.New("query failed"),
			},
			wantCode: http.StatusInternalServerError,
		},
		{
			name: "telemetry_query_timeout_returns_504",
			path: "/gpus/GPU-abc/telemetry",
			reader: &mockReader{
				exists:  true,
				teleErr: context.DeadlineExceeded,
			},
			wantCode: http.StatusGatewayTimeout,
		},
		{
			name:     "invalid_start_time_returns_400",
			path:     "/gpus/GPU-abc/telemetry?start_time=not-a-date",
			reader:   &mockReader{exists: true},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_end_time_returns_400",
			path:     "/gpus/GPU-abc/telemetry?end_time=not-a-date",
			reader:   &mockReader{exists: true},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "start_after_end_returns_400",
			path:     "/gpus/GPU-abc/telemetry?start_time=2025-01-01T02:00:00Z&end_time=2025-01-01T01:00:00Z",
			reader:   &mockReader{exists: true},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
			_, engine := newTestHandler(t, tc.reader, lister)

			w := doRequest(engine, tc.path)

			if w.Code != tc.wantCode {
				t.Errorf("path=%q: got %d, want %d (body: %s)",
					tc.path, w.Code, tc.wantCode, w.Body.String())
			}
		})
	}
}

// TestHandleGetTelemetry_ResponseShape verifies the JSON envelope for a
// successful telemetry response has the correct id, count, and data fields.
func TestHandleGetTelemetry_ResponseShape(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	entry := store.TelemetryEntry{
		Timestamp:  ts,
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		Value:      75.0,
		Hostname:   "node1",
		GpuID:      "0",
		ModelName:  "H100",
	}
	reader := &mockReader{exists: true, entries: []store.TelemetryEntry{entry}}
	lister := &mockListerForHandler{gpus: []store.GPUSummary{}}
	_, engine := newTestHandler(t, reader, lister)

	w := doRequest(engine, "/gpus/GPU-abc/telemetry")

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", w.Code)
	}

	// Decode envelope.
	var envelope struct {
		ID    string                 `json:"id"`
		Count int                    `json:"count"`
		Data  []store.TelemetryEntry `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if envelope.ID != "GPU-abc" {
		t.Errorf("id: got %q, want %q", envelope.ID, "GPU-abc")
	}
	if envelope.Count != 1 {
		t.Errorf("count: got %d, want 1", envelope.Count)
	}
	if len(envelope.Data) != 1 {
		t.Fatalf("data length: got %d, want 1", len(envelope.Data))
	}
	if envelope.Data[0].Value != 75.0 {
		t.Errorf("data[0].value: got %v, want 75.0", envelope.Data[0].Value)
	}
}

// ─── parseTimeParam ───────────────────────────────────────────────────────────

// TestParseTimeParam exercises all branches of the helper function.
func TestParseTimeParam(t *testing.T) {
	defaultTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	validTime := "2025-06-15T10:00:00Z"
	parsedTime := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		raw      string
		wantTime time.Time
		wantErr  bool
	}{
		{"empty_returns_default", "", defaultTime, false},
		{"valid_rfc3339_parsed", validTime, parsedTime, false},
		{"invalid_format_error", "2025-01-01", time.Time{}, true},
		{"invalid_string_error", "not-a-date", time.Time{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTimeParam(tc.raw, defaultTime)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseTimeParam(%q): expected error, got nil", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTimeParam(%q): unexpected error: %v", tc.raw, err)
			}
			if !got.Equal(tc.wantTime) {
				t.Errorf("parseTimeParam(%q): got %v, want %v", tc.raw, got, tc.wantTime)
			}
		})
	}
}

// ─── respondError ─────────────────────────────────────────────────────────────

// TestRespondError_Table verifies all error-to-status-code mappings.
func TestRespondError_Table(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode int
	}{
		{"deadline_exceeded_504", context.DeadlineExceeded, http.StatusGatewayTimeout},
		{"context_canceled_503", context.Canceled, http.StatusServiceUnavailable},
		{"generic_error_500", errors.New("boom"), http.StatusInternalServerError},
		{"wrapped_deadline_504", fmt.Errorf("query: %w", context.DeadlineExceeded), http.StatusGatewayTimeout},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := metrics.New()
			c := cache.New(&mockListerForHandler{gpus: []store.GPUSummary{}}, time.Minute)
			h := NewHandler(&mockReader{}, c, m, 5*time.Second, 1000)

			// Build a minimal Gin context backed by a recorder.
			w := httptest.NewRecorder()
			gc, engine := gin.CreateTestContext(w)
			_ = engine
			gc.Request = httptest.NewRequest(http.MethodGet, "/", nil)

			h.respondError(gc, tc.err)

			if w.Code != tc.wantCode {
				t.Errorf("respondError(%T): got %d, want %d", tc.err, w.Code, tc.wantCode)
			}
		})
	}
}
