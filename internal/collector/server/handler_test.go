package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/metrics"
)

func init() {
	// Suppress Gin's default debug output in test logs.
	gin.SetMode(gin.TestMode)
}

// newTestEngine creates a Gin engine and a Handler wired together, returning
// both so tests can call SetReady and fire HTTP requests through the engine.
func newTestEngine(t *testing.T) (*gin.Engine, *Handler) {
	t.Helper()
	m := metrics.New()
	h := NewHandler(m)

	// Use the production New() so the middleware stack is also exercised.
	engine := New(1 << 20)
	h.RegisterRoutes(engine)

	return engine, h
}

// --- NewHandler ---

// TestNewHandler verifies that NewHandler returns a non-nil Handler and that
// the handler starts in the "not ready" state.
func TestNewHandler(t *testing.T) {
	m := metrics.New()
	h := NewHandler(m)

	if h == nil {
		t.Fatal("NewHandler returned nil")
	}
	// The ready flag must be 0 (not ready) immediately after construction.
	if h.readyFlag.Load() != 0 {
		t.Errorf("readyFlag: got %d, want 0 (not ready)", h.readyFlag.Load())
	}
}

// --- SetReady ---

// TestSetReady covers the true→1 and false→0 branches of SetReady and verifies
// idempotency by calling each path twice.
func TestSetReady(t *testing.T) {
	tests := []struct {
		name     string
		ready    bool
		wantFlag int32
	}{
		{"SetReady(true) sets flag to 1", true, 1},
		{"SetReady(false) sets flag to 0", false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := metrics.New()
			h := NewHandler(m)

			// Call twice to verify idempotency.
			h.SetReady(tt.ready)
			h.SetReady(tt.ready)

			got := h.readyFlag.Load()
			if got != tt.wantFlag {
				t.Errorf("readyFlag after SetReady(%v): got %d, want %d", tt.ready, got, tt.wantFlag)
			}
		})
	}
}

// TestSetReady_Transition verifies that flipping from ready to not-ready
// (and back) updates the flag correctly.
func TestSetReady_Transition(t *testing.T) {
	m := metrics.New()
	h := NewHandler(m)

	// Start not ready.
	if h.readyFlag.Load() != 0 {
		t.Fatalf("initial readyFlag must be 0")
	}

	// Transition to ready.
	h.SetReady(true)
	if h.readyFlag.Load() != 1 {
		t.Errorf("readyFlag after SetReady(true): got %d, want 1", h.readyFlag.Load())
	}

	// Transition back to not ready (simulating SIGTERM).
	h.SetReady(false)
	if h.readyFlag.Load() != 0 {
		t.Errorf("readyFlag after SetReady(false): got %d, want 0", h.readyFlag.Load())
	}
}

// --- GET /health ---

// TestHandleHealth verifies that GET /health always returns 200 OK with the
// expected JSON body regardless of the ready flag state.
func TestHandleHealth(t *testing.T) {
	tests := []struct {
		name  string
		ready bool // whether SetReady(true) has been called
	}{
		{"Health when not ready", false},
		{"Health when ready", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, h := newTestEngine(t)
			h.SetReady(tt.ready)

			// Fire the request through the Gin engine.
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			engine.ServeHTTP(w, req)

			// Liveness probe must always be 200.
			if w.Code != http.StatusOK {
				t.Errorf("status: got %d, want 200", w.Code)
			}

			// Decode the response body and check the status field.
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("unmarshal body: %v", err)
			}
			if body["status"] != "ok" {
				t.Errorf("body.status: got %q, want \"ok\"", body["status"])
			}
		})
	}
}

// --- GET /ready ---

// TestHandleReady verifies that GET /ready returns 200 when ready and 503
// when not, and includes the correct JSON status field in both cases.
func TestHandleReady(t *testing.T) {
	tests := []struct {
		name       string
		ready      bool
		wantStatus int
		wantBody   string
	}{
		{
			name:       "Not ready returns 503",
			ready:      false,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "not ready",
		},
		{
			name:       "Ready returns 200",
			ready:      true,
			wantStatus: http.StatusOK,
			wantBody:   "ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, h := newTestEngine(t)
			h.SetReady(tt.ready)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/ready", nil)
			engine.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status: got %d, want %d", w.Code, tt.wantStatus)
			}

			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("unmarshal body: %v", err)
			}
			// When not ready, we now return ErrorResponse { Error: "not ready" }
			// instead of ReadyResponse { Status: "not ready" }.
			got := body["status"]
			if tt.wantStatus != http.StatusOK {
				got = body["error"]
			}

			if got != tt.wantBody {
				t.Errorf("body status/error: got %q, want %q", got, tt.wantBody)
			}
		})
	}
}

// --- GET /metrics ---

// TestHandleMetrics verifies that GET /metrics returns 200 and the JSON fields
// match what the underlying Metrics instance reports.
func TestHandleMetrics(t *testing.T) {
	tests := []struct {
		name  string
		setup func(m *metrics.Metrics)
		check func(t *testing.T, snap map[string]interface{})
	}{
		{
			name:  "Fresh metrics — all counters zero",
			setup: func(m *metrics.Metrics) {},
			check: func(t *testing.T, snap map[string]interface{}) {
				t.Helper()
				// Expect zero for all cumulative counters.
				for _, field := range []string{
					"messages_consumed_total",
					"db_writes_success_total",
					"db_writes_error_total",
					"validation_errors_total",
					"last_db_write_timestamp_seconds",
				} {
					if v, ok := snap[field]; !ok || v.(float64) != 0 {
						t.Errorf("%s: got %v, want 0", field, v)
					}
				}
			},
		},
		{
			name: "After updates — counters reflected",
			setup: func(m *metrics.Metrics) {
				m.AddMessagesConsumed(10)
				m.IncDBWritesSuccess()
				m.IncDBWritesError()
				m.IncValidationError()
			},
			check: func(t *testing.T, snap map[string]interface{}) {
				t.Helper()
				if snap["messages_consumed_total"].(float64) != 10 {
					t.Errorf("messages_consumed_total: got %v, want 10", snap["messages_consumed_total"])
				}
				if snap["db_writes_success_total"].(float64) != 1 {
					t.Errorf("db_writes_success_total: got %v, want 1", snap["db_writes_success_total"])
				}
				if snap["db_writes_error_total"].(float64) != 1 {
					t.Errorf("db_writes_error_total: got %v, want 1", snap["db_writes_error_total"])
				}
				if snap["validation_errors_total"].(float64) != 1 {
					t.Errorf("validation_errors_total: got %v, want 1", snap["validation_errors_total"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build a dedicated metrics instance and apply the setup function
			// before wiring it to a handler so the test is self-contained.
			m := metrics.New()
			tt.setup(m)

			h := NewHandler(m)
			engine := New(1 << 20)
			h.RegisterRoutes(engine)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			engine.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("status: got %d, want 200", w.Code)
			}

			// Decode to a generic map for field-by-field inspection.
			var snap map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &snap); err != nil {
				t.Fatalf("unmarshal metrics body: %v", err)
			}

			tt.check(t, snap)
		})
	}
}
