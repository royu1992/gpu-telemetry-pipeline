package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/metrics"
)

func TestNewHandler(t *testing.T) {
	// Step: Verify NewHandler returns a non-nil Handler.
	m := metrics.New()
	h := NewHandler(m)
	if h == nil {
		t.Fatal("NewHandler() returned nil")
	}
}

func TestHandler_SetReady(t *testing.T) {
	tests := []struct {
		name     string
		ready    bool
		wantFlag int32
	}{
		{
			// Step: Calling SetReady(true) should store 1 in the atomic flag.
			name:     "SetReady true sets flag to 1",
			ready:    true,
			wantFlag: 1,
		},
		{
			// Step: Calling SetReady(false) should store 0 in the atomic flag.
			name:     "SetReady false sets flag to 0",
			ready:    false,
			wantFlag: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(metrics.New())

			// Step: Apply the ready state.
			h.SetReady(tt.ready)

			// Step: Inspect the internal flag via the atomic load.
			if got := h.readyFlag.Load(); got != tt.wantFlag {
				t.Errorf("readyFlag = %d, want %d", got, tt.wantFlag)
			}
		})
	}
}

func TestHandler_RegisterRoutes(t *testing.T) {
	// Step: Register routes on a fresh engine.
	r := New(1024)
	h := NewHandler(metrics.New())
	h.RegisterRoutes(r)

	// Step: Verify each expected route responds (not 404).
	routes := []string{"/health", "/ready", "/metrics"}
	for _, path := range routes {
		t.Run("route "+path+" is registered", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			// Step: A registered route should never return 404.
			if w.Code == http.StatusNotFound {
				t.Errorf("GET %s returned 404; route not registered", path)
			}
		})
	}
}

func TestHandler_HandleHealth(t *testing.T) {
	// Step: Set up the server and register routes.
	r := New(1024)
	h := NewHandler(metrics.New())
	h.RegisterRoutes(r)

	// Step: Send a GET /health request.
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Step: Health probe must always return 200 OK.
	if w.Code != http.StatusOK {
		t.Errorf("GET /health status = %d, want %d", w.Code, http.StatusOK)
	}

	// Step: Verify the response body contains the expected status field.
	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decoding /health body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("body[status] = %q, want %q", body["status"], "ok")
	}
}

func TestHandler_HandleReady(t *testing.T) {
	tests := []struct {
		name       string
		ready      bool
		wantStatus int
		wantBody   string
	}{
		{
			// Step: Readiness probe returns 200 when the loop is running.
			name:       "returns 200 when ready",
			ready:      true,
			wantStatus: http.StatusOK,
			wantBody:   "ready",
		},
		{
			// Step: Readiness probe returns 503 when the loop has not started or is shutting down.
			name:       "returns 503 when not ready",
			ready:      false,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "not ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step: Build engine and handler, then configure readiness state.
			r := New(1024)
			h := NewHandler(metrics.New())
			h.SetReady(tt.ready)
			h.RegisterRoutes(r)

			// Step: Send the request.
			req := httptest.NewRequest(http.MethodGet, "/ready", nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			// Step: Verify status code.
			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}

			// Step: Verify response body contains the expected status string.
			var body map[string]string
			if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
				t.Fatalf("decoding /ready body: %v", err)
			}
			// When not ready, we return { "error": "not ready" }
			got := body["status"]
			if tt.wantStatus != http.StatusOK {
				got = body["error"]
			}
			if got != tt.wantBody {
				t.Errorf("body[status/error] = %q, want %q", got, tt.wantBody)
			}
		})
	}
}

func TestHandler_HandleMetrics(t *testing.T) {
	// Step: Populate the metrics instance with known values.
	m := metrics.New()
	m.IncRowsSent()
	m.IncRowsSent()
	m.IncErrors()

	// Step: Build the engine and register routes.
	r := New(1024)
	h := NewHandler(m)
	h.RegisterRoutes(r)

	// Step: Send GET /metrics and record the response.
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Step: Verify status 200.
	if w.Code != http.StatusOK {
		t.Errorf("GET /metrics status = %d, want %d", w.Code, http.StatusOK)
	}

	// Step: Decode and verify the snapshot fields.
	var snap metrics.Snapshot
	if err := json.NewDecoder(w.Body).Decode(&snap); err != nil {
		t.Fatalf("decoding /metrics body: %v", err)
	}
	if snap.RowsSentTotal != 2 {
		t.Errorf("rows_sent_total = %d, want 2", snap.RowsSentTotal)
	}
	if snap.ErrorsTotal != 1 {
		t.Errorf("errors_total = %d, want 1", snap.ErrorsTotal)
	}
}
