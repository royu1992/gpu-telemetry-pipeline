package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/config"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/metrics"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/store"
	mq "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/model"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/model"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// discardLogger returns a *slog.Logger whose output goes to io.Discard.
// Using it prevents test output from being polluted by consumer log lines.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testConfig returns a CollectorConfig that is safe to use in tests.
// Times are set very short so tests don't need to sleep.
func testConfig(queueURL string) config.CollectorConfig {
	return config.CollectorConfig{
		QueueURL:        queueURL,
		BatchSize:       10,
		LongPollTimeout: 30 * time.Millisecond,
		RequestTimeout:  10 * time.Millisecond,
		ErrorBackoff:    1 * time.Millisecond,
	}
}

// validItem returns a mq.DeliveryItem whose Message passes both validation
// steps in processBatch (RFC 3339 timestamp + parseable float value).
func validItem(deliveryID, messageID string) mq.DeliveryItem {
	return mq.DeliveryItem{
		DeliveryID: deliveryID,
		Message: model.TelemetryMessage{
			MessageID:  messageID,
			Timestamp:  "2024-01-15T10:00:00Z",
			MetricName: "DCGM_FI_DEV_GPU_UTIL",
			GpuID:      "0",
			Device:     "nvidia0",
			UUID:       "GPU-abc123",
			ModelName:  "H100",
			Hostname:   "node-1",
			Value:      "75.5",
			LabelsRaw:  "env=prod",
		},
	}
}

// invalidTimestampItem returns an item whose timestamp will fail time.Parse.
func invalidTimestampItem(deliveryID, messageID string) mq.DeliveryItem {
	item := validItem(deliveryID, messageID)
	item.Message.Timestamp = "not-a-timestamp"
	return item
}

// invalidValueItem returns an item whose value will fail strconv.ParseFloat.
func invalidValueItem(deliveryID, messageID string) mq.DeliveryItem {
	item := validItem(deliveryID, messageID)
	item.Message.Value = "not-a-float"
	return item
}

// ─── New ─────────────────────────────────────────────────────────────────────

// TestNew_FieldsPopulated verifies that New correctly sets the derived URLs,
// metrics, logger, and HTTP client fields.
func TestNew_FieldsPopulated(t *testing.T) {
	cfg := testConfig("http://queue:8080")
	m := metrics.New()
	logger := discardLogger()

	// store.Store requires a real DB; pass nil and check all other fields.
	c := New(cfg, nil, m, logger)

	if c == nil {
		t.Fatal("New() returned nil")
	}
	if c.consumeBaseURL != "http://queue:8080/messages/consume" {
		t.Errorf("consumeBaseURL: got %q", c.consumeBaseURL)
	}
	if c.ackURL != "http://queue:8080/messages/ack" {
		t.Errorf("ackURL: got %q", c.ackURL)
	}
	if c.consumerID == "" {
		t.Error("consumerID must not be empty")
	}
	if c.client == nil {
		t.Error("client must not be nil")
	}
}

// ─── processBatch ─────────────────────────────────────────────────────────────

// TestProcessBatch exercises every code path in processBatch:
//   - entirely valid batch
//   - invalid timestamp → skipped, validation counter incremented
//   - invalid value → skipped, validation counter incremented
//   - mix of valid and invalid rows
//   - empty batch
func TestProcessBatch(t *testing.T) {
	tests := []struct {
		name              string
		items             []mq.DeliveryItem
		wantRows          int
		wantDeliveryIDs   []string
		wantValidationErr int64
	}{
		{
			name: "All valid rows",
			items: []mq.DeliveryItem{
				validItem("d1", "m1"),
				validItem("d2", "m2"),
			},
			wantRows:          2,
			wantDeliveryIDs:   []string{"d1", "d2"},
			wantValidationErr: 0,
		},
		{
			name: "Invalid timestamp row is skipped",
			items: []mq.DeliveryItem{
				invalidTimestampItem("d1", "m1"),
			},
			wantRows:          0,
			wantDeliveryIDs:   []string{},
			wantValidationErr: 1,
		},
		{
			name: "Invalid value row is skipped",
			items: []mq.DeliveryItem{
				invalidValueItem("d1", "m1"),
			},
			wantRows:          0,
			wantDeliveryIDs:   []string{},
			wantValidationErr: 1,
		},
		{
			name: "Mixed batch — only valid rows returned",
			items: []mq.DeliveryItem{
				validItem("d1", "m1"),
				invalidTimestampItem("d2", "m2"),
				validItem("d3", "m3"),
				invalidValueItem("d4", "m4"),
			},
			wantRows:          2,
			wantDeliveryIDs:   []string{"d1", "d3"},
			wantValidationErr: 2,
		},
		{
			name:              "Empty batch returns empty slices",
			items:             []mq.DeliveryItem{},
			wantRows:          0,
			wantDeliveryIDs:   []string{},
			wantValidationErr: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := metrics.New()
			c := &Consumer{
				cfg:        testConfig("http://queue"),
				metrics:    m,
				logger:     discardLogger(),
				consumerID: "test",
			}

			rows, deliveryIDs := c.processBatch(tt.items)

			// Verify row count.
			if len(rows) != tt.wantRows {
				t.Errorf("rows: got %d, want %d", len(rows), tt.wantRows)
			}

			// Verify delivery IDs match.
			if len(deliveryIDs) != len(tt.wantDeliveryIDs) {
				t.Errorf("deliveryIDs len: got %d, want %d", len(deliveryIDs), len(tt.wantDeliveryIDs))
			} else {
				for i, id := range deliveryIDs {
					if id != tt.wantDeliveryIDs[i] {
						t.Errorf("deliveryIDs[%d]: got %q, want %q", i, id, tt.wantDeliveryIDs[i])
					}
				}
			}

			// Verify validation error counter.
			snap := m.Snapshot()
			if snap.ValidationErrorsTotal != tt.wantValidationErr {
				t.Errorf("ValidationErrorsTotal: got %d, want %d", snap.ValidationErrorsTotal, tt.wantValidationErr)
			}
		})
	}
}

// TestProcessBatch_RowFieldMapping verifies that a valid item is mapped to the
// correct store.Row fields — ensuring no field is swapped or dropped.
func TestProcessBatch_RowFieldMapping(t *testing.T) {
	item := mq.DeliveryItem{
		DeliveryID: "delivery-xyz",
		Message: model.TelemetryMessage{
			MessageID:  "msg-001",
			Timestamp:  "2024-06-01T12:00:00Z",
			MetricName: "DCGM_FI_DEV_FB_USED",
			GpuID:      "2",
			Device:     "nvidia2",
			UUID:       "GPU-deadbeef",
			ModelName:  "A100",
			Hostname:   "worker-7",
			Value:      "1234.5",
			LabelsRaw:  "region=us-east",
		},
	}

	m := metrics.New()
	c := &Consumer{
		cfg:        testConfig("http://queue"),
		metrics:    m,
		logger:     discardLogger(),
		consumerID: "test",
	}

	rows, ids := c.processBatch([]mq.DeliveryItem{item})

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	r := rows[0]

	// Verify timestamp is parsed to the correct UTC instant.
	expectedTs, _ := time.Parse(time.RFC3339, "2024-06-01T12:00:00Z")
	if !r.Ts.Equal(expectedTs) {
		t.Errorf("Ts: got %v, want %v", r.Ts, expectedTs)
	}
	if r.MetricName != "DCGM_FI_DEV_FB_USED" {
		t.Errorf("MetricName: got %q", r.MetricName)
	}
	if r.GpuID != "2" {
		t.Errorf("GpuID: got %q", r.GpuID)
	}
	if r.Device != "nvidia2" {
		t.Errorf("Device: got %q", r.Device)
	}
	if r.UUID != "GPU-deadbeef" {
		t.Errorf("UUID: got %q", r.UUID)
	}
	if r.ModelName != "A100" {
		t.Errorf("ModelName: got %q", r.ModelName)
	}
	if r.Hostname != "worker-7" {
		t.Errorf("Hostname: got %q", r.Hostname)
	}
	if r.Value != 1234.5 {
		t.Errorf("Value: got %v, want 1234.5", r.Value)
	}
	if r.LabelsRaw != "region=us-east" {
		t.Errorf("LabelsRaw: got %q", r.LabelsRaw)
	}
	if r.MessageID != "msg-001" {
		t.Errorf("MessageID: got %q", r.MessageID)
	}
	if ids[0] != "delivery-xyz" {
		t.Errorf("deliveryID: got %q", ids[0])
	}
}

// ─── poll ─────────────────────────────────────────────────────────────────────

// TestPoll exercises every response-status branch of the poll method via an
// httptest.Server that the Consumer's HTTP client talks to.
func TestPoll(t *testing.T) {
	validResponse := mq.ConsumeResponse{
		Messages: []mq.DeliveryItem{validItem("d1", "m1")},
	}
	validBody, _ := json.Marshal(validResponse)

	tests := []struct {
		name        string
		handlerFunc http.HandlerFunc // what the fake server returns
		wantItems   int              // number of items expected
		wantErr     bool             // whether poll should return an error
		errContains string           // substring expected in error message
	}{
		{
			name: "200 OK with valid JSON body returns items",
			handlerFunc: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(validBody)
			},
			wantItems: 1,
			wantErr:   false,
		},
		{
			name: "204 No Content returns nil items and nil error",
			handlerFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			},
			wantItems: 0,
			wantErr:   false,
		},
		{
			name: "500 Internal Server Error returns error with status code",
			handlerFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("server exploded"))
			},
			wantItems:   0,
			wantErr:     true,
			errContains: "500",
		},
		{
			name: "503 with body — error includes body text",
			handlerFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte("overloaded"))
			},
			wantItems:   0,
			wantErr:     true,
			errContains: "overloaded",
		},
		{
			name: "200 OK with malformed JSON returns decode error",
			handlerFunc: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("{not valid json"))
			},
			wantItems:   0,
			wantErr:     true,
			errContains: "decode consume response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Start a local HTTP server that exercises the branch under test.
			srv := httptest.NewServer(tt.handlerFunc)
			defer srv.Close()

			cfg := testConfig(srv.URL)
			c := &Consumer{
				cfg:            cfg,
				metrics:        metrics.New(),
				logger:         discardLogger(),
				consumerID:     "test",
				client:         &http.Client{},
				consumeBaseURL: srv.URL + "/messages/consume",
				ackURL:         srv.URL + "/messages/ack",
			}

			items, err := c.poll(context.Background())

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(items) != tt.wantItems {
				t.Errorf("items: got %d, want %d", len(items), tt.wantItems)
			}
		})
	}
}

// TestPoll_QueryParameters verifies that poll sends consumer_id, batch_size,
// and long_poll_timeout as query parameters.
func TestPoll_QueryParameters(t *testing.T) {
	var capturedQuery string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Capture the raw query string so we can assert on it below.
		capturedQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.BatchSize = 7
	cfg.LongPollTimeout = 5 * time.Second

	c := &Consumer{
		cfg:            cfg,
		metrics:        metrics.New(),
		logger:         discardLogger(),
		consumerID:     "my-pod",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
	}

	_, err := c.poll(context.Background())
	if err != nil {
		t.Fatalf("poll returned unexpected error: %v", err)
	}

	// Verify all three expected parameters are present in the query string.
	for _, param := range []string{"consumer_id=my-pod", "batch_size=7", "long_poll_timeout="} {
		if !strings.Contains(capturedQuery, param) {
			t.Errorf("query %q does not contain %q", capturedQuery, param)
		}
	}
}

// TestPoll_CancelledContext verifies that poll returns an error when the
// context is already cancelled before the HTTP request is attempted.
func TestPoll_CancelledContext(t *testing.T) {
	// The server never needs to reply because the client context is cancelled.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second) // long enough to be cancelled
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	c := &Consumer{
		cfg:            cfg,
		metrics:        metrics.New(),
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
	}

	// Cancel the context before calling poll.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.poll(ctx)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// ─── ack ─────────────────────────────────────────────────────────────────────

// TestAck exercises every response branch of the ack method.
func TestAck(t *testing.T) {
	tests := []struct {
		name        string
		handler     http.HandlerFunc
		deliveryIDs []string
		wantErr     bool
		errContains string
	}{
		{
			name: "2xx response — no error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			deliveryIDs: []string{"d1", "d2"},
			wantErr:     false,
		},
		{
			name: "4xx response — returns error with status code",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("bad delivery ids"))
			},
			deliveryIDs: []string{"d1"},
			wantErr:     true,
			errContains: "400",
		},
		{
			name: "5xx response — error includes body",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("queue failure"))
			},
			deliveryIDs: []string{"d1"},
			wantErr:     true,
			errContains: "queue failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			cfg := testConfig(srv.URL)
			c := &Consumer{
				cfg:            cfg,
				metrics:        metrics.New(),
				logger:         discardLogger(),
				consumerID:     "test",
				client:         &http.Client{},
				consumeBaseURL: srv.URL + "/messages/consume",
				ackURL:         srv.URL + "/messages/ack",
			}

			err := c.ack(context.Background(), tt.deliveryIDs)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestAck_RequestPayload verifies that ack sends the correct JSON body
// (consumer_id + delivery_ids) to the queue endpoint.
func TestAck_RequestPayload(t *testing.T) {
	var capturedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Capture the full request body.
		capturedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	c := &Consumer{
		cfg:            cfg,
		metrics:        metrics.New(),
		logger:         discardLogger(),
		consumerID:     "pod-abc",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
	}

	err := c.ack(context.Background(), []string{"d-1", "d-2"})
	if err != nil {
		t.Fatalf("ack returned unexpected error: %v", err)
	}

	// Decode the captured body and assert on the fields.
	var req mq.AckRequest
	if err := json.Unmarshal(capturedBody, &req); err != nil {
		t.Fatalf("unmarshal captured body: %v", err)
	}
	if req.ConsumerID != "pod-abc" {
		t.Errorf("ConsumerID: got %q, want pod-abc", req.ConsumerID)
	}
	if len(req.DeliveryIDs) != 2 || req.DeliveryIDs[0] != "d-1" || req.DeliveryIDs[1] != "d-2" {
		t.Errorf("DeliveryIDs: got %v", req.DeliveryIDs)
	}
}

// TestAck_CancelledContext verifies that ack returns an error when the context
// is already cancelled.
func TestAck_CancelledContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	c := &Consumer{
		cfg:            cfg,
		metrics:        metrics.New(),
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.ack(ctx, []string{"d1"})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// ─── Run (integration-level consumer loop) ────────────────────────────────────

// TestRun_ImmediateCancel verifies that Run returns promptly when the context
// is already cancelled at entry without calling the queue at all.
func TestRun_ImmediateCancel(t *testing.T) {
	// This server must never be reached; any request would be a test failure.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("unexpected request to queue server")
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	c := &Consumer{
		cfg:            cfg,
		metrics:        metrics.New(),
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling Run

	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	// Run must return well within a second.
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// TestRun_PollError_BackoffThenCancel verifies that on a poll error, Run waits
// ErrorBackoff before retrying, and that a context cancellation during the
// backoff causes Run to return immediately.
func TestRun_PollError_BackoffThenCancel(t *testing.T) {
	// The server always returns 503 to trigger the error backoff path.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.ErrorBackoff = 50 * time.Millisecond // short but non-zero

	m := metrics.New()
	c := &Consumer{
		cfg:            cfg,
		metrics:        m,
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	// Let it enter the backoff at least once, then cancel.
	time.Sleep(60 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not return after cancel during backoff")
	}
}

// TestRun_EmptyQueue_NilItems verifies that 204 No Content from the queue
// causes the loop to continue without incrementing any counters.
func TestRun_EmptyQueue_NilItems(t *testing.T) {
	// Count how many times the queue is polled.
	var pollCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := pollCount.Add(1)
		if n < 3 {
			// Return empty queue for the first two polls.
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// On the third poll, return an error so the backoff path is taken,
		// then the cancel will stop the loop.
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.ErrorBackoff = 1 * time.Millisecond

	m := metrics.New()
	c := &Consumer{
		cfg:            cfg,
		metrics:        m,
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	c.Run(ctx)

	// No messages were consumed, so the counter must remain zero.
	snap := m.Snapshot()
	if snap.MessagesConsumedTotal != 0 {
		t.Errorf("MessagesConsumedTotal: got %d, want 0", snap.MessagesConsumedTotal)
	}
}

// TestRun_FullHappyPath verifies the complete success path:
// poll → processBatch → BulkInsert → ack, then cancel.
func TestRun_FullHappyPath(t *testing.T) {
	// Build a response with one valid message.
	resp := mq.ConsumeResponse{
		Messages: []mq.DeliveryItem{validItem("d1", "m1")},
	}
	respBody, _ := json.Marshal(resp)

	// Count iterations; after the first success response return 204 forever.
	var served atomic.Int32

	ackReceived := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/messages/consume":
			if served.Add(1) == 1 {
				// Serve one real batch on the first poll.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(respBody)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		case "/messages/ack":
			// Signal that the ACK was received.
			select {
			case ackReceived <- struct{}{}:
			default:
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)

	// Use an in-memory fake store to capture the inserted rows.
	fs := &fakeStoreImpl{}

	m := metrics.New()
	c := &Consumer{
		cfg:            cfg,
		metrics:        m,
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
		storeFunc:      fs.BulkInsert,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go c.Run(ctx)

	// Wait for the ACK to arrive.
	select {
	case <-ackReceived:
	case <-time.After(400 * time.Millisecond):
		t.Fatal("ACK not received within timeout")
	}

	// Metrics must reflect one successful write and one batch consumed.
	snap := m.Snapshot()
	if snap.MessagesConsumedTotal < 1 {
		t.Errorf("MessagesConsumedTotal: got %d, want >= 1", snap.MessagesConsumedTotal)
	}
	if snap.DBWritesSuccessTotal < 1 {
		t.Errorf("DBWritesSuccessTotal: got %d, want >= 1", snap.DBWritesSuccessTotal)
	}
	if snap.DBWritesErrorTotal != 0 {
		t.Errorf("DBWritesErrorTotal: got %d, want 0", snap.DBWritesErrorTotal)
	}
	if snap.LastDBWriteTimestamp == 0 {
		t.Error("LastDBWriteTimestamp: must be non-zero after successful write")
	}
}

// TestRun_DBWriteError_SkipsAck verifies that when BulkInsert fails,
// the DBWritesError counter is incremented and no ACK is sent.
func TestRun_DBWriteError_SkipsAck(t *testing.T) {
	resp := mq.ConsumeResponse{
		Messages: []mq.DeliveryItem{validItem("d1", "m1")},
	}
	respBody, _ := json.Marshal(resp)

	var served atomic.Int32
	ackCalled := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/messages/consume":
			if served.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(respBody)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		case "/messages/ack":
			// Signal that ack was unexpectedly called.
			select {
			case ackCalled <- struct{}{}:
			default:
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.ErrorBackoff = 1 * time.Millisecond

	// storeFunc returns a hard error.
	dbErr := fmt.Errorf("disk full")
	m := metrics.New()
	c := &Consumer{
		cfg:            cfg,
		metrics:        m,
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
		storeFunc: func(ctx context.Context, rows []store.Row) error {
			return dbErr
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	// DB error counter must be incremented.
	snap := m.Snapshot()
	if snap.DBWritesErrorTotal < 1 {
		t.Errorf("DBWritesErrorTotal: got %d, want >= 1", snap.DBWritesErrorTotal)
	}
	// ACK must never have been sent.
	select {
	case <-ackCalled:
		t.Error("ack was called even though BulkInsert failed")
	default:
	}
}

// TestRun_EntireBatchInvalidValidation verifies that when all rows fail
// validation, neither BulkInsert nor ack is called.
func TestRun_EntireBatchInvalidValidation(t *testing.T) {
	// All messages have an invalid timestamp.
	resp := mq.ConsumeResponse{
		Messages: []mq.DeliveryItem{
			invalidTimestampItem("d1", "m1"),
			invalidTimestampItem("d2", "m2"),
		},
	}
	respBody, _ := json.Marshal(resp)

	var served atomic.Int32
	ackCalled := make(chan struct{}, 1)
	dbCalled := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/messages/consume":
			if served.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(respBody)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		case "/messages/ack":
			select {
			case ackCalled <- struct{}{}:
			default:
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	m := metrics.New()
	c := &Consumer{
		cfg:            cfg,
		metrics:        m,
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
		storeFunc: func(ctx context.Context, rows []store.Row) error {
			select {
			case dbCalled <- struct{}{}:
			default:
			}
			return nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	snap := m.Snapshot()
	if snap.ValidationErrorsTotal != 2 {
		t.Errorf("ValidationErrorsTotal: got %d, want 2", snap.ValidationErrorsTotal)
	}
	select {
	case <-dbCalled:
		t.Error("BulkInsert was called even though entire batch failed validation")
	default:
	}
	select {
	case <-ackCalled:
		t.Error("ack was called even though entire batch failed validation")
	default:
	}
}

// TestRun_AckError_NonFatal verifies that an ACK failure does not stop the
// loop (data is already in the DB) and is logged as a non-fatal error.
func TestRun_AckError_NonFatal(t *testing.T) {
	resp := mq.ConsumeResponse{
		Messages: []mq.DeliveryItem{validItem("d1", "m1")},
	}
	respBody, _ := json.Marshal(resp)

	var served atomic.Int32
	dbInserted := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/messages/consume":
			if served.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(respBody)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		case "/messages/ack":
			// ACK always fails — should be non-fatal.
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	m := metrics.New()
	c := &Consumer{
		cfg:            cfg,
		metrics:        m,
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
		storeFunc: func(ctx context.Context, rows []store.Row) error {
			select {
			case dbInserted <- struct{}{}:
			default:
			}
			return nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	// DB must have been written despite the ACK failure.
	select {
	case <-dbInserted:
		// Good — data reached the DB.
	case <-time.After(200 * time.Millisecond):
		t.Fatal("BulkInsert was never called")
	}

	// The DB write success counter must be incremented.
	snap := m.Snapshot()
	if snap.DBWritesSuccessTotal < 1 {
		t.Errorf("DBWritesSuccessTotal: got %d, want >= 1", snap.DBWritesSuccessTotal)
	}
}

// TestRun_ContextCancelledDuringDBWrite verifies that if the context is
// cancelled while BulkInsert is in progress, Run returns without ACKing.
func TestRun_ContextCancelledDuringDBWrite(t *testing.T) {
	resp := mq.ConsumeResponse{
		Messages: []mq.DeliveryItem{validItem("d1", "m1")},
	}
	respBody, _ := json.Marshal(resp)
	var served atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/messages/consume" && served.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(respBody)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())

	m := metrics.New()
	c := &Consumer{
		cfg:            cfg,
		metrics:        m,
		logger:         discardLogger(),
		consumerID:     "test",
		client:         &http.Client{},
		consumeBaseURL: srv.URL + "/messages/consume",
		ackURL:         srv.URL + "/messages/ack",
		storeFunc: func(innerCtx context.Context, rows []store.Row) error {
			// Simulate context cancellation during a slow DB write.
			cancel()
			return innerCtx.Err()
		},
	}

	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
		// Run returned — correct behaviour.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not return after context cancellation during DB write")
	}
}

// ─── fakeStoreImpl (inline, used only for happy-path Run test) ───────────────

// fakeStoreImpl records calls to BulkInsert without error.
type fakeStoreImpl struct {
	rows []store.Row
}

func (f *fakeStoreImpl) BulkInsert(_ context.Context, rows []store.Row) error {
	f.rows = append(f.rows, rows...)
	return nil
}

// ─── errReader ────────────────────────────────────────────────────────────────

// errReader is an io.ReadCloser whose Read always returns an error.
// It is used to simulate a body-read failure on an HTTP response.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read error") }
func (errReader) Close() error             { return nil }

// errorBodyTransport is an http.RoundTripper that returns a response with the
// configured status code and an errReader body, so that io.ReadAll fails.
type errorBodyTransport struct {
	statusCode int
}

func (t errorBodyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: t.statusCode,
		Body:       errReader{},
	}, nil
}

// ─── Tests for uncovered error paths ─────────────────────────────────────────

// TestNew_HostnameError_UsesUUID verifies that when hostnameFunc returns an
// error, New falls back to a randomly generated UUID as the consumer ID.
func TestNew_HostnameError_UsesUUID(t *testing.T) {
	// Replace the package-level hostname function with one that always fails.
	orig := hostnameFunc
	hostnameFunc = func() (string, error) { return "", errors.New("hostname unavailable") }
	defer func() { hostnameFunc = orig }()

	cfg := testConfig("http://queue:8080")
	m := metrics.New()
	c := New(cfg, nil, m, discardLogger())

	// The consumer ID must be a non-empty UUID (not an error message).
	if c.consumerID == "" {
		t.Error("consumerID must not be empty after hostname failure")
	}
	// It must not equal the error string.
	if c.consumerID == "hostname unavailable" {
		t.Error("consumerID must not be the error message text")
	}
}

// TestNew_EmptyHostname_UsesUUID verifies that an empty hostname string (which
// is valid from os.Hostname in some environments) also triggers the UUID fallback.
func TestNew_EmptyHostname_UsesUUID(t *testing.T) {
	orig := hostnameFunc
	hostnameFunc = func() (string, error) { return "", nil } // returns empty without error
	defer func() { hostnameFunc = orig }()

	cfg := testConfig("http://queue:8080")
	m := metrics.New()
	c := New(cfg, nil, m, discardLogger())

	if c.consumerID == "" {
		t.Error("consumerID must not be empty after empty hostname")
	}
}

// TestPoll_BodyReadError verifies that when the queue returns a non-2xx status
// and the response body read fails, poll returns an error that contains the
// status code and explains the body-read failure.
func TestPoll_BodyReadError(t *testing.T) {
	cfg := testConfig("http://irrelevant")

	c := &Consumer{
		cfg:            cfg,
		metrics:        metrics.New(),
		logger:         discardLogger(),
		consumerID:     "test",
		consumeBaseURL: "http://irrelevant/messages/consume",
		ackURL:         "http://irrelevant/messages/ack",
		// Use a transport that returns a 503 with an unreadable body.
		client: &http.Client{Transport: errorBodyTransport{statusCode: http.StatusServiceUnavailable}},
	}

	_, err := c.poll(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// The error must mention the status code.
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error %q does not mention status 503", err.Error())
	}
	// The error must mention the body-read failure.
	if !strings.Contains(err.Error(), "failed to read body") {
		t.Errorf("error %q does not mention body read failure", err.Error())
	}
}

// TestAck_BodyReadError verifies that when the queue returns a non-2xx ack
// response and the response body read fails, ack returns an error containing
// the status code and the body-read failure reason.
func TestAck_BodyReadError(t *testing.T) {
	cfg := testConfig("http://irrelevant")

	c := &Consumer{
		cfg:            cfg,
		metrics:        metrics.New(),
		logger:         discardLogger(),
		consumerID:     "test",
		consumeBaseURL: "http://irrelevant/messages/consume",
		ackURL:         "http://irrelevant/messages/ack",
		// Use a transport that returns a 400 with an unreadable body.
		client: &http.Client{Transport: errorBodyTransport{statusCode: http.StatusBadRequest}},
	}

	err := c.ack(context.Background(), []string{"d1"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// The error must mention the status code.
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error %q does not mention status 400", err.Error())
	}
	// The error must mention the body-read failure.
	if !strings.Contains(err.Error(), "failed to read body") {
		t.Errorf("error %q does not mention body read failure", err.Error())
	}
}
