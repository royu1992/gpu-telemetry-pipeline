package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	queuecfg "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/config"
	message_queue "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/model"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/model"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func newTestHandler(capacity int) (*MessageQueueHandler, *Buffer, *message_queue.Metrics) {
	cfg := queuecfg.QueueConfig{
		BatchSize:       10,
		LongPollTimeout: 200 * time.Millisecond,
		PublishTimeout:  500 * time.Millisecond,
	}
	metrics := message_queue.NewMetrics()
	buf := NewBuffer(capacity, 30*time.Second, 3, metrics)
	h := NewMessageQueueHandler(buf, metrics, cfg)
	return h, buf, metrics
}

func testRouter(h *MessageQueueHandler) *gin.Engine {
	r := gin.New()
	h.RegisterRoutes(r)
	return r
}

// ---- handlePublish ----

func TestHandlePublish_Success(t *testing.T) {
	h, _, _ := newTestHandler(10)
	r := testRouter(h)

	body := `{"timestamp":"2026-05-10T12:00:00Z","metric_name":"gpu_util","uuid":"gpu-1","value":"55"}`
	req := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var resp message_queue.PublishResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil || resp.MessageID == "" {
		t.Error("expected message_id in response")
	}
}

func TestHandlePublish_BadRequest(t *testing.T) {
	h, _, _ := newTestHandler(10)
	r := testRouter(h)

	// Missing required fields
	body := `{"timestamp":"2026-05-10T12:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandlePublish_Closing(t *testing.T) {
	h, _, _ := newTestHandler(10)
	h.SetClosing()
	r := testRouter(h)

	body := `{"timestamp":"2026-05-10T12:00:00Z","metric_name":"m","uuid":"1","value":"1"}`
	req := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestHandlePublish_QueueFull(t *testing.T) {
	// Capacity 0 is not allowed, use 1 and fill it
	h, buf, _ := newTestHandler(1)
	r := testRouter(h)

	// Pre-fill the single slot
	ctx := context.Background()
	buf.Publish(ctx, model.TelemetryMessage{UUID: "1"})

	body := `{"timestamp":"2026-05-10T12:00:00Z","metric_name":"m","uuid":"1","value":"v"}`
	req := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// PublishTimeout is 500ms; buffer is full so Publish returns ErrFull -> 429
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d: %s", w.Code, w.Body.String())
	}
}

// ---- handleConsume ----

func TestHandleConsume_Success(t *testing.T) {
	h, buf, _ := newTestHandler(10)
	r := testRouter(h)

	ctx := context.Background()
	buf.Publish(ctx, model.TelemetryMessage{UUID: "1", MetricName: "m", Value: "v"})

	req := httptest.NewRequest(http.MethodGet, "/messages/consume?consumer_id=c1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp message_queue.ConsumeResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(resp.Messages))
	}
}

func TestHandleConsume_NoConsumerID(t *testing.T) {
	h, _, _ := newTestHandler(10)
	r := testRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/messages/consume", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleConsume_NoContent(t *testing.T) {
	h, _, _ := newTestHandler(10)
	r := testRouter(h)

	// Queue is empty — long-poll should return 204 after timeout
	req := httptest.NewRequest(http.MethodGet, "/messages/consume?consumer_id=c1&long_poll_timeout=50ms", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", w.Code)
	}
}

func TestHandleConsume_Closing(t *testing.T) {
	h, _, _ := newTestHandler(10)
	h.SetClosing()
	r := testRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/messages/consume?consumer_id=c1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestHandleConsume_CustomBatchSize(t *testing.T) {
	h, buf, _ := newTestHandler(10)
	r := testRouter(h)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		buf.Publish(ctx, model.TelemetryMessage{UUID: "gpu"})
	}

	req := httptest.NewRequest(http.MethodGet, "/messages/consume?consumer_id=c1&batch_size=3", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp message_queue.ConsumeResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Messages) != 3 {
		t.Errorf("expected 3 messages (batch_size=3), got %d", len(resp.Messages))
	}
}

func TestHandleConsume_InvalidBatchSizeFallsToDefault(t *testing.T) {
	h, buf, _ := newTestHandler(10)
	r := testRouter(h)

	ctx := context.Background()
	buf.Publish(ctx, model.TelemetryMessage{UUID: "1"})

	// batch_size=-1 is invalid, should fall back to configured default (10)
	req := httptest.NewRequest(http.MethodGet, "/messages/consume?consumer_id=c1&batch_size=-1&long_poll_timeout=50ms", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleConsume_InvalidLongPollTimeoutFallsToDefault(t *testing.T) {
	h, _, _ := newTestHandler(10)
	r := testRouter(h)

	// long_poll_timeout=not-a-duration is invalid, falls back to configured 200ms
	req := httptest.NewRequest(http.MethodGet, "/messages/consume?consumer_id=c1&long_poll_timeout=not-a-duration", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Queue is empty, default timeout fires, returns 204
	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", w.Code)
	}
}

// ---- handleAck ----

func TestHandleAck_Success(t *testing.T) {
	h, buf, _ := newTestHandler(10)
	r := testRouter(h)

	ctx := context.Background()
	buf.Publish(ctx, model.TelemetryMessage{UUID: "1"})
	items, _ := buf.Consume(ctx, "c1", 1)
	deliveryID := items[0].DeliveryID

	body, _ := json.Marshal(message_queue.AckRequest{
		ConsumerID:  "c1",
		DeliveryIDs: []string{deliveryID},
	})
	req := httptest.NewRequest(http.MethodPost, "/messages/ack", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var result message_queue.AckResult
	json.NewDecoder(w.Body).Decode(&result)
	if result.Acked != 1 || result.Rejected != 0 {
		t.Errorf("expected acked=1 rejected=0, got %+v", result)
	}
}

func TestHandleAck_BadRequest(t *testing.T) {
	h, _, _ := newTestHandler(10)
	r := testRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/messages/ack", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleAck_PartialRejection(t *testing.T) {
	h, buf, _ := newTestHandler(10)
	r := testRouter(h)

	ctx := context.Background()
	buf.Publish(ctx, model.TelemetryMessage{UUID: "1"})
	items, _ := buf.Consume(ctx, "c1", 1)
	deliveryID := items[0].DeliveryID

	body, _ := json.Marshal(message_queue.AckRequest{
		ConsumerID:  "c1",
		DeliveryIDs: []string{deliveryID, "unknown-id"},
	})
	req := httptest.NewRequest(http.MethodPost, "/messages/ack", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusMultiStatus {
		t.Errorf("expected 207, got %d", w.Code)
	}
	var result message_queue.AckResult
	json.NewDecoder(w.Body).Decode(&result)
	if result.Acked != 1 || result.Rejected != 1 {
		t.Errorf("expected acked=1 rejected=1, got %+v", result)
	}
}

// ---- handleHealth ----

func TestHandleHealth(t *testing.T) {
	h, _, _ := newTestHandler(10)
	r := testRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp message_queue.HealthResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Status != "ok" {
		t.Errorf("expected status ok, got %s", resp.Status)
	}
}

// ---- handleReady ----

func TestHandleReady_Ready(t *testing.T) {
	h, _, _ := newTestHandler(10)
	r := testRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp message_queue.ReadyResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Status != "ready" {
		t.Errorf("expected status ready, got %s", resp.Status)
	}
}

func TestHandleReady_NotReady(t *testing.T) {
	h, _, _ := newTestHandler(10)
	h.SetClosing()
	r := testRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
	var resp message_queue.ReadyResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Status != "not ready" {
		t.Errorf("expected 'not ready', got %s", resp.Status)
	}
}

// ---- handleMetrics ----

func TestHandleMetrics(t *testing.T) {
	h, buf, _ := newTestHandler(10)
	r := testRouter(h)

	ctx := context.Background()
	buf.Publish(ctx, model.TelemetryMessage{UUID: "1"})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var snap message_queue.Snapshot
	json.NewDecoder(w.Body).Decode(&snap)
	if snap.TotalPublished != 1 {
		t.Errorf("expected total_published=1, got %d", snap.TotalPublished)
	}
}

// TestHandlePublish_BufferClosedDuringWait covers the default error branch in
// handlePublish — triggered when Publish returns ErrClosing because the buffer
// was closed while the request was blocked waiting for a free slot.
func TestHandlePublish_BufferClosedDuringWait(t *testing.T) {
	h, buf, _ := newTestHandler(1)
	r := testRouter(h)

	// Fill the buffer so the next publish blocks
	ctx := context.Background()
	buf.Publish(ctx, model.TelemetryMessage{UUID: "1"})

	// Publish in a goroutine — it will block because the buffer is full
	done := make(chan int, 1)
	go func() {
		body := `{"timestamp":"2026-05-10T12:00:00Z","metric_name":"m","uuid":"1","value":"v"}`
		req := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		done <- w.Code
	}()

	// Give the goroutine time to block inside Publish
	time.Sleep(100 * time.Millisecond)

	// Close the buffer — triggers ErrClosing inside the blocked Publish,
	// which hits the default branch in handlePublish
	buf.Close()

	select {
	case code := <-done:
		if code != http.StatusServiceUnavailable {
			t.Errorf("expected 503 when buffer closed mid-publish, got %d", code)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("request did not return after buffer close")
	}
}

// ---- handleConsume when buffer closes mid-wait ----

func TestHandleConsume_BufferClosedMidWait(t *testing.T) {
	h, buf, _ := newTestHandler(10)
	r := testRouter(h)

	// In a goroutine, consume with a long timeout — will block
	done := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/messages/consume?consumer_id=c1&long_poll_timeout=10s", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		done <- w.Code
	}()

	time.Sleep(100 * time.Millisecond)
	buf.Close()

	select {
	case code := <-done:
		if code != http.StatusServiceUnavailable {
			t.Errorf("expected 503 after buffer close, got %d", code)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("consume did not unblock after buffer close")
	}
}
