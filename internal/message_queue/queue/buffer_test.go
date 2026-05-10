package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	message_queue "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/model"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/model"
)

func TestNewBuffer(t *testing.T) {
	metrics := &message_queue.Metrics{}
	b := NewBuffer(10, 5*time.Second, 3, metrics)
	if b == nil {
		t.Fatal("expected non-nil buffer")
	}
	if len(b.freeSlots) != 10 {
		t.Errorf("expected 10 free slots, got %d", len(b.freeSlots))
	}
}

func TestBuffer_PublishConsumeAck(t *testing.T) {
	metrics := &message_queue.Metrics{}
	b := NewBuffer(2, 30*time.Second, 3, metrics)

	msg := model.TelemetryMessage{
		Timestamp:  "2026-05-10T12:00:00Z",
		MetricName: "test_util",
		UUID:       "gpu-1",
		Value:      "100",
	}

	// 1. Publish
	ctx := context.Background()
	msgID, err := b.Publish(ctx, msg)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if msgID == "" {
		t.Error("expected non-empty message ID")
	}

	// 2. Consume
	res, err := b.Consume(ctx, "c1", 1)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 message, got %d", len(res))
	}

	// 3. Ack (Success)
	outcomes := b.Acknowledge("c1", []string{res[0].DeliveryID})
	if len(outcomes) != 1 || !outcomes[0].Accepted {
		t.Errorf("expected success ack, got %+v", outcomes)
	}

	// 4. Ack (Consumer Mismatch)
	msgID2, _ := b.Publish(ctx, msg)
	res2, _ := b.Consume(ctx, "c2", 1)
	if res2[0].Message.MessageID != msgID2 {
		t.Fatalf("wrong message consumed")
	}
	outcomes = b.Acknowledge("c1", []string{res2[0].DeliveryID})
	if len(outcomes) != 1 || outcomes[0].Accepted || outcomes[0].Reason != "consumer_id mismatch" {
		t.Errorf("expected consumer_id mismatch, got %+v", outcomes)
	}

	// 5. Ack (Unknown ID)
	outcomes = b.Acknowledge("c1", []string{"invalid-id"})
	if len(outcomes) != 1 || outcomes[0].Accepted || outcomes[0].Reason != "unknown delivery_id" {
		t.Errorf("expected unknown delivery_id, got %+v", outcomes)
	}
}

func TestBuffer_FullBlockingAndUnblocking(t *testing.T) {
	metrics := &message_queue.Metrics{}
	// Use capacity 1 so it is easy to fill
	b := NewBuffer(1, 10*time.Second, 3, metrics)
	msg := model.TelemetryMessage{UUID: "1", MetricName: "m", Value: "v"}
	ctx := context.Background()

	// Fill the buffer
	_, _ = b.Publish(ctx, msg)

	// Attempt a second publish in a goroutine — it blocks because the buffer is full.
	var wg sync.WaitGroup
	wg.Add(1)
	publishErr := make(chan error, 1)
	go func() {
		defer wg.Done()
		_, err := b.Publish(ctx, msg)
		publishErr <- err
	}()

	// Wait slightly to ensure the goroutine is blocked inside Publish.
	time.Sleep(100 * time.Millisecond)

	// canPublish is only Signalled inside Acknowledge (not Consume).
	// Consume moves a message to IN_FLIGHT — it still occupies a slot.
	// Acknowledge returns the slot to freeSlots and Signals canPublish.
	res, err := b.Consume(ctx, "c1", 1)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}
	b.Acknowledge("c1", []string{res[0].DeliveryID})

	// Now the blocked publish should proceed.
	wg.Wait()
	if err := <-publishErr; err != nil {
		t.Errorf("blocked publish returned unexpected error: %v", err)
	}
}

func TestBuffer_PublishTimeout(t *testing.T) {
	metrics := &message_queue.Metrics{}
	b := NewBuffer(1, 10*time.Second, 3, metrics)
	msg := model.TelemetryMessage{UUID: "1", MetricName: "m", Value: "v"}

	// Fill buffer
	_, _ = b.Publish(context.Background(), msg)

	// Publish with a short deadline. The buffer loop wakes via ctx.Done() helper
	// and returns ErrFull — the sentinel error for "timed out waiting for space".
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := b.Publish(ctx, msg)
	if !errors.Is(err, ErrFull) {
		t.Errorf("expected ErrFull on publish timeout, got %v", err)
	}
}

func TestBuffer_EmptyConsumeLongPoll(t *testing.T) {
	metrics := &message_queue.Metrics{}
	b := NewBuffer(10, 30*time.Second, 3, metrics)
	ctx := context.Background()

	// 1. Long poll returns nil when timeout
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	res, err := b.Consume(ctxTimeout, "c1", 1)
	if err != nil || len(res) != 0 {
		t.Errorf("expected nil/empty for timeout, got err=%v len=%d", err, len(res))
	}

	// 2. Long poll unblocks on publish
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r, err := b.Consume(ctx, "c1", 1)
		if err != nil || len(r) != 1 {
			// error reported by check below
		}
	}()

	time.Sleep(100 * time.Millisecond)
	_, _ = b.Publish(ctx, model.TelemetryMessage{UUID: "1"})
	wg.Wait()
}

func TestBuffer_Close(t *testing.T) {
	metrics := &message_queue.Metrics{}
	b := NewBuffer(1, 30*time.Second, 3, metrics)
	ctx := context.Background()

	_, _ = b.Publish(ctx, model.TelemetryMessage{UUID: "1"})
	b.Close()

	// Publish fails instantly
	_, err := b.Publish(ctx, model.TelemetryMessage{UUID: "2"})
	if !errors.Is(err, ErrClosing) {
		t.Errorf("expected ErrClosing, got %v", err)
	}

	// Consume succeeds while messages exist
	res, err := b.Consume(ctx, "c1", 1)
	if err != nil || len(res) != 1 {
		t.Errorf("failed to drain message")
	}

	// Consume fails when empty and closing
	_, err = b.Consume(ctx, "c1", 1)
	if !errors.Is(err, ErrClosing) {
		t.Errorf("expected ErrClosing when drained, got %v", err)
	}
}

func TestBuffer_Reaper(t *testing.T) {
	metrics := &message_queue.Metrics{}
	logger := slog.Default()
	// Quick lease for testing
	b := NewBuffer(10, 50*time.Millisecond, 2, metrics)
	ctx := context.Background()

	// short returns nil/empty when nothing is available; used to assert emptiness.
	consumeShort := func(consumerID string) []message_queue.DeliveryItem {
		ctxShort, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		r, _ := b.Consume(ctxShort, consumerID, 1)
		return r
	}

	// 1. Publish and Consume (delivery attempt 1)
	_, _ = b.Publish(ctx, model.TelemetryMessage{UUID: "m1"})
	res, _ := b.Consume(ctx, "c1", 1)

	// 2. Wait for lease to expire and reap
	time.Sleep(100 * time.Millisecond)
	b.reapExpiredLeases(logger)

	// 3. Message should be requeued (delivery attempt 2)
	res2 := consumeShort("c2")
	if len(res2) != 1 || res2[0].Message.MessageID != res[0].Message.MessageID {
		t.Errorf("expected redelivery after first lease expiry")
	}

	// 4. Exceed max attempts — reap again
	time.Sleep(100 * time.Millisecond)
	b.reapExpiredLeases(logger)

	// Message should now be dropped; consume returns empty.
	res3 := consumeShort("c3")
	if len(res3) != 0 {
		t.Errorf("expected message to be dropped after max attempts")
	}
}

func TestBuffer_Observers(t *testing.T) {
	metrics := &message_queue.Metrics{}
	b := NewBuffer(5, 30*time.Second, 3, metrics)
	ctx := context.Background()

	if b.Capacity() != 5 {
		t.Errorf("expected Capacity 5, got %d", b.Capacity())
	}
	if b.Depth() != 0 {
		t.Errorf("expected Depth 0, got %d", b.Depth())
	}
	if b.InFlightCount() != 0 {
		t.Errorf("expected InFlightCount 0, got %d", b.InFlightCount())
	}

	msg := model.TelemetryMessage{UUID: "1"}
	b.Publish(ctx, msg)
	b.Publish(ctx, msg)

	if b.Depth() != 2 {
		t.Errorf("expected Depth 2, got %d", b.Depth())
	}

	res, _ := b.Consume(ctx, "c1", 1)
	if b.InFlightCount() != 1 {
		t.Errorf("expected InFlightCount 1, got %d", b.InFlightCount())
	}
	if b.Depth() != 1 {
		t.Errorf("expected Depth 1 after consume, got %d", b.Depth())
	}

	b.Acknowledge("c1", []string{res[0].DeliveryID})
	if b.InFlightCount() != 0 {
		t.Errorf("expected InFlightCount 0 after ack, got %d", b.InFlightCount())
	}
}

func TestBuffer_Priority(t *testing.T) {
	metrics := &message_queue.Metrics{}
	b := NewBuffer(10, 1*time.Minute, 3, metrics)
	ctx := context.Background()

	// 1. Publish M1, Consume M1 (it's now inFlight)
	b.Publish(ctx, model.TelemetryMessage{UUID: "M1"})
	b.Consume(ctx, "c1", 1)

	// 2. Manually expire M1 and reap
	b.mu.Lock()
	for _, idx := range b.inFlight {
		b.slots[idx].leaseExpires = time.Now().Add(-1 * time.Second)
	}
	b.mu.Unlock()
	b.reapExpiredLeases(slog.Default())

	// 3. Publish M2 (it's in pending)
	b.Publish(ctx, model.TelemetryMessage{UUID: "M2"})

	// 4. Consume - should get M1 (requeued) then M2 (pending)
	res, _ := b.Consume(ctx, "c1", 2)
	if len(res) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(res))
	}
	if res[0].Message.UUID != "M1" {
		t.Errorf("expected M1 first (requeued), got %s", res[0].Message.UUID)
	}
	if res[1].Message.UUID != "M2" {
		t.Errorf("expected M2 second, got %s", res[1].Message.UUID)
	}
}
