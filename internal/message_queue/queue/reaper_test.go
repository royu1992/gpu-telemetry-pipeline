package queue

import (
	"context"
	"log/slog"
	"testing"
	"time"

	message_queue "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/model"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/model"
)

// TestReapExpiredLeases_AckBetweenPhases covers the path where a delivery ID
// is collected as expired in phase 1 but then acked before phase 2 re-checks
// the inFlight map. The reaper must take the `continue` branch gracefully.
func TestReapExpiredLeases_AckBetweenPhases(t *testing.T) {
	metrics := &message_queue.Metrics{}
	b := NewBuffer(10, 1*time.Millisecond, 3, metrics)
	ctx := context.Background()
	logger := slog.Default()

	b.Publish(ctx, model.TelemetryMessage{UUID: "1"})
	res, _ := b.Consume(ctx, "c1", 1)

	// Wait for the lease to expire so it appears in the phase-1 snapshot.
	time.Sleep(10 * time.Millisecond)

	// Simulate the race: ack the message before the reaper processes it.
	b.Acknowledge("c1", []string{res[0].DeliveryID})

	// reapExpiredLeases should find the ID in the phase-1 snapshot but not in
	// inFlight during phase 2 and skip it without panicking.
	b.reapExpiredLeases(logger)
}

// TestRunReaper_StopsOnContextCancel verifies RunReaper exits when the context
// is cancelled.
func TestRunReaper_StopsOnContextCancel(t *testing.T) {
	metrics := &message_queue.Metrics{}
	buf := NewBuffer(10, 50*time.Millisecond, 3, metrics)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		RunReaper(ctx, buf, 10*time.Millisecond, slog.Default())
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// RunReaper returned cleanly
	case <-time.After(500 * time.Millisecond):
		t.Fatal("RunReaper did not stop after context cancellation")
	}
}

// TestRunReaper_ReapsExpiredLeases verifies that RunReaper detects expired
// leases on its tick interval and requeues messages.
func TestRunReaper_ReapsExpiredLeases(t *testing.T) {
	metrics := &message_queue.Metrics{}
	buf := NewBuffer(10, 25*time.Millisecond, 3, metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunReaper(ctx, buf, 10*time.Millisecond, slog.Default())

	bCtx := context.Background()
	buf.Publish(bCtx, model.TelemetryMessage{UUID: "r1"})
	res, _ := buf.Consume(bCtx, "c1", 1)
	if len(res) != 1 {
		t.Fatal("expected 1 message")
	}

	// Wait for lease to expire and reaper to requeue.
	time.Sleep(100 * time.Millisecond)

	ctxShort, shortCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer shortCancel()
	res2, _ := buf.Consume(ctxShort, "c2", 1)
	if len(res2) != 1 || res2[0].Message.MessageID != res[0].Message.MessageID {
		t.Errorf("expected message to be requeued by reaper")
	}
}
