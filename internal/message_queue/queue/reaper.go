package queue

import (
	"context"
	"log/slog"
	"time"
)

// RunReaper starts a blocking loop that periodically scans for in-flight
// messages whose lease has expired and requeues or drops them accordingly.
// It runs until ctx is cancelled, at which point it returns cleanly.
func RunReaper(ctx context.Context, buf *Buffer, interval time.Duration, logger *slog.Logger) {
	// Create a ticker that fires at the configured reaper interval.
	ticker := time.NewTicker(interval)
	// Ensure the ticker's internal goroutine is cleaned up when RunReaper returns.
	defer ticker.Stop()

	logger.Info("lease reaper started", "interval", interval)

	for {
		select {
		case <-ctx.Done():
			// The context was cancelled (shutdown in progress) — exit cleanly.
			logger.Info("lease reaper stopped")
			return
		case <-ticker.C:
			// The interval has elapsed — scan for expired leases and handle them.
			buf.reapExpiredLeases(logger)
		}
	}
}
