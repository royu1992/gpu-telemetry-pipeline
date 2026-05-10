package telemetry_loop

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/config"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/csv_reader"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/metrics"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/publisher"
)

// TelemetryLoop orchestrates the main streaming work: reading CSV rows sequentially,
// delivering each one to the message-queue, updating metrics, and pacing sends
// with a configurable inter-row delay. It runs as a single goroutine so there
// are no concurrency hazards around the reader, sender, or metrics updates.
type TelemetryLoop struct {
	// reader provides sequential row access from the CSV file, including
	// EOF detection and rewind capability.
	reader *csv_reader.CSVReader

	// publisher delivers a single row to the message-queue with built-in retry.
	publisher *publisher.Publisher

	// metrics is updated on every successful send, every error, and every
	// successful row read. It is also read by the Gin server goroutine via
	// atomic operations, so no locking is needed here.
	metrics *metrics.Metrics

	// cfg holds all timing and policy parameters: interval, retry counts,
	// max consecutive errors, etc.
	cfg config.StreamerConfig

	// logger is used for structured log output. The single-goroutine design
	// means log calls are never concurrent, but slog is safe either way.
	logger *slog.Logger
}

// New creates a TelemetryLoop that is ready to run. The caller must have already
// opened the Reader and constructed the Publisher before calling New.
func New(r *csv_reader.CSVReader, p *publisher.Publisher, m *metrics.Metrics, cfg config.StreamerConfig, logger *slog.Logger) *TelemetryLoop {
	return &TelemetryLoop{
		reader:    r,
		publisher: p,
		metrics:   m,
		cfg:       cfg,
		logger:    logger,
	}
}

// Run is the main streaming loop. It reads rows one at a time, validates them,
// sends them to the queue, sleeps for the configured interval, and repeats
// indefinitely until ctx is cancelled (shutdown signal).
//
// Run also enforces the consecutive-bad-row safety policy: if more than
// cfg.MaxConsecutiveErrors rows in a row fail validation, it calls
// os.Exit(1) to signal that the mounted file is likely corrupt.
func (l *TelemetryLoop) Run(ctx context.Context) {
	// consecutiveBad tracks how many consecutive rows have failed validation.
	// A successful send (even after retries) resets this counter to zero.
	// A send failure (network error) does NOT increment this counter — the row
	// itself was valid; only the delivery failed.
	consecutiveBad := 0

	for {
		// Check for context cancellation before reading the next row. This is
		// the primary shutdown checkpoint and ensures the loop exits promptly
		// after the signal handler calls the cancel function.
		select {
		case <-ctx.Done():
			l.logger.Info("telemetry loop stopping", "reason", ctx.Err())
			return
		default:
			// Context still active; continue to the next row.
		}

		// Attempt to read the next row from the CSV file.
		row, err := l.reader.ReadRow()
		if err != nil {
			if err == io.EOF {
				// EOF is a normal event, not an error. The file has been fully
				// read; sleep for one interval to maintain a consistent pacing
				// cadence at the loop boundary, then rewind to the beginning.
				l.logger.Info("reached end of CSV file, rewinding")
				if !l.sleepOrStop(ctx) {
					return
				}

				// Reset the consecutive bad-row counter on each file rewind.
				// A fresh loop through the file is treated as a clean slate.
				consecutiveBad = 0

				// Seek back to the first data row. A rewind error is fatal
				// because we cannot continue without a valid file position.
				if err := l.reader.Rewind(); err != nil {
					l.logger.Error("failed to rewind CSV file", "err", err)
					return
				}

				// Do not sleep again after the rewind; the pre-rewind sleep
				// already provided the inter-row interval.
				continue
			}

			// A non-EOF error from ReadRow() means the CSV parser encountered
			// a structural problem (e.g. wrong number of fields). Treat this
			// as a bad row: log it, increment both error counters, and continue.
			l.logger.Warn("CSV parse error", "err", err)
			l.metrics.IncErrors()
			consecutiveBad++
			if l.isTooManyConsecutiveErrors(consecutiveBad) {
				return
			}
			continue
		}

		// Record the timestamp of the last successful row read. This gauge
		// lets operators distinguish between "stuck reading" and "stuck sending".
		l.metrics.SetLastRowRead(time.Now())

		// Validate the parsed row. A validation failure means a required field
		// (timestamp, metric_name, uuid, or value) is empty. This is distinct
		// from a send failure: retrying will not help because the data is bad.
		if err := row.Validate(); err != nil {
			l.logger.Warn("skipping invalid row", "err", err)
			l.metrics.IncErrors()
			consecutiveBad++
			if l.isTooManyConsecutiveErrors(consecutiveBad) {
				return
			}

			// Sleep before moving to the next row so the inter-row interval
			// is maintained even when rows are being skipped.
			if !l.sleepOrStop(ctx) {
				return
			}

			continue
		}

		// Deliver the row to the message-queue. The publisher applies the full
		// retry policy (up to cfg.RetryAttempts attempts with cfg.RetryDelay
		// between each) internally, so we only see success or final failure here.
		sendErr := l.publisher.Publish(ctx, row)

		if sendErr != nil {
			// The row was valid but could not be delivered after all retries.
			// Increment errors_total, log the failure, and skip to the next row.
			// Do NOT increment consecutiveBad — the data was good; the network
			// (or queue) was not. Do NOT reset consecutiveBad either.
			l.logger.Error("failed to deliver row after all retries", "err", sendErr)
			l.metrics.IncErrors()
		} else {
			// Successful delivery: update the sent counter and timestamp, then
			// reset the consecutive bad-row counter because a good delivery
			// confirms the file and network are both healthy.
			l.metrics.IncRowsSent()
			l.metrics.SetLastSent(time.Now())
			consecutiveBad = 0
		}

		// Sleep for the configured interval before reading the next row.
		// This is the primary throughput knob: increasing STREAMER_INTERVAL_MS
		// reduces the load on the message-queue.
		if !l.sleepOrStop(ctx) {
			return
		}
	}
}

// sleepOrStop pauses for cfg.Interval. It returns true if the sleep completed
// normally, or false if ctx was cancelled during the sleep (indicating that
// the caller should exit the loop immediately).
func (l *TelemetryLoop) sleepOrStop(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		// Shutdown signal received during sleep; tell the caller to exit.
		return false
	case <-time.After(l.cfg.Interval):
		// Interval elapsed; the loop may continue.
		return true
	}
}

// isTooManyConsecutiveErrors checks whether consecutiveBad has reached the
// fatal threshold. If it has, it logs a fatal-level message and returns true,
// signalling the caller to exit the loop (and ultimately the process).
//
// Ten consecutive bad rows almost certainly means the CSV file is corrupt or
// the wrong file was mounted, so continuing to skip rows silently would hide
// a critical operational problem.
func (l *TelemetryLoop) isTooManyConsecutiveErrors(consecutiveBad int) bool {
	if consecutiveBad < l.cfg.MaxConsecutiveErrors {
		return false
	}

	l.logger.Error(
		"too many consecutive bad rows — the mounted CSV file may be corrupt or wrong",
		"consecutive_bad", consecutiveBad,
		"max_allowed", l.cfg.MaxConsecutiveErrors,
	)

	return true
}
