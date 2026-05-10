package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/config"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/csv_reader"
)

// publishRequest is the JSON payload the message-queue expects on POST /messages.
// It mirrors message_queue.PublishRequest from the queue service's model package,
// defined here to avoid a circular dependency between streamer and message-queue
// internal packages.
type publishRequest struct {
	Timestamp  string `json:"timestamp"`
	MetricName string `json:"metric_name"`
	GpuID      string `json:"gpu_id"`
	Device     string `json:"device"`
	UUID       string `json:"uuid"`
	ModelName  string `json:"model_name"`
	Hostname   string `json:"hostname"`
	Value      string `json:"value"`
	LabelsRaw  string `json:"labels_raw"`
}

// publishResponse is the success body returned by POST /messages.
// We only read MessageID for logging purposes.
type publishResponse struct {
	MessageID string `json:"message_id"`
}

// Publisher delivers a single CSV row to the message-queue via HTTP POST.
// It encapsulates the retry policy (attempts, delay) and per-request timeout
// configured in StreamerConfig. It is not safe for concurrent use, but because
// the telemetry loop is a single goroutine, no locking is required.
type Publisher struct {
	// client is a shared HTTP client whose Transport reuses connections across
	// requests, avoiding the overhead of a new TCP handshake per row.
	client *http.Client

	// messagesURL is the fully-qualified POST endpoint, pre-built once at
	// construction to avoid repeated string concatenation on the hot path.
	messagesURL string

	// cfg holds the retry and timeout policy copied from StreamerConfig.
	cfg config.StreamerConfig

	// logger is used to record per-attempt failures without stopping the loop.
	logger *slog.Logger
}

// New creates a Publisher ready to deliver rows to the message-queue.
// The HTTP client is configured with a transport that respects the configured
// per-request timeout at the context level (not the client level), so each
// individual attempt gets its own isolated deadline.
func New(cfg config.StreamerConfig, logger *slog.Logger) *Publisher {
	return &Publisher{
		// Use a default transport so idle connections are reused across rows.
		// Per-request timeouts are enforced via context.WithTimeout in doPublish,
		// not via http.Client.Timeout, so that each retry gets its own deadline.
		client:      &http.Client{},
		messagesURL: cfg.QueueURL + "/messages",
		cfg:         cfg,
		logger:      logger,
	}
}

// Publish delivers row to the message-queue, retrying up to cfg.RetryAttempts times
// on transient failure. Between attempts it sleeps for cfg.RetryDelay, but
// cancels the sleep immediately if ctx is done (e.g. on shutdown).
//
// Returns nil on success. Returns a non-nil error if all attempts fail, which
// the caller (loop) uses to increment errors_total and skip the row.
func (s *Publisher) Publish(ctx context.Context, row csv_reader.CSVRow) error {
	// Serialise the Row into a JSON payload once. The same bytes are reused
	// across all retry attempts because the payload does not change.
	body, err := s.marshalRow(row)
	if err != nil {
		// JSON marshalling failure is not a transient error; retrying would
		// produce the same result. Return immediately.
		return fmt.Errorf("marshalling row: %w", err)
	}

	var lastErr error

	for attempt := 1; attempt <= s.cfg.RetryAttempts; attempt++ {
		// On all attempts after the first, wait for the retry delay before
		// publishing. The select also exits early if the context is cancelled so
		// shutdown is not delayed by a pending retry sleep.
		if attempt > 1 {
			select {
			case <-ctx.Done():
				// The outer context was cancelled (shutdown in progress).
				// Return the last publish error so the loop can decide to exit.
				return fmt.Errorf("publish cancelled during retry backoff: %w", lastErr)
			case <-time.After(s.cfg.RetryDelay):
				// Delay elapsed; proceed with the next attempt.
			}
		}

		// Each attempt gets an independent timeout context so a slow response
		// on attempt N cannot eat into the deadline budget of attempt N+1.
		attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
		err := s.doPublish(attemptCtx, body)
		cancel()

		if err == nil {
			// The row was accepted by the queue. Return success immediately
			// so the loop can update metrics and advance to the next row.
			return nil
		}

		// Record the failure for potential return after all retries are exhausted.
		lastErr = err
		s.logger.Warn("publish attempt failed",
			"attempt", attempt,
			"max_attempts", s.cfg.RetryAttempts,
			"err", err,
		)
	}

	// All attempts failed. Return the last error so the caller can decide
	// whether to increment errors_total and skip the row.
	return fmt.Errorf("all %d publish attempts failed: %w", s.cfg.RetryAttempts, lastErr)
}

// doPublish performs a single HTTP POST to the messages endpoint using the
// provided context for the per-attempt deadline. It returns nil if the queue
// accepted the message (HTTP 2xx), or a descriptive error otherwise.
func (s *Publisher) doPublish(ctx context.Context, body []byte) error {
	// Build the request with the caller's deadline context so the HTTP stack
	// respects the per-attempt timeout and cancels the connection if exceeded.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.messagesURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building HTTP request: %w", err)
	}

	// The message-queue's Gin handler uses ShouldBindJSON which requires the
	// Content-Type header to be application/json.
	req.Header.Set("Content-Type", "application/json")

	// Execute the request. The client's transport reuses idle connections
	// from a shared pool so no new TCP handshake is needed per row.
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("executing HTTP POST: %w", err)
	}

	// Always drain and close the response body to return the connection to
	// the pool and prevent resource leaks.
	defer func() {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()
	}()

	// Any 2xx status means the queue accepted the message. We parse the
	// response body to extract the message_id for logging but do not
	// surface a decode error as a publish failure.
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var res publishResponse
		if err := json.NewDecoder(resp.Body).Decode(&res); err == nil && res.MessageID != "" {
			s.logger.Debug("row accepted by queue", "message_id", res.MessageID)
		}
		return nil
	}

	// Non-2xx responses are treated as transient failures that are eligible
	// for retry. We include the status code in the error so retry log messages
	// carry enough context for diagnosis.
	return fmt.Errorf("queue returned HTTP %d", resp.StatusCode)
}

// marshalRow converts a csv_reader.CSVRow into the JSON bytes expected by the
// message-queue POST /messages endpoint.
func (s *Publisher) marshalRow(row csv_reader.CSVRow) ([]byte, error) {
	// Map each Row field to the corresponding PublishRequest JSON key.
	// The struct is defined privately in this package to avoid importing
	// the message-queue's internal model package (which would create a
	// cross-service internal dependency).
	req := publishRequest{
		Timestamp:  row.Timestamp,
		MetricName: row.MetricName,
		GpuID:      row.GpuID,
		Device:     row.Device,
		UUID:       row.UUID,
		ModelName:  row.ModelName,
		Hostname:   row.Hostname,
		Value:      row.Value,
		LabelsRaw:  row.LabelsRaw,
	}

	// Marshal to compact JSON; the queue does not require pretty-printing.
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	return data, nil
}
