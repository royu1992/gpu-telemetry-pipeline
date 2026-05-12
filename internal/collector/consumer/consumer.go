package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/config"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/metrics"
	mq "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/model"
	store "github.com/royu1992/gpu-telemetry-pipeline/internal/store"
)

// Consumer manages the full poll → validate → persist → ACK lifecycle.
// It owns the HTTP client used to communicate with the message-queue and
// delegates all Postgres operations to the Store it receives at construction.
type Consumer struct {
	// cfg holds all timing and connection parameters.
	cfg config.CollectorConfig

	// store is the Postgres persistence layer. BulkInsert is the only
	// method called from the consumption loop.
	store *store.Store

	// metrics holds the shared atomic counters updated during the loop.
	metrics *metrics.Metrics

	// consumerID is sent with every consume and ack request so the queue
	// can track lease ownership per-pod. It is stable for the process lifetime.
	consumerID string

	// logger is the structured JSON logger shared with main.
	logger *slog.Logger

	// client is a shared HTTP client whose transport reuses idle TCP connections
	// across all poll and ack requests.
	client *http.Client

	// consumeBaseURL is the fully-qualified consume endpoint, pre-built once
	// to avoid repeated string concatenation on the hot path.
	consumeBaseURL string

	// ackURL is the fully-qualified POST /messages/ack endpoint.
	ackURL string

	// storeFunc is the function called to persist a validated batch of rows.
	// In production it is set to c.store.BulkInsert; tests may replace it with
	// a lightweight in-memory implementation to avoid a real Postgres dependency.
	storeFunc func(ctx context.Context, rows []store.Row) error
}

// hostnameFunc is the function used to obtain the OS hostname.
// It is a package-level variable so tests can substitute a failing
// implementation to exercise the UUID fallback path in New without
// requiring OS-level manipulation.
var hostnameFunc = os.Hostname

// New creates a Consumer and derives the consumer ID from the OS hostname,
// falling back to a random UUID if the hostname is unavailable. This ensures
// each pod in a Kubernetes deployment identifies itself uniquely to the queue.
func New(cfg config.CollectorConfig, s *store.Store, m *metrics.Metrics, logger *slog.Logger) *Consumer {
	// Attempt to use the OS hostname (the Kubernetes Pod name) as the consumer
	// identifier. If hostnameFunc fails or returns an empty string, generate a
	// random UUID so the service remains functional in non-Kubernetes environments.
	id, err := hostnameFunc()
	if err != nil || id == "" {
		id = uuid.New().String()
		logger.Info("hostname unavailable, using generated consumer ID", "consumer_id", id)
	}

	c := &Consumer{
		cfg:            cfg,
		store:          s,
		metrics:        m,
		consumerID:     id,
		logger:         logger,
		client:         &http.Client{},
		consumeBaseURL: cfg.QueueURL + "/messages/consume",
		ackURL:         cfg.QueueURL + "/messages/ack",
	}

	// Wire the store's BulkInsert as the default persistence function.
	// Tests that do not need a real Postgres connection can set storeFunc
	// to a lightweight in-memory implementation after construction.
	if s != nil {
		c.storeFunc = s.BulkInsert
	}

	return c
}

// Run is the main consumption loop. It repeatedly polls the queue, processes
// each received batch, persists valid rows to Postgres, and acknowledges
// successfully stored messages. Run blocks until ctx is cancelled, after which
// it returns so the caller (main) can proceed with shutdown.
//
// Error handling per iteration:
//   - Poll error (non-context): log, apply ErrorBackoff, continue.
//   - Empty response (204):     re-poll immediately without logging.
//   - Validation errors:        skip the individual row, increment counter.
//   - DB write error:           log, increment counter, skip ACK, continue.
//   - ACK error:                log (non-fatal, data is in DB, duplicates deduped).
func (c *Consumer) Run(ctx context.Context) {
	c.logger.Info("consumption loop started", "consumer_id", c.consumerID)

	for {
		// Check for cancellation at the top of every iteration so we return
		// immediately after SIGTERM without waiting for the next poll.
		select {
		case <-ctx.Done():
			c.logger.Info("consumption loop stopping", "reason", ctx.Err())
			return
		default:
		}

		// Poll the queue for a batch of messages.
		items, err := c.poll(ctx)
		if err != nil {
			// Distinguish a normal context-cancellation from a real error.
			// On cancellation we return immediately; on real errors we apply
			// a backoff to avoid hammering a degraded upstream service.
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("poll failed", "err", err)

			// Apply the error backoff. The select ensures we still respond
			// to a cancellation signal during the backoff sleep.
			select {
			case <-time.After(c.cfg.ErrorBackoff):
			case <-ctx.Done():
				return
			}

			continue
		}

		// A nil or empty slice means the queue returned 204 No Content —
		// the queue is empty and the long-poll timed out. Re-poll immediately.
		if len(items) == 0 {
			continue
		}

		// Update the consumed counter with the full batch size, including rows
		// that may later fail validation. This gives an accurate picture of
		// throughput from the queue's perspective.
		c.metrics.AddMessagesConsumed(int64(len(items)))

		// Validate and convert each message. Returns the DB-ready rows and
		// the DeliveryIDs of the rows that passed validation.
		rows, deliveryIDs := c.processBatch(items)

		// If the entire batch failed validation, skip the DB write and ACK.
		// The queue's lease-expiry will redeliver the messages; if they fail
		// again they will eventually reach the max-delivery-attempts limit and
		// be dropped by the queue.
		if len(rows) == 0 {
			c.logger.Warn("entire batch failed validation, skipping", "batch_size", len(items))
			continue
		}

		// Attempt to persist all validated rows in a single round-trip.
		if err := c.storeFunc(ctx, rows); err != nil {
			// Distinguish context cancellation from a genuine DB failure.
			if ctx.Err() != nil {
				return
			}

			c.logger.Error("bulk insert failed",
				"err", err,
				"rows", len(rows),
			)

			// Increment the DB error counter so the /metrics endpoint reflects
			// the failure, then continue without ACKing so the queue redelivers.
			c.metrics.IncDBWritesError()
			continue
		}

		// Record successful write metrics.
		c.metrics.IncDBWritesSuccess()
		c.metrics.SetLastDBWrite(time.Now())

		// Acknowledge only the DeliveryIDs of rows that were successfully
		// stored. Rows that failed validation were excluded from deliveryIDs
		// above; their leases will expire and the queue will redeliver them.
		if err := c.ack(ctx, deliveryIDs); err != nil {
			if ctx.Err() != nil {
				return
			}

			// ACK failure is non-fatal: the data is already in Postgres.
			// Any redelivered duplicates will be silently ignored by
			// ON CONFLICT DO NOTHING.
			c.logger.Error("ack failed — data committed, duplicates will be deduped",
				"err", err,
				"delivery_ids", len(deliveryIDs),
			)
		}
	}
}

// poll sends a single long-poll GET /messages/consume request to the queue
// and returns the received delivery items.
//
// It returns (nil, nil) when the queue is empty (204 No Content).
// It returns (nil, err) on any HTTP error or response-body parse failure.
//
// The request context combines the configured long-poll window plus the
// request-timeout buffer, so the HTTP client deadline always exceeds the
// server-side wait duration.
func (c *Consumer) poll(ctx context.Context) ([]mq.DeliveryItem, error) {
	// Build the consume URL with query parameters using net/url so the
	// consumer_id is properly percent-encoded (important for UUIDs or hostnames
	// containing special characters).
	u, err := url.Parse(c.consumeBaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse consume URL: %w", err)
	}
	q := u.Query()
	q.Set("consumer_id", c.consumerID)
	q.Set("batch_size", strconv.Itoa(c.cfg.BatchSize))
	q.Set("long_poll_timeout", c.cfg.LongPollTimeout.String())
	u.RawQuery = q.Encode()

	// The request deadline must exceed LongPollTimeout so the HTTP client
	// does not cancel the connection before the server has had its full
	// wait window. We add RequestTimeout as a buffer for connection and
	// response-write overhead.
	pollCtx, cancel := context.WithTimeout(ctx, c.cfg.LongPollTimeout+c.cfg.RequestTimeout)
	defer cancel()

	// Construct the request with the derived context so it is automatically
	// cancelled if the parent context (shutdown signal) fires.
	req, err := http.NewRequestWithContext(pollCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create poll request: %w", err)
	}

	// Execute the request. The transport reuses TCP connections from the pool.
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do poll request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// Decode the JSON response body into the ConsumeResponse wrapper.
		var result mq.ConsumeResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("decode consume response: %w", err)
		}
		return result.Messages, nil

	case http.StatusNoContent:
		// The queue is empty and the long-poll timed out. This is the normal
		// idle state — return nil without an error so the loop re-polls.
		return nil, nil

	default:
		// Any other status (4xx, 5xx) is an unexpected error. Read the body
		// to include it in the error message for easier debugging.
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("unexpected status %d from queue (failed to read body: %w)", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("unexpected status %d from queue: %s", resp.StatusCode, body)
	}
}

// processBatch validates and converts each delivery item in the batch.
// It returns two parallel slices:
//   - rows: the DB-ready store.Row values for items that passed validation.
//   - deliveryIDs: the DeliveryIDs corresponding to the rows above.
//
// Items that fail timestamp parsing or value parsing are logged and counted
// as validation errors; they are excluded from both return slices so their
// leases expire and the queue redelivers them.
func (c *Consumer) processBatch(items []mq.DeliveryItem) ([]store.Row, []string) {
	// Pre-allocate with the batch capacity to avoid repeated reallocation.
	rows := make([]store.Row, 0, len(items))
	deliveryIDs := make([]string, 0, len(items))

	for _, item := range items {
		msg := item.Message

		// Parse the timestamp string (RFC 3339 / ISO 8601) into time.Time.
		// The queue stores all timestamps as strings to remain format-agnostic.
		ts, err := time.Parse(time.RFC3339, msg.Timestamp)
		if err != nil {
			c.logger.Warn("invalid timestamp, skipping row",
				"message_id", msg.MessageID,
				"timestamp", msg.Timestamp,
				"err", err,
			)

			c.metrics.IncValidationError()

			continue
		}

		// Parse the value string into float64 for storage as DOUBLE PRECISION.
		// DCGM exports values as decimal strings (e.g. "100", "0.5").
		val, err := strconv.ParseFloat(msg.Value, 64)
		if err != nil {
			c.logger.Warn("invalid value, skipping row",
				"message_id", msg.MessageID,
				"value", msg.Value,
				"err", err,
			)

			c.metrics.IncValidationError()

			continue
		}

		// Conversion succeeded: append the DB-ready row and its corresponding
		// DeliveryID (not MessageID — see architecture doc for the distinction).
		rows = append(rows, store.Row{
			Ts:         ts,
			Hostname:   msg.Hostname,
			GpuID:      msg.GpuID,
			MetricName: msg.MetricName,
			Value:      val,
			Device:     msg.Device,
			UUID:       msg.UUID,
			ModelName:  msg.ModelName,
			LabelsRaw:  msg.LabelsRaw,
			MessageID:  msg.MessageID,
		})

		deliveryIDs = append(deliveryIDs, item.DeliveryID)
	}

	return rows, deliveryIDs
}

// ack sends a POST /messages/ack request to the queue to release the leases
// for the provided DeliveryIDs. Only delivery IDs corresponding to rows that
// were successfully committed to Postgres should be passed here.
//
// A failed ACK is non-fatal: the data is already in Postgres and any
// re-delivered duplicates will be silently dropped by ON CONFLICT DO NOTHING.
func (c *Consumer) ack(ctx context.Context, deliveryIDs []string) error {
	// Build the ACK payload with the consumer_id (required by the queue API)
	// and the list of DeliveryIDs to release.
	payload := mq.AckRequest{
		ConsumerID:  c.consumerID,
		DeliveryIDs: deliveryIDs,
	}

	// Serialise the payload to JSON.
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal ack payload: %w", err)
	}

	// Apply the per-call request timeout. Unlike the poll, the ACK is a fast
	// round-trip with no long-poll wait, so RequestTimeout is the full budget.
	ackCtx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()

	// Construct the POST request with the JSON body and context deadline.
	req, err := http.NewRequestWithContext(ackCtx, http.MethodPost, c.ackURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create ack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute the ACK request.
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("do ack request: %w", err)
	}
	defer resp.Body.Close()

	// Treat any non-2xx status as an error. Read the body for diagnostics.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("unexpected ack status %d (failed to read body: %w)", resp.StatusCode, err)
		}
		return fmt.Errorf("unexpected ack status %d: %s", resp.StatusCode, b)
	}

	return nil
}
