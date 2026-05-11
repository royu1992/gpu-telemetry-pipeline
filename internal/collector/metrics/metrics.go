package metrics

import (
	"sync/atomic"
	"time"
)

// Metrics holds the observability counters for the collector service.
// All fields use atomic operations so the consumption loop goroutine can
// write to them while the Gin server goroutine reads them without a mutex.
type Metrics struct {
	// messagesConsumedTotal counts every message pulled from the queue,
	// regardless of whether it was subsequently validated and stored.
	messagesConsumedTotal atomic.Int64

	// dbWritesSuccessTotal counts every batch that was successfully committed
	// to Postgres. One bulk-insert call increments this by one, not by batch size.
	dbWritesSuccessTotal atomic.Int64

	// dbWritesErrorTotal counts every batch that failed to commit to Postgres.
	// A non-zero value means redeliveries will occur from the queue.
	dbWritesErrorTotal atomic.Int64

	// validationErrorsTotal counts individual messages within a batch that
	// failed the type-conversion or validation step and were dropped.
	validationErrorsTotal atomic.Int64

	// lastDBWriteNano is the Unix timestamp (nanoseconds) of the most recent
	// successful bulk-insert commit. A stale value relative to
	// messagesConsumedTotal indicates the DB write path has stalled.
	lastDBWriteNano atomic.Int64

	// startTime is captured at construction and used to compute the uptime
	// field in the snapshot without storing a separate elapsed counter.
	startTime time.Time
}

// Snapshot is a point-in-time copy of all metrics values, ready for JSON
// serialisation and HTTP delivery via GET /metrics.
type Snapshot struct {
	MessagesConsumedTotal int64   `json:"messages_consumed_total"`
	DBWritesSuccessTotal  int64   `json:"db_writes_success_total"`
	DBWritesErrorTotal    int64   `json:"db_writes_error_total"`
	ValidationErrorsTotal int64   `json:"validation_errors_total"`
	LastDBWriteTimestamp  float64 `json:"last_db_write_timestamp_seconds"`
	UptimeSeconds         int64   `json:"uptime_seconds"`
}

// New creates an initialised Metrics instance with all counters at zero
// and the start time recorded at the moment of construction.
func New() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// AddMessagesConsumed increments the messages-consumed counter by n.
// Called once per successful poll response with the number of items received.
func (m *Metrics) AddMessagesConsumed(n int64) {
	m.messagesConsumedTotal.Add(n)
}

// IncDBWritesSuccess increments the successful-DB-writes counter by one.
// Called after each batch is committed to Postgres without error.
func (m *Metrics) IncDBWritesSuccess() {
	m.dbWritesSuccessTotal.Add(1)
}

// IncDBWritesError increments the failed-DB-writes counter by one.
// Called when a bulk-insert returns an error, resulting in no ACK being sent.
func (m *Metrics) IncDBWritesError() {
	m.dbWritesErrorTotal.Add(1)
}

// IncValidationError increments the validation-errors counter by one.
// Called for each individual message in a batch that fails type conversion.
func (m *Metrics) IncValidationError() {
	m.validationErrorsTotal.Add(1)
}

// SetLastDBWrite records the timestamp of the most recent successful
// bulk-insert commit. Called immediately after the pool.SendBatch call succeeds.
func (m *Metrics) SetLastDBWrite(t time.Time) {
	m.lastDBWriteNano.Store(t.UnixNano())
}

// Snapshot returns a consistent point-in-time copy of all metric values.
// The last-write timestamp is converted from nanoseconds to fractional seconds.
// A zero timestamp means no successful write has occurred since process start.
func (m *Metrics) Snapshot() Snapshot {
	// Load all atomic values before converting so the observation window
	// is as small as possible.
	writeNano := m.lastDBWriteNano.Load()

	// Convert nanosecond timestamp to fractional seconds.
	// Zero means the event has not occurred since process start.
	var writeSec float64
	if writeNano != 0 {
		writeSec = float64(writeNano) / float64(time.Second)
	}

	return Snapshot{
		MessagesConsumedTotal: m.messagesConsumedTotal.Load(),
		DBWritesSuccessTotal:  m.dbWritesSuccessTotal.Load(),
		DBWritesErrorTotal:    m.dbWritesErrorTotal.Load(),
		ValidationErrorsTotal: m.validationErrorsTotal.Load(),
		LastDBWriteTimestamp:  writeSec,
		UptimeSeconds:         int64(time.Since(m.startTime).Seconds()),
	}
}
