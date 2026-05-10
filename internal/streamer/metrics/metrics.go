package metrics

import (
	"sync/atomic"
	"time"
)

// Metrics holds the four observability indicators for the streamer service.
// All fields are updated exclusively by the single telemetry loop goroutine,
// and read by the Gin server goroutine when serving GET /metrics.
// atomic operations make cross-goroutine reads safe without a mutex.
type Metrics struct {
	// rowsSentTotal counts every row that was successfully delivered to the
	// message-queue. This counter only ever increases (even across file loops).
	rowsSentTotal atomic.Int64

	// errorsTotal counts every failure — both send failures (after all retries
	// are exhausted) and bad-row skips (validation errors). Like rowsSentTotal,
	// this counter is cumulative and resets to zero only on process restart.
	errorsTotal atomic.Int64

	// lastSentNano is the Unix timestamp (in nanoseconds) of the most recent
	// successful POST to the message-queue. It is stored as nanoseconds so a
	// single atomic Int64 can hold the full precision of time.Time.
	lastSentNano atomic.Int64

	// lastRowReadNano is the Unix timestamp (in nanoseconds) of the most recent
	// successful CSV row read. Comparing this against lastSentNano lets an
	// operator tell whether a stall is on the read side or the send side.
	lastRowReadNano atomic.Int64
}

// Snapshot is a point-in-time copy of all metrics values, ready for JSON
// serialisation. Timestamps are represented as Unix seconds (float64) so they
// can be consumed directly by Prometheus or a JSON dashboard.
type Snapshot struct {
	RowsSentTotal               int64   `json:"rows_sent_total"`
	ErrorsTotal                 int64   `json:"errors_total"`
	LastSentTimestampSeconds    float64 `json:"last_sent_timestamp_seconds"`
	LastRowReadTimestampSeconds float64 `json:"last_row_read_timestamp_seconds"`
}

// New creates an initialised Metrics instance with all counters at zero.
func New() *Metrics {
	return &Metrics{}
}

// IncRowsSent increments the rows-sent counter by one.
// Called by the loop after every successful POST to the queue.
func (m *Metrics) IncRowsSent() {
	m.rowsSentTotal.Add(1)
}

// IncErrors increments the error counter by one.
// Called by the loop on both send failures (after retries) and bad-row skips.
func (m *Metrics) IncErrors() {
	m.errorsTotal.Add(1)
}

// SetLastSent records the timestamp of the most recent successful send.
// Called by the loop immediately after a POST returns 2xx.
func (m *Metrics) SetLastSent(t time.Time) {
	m.lastSentNano.Store(t.UnixNano())
}

// SetLastRowRead records the timestamp of the most recent successful CSV row read.
// Called by the loop immediately after csv.Reader.Read() returns without error.
func (m *Metrics) SetLastRowRead(t time.Time) {
	m.lastRowReadNano.Store(t.UnixNano())
}

// Snapshot returns a consistent point-in-time copy of all metric values.
// Timestamps are converted from nanoseconds to seconds. A zero timestamp
// (i.e. no row has been read or sent yet) is represented as 0.0.
func (m *Metrics) Snapshot() Snapshot {
	// Load all atomic values before converting so we minimise the window
	// in which each value could be updated between reads.
	sentNano := m.lastSentNano.Load()
	readNano := m.lastRowReadNano.Load()

	// Convert nanosecond timestamps to fractional seconds. A zero value
	// means the event has not yet occurred since process start.
	var sentSec, readSec float64
	if sentNano != 0 {
		sentSec = float64(sentNano) / float64(time.Second)
	}
	if readNano != 0 {
		readSec = float64(readNano) / float64(time.Second)
	}

	return Snapshot{
		RowsSentTotal:               m.rowsSentTotal.Load(),
		ErrorsTotal:                 m.errorsTotal.Load(),
		LastSentTimestampSeconds:    sentSec,
		LastRowReadTimestampSeconds: readSec,
	}
}
