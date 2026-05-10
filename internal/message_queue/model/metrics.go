package message_queue

import (
	"sync/atomic"
	"time"
)

// Metrics holds the message-queue observability counters.
// All counters are monotonically increasing and safe for concurrent access.
type Metrics struct {
	totalPublished   atomic.Int64
	totalAcked       atomic.Int64
	totalRedelivered atomic.Int64
	totalDropped     atomic.Int64
	startTime        time.Time
}

// Snapshot is a point-in-time copy of queue metrics combined with live queue
// state passed in by the caller.
type Snapshot struct {
	QueueDepth       int   `json:"queue_depth"`
	Capacity         int   `json:"capacity"`
	InFlight         int   `json:"in_flight"`
	TotalPublished   int64 `json:"total_published"`
	TotalAcked       int64 `json:"total_acked"`
	TotalRedelivered int64 `json:"total_redelivered"`
	TotalDropped     int64 `json:"total_dropped"`
	UptimeSeconds    int64 `json:"uptime_seconds"`
}

// NewMetrics creates and initialises a Metrics instance.
func NewMetrics() *Metrics {
	// Record the wall-clock time at initialisation so uptime can be derived
	// on demand without storing a separate start timestamp elsewhere.
	return &Metrics{
		startTime: time.Now(),
	}
}

// IncPublished increments the published message counter.
func (m *Metrics) IncPublished() {
	m.totalPublished.Add(1)
}

// IncAcked increments the acknowledged message counter.
func (m *Metrics) IncAcked() {
	m.totalAcked.Add(1)
}

// IncRedelivered increments the redelivered message counter.
func (m *Metrics) IncRedelivered() {
	m.totalRedelivered.Add(1)
}

// IncDropped increments the dropped message counter.
func (m *Metrics) IncDropped() {
	m.totalDropped.Add(1)
}

// Snapshot returns current metric values merged with the provided live queue stats.
func (m *Metrics) Snapshot(queueDepth, capacity, inFlight int) Snapshot {
	return Snapshot{
		QueueDepth:       queueDepth,
		Capacity:         capacity,
		InFlight:         inFlight,
		TotalPublished:   m.totalPublished.Load(),
		TotalAcked:       m.totalAcked.Load(),
		TotalRedelivered: m.totalRedelivered.Load(),
		TotalDropped:     m.totalDropped.Load(),
		UptimeSeconds:    int64(time.Since(m.startTime).Seconds()),
	}
}
