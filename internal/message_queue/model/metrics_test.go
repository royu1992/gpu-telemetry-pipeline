package message_queue

import (
	"testing"
	"time"
)

func TestNewMetrics(t *testing.T) {
	m := NewMetrics()
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}
	snap := m.Snapshot(0, 100, 0)
	if snap.TotalPublished != 0 || snap.TotalAcked != 0 || snap.TotalRedelivered != 0 || snap.TotalDropped != 0 {
		t.Error("expected all zero counters on fresh metrics")
	}
	if snap.Capacity != 100 {
		t.Errorf("expected Capacity 100, got %d", snap.Capacity)
	}
}

func TestMetrics_Counters(t *testing.T) {
	tests := []struct {
		name        string
		published   int
		acked       int
		redelivered int
		dropped     int
	}{
		{"All zero", 0, 0, 0, 0},
		{"Published only", 5, 0, 0, 0},
		{"Full cycle", 10, 8, 2, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMetrics()
			for i := 0; i < tt.published; i++ {
				m.IncPublished()
			}
			for i := 0; i < tt.acked; i++ {
				m.IncAcked()
			}
			for i := 0; i < tt.redelivered; i++ {
				m.IncRedelivered()
			}
			for i := 0; i < tt.dropped; i++ {
				m.IncDropped()
			}
			snap := m.Snapshot(3, 50, 2)
			if snap.TotalPublished != int64(tt.published) {
				t.Errorf("expected TotalPublished %d, got %d", tt.published, snap.TotalPublished)
			}
			if snap.TotalAcked != int64(tt.acked) {
				t.Errorf("expected TotalAcked %d, got %d", tt.acked, snap.TotalAcked)
			}
			if snap.TotalRedelivered != int64(tt.redelivered) {
				t.Errorf("expected TotalRedelivered %d, got %d", tt.redelivered, snap.TotalRedelivered)
			}
			if snap.TotalDropped != int64(tt.dropped) {
				t.Errorf("expected TotalDropped %d, got %d", tt.dropped, snap.TotalDropped)
			}
			if snap.QueueDepth != 3 {
				t.Errorf("expected QueueDepth 3, got %d", snap.QueueDepth)
			}
			if snap.Capacity != 50 {
				t.Errorf("expected Capacity 50, got %d", snap.Capacity)
			}
			if snap.InFlight != 2 {
				t.Errorf("expected InFlight 2, got %d", snap.InFlight)
			}
		})
	}
}

func TestMetrics_UptimeSeconds(t *testing.T) {
	m := NewMetrics()
	time.Sleep(1 * time.Second)
	snap := m.Snapshot(0, 10, 0)
	if snap.UptimeSeconds < 1 {
		t.Errorf("expected uptime >= 1s, got %d", snap.UptimeSeconds)
	}
}
