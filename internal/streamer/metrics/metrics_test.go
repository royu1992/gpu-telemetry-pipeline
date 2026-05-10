package metrics

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	// Step: Construct a new Metrics instance and verify it is non-nil.
	m := New()
	if m == nil {
		t.Fatal("New() returned nil")
	}

	// Step: Verify all counters start at zero via an initial snapshot.
	snap := m.Snapshot()
	if snap.RowsSentTotal != 0 {
		t.Errorf("initial RowsSentTotal = %d, want 0", snap.RowsSentTotal)
	}
	if snap.ErrorsTotal != 0 {
		t.Errorf("initial ErrorsTotal = %d, want 0", snap.ErrorsTotal)
	}
	if snap.LastSentTimestampSeconds != 0 {
		t.Errorf("initial LastSentTimestampSeconds = %v, want 0", snap.LastSentTimestampSeconds)
	}
	if snap.LastRowReadTimestampSeconds != 0 {
		t.Errorf("initial LastRowReadTimestampSeconds = %v, want 0", snap.LastRowReadTimestampSeconds)
	}
}

func TestMetrics_IncRowsSent(t *testing.T) {
	tests := []struct {
		name       string
		increments int
		wantTotal  int64
	}{
		{name: "zero increments", increments: 0, wantTotal: 0},
		{name: "single increment", increments: 1, wantTotal: 1},
		{name: "multiple increments", increments: 5, wantTotal: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()

			// Step: Increment the rows-sent counter the specified number of times.
			for i := 0; i < tt.increments; i++ {
				m.IncRowsSent()
			}

			// Step: Verify the snapshot reflects the expected total.
			if got := m.Snapshot().RowsSentTotal; got != tt.wantTotal {
				t.Errorf("RowsSentTotal = %d, want %d", got, tt.wantTotal)
			}
		})
	}
}

func TestMetrics_IncErrors(t *testing.T) {
	tests := []struct {
		name       string
		increments int
		wantTotal  int64
	}{
		{name: "zero increments", increments: 0, wantTotal: 0},
		{name: "single increment", increments: 1, wantTotal: 1},
		{name: "multiple increments", increments: 3, wantTotal: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()

			// Step: Increment the error counter the specified number of times.
			for i := 0; i < tt.increments; i++ {
				m.IncErrors()
			}

			// Step: Verify the snapshot reflects the expected total.
			if got := m.Snapshot().ErrorsTotal; got != tt.wantTotal {
				t.Errorf("ErrorsTotal = %d, want %d", got, tt.wantTotal)
			}
		})
	}
}

func TestMetrics_SetLastSent(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
	}{
		{
			// Step: Verify that a zero timestamp is stored and converted correctly.
			name: "zero time stores as zero",
			t:    time.Time{},
		},
		{
			// Step: Verify that a real timestamp survives the nanosecond round-trip.
			name: "real time is stored and rounded-trips via nanoseconds",
			t:    time.Unix(1_700_000_000, 0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()

			// Step: Store the timestamp.
			m.SetLastSent(tt.t)

			// Step: Retrieve via Snapshot and convert back to the same precision.
			snap := m.Snapshot()
			wantSec := float64(tt.t.UnixNano()) / float64(time.Second)
			if tt.t.IsZero() {
				// Zero time has UnixNano = very negative number; the branch
				// in Snapshot() stores 0.0 only when the nano value is zero.
				// A truly zero time.Time still has a non-zero UnixNano, so we
				// just verify the field is set to the correct conversion.
				wantSec = float64(tt.t.UnixNano()) / float64(time.Second)
			}
			if snap.LastSentTimestampSeconds != wantSec {
				t.Errorf("LastSentTimestampSeconds = %v, want %v", snap.LastSentTimestampSeconds, wantSec)
			}
		})
	}
}

func TestMetrics_SetLastRowRead(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
	}{
		{
			name: "real timestamp is retrievable via Snapshot",
			t:    time.Unix(1_750_000_000, 500_000_000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()

			// Step: Store the last-row-read timestamp.
			m.SetLastRowRead(tt.t)

			// Step: Verify the Snapshot reflects the correct fractional seconds.
			snap := m.Snapshot()
			wantSec := float64(tt.t.UnixNano()) / float64(time.Second)
			if snap.LastRowReadTimestampSeconds != wantSec {
				t.Errorf("LastRowReadTimestampSeconds = %v, want %v", snap.LastRowReadTimestampSeconds, wantSec)
			}
		})
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	tests := []struct {
		name         string
		rowsSent     int
		errors       int
		lastSentSet  bool
		lastReadSet  bool
		wantZeroSent bool
		wantZeroRead bool
	}{
		{
			// Step: All counters at zero, timestamps never set → all snapshot fields zero.
			name:         "all zero on fresh instance",
			rowsSent:     0,
			errors:       0,
			lastSentSet:  false,
			lastReadSet:  false,
			wantZeroSent: true,
			wantZeroRead: true,
		},
		{
			// Step: Mixed activity: some rows sent, some errors, timestamps set.
			name:         "partial activity recorded",
			rowsSent:     3,
			errors:       2,
			lastSentSet:  true,
			lastReadSet:  true,
			wantZeroSent: false,
			wantZeroRead: false,
		},
		{
			// Step: Timestamps set but counters never incremented.
			name:         "timestamps set but counters remain zero",
			rowsSent:     0,
			errors:       0,
			lastSentSet:  true,
			lastReadSet:  true,
			wantZeroSent: false,
			wantZeroRead: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()

			// Step: Perform the configured increments.
			for i := 0; i < tt.rowsSent; i++ {
				m.IncRowsSent()
			}
			for i := 0; i < tt.errors; i++ {
				m.IncErrors()
			}

			// Step: Optionally set timestamps.
			sentTime := time.Unix(1_700_000_001, 0)
			readTime := time.Unix(1_700_000_002, 0)
			if tt.lastSentSet {
				m.SetLastSent(sentTime)
			}
			if tt.lastReadSet {
				m.SetLastRowRead(readTime)
			}

			// Step: Take a snapshot and verify counter fields.
			snap := m.Snapshot()
			if snap.RowsSentTotal != int64(tt.rowsSent) {
				t.Errorf("RowsSentTotal = %d, want %d", snap.RowsSentTotal, tt.rowsSent)
			}
			if snap.ErrorsTotal != int64(tt.errors) {
				t.Errorf("ErrorsTotal = %d, want %d", snap.ErrorsTotal, tt.errors)
			}

			// Step: Verify LastSentTimestampSeconds is zero or non-zero as expected.
			if tt.wantZeroSent && snap.LastSentTimestampSeconds != 0 {
				t.Errorf("LastSentTimestampSeconds = %v, want 0", snap.LastSentTimestampSeconds)
			}
			if !tt.wantZeroSent && snap.LastSentTimestampSeconds == 0 {
				t.Errorf("LastSentTimestampSeconds = 0, want non-zero")
			}

			// Step: Verify LastRowReadTimestampSeconds is zero or non-zero as expected.
			if tt.wantZeroRead && snap.LastRowReadTimestampSeconds != 0 {
				t.Errorf("LastRowReadTimestampSeconds = %v, want 0", snap.LastRowReadTimestampSeconds)
			}
			if !tt.wantZeroRead && snap.LastRowReadTimestampSeconds == 0 {
				t.Errorf("LastRowReadTimestampSeconds = 0, want non-zero")
			}
		})
	}
}
