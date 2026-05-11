package metrics

import (
	"math"
	"testing"
	"time"
)

// TestNew verifies that New() returns a non-nil Metrics instance with all
// counters initialised to zero and the start time recorded.
func TestNew(t *testing.T) {
	// Record a lower bound for the start time before construction.
	before := time.Now()

	m := New()

	// Record an upper bound for the start time after construction.
	after := time.Now()

	if m == nil {
		t.Fatal("New() returned nil")
	}

	// The start time must fall within [before, after].
	if m.startTime.Before(before) || m.startTime.After(after) {
		t.Errorf("startTime %v is outside [%v, %v]", m.startTime, before, after)
	}

	// All counters must be zero immediately after construction.
	snap := m.Snapshot()
	if snap.MessagesConsumedTotal != 0 {
		t.Errorf("MessagesConsumedTotal: got %d, want 0", snap.MessagesConsumedTotal)
	}
	if snap.DBWritesSuccessTotal != 0 {
		t.Errorf("DBWritesSuccessTotal: got %d, want 0", snap.DBWritesSuccessTotal)
	}
	if snap.DBWritesErrorTotal != 0 {
		t.Errorf("DBWritesErrorTotal: got %d, want 0", snap.DBWritesErrorTotal)
	}
	if snap.ValidationErrorsTotal != 0 {
		t.Errorf("ValidationErrorsTotal: got %d, want 0", snap.ValidationErrorsTotal)
	}
	if snap.LastDBWriteTimestamp != 0 {
		t.Errorf("LastDBWriteTimestamp: got %v, want 0", snap.LastDBWriteTimestamp)
	}
}

// TestAddMessagesConsumed verifies that AddMessagesConsumed accumulates the
// provided deltas correctly, including a multi-increment scenario.
func TestAddMessagesConsumed(t *testing.T) {
	tests := []struct {
		name   string
		deltas []int64
		want   int64
	}{
		{
			name:   "Single increment",
			deltas: []int64{5},
			want:   5,
		},
		{
			name:   "Multiple increments",
			deltas: []int64{3, 7, 10},
			want:   20,
		},
		{
			name:   "Zero increment",
			deltas: []int64{0},
			want:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()

			// Apply each delta in sequence.
			for _, d := range tt.deltas {
				m.AddMessagesConsumed(d)
			}

			snap := m.Snapshot()
			if snap.MessagesConsumedTotal != tt.want {
				t.Errorf("MessagesConsumedTotal: got %d, want %d", snap.MessagesConsumedTotal, tt.want)
			}
		})
	}
}

// TestIncDBWritesSuccess verifies that IncDBWritesSuccess increments the
// success counter by exactly one per call.
func TestIncDBWritesSuccess(t *testing.T) {
	tests := []struct {
		name  string
		calls int
		want  int64
	}{
		{"Zero calls", 0, 0},
		{"One call", 1, 1},
		{"Three calls", 3, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			for i := 0; i < tt.calls; i++ {
				m.IncDBWritesSuccess()
			}
			snap := m.Snapshot()
			if snap.DBWritesSuccessTotal != tt.want {
				t.Errorf("DBWritesSuccessTotal: got %d, want %d", snap.DBWritesSuccessTotal, tt.want)
			}
		})
	}
}

// TestIncDBWritesError verifies that IncDBWritesError increments the error
// counter by exactly one per call and is independent of the success counter.
func TestIncDBWritesError(t *testing.T) {
	tests := []struct {
		name  string
		calls int
		want  int64
	}{
		{"Zero calls", 0, 0},
		{"One call", 1, 1},
		{"Five calls", 5, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			for i := 0; i < tt.calls; i++ {
				m.IncDBWritesError()
			}
			snap := m.Snapshot()
			if snap.DBWritesErrorTotal != tt.want {
				t.Errorf("DBWritesErrorTotal: got %d, want %d", snap.DBWritesErrorTotal, tt.want)
			}
			// Success counter must remain zero — the error counter is independent.
			if snap.DBWritesSuccessTotal != 0 {
				t.Errorf("DBWritesSuccessTotal must remain 0, got %d", snap.DBWritesSuccessTotal)
			}
		})
	}
}

// TestIncValidationError verifies that IncValidationError increments the
// validation-errors counter by exactly one per call.
func TestIncValidationError(t *testing.T) {
	tests := []struct {
		name  string
		calls int
		want  int64
	}{
		{"Zero calls", 0, 0},
		{"Two calls", 2, 2},
		{"Ten calls", 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			for i := 0; i < tt.calls; i++ {
				m.IncValidationError()
			}
			snap := m.Snapshot()
			if snap.ValidationErrorsTotal != tt.want {
				t.Errorf("ValidationErrorsTotal: got %d, want %d", snap.ValidationErrorsTotal, tt.want)
			}
		})
	}
}

// TestSetLastDBWrite verifies that SetLastDBWrite stores the timestamp and
// that Snapshot converts it correctly from nanoseconds to fractional seconds.
func TestSetLastDBWrite(t *testing.T) {
	tests := []struct {
		name     string
		setTime  *time.Time // nil means SetLastDBWrite is never called
		wantZero bool       // true when we expect LastDBWriteTimestamp == 0
	}{
		{
			name:     "Never written — timestamp is zero",
			setTime:  nil,
			wantZero: true,
		},
		{
			name:     "Written once — timestamp is non-zero and accurate",
			setTime:  func() *time.Time { t := time.Now(); return &t }(),
			wantZero: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()

			if tt.setTime != nil {
				m.SetLastDBWrite(*tt.setTime)
			}

			snap := m.Snapshot()

			if tt.wantZero {
				// When never set, the converted value must be exactly zero.
				if snap.LastDBWriteTimestamp != 0 {
					t.Errorf("LastDBWriteTimestamp: got %v, want 0", snap.LastDBWriteTimestamp)
				}
			} else {
				// The snapshot value is in fractional seconds.
				// Re-derive it from the original time and compare within 1µs.
				wantSec := float64(tt.setTime.UnixNano()) / float64(time.Second)
				diff := math.Abs(snap.LastDBWriteTimestamp - wantSec)
				if diff > 1e-6 {
					t.Errorf("LastDBWriteTimestamp: got %v, want ~%v (diff %v)", snap.LastDBWriteTimestamp, wantSec, diff)
				}
			}
		})
	}
}

// TestSnapshot_Uptime verifies that the UptimeSeconds field in Snapshot grows
// monotonically. We construct the Metrics, wait a short interval, and assert
// that uptime is at least 1 second.
func TestSnapshot_Uptime(t *testing.T) {
	m := New()

	// Sleep so the uptime field has at least one full second to accumulate.
	time.Sleep(1 * time.Second)

	snap := m.Snapshot()

	if snap.UptimeSeconds < 1 {
		t.Errorf("UptimeSeconds: got %d, want >= 1", snap.UptimeSeconds)
	}
}

// TestSnapshot_AllCountersTogether exercises a scenario where all counters
// are updated before a single Snapshot call, verifying the values are
// collected atomically as a consistent group.
func TestSnapshot_AllCountersTogether(t *testing.T) {
	m := New()
	writeTime := time.Now()

	// Populate every counter.
	m.AddMessagesConsumed(100)
	m.IncDBWritesSuccess()
	m.IncDBWritesSuccess()
	m.IncDBWritesError()
	m.IncValidationError()
	m.IncValidationError()
	m.IncValidationError()
	m.SetLastDBWrite(writeTime)

	snap := m.Snapshot()

	if snap.MessagesConsumedTotal != 100 {
		t.Errorf("MessagesConsumedTotal: got %d, want 100", snap.MessagesConsumedTotal)
	}
	if snap.DBWritesSuccessTotal != 2 {
		t.Errorf("DBWritesSuccessTotal: got %d, want 2", snap.DBWritesSuccessTotal)
	}
	if snap.DBWritesErrorTotal != 1 {
		t.Errorf("DBWritesErrorTotal: got %d, want 1", snap.DBWritesErrorTotal)
	}
	if snap.ValidationErrorsTotal != 3 {
		t.Errorf("ValidationErrorsTotal: got %d, want 3", snap.ValidationErrorsTotal)
	}

	// Verify the timestamp conversion.
	wantSec := float64(writeTime.UnixNano()) / float64(time.Second)
	diff := math.Abs(snap.LastDBWriteTimestamp - wantSec)
	if diff > 1e-6 {
		t.Errorf("LastDBWriteTimestamp diff %v exceeds 1µs tolerance", diff)
	}
}
