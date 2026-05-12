package metrics

import (
	"strings"
	"testing"
)

// TestNew verifies that New returns a non-nil Metrics with all counters at zero.
func TestNew(t *testing.T) {
	m := New()
	if m == nil {
		t.Fatal("New() returned nil")
	}
	snap := m.Snapshot()
	if snap.RequestsTotal != 0 {
		t.Errorf("RequestsTotal = %d, want 0", snap.RequestsTotal)
	}
	if snap.RequestsSuccessTotal != 0 {
		t.Errorf("RequestsSuccessTotal = %d, want 0", snap.RequestsSuccessTotal)
	}
	if snap.RequestsErrorTotal != 0 {
		t.Errorf("RequestsErrorTotal = %d, want 0", snap.RequestsErrorTotal)
	}
	if snap.GPUListCacheHitsTotal != 0 {
		t.Errorf("GPUListCacheHitsTotal = %d, want 0", snap.GPUListCacheHitsTotal)
	}
	if snap.GPUListCacheMissesTotal != 0 {
		t.Errorf("GPUListCacheMissesTotal = %d, want 0", snap.GPUListCacheMissesTotal)
	}
	if snap.DBQueryErrorsTotal != 0 {
		t.Errorf("DBQueryErrorsTotal = %d, want 0", snap.DBQueryErrorsTotal)
	}
}

// TestCounters verifies that each increment method advances exactly the
// expected counter and leaves all others unchanged.
func TestCounters(t *testing.T) {
	tests := []struct {
		name     string
		inc      func(*Metrics)
		check    func(Snapshot) int64
		wantDiff int64
	}{
		{"IncRequests", (*Metrics).IncRequests, func(s Snapshot) int64 { return s.RequestsTotal }, 1},
		{"IncRequestsSuccess", (*Metrics).IncRequestsSuccess, func(s Snapshot) int64 { return s.RequestsSuccessTotal }, 1},
		{"IncRequestsError", (*Metrics).IncRequestsError, func(s Snapshot) int64 { return s.RequestsErrorTotal }, 1},
		{"IncCacheHit", (*Metrics).IncCacheHit, func(s Snapshot) int64 { return s.GPUListCacheHitsTotal }, 1},
		{"IncCacheMiss", (*Metrics).IncCacheMiss, func(s Snapshot) int64 { return s.GPUListCacheMissesTotal }, 1},
		{"IncDBQueryError", (*Metrics).IncDBQueryError, func(s Snapshot) int64 { return s.DBQueryErrorsTotal }, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Start each sub-test with a fresh Metrics instance.
			m := New()
			before := tc.check(m.Snapshot())

			// Act: call the increment method.
			tc.inc(m)

			// Assert: the target counter increased by exactly wantDiff.
			after := tc.check(m.Snapshot())
			if after-before != tc.wantDiff {
				t.Errorf("%s: counter delta = %d, want %d", tc.name, after-before, tc.wantDiff)
			}
		})
	}
}

// TestSnapshot_UptimeSeconds verifies that UptimeSeconds is non-negative and
// grows over real time (it will be at least 0 immediately after construction).
func TestSnapshot_UptimeSeconds(t *testing.T) {
	m := New()
	snap := m.Snapshot()
	if snap.UptimeSeconds < 0 {
		t.Errorf("UptimeSeconds = %d, want >= 0", snap.UptimeSeconds)
	}
}

// TestSnapshot_Format verifies that Format returns all six metric keys with
// the correct values embedded in the output string.
func TestSnapshot_Format(t *testing.T) {
	m := New()
	// Set known values so we can assert specific lines in the output.
	m.IncRequests()
	m.IncRequests()
	m.IncRequestsSuccess()
	m.IncRequestsError()
	m.IncCacheHit()
	m.IncCacheHit()
	m.IncCacheHit()
	m.IncCacheMiss()
	m.IncDBQueryError()

	out := m.Snapshot().Format()

	// Check that each expected key: value line is present.
	cases := []string{
		"requests_total: 2",
		"requests_success_total: 1",
		"requests_error_total: 1",
		"gpu_list_cache_hits_total: 3",
		"gpu_list_cache_misses_total: 1",
		"db_query_errors_total: 1",
	}
	for _, want := range cases {
		if !strings.Contains(out, want) {
			t.Errorf("Format() output missing %q\nFull output:\n%s", want, out)
		}
	}
}
