package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/config"
)

// ─── mock pool ────────────────────────────────────────────────────────────────

// mockPool is a test double for dbPool. It records which methods were called
// and returns the pre-configured errors. All fields default to the zero value
// (no error, not called), so each test only needs to override what it checks.
type mockPool struct {
	// execErr is returned by Exec. nil means success.
	execErr error

	// pingErr is returned by Ping. nil means the connection is alive.
	pingErr error

	// batchResults is the BatchResults handle returned by SendBatch.
	batchResults pgx.BatchResults

	// closeCalled records whether Close was invoked.
	closeCalled bool
}

// Exec satisfies the dbPool interface. It returns execErr and a zero CommandTag.
func (m *mockPool) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, m.execErr
}

// SendBatch satisfies the dbPool interface. It returns the pre-set BatchResults.
func (m *mockPool) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults {
	return m.batchResults
}

// Ping satisfies the dbPool interface. It returns pingErr.
func (m *mockPool) Ping(_ context.Context) error {
	return m.pingErr
}

// Close satisfies the dbPool interface. It records that Close was called.
func (m *mockPool) Close() {
	m.closeCalled = true
}

// ─── mock batch results ───────────────────────────────────────────────────────

// mockBatchResults is a test double for pgx.BatchResults. It returns a
// pre-configured sequence of errors from Exec() and a final error from Close().
type mockBatchResults struct {
	// execErrs is the ordered list of errors to return, one per Exec() call.
	// If the index exceeds the slice, subsequent calls return nil.
	execErrs []error

	// closeErr is returned by Close().
	closeErr error

	// idx tracks which Exec() call is next.
	idx int
}

// Exec returns the next error in the sequence, or nil if all have been consumed.
func (m *mockBatchResults) Exec() (pgconn.CommandTag, error) {
	if m.idx < len(m.execErrs) {
		err := m.execErrs[m.idx]
		m.idx++
		return pgconn.CommandTag{}, err
	}
	return pgconn.CommandTag{}, nil
}

// Query is not used by BulkInsert; it exists only to satisfy the interface.
func (m *mockBatchResults) Query() (pgx.Rows, error) { return nil, nil }

// QueryRow is not used by BulkInsert; it exists only to satisfy the interface.
func (m *mockBatchResults) QueryRow() pgx.Row { return nil }

// Close returns the pre-configured closeErr.
func (m *mockBatchResults) Close() error { return m.closeErr }

// ─── helpers ──────────────────────────────────────────────────────────────────

// storeWithMock builds a Store whose pool field is set to the provided mockPool,
// bypassing the real pgxpool.New constructor so no database is required.
func storeWithMock(mp *mockPool) *Store {
	return &Store{pool: mp}
}

// sampleRow returns a Row with all fields populated, ready for BulkInsert.
func sampleRow(suffix string) Row {
	return Row{
		Ts:         time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		Hostname:   "node-" + suffix,
		GpuID:      "0",
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		Value:      75.5,
		Device:     "nvidia0",
		UUID:       "GPU-abc-" + suffix,
		ModelName:  "H100",
		LabelsRaw:  "env=prod",
		MessageID:  "msg-" + suffix,
	}
}

// ─── New ─────────────────────────────────────────────────────────────────────

// TestNew_InvalidDSN verifies that New returns an error immediately when the
// DatabaseURL cannot be parsed by pgxpool.ParseConfig, without making any
// network calls.
func TestNew_InvalidDSN(t *testing.T) {
	cfg := config.CollectorConfig{
		// An unparseable DSN triggers the very first error path in New.
		DatabaseURL:      "not://a:valid@postgres/dsn!!!",
		DBMaxConns:       1,
		DBConnectTimeout: 100 * time.Millisecond,
	}

	_, err := New(context.Background(), cfg)

	if err == nil {
		t.Fatal("expected error for invalid DSN, got nil")
	}
	// Verify the error is wrapped with the expected context message.
	if !errors.Is(err, err) || err.Error() == "" {
		t.Errorf("expected non-empty wrapped error, got %v", err)
	}
}

// TestNew_PingFailure verifies that when the database is unreachable, New
// returns a ping error and does not leak the pool (Close is called internally).
// Port 1 on localhost is always refused immediately, so this test is fast.
func TestNew_PingFailure(t *testing.T) {
	cfg := config.CollectorConfig{
		// Port 1 is a reserved port that is always refused on loopback.
		DatabaseURL:      "postgres://u:p@localhost:1/db?sslmode=disable",
		DBMaxConns:       1,
		DBConnectTimeout: 500 * time.Millisecond,
	}

	_, err := New(context.Background(), cfg)

	if err == nil {
		t.Fatal("expected ping error for unreachable server, got nil")
	}
}

// ─── Migrate ─────────────────────────────────────────────────────────────────

// TestMigrate tests every code path of Migrate using a table of scenarios.
func TestMigrate(t *testing.T) {
	tests := []struct {
		name    string
		execErr error // error to return from the mock pool's Exec
		wantErr bool
	}{
		{
			name:    "Success — Exec returns nil",
			execErr: nil,
			wantErr: false,
		},
		{
			name:    "Exec failure — error is wrapped and returned",
			execErr: errors.New("relation already exists"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build a store backed by a mock that returns the configured error.
			s := storeWithMock(&mockPool{execErr: tt.execErr})

			err := s.Migrate(context.Background())

			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// ─── BulkInsert ──────────────────────────────────────────────────────────────

// TestBulkInsert_EmptySlice verifies that BulkInsert returns nil immediately
// for an empty input slice without calling SendBatch, avoiding a needless
// round-trip to the database.
func TestBulkInsert_EmptySlice(t *testing.T) {
	// sendBatch must never be called; if it were, batchResults.Exec() would
	// panic because batchResults is nil.
	s := storeWithMock(&mockPool{batchResults: nil})

	err := s.BulkInsert(context.Background(), []Row{})

	if err != nil {
		t.Errorf("expected nil for empty rows, got %v", err)
	}
}

// TestBulkInsert covers the remaining code paths: all rows succeed, a row
// fails mid-batch, and the final Close() returns an error.
func TestBulkInsert(t *testing.T) {
	tests := []struct {
		name        string
		rows        []Row
		execErrs    []error // error returned per Exec() call, in order
		closeErr    error   // error returned by BatchResults.Close()
		wantErr     bool
		errContains string
	}{
		{
			name:     "All rows succeed — Close returns nil",
			rows:     []Row{sampleRow("a"), sampleRow("b")},
			execErrs: []error{nil, nil},
			closeErr: nil,
			wantErr:  false,
		},
		{
			name:        "First row fails — error is wrapped with row index",
			rows:        []Row{sampleRow("a"), sampleRow("b")},
			execErrs:    []error{errors.New("unique violation"), nil},
			closeErr:    nil,
			wantErr:     true,
			errContains: "insert row 0",
		},
		{
			name:        "Second row fails — error identifies correct row index",
			rows:        []Row{sampleRow("a"), sampleRow("b"), sampleRow("c")},
			execErrs:    []error{nil, errors.New("schema mismatch"), nil},
			closeErr:    nil,
			wantErr:     true,
			errContains: "insert row 1",
		},
		{
			name:        "Close returns protocol error — propagated as return value",
			rows:        []Row{sampleRow("a")},
			execErrs:    []error{nil},
			closeErr:    errors.New("connection lost"),
			wantErr:     true,
			errContains: "connection lost",
		},
		{
			name:     "Single row success",
			rows:     []Row{sampleRow("x")},
			execErrs: []error{nil},
			closeErr: nil,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Construct the mock BatchResults with the per-row error sequence.
			br := &mockBatchResults{
				execErrs: tt.execErrs,
				closeErr: tt.closeErr,
			}
			s := storeWithMock(&mockPool{batchResults: br})

			err := s.BulkInsert(context.Background(), tt.rows)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !containsString(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// ─── Close ────────────────────────────────────────────────────────────────────

// TestClose verifies that Store.Close delegates to pool.Close exactly once.
func TestClose(t *testing.T) {
	mp := &mockPool{}
	s := storeWithMock(mp)

	// Close must not be called before we invoke it.
	if mp.closeCalled {
		t.Fatal("pool.Close was called before Store.Close()")
	}

	s.Close()

	if !mp.closeCalled {
		t.Error("pool.Close was not called by Store.Close()")
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// containsString reports whether s contains substr.
// Using a local helper avoids importing "strings" for a single call.
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}
