package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ─── shared mock pool ─────────────────────────────────────────────────────────

// mockPool is a test double for dbPool that records calls and returns
// pre-configured results. Tests only override the fields they care about.
type mockPool struct {
	// execErr is returned by Exec (used by Migrate).
	execErr error

	// pingErr is returned by Ping.
	pingErr error

	// batchResults is returned by SendBatch (used by BulkInsert).
	batchResults pgx.BatchResults

	// queryRows is returned by Query (used by ListGPUs, GetTelemetry).
	queryRows pgx.Rows

	// queryErr is the error returned by Query before returning rows.
	queryErr error

	// queryRowResult is the pgx.Row returned by QueryRow (used by GPUExists).
	queryRowResult pgx.Row

	// closeCalled records whether Close was invoked.
	closeCalled bool
}

// Exec satisfies dbPool. Returns execErr and a zero CommandTag.
func (m *mockPool) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, m.execErr
}

// SendBatch satisfies dbPool. Returns batchResults.
func (m *mockPool) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults {
	return m.batchResults
}

// Query satisfies dbPool. Returns queryErr or queryRows.
func (m *mockPool) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return m.queryRows, nil
}

// QueryRow satisfies dbPool. Returns queryRowResult.
func (m *mockPool) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return m.queryRowResult
}

// Ping satisfies dbPool. Returns pingErr.
func (m *mockPool) Ping(_ context.Context) error {
	return m.pingErr
}

// Close satisfies dbPool. Records that it was called.
func (m *mockPool) Close() {
	m.closeCalled = true
}

// ─── shared mock batch results ────────────────────────────────────────────────

// mockBatchResults is a test double for pgx.BatchResults returned by SendBatch.
type mockBatchResults struct {
	// execErrs is the sequence of errors returned by successive Exec() calls.
	execErrs []error
	// closeErr is returned by Close().
	closeErr error
	// idx tracks the next Exec() position.
	idx int
}

func (m *mockBatchResults) Exec() (pgconn.CommandTag, error) {
	if m.idx < len(m.execErrs) {
		err := m.execErrs[m.idx]
		m.idx++
		return pgconn.CommandTag{}, err
	}
	return pgconn.CommandTag{}, nil
}

func (m *mockBatchResults) Query() (pgx.Rows, error) { return nil, nil }
func (m *mockBatchResults) QueryRow() pgx.Row        { return nil }
func (m *mockBatchResults) Close() error             { return m.closeErr }

// ─── helpers ──────────────────────────────────────────────────────────────────

// storeWithPool builds a Store backed by the given mock, bypassing the real
// pgxpool.New constructor so no database connection is needed.
func storeWithPool(mp *mockPool) *Store {
	return &Store{pool: mp}
}

// sampleRow returns a fully-populated Row for use in BulkInsert tests.
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

// TestNew_InvalidDSN verifies that New returns an error when the DSN cannot
// be parsed, without making any network calls.
func TestNew_InvalidDSN(t *testing.T) {
	cfg := Config{
		DatabaseURL:      "not://a:valid@postgres/dsn!!!",
		DBMaxConns:       5,
		DBConnectTimeout: 100 * time.Millisecond,
	}
	_, err := New(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for invalid DSN, got nil")
	}
}

// ─── Ping ─────────────────────────────────────────────────────────────────────

// TestPing verifies that Ping delegates to pool.Ping and propagates its error.
func TestPing(t *testing.T) {
	pingErr := errors.New("db unreachable")
	tests := []struct {
		name    string
		pingErr error
		wantErr bool
	}{
		{name: "success", pingErr: nil, wantErr: false},
		{name: "error", pingErr: pingErr, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange: build a store whose pool.Ping returns the configured error.
			s := storeWithPool(&mockPool{pingErr: tc.pingErr})

			// Act: call Ping.
			err := s.Ping(context.Background())

			// Assert: error presence matches expectation.
			if (err != nil) != tc.wantErr {
				t.Errorf("Ping() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// ─── Close ────────────────────────────────────────────────────────────────────

// TestClose verifies that Close delegates to pool.Close.
func TestClose(t *testing.T) {
	// Arrange: pool with Close tracking.
	mp := &mockPool{}
	s := storeWithPool(mp)

	// Act: call Close.
	s.Close()

	// Assert: pool.Close was invoked.
	if !mp.closeCalled {
		t.Error("expected pool.Close to be called")
	}
}
