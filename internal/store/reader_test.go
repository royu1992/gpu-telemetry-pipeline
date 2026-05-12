package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ─── mock rows ────────────────────────────────────────────────────────────────

// mockRows implements pgx.Rows for testing ListGPUs and GetTelemetry.
// It replays a pre-loaded slice of row data and returns a configured scan error.
type mockRows struct {
	// data holds the values for each row. Each inner slice maps to one
	// positional column, matching the SELECT column order in the SQL query.
	data [][]any
	// idx is the current position in data (advanced by each Next() call).
	idx int
	// iterErr is returned by Err() after iteration completes.
	iterErr error
	// scanErr is returned by Scan() on every call when non-nil.
	scanErr error
}

// Close is a no-op; the mock holds no real resources.
func (m *mockRows) Close() {}

// Err returns the pre-configured iteration error.
func (m *mockRows) Err() error { return m.iterErr }

// CommandTag returns a zero value; not used by the reader.
func (m *mockRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

// FieldDescriptions returns nil; not used by the reader.
func (m *mockRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

// Next advances the cursor. Returns true while rows remain.
func (m *mockRows) Next() bool {
	return m.idx < len(m.data)
}

// Scan copies values from the current row into dest using reflection, then
// advances the cursor. Returns scanErr immediately if it is set.
func (m *mockRows) Scan(dest ...any) error {
	// If a scan error is configured, return it without advancing.
	if m.scanErr != nil {
		m.idx++
		return m.scanErr
	}
	// Copy each column value into the corresponding destination pointer.
	row := m.data[m.idx]
	m.idx++
	for i, dp := range dest {
		if i >= len(row) {
			break
		}
		// Use reflection to set the value through the pointer.
		rv := reflect.ValueOf(dp)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() {
			rv.Elem().Set(reflect.ValueOf(row[i]))
		}
	}
	return nil
}

// Values is not used by the reader; returns nil.
func (m *mockRows) Values() ([]any, error) { return nil, nil }

// RawValues is not used by the reader; returns nil.
func (m *mockRows) RawValues() [][]byte { return nil }

// Conn is not used by the reader; returns nil.
func (m *mockRows) Conn() *pgx.Conn { return nil }

// ─── mock row ─────────────────────────────────────────────────────────────────

// mockRow implements pgx.Row for testing GPUExists.
type mockRow struct {
	// values holds the single row's column values.
	values []any
	// scanErr is returned by Scan when non-nil (simulates pgx.ErrNoRows or
	// other database errors).
	scanErr error
}

// Scan copies values into dest or returns scanErr.
func (m *mockRow) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	for i, dp := range dest {
		if i >= len(m.values) {
			break
		}
		rv := reflect.ValueOf(dp)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() {
			rv.Elem().Set(reflect.ValueOf(m.values[i]))
		}
	}
	return nil
}

// ─── ListGPUs ─────────────────────────────────────────────────────────────────

// TestListGPUs exercises every code path in Store.ListGPUs.
func TestListGPUs(t *testing.T) {
	ts := time.Date(2025, 7, 18, 20, 42, 34, 0, time.UTC)
	_ = ts // unused in ListGPUs but kept for clarity

	tests := []struct {
		name      string
		queryErr  error
		rows      *mockRows
		wantLen   int
		wantFirst GPUSummary
		wantErr   bool
	}{
		{
			name:     "success — two GPUs returned",
			queryErr: nil,
			rows: &mockRows{
				data: [][]any{
					{"GPU-aaa", "node-1", "0", "NVIDIA H100"},
					{"GPU-bbb", "node-1", "1", "NVIDIA H100"},
				},
			},
			wantLen:   2,
			wantFirst: GPUSummary{ID: "GPU-aaa", Hostname: "node-1", GpuID: "0", ModelName: "NVIDIA H100"},
			wantErr:   false,
		},
		{
			name:     "success — empty table returns empty slice",
			queryErr: nil,
			rows:     &mockRows{data: [][]any{}},
			wantLen:  0,
			wantErr:  false,
		},
		{
			name:     "query error — pool.Query fails",
			queryErr: errors.New("connection refused"),
			rows:     nil,
			wantErr:  true,
		},
		{
			name:    "scan error — rows.Scan returns error",
			rows:    &mockRows{data: [][]any{{"GPU-aaa", "node-1", "0", "H100"}}, scanErr: errors.New("scan failed")},
			wantErr: true,
		},
		{
			name:    "iteration error — rows.Err returns error",
			rows:    &mockRows{data: [][]any{}, iterErr: errors.New("network reset")},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange: build a store with the configured query response.
			mp := &mockPool{queryErr: tc.queryErr, queryRows: tc.rows}
			s := storeWithPool(mp)

			// Act: call ListGPUs.
			got, err := s.ListGPUs(context.Background())

			// Assert: error presence.
			if (err != nil) != tc.wantErr {
				t.Fatalf("ListGPUs() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}

			// Assert: result count.
			if len(got) != tc.wantLen {
				t.Errorf("len(got) = %d, want %d", len(got), tc.wantLen)
			}

			// Assert: first element content (when rows are present).
			if tc.wantLen > 0 && got[0] != tc.wantFirst {
				t.Errorf("got[0] = %+v, want %+v", got[0], tc.wantFirst)
			}
		})
	}
}

// ─── GPUExists ────────────────────────────────────────────────────────────────

// TestGPUExists exercises every code path in Store.GPUExists.
func TestGPUExists(t *testing.T) {
	dbErr := errors.New("db error")

	tests := []struct {
		name       string
		scanErr    error
		scanValues []any
		want       bool
		wantErr    bool
	}{
		{
			name:       "exists — row found",
			scanValues: []any{1},
			want:       true,
			wantErr:    false,
		},
		{
			name:    "not found — pgx.ErrNoRows",
			scanErr: pgx.ErrNoRows,
			want:    false,
			wantErr: false,
		},
		{
			name:    "error — unexpected DB error",
			scanErr: dbErr,
			want:    false,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange: build a store whose QueryRow returns the configured row mock.
			row := &mockRow{scanErr: tc.scanErr, values: tc.scanValues}
			s := storeWithPool(&mockPool{queryRowResult: row})

			// Act: call GPUExists.
			got, err := s.GPUExists(context.Background(), "GPU-test")

			// Assert: error presence.
			if (err != nil) != tc.wantErr {
				t.Fatalf("GPUExists() error = %v, wantErr = %v", err, tc.wantErr)
			}

			// Assert: boolean result.
			if got != tc.want {
				t.Errorf("GPUExists() = %v, want %v", got, tc.want)
			}
		})
	}
}

// ─── GetTelemetry ─────────────────────────────────────────────────────────────

// TestGetTelemetry exercises every code path in Store.GetTelemetry.
func TestGetTelemetry(t *testing.T) {
	ts1 := time.Date(2025, 7, 18, 20, 42, 34, 0, time.UTC)
	ts2 := time.Date(2025, 7, 18, 20, 42, 35, 0, time.UTC)
	filter := TelemetryFilter{
		StartTime: ts1,
		EndTime:   ts2,
		Limit:     100,
	}

	tests := []struct {
		name     string
		queryErr error
		rows     *mockRows
		wantLen  int
		wantErr  bool
	}{
		{
			name:     "success — two entries returned",
			queryErr: nil,
			rows: &mockRows{
				data: [][]any{
					{ts1, "DCGM_FI_DEV_GPU_UTIL", 98.5, "node-1", "0", "NVIDIA H100"},
					{ts2, "DCGM_FI_DEV_GPU_TEMP", 72.0, "node-1", "0", "NVIDIA H100"},
				},
			},
			wantLen: 2,
			wantErr: false,
		},
		{
			name:     "success — empty result for valid uuid outside time range",
			queryErr: nil,
			rows:     &mockRows{data: [][]any{}},
			wantLen:  0,
			wantErr:  false,
		},
		{
			name:     "query error — pool.Query fails",
			queryErr: errors.New("connection refused"),
			rows:     nil,
			wantErr:  true,
		},
		{
			name: "scan error — rows.Scan returns error",
			rows: &mockRows{
				data:    [][]any{{ts1, "DCGM_FI_DEV_GPU_UTIL", 98.5, "node-1", "0", "NVIDIA H100"}},
				scanErr: errors.New("type mismatch"),
			},
			wantErr: true,
		},
		{
			name:    "iteration error — rows.Err returns error",
			rows:    &mockRows{data: [][]any{}, iterErr: errors.New("stream reset")},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange: build a store with the configured query response.
			mp := &mockPool{queryErr: tc.queryErr, queryRows: tc.rows}
			s := storeWithPool(mp)

			// Act: call GetTelemetry.
			got, err := s.GetTelemetry(context.Background(), "GPU-test", filter)

			// Assert: error presence.
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetTelemetry() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}

			// Assert: result count.
			if len(got) != tc.wantLen {
				t.Errorf("len(got) = %d, want %d", len(got), tc.wantLen)
			}
		})
	}
}
