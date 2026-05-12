package store

import (
	"context"
	"errors"
	"testing"
)

// ─── Migrate ─────────────────────────────────────────────────────────────────

// TestMigrate exercises every branch of the Migrate function.
func TestMigrate(t *testing.T) {
	execErr := errors.New("exec failed")
	tests := []struct {
		name    string
		execErr error
		wantErr bool
	}{
		{
			name:    "success — pool.Exec returns nil",
			execErr: nil,
			wantErr: false,
		},
		{
			name:    "failure — pool.Exec returns error",
			execErr: execErr,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange: build a store whose Exec returns the configured error.
			s := storeWithPool(&mockPool{execErr: tc.execErr})

			// Act: run the migration.
			err := s.Migrate(context.Background())

			// Assert: error presence matches expectation.
			if (err != nil) != tc.wantErr {
				t.Errorf("Migrate() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// ─── BulkInsert ──────────────────────────────────────────────────────────────

// TestBulkInsert_Empty verifies that an empty slice is a no-op and returns nil,
// without making any network calls.
func TestBulkInsert_Empty(t *testing.T) {
	// Arrange: pool with batchResults set to nil — if SendBatch were called
	// a nil-deref panic would surface the mistake.
	s := storeWithPool(&mockPool{})

	// Act: insert an empty slice.
	err := s.BulkInsert(context.Background(), nil)

	// Assert: no error for an empty batch.
	if err != nil {
		t.Errorf("BulkInsert(nil) unexpected error: %v", err)
	}
}

// TestBulkInsert_Success verifies that a normal batch of rows is inserted
// without error when all batch result Exec() calls succeed.
func TestBulkInsert_Success(t *testing.T) {
	// Arrange: two rows and a batch results that returns nil for each Exec.
	rows := []Row{sampleRow("a"), sampleRow("b")}
	s := storeWithPool(&mockPool{
		batchResults: &mockBatchResults{
			// Two nil errors — one per row.
			execErrs: []error{nil, nil},
		},
	})

	// Act: bulk insert the rows.
	err := s.BulkInsert(context.Background(), rows)

	// Assert: no error.
	if err != nil {
		t.Errorf("BulkInsert() unexpected error: %v", err)
	}
}

// TestBulkInsert_ExecError verifies that BulkInsert returns an error and
// closes the batch results when an individual row's Exec fails.
func TestBulkInsert_ExecError(t *testing.T) {
	// Arrange: first row succeeds, second fails.
	insertErr := errors.New("constraint violation")
	rows := []Row{sampleRow("a"), sampleRow("b")}
	s := storeWithPool(&mockPool{
		batchResults: &mockBatchResults{
			execErrs: []error{nil, insertErr},
		},
	})

	// Act: bulk insert the rows.
	err := s.BulkInsert(context.Background(), rows)

	// Assert: error is propagated.
	if err == nil {
		t.Fatal("expected error from BulkInsert, got nil")
	}
	if !errors.Is(err, insertErr) {
		t.Errorf("expected wrapped insertErr, got: %v", err)
	}
}

// TestBulkInsert_CloseError verifies that BulkInsert propagates an error
// returned by BatchResults.Close(), which indicates a connection-level failure
// after all individual rows have been processed successfully.
func TestBulkInsert_CloseError(t *testing.T) {
	// Arrange: one row, no per-row error, but Close returns an error.
	closeErr := errors.New("connection lost")
	rows := []Row{sampleRow("a")}
	s := storeWithPool(&mockPool{
		batchResults: &mockBatchResults{
			execErrs: []error{nil},
			closeErr: closeErr,
		},
	})

	// Act: bulk insert the rows.
	err := s.BulkInsert(context.Background(), rows)

	// Assert: Close error is propagated.
	if err == nil {
		t.Fatal("expected Close error, got nil")
	}
	if !errors.Is(err, closeErr) {
		t.Errorf("expected wrapped closeErr, got: %v", err)
	}
}
