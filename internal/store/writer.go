package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// createTableSQL is the idempotent DDL that creates the gpu_metrics table and
// the secondary index required for efficient API Gateway queries.
//
// Primary Key (ts, hostname, gpu_id, metric_name):
//   - Provides the composite uniqueness constraint for ON CONFLICT deduplication.
//   - Optimised for the Collector's write path which deduplicates by time + card +
//     metric during at-least-once redelivery.
//
// Secondary Index idx_gpu_metrics_uuid_ts (uuid, ts DESC):
//   - Allows the API Gateway's telemetry query (WHERE uuid = $1 AND ts BETWEEN …)
//     to use an index scan instead of a sequential scan, even on a large table.
//   - ts DESC places the most recently ingested data at the leaf level so the
//     most common "last N seconds" queries read the fewest pages.
const createTableSQL = `
CREATE TABLE IF NOT EXISTS gpu_metrics (
    ts          TIMESTAMPTZ      NOT NULL,
    hostname    TEXT             NOT NULL,
    gpu_id      TEXT             NOT NULL,
    metric_name TEXT             NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    device      TEXT             NOT NULL DEFAULT '',
    uuid        TEXT             NOT NULL DEFAULT '',
    model_name  TEXT             NOT NULL DEFAULT '',
    labels_raw  TEXT             NOT NULL DEFAULT '',
    message_id  TEXT             NOT NULL DEFAULT '',
    PRIMARY KEY (ts, hostname, gpu_id, metric_name)
);
CREATE INDEX IF NOT EXISTS idx_gpu_metrics_uuid_ts
    ON gpu_metrics (uuid, ts DESC);`

// insertSQL is the parameterised statement used for every row in a batch.
// ON CONFLICT DO NOTHING means a duplicate row (same primary key) is silently
// skipped, providing idempotent insert semantics without returning an error.
const insertSQL = `
INSERT INTO gpu_metrics
    (ts, hostname, gpu_id, metric_name, value, device, uuid, model_name, labels_raw, message_id)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (ts, hostname, gpu_id, metric_name) DO NOTHING`

// Migrate runs the auto-migration DDL: CREATE TABLE IF NOT EXISTS and the
// companion secondary index. It is safe to call on every startup — both
// statements are idempotent. Callers should invoke Migrate once during startup,
// before any read or write operations are performed.
func (s *Store) Migrate(ctx context.Context) error {
	// Execute the combined DDL statement. pgxpool.Exec acquires a connection
	// from the pool, executes the statement, and returns the connection
	// immediately without requiring an explicit transaction.
	if _, err := s.pool.Exec(ctx, createTableSQL); err != nil {
		return fmt.Errorf("run migration: %w", err)
	}
	return nil
}

// BulkInsert persists a slice of validated rows to Postgres using a single
// pgx.Batch, which sends all INSERT statements in one TCP round-trip via the
// PostgreSQL extended query protocol. This is equivalent to a bulk INSERT in
// terms of network efficiency while retaining per-row ON CONFLICT semantics.
//
// If any individual INSERT fails with a hard error (e.g. schema mismatch),
// BulkInsert returns that error immediately and the caller should NOT send an
// ACK, allowing the queue to redeliver the entire batch.
//
// Duplicate rows (same primary key) are silently skipped by ON CONFLICT DO NOTHING
// and do not constitute an error.
//
// Returns nil if all rows were processed (inserted or deduplicated) without error.
func (s *Store) BulkInsert(ctx context.Context, rows []Row) error {
	// Nothing to do for an empty batch. Return early to avoid sending an
	// empty pgx.Batch which would still incur a round-trip.
	if len(rows) == 0 {
		return nil
	}

	// Queue one INSERT statement per row into the batch object.
	// The batch does not execute until SendBatch is called below.
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(insertSQL,
			r.Ts, r.Hostname, r.GpuID, r.MetricName,
			r.Value, r.Device, r.UUID, r.ModelName,
			r.LabelsRaw, r.MessageID,
		)
	}

	// Send the entire batch to Postgres in a single round-trip and obtain
	// a results handle to iterate over the individual outcomes.
	results := s.pool.SendBatch(ctx, batch)

	// Iterate over each result in the same order as the queued statements.
	// Exec() blocks until Postgres returns the result for each row.
	for i := range rows {
		if _, err := results.Exec(); err != nil {
			// Close the results pipeline before returning to avoid leaking
			// the connection back to the pool in an undefined state.
			results.Close()
			return fmt.Errorf("insert row %d: %w", i, err)
		}
	}

	// Close flushes the pipeline and returns any final protocol-level error.
	// Under normal operation this is nil; a non-nil value indicates a
	// connection-level failure rather than a per-row constraint violation.
	return results.Close()
}
