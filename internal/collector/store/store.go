package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/config"
)

// dbPool is the subset of pgxpool.Pool methods used by Store. Defining it as
// an interface allows tests to substitute a lightweight in-memory mock without
// requiring a live Postgres connection.
type dbPool interface {
	// Exec executes a single SQL statement and discards any returned rows.
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	// SendBatch submits a batch of statements in a single round-trip and
	// returns a handle for iterating over the individual results.
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	// Ping verifies that at least one connection to the server is alive.
	Ping(ctx context.Context) error
	// Close drains and releases all connections in the pool.
	Close()
}

// createTableSQL is the idempotent DDL statement executed during auto-migration.
// PRIMARY KEY on (ts, hostname, gpu_id, metric_name) provides the composite
// uniqueness constraint that enables the ON CONFLICT DO NOTHING deduplication
// strategy, covering the at-least-once redelivery window.
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
);`

// insertSQL is the parameterised statement used for every row in a batch.
// ON CONFLICT DO NOTHING means a duplicate row (same ts/hostname/gpu_id/metric_name)
// is silently skipped, providing idempotent insert semantics without an error.
const insertSQL = `
INSERT INTO gpu_metrics
    (ts, hostname, gpu_id, metric_name, value, device, uuid, model_name, labels_raw, message_id)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (ts, hostname, gpu_id, metric_name) DO NOTHING`

// Row is a fully validated and type-converted telemetry record ready for SQL
// insertion. The consumer package converts raw queue messages into this type
// before handing them to the store, keeping the store free of validation logic.
type Row struct {
	// Ts is the parsed timestamp. Stored as TIMESTAMPTZ in Postgres.
	Ts time.Time
	// Hostname is the originating node name.
	Hostname string
	// GpuID is the ordinal GPU identifier on the host (e.g. "0", "1").
	GpuID string
	// MetricName is the DCGM metric identifier (e.g. "DCGM_FI_DEV_GPU_UTIL").
	MetricName string
	// Value is the parsed float64 reading.
	Value float64
	// Device is the OS-level device name (e.g. "nvidia0").
	Device string
	// UUID is the globally unique GPU hardware identifier.
	UUID string
	// ModelName is the GPU product name (e.g. "NVIDIA H100 80GB HBM3").
	ModelName string
	// LabelsRaw is the raw Prometheus-style label string from the CSV export.
	LabelsRaw string
	// MessageID is the identifier assigned by the message-queue at publish time.
	MessageID string
}

// Store wraps a dbPool and exposes the two database operations the
// collector needs: Migrate (schema creation) and BulkInsert (data persistence).
type Store struct {
	// pool is the shared connection pool. All database operations are issued
	// through it so connections are reused across batches. In production this
	// is a *pgxpool.Pool; in tests it is a lightweight mock.
	pool dbPool
}

// New creates a Store by parsing the DSN, configuring the pool, and verifying
// connectivity with a lightweight ping. The ctx deadline controls how long the
// initial connection attempt is allowed to take; callers should use
// cfg.DBConnectTimeout as the timeout.
//
// Returns an error if the DSN is invalid, the pool cannot be created, or the
// ping fails within the context deadline. In all error cases the pool is closed
// before returning so no resources are leaked.
func New(ctx context.Context, cfg config.CollectorConfig) (*Store, error) {
	// Parse the DSN into a pgxpool.Config so we can override individual pool
	// parameters programmatically rather than embedding them in the URL.
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	// Override the maximum pool size from service configuration. The pgxpool
	// field is int32, so we convert explicitly.
	poolCfg.MaxConns = int32(cfg.DBMaxConns) //nolint:gosec // config value is bounded

	// Apply the startup connect deadline so the process fails fast if Postgres
	// is unreachable at boot rather than hanging until the OS TCP timeout fires.
	connectCtx, cancel := context.WithTimeout(ctx, cfg.DBConnectTimeout)
	defer cancel()

	// Allocate the pool. This does NOT immediately open connections; it just
	// configures the pool object. The Ping below forces the first connection.
	pool, err := pgxpool.NewWithConfig(connectCtx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	// Verify that at least one connection can be established within the deadline.
	// This surfaces misconfigured credentials or network issues at startup rather
	// than silently at the first real database operation.
	if err := pool.Ping(connectCtx); err != nil {
		// Close the pool to release any partially-acquired resources before
		// propagating the error to the caller.
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Store{pool: pool}, nil
}

// Migrate runs the auto-migration DDL (CREATE TABLE IF NOT EXISTS).
// It is safe to call on every startup: if the table already exists,
// the statement is a no-op. Callers should invoke this once, during the
// startup sequence, before the consumption loop is started.
func (s *Store) Migrate(ctx context.Context) error {
	// Execute the DDL statement. pgxpool.Exec acquires a connection from the
	// pool, executes the statement, and returns the connection immediately.
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
// BulkInsert returns that error immediately and the caller should NOT send
// an ACK, allowing the queue to redeliver the entire batch.
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

// Close drains and closes all connections in the pool.
// It is called from main as part of the graceful shutdown sequence, after
// the consumption loop has stopped and the final ACKs have been sent.
func (s *Store) Close() {
	s.pool.Close()
}
