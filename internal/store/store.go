package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dbPool is the subset of pgxpool.Pool methods used by Store. Defining it as
// an interface allows tests to substitute a lightweight in-memory mock without
// requiring a live Postgres connection.
type dbPool interface {
	// Exec executes a single SQL statement and discards any returned rows.
	// Used by Migrate() for DDL statements.
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)

	// SendBatch submits a batch of statements in a single round-trip and
	// returns a handle for iterating over the individual results.
	// Used by BulkInsert() for high-throughput write operations.
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults

	// Query executes a SQL query and returns a Rows handle for iteration.
	// Used by ListGPUs() and GetTelemetry() read operations.
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)

	// QueryRow executes a SQL query expected to return at most one row.
	// Used by GPUExists() for lightweight existence checks.
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row

	// Ping verifies that at least one connection to the server is alive.
	// Used at startup and by the readiness probe.
	Ping(ctx context.Context) error

	// Close drains and releases all connections in the pool.
	// Called during graceful shutdown.
	Close()
}

// Config holds the database connection parameters shared by both the Collector
// (write path) and the API Gateway (read path).
type Config struct {
	// DatabaseURL is the full Postgres DSN, e.g.
	// "postgres://user:pass@host:5432/telemetry?sslmode=disable".
	DatabaseURL string

	// DBMaxConns is the maximum number of open connections in the pool.
	// Choose a value that leaves headroom for other services connecting to
	// the same Postgres instance.
	DBMaxConns int

	// DBConnectTimeout is the deadline for the initial connection attempt
	// at startup. If Postgres is unreachable within this window the process
	// exits rather than hanging indefinitely.
	DBConnectTimeout time.Duration
}

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

// Store wraps a dbPool and exposes all database operations used across the
// pipeline: schema management (Migrate), bulk writes (BulkInsert), and read
// queries (ListGPUs, GPUExists, GetTelemetry).
type Store struct {
	// pool is the shared connection pool. In production this is a *pgxpool.Pool;
	// in tests it is a lightweight mock that satisfies the dbPool interface.
	pool dbPool
}

// New creates a Store by parsing the DSN, configuring the pool, and verifying
// connectivity with a lightweight ping. The ctx deadline controls how long the
// overall startup sequence is allowed to take.
//
// Returns an error if the DSN is invalid, the pool cannot be created, or the
// ping fails within the context deadline. In all error cases the pool is closed
// before returning so no resources are leaked.
func New(ctx context.Context, cfg Config) (*Store, error) {
	// Parse the DSN into a pgxpool.Config so we can override individual pool
	// parameters programmatically rather than embedding them in the URL.
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	// Override the maximum pool size from the service-level configuration.
	// The pgxpool field is int32, so we convert explicitly.
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

// Ping verifies that the database connection is still alive. It is called by
// the API Gateway's readiness probe handler on every GET /ready request.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close drains and closes all connections in the pool.
// It is called from each service's main function as part of the graceful
// shutdown sequence, after all in-flight operations have completed.
func (s *Store) Close() {
	s.pool.Close()
}
