package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// SQL statements used by the read path. All are parameterised to prevent SQL
// injection; never use string formatting to build WHERE clauses.

// listGPUsSQL returns one row per distinct GPU in the database, ordered by
// hostname then gpu_id for a stable, human-readable listing. The query relies
// on the idx_gpu_metrics_uuid_ts index to avoid a full-table scan.
const listGPUsSQL = `
SELECT DISTINCT uuid, hostname, gpu_id, model_name
FROM gpu_metrics
ORDER BY hostname, gpu_id`

// gpuExistsSQL is a lightweight existence probe that returns a single integer
// if the given uuid is present. Using LIMIT 1 lets Postgres short-circuit the
// scan on the first matching index entry.
const gpuExistsSQL = `
SELECT 1 FROM gpu_metrics WHERE uuid = $1 LIMIT 1`

// getTelemetrySQL returns time-series rows for a specific GPU UUID within
// the requested time window, ordered by timestamp ascending. The idx_gpu_metrics_uuid_ts
// index (uuid, ts DESC) allows Postgres to use an index range scan for both the
// uuid equality predicate and the ts BETWEEN range, then scan backwards for ASC order.
const getTelemetrySQL = `
SELECT ts, metric_name, value, hostname, gpu_id, model_name
FROM gpu_metrics
WHERE uuid = $1
  AND ts >= $2
  AND ts <= $3
ORDER BY ts ASC
LIMIT $4`

// GPUSummary is the API-level representation of a distinct GPU returned by
// ListGPUs. The ID field is the hardware uuid — globally unique across the
// cluster and usable directly as the {id} path parameter for telemetry queries.
type GPUSummary struct {
	// ID is the hardware UUID (e.g. "GPU-5fd4f087-86f3-7a43-b711-4771313afc50").
	// It is the value that callers must supply to GET /gpus/{id}/telemetry.
	ID string `json:"id"`
	// Hostname is the originating node (e.g. "mtv5-dgx1-hgpu-031").
	Hostname string `json:"hostname"`
	// GpuID is the ordinal index on that host (e.g. "0", "1").
	GpuID string `json:"gpu_id"`
	// ModelName is the GPU product name (e.g. "NVIDIA H100 80GB HBM3").
	ModelName string `json:"model_name"`
}

// TelemetryEntry is one time-series data point returned by GetTelemetry.
// It represents a single metric reading for a specific GPU at a specific time.
type TelemetryEntry struct {
	// Timestamp is when the metric was recorded (RFC3339 in JSON output).
	Timestamp time.Time `json:"timestamp"`
	// MetricName is the DCGM metric identifier (e.g. "DCGM_FI_DEV_GPU_UTIL").
	MetricName string `json:"metric_name"`
	// Value is the numeric reading.
	Value float64 `json:"value"`
	// Hostname identifies which node the GPU belongs to.
	Hostname string `json:"hostname"`
	// GpuID is the ordinal on the host.
	GpuID string `json:"gpu_id"`
	// ModelName is the GPU product name.
	ModelName string `json:"model_name"`
}

// TelemetryFilter holds the time-window boundaries and the hard row cap for
// a GetTelemetry query. All three fields must be set before passing to GetTelemetry.
type TelemetryFilter struct {
	// StartTime is the inclusive lower bound of the time window.
	StartTime time.Time
	// EndTime is the inclusive upper bound of the time window.
	EndTime time.Time
	// Limit is the maximum number of rows to return. The gateway enforces a
	// service-level cap (default 1000) to prevent OOM on large time ranges.
	Limit int
}

// ListGPUs returns the distinct set of GPUs for which telemetry is stored,
// ordered by hostname then gpu_id. The result is intended to be cached by the
// API Gateway; callers should not call this on every HTTP request.
//
// Returns an empty slice (not nil) if no data is present, so callers always
// receive a valid JSON array on serialisation.
func (s *Store) ListGPUs(ctx context.Context) ([]GPUSummary, error) {
	// Execute the DISTINCT query. The idx_gpu_metrics_uuid_ts index makes
	// this fast even on large tables.
	rows, err := s.pool.Query(ctx, listGPUsSQL)
	if err != nil {
		return nil, fmt.Errorf("list GPUs query: %w", err)
	}
	// Always close the rows cursor to return the connection to the pool.
	defer rows.Close()

	// Pre-allocate a non-nil slice so the caller always gets a valid JSON array.
	gpus := make([]GPUSummary, 0)

	// Iterate over each result row and scan the four columns into a GPUSummary.
	for rows.Next() {
		var g GPUSummary
		if err := rows.Scan(&g.ID, &g.Hostname, &g.GpuID, &g.ModelName); err != nil {
			return nil, fmt.Errorf("scan GPU row: %w", err)
		}
		gpus = append(gpus, g)
	}

	// rows.Err() returns any error encountered during iteration (e.g. a lost
	// connection mid-stream). Checking it is separate from the per-row Scan errors.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate GPU rows: %w", err)
	}

	return gpus, nil
}

// GPUExists returns true if the given uuid is present in the gpu_metrics table.
// It is called by the telemetry handler to distinguish between "unknown UUID"
// (→ 404) and "UUID exists but no data in time window" (→ 200 with empty array).
func (s *Store) GPUExists(ctx context.Context, uuid string) (bool, error) {
	// QueryRow executes the existence probe. The query returns a single integer
	// (1) if any row matches, or no rows otherwise.
	var exists int
	err := s.pool.QueryRow(ctx, gpuExistsSQL, uuid).Scan(&exists)

	if errors.Is(err, pgx.ErrNoRows) {
		// No rows means the UUID is not present — this is not an error.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("gpu exists query: %w", err)
	}

	// A non-error result from Scan means the row was found.
	return true, nil
}

// GetTelemetry returns time-series entries for the given GPU uuid within the
// time window specified by f. Rows are ordered by timestamp ascending.
//
// If the uuid is valid but no data falls within the window, an empty (non-nil)
// slice is returned without error. Callers must call GPUExists first if they
// need to distinguish "unknown uuid" from "no data in range".
func (s *Store) GetTelemetry(ctx context.Context, uuid string, f TelemetryFilter) ([]TelemetryEntry, error) {
	// Execute the parameterised range query. All four parameters are bound
	// positionally to prevent SQL injection.
	rows, err := s.pool.Query(ctx, getTelemetrySQL, uuid, f.StartTime, f.EndTime, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("get telemetry query: %w", err)
	}
	// Always close the rows cursor to return the connection to the pool.
	defer rows.Close()

	// Pre-allocate a non-nil slice so the caller always gets a valid JSON array.
	entries := make([]TelemetryEntry, 0)

	// Scan each row into a TelemetryEntry.
	for rows.Next() {
		var e TelemetryEntry
		if err := rows.Scan(
			&e.Timestamp,
			&e.MetricName,
			&e.Value,
			&e.Hostname,
			&e.GpuID,
			&e.ModelName,
		); err != nil {
			return nil, fmt.Errorf("scan telemetry row: %w", err)
		}
		entries = append(entries, e)
	}

	// Check for any error that occurred during row iteration.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate telemetry rows: %w", err)
	}

	return entries, nil
}
