// Package db provides the PostgreSQL client, connection pool setup, and
// query helpers used by the Collector and API Gateway services.
//
// The Collector uses it for idempotent upserts into the telemetry table.
// The API Gateway uses it for read-only queries.
package db
