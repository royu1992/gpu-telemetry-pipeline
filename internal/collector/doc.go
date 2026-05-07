// Package collector implements the message consumption and persistence logic
// for the Telemetry Collector service.
//
// It long-polls the message-queue for batches of TelemetryMessages, writes
// them to PostgreSQL via idempotent upsert, and acknowledges each batch.
package collector
