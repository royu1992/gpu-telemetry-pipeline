// Package streamer implements the CSV reading and message publishing logic
// for the Telemetry Streamer service.
//
// It loops over the configured CSV file, parses each row into a
// TelemetryMessage, and publishes it to the message-queue with
// configurable emit interval and retry/backoff on failure.
package streamer
