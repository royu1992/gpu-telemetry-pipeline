package config

import (
	"os"
	"strconv"
	"time"
)

// StreamerConfig holds all runtime configuration for the streamer service.
// Every field is populated from an environment variable with a documented default.
type StreamerConfig struct {
	// CSVPath is the absolute path to the mounted CSV telemetry data file.
	CSVPath string

	// QueueURL is the base URL of the message-queue service.
	// Example: "http://message-queue:8080"
	QueueURL string

	// Interval is the pause between consecutive row sends.
	// Controlled by STREAMER_INTERVAL_MS (in milliseconds).
	Interval time.Duration

	// RequestTimeout is the per-POST HTTP deadline applied to each individual
	// send attempt (including retries).
	// Controlled by STREAMER_REQUEST_TIMEOUT_SECONDS.
	RequestTimeout time.Duration

	// ShutdownGrace is the total process-level shutdown deadline.
	// If the streamer has not fully stopped within this window after receiving
	// a termination signal, it exits forcefully.
	// Controlled by STREAMER_SHUTDOWN_GRACE_SECONDS.
	ShutdownGrace time.Duration

	// MaxConsecutiveErrors is the number of consecutive bad CSV rows that
	// triggers a fatal exit. A "bad row" is one that parses successfully but
	// fails validation (e.g. a required field is empty). Ten consecutive
	// failures almost certainly mean the mounted file is corrupt or wrong.
	// Controlled by STREAMER_MAX_CONSECUTIVE_ERRORS.
	MaxConsecutiveErrors int

	// RetryAttempts is the maximum number of send attempts per row.
	// On exhaustion the row is skipped and errors_total is incremented.
	// Controlled by STREAMER_RETRY_ATTEMPTS.
	RetryAttempts int

	// RetryDelay is the fixed pause between consecutive send retries.
	// Controlled by STREAMER_RETRY_DELAY_SECONDS.
	RetryDelay time.Duration

	// Port is the listen port for the health, readiness, and metrics Gin server.
	// Controlled by STREAMER_PORT.
	Port string
}

// Load reads all configuration from environment variables and falls back to
// the documented production defaults for any variable that is absent or invalid.
func Load() StreamerConfig {
	return StreamerConfig{
		// File system and network coordinates.
		CSVPath:  envStr("STREAMER_CSV_PATH", "docs/dcgm_metrics_20250718_134233.csv"),
		QueueURL: envStr("STREAMER_QUEUE_URL", "http://message-queue:8080"),

		// Timing: interval is given in milliseconds; the rest are in seconds.
		Interval:       time.Duration(envInt("STREAMER_INTERVAL_MS", 100)) * time.Millisecond,
		RequestTimeout: time.Duration(envInt("STREAMER_REQUEST_TIMEOUT_SECONDS", 10)) * time.Second,
		ShutdownGrace:  time.Duration(envInt("STREAMER_SHUTDOWN_GRACE_SECONDS", 30)) * time.Second,

		// Reliability policy.
		MaxConsecutiveErrors: envInt("STREAMER_MAX_CONSECUTIVE_ERRORS", 10),
		RetryAttempts:        envInt("STREAMER_RETRY_ATTEMPTS", 3),
		RetryDelay:           time.Duration(envInt("STREAMER_RETRY_DELAY_SECONDS", 2)) * time.Second,

		// HTTP server.
		Port: envStr("STREAMER_PORT", "8081"),
	}
}

// envStr returns the value of the environment variable named key.
// If the variable is unset or empty, def is returned unchanged.
func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

// envInt parses the environment variable named key as a non-negative integer.
// If the variable is absent, empty, or not parseable as a non-negative integer,
// def is returned unchanged.
func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}

	// Reject anything that is not a valid non-negative integer.
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}

	return n
}
