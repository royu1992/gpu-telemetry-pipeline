package config

import (
	"os"
	"strconv"
	"time"
)

// CollectorConfig holds all runtime configuration for the collector service.
// Every field is populated from an environment variable with a documented
// default so the binary is runnable with zero configuration for local development.
type CollectorConfig struct {
	// Port is the listen port for the health, readiness, and metrics Gin server.
	// Controlled by COLLECTOR_PORT.
	Port string

	// QueueURL is the base URL of the message-queue service.
	// Example: "http://message-queue:8080"
	// Controlled by COLLECTOR_QUEUE_URL.
	QueueURL string

	// DatabaseURL is the full Postgres connection DSN.
	// Providing a single DSN is simpler and more portable than separate
	// host/user/password fields — it maps directly to what pgx expects.
	// Example: "postgres://user:pass@host:5432/dbname?sslmode=disable"
	// Controlled by COLLECTOR_DATABASE_URL.
	DatabaseURL string

	// BatchSize is the maximum number of messages the collector pulls from
	// the queue in a single long-poll request.
	// Controlled by COLLECTOR_BATCH_SIZE.
	BatchSize int

	// LongPollTimeout is how long the queue will block a consume request
	// waiting for messages when the queue is empty. Must be less than the
	// queue server's write timeout.
	// Controlled by COLLECTOR_LONG_POLL_TIMEOUT.
	LongPollTimeout time.Duration

	// DBMaxConns is the maximum number of open connections in the pgxpool.
	// Controlled by COLLECTOR_DB_MAX_CONNS.
	DBMaxConns int

	// DBConnectTimeout is the deadline applied when establishing the initial
	// connection to Postgres during startup. If the database is unreachable
	// within this window, the process exits immediately rather than hanging.
	// Controlled by COLLECTOR_DB_CONNECT_TIMEOUT.
	DBConnectTimeout time.Duration

	// RequestTimeout is the per-call HTTP deadline applied to outgoing calls
	// that are not long-polls — specifically the POST /messages/ack requests.
	// Controlled by COLLECTOR_REQUEST_TIMEOUT.
	RequestTimeout time.Duration

	// ErrorBackoff is the pause inserted between poll iterations after a
	// non-context-cancellation error (e.g. queue returns 5xx). This prevents
	// the collector from slamming a temporarily unavailable upstream.
	// Controlled by COLLECTOR_ERROR_BACKOFF.
	ErrorBackoff time.Duration

	// ShutdownGrace is the total process-level shutdown deadline. If the
	// consumption loop and health server have not finished within this window
	// after SIGTERM, the process exits forcefully.
	// Controlled by COLLECTOR_SHUTDOWN_GRACE.
	ShutdownGrace time.Duration
}

// Load reads all configuration from environment variables and falls back to
// the documented production defaults for any variable that is absent or invalid.
func Load() CollectorConfig {
	return CollectorConfig{
		// Network coordinates.
		Port:        envStr("COLLECTOR_PORT", "8082"),
		QueueURL:    envStr("COLLECTOR_QUEUE_URL", "http://message-queue:8080"),
		DatabaseURL: envStr("COLLECTOR_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/telemetry?sslmode=disable"),

		// Consumption tuning.
		BatchSize:       envInt("COLLECTOR_BATCH_SIZE", 10),
		LongPollTimeout: envDuration("COLLECTOR_LONG_POLL_TIMEOUT", 30*time.Second),

		// Database pool.
		DBMaxConns:       envInt("COLLECTOR_DB_MAX_CONNS", 5),
		DBConnectTimeout: envDuration("COLLECTOR_DB_CONNECT_TIMEOUT", 10*time.Second),

		// HTTP client timeouts.
		RequestTimeout: envDuration("COLLECTOR_REQUEST_TIMEOUT", 10*time.Second),
		ErrorBackoff:   envDuration("COLLECTOR_ERROR_BACKOFF", 2*time.Second),

		// Shutdown.
		ShutdownGrace: envDuration("COLLECTOR_SHUTDOWN_GRACE", 30*time.Second),
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

// envInt returns the integer value of the environment variable named key.
// The value must parse as a positive integer; otherwise def is returned.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		// Reject zero and negative values because all integer config fields
		// represent counts or sizes that must be strictly positive.
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return def
}

// envDuration returns the time.Duration value of the environment variable
// named key. The value must be a valid Go duration string (e.g. "30s") and
// must be positive; otherwise def is returned.
func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		// Reject zero and negative durations because all duration config
		// fields must represent positive intervals.
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}
