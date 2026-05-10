package config

import (
	"os"
	"strconv"
	"time"
)

// QueueConfig holds all runtime configuration for the message-queue service.
// Every field is populated from an environment variable with a documented default.
type QueueConfig struct {
	// Port is the HTTP server listen port.
	Port string

	// Capacity is the total number of slots in the ring buffer.
	Capacity int

	// LeaseDuration is how long a Collector has to acknowledge a message
	// before it is considered expired and redelivered.
	LeaseDuration time.Duration

	// LeaseReaperInterval controls how often the background goroutine scans
	// for expired in-flight messages.
	LeaseReaperInterval time.Duration

	// MaxDeliveryAttempts is the number of times a message may be delivered
	// before it is dropped and logged.
	MaxDeliveryAttempts int

	// BatchSize is the maximum number of messages returned per consume request.
	BatchSize int

	// PublishTimeout is how long a publish request may block waiting for a
	// free slot before the queue returns 429 Too Many Requests.
	PublishTimeout time.Duration

	// LongPollTimeout is the maximum time a consume request will wait for
	// messages when the queue is empty.
	LongPollTimeout time.Duration

	// ShutdownGracePeriod is the time allowed for draining in-flight connections
	// after a SIGTERM is received.
	ShutdownGracePeriod time.Duration

	// MaxRequestBodySize is the maximum accepted size (bytes) for POST /messages.
	MaxRequestBodySize int64

	// HTTPReadTimeout, HTTPWriteTimeout, HTTPIdleTimeout configure the underlying
	// net/http server. WriteTimeout must exceed LongPollTimeout.
	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration
	HTTPIdleTimeout  time.Duration
}

// LoadQueueConfig reads configuration from environment variables.
// Missing or unparseable values fall back to the documented defaults.
func LoadQueueConfig() QueueConfig {
	// Build and return the config struct by reading each setting from its
	// corresponding environment variable. The helper functions each fall back
	// to the documented default when the variable is absent or invalid.
	return QueueConfig{
		Port:                envStr("QUEUE_PORT", "8080"),
		Capacity:            envInt("QUEUE_CAPACITY", 10000),
		LeaseDuration:       envDuration("QUEUE_LEASE_DURATION", 30*time.Second),
		LeaseReaperInterval: envDuration("QUEUE_LEASE_REAPER_INTERVAL", 5*time.Second),
		MaxDeliveryAttempts: envInt("QUEUE_MAX_DELIVERY_ATTEMPTS", 3),
		BatchSize:           envInt("QUEUE_BATCH_SIZE", 10),
		PublishTimeout:      envDuration("QUEUE_PUBLISH_TIMEOUT", 10*time.Second),
		LongPollTimeout:     envDuration("QUEUE_LONG_POLL_TIMEOUT", 30*time.Second),
		ShutdownGracePeriod: envDuration("QUEUE_SHUTDOWN_GRACE_PERIOD", 30*time.Second),
		MaxRequestBodySize:  int64(envInt("QUEUE_MAX_REQUEST_BODY_SIZE", 65536)),
		HTTPReadTimeout:     envDuration("QUEUE_HTTP_READ_TIMEOUT", 15*time.Second),
		HTTPWriteTimeout:    envDuration("QUEUE_HTTP_WRITE_TIMEOUT", 40*time.Second),
		HTTPIdleTimeout:     envDuration("QUEUE_HTTP_IDLE_TIMEOUT", 60*time.Second),
	}
}

// envStr returns the value of the environment variable named key.
// If the variable is unset or empty, def is returned unchanged.
func envStr(key, def string) string {
	// Read the raw value from the process environment.
	if v := os.Getenv(key); v != "" {
		return v
	}
	// Environment variable is absent or empty — return the compiled-in default.
	return def
}

// envInt returns the integer value of the environment variable named key.
// The value must parse as a positive integer; otherwise def is returned.
func envInt(key string, def int) int {
	// Read the raw value from the process environment.
	if v := os.Getenv(key); v != "" {
		// Attempt to parse as a base-10 integer. Reject zero and negative values
		// because all integer config fields represent counts or sizes that must
		// be strictly positive.
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	// Environment variable is absent, empty, or invalid — return the default.
	return def
}

// envDuration returns the time.Duration value of the environment variable
// named key. The value must be a valid Go duration string (e.g. "30s") and
// must be positive; otherwise def is returned.
func envDuration(key string, def time.Duration) time.Duration {
	// Read the raw value from the process environment.
	if v := os.Getenv(key); v != "" {
		// Attempt to parse as a Go duration string. Reject zero and negative
		// durations because all duration config fields must be positive intervals.
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	// Environment variable is absent, empty, or invalid — return the default.
	return def
}
