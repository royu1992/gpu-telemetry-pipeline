package config

import (
	"os"
	"strconv"
	"time"
)

// GatewayConfig holds all runtime configuration for the api-gateway service.
// Every field is populated from an environment variable with a documented
// default so the binary is runnable with zero configuration for local development.
type GatewayConfig struct {
	// Port is the TCP port the Gin HTTP server listens on.
	// Controlled by GATEWAY_PORT.
	Port string

	// DatabaseURL is the full Postgres DSN for the read-only gateway user.
	// Example: "postgres://gateway_user:pass@postgres:5432/telemetry?sslmode=disable"
	// Controlled by GATEWAY_DB_DSN.
	DatabaseURL string

	// DBMaxConns is the maximum number of open connections in the read pool.
	// Kept intentionally small to avoid starving the Collector's write pool.
	// Controlled by GATEWAY_DB_MAX_CONNS.
	DBMaxConns int

	// DBConnectTimeout is the deadline for the initial connection attempt at startup.
	// If Postgres is unreachable within this window, the process exits cleanly.
	// Controlled by GATEWAY_DB_CONNECT_TIMEOUT.
	DBConnectTimeout time.Duration

	// DBQueryTimeout is the per-request context deadline applied to every DB query.
	// Queries that exceed this duration are cancelled and a 504 is returned.
	// Controlled by GATEWAY_DB_QUERY_TIMEOUT.
	DBQueryTimeout time.Duration

	// MaxResponseRows is the hard LIMIT applied to every GetTelemetry query.
	// Prevents a single request from returning millions of rows and exhausting
	// the Gateway's memory.
	// Controlled by GATEWAY_MAX_RESPONSE_ROWS.
	MaxResponseRows int

	// CacheTTLGPUs is the Time-To-Live for the in-memory GPU list cache.
	// The GPU list is re-queried from the DB at most once per TTL window.
	// Controlled by GATEWAY_CACHE_TTL_GPUS.
	CacheTTLGPUs time.Duration

	// CORSOrigins is the comma-separated list of allowed CORS origins.
	// Use "*" for permissive development defaults, or lock to specific origins
	// in production.
	// Controlled by GATEWAY_CORS_ORIGINS.
	CORSOrigins string

	// ShutdownGrace is the maximum time the HTTP server waits for in-flight
	// requests to complete before forcefully closing connections.
	// Controlled by GATEWAY_SHUTDOWN_GRACE.
	ShutdownGrace time.Duration
}

// Load reads all configuration from environment variables and falls back to
// the documented production-safe defaults for any variable that is absent or invalid.
func Load() GatewayConfig {
	return GatewayConfig{
		// Network.
		Port: envStr("GATEWAY_PORT", "8083"),

		// Database connection.
		DatabaseURL:      envStr("GATEWAY_DB_DSN", "postgres://postgres:postgres@localhost:5432/telemetry?sslmode=disable"),
		DBMaxConns:       envInt("GATEWAY_DB_MAX_CONNS", 10),
		DBConnectTimeout: envDuration("GATEWAY_DB_CONNECT_TIMEOUT", 10*time.Second),

		// Query safety.
		DBQueryTimeout:  envDuration("GATEWAY_DB_QUERY_TIMEOUT", 5*time.Second),
		MaxResponseRows: envInt("GATEWAY_MAX_RESPONSE_ROWS", 1000),

		// Cache.
		CacheTTLGPUs: envDuration("GATEWAY_CACHE_TTL_GPUS", 1*time.Minute),

		// CORS.
		CORSOrigins: envStr("GATEWAY_CORS_ORIGINS", "*"),

		// Shutdown.
		ShutdownGrace: envDuration("GATEWAY_SHUTDOWN_GRACE", 10*time.Second),
	}
}

// envStr returns the value of the environment variable key.
// If the variable is unset or empty, def is returned.
func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envInt returns the integer value of the environment variable key.
// If the variable is unset, empty, or not a valid integer, def is returned.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envDuration returns the time.Duration value of the environment variable key.
// If the variable is unset, empty, or not a valid duration string, def is returned.
func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
