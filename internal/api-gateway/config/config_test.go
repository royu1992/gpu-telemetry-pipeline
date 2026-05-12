package config

import (
	"os"
	"testing"
	"time"
)

// TestLoad_Defaults verifies that Load returns the compiled-in defaults when
// no environment variables are set. Each field is checked individually so a
// future addition that forgets to set a default is caught immediately.
func TestLoad_Defaults(t *testing.T) {
	// Ensure no stray environment variables from the test runner interfere.
	unsetAll(t)

	cfg := Load()

	tests := []struct {
		name string
		got  any
		want any
	}{
		{"Port", cfg.Port, "8083"},
		{"DatabaseURL", cfg.DatabaseURL, "postgres://postgres:postgres@localhost:5432/telemetry?sslmode=disable"},
		{"DBMaxConns", cfg.DBMaxConns, 10},
		{"DBConnectTimeout", cfg.DBConnectTimeout, 10 * time.Second},
		{"DBQueryTimeout", cfg.DBQueryTimeout, 5 * time.Second},
		{"MaxResponseRows", cfg.MaxResponseRows, 1000},
		{"CacheTTLGPUs", cfg.CacheTTLGPUs, 1 * time.Minute},
		{"CORSOrigins", cfg.CORSOrigins, "*"},
		{"ShutdownGrace", cfg.ShutdownGrace, 10 * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("Load().%s = %v, want %v", tc.name, tc.got, tc.want)
			}
		})
	}
}

// TestLoad_EnvOverrides verifies that each field is overridden when the
// corresponding environment variable is set to a valid value.
func TestLoad_EnvOverrides(t *testing.T) {
	// Set every supported variable to a recognisable non-default value.
	t.Setenv("GATEWAY_PORT", "9090")
	t.Setenv("GATEWAY_DB_DSN", "postgres://gw:secret@db:5432/prod")
	t.Setenv("GATEWAY_DB_MAX_CONNS", "20")
	t.Setenv("GATEWAY_DB_CONNECT_TIMEOUT", "30s")
	t.Setenv("GATEWAY_DB_QUERY_TIMEOUT", "3s")
	t.Setenv("GATEWAY_MAX_RESPONSE_ROWS", "500")
	t.Setenv("GATEWAY_CACHE_TTL_GPUS", "2m")
	t.Setenv("GATEWAY_CORS_ORIGINS", "https://dashboard.company.com")
	t.Setenv("GATEWAY_SHUTDOWN_GRACE", "15s")

	cfg := Load()

	tests := []struct {
		name string
		got  any
		want any
	}{
		{"Port", cfg.Port, "9090"},
		{"DatabaseURL", cfg.DatabaseURL, "postgres://gw:secret@db:5432/prod"},
		{"DBMaxConns", cfg.DBMaxConns, 20},
		{"DBConnectTimeout", cfg.DBConnectTimeout, 30 * time.Second},
		{"DBQueryTimeout", cfg.DBQueryTimeout, 3 * time.Second},
		{"MaxResponseRows", cfg.MaxResponseRows, 500},
		{"CacheTTLGPUs", cfg.CacheTTLGPUs, 2 * time.Minute},
		{"CORSOrigins", cfg.CORSOrigins, "https://dashboard.company.com"},
		{"ShutdownGrace", cfg.ShutdownGrace, 15 * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("Load().%s = %v, want %v", tc.name, tc.got, tc.want)
			}
		})
	}
}

// TestLoad_InvalidValues verifies that malformed environment variables fall
// back to the compiled-in default rather than producing a zero value or panic.
func TestLoad_InvalidValues(t *testing.T) {
	// Set every variable to a clearly invalid value.
	t.Setenv("GATEWAY_DB_MAX_CONNS", "not-a-number")
	t.Setenv("GATEWAY_DB_CONNECT_TIMEOUT", "bad-duration")
	t.Setenv("GATEWAY_DB_QUERY_TIMEOUT", "bad-duration")
	t.Setenv("GATEWAY_MAX_RESPONSE_ROWS", "xyz")
	t.Setenv("GATEWAY_CACHE_TTL_GPUS", "???")
	t.Setenv("GATEWAY_SHUTDOWN_GRACE", "???")

	cfg := Load()

	// All numeric/duration fields should fall back to their defaults.
	if cfg.DBMaxConns != 10 {
		t.Errorf("DBMaxConns = %d, want 10 (default)", cfg.DBMaxConns)
	}
	if cfg.DBConnectTimeout != 10*time.Second {
		t.Errorf("DBConnectTimeout = %v, want 10s (default)", cfg.DBConnectTimeout)
	}
	if cfg.DBQueryTimeout != 5*time.Second {
		t.Errorf("DBQueryTimeout = %v, want 5s (default)", cfg.DBQueryTimeout)
	}
	if cfg.MaxResponseRows != 1000 {
		t.Errorf("MaxResponseRows = %d, want 1000 (default)", cfg.MaxResponseRows)
	}
	if cfg.CacheTTLGPUs != 1*time.Minute {
		t.Errorf("CacheTTLGPUs = %v, want 1m (default)", cfg.CacheTTLGPUs)
	}
	if cfg.ShutdownGrace != 10*time.Second {
		t.Errorf("ShutdownGrace = %v, want 10s (default)", cfg.ShutdownGrace)
	}
}

// unsetAll clears every GATEWAY_* environment variable so tests start from
// a clean state regardless of the developer's local environment.
func unsetAll(t *testing.T) {
	t.Helper()
	vars := []string{
		"GATEWAY_PORT",
		"GATEWAY_DB_DSN",
		"GATEWAY_DB_MAX_CONNS",
		"GATEWAY_DB_CONNECT_TIMEOUT",
		"GATEWAY_DB_QUERY_TIMEOUT",
		"GATEWAY_MAX_RESPONSE_ROWS",
		"GATEWAY_CACHE_TTL_GPUS",
		"GATEWAY_CORS_ORIGINS",
		"GATEWAY_SHUTDOWN_GRACE",
	}
	for _, v := range vars {
		// Save and restore the original value so the test is side-effect-free.
		orig, exists := os.LookupEnv(v)
		os.Unsetenv(v) //nolint:errcheck
		t.Cleanup(func() {
			if exists {
				os.Setenv(v, orig) //nolint:errcheck
			} else {
				os.Unsetenv(v) //nolint:errcheck
			}
		})
	}
}
