package config

import (
	"os"
	"testing"
	"time"
)

// envKeys lists every environment variable owned by the collector config
// package. They are captured before each test and restored afterwards so
// parallel test execution and IDE test runners do not corrupt each other.
var envKeys = []string{
	"COLLECTOR_PORT",
	"COLLECTOR_QUEUE_URL",
	"COLLECTOR_DATABASE_URL",
	"COLLECTOR_BATCH_SIZE",
	"COLLECTOR_LONG_POLL_TIMEOUT",
	"COLLECTOR_DB_MAX_CONNS",
	"COLLECTOR_DB_CONNECT_TIMEOUT",
	"COLLECTOR_REQUEST_TIMEOUT",
	"COLLECTOR_ERROR_BACKOFF",
	"COLLECTOR_SHUTDOWN_GRACE",
}

// snapshotEnv captures the current values of all collector env vars and
// returns a restore function that puts them back when called.
func snapshotEnv(t *testing.T) func() {
	t.Helper()
	saved := make(map[string]string, len(envKeys))
	for _, k := range envKeys {
		saved[k] = os.Getenv(k)
	}
	return func() {
		for k, v := range saved {
			if v == "" {
				os.Unsetenv(k) //nolint:errcheck
			} else {
				os.Setenv(k, v) //nolint:errcheck
			}
		}
	}
}

// setEnv applies a map of key→value pairs and unsets any key not present.
func setEnv(env map[string]string) {
	// First clear every key so leftover values from previous iterations cannot
	// bleed into the case being set up.
	for _, k := range envKeys {
		os.Unsetenv(k) //nolint:errcheck
	}
	// Then apply only the values specified for this test case.
	for k, v := range env {
		os.Setenv(k, v) //nolint:errcheck
	}
}

// TestLoad_Defaults verifies that when no environment variables are set Load
// returns the documented production defaults for every field.
func TestLoad_Defaults(t *testing.T) {
	// Restore the original environment after this test.
	restore := snapshotEnv(t)
	defer restore()

	// Clear all collector env vars so the default path is exercised.
	setEnv(map[string]string{})

	cfg := Load()

	// Table of (field description, got, want) triples used to produce a
	// single failure line per field rather than stopping at the first mismatch.
	type check struct {
		field string
		got   interface{}
		want  interface{}
	}

	checks := []check{
		{"Port", cfg.Port, "8082"},
		{"QueueURL", cfg.QueueURL, "http://message-queue:8080"},
		{"DatabaseURL", cfg.DatabaseURL, "postgres://postgres:postgres@localhost:5432/telemetry?sslmode=disable"},
		{"BatchSize", cfg.BatchSize, 10},
		{"LongPollTimeout", cfg.LongPollTimeout, 30 * time.Second},
		{"DBMaxConns", cfg.DBMaxConns, 5},
		{"DBConnectTimeout", cfg.DBConnectTimeout, 10 * time.Second},
		{"RequestTimeout", cfg.RequestTimeout, 10 * time.Second},
		{"ErrorBackoff", cfg.ErrorBackoff, 2 * time.Second},
		{"ShutdownGrace", cfg.ShutdownGrace, 30 * time.Second},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("default %s: got %v, want %v", c.field, c.got, c.want)
		}
	}
}

// TestLoad_CustomValues verifies that valid environment variables are parsed
// and returned as the corresponding typed values.
func TestLoad_CustomValues(t *testing.T) {
	restore := snapshotEnv(t)
	defer restore()

	setEnv(map[string]string{
		"COLLECTOR_PORT":               "9090",
		"COLLECTOR_QUEUE_URL":          "http://queue:1234",
		"COLLECTOR_DATABASE_URL":       "postgres://u:p@db:5432/mydb",
		"COLLECTOR_BATCH_SIZE":         "50",
		"COLLECTOR_LONG_POLL_TIMEOUT":  "15s",
		"COLLECTOR_DB_MAX_CONNS":       "20",
		"COLLECTOR_DB_CONNECT_TIMEOUT": "5s",
		"COLLECTOR_REQUEST_TIMEOUT":    "3s",
		"COLLECTOR_ERROR_BACKOFF":      "1s",
		"COLLECTOR_SHUTDOWN_GRACE":     "45s",
	})

	cfg := Load()

	// Verify every field received its custom value.
	if cfg.Port != "9090" {
		t.Errorf("Port: got %q, want 9090", cfg.Port)
	}
	if cfg.QueueURL != "http://queue:1234" {
		t.Errorf("QueueURL: got %q", cfg.QueueURL)
	}
	if cfg.DatabaseURL != "postgres://u:p@db:5432/mydb" {
		t.Errorf("DatabaseURL: got %q", cfg.DatabaseURL)
	}
	if cfg.BatchSize != 50 {
		t.Errorf("BatchSize: got %d, want 50", cfg.BatchSize)
	}
	if cfg.LongPollTimeout != 15*time.Second {
		t.Errorf("LongPollTimeout: got %v, want 15s", cfg.LongPollTimeout)
	}
	if cfg.DBMaxConns != 20 {
		t.Errorf("DBMaxConns: got %d, want 20", cfg.DBMaxConns)
	}
	if cfg.DBConnectTimeout != 5*time.Second {
		t.Errorf("DBConnectTimeout: got %v, want 5s", cfg.DBConnectTimeout)
	}
	if cfg.RequestTimeout != 3*time.Second {
		t.Errorf("RequestTimeout: got %v, want 3s", cfg.RequestTimeout)
	}
	if cfg.ErrorBackoff != 1*time.Second {
		t.Errorf("ErrorBackoff: got %v, want 1s", cfg.ErrorBackoff)
	}
	if cfg.ShutdownGrace != 45*time.Second {
		t.Errorf("ShutdownGrace: got %v, want 45s", cfg.ShutdownGrace)
	}
}

// TestEnvStr covers every branch of the envStr helper: present value, absent
// key, and empty string (treated the same as absent).
func TestEnvStr(t *testing.T) {
	restore := snapshotEnv(t)
	defer restore()

	const key = "COLLECTOR_PORT" // reuse an existing key; value will be restored

	tests := []struct {
		name    string
		setup   func()
		wantVal string
	}{
		{
			name:    "Key present with value",
			setup:   func() { os.Setenv(key, "1234") },
			wantVal: "1234",
		},
		{
			name:    "Key absent returns default",
			setup:   func() { os.Unsetenv(key) },
			wantVal: "default-value",
		},
		{
			name:    "Key set to empty string returns default",
			setup:   func() { os.Setenv(key, "") },
			wantVal: "default-value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			got := envStr(key, "default-value")
			if got != tt.wantVal {
				t.Errorf("envStr(%q): got %q, want %q", key, got, tt.wantVal)
			}
		})
	}
}

// TestEnvInt covers every branch of the envInt helper: valid positive value,
// absent key, invalid string, zero (rejected), and negative (rejected).
func TestEnvInt(t *testing.T) {
	restore := snapshotEnv(t)
	defer restore()

	const key = "COLLECTOR_BATCH_SIZE"

	tests := []struct {
		name    string
		setup   func()
		wantVal int
	}{
		{
			name:    "Valid positive integer",
			setup:   func() { os.Setenv(key, "42") },
			wantVal: 42,
		},
		{
			name:    "Key absent returns default",
			setup:   func() { os.Unsetenv(key) },
			wantVal: 99,
		},
		{
			name:    "Non-numeric string returns default",
			setup:   func() { os.Setenv(key, "not-a-number") },
			wantVal: 99,
		},
		{
			name:    "Zero returns default (not strictly positive)",
			setup:   func() { os.Setenv(key, "0") },
			wantVal: 99,
		},
		{
			name:    "Negative integer returns default",
			setup:   func() { os.Setenv(key, "-5") },
			wantVal: 99,
		},
		{
			name:    "Empty string returns default",
			setup:   func() { os.Setenv(key, "") },
			wantVal: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			got := envInt(key, 99)
			if got != tt.wantVal {
				t.Errorf("envInt(%q): got %d, want %d", key, got, tt.wantVal)
			}
		})
	}
}

// TestEnvDuration covers every branch of the envDuration helper: valid
// positive duration, absent key, invalid string, zero duration, and negative.
func TestEnvDuration(t *testing.T) {
	restore := snapshotEnv(t)
	defer restore()

	const key = "COLLECTOR_ERROR_BACKOFF"
	const def = 7 * time.Second

	tests := []struct {
		name    string
		setup   func()
		wantVal time.Duration
	}{
		{
			name:    "Valid positive duration",
			setup:   func() { os.Setenv(key, "15s") },
			wantVal: 15 * time.Second,
		},
		{
			name:    "Key absent returns default",
			setup:   func() { os.Unsetenv(key) },
			wantVal: def,
		},
		{
			name:    "Non-duration string returns default",
			setup:   func() { os.Setenv(key, "not-a-duration") },
			wantVal: def,
		},
		{
			name:    "Zero duration returns default",
			setup:   func() { os.Setenv(key, "0s") },
			wantVal: def,
		},
		{
			name:    "Negative duration returns default",
			setup:   func() { os.Setenv(key, "-5s") },
			wantVal: def,
		},
		{
			name:    "Empty string returns default",
			setup:   func() { os.Setenv(key, "") },
			wantVal: def,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			got := envDuration(key, def)
			if got != tt.wantVal {
				t.Errorf("envDuration(%q): got %v, want %v", key, got, tt.wantVal)
			}
		})
	}
}
