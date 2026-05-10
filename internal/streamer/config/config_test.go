package config

import (
	"testing"
	"time"
)

// allEnvKeys lists every environment variable controlled by this package.
// They are cleared before each sub-test to ensure isolation.
var allEnvKeys = []string{
	"STREAMER_CSV_PATH",
	"STREAMER_QUEUE_URL",
	"STREAMER_INTERVAL_MS",
	"STREAMER_REQUEST_TIMEOUT_SECONDS",
	"STREAMER_SHUTDOWN_GRACE_SECONDS",
	"STREAMER_MAX_CONSECUTIVE_ERRORS",
	"STREAMER_RETRY_ATTEMPTS",
	"STREAMER_RETRY_DELAY_SECONDS",
	"STREAMER_PORT",
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name string
		// env contains the key-value pairs to set before calling Load.
		// Keys absent from this map are cleared so defaults apply.
		env  map[string]string
		want StreamerConfig
	}{
		{
			// Step: Verify production defaults when no env vars are set.
			name: "default values when no env vars set",
			env:  map[string]string{},
			want: StreamerConfig{
				CSVPath:              "docs/dcgm_metrics_20250718_134233.csv",
				QueueURL:             "http://message-queue:8080",
				Interval:             100 * time.Millisecond,
				RequestTimeout:       10 * time.Second,
				ShutdownGrace:        30 * time.Second,
				MaxConsecutiveErrors: 10,
				RetryAttempts:        3,
				RetryDelay:           2 * time.Second,
				Port:                 "8081",
			},
		},
		{
			// Step: Verify that valid env vars override every default.
			name: "all fields overridden by env vars",
			env: map[string]string{
				"STREAMER_CSV_PATH":               "/data/metrics.csv",
				"STREAMER_QUEUE_URL":              "http://queue:9090",
				"STREAMER_INTERVAL_MS":            "200",
				"STREAMER_REQUEST_TIMEOUT_SECONDS": "5",
				"STREAMER_SHUTDOWN_GRACE_SECONDS":  "15",
				"STREAMER_MAX_CONSECUTIVE_ERRORS":  "20",
				"STREAMER_RETRY_ATTEMPTS":          "5",
				"STREAMER_RETRY_DELAY_SECONDS":     "3",
				"STREAMER_PORT":                   "9000",
			},
			want: StreamerConfig{
				CSVPath:              "/data/metrics.csv",
				QueueURL:             "http://queue:9090",
				Interval:             200 * time.Millisecond,
				RequestTimeout:       5 * time.Second,
				ShutdownGrace:        15 * time.Second,
				MaxConsecutiveErrors: 20,
				RetryAttempts:        5,
				RetryDelay:           3 * time.Second,
				Port:                 "9000",
			},
		},
		{
			// Step: Verify that non-numeric string values fall back to defaults.
			name: "non-numeric integer env vars fall back to defaults",
			env: map[string]string{
				"STREAMER_INTERVAL_MS":            "not-a-number",
				"STREAMER_REQUEST_TIMEOUT_SECONDS": "abc",
				"STREAMER_MAX_CONSECUTIVE_ERRORS":  "xyz",
			},
			want: StreamerConfig{
				CSVPath:              "docs/dcgm_metrics_20250718_134233.csv",
				QueueURL:             "http://message-queue:8080",
				Interval:             100 * time.Millisecond,
				RequestTimeout:       10 * time.Second,
				ShutdownGrace:        30 * time.Second,
				MaxConsecutiveErrors: 10,
				RetryAttempts:        3,
				RetryDelay:           2 * time.Second,
				Port:                 "8081",
			},
		},
		{
			// Step: Verify that negative integers fall back to defaults.
			// envInt rejects n < 0, ensuring durations are never negative.
			name: "negative integer env vars fall back to defaults",
			env: map[string]string{
				"STREAMER_INTERVAL_MS":            "-1",
				"STREAMER_REQUEST_TIMEOUT_SECONDS": "-5",
				"STREAMER_RETRY_ATTEMPTS":          "-3",
			},
			want: StreamerConfig{
				CSVPath:              "docs/dcgm_metrics_20250718_134233.csv",
				QueueURL:             "http://message-queue:8080",
				Interval:             100 * time.Millisecond,
				RequestTimeout:       10 * time.Second,
				ShutdownGrace:        30 * time.Second,
				MaxConsecutiveErrors: 10,
				RetryAttempts:        3,
				RetryDelay:           2 * time.Second,
				Port:                 "8081",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step: Clear all controlled env vars so previous runs do not bleed in.
			for _, k := range allEnvKeys {
				t.Setenv(k, "")
			}

			// Step: Apply the test-specific env var overrides.
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			// Step: Load config from the environment.
			got := Load()

			// Step: Compare every field individually for clear failure messages.
			if got.CSVPath != tt.want.CSVPath {
				t.Errorf("CSVPath = %q, want %q", got.CSVPath, tt.want.CSVPath)
			}
			if got.QueueURL != tt.want.QueueURL {
				t.Errorf("QueueURL = %q, want %q", got.QueueURL, tt.want.QueueURL)
			}
			if got.Interval != tt.want.Interval {
				t.Errorf("Interval = %v, want %v", got.Interval, tt.want.Interval)
			}
			if got.RequestTimeout != tt.want.RequestTimeout {
				t.Errorf("RequestTimeout = %v, want %v", got.RequestTimeout, tt.want.RequestTimeout)
			}
			if got.ShutdownGrace != tt.want.ShutdownGrace {
				t.Errorf("ShutdownGrace = %v, want %v", got.ShutdownGrace, tt.want.ShutdownGrace)
			}
			if got.MaxConsecutiveErrors != tt.want.MaxConsecutiveErrors {
				t.Errorf("MaxConsecutiveErrors = %d, want %d", got.MaxConsecutiveErrors, tt.want.MaxConsecutiveErrors)
			}
			if got.RetryAttempts != tt.want.RetryAttempts {
				t.Errorf("RetryAttempts = %d, want %d", got.RetryAttempts, tt.want.RetryAttempts)
			}
			if got.RetryDelay != tt.want.RetryDelay {
				t.Errorf("RetryDelay = %v, want %v", got.RetryDelay, tt.want.RetryDelay)
			}
			if got.Port != tt.want.Port {
				t.Errorf("Port = %q, want %q", got.Port, tt.want.Port)
			}
		})
	}
}
