package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadQueueConfig(t *testing.T) {
	// Restore environment after tests
	originalEnv := make(map[string]string)
	keys := []string{
		"QUEUE_PORT", "QUEUE_CAPACITY", "QUEUE_LEASE_DURATION",
		"QUEUE_LEASE_REAPER_INTERVAL", "QUEUE_MAX_DELIVERY_ATTEMPTS",
		"QUEUE_BATCH_SIZE", "QUEUE_PUBLISH_TIMEOUT", "QUEUE_LONG_POLL_TIMEOUT",
		"QUEUE_SHUTDOWN_GRACE_PERIOD", "QUEUE_MAX_REQUEST_BODY_SIZE",
		"QUEUE_HTTP_READ_TIMEOUT", "QUEUE_HTTP_WRITE_TIMEOUT", "QUEUE_HTTP_IDLE_TIMEOUT",
	}
	for _, k := range keys {
		originalEnv[k] = os.Getenv(k)
	}
	defer func() {
		for k, v := range originalEnv {
			if v == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, v)
			}
		}
	}()

	// Clear all for the entire test suite to ensure clean state
	for _, k := range keys {
		os.Unsetenv(k)
	}

	tests := []struct {
		name     string
		env      map[string]string
		validate func(*testing.T, QueueConfig)
	}{
		{
			name: "Default Values",
			env:  map[string]string{}, // All empty
			validate: func(t *testing.T, cfg QueueConfig) {
				if cfg.Port != "8080" {
					t.Errorf("expected Port 8080, got %s", cfg.Port)
				}
				if cfg.Capacity != 10000 {
					t.Errorf("expected Capacity 10000, got %d", cfg.Capacity)
				}
				if cfg.LeaseDuration != 30*time.Second {
					t.Errorf("expected LeaseDuration 30s, got %v", cfg.LeaseDuration)
				}
			},
		},
		{
			name: "Custom Values",
			env: map[string]string{
				"QUEUE_PORT":                  "9090",
				"QUEUE_CAPACITY":              "500",
				"QUEUE_LEASE_DURATION":        "1m",
				"QUEUE_MAX_DELIVERY_ATTEMPTS": "5",
				"QUEUE_BATCH_SIZE":            "20",
				"QUEUE_PUBLISH_TIMEOUT":       "5s",
				"QUEUE_LONG_POLL_TIMEOUT":     "10s",
				"QUEUE_MAX_REQUEST_BODY_SIZE": "1024",
			},
			validate: func(t *testing.T, cfg QueueConfig) {
				if cfg.Port != "9090" {
					t.Errorf("expected Port 9090, got %s", cfg.Port)
				}
				if cfg.Capacity != 500 {
					t.Errorf("expected Capacity 500, got %d", cfg.Capacity)
				}
				if cfg.LeaseDuration != time.Minute {
					t.Errorf("expected LeaseDuration 1m, got %v", cfg.LeaseDuration)
				}
				if cfg.MaxDeliveryAttempts != 5 {
					t.Errorf("expected MaxDeliveryAttempts 5, got %d", cfg.MaxDeliveryAttempts)
				}
				if cfg.BatchSize != 20 {
					t.Errorf("expected BatchSize 20, got %d", cfg.BatchSize)
				}
				if cfg.PublishTimeout != 5*time.Second {
					t.Errorf("expected PublishTimeout 5s, got %v", cfg.PublishTimeout)
				}
				if cfg.MaxRequestBodySize != 1024 {
					t.Errorf("expected MaxRequestBodySize 1024, got %d", cfg.MaxRequestBodySize)
				}
			},
		},
		{
			name: "Invalid Integer and Duration Values (Should fall back to defaults)",
			env: map[string]string{
				"QUEUE_CAPACITY":              "not-an-int",
				"QUEUE_LEASE_DURATION":        "not-a-duration",
				"QUEUE_MAX_DELIVERY_ATTEMPTS": "-1", // Negative should be rejected by envInt
				"QUEUE_BATCH_SIZE":            "0",  // Zero should be rejected by envInt
			},
			validate: func(t *testing.T, cfg QueueConfig) {
				if cfg.Capacity != 10000 {
					t.Errorf("expected default Capacity 10000, got %d", cfg.Capacity)
				}
				if cfg.LeaseDuration != 30*time.Second {
					t.Errorf("expected default LeaseDuration 30s, got %v", cfg.LeaseDuration)
				}
				if cfg.MaxDeliveryAttempts != 3 {
					t.Errorf("expected default MaxDeliveryAttempts 3, got %d", cfg.MaxDeliveryAttempts)
				}
				if cfg.BatchSize != 10 {
					t.Errorf("expected default BatchSize 10, got %d", cfg.BatchSize)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all first
			for _, k := range keys {
				os.Unsetenv(k)
			}
			// Set test env
			for k, v := range tt.env {
				os.Setenv(k, v)
			}
			cfg := LoadQueueConfig()
			tt.validate(t, cfg)
		})
	}
}
