package publisher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/config"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/csv_reader"
)

func TestPublisher_Publish(t *testing.T) {
	// Standard test row
	row := csv_reader.CSVRow{
		Timestamp:  "2025-07-18T12:00:00Z",
		MetricName: "gpu_temp",
		GpuID:      "0",
		Device:     "GeForce",
		UUID:       "UUID-123",
		ModelName:  "RTX 3080",
		Hostname:   "Node-A",
		Value:      "70.5",
		LabelsRaw:  "env=prod",
	}

	tests := []struct {
		name           string
		handler        http.HandlerFunc
		retryAttempts  int
		requestTimeout time.Duration
		wantErr        bool
	}{
		{
			name: "successful publish first try",
			handler: func(w http.ResponseWriter, r *http.Request) {
				// Step: Validate request content type
				if r.Header.Get("Content-Type") != "application/json" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				// Step: Validate JSON body
				var payload map[string]string
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if payload["hostname"] != "Node-A" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				io.WriteString(w, `{"message_id": "msg-123"}`)
			},
			retryAttempts:  3,
			requestTimeout: 100 * time.Millisecond,
			wantErr:        false,
		},
		{
			name: "success after retry",
			handler: (func() http.HandlerFunc {
				attempts := 0
				return func(w http.ResponseWriter, r *http.Request) {
					attempts++
					if attempts == 1 {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					w.WriteHeader(http.StatusOK)
					io.WriteString(w, `{"message_id": "msg-retry"}`)
				}
			})(),
			retryAttempts:  3,
			requestTimeout: 100 * time.Millisecond,
			wantErr:        false,
		},
		{
			name: "fail after all retries",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			retryAttempts:  2,
			requestTimeout: 100 * time.Millisecond,
			wantErr:        true,
		},
		{
			name: "request timeout",
			handler: func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(200 * time.Millisecond) // Longer than requestTimeout
				w.WriteHeader(http.StatusOK)
			},
			retryAttempts:  1,
			requestTimeout: 50 * time.Millisecond,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step: Start local test server
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			// Step: Build config with test server URL
			cfg := config.StreamerConfig{
				QueueURL:       server.URL,
				RetryAttempts:  tt.retryAttempts,
				RetryDelay:     5 * time.Millisecond,
				RequestTimeout: tt.requestTimeout,
			}

			// Step: Initialize Publisher
			p := New(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))

			// Step: Perform Publish
			err := p.Publish(context.Background(), row)

			// Step: Verify result
			if (err != nil) != tt.wantErr {
				t.Errorf("Publisher.Publish() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPublisher_Publish_ValidationErrors(t *testing.T) {
	t.Run("invalid json response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, `invalid-json`)
		}))
		defer server.Close()

		p := New(config.StreamerConfig{QueueURL: server.URL, RetryAttempts: 1, RequestTimeout: time.Second}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
		err := p.Publish(context.Background(), csv_reader.CSVRow{})
		if err != nil {
			t.Errorf("expected no error for invalid response body, got %v", err)
		}
	})

	t.Run("empty message_id response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, `{"message_id": ""}`)
		}))
		defer server.Close()

		p := New(config.StreamerConfig{QueueURL: server.URL, RetryAttempts: 1, RequestTimeout: time.Second}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
		err := p.Publish(context.Background(), csv_reader.CSVRow{})
		if err != nil {
			t.Errorf("expected no error for empty message_id, got %v", err)
		}
	})

	t.Run("request creation error", func(t *testing.T) {
		// NewRequestWithContext fails if URL is invalid (e.g. contains space)
		p := New(config.StreamerConfig{QueueURL: "http://invalid URL", RetryAttempts: 1}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
		err := p.Publish(context.Background(), csv_reader.CSVRow{})
		if err == nil {
			t.Error("expected error for invalid URL, got nil")
		}
	})
}

func TestPublisher_ShutdownDuringRetry(t *testing.T) {
	// Step: Create server that always fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	cfg := config.StreamerConfig{
		QueueURL:       server.URL,
		RetryAttempts:  10,
		RetryDelay:     500 * time.Millisecond,
		RequestTimeout: 50 * time.Millisecond,
	}

	p := New(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())

	// Step: Start publish in background
	errChan := make(chan error, 1)
	go func() {
		errChan <- p.Publish(ctx, csv_reader.CSVRow{})
	}()

	// Step: Wait for first failure then cancel context
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Step: Verify early exit
	err := <-errChan
	if err == nil || !strings.Contains(err.Error(), "cancelled during retry") {
		t.Errorf("expected cancellation error, got %v", err)
	}
}
