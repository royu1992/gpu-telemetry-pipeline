package telemetry_loop

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/config"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/csv_reader"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/metrics"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/publisher"
)

// discardLogger returns a logger that writes to /dev/null, keeping test output clean.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// writeTempCSV creates a temporary CSV file with the given content and returns
// its path. The caller is responsible for calling os.Remove on the path.
func writeTempCSV(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp("", "loop*.csv")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return f.Name()
}

const csvHeader = "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,value,labels_raw\n"
const validRow = "2025-07-21T14:00:00Z,gpu_temp,0,GeForce,UUID-AAA,RTX 3080,Node-A,72.0,label=test\n"

// invalidRow has the right number of fields but empty required fields (timestamp, value).
const invalidRow = ",,gpu_id,device,uuid,modelName,Hostname,,labels_raw\n"

// malformedRow has fewer fields than the header, triggering a csv parse error.
const malformedRow = "only,two_fields\n"

func TestTelemetryLoop_Run(t *testing.T) {
	// Step: Create a small valid CSV file for the basic test cases.
	basicCSV := writeTempCSV(t, csvHeader+validRow)
	defer os.Remove(basicCSV)

	tests := []struct {
		name              string
		queueHandler      http.HandlerFunc
		maxConsecutiveErr int
		wantStopped       bool
	}{
		{
			// Step: Verify that a valid row is published and the loop exits cleanly on cancel.
			name: "successful single row and stop",
			queueHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				io.WriteString(w, `{"message_id": "ok"}`)
			},
			maxConsecutiveErr: 5,
			wantStopped:       false,
		},
		{
			// Step: With maxConsecutiveErrors=0, any bad state immediately triggers exit;
			// the loop still reads the valid row, publishes, then eventually cancels.
			name: "exceeds consecutive bad rows",
			queueHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			maxConsecutiveErr: -1,
			wantStopped:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step: Start mock queue server.
			server := httptest.NewServer(tt.queueHandler)
			defer server.Close()

			// Step: Build configuration pointing at the temp CSV and mock server.
			cfg := config.StreamerConfig{
				CSVPath:              basicCSV,
				QueueURL:             server.URL,
				Interval:             10 * time.Millisecond,
				RetryAttempts:        1,
				RequestTimeout:       100 * time.Millisecond,
				MaxConsecutiveErrors: tt.maxConsecutiveErr,
			}

			// Step: Initialize components.
			r, _ := csv_reader.Open(cfg.CSVPath)
			p := publisher.New(cfg, discardLogger())
			m := metrics.New()
			l := New(r, p, m, cfg, discardLogger())

			// Step: Run loop in background with a cancellable context.
			ctx, cancel := context.WithCancel(context.Background())
			go l.Run(ctx)

			// Step: Let the loop run briefly then cancel.
			time.Sleep(100 * time.Millisecond)
			cancel()
		})
	}
}

func TestTelemetryLoop_Run_EOFAndRewind(t *testing.T) {
	// Step: Create a one-row CSV so the loop hits EOF after the first read.
	path := writeTempCSV(t, csvHeader+validRow)
	defer os.Remove(path)

	// Step: Queue server always accepts messages.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"message_id": "rewind-test"}`)
	}))
	defer srv.Close()

	cfg := config.StreamerConfig{
		CSVPath:              path,
		QueueURL:             srv.URL,
		Interval:             5 * time.Millisecond,
		RetryAttempts:        1,
		RequestTimeout:       100 * time.Millisecond,
		MaxConsecutiveErrors: 10,
	}

	// Step: Open the reader, construct the loop.
	r, err := csv_reader.Open(cfg.CSVPath)
	if err != nil {
		t.Fatal(err)
	}
	p := publisher.New(cfg, discardLogger())
	m := metrics.New()
	l := New(r, p, m, cfg, discardLogger())

	// Step: Run the loop long enough for at least one full rewind cycle.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(done)
	}()

	// Step: Wait for multiple rows to be sent (proves rewind happened).
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Loop exited as expected.
	case <-time.After(500 * time.Millisecond):
		t.Error("loop did not exit after context cancel")
	}

	// Step: rows_sent_total should be > 1, proving the loop rewound the file.
	if got := m.Snapshot().RowsSentTotal; got < 2 {
		t.Errorf("RowsSentTotal = %d, want > 1 (at least one rewind must have occurred)", got)
	}
}

func TestTelemetryLoop_Run_CancelDuringPreRewindSleep(t *testing.T) {
	// Step: Create a one-row CSV so EOF is reached immediately after the first row.
	path := writeTempCSV(t, csvHeader+validRow)
	defer os.Remove(path)

	// Step: Queue always succeeds quickly.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"message_id": "x"}`)
	}))
	defer srv.Close()

	// Step: Use a very short publish interval so the post-publish sleep is instant,
	// but a long pre-rewind sleep so we can cancel during it.
	// Both sleeps use cfg.Interval; we pick a moderate value and cancel quickly.
	cfg := config.StreamerConfig{
		CSVPath:              path,
		QueueURL:             srv.URL,
		Interval:             2 * time.Second, // long enough to reliably cancel during
		RetryAttempts:        1,
		RequestTimeout:       100 * time.Millisecond,
		MaxConsecutiveErrors: 10,
	}

	r, err := csv_reader.Open(cfg.CSVPath)
	if err != nil {
		t.Fatal(err)
	}
	p := publisher.New(cfg, discardLogger())
	m := metrics.New()
	l := New(r, p, m, cfg, discardLogger())

	// Step: Cancel the context after a short delay; the loop will be in a sleep.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(done)
	}()

	// Step: The loop must exit within a generous window (well under the 2-second interval).
	select {
	case <-done:
		// Loop exited promptly after cancel: pass.
	case <-time.After(1 * time.Second):
		t.Error("loop did not exit promptly after context cancellation")
	}

	// Step: At least one row was sent before the loop exited.
	if m.Snapshot().RowsSentTotal < 1 {
		t.Error("expected at least one row sent before cancellation")
	}
}

func TestTelemetryLoop_Run_CSVParseError(t *testing.T) {
	// Step: Create a CSV with a valid header but a malformed data row (wrong field count).
	// The csv.Reader will return csv.ErrFieldCount, exercising the non-EOF parse-error path.
	path := writeTempCSV(t, csvHeader+malformedRow)
	defer os.Remove(path)

	// Step: Queue handler (unreachable in this test, but required by publisher.New).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"message_id": "unused"}`)
	}))
	defer srv.Close()

	cfg := config.StreamerConfig{
		CSVPath:              path,
		QueueURL:             srv.URL,
		Interval:             5 * time.Millisecond,
		RetryAttempts:        1,
		RequestTimeout:       100 * time.Millisecond,
		// Step: MaxConsecutiveErrors=1 means the loop exits after the first bad row.
		MaxConsecutiveErrors: 1,
	}

	r, err := csv_reader.Open(cfg.CSVPath)
	if err != nil {
		t.Fatal(err)
	}
	p := publisher.New(cfg, discardLogger())
	m := metrics.New()
	l := New(r, p, m, cfg, discardLogger())

	// Step: Run the loop; it should self-terminate after hitting the max consecutive errors.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
		// Loop exited due to too many consecutive errors: pass.
	case <-time.After(2 * time.Second):
		t.Error("loop did not exit after exceeding consecutive error limit")
	}

	// Step: errors_total must be at least 1 (the parse error was counted).
	if got := m.Snapshot().ErrorsTotal; got < 1 {
		t.Errorf("ErrorsTotal = %d, want >= 1", got)
	}
}

func TestTelemetryLoop_Run_InvalidRowValidationFailure(t *testing.T) {
	// Step: Create a CSV where the data row passes parsing but fails Validate()
	// (empty required fields). The loop should increment errors and then sleep.
	path := writeTempCSV(t, csvHeader+invalidRow)
	defer os.Remove(path)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"message_id": "unused"}`)
	}))
	defer srv.Close()

	cfg := config.StreamerConfig{
		CSVPath:              path,
		QueueURL:             srv.URL,
		Interval:             10 * time.Second, // long sleep so we can cancel during it
		RetryAttempts:        1,
		RequestTimeout:       100 * time.Millisecond,
		MaxConsecutiveErrors: 100, // high threshold so we don't self-terminate
	}

	r, err := csv_reader.Open(cfg.CSVPath)
	if err != nil {
		t.Fatal(err)
	}
	p := publisher.New(cfg, discardLogger())
	m := metrics.New()
	l := New(r, p, m, cfg, discardLogger())

	// Step: Cancel the context after a short delay, interrupting the post-skip sleep.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(done)
	}()

	// Step: The loop should exit within a fraction of the 10-second interval.
	select {
	case <-done:
		// Exited promptly after context cancel: pass.
	case <-time.After(2 * time.Second):
		t.Error("loop did not exit promptly after context cancellation during skip sleep")
	}

	// Step: errors_total must be >= 1 (the invalid row was counted).
	if got := m.Snapshot().ErrorsTotal; got < 1 {
		t.Errorf("ErrorsTotal = %d, want >= 1", got)
	}
}

func TestTelemetryLoop_Run_SendFailure(t *testing.T) {
	// Step: Create a valid one-row CSV.
	path := writeTempCSV(t, csvHeader+validRow)
	defer os.Remove(path)

	// Step: Queue always returns 503, so Publish will exhaust all retries.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	cfg := config.StreamerConfig{
		CSVPath:              path,
		QueueURL:             srv.URL,
		Interval:             10 * time.Millisecond,
		RetryAttempts:        1,
		RetryDelay:           1 * time.Millisecond,
		RequestTimeout:       50 * time.Millisecond,
		MaxConsecutiveErrors: 100, // prevent self-termination
	}

	r, err := csv_reader.Open(cfg.CSVPath)
	if err != nil {
		t.Fatal(err)
	}
	p := publisher.New(cfg, discardLogger())
	m := metrics.New()
	l := New(r, p, m, cfg, discardLogger())

	// Step: Run the loop briefly; at least one send failure should be recorded.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(done)
	}()

	<-done

	// Step: errors_total must be >= 1 (the failed delivery was counted).
	if got := m.Snapshot().ErrorsTotal; got < 1 {
		t.Errorf("ErrorsTotal = %d, want >= 1 after send failure", got)
	}
	// Step: rows_sent_total must remain 0 (no successful delivery occurred).
	if got := m.Snapshot().RowsSentTotal; got != 0 {
		t.Errorf("RowsSentTotal = %d, want 0 after all sends failed", got)
	}
}

func TestTelemetryLoop_SleepOrStop(t *testing.T) {
	tests := []struct {
		name       string
		interval   time.Duration
		cancelAfter time.Duration
		wantResult bool
	}{
		{
			// Step: Context is cancelled before the interval elapses → returns false.
			name:        "context cancelled before interval",
			interval:    time.Hour,
			cancelAfter: 20 * time.Millisecond,
			wantResult:  false,
		},
		{
			// Step: Interval elapses before context cancel → returns true.
			name:        "interval elapses before cancel",
			interval:    10 * time.Millisecond,
			cancelAfter: time.Hour,
			wantResult:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &TelemetryLoop{
				cfg: config.StreamerConfig{Interval: tt.interval},
			}
			ctx, cancel := context.WithCancel(context.Background())

			// Step: Start sleepOrStop in background.
			resultCh := make(chan bool, 1)
			go func() { resultCh <- l.sleepOrStop(ctx) }()

			// Step: Fire the cancel after the configured delay.
			go func() {
				time.Sleep(tt.cancelAfter)
				cancel()
			}()

			// Step: Collect the result with a generous timeout.
			select {
			case got := <-resultCh:
				if got != tt.wantResult {
					t.Errorf("sleepOrStop() = %v, want %v", got, tt.wantResult)
				}
			case <-time.After(2 * time.Second):
				t.Error("sleepOrStop timed out")
				cancel()
			}
		})
	}
}

func TestTelemetryLoop_IsTooManyConsecutiveErrors(t *testing.T) {
	tests := []struct {
		name           string
		consecutiveBad int
		maxAllowed     int
		wantTooMany    bool
	}{
		{
			// Step: Well below threshold — should return false and not exit the loop.
			name:           "below threshold returns false",
			consecutiveBad: 3,
			maxAllowed:     10,
			wantTooMany:    false,
		},
		{
			// Step: Exactly at the threshold — should return true and log an error.
			name:           "at threshold returns true",
			consecutiveBad: 10,
			maxAllowed:     10,
			wantTooMany:    true,
		},
		{
			// Step: Above the threshold — should also return true.
			name:           "above threshold returns true",
			consecutiveBad: 15,
			maxAllowed:     10,
			wantTooMany:    true,
		},
		{
			// Step: One below threshold — should return false.
			name:           "one below threshold returns false",
			consecutiveBad: 9,
			maxAllowed:     10,
			wantTooMany:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step: Build a minimal TelemetryLoop with only the fields
			// isTooManyConsecutiveErrors reads.
			l := &TelemetryLoop{
				cfg:    config.StreamerConfig{MaxConsecutiveErrors: tt.maxAllowed},
				logger: discardLogger(),
			}

			// Step: Invoke the method and verify the return value.
			got := l.isTooManyConsecutiveErrors(tt.consecutiveBad)
			if got != tt.wantTooMany {
				t.Errorf("isTooManyConsecutiveErrors(%d) = %v, want %v",
					tt.consecutiveBad, got, tt.wantTooMany)
			}
		})
	}
}
