// @title           GPU Telemetry Pipeline — Streamer API
// @version         1.0
// @description     Internal health and observability API for the Telemetry Streamer service.
// @host            localhost:8081
// @BasePath        /
// @schemes         http
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/config"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/csv_reader"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/metrics"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/publisher"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/server"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/streamer/telemetry_loop"
)

func main() {
	// Initialise a structured JSON logger writing to stdout. Kubernetes log
	// aggregators parse JSON natively, making fields
	// like "err" and "addr" searchable without additional parsing rules.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Load all configuration from environment variables, falling back to the
	// compiled-in defaults for any variable that is absent or invalid.
	cfg := config.Load()
	logger.Info("starting streamer",
		"csv_path", cfg.CSVPath,
		"queue_url", cfg.QueueURL,
		"interval_ms", cfg.Interval.Milliseconds(),
		"retry_attempts", cfg.RetryAttempts,
		"port", cfg.Port,
	)

	// Open the CSV file and build the column index. This is the first
	// operation that touches the file system, so any misconfiguration
	// (wrong path, missing file, wrong schema) will surface here rather
	// than silently at runtime.
	r, err := csv_reader.Open(cfg.CSVPath)
	if err != nil {
		logger.Error("failed to open CSV file", "path", cfg.CSVPath, "err", err)
		os.Exit(1)
	}
	// Defer Close so the OS file handle is released on any exit path,
	// including panic recovery and the graceful shutdown sequence below.
	defer r.Close()

	// Initialise the shared observability counters. The loop goroutine writes
	// to these and the Gin server goroutine reads them via atomic operations.
	m := metrics.New()

	// Construct the HTTP publisher. It holds a shared http.Client whose transport
	// reuses idle TCP connections across all row deliveries.
	p := publisher.New(cfg, logger)

	// Build the telemetry loop. It wires together the reader, publisher, and
	// metrics, and owns the consecutive-bad-row safety counter.
	l := telemetry_loop.New(r, p, m, cfg, logger)

	// Build the health/ready/metrics HTTP handler. The handler starts in the
	// "not ready" state; SetReady(true) is called after the loop goroutine
	// is confirmed running so the readiness probe only returns 200 once the
	// streamer is genuinely active.
	h := server.NewHandler(m)

	// Construct the Gin engine with the standard middleware stack (recovery,
	// logger, body-size limit). The 4096-byte cap is generous for GET-only
	// endpoints but provides a safety net if routes are added in the future.
	ginEngine := server.New(4096)

	// Wire the three operational endpoints onto the engine.
	h.RegisterRoutes(ginEngine)

	// Configure the net/http server. ReadTimeout and WriteTimeout are kept
	// small because all three endpoints respond immediately — there is no
	// long-polling on the streamer's health server.
	ginSrv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      ginEngine,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start the Gin health/metrics server in a background goroutine so it
	// remains responsive even when the telemetry loop is blocked on a slow POST.
	go func() {
		logger.Info("health server listening", "addr", ginSrv.Addr)

		if err := ginSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Any error other than the expected ErrServerClosed means the
			// server failed outside the normal shutdown path.
			logger.Error("health server error", "err", err)
			os.Exit(1)
		}
	}()

	// Create a cancellable context that controls the telemetry loop lifetime.
	// Cancelling this context is the clean shutdown signal for the loop.
	loopCtx, cancelLoop := context.WithCancel(context.Background())

	// Channel closed by the loop goroutine when it returns. Used below to
	// wait for the loop to finish draining its in-flight request.
	loopDone := make(chan struct{})

	// Start the telemetry loop goroutine. This is the only goroutine that reads
	// the CSV and delivers rows to the queue.
	go func() {
		// Close loopDone when Run() returns so main() can synchronise on it.
		defer close(loopDone)
		l.Run(loopCtx)
	}()

	// Mark the service as ready now that the loop is running. From this point
	// the Kubernetes readiness probe will start returning 200 OK, and the load
	// balancer will route traffic to this pod.
	h.SetReady(true)
	logger.Info("streamer ready")

	// Block until SIGTERM or SIGINT is delivered. The channel is buffered by 1
	// so the signal package never blocks when delivering the signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	logger.Info("shutdown signal received")

	// Lower the readiness flag immediately so Kubernetes stops routing new
	// traffic to this pod before we begin stopping the loop.
	h.SetReady(false)

	// Cancel the loop context. The loop will finish its current in-flight send
	// (or retry sleep) and then return from Run().
	cancelLoop()

	// Create the overall shutdown deadline (the "safety valve"). If the loop
	// and server have not finished within ShutdownGrace seconds, we log a
	// fatal error and let the process exit anyway. This prevents zombie pods
	// that block Kubernetes rollouts.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancelShutdown()

	// Wait for the telemetry loop goroutine to finish. This gives any
	// in-flight POST request up to one RequestTimeout to complete naturally.
	select {
	case <-loopDone:
		logger.Info("telemetry loop stopped cleanly")
	case <-shutdownCtx.Done():
		logger.Error("telemetry loop did not stop within the grace period",
			"grace_seconds", cfg.ShutdownGrace.Seconds(),
		)
	}

	// Shut down the Gin health/metrics server gracefully. In-flight requests
	// (realistically none at this point) are given until the remaining
	// shutdownCtx deadline to complete before connections are forcibly closed.
	if err := ginSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("health server shutdown error", "err", err)
	}

	logger.Info("streamer stopped")
}
