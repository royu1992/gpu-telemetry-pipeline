// @title           GPU Telemetry Pipeline — Collector API
// @version         1.0
// @description     Internal health and observability API for the Telemetry Collector service.
// @host            localhost:8082
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

	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/config"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/consumer"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/metrics"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/collector/server"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/store"
)

func main() {
	// Initialise a structured JSON logger writing to stdout. Kubernetes log
	// aggregators parse JSON natively, making fields like "err" and "rows"
	// searchable without additional parsing rules.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Load all configuration from environment variables, falling back to the
	// compiled-in defaults for any variable that is absent or invalid.
	cfg := config.Load()
	logger.Info("starting collector",
		"port", cfg.Port,
		"queue_url", cfg.QueueURL,
		"batch_size", cfg.BatchSize,
		"long_poll_timeout", cfg.LongPollTimeout,
		"db_max_conns", cfg.DBMaxConns,
	)

	// Initialise the shared observability counters. The consumption loop goroutine
	// writes to these and the Gin server goroutine reads them via atomic operations,
	// so no mutex is required.
	m := metrics.New()

	// Build the health/ready/metrics HTTP handler. The handler starts in the
	// "not ready" state; SetReady(true) is called only after the DB connection,
	// migration, and loop goroutine are all confirmed healthy.
	h := server.NewHandler(m)

	// Construct the Gin engine with the standard middleware stack (recovery,
	// logger, body-size limit). The 4096-byte cap is generous for GET-only
	// endpoints but acts as a safety net if routes are added in the future.
	ginEngine := server.New(4096)

	// Wire the three operational endpoints onto the engine.
	h.RegisterRoutes(ginEngine)

	// Configure the net/http server. ReadTimeout and WriteTimeout are kept
	// small because all three endpoints respond immediately — there is no
	// long-polling on the collector's health server.
	ginSrv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      ginEngine,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start the Gin health/metrics server in a background goroutine so it
	// remains responsive during DB startup and while the consumption loop runs.
	go func() {
		logger.Info("health server listening", "addr", ginSrv.Addr)
		if err := ginSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Any error other than the expected ErrServerClosed means the
			// server failed outside the normal shutdown path.
			logger.Error("health server error", "err", err)
			os.Exit(1)
		}
	}()

	// ---- Startup sequence: DB → migration → loop → ready ----
	//
	// Readiness is only declared after all three steps succeed, ensuring
	// that Kubernetes does not route traffic to a pod that cannot yet serve.

	// Step 1: Establish the Postgres connection pool.
	// The connect timeout is enforced inside store.New via context.WithTimeout.
	dbStore, err := store.New(context.Background(), store.Config{
		DatabaseURL:      cfg.DatabaseURL,
		DBMaxConns:       cfg.DBMaxConns,
		DBConnectTimeout: cfg.DBConnectTimeout,
	})
	if err != nil {
		logger.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}
	// Defer Close so the pool drains cleanly on any exit path.
	defer dbStore.Close()
	logger.Info("database connection established")

	// Step 2: Run auto-migration to ensure the gpu_metrics table exists.
	// CREATE TABLE IF NOT EXISTS is idempotent — safe to run on every startup.
	if err := dbStore.Migrate(context.Background()); err != nil {
		logger.Error("failed to run database migration", "err", err)
		os.Exit(1)
	}
	logger.Info("database migration complete")

	// Step 3: Construct the consumer and launch the consumption loop goroutine.
	// The consumer derives its consumer_id from the OS hostname (Pod name),
	// falling back to a random UUID if the hostname is unavailable.
	c := consumer.New(cfg, dbStore, m, logger)

	// loopCtx is the lifetime context for the consumption loop. Cancelling it
	// is the clean shutdown signal that causes Run() to return.
	loopCtx, cancelLoop := context.WithCancel(context.Background())

	// loopDone is closed by the loop goroutine when Run() returns, allowing
	// main to synchronise on it during graceful shutdown.
	loopDone := make(chan struct{})

	go func() {
		// Signal completion regardless of how Run() returns.
		defer close(loopDone)
		c.Run(loopCtx)
	}()

	// All three startup steps succeeded — declare the pod ready. From this
	// point Kubernetes will route traffic and the readiness probe returns 200.
	h.SetReady(true)
	logger.Info("collector ready")

	// ---- Wait for shutdown signal ----

	// Block until SIGTERM or SIGINT is delivered. The channel is buffered by 1
	// so the signal package never blocks when delivering the signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	logger.Info("shutdown signal received")

	// Lower the readiness flag immediately so Kubernetes stops routing new
	// traffic to this pod before we begin draining the consumption loop.
	h.SetReady(false)

	// Cancel the loop context. Run() will finish the current batch and ACK
	// call (or detect cancellation mid-call) and then return.
	cancelLoop()

	// Create the overall shutdown deadline. If the loop and health server have
	// not finished within ShutdownGrace, we log a fatal error and allow the
	// process to exit. This prevents zombie pods that block Kubernetes rollouts.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancelShutdown()

	// Wait for the consumption loop to finish its current batch and return.
	select {
	case <-loopDone:
		logger.Info("consumption loop stopped cleanly")
	case <-shutdownCtx.Done():
		logger.Error("consumption loop did not stop within grace period",
			"grace", cfg.ShutdownGrace,
		)
	}

	// Shut down the Gin health server. In-flight requests (realistically none
	// at this point) are given until the remaining shutdownCtx deadline to
	// complete before connections are forcibly closed.
	if err := ginSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("health server shutdown error", "err", err)
	}

	// dbStore.Close() is deferred above and will be called here, draining the
	// connection pool after the consumption loop has fully stopped.
	logger.Info("collector stopped")
}
