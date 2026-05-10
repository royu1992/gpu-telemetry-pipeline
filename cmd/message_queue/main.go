package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	queuecfg "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/config"
	message_queue "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/model"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/queue"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/server"
)

func main() {
	// Initialise a structured JSON logger writing to stdout. Kubernetes log
	// aggregators parse JSON natively.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Load all configuration from environment variables, falling back to
	// compiled-in defaults for any variable that is absent or invalid.
	cfg := queuecfg.LoadQueueConfig()
	logger.Info("starting message-queue",
		"port", cfg.Port,
		"capacity", cfg.Capacity,
		"lease_duration", cfg.LeaseDuration,
		"max_delivery_attempts", cfg.MaxDeliveryAttempts,
	)

	// Initialise the shared metrics counters used by the buffer and handler.
	m := message_queue.NewMetrics()

	// Allocate the fixed-size ring buffer that stores all in-flight messages.
	buf := queue.NewBuffer(cfg.Capacity, cfg.LeaseDuration, cfg.MaxDeliveryAttempts, m)

	// Create a dedicated context for the lease reaper so it can be stopped
	// independently from the HTTP server during graceful shutdown.
	reaperCtx, cancelReaper := context.WithCancel(context.Background())
	defer cancelReaper()

	// Run the lease reaper in the background. It periodically scans for
	// in-flight messages whose lease has expired and requeues or drops them.
	go queue.RunReaper(reaperCtx, buf, cfg.LeaseReaperInterval, logger)

	// Construct the HTTP handler with all six route registrations.
	mqHandler := queue.NewMessageQueueHandler(buf, m, cfg)

	// Create the Gin engine with the standard middleware stack (recovery, logger,
	// body-size limit).
	r := server.New(cfg.MaxRequestBodySize)

	// Wire all message-queue routes onto the router.
	mqHandler.RegisterRoutes(r)

	// Configure the net/http server with tuned read/write/idle timeouts.
	// WriteTimeout must exceed LongPollTimeout so the server does not close
	// long-poll connections before the response can be written.
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	}

	// Start listening in a goroutine so the main goroutine can block on the
	// OS signal channel below.
	go func() {
		logger.Info("listening", "addr", srv.Addr)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Any error other than http.ErrServerClosed means the server failed
			// outside the normal shutdown path, so terminate the process.
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// Block until SIGTERM or SIGINT arrives. The channel is buffered by 1 so
	// the signal package never blocks when delivering the signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	logger.Info("shutdown signal received")

	// Lower the readiness flag and reject new publishes so the load balancer
	// stops routing traffic to this pod before we begin draining.
	mqHandler.SetClosing()

	// Unblock any goroutines waiting inside Publish or Consume so they return
	// ErrClosing and their HTTP handlers can respond and release connections.
	buf.Close()

	// Give in-flight HTTP requests time to complete. After the grace period,
	// Shutdown closes any remaining idle connections forcefully.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
	defer cancel()

	// Shutdown the HTTP server gracefully, allowing in-flight requests to complete
	// before closing idle connections. If the server fails to shutdown cleanly,
	// log the error but do not terminate with a non-zero exit code since the
	// process is already in the shutdown path.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "err", err)
	}

	logger.Info("message-queue stopped")
}
