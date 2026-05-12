// @title           GPU Telemetry Pipeline API
// @version         1.0
// @description     REST API for querying GPU telemetry data collected from an AI cluster.
// @host            localhost:9090
// @BasePath        /api/v1
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

	"github.com/royu1992/gpu-telemetry-pipeline/internal/api-gateway/cache"
	gconfig "github.com/royu1992/gpu-telemetry-pipeline/internal/api-gateway/config"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/api-gateway/metrics"
	gserver "github.com/royu1992/gpu-telemetry-pipeline/internal/api-gateway/server"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/store"
)

func main() {
	// Initialise a structured JSON logger writing to stdout. Kubernetes log
	// aggregators parse JSON natively, making fields such as "port" and "err"
	// directly queryable without additional parsing rules.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Load all configuration from environment variables, applying compiled-in
	// defaults for any variable that is absent or contains an invalid value.
	cfg := gconfig.Load()
	logger.Info("starting api-gateway",
		"port", cfg.Port,
		"db_max_conns", cfg.DBMaxConns,
		"cache_ttl_gpus", cfg.CacheTTLGPUs,
		"max_response_rows", cfg.MaxResponseRows,
		"cors_origins", cfg.CORSOrigins,
	)

	// Initialise the shared observability counters. The Gin handler goroutines
	// write to these using atomic operations; no mutex is required for reads.
	m := metrics.New()

	// ── Startup sequence ──────────────────────────────────────────────────────
	// Step 1: Establish the Postgres connection pool.
	// The connect deadline is enforced inside store.New via context.WithTimeout.
	dbStore, err := store.New(context.Background(), store.Config{
		DatabaseURL:      cfg.DatabaseURL,
		DBMaxConns:       cfg.DBMaxConns,
		DBConnectTimeout: cfg.DBConnectTimeout,
	})
	if err != nil {
		// A fatal connection failure during startup causes the process to exit
		// so that the orchestrator can restart it (e.g., until Postgres is ready).
		logger.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}
	// Defer Close so the connection pool drains cleanly on every exit path.
	defer dbStore.Close()
	logger.Info("database connection established")

	// Step 2: Build the GPU-list cache backed by the store.
	// The cache is cold on startup; the first GET /api/v1/gpus request will
	// trigger a database query and prime the cache.
	gpuCache := cache.New(dbStore, cfg.CacheTTLGPUs)

	// Step 3: Build the HTTP handler, wiring in all dependencies.
	h := gserver.NewHandler(dbStore, gpuCache, m, cfg.DBQueryTimeout, cfg.MaxResponseRows)

	// Step 4: Construct the Gin engine with the standard middleware stack
	// (recovery, logger, CORS, body-size limit). We cap the request body at
	// 4 KiB — the gateway serves GET-only API endpoints with no request bodies,
	// so this is purely a defence-in-depth safety net.
	ginEngine := gserver.New(cfg.CORSOrigins, 4096)
	h.RegisterRoutes(ginEngine)

	// Step 5: Configure the net/http server with conservative timeouts.
	// ReadTimeout prevents slow-loris style attacks; WriteTimeout caps the
	// total time to stream a response back to the client.
	ginSrv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      ginEngine,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Step 6: Start the HTTP server in a background goroutine so the main
	// goroutine can proceed to signal handling and graceful shutdown.
	go func() {
		logger.Info("api-gateway listening", "addr", ginSrv.Addr)
		if err := ginSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Any error other than the expected ErrServerClosed on shutdown
			// means the server failed unexpectedly.
			logger.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()

	// Step 7: Signal readiness so the /ready probe returns HTTP 200.
	// SetReady is called after the server goroutine is launched so the
	// /ready endpoint can be polled by the orchestrator during startup.
	h.SetReady(true)
	logger.Info("api-gateway ready")

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	// Block until we receive SIGTERM (Kubernetes pod eviction / rolling deploy)
	// or SIGINT (Ctrl-C in local development).
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	logger.Info("shutdown signal received")

	// Mark the gateway as no longer ready so load-balancers stop routing
	// traffic before we begin draining in-flight requests.
	h.SetReady(false)

	// Give in-flight HTTP requests up to ShutdownGrace to complete before
	// closing the listener. Requests that do not finish within the window are
	// abandoned and the client receives a connection reset.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()

	if err := ginSrv.Shutdown(shutdownCtx); err != nil {
		// Log but do not exit with a non-zero code: the deferred dbStore.Close()
		// still needs to run to cleanly drain the connection pool.
		logger.Error("http server shutdown error", "err", err)
	}

	logger.Info("api-gateway stopped")
}
