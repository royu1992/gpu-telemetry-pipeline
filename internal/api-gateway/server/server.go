package server

import (
	"net/http"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// New creates a production-ready Gin engine for the API Gateway.
//
// Middleware stack (in order):
//  1. gin.Recovery – converts panics into HTTP 500 responses so the process
//     does not crash on unexpected handler errors.
//  2. gin.Logger – writes one structured log line per request.
//  3. CORS – applies the configured allowed-origins policy so browser-based
//     dashboards can query the gateway directly.
//  4. bodyLimitMiddleware – enforces the per-request body-size cap before any
//     handler attempts to decode JSON, preventing memory exhaustion from
//     maliciously large or malformed payloads.
//
// corsOrigins is a comma-separated list of allowed CORS origins, e.g.
// "https://grafana.corp,https://dashboard.corp". An asterisk ("*") allows all
// origins and is suitable only for development or fully-public deployments.
func New(corsOrigins string, maxBodyBytes int64) *gin.Engine {
	// Run in release mode to suppress debug-level output and maximise throughput.
	gin.SetMode(gin.ReleaseMode)

	// gin.New() gives us a blank engine with no middleware attached.
	r := gin.New()

	// Recover from handler panics; converts panic → 500 without crashing.
	r.Use(gin.Recovery())

	// Structured per-request logging.
	r.Use(gin.Logger())

	// Apply CORS headers based on the operator-configured origins.
	r.Use(corsMiddleware(corsOrigins))

	// Enforce body-size cap; must come after CORS so preflight OPTIONS
	// requests (which have no body) are not affected unnecessarily.
	r.Use(bodyLimitMiddleware(maxBodyBytes))

	return r
}

// corsMiddleware builds a gin-contrib/cors handler from the operator-supplied
// origins string. The origins string is split on commas and each token is
// trimmed. A single asterisk ("*") is treated as the wildcard "allow all".
func corsMiddleware(origins string) gin.HandlerFunc {
	// Parse the comma-separated origins into a deduplicated slice.
	parts := strings.Split(origins, ",")
	allowed := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			allowed = append(allowed, p)
		}
	}

	// gin-contrib/cors is configured with a Config struct. We allow common
	// HTTP methods and headers that JSON-over-HTTP API clients use.
	cfg := cors.Config{
		AllowOrigins:     allowed,
		AllowMethods:     []string{"GET", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: false,
	}

	// If the operator configured a wildcard, allow all origins rather than
	// listing "*" literally (gin-contrib/cors handles this via AllowAllOrigins).
	if len(allowed) == 1 && allowed[0] == "*" {
		cfg.AllowAllOrigins = true
		cfg.AllowOrigins = nil
	}

	return cors.New(cfg)
}

// bodyLimitMiddleware wraps each request body with an http.MaxBytesReader so
// no handler can consume more than maxBytes, protecting against oversized or
// malicious payloads.
func bodyLimitMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Wrap the body; reads beyond maxBytes return an error to the decoder.
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		// Pass control to the next handler in the chain.
		c.Next()
	}
}
