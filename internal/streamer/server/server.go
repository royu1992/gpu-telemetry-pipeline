package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// New creates a Gin engine configured for production use.
// It registers the recovery and logger middleware and enforces a per-request
// body size limit to guard against oversized payloads.
// This factory follows the same pattern used by the message-queue service.
func New(maxBodyBytes int64) *gin.Engine {
	// Run Gin in release mode to suppress debug output and activate
	// production-grade performance paths inside the framework.
	gin.SetMode(gin.ReleaseMode)

	// gin.New() creates an engine with no pre-attached middleware, giving us
	// full control over the order of the middleware stack.
	r := gin.New()

	// Recover from panics in any handler and return 500 instead of crashing.
	r.Use(gin.Recovery())
	// Log each request with method, path, status code, and latency.
	r.Use(gin.Logger())
	// Enforce a per-request body size cap before any handler reads the body.
	r.Use(bodyLimitMiddleware(maxBodyBytes))

	return r
}

// bodyLimitMiddleware wraps each incoming request body with an io.LimitedReader
// so handlers cannot consume more than maxBytes from the wire. This protects
// the process against unexpectedly large or malicious inbound payloads.
// Note: the streamer's own server only serves GET endpoints, but having the
// limit applied globally ensures safety if routes are added in the future.
func bodyLimitMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Wrap the incoming body. Any read beyond maxBytes returns an error,
		// protecting the process before the payload reaches any JSON parser.
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)

		// Pass control to the next handler in the middleware chain.
		c.Next()
	}
}
