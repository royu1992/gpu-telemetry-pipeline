package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() {
	// Suppress Gin debug output from all tests in this file.
	gin.SetMode(gin.TestMode)
}

// TestNew_ReturnsEngine verifies that New returns a non-nil *gin.Engine.
func TestNew_ReturnsEngine(t *testing.T) {
	engine := New(1 << 20)
	if engine == nil {
		t.Fatal("New() returned nil engine")
	}
}

// TestNew_RunsInReleaseMode verifies that the engine is configured in release
// mode (Gin.Mode() == "release") after New() is called.
func TestNew_RunsInReleaseMode(t *testing.T) {
	// New() internally calls gin.SetMode(gin.ReleaseMode).
	New(1 << 20)
	if gin.Mode() != gin.ReleaseMode {
		t.Errorf("gin.Mode(): got %q, want %q", gin.Mode(), gin.ReleaseMode)
	}
}

// TestBodyLimitMiddleware_BelowLimit verifies that a request whose body is
// within the configured limit is passed through to the handler unchanged.
func TestBodyLimitMiddleware_BelowLimit(t *testing.T) {
	// Configure a 10-byte limit. The test request body is 5 bytes.
	const limit = 10
	engine := New(limit)

	// Register a simple POST endpoint that reads the body and echoes it.
	engine.POST("/echo", func(c *gin.Context) {
		// Read up to the limit; under the limit this succeeds fully.
		buf := make([]byte, 64)
		n, _ := c.Request.Body.Read(buf)
		c.String(http.StatusOK, string(buf[:n]))
	})

	body := "hello" // 5 bytes — within the 10-byte limit
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/echo", strings.NewReader(body))
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
	if w.Body.String() != body {
		t.Errorf("body: got %q, want %q", w.Body.String(), body)
	}
}

// TestBodyLimitMiddleware_AtExactLimit verifies that a request body of exactly
// maxBytes is accepted.
func TestBodyLimitMiddleware_AtExactLimit(t *testing.T) {
	const limit = 5
	engine := New(limit)

	engine.POST("/echo", func(c *gin.Context) {
		buf := make([]byte, 64)
		n, _ := c.Request.Body.Read(buf)
		c.String(http.StatusOK, string(buf[:n]))
	})

	// Body is exactly 5 bytes — equal to the limit.
	body := "abcde"
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/echo", strings.NewReader(body))
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
}

// TestBodyLimitMiddleware_ExceedsLimit verifies that http.MaxBytesReader is
// applied and that a handler reading beyond the limit receives an error. The
// middleware itself does not immediately reject the request, but the handler
// will get an error when it tries to read past the boundary.
func TestBodyLimitMiddleware_ExceedsLimit(t *testing.T) {
	const limit = 5
	engine := New(limit)

	// A handler that deliberately reads more than the limit and reports whether
	// an error occurred.
	engine.POST("/oversized", func(c *gin.Context) {
		// Try to read 1 MB — well beyond the 5-byte limit.
		buf := make([]byte, 1<<20)
		_, err := c.Request.Body.Read(buf)
		if err != nil {
			// The LimitedReader returns an error when the cap is exceeded.
			c.String(http.StatusRequestEntityTooLarge, "limited")
			return
		}
		c.String(http.StatusOK, "not limited")
	})

	// Send 10 bytes, but the limit is 5.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/oversized", strings.NewReader("0123456789"))
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status: got %d, want 413", w.Code)
	}
	if w.Body.String() != "limited" {
		t.Errorf("body: got %q, want \"limited\"", w.Body.String())
	}
}

// TestBodyLimitMiddleware_GetRequestNotAffected verifies that GET requests
// (which have no body) are served normally even with the body limit in place.
func TestBodyLimitMiddleware_GetRequestNotAffected(t *testing.T) {
	engine := New(1) // extreme 1-byte limit — should not affect GET

	engine.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
}
