package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() {
	// Suppress Gin debug/log output from all tests in this package.
	gin.SetMode(gin.TestMode)
}

// ─── New ─────────────────────────────────────────────────────────────────────

// TestNew_ReturnsEngine verifies that New returns a non-nil *gin.Engine.
func TestNew_ReturnsEngine(t *testing.T) {
	// Act.
	engine := New("*", 1<<20)

	// Assert: a nil engine would cause a nil-pointer dereference on the first
	// HTTP call, so we treat it as a fatal misconfiguration.
	if engine == nil {
		t.Fatal("New() returned nil engine")
	}
}

// TestNew_RunsInReleaseMode verifies that Gin is set to release mode after
// New() returns. Release mode suppresses verbose debug output in production.
func TestNew_RunsInReleaseMode(t *testing.T) {
	// New() calls gin.SetMode(gin.ReleaseMode) internally.
	New("*", 1<<20)
	if gin.Mode() != gin.ReleaseMode {
		t.Errorf("gin.Mode() = %q, want %q", gin.Mode(), gin.ReleaseMode)
	}
}

// ─── bodyLimitMiddleware ─────────────────────────────────────────────────────

// TestBodyLimitMiddleware_BelowLimit verifies that a request whose body is
// within the configured byte limit is forwarded to the handler unchanged.
func TestBodyLimitMiddleware_BelowLimit(t *testing.T) {
	// Arrange: 10-byte limit; test body is 5 bytes.
	const limit = 10
	engine := New("*", limit)
	engine.POST("/echo", func(c *gin.Context) {
		buf := make([]byte, 64)
		n, _ := c.Request.Body.Read(buf)
		c.String(http.StatusOK, string(buf[:n]))
	})

	body := "hello" // 5 bytes — within the limit.
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

// TestBodyLimitMiddleware_ExceedsLimit verifies that a request body larger
// than the limit is rejected (the read returns an error inside the handler).
func TestBodyLimitMiddleware_ExceedsLimit(t *testing.T) {
	// Arrange: 5-byte limit; test body is 10 bytes.
	const limit = 5
	engine := New("*", limit)
	engine.POST("/echo", func(c *gin.Context) {
		// Attempt to read 64 bytes; the LimitedReader will return an error
		// after the first 5.
		buf := make([]byte, 64)
		_, readErr := c.Request.Body.Read(buf)
		if readErr != nil {
			// Signal that the limit was triggered correctly.
			c.Status(http.StatusRequestEntityTooLarge)
			return
		}
		c.Status(http.StatusOK)
	})

	body := "0123456789" // 10 bytes — exceeds the 5-byte limit.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/echo", strings.NewReader(body))
	engine.ServeHTTP(w, req)

	// The handler detected the read error and returned 413.
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status: got %d, want 413", w.Code)
	}
}

// ─── corsMiddleware ───────────────────────────────────────────────────────────

// TestCORSMiddleware_WildcardAllowsAnyOrigin verifies that when corsOrigins is
// "*", the response to a preflight OPTIONS request includes the correct CORS
// headers allowing any origin.
func TestCORSMiddleware_WildcardAllowsAnyOrigin(t *testing.T) {
	// Arrange: wildcard origin.
	engine := New("*", 1<<20)
	engine.GET("/probe", func(c *gin.Context) { c.Status(http.StatusOK) })

	// Simulate a preflight OPTIONS request from an arbitrary origin.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/probe", nil)
	req.Header.Set("Origin", "https://arbitrary.example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	engine.ServeHTTP(w, req)

	// The CORS middleware should allow the request (2xx status).
	if w.Code >= 400 {
		t.Errorf("preflight status: got %d, want < 400", w.Code)
	}
}

// TestCORSMiddleware_ExplicitOriginAllowed verifies that an origin explicitly
// listed in corsOrigins is allowed.
func TestCORSMiddleware_ExplicitOriginAllowed(t *testing.T) {
	// Arrange: only allow a specific origin.
	allowed := "https://dashboard.corp"
	engine := New(allowed, 1<<20)
	engine.GET("/probe", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/probe", nil)
	req.Header.Set("Origin", allowed)
	req.Header.Set("Access-Control-Request-Method", "GET")
	engine.ServeHTTP(w, req)

	if w.Code >= 400 {
		t.Errorf("preflight status: got %d, want < 400 for allowed origin", w.Code)
	}
}

// TestCORSMiddleware_MultipleOrigins verifies that corsOrigins with multiple
// comma-separated origins correctly configures the middleware for all of them.
func TestCORSMiddleware_MultipleOrigins(t *testing.T) {
	// Arrange: two allowed origins.
	origins := "https://a.corp, https://b.corp"
	engine := New(origins, 1<<20)
	engine.GET("/probe", func(c *gin.Context) { c.Status(http.StatusOK) })

	// Both origins should be allowed.
	for _, origin := range []string{"https://a.corp", "https://b.corp"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodOptions, "/probe", nil)
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "GET")
		engine.ServeHTTP(w, req)

		if w.Code >= 400 {
			t.Errorf("origin %q: preflight status got %d, want < 400", origin, w.Code)
		}
	}
}
