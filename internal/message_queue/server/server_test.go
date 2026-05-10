package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestNew(t *testing.T) {
	// Initialize the server with a 100-byte limit
	r := New(100)

	if r == nil {
		t.Fatal("expected gin engine to be non-nil")
	}

	// Add a dummy handler to check if middleware works
	r.POST("/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	// Test request within limit
	bodySmall := bytes.Repeat([]byte("a"), 50)
	reqSmall := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBuffer(bodySmall))
	wSmall := httptest.NewRecorder()
	r.ServeHTTP(wSmall, reqSmall)

	if wSmall.Code != http.StatusOK {
		t.Errorf("expected status 200 for small body, got %d", wSmall.Code)
	}

	// Test request exceeding limit
	// http.MaxBytesReader will return an error when reading.
	// Gin's BindJSON/ShouldBind would fail or we can manually read it.
	r.POST("/read", func(c *gin.Context) {
		buf := make([]byte, 200)
		_, err := c.Request.Body.Read(buf)
		if err != nil {
			c.String(http.StatusRequestEntityTooLarge, "too large")
			return
		}
		c.Status(http.StatusOK)
	})

	bodyLarge := bytes.Repeat([]byte("a"), 150)
	reqLarge := httptest.NewRequest(http.MethodPost, "/read", bytes.NewBuffer(bodyLarge))
	wLarge := httptest.NewRecorder()
	r.ServeHTTP(wLarge, reqLarge)

	if wLarge.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected status 413 for large body, got %d", wLarge.Code)
	}
}

func TestBodyLimitMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(bodyLimitMiddleware(10))
	r.POST("/limit", func(c *gin.Context) {
		buf := new(bytes.Buffer)
		_, err := buf.ReadFrom(c.Request.Body)
		if err != nil {
			c.Status(http.StatusRequestEntityTooLarge)
			return
		}
		c.Status(http.StatusOK)
	})

	// Over limit
	reqLarge := httptest.NewRequest(http.MethodPost, "/limit", bytes.NewBufferString("this is way more than 10 bytes"))
	wLarge := httptest.NewRecorder()
	r.ServeHTTP(wLarge, reqLarge)
	if wLarge.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413, got %d", wLarge.Code)
	}

	// Exact limit (10 bytes)
	reqExact := httptest.NewRequest(http.MethodPost, "/limit", bytes.NewBufferString("1234567890"))
	wExact := httptest.NewRecorder()
	r.ServeHTTP(wExact, reqExact)
	if wExact.Code != http.StatusOK {
		t.Errorf("expected 200 for 10 bytes, got %d", wExact.Code)
	}
}
