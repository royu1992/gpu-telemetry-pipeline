package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// ---------------------------------------------------------------------------
// server.go: New and bodyLimitMiddleware
// ---------------------------------------------------------------------------

func TestNew(t *testing.T) {
	// Step: Create a Gin engine via New and verify it is non-nil.
	r := New(1024)
	if r == nil {
		t.Fatal("New() returned nil gin.Engine")
	}
}

func TestBodyLimitMiddleware(t *testing.T) {
	tests := []struct {
		name       string
		maxBytes   int64
		body       string
		wantStatus int
	}{
		{
			// Step: Body within the limit should be processed normally.
			name:       "body within limit is allowed",
			maxBytes:   1024,
			body:       `{"test": "ok"}`,
			wantStatus: http.StatusOK,
		},
		{
			// Step: Body exceeding the limit should result in a 413 or 400 response.
			// The Gin recovery middleware intercepts the http.MaxBytesReader panic/error.
			name:       "body exceeding limit is rejected",
			maxBytes:   5,
			body:       `{"this_body_is_far_too_long": true}`,
			wantStatus: http.StatusRequestEntityTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step: Build a Gin engine with the body limit and a simple echo handler.
			r := New(tt.maxBytes)
			r.POST("/test", func(c *gin.Context) {
				// Step: Attempt to read the body. If it exceeds the limit,
				// the MaxBytesReader will return an error here.
				var payload map[string]interface{}
				if err := c.ShouldBindJSON(&payload); err != nil {
					c.Status(http.StatusRequestEntityTooLarge)
					return
				}
				c.Status(http.StatusOK)
			})

			// Step: Send the request and record the response.
			req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			// Step: Verify the status code matches the expectation.
			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}
		})
	}
}
