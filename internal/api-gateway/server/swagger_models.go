package server

// HealthResponse represents a standard 200 OK health response.
type HealthResponse struct {
	Status string `json:"status" example:"ok"`
}

// ReadyResponse represents a standard readiness response.
type ReadyResponse struct {
	Status string `json:"status" example:"ready"`
}

// ErrorResponse represents a standard error response.
type ErrorResponse struct {
	Error string `json:"error" example:"error message"`
}

// TelemetryResponse represents the time-series wrapper for a GPU.
type TelemetryResponse struct {
	ID    string      `json:"id" example:"GPU-5fd4..."`
	Count int         `json:"count" example:"100"`
	Data  interface{} `json:"data"`
}
