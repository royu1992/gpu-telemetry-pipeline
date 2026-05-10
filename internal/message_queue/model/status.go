package message_queue

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status string `json:"status"`
}

// ReadyResponse is returned by GET /ready.
type ReadyResponse struct {
	Status     string `json:"status"`
	Reason     string `json:"reason,omitempty"`
	QueueDepth int    `json:"queue_depth,omitempty"`
	Capacity   int    `json:"capacity,omitempty"`
}

// ErrorResponse is the body returned for all non-2xx responses.
type ErrorResponse struct {
	Error string `json:"error"`
}
