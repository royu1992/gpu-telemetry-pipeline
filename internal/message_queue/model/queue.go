package message_queue

import (
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/model"
)

// PublishRequest is the request body sent by a Streamer to POST /messages.
// Required fields are validated by Gin binding.
type PublishRequest struct {
	Timestamp  string `json:"timestamp"    binding:"required"`
	MetricName string `json:"metric_name"  binding:"required"`
	GpuID      string `json:"gpu_id"`
	Device     string `json:"device"`
	UUID       string `json:"uuid"         binding:"required"`
	ModelName  string `json:"model_name"`
	Hostname   string `json:"hostname"`
	Value      string `json:"value"        binding:"required"`
	LabelsRaw  string `json:"labels_raw"`
}

// PublishResponse is returned by POST /messages on success.
type PublishResponse struct {
	MessageID string `json:"message_id"`
}

// DeliveryItem is a single message in a consume response, bundled with its
// delivery metadata. The DeliveryID must be returned in the subsequent ack.
type DeliveryItem struct {
	DeliveryID   string                 `json:"delivery_id"`
	LeaseExpires time.Time              `json:"lease_expires"`
	Message      model.TelemetryMessage `json:"message"`
}

// ConsumeResponse is returned by GET /messages/consume.
type ConsumeResponse struct {
	Messages []DeliveryItem `json:"messages"`
}

// AckOutcome describes the result of a single acknowledgment attempt.
type AckOutcome struct {
	DeliveryID string `json:"delivery_id"`
	Accepted   bool   `json:"accepted"`
	Reason     string `json:"reason,omitempty"`
}

// AckRequest is the request body sent by a Collector to POST /messages/ack.
type AckRequest struct {
	ConsumerID  string   `json:"consumer_id"  binding:"required"`
	DeliveryIDs []string `json:"delivery_ids" binding:"required"`
}

// AckResult is the response body for POST /messages/ack.
type AckResult struct {
	Acked       int      `json:"acked"`
	Rejected    int      `json:"rejected"`
	RejectedIDs []string `json:"rejected_ids,omitempty"`
}
