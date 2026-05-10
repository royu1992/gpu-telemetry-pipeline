package model

// TelemetryMessage is the canonical representation of a single GPU telemetry
// data point. It maps directly to a row in the DCGM CSV export and is the
// wire format for messages flowing through the message queue.
type TelemetryMessage struct {
	// MessageID is assigned by the message-queue service upon receipt.
	// It is stable across redeliveries.
	MessageID  string `json:"message_id"`
	Timestamp  string `json:"timestamp"`
	MetricName string `json:"metric_name"`
	GpuID      string `json:"gpu_id"`
	Device     string `json:"device"`
	// UUID uniquely identifies a GPU across all hosts.
	UUID      string `json:"uuid"`
	ModelName string `json:"model_name"`
	Hostname  string `json:"hostname"`
	Value     string `json:"value"`
	LabelsRaw string `json:"labels_raw,omitempty"`
}
