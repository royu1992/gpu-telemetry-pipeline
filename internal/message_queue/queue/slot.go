package queue

import (
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/model"
)

// slotStatus represents the lifecycle state of a single ring buffer slot.
// Valid transitions are documented in the slot state machine in the architecture doc.
type slotStatus int

const (
	statusEmpty    slotStatus = iota // slot is free and available for new messages
	statusPending                    // message written, waiting to be dispatched to a Collector
	statusInFlight                   // dispatched to a Collector, awaiting acknowledgment
)

// slot is a single entry in the ring buffer.
type slot struct {
	message          model.TelemetryMessage
	status           slotStatus
	deliveryID       string
	consumerID       string
	leaseExpires     time.Time
	deliveryAttempts int
}

// reset clears the slot back to its zero value, returning it to statusEmpty.
func (s *slot) reset() {
	// Overwrite every field at once by assigning the zero value of the slot
	// struct. This clears message, status, deliveryID, consumerID,
	// leaseExpires, and deliveryAttempts in a single statement.
	*s = slot{}
}
