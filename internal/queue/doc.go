// Package queue implements the custom in-memory message queue.
//
// It contains the ring buffer, slot state machine (EMPTY → PENDING →
// IN_FLIGHT → EMPTY/DROPPED), lease-based redelivery, backpressure via
// blocking publish, and the background lease reaper goroutine.
package queue
