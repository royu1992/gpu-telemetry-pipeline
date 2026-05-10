package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	message_queue "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/model"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/model"
)

// Sentinel errors returned by Buffer operations.
var (
	// ErrClosing is returned when an operation is attempted on a closing buffer.
	ErrClosing = errors.New("queue is closing")

	// ErrFull is returned when a publish times out because the buffer is full.
	ErrFull = errors.New("queue is full")
)

// Buffer is a bounded, in-memory message store with at-least-once delivery semantics.
//
// Internally it uses a fixed-size slot array with a free-list to track available
// positions. Messages progress through the slot state machine:
//
//	EMPTY → PENDING → IN_FLIGHT → EMPTY   (happy path)
//	IN_FLIGHT → PENDING                   (lease expired — redelivery)
//	IN_FLIGHT → EMPTY                     (max attempts exceeded — drop)
//
// All public methods are safe for concurrent use.
type Buffer struct {
	slots    []slot
	capacity int

	// freeSlots is a stack of slot indices that are currently EMPTY and
	// available for new writes. Initialised with all indices at startup.
	freeSlots []int

	// pendingQueue holds slot indices for freshly published messages in FIFO order.
	pendingQueue []int

	// requeuedQueue holds slot indices for messages requeued after a lease expiry.
	// These are prioritised over pendingQueue to minimise redelivery latency.
	requeuedQueue []int

	// inFlight maps delivery_id → slot index for O(1) acknowledgment lookup.
	inFlight map[string]int

	mu         sync.Mutex
	canPublish *sync.Cond // signalled when a slot becomes EMPTY
	canConsume *sync.Cond // signalled when a slot becomes PENDING

	leaseDuration       time.Duration
	maxDeliveryAttempts int
	metrics             *message_queue.Metrics
	closing             bool
}

// NewBuffer allocates and returns a ready-to-use Buffer with the given capacity.
func NewBuffer(capacity int, leaseDuration time.Duration, maxDeliveryAttempts int, m *message_queue.Metrics) *Buffer {
	// Pre-populate the free list with every slot index so that all slots are
	// immediately available for publishing on startup.
	freeSlots := make([]int, capacity)
	for i := range freeSlots {
		freeSlots[i] = i
	}

	b := &Buffer{
		slots:     make([]slot, capacity),
		capacity:  capacity,
		freeSlots: freeSlots,
		// Pre-size the in-flight map to 64 entries to avoid early rehashing
		// under typical concurrent load.
		inFlight:            make(map[string]int, 64),
		leaseDuration:       leaseDuration,
		maxDeliveryAttempts: maxDeliveryAttempts,
		metrics:             m,
	}

	// Attach both condition variables to the same mutex so they share the
	// lock that protects all buffer state fields.
	b.canPublish = sync.NewCond(&b.mu)
	b.canConsume = sync.NewCond(&b.mu)

	return b
}

// isFull reports whether no EMPTY slots remain. Must be called with b.mu held.
func (b *Buffer) isFull() bool {
	// The buffer is full when no slot indices remain in the free list.
	return len(b.freeSlots) == 0
}

// hasPending reports whether any message is ready to be dispatched.
// Must be called with b.mu held.
func (b *Buffer) hasPending() bool {
	// A message is dispatchable when either the freshly-published queue or
	// the higher-priority redelivery queue is non-empty.
	return len(b.requeuedQueue) > 0 || len(b.pendingQueue) > 0
}

// Depth returns the number of messages currently waiting to be dispatched.
func (b *Buffer) Depth() int {
	// Acquire the mutex to read a consistent snapshot of both dispatch queues.
	b.mu.Lock()
	defer b.mu.Unlock()

	// Sum fresh and requeued messages. In-flight messages are excluded because
	// they have already been dispatched and are awaiting acknowledgment.
	return len(b.pendingQueue) + len(b.requeuedQueue)
}

// InFlightCount returns the number of messages currently held by Collectors.
func (b *Buffer) InFlightCount() int {
	// Acquire the mutex to read a consistent map length.
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.inFlight)
}

// Capacity returns the fixed slot capacity of this buffer.
func (b *Buffer) Capacity() int {
	// The capacity is fixed at construction time and never changes.
	return b.capacity
}

// Close signals the buffer to stop accepting new messages and unblocks any
// goroutines waiting in Publish or Consume.
func (b *Buffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Mark the buffer as closing so subsequent Publish and Consume calls
	// return ErrClosing immediately without entering the wait loop.
	b.closing = true

	// Wake every goroutine blocked inside Publish (waiting for a free slot)
	// so they observe the closing flag and exit cleanly.
	b.canPublish.Broadcast()

	// Wake every goroutine blocked inside Consume (waiting for a pending
	// message) so they observe the closing flag and exit cleanly.
	b.canConsume.Broadcast()
}

// Publish writes a message to the buffer. It blocks until a slot is available
// or ctx is cancelled. The queue assigns a unique message_id and returns it.
//
// Returns ErrClosing if the buffer is shutting down.
// Returns ErrFull if ctx times out while the buffer remains full.
func (b *Buffer) Publish(ctx context.Context, msg model.TelemetryMessage) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Block while the buffer is full and the queue is not closing. Check the
	// context before each sleep so a timed-out caller exits promptly without
	// waiting for a canPublish signal that may never arrive.
	for b.isFull() && !b.closing {
		if ctx.Err() != nil {
			return "", ErrFull
		}

		// released is used to signal the helper goroutine to exit once we are
		// done waiting, preventing a goroutine leak.
		released := make(chan struct{})

		// Spawn a helper goroutine that broadcasts on canPublish when ctx is
		// cancelled. sync.Cond.Wait does not accept a context, so this is
		// the idiomatic pattern to make the wait context-aware.
		go func() {
			select {
			case <-ctx.Done():
				// Re-acquire the lock before broadcasting, as required by sync.Cond.
				b.mu.Lock()
				b.canPublish.Broadcast()
				b.mu.Unlock()
			case <-released:
				// Publish completed normally; the helper is no longer needed.
			}
		}()

		// Wait for a canPublish signal from another goroutine that freed a slot, or
		// for the helper goroutine to wake us when ctx is cancelled.
		b.canPublish.Wait()

		// Signal the helper goroutine to exit now that we have been woken.
		// Closing the channel is a safe and efficient way to signal the helper
		// goroutine to exit, as it unblocks the select immediately.
		close(released)
	}

	// Post-wait checks: the loop may have exited because b.closing was set.
	if b.closing {
		return "", ErrClosing
	}

	// The context may have expired at exactly the same tick as a notFull signal.
	if ctx.Err() != nil {
		return "", ErrFull
	}

	// Assign a globally unique message ID. This ID is stable across redeliveries.
	msg.MessageID = uuid.New().String()

	// Pop the last entry from the free-list stack (O(1), no element shifts).
	idx := b.freeSlots[len(b.freeSlots)-1]
	b.freeSlots = b.freeSlots[:len(b.freeSlots)-1]

	// Write the message into the chosen slot and mark it PENDING so the
	// Consume path can dispatch it to a Collector.
	b.slots[idx] = slot{
		message: msg,
		status:  statusPending,
	}

	// Append the slot index to the pending FIFO queue to preserve publish order.
	b.pendingQueue = append(b.pendingQueue, idx)

	// Increment the published counter before signalling so any observer that
	// wakes up always sees a count at least as large as the current queue depth.
	b.metrics.IncPublished()

	// Wake one goroutine waiting inside Consume now that a message is available.
	b.canConsume.Signal()

	return msg.MessageID, nil
}

// Consume waits for up to max PENDING messages and returns them with lease
// metadata. It blocks until at least one message is available or ctx is cancelled.
//
// Returns an empty slice (not an error) when ctx times out with no messages —
// the caller should return 204 No Content.
// Returns ErrClosing if the buffer is shutting down with no messages available.
func (b *Buffer) Consume(ctx context.Context, consumerID string, max int) ([]message_queue.DeliveryItem, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Block while there are no pending messages and the buffer is not closing.
	// Check the context before each sleep so a long-poll timeout wakes promptly
	// without needing a canConsume signal from another goroutine.
	for !b.hasPending() && !b.closing {
		if ctx.Err() != nil {
			// Long-poll deadline elapsed with no messages — caller returns 204.
			return nil, nil
		}

		// released is used to signal the helper goroutine to exit once we are
		// done waiting, preventing a goroutine leak.
		released := make(chan struct{})

		// Spawn a helper goroutine that broadcasts on canConsume when ctx is
		// cancelled. sync.Cond.Wait does not accept a context, so this is
		// the idiomatic pattern to make the wait context-aware.
		go func() {
			select {
			case <-ctx.Done():
				// Re-acquire the lock before broadcasting, as required by sync.Cond.
				b.mu.Lock()
				b.canConsume.Broadcast()
				b.mu.Unlock()
			case <-released:
				// Consume completed normally; the helper is no longer needed.
			}
		}()

		// Wait for a canConsume signal from another goroutine that published a message, or
		// for the helper goroutine to wake us when ctx is cancelled.
		b.canConsume.Wait()

		// Signal the helper goroutine to exit now that we have been woken.
		// Closing the channel is a safe and efficient way to signal the helper
		// goroutine to exit, as it unblocks the select immediately.
		close(released)
	}

	// If the buffer is closing and no messages remain, signal the caller to
	// stop polling rather than returning an empty 204 response.
	if b.closing && !b.hasPending() {
		return nil, ErrClosing
	}

	// The context may have expired at the same tick as a notEmpty signal.
	if ctx.Err() != nil {
		return nil, nil
	}

	// Compute a single shared lease expiry for all messages in this batch so
	// all leases from one consume call expire at the same time.
	now := time.Now()
	leaseExp := now.Add(b.leaseDuration)
	items := make([]message_queue.DeliveryItem, 0, max)

	// Drain up to max messages from the dispatch queues.
	for len(items) < max && b.hasPending() {
		var idx int

		// Prioritise requeued messages (lease-expired redeliveries) over newly
		// published ones to reduce redelivery latency.
		if len(b.requeuedQueue) > 0 {
			idx, b.requeuedQueue = b.requeuedQueue[0], b.requeuedQueue[1:]
		} else {
			idx, b.pendingQueue = b.pendingQueue[0], b.pendingQueue[1:]
		}

		// Take a pointer to the live slot in the backing array so any state changes
		// below update the buffer's real slot instead of a copy.
		s := &b.slots[idx]

		// Generate a unique delivery ID that the Collector must echo in the ack.
		// The "dlv-" prefix distinguishes it from message IDs in logs.
		dlvID := "dlv-" + uuid.New().String()

		// Transition the slot to IN_FLIGHT and stamp it with lease metadata.
		s.status = statusInFlight
		s.deliveryID = dlvID
		s.consumerID = consumerID
		s.leaseExpires = leaseExp

		// Increment before registering in inFlight so the reaper always sees
		// an accurate attempt count when it inspects this slot.
		s.deliveryAttempts++

		// Register in the in-flight map for O(1) lookup during acknowledgment.
		b.inFlight[dlvID] = idx

		// Append the message and lease metadata to the response batch.
		items = append(items, message_queue.DeliveryItem{
			DeliveryID:   dlvID,
			LeaseExpires: leaseExp,
			Message:      s.message,
		})
	}

	return items, nil
}

// Acknowledge processes a batch of delivery IDs. Each ID is independently
// validated; a single batch may contain both accepted and rejected outcomes.
//
// An ack is rejected when the delivery_id is unknown (e.g. after a queue
// restart) or the consumer_id does not match the original consumer.
func (b *Buffer) Acknowledge(consumerID string, deliveryIDs []string) []message_queue.AckOutcome {
	b.mu.Lock()
	defer b.mu.Unlock()

	outcomes := make([]message_queue.AckOutcome, 0, len(deliveryIDs))
	for _, dlvID := range deliveryIDs {
		// Look up the slot index using the delivery ID as the map key.
		idx, ok := b.inFlight[dlvID]
		if !ok {
			// Delivery ID is unknown — it may have already been acked, expired,
			// or the message may have been dropped after exceeding max attempts.
			outcomes = append(outcomes, message_queue.AckOutcome{
				DeliveryID: dlvID,
				Accepted:   false,
				Reason:     "unknown delivery_id",
			})

			continue
		}

		// Take a pointer to the live slot in the backing array so any state changes
		// below update the buffer's real slot instead of a copy.
		s := &b.slots[idx]

		// Validate that the ack originates from the same Collector that consumed
		// the message. This prevents a different consumer from incorrectly
		// freeing a slot it does not own.
		if s.consumerID != consumerID {
			outcomes = append(outcomes, message_queue.AckOutcome{
				DeliveryID: dlvID,
				Accepted:   false,
				Reason:     "consumer_id mismatch",
			})

			continue
		}

		// Remove the delivery entry from the in-flight map.
		delete(b.inFlight, dlvID)

		// Clear all slot fields, resetting its status back to EMPTY.
		s.reset()

		// Return the now-free slot index to the free list so it can be reused.
		b.freeSlots = append(b.freeSlots, idx)

		// Increment the acked counter before signalling so any observer that wakes
		// up always sees a count at least as large as the current acked count.
		b.metrics.IncAcked()

		// Wake one goroutine waiting inside Publish now that a slot has freed.
		b.canPublish.Signal()

		// Record a successful acknowledgment for this delivery ID.
		outcomes = append(outcomes, message_queue.AckOutcome{
			DeliveryID: dlvID,
			Accepted:   true,
		})
	}

	return outcomes
}

// reapExpiredLeases is called by the reaper to reset or drop messages whose lease
// has expired. The mutex is acquired and released per slot to avoid prolonged
// blocking of Publish and Consume goroutines during large scans.
func (b *Buffer) reapExpiredLeases(logger *slog.Logger) {
	now := time.Now()

	// Phase 1: collect expired delivery IDs while holding the lock briefly.
	// We snapshot only the IDs, not the slot data, to keep the critical
	// section short and avoid blocking concurrent Publish or Consume callers.
	b.mu.Lock()
	expired := make([]string, 0)
	for dlvID, idx := range b.inFlight {
		if !b.slots[idx].leaseExpires.After(now) {
			expired = append(expired, dlvID)
		}
	}
	b.mu.Unlock()

	// Nothing to do this tick — exit early to avoid unnecessary locking.
	if len(expired) == 0 {
		return
	}

	// Phase 2: process each expired delivery ID, re-acquiring the lock per
	// slot. This prevents a single reaper pass from holding the lock for the
	// full duration of the scan, which would starve producers and consumers.
	for _, dlvID := range expired {
		b.mu.Lock()

		// Re-check that the delivery is still in-flight. It may have been
		// acknowledged by the Collector between phase 1 and now.
		idx, ok := b.inFlight[dlvID]
		if !ok {
			b.mu.Unlock()
			continue
		}

		// Take a pointer to the live slot in the backing array so any state
		// changes below update the buffer's real slot instead of a copy.
		s := &b.slots[idx]
		if s.deliveryAttempts >= b.maxDeliveryAttempts {
			// The message has exceeded the maximum delivery attempts — drop it
			// permanently and log enough context to aid post-mortem analysis.
			logger.Error("dropping message: exceeded max delivery attempts",
				"message_id", s.message.MessageID,
				"uuid", s.message.UUID,
				"metric_name", s.message.MetricName,
				"delivery_attempts", s.deliveryAttempts,
			)

			// Remove the stale delivery entry from the in-flight map.
			delete(b.inFlight, dlvID)

			// Clear the slot and return it to the free list for reuse.
			s.reset()

			// Add the now-empty slot index back to the free list stack (O(1), no element shifts).
			b.freeSlots = append(b.freeSlots, idx)

			// Increment the dropped counter before signalling so any observer that wakes
			// up always sees a count at least as large as the current drop count.
			b.metrics.IncDropped()

			// Wake a waiting Publish call now that a slot has been freed.
			b.canPublish.Signal()
		} else {
			// Still within the attempt limit — requeue the message for redelivery.
			logger.Warn("lease expired: requeueing message for redelivery",
				"message_id", s.message.MessageID,
				"delivery_id", dlvID,
				"attempt", s.deliveryAttempts,
			)

			// Remove the stale delivery ID from the in-flight map.
			delete(b.inFlight, dlvID)

			// Reset delivery metadata but preserve the message payload and
			// the attempt count so the reaper can enforce the attempt limit.
			s.status = statusPending
			s.deliveryID = ""
			s.consumerID = ""

			// Add to the priority requeue list, which is drained before
			// pendingQueue in Consume to minimise redelivery latency.
			b.requeuedQueue = append(b.requeuedQueue, idx)

			// Increment the redelivery counter before signalling so any observer that wakes
			// up always sees a count at least as large as the current redelivery count.
			b.metrics.IncRedelivered()

			// Wake a waiting Consume call now that a message is pending.
			b.canConsume.Signal()
		}

		b.mu.Unlock()
	}
}
