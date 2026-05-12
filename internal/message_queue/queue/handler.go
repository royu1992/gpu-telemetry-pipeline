package queue

import (
	"context"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	queuecfg "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/config"
	message_queue "github.com/royu1992/gpu-telemetry-pipeline/internal/message_queue/model"
	"github.com/royu1992/gpu-telemetry-pipeline/internal/model"
)

// MessageQueueHandler holds the dependencies required to serve the message-queue HTTP API.
type MessageQueueHandler struct {
	buf     *Buffer
	metrics *message_queue.Metrics
	cfg     queuecfg.QueueConfig
	ready   atomic.Bool
	closing atomic.Bool
}

// NewMessageQueueHandler creates a MessageQueueHandler and immediately marks the service as ready.
func NewMessageQueueHandler(buf *Buffer, m *message_queue.Metrics, cfg queuecfg.QueueConfig) *MessageQueueHandler {
	h := &MessageQueueHandler{
		buf:     buf,
		metrics: m,
		cfg:     cfg,
	}

	// Mark the service as ready immediately so the Kubernetes readiness probe
	// returns 200 as soon as the handler is wired into the HTTP server.
	h.ready.Store(true)

	return h
}

// SetClosing marks the handler as closing. The /ready probe will begin
// returning 503 and publish requests will be rejected.
func (h *MessageQueueHandler) SetClosing() {
	// Lower the readiness flag first so the load balancer stops routing new
	// requests to this pod before we begin actively rejecting them.
	h.ready.Store(false)

	// Raise the closing flag so handlePublish and handleConsume immediately
	// return 503 Service Unavailable to any requests that arrive afterwards.
	h.closing.Store(true)
}

// RegisterRoutes attaches all message-queue endpoints to the provided Gin engine.
func (h *MessageQueueHandler) RegisterRoutes(r *gin.Engine) {
	// Message lifecycle endpoints used by Streamers and Collectors.
	r.POST("/messages", h.handlePublish)
	r.GET("/messages/consume", h.handleConsume)
	r.POST("/messages/ack", h.handleAck)

	// Operational endpoints used by Kubernetes probes and monitoring systems.
	r.GET("/health", h.handleHealth)
	r.GET("/ready", h.handleReady)
	r.GET("/metrics", h.handleMetrics)
}

// handlePublish handles POST /messages.
// Accepts a telemetry message from a Streamer and places it on the queue.
//
// @Summary      Publish a telemetry message
// @Description  Enqueues a single GPU telemetry data point. Blocks until a slot is free or the publish timeout elapses.
// @Tags         messages
// @Accept       json
// @Produce      json
// @Param        message  body      message_queue.PublishRequest   true  "Telemetry message to enqueue"
// @Success      201      {object}  message_queue.PublishResponse  "Message accepted; returns the assigned message_id"
// @Failure      400      {object}  message_queue.ErrorResponse    "Missing or invalid request body"
// @Failure      429      {object}  message_queue.ErrorResponse    "Queue is full; retry with backoff"
// @Failure      503      {object}  message_queue.ErrorResponse    "Queue is shutting down"
// @Failure      500      {object}  message_queue.ErrorResponse    "Internal error"
// @Router       /messages [post]
func (h *MessageQueueHandler) handlePublish(c *gin.Context) {
	// Reject incoming messages during graceful shutdown so the Streamer backs
	// off and retries against a healthy pod.
	if h.closing.Load() {
		jsonError(c, http.StatusServiceUnavailable, "queue is shutting down")
		return
	}

	// Parse and validate the request body. Gin's binding layer enforces the
	// `binding:"required"` constraints declared on PublishRequest fields.
	var req message_queue.PublishRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		jsonError(c, http.StatusBadRequest, err.Error())
		return
	}

	// Map the validated request fields to the internal message type.
	// MessageID is intentionally left blank here; the Buffer assigns it.
	msg := model.TelemetryMessage{
		Timestamp:  req.Timestamp,
		MetricName: req.MetricName,
		GpuID:      req.GpuID,
		Device:     req.Device,
		UUID:       req.UUID,
		ModelName:  req.ModelName,
		Hostname:   req.Hostname,
		Value:      req.Value,
		LabelsRaw:  req.LabelsRaw,
	}

	// Apply a publish timeout so a full queue does not hold the HTTP connection
	// open indefinitely, allowing the Streamer to retry quickly.
	ctx, cancel := context.WithTimeout(c.Request.Context(), h.cfg.PublishTimeout)
	defer cancel()

	// Attempt to enqueue the message. Blocks until a free slot is available,
	// the timeout fires, or the buffer is closed.
	msgID, err := h.buf.Publish(ctx, msg)
	if err != nil {
		switch err {
		case ErrClosing:
			jsonError(c, http.StatusServiceUnavailable, "queue is shutting down")
		case ErrFull:
			// Instruct the Streamer to back off; the queue is at capacity.
			jsonError(c, http.StatusTooManyRequests, "queue is full, retry with backoff")
		default:
			jsonError(c, http.StatusInternalServerError, "internal error")
		}
		return
	}

	// Return the assigned message ID so the Streamer can correlate log entries.
	c.JSON(http.StatusCreated, message_queue.PublishResponse{MessageID: msgID})
}

// handleConsume handles GET /messages/consume.
// Performs a long poll: blocks until messages are available or timeout elapses.
//
// @Summary      Consume messages (long poll)
// @Description  Long-polls for up to `long_poll_timeout` seconds. Returns up to `batch_size` messages leased to `consumer_id`. Returns 204 on timeout with no messages.
// @Tags         messages
// @Produce      json
// @Param        consumer_id       query     string  true   "Unique identifier for this consumer instance (typically pod hostname)"
// @Param        batch_size        query     int     false  "Maximum number of messages to return (default: server-configured batch size)"
// @Param        long_poll_timeout query     string  false  "Maximum wait duration, e.g. 30s (default: server-configured value)"
// @Success      200               {object}  message_queue.ConsumeResponse  "One or more messages leased to the caller"
// @Success      204               "Long-poll timed out; no messages available — poll again immediately"
// @Failure      400               {object}  message_queue.ErrorResponse    "consumer_id missing"
// @Failure      503               {object}  message_queue.ErrorResponse    "Queue is shutting down"
// @Router       /messages/consume [get]
func (h *MessageQueueHandler) handleConsume(c *gin.Context) {
	// Reject new poll requests during graceful shutdown.
	if h.closing.Load() {
		jsonError(c, http.StatusServiceUnavailable, "queue is shutting down")
		return
	}

	// consumer_id is required so the buffer can validate ownership at ack time.
	consumerID := c.Query("consumer_id")
	if consumerID == "" {
		jsonError(c, http.StatusBadRequest, "consumer_id is required")
		return
	}

	// Allow the caller to override the configured batch size for this request.
	max := h.cfg.BatchSize
	if v := c.Query("batch_size"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			max = n
		}
	}

	// Allow the caller to shorten the long-poll window for this request.
	timeout := h.cfg.LongPollTimeout
	if v := c.Query("long_poll_timeout"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			timeout = d
		}
	}

	// Derive a context deadline from the long-poll timeout. The HTTP server's
	// WriteTimeout is configured larger than LongPollTimeout so the response
	// can always be written before the server forcibly closes the connection.
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()

	// Block until messages arrive, the poll deadline fires, or the buffer closes.
	items, err := h.buf.Consume(ctx, consumerID, max)
	if err == ErrClosing {
		jsonError(c, http.StatusServiceUnavailable, "queue is shutting down")
		return
	}

	// An empty slice with no error means the long-poll timed out — return 204
	// so the Collector knows to re-poll immediately.
	if len(items) == 0 {
		c.Status(http.StatusNoContent)
		return
	}

	// Convert internal DeliveryItems to the API response shape.
	resp := message_queue.ConsumeResponse{
		Messages: make([]message_queue.DeliveryItem, len(items)),
	}
	for i, item := range items {
		resp.Messages[i] = message_queue.DeliveryItem{
			DeliveryID:   item.DeliveryID,
			LeaseExpires: item.LeaseExpires,
			Message:      item.Message,
		}
	}

	c.JSON(http.StatusOK, resp)
}

// handleAck handles POST /messages/ack.
// Processes a batch of delivery acknowledgments from a Collector.
//
// @Summary      Acknowledge messages
// @Description  Marks delivered messages as processed. Returns 207 Multi-Status if any delivery_id was rejected (expired lease, wrong consumer, or unknown ID).
// @Tags         messages
// @Accept       json
// @Produce      json
// @Param        ack  body      message_queue.AckRequest  true  "Consumer ID and list of delivery IDs to acknowledge"
// @Success      200  {object}  message_queue.AckResult   "All messages acknowledged"
// @Success      207  {object}  message_queue.AckResult   "Partial success; check rejected_ids"
// @Failure      400  {object}  message_queue.ErrorResponse "Invalid request body"
// @Router       /messages/ack [post]
func (h *MessageQueueHandler) handleAck(c *gin.Context) {
	// Parse and validate the request body. Gin binding enforces required fields.
	var req message_queue.AckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		jsonError(c, http.StatusBadRequest, err.Error())
		return
	}

	// Process each delivery ID independently; outcomes may differ per entry
	// (e.g. one ID is valid while another has already expired).
	outcomes := h.buf.Acknowledge(req.ConsumerID, req.DeliveryIDs)

	// Tally accepted and rejected outcomes to populate the response body.
	var accepted, rejected int
	var rejectedIDs []string

	// If at least one ID was rejected, the overall request is considered
	// partially failed and the response status is 207 Multi-Status instead of 200 OK.
	for _, o := range outcomes {
		if o.Accepted {
			accepted++
		} else {
			rejected++
			rejectedIDs = append(rejectedIDs, o.DeliveryID)
		}
	}

	result := message_queue.AckResult{
		Acked:       accepted,
		Rejected:    rejected,
		RejectedIDs: rejectedIDs,
	}

	// Return 207 Multi-Status when at least one ID was rejected so the Collector
	// knows to inspect rejected_ids and decide on corrective action.
	if rejected > 0 {
		c.JSON(http.StatusMultiStatus, result)
		return
	}

	c.JSON(http.StatusOK, result)
}

// handleHealth handles GET /health — Kubernetes liveness probe.
// Returns 200 as long as the process is alive.
//
// @Summary      Liveness probe
// @Description  Returns 200 while the process is alive. Used by Kubernetes as a liveness probe.
// @Tags         operations
// @Produce      json
// @Success      200  {object}  message_queue.HealthResponse
// @Router       /health [get]
func (h *MessageQueueHandler) handleHealth(c *gin.Context) {
	// Always return 200 while the process is running. The liveness probe does
	// not reflect queue load or readiness — only that the process is alive.
	c.JSON(http.StatusOK, message_queue.HealthResponse{
		Status: "ok",
	})
}

// handleReady handles GET /ready — Kubernetes readiness probe.
// Returns 503 during startup initialisation and graceful shutdown.
//
// @Summary      Readiness probe
// @Description  Returns 200 when the queue is ready to serve traffic, including live queue depth and capacity. Returns 503 during shutdown.
// @Tags         operations
// @Produce      json
// @Success      200  {object}  message_queue.ReadyResponse
// @Failure      503  {object}  message_queue.ReadyResponse
// @Router       /ready [get]
func (h *MessageQueueHandler) handleReady(c *gin.Context) {
	// Return 503 if the service is shutting down, causing the Kubernetes
	// readiness probe to remove this pod from the load balancer rotation.
	if !h.ready.Load() {
		c.JSON(http.StatusServiceUnavailable, message_queue.ReadyResponse{
			Status: "not ready",
			Reason: "shutting down",
		})

		return
	}

	// Include live queue depth and capacity so operators can gauge load at a
	// glance without querying the metrics endpoint separately.
	c.JSON(http.StatusOK, message_queue.ReadyResponse{
		Status:     "ready",
		QueueDepth: h.buf.Depth(),
		Capacity:   h.buf.Capacity(),
	})
}

// handleMetrics handles GET /metrics — queue statistics endpoint.
//
// @Summary      Queue metrics
// @Description  Returns a JSON snapshot of queue counters (published, acked, redelivered, dropped) and live state (depth, capacity, in-flight).
// @Tags         operations
// @Produce      json
// @Success      200  {object}  message_queue.Snapshot
// @Router       /metrics [get]
func (h *MessageQueueHandler) handleMetrics(c *gin.Context) {
	// Collect a consistent point-in-time snapshot of counters and live queue
	// state in a single call to avoid skew between separate reads.
	snap := h.metrics.Snapshot(
		h.buf.Depth(),
		h.buf.Capacity(),
		h.buf.InFlightCount(),
	)

	c.JSON(http.StatusOK, snap)
}

// jsonError writes a consistent JSON error response body.
func jsonError(c *gin.Context, status int, msg string) {
	// Encode the error as a JSON object so clients can parse it consistently
	// regardless of the HTTP status code.
	c.JSON(status, message_queue.ErrorResponse{
		Error: msg,
	})
}
