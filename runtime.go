package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/masp-92/relay"

// handlerEntry is a type-erased handler stored in the registry.
type handlerEntry struct {
	fn func(ctx context.Context, msgID string, env *Envelope, meta Metadata) error
}

// Runtime is the relay worker runtime.
// It polls SQS, decodes messages, dispatches to typed handlers,
// and manages lifecycle concerns (retry, discard, lease, shutdown).
type Runtime struct {
	client   SQSClient
	config   Config
	opts     options
	handlers map[string]handlerEntry
	sealed   atomic.Bool

	// in-flight tracking
	mu       sync.Mutex
	inflight map[string]*inflightMsg
	wg       sync.WaitGroup
	sem      chan struct{}

	logger *slog.Logger
	tracer trace.Tracer
}

type inflightMsg struct {
	receiptHandle string
	startedAt     time.Time
	cancelLease   context.CancelFunc
}

// New creates a new Runtime. It does not start polling.
// The client must implement the SQSClient interface (e.g., sqs.Client from AWS SDK v2).
func New(client SQSClient, cfg Config, opts ...Option) *Runtime {
	cfg.setDefaults()

	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	if o.tracerProvider == nil {
		o.tracerProvider = otel.GetTracerProvider()
	}

	return &Runtime{
		client:   client,
		config:   cfg,
		opts:     o,
		handlers: make(map[string]handlerEntry),
		inflight: make(map[string]*inflightMsg),
		sem:      make(chan struct{}, cfg.Concurrency),
		logger:   o.logger.With("component", "relay"),
		tracer:   o.tracerProvider.Tracer(tracerName),
	}
}

// Handle registers a typed handler for the given message type.
// T is the payload type that will be JSON-decoded from the envelope.
//
// Handle panics if:
//   - messageType is already registered (programming error)
//   - called after Run has started
//
// This is a package-level generic function because Go does not support
// generic methods on concrete types.
func Handle[T any](rt *Runtime, messageType string, fn func(context.Context, Message[T]) error) {
	if rt.sealed.Load() {
		panic(fmt.Sprintf("relay: Handle called after Run for type %q", messageType))
	}
	if _, exists := rt.handlers[messageType]; exists {
		panic(fmt.Sprintf("relay: duplicate handler for type %q", messageType))
	}

	rt.handlers[messageType] = handlerEntry{
		fn: func(ctx context.Context, msgID string, env *Envelope, meta Metadata) error {
			var payload T
			if err := json.Unmarshal(env.Payload, &payload); err != nil {
				return fmt.Errorf("relay: unmarshal payload for type %q: %w", messageType, err)
			}
			msg := Message[T]{
				ID:       msgID,
				Type:     messageType,
				Payload:  payload,
				Metadata: meta,
			}
			return fn(ctx, msg)
		},
	}
}

// Run starts the polling loop and blocks until ctx is cancelled or a fatal error occurs.
// After ctx cancellation, it performs graceful shutdown.
func (rt *Runtime) Run(ctx context.Context) error {
	if err := rt.config.validate(); err != nil {
		return err
	}
	rt.sealed.Store(true)

	rt.logger.InfoContext(ctx, "relay runtime starting",
		"queue_url", rt.config.QueueURL,
		"concurrency", rt.config.Concurrency,
	)

	// Main polling loop
	rt.pollLoop(ctx)

	// Graceful shutdown: wait for in-flight handlers
	rt.logger.InfoContext(ctx, "relay runtime shutting down, draining in-flight handlers")
	done := make(chan struct{})
	go func() {
		rt.wg.Wait()
		close(done)
	}()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), rt.config.ShutdownTimeout)
	defer shutdownCancel()

	select {
	case <-done:
		rt.logger.Info("relay runtime shutdown complete, all handlers drained")
	case <-shutdownCtx.Done():
		rt.logger.Warn("relay runtime shutdown timeout, some handlers may still be running")
	}

	return nil
}

func (rt *Runtime) pollLoop(ctx context.Context) {
	var consecutiveErrors int

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := rt.receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // context cancelled, exit cleanly
			}
			consecutiveErrors++
			rt.logger.ErrorContext(ctx, "relay: receive error",
				"error", err,
				"consecutive_errors", consecutiveErrors,
			)
			rt.backoff(ctx, consecutiveErrors)
			continue
		}
		consecutiveErrors = 0

		for _, sqsMsg := range msgs {
			// Acquire semaphore slot
			select {
			case <-ctx.Done():
				return
			case rt.sem <- struct{}{}:
			}

			rt.wg.Add(1)
			go func(m sqstypes.Message) {
				defer func() {
					<-rt.sem
					rt.wg.Done()
				}()
				rt.processMessage(ctx, m)
			}(sqsMsg)
		}
	}
}

func (rt *Runtime) receive(ctx context.Context) ([]sqstypes.Message, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &rt.config.QueueURL,
		WaitTimeSeconds:     rt.config.WaitTimeSeconds,
		MaxNumberOfMessages: rt.config.MaxNumberOfMessages,
		VisibilityTimeout:   rt.config.VisibilityTimeoutSeconds,
		AttributeNames:      []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
			sqstypes.MessageSystemAttributeNameApproximateReceiveCount,
			sqstypes.MessageSystemAttributeNameMessageGroupId,
			sqstypes.MessageSystemAttributeNameMessageDeduplicationId,
		},
	}

	output, err := rt.client.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, err
	}
	return output.Messages, nil
}

func (rt *Runtime) processMessage(ctx context.Context, sqsMsg sqstypes.Message) {
	msgID := derefStr(sqsMsg.MessageId)
	receiptHandle := derefStr(sqsMsg.ReceiptHandle)
	body := derefStr(sqsMsg.Body)

	// OnReceive hook
	if rt.opts.hooks.OnReceive != nil {
		rt.opts.hooks.OnReceive(ctx, RawMessage{
			MessageID:     msgID,
			Body:          body,
			ReceiptHandle: receiptHandle,
		})
	}

	rt.logger.DebugContext(ctx, "relay: message received", "message_id", msgID)

	// Decode envelope
	var env Envelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		rt.logger.ErrorContext(ctx, "relay: envelope decode failure",
			"message_id", msgID,
			"error", err,
		)
		rt.deleteMessage(ctx, receiptHandle, msgID)
		return
	}

	if env.Type == "" {
		rt.logger.ErrorContext(ctx, "relay: envelope missing type field",
			"message_id", msgID,
		)
		rt.deleteMessage(ctx, receiptHandle, msgID)
		return
	}

	// Lookup handler
	entry, ok := rt.handlers[env.Type]
	if !ok {
		rt.logger.ErrorContext(ctx, "relay: unknown message type",
			"message_id", msgID,
			"message_type", env.Type,
		)
		// Discard: delete to prevent poison message loop
		rt.deleteMessage(ctx, receiptHandle, msgID)
		return
	}

	// Build metadata
	attempt := parseAttempt(sqsMsg.Attributes)
	meta := Metadata{
		Attempt:       attempt,
		EnqueuedAt:    env.Meta.EnqueuedAt,
		CorrelationID: env.Meta.CorrelationID,
		ReceiptHandle: receiptHandle,
	}
	if gid, ok := sqsMsg.Attributes["MessageGroupId"]; ok {
		meta.MessageGroupID = gid
	}
	if did, ok := sqsMsg.Attributes["MessageDeduplicationId"]; ok {
		meta.MessageDedupID = did
	}

	info := MessageInfo{
		ID:            msgID,
		Type:          env.Type,
		Attempt:       attempt,
		CorrelationID: env.Meta.CorrelationID,
		ReceiptHandle: receiptHandle,
	}

	// Start tracing span
	spanCtx, span := rt.tracer.Start(ctx, "relay.process",
		trace.WithAttributes(
			attribute.String("messaging.system", "sqs"),
			attribute.String("messaging.destination", rt.config.QueueURL),
			attribute.String("messaging.message.id", msgID),
			attribute.String("messaging.operation", "process"),
			attribute.String("relay.message.type", env.Type),
			attribute.Int("relay.attempt", attempt),
		),
	)
	defer span.End()

	// Register in-flight (for lease extension)
	var leaseCtx context.Context
	var leaseCancel context.CancelFunc
	if rt.config.LeaseExtensionInterval > 0 {
		leaseCtx, leaseCancel = context.WithCancel(context.Background())
		rt.registerInflight(msgID, receiptHandle, leaseCancel)
		go rt.extendLease(leaseCtx, msgID, receiptHandle, info)
	}

	// OnHandlerStart hook
	if rt.opts.hooks.OnHandlerStart != nil {
		rt.opts.hooks.OnHandlerStart(spanCtx, info)
	}

	// Execute handler with panic recovery
	handlerErr := rt.executeHandler(spanCtx, entry, msgID, &env, meta, info)

	// Stop lease extension
	if leaseCancel != nil {
		leaseCancel()
	}
	rt.unregisterInflight(msgID)

	// OnHandlerFinish hook
	if rt.opts.hooks.OnHandlerFinish != nil {
		rt.opts.hooks.OnHandlerFinish(spanCtx, info, handlerErr)
	}

	// Determine outcome
	switch {
	case handlerErr == nil:
		// Success: delete message
		rt.logger.InfoContext(spanCtx, "relay: handler success",
			"message_id", msgID,
			"message_type", env.Type,
		)
		span.SetStatus(codes.Ok, "")
		rt.deleteMessage(spanCtx, receiptHandle, msgID)

	case IsDiscard(handlerErr):
		// Discard: delete message, record discard
		rt.logger.WarnContext(spanCtx, "relay: handler discard",
			"message_id", msgID,
			"message_type", env.Type,
			"error", handlerErr,
		)
		span.SetStatus(codes.Ok, "discarded")
		span.SetAttributes(attribute.String("relay.result", "discard"))
		rt.deleteMessage(spanCtx, receiptHandle, msgID)

	default:
		// Retry (explicit or implicit): do NOT delete
		rt.logger.WarnContext(spanCtx, "relay: handler retry",
			"message_id", msgID,
			"message_type", env.Type,
			"error", handlerErr,
		)
		span.RecordError(handlerErr)
		span.SetStatus(codes.Error, "retry")
		span.SetAttributes(attribute.String("relay.result", "retry"))
	}
}

func (rt *Runtime) executeHandler(ctx context.Context, entry handlerEntry, msgID string, env *Envelope, meta Metadata, info MessageInfo) (handlerErr error) {
	defer func() {
		if r := recover(); r != nil {
			handlerErr = fmt.Errorf("relay: handler panicked: %v", r)
			rt.logger.ErrorContext(ctx, "relay: panic recovered",
				"message_id", info.ID,
				"message_type", info.Type,
				"recovered", r,
			)
			if rt.opts.hooks.OnPanic != nil {
				rt.opts.hooks.OnPanic(ctx, info, r)
			}
			// Panic → retry (do not delete)
		}
	}()

	return entry.fn(ctx, msgID, env, meta)
}

func (rt *Runtime) extendLease(ctx context.Context, msgID, receiptHandle string, info MessageInfo) {
	ticker := time.NewTicker(rt.config.LeaseExtensionInterval)
	defer ticker.Stop()

	startedAt := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check max processing time
			if rt.config.MaxProcessingTime > 0 && time.Since(startedAt) > rt.config.MaxProcessingTime {
				rt.logger.WarnContext(ctx, "relay: max processing time exceeded, stopping lease extension",
					"message_id", msgID,
					"elapsed", time.Since(startedAt),
				)
				return
			}

			_, err := rt.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          &rt.config.QueueURL,
				ReceiptHandle:     &receiptHandle,
				VisibilityTimeout: rt.config.VisibilityTimeoutSeconds,
			})
			if err != nil {
				rt.logger.ErrorContext(ctx, "relay: lease extension failed",
					"message_id", msgID,
					"error", err,
				)
				if rt.opts.hooks.OnLeaseExtendFail != nil {
					rt.opts.hooks.OnLeaseExtendFail(ctx, info, err)
				}
				// Continue — handler is still running
			}
		}
	}
}

func (rt *Runtime) registerInflight(msgID, receiptHandle string, cancel context.CancelFunc) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.inflight[msgID] = &inflightMsg{
		receiptHandle: receiptHandle,
		startedAt:     time.Now(),
		cancelLease:   cancel,
	}
}

func (rt *Runtime) unregisterInflight(msgID string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	delete(rt.inflight, msgID)
}

func (rt *Runtime) deleteMessage(ctx context.Context, receiptHandle, msgID string) {
	_, err := rt.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &rt.config.QueueURL,
		ReceiptHandle: &receiptHandle,
	})
	if err != nil {
		rt.logger.ErrorContext(ctx, "relay: delete message failed",
			"message_id", msgID,
			"error", err,
		)
	}
}

func (rt *Runtime) backoff(ctx context.Context, consecutiveErrors int) {
	delay := time.Duration(math.Min(
		float64(time.Second)*math.Pow(2, float64(consecutiveErrors-1)),
		float64(30*time.Second),
	))
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func parseAttempt(attrs map[string]string) int {
	if v, ok := attrs["ApproximateReceiveCount"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 1
}
