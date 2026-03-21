package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/masp-92/relay"

// Runtime is the relay worker runtime.
// It polls SQS, decodes messages, dispatches to a typed handler,
// and manages lifecycle concerns (retry, discard, lease, shutdown).
type Runtime[T any] struct {
	client  SQSClient
	config  Config
	opts    options
	handler HandlerFunc[T]

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

// New creates a new Runtime with a typed handler. It does not start polling.
// The client must implement the SQSClient interface (e.g., sqs.Client from AWS SDK v2).
func New[T any](client SQSClient, cfg Config, handler HandlerFunc[T], opts ...Option) *Runtime[T] {
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

	return &Runtime[T]{
		client:   client,
		config:   cfg,
		opts:     o,
		handler:  handler,
		inflight: make(map[string]*inflightMsg),
		sem:      make(chan struct{}, cfg.Concurrency),
		logger:   o.logger.With("component", "relay"),
		tracer:   o.tracerProvider.Tracer(tracerName),
	}
}

// Run starts the polling loop and blocks until ctx is cancelled or a fatal error occurs.
// After ctx cancellation, it performs graceful shutdown.
func (rt *Runtime[T]) Run(ctx context.Context) error {
	if err := rt.config.validate(); err != nil {
		return err
	}

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

func (rt *Runtime[T]) pollLoop(ctx context.Context) {
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

func (rt *Runtime[T]) receive(ctx context.Context) ([]sqstypes.Message, error) {
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

func (rt *Runtime[T]) processMessage(ctx context.Context, sqsMsg sqstypes.Message) {
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

	// Decode body directly into T
	var payload T
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		rt.logger.ErrorContext(ctx, "relay: message decode failure",
			"message_id", msgID,
			"error", err,
		)
		rt.deleteMessage(ctx, receiptHandle, msgID)
		return
	}

	// Build metadata
	attempt := parseAttempt(sqsMsg.Attributes)
	meta := Metadata{
		Attempt:       attempt,
		ReceiptHandle: receiptHandle,
	}
	if gid, ok := sqsMsg.Attributes["MessageGroupId"]; ok {
		meta.MessageGroupID = gid
	}
	if did, ok := sqsMsg.Attributes["MessageDeduplicationId"]; ok {
		meta.MessageDedupID = did
	}

	msg := Message[T]{
		ID:       msgID,
		Payload:  payload,
		Metadata: meta,
	}

	info := MessageInfo{
		ID:            msgID,
		Attempt:       attempt,
		ReceiptHandle: receiptHandle,
	}

	// Start tracing span
	spanCtx, span := rt.tracer.Start(ctx, "relay.process",
		trace.WithAttributes(
			attribute.String("messaging.system", "sqs"),
			attribute.String("messaging.destination", rt.config.QueueURL),
			attribute.String("messaging.message.id", msgID),
			attribute.String("messaging.operation", "process"),
			attribute.Int("relay.attempt", attempt),
		),
	)
	defer span.End()

	// Register in-flight (for lease extension)
	var leaseCancel context.CancelFunc
	if rt.config.LeaseExtensionInterval > 0 {
		var leaseCtx context.Context
		leaseCtx, leaseCancel = context.WithCancel(context.Background())
		rt.registerInflight(msgID, receiptHandle, leaseCancel)
		go rt.extendLease(leaseCtx, msgID, receiptHandle, info)
	}

	// OnHandlerStart hook
	if rt.opts.hooks.OnHandlerStart != nil {
		rt.opts.hooks.OnHandlerStart(spanCtx, info)
	}

	// Execute handler with panic recovery
	handlerErr := rt.executeHandler(spanCtx, msg, info)

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
		rt.logger.InfoContext(spanCtx, "relay: handler success",
			"message_id", msgID,
		)
		span.SetStatus(codes.Ok, "")
		rt.deleteMessage(spanCtx, receiptHandle, msgID)

	case IsDiscard(handlerErr):
		rt.logger.WarnContext(spanCtx, "relay: handler discard",
			"message_id", msgID,
			"error", handlerErr,
		)
		span.SetStatus(codes.Ok, "discarded")
		span.SetAttributes(attribute.String("relay.result", "discard"))
		rt.deleteMessage(spanCtx, receiptHandle, msgID)

	default:
		rt.logger.WarnContext(spanCtx, "relay: handler retry",
			"message_id", msgID,
			"error", handlerErr,
		)
		span.RecordError(handlerErr)
		span.SetStatus(codes.Error, "retry")
		span.SetAttributes(attribute.String("relay.result", "retry"))
	}
}

func (rt *Runtime[T]) executeHandler(ctx context.Context, msg Message[T], info MessageInfo) (handlerErr error) {
	defer func() {
		if r := recover(); r != nil {
			handlerErr = fmt.Errorf("relay: handler panicked: %v", r)
			rt.logger.ErrorContext(ctx, "relay: panic recovered",
				"message_id", info.ID,
				"recovered", r,
			)
			if rt.opts.hooks.OnPanic != nil {
				rt.opts.hooks.OnPanic(ctx, info, r)
			}
			// Panic → retry (do not delete)
		}
	}()

	return rt.handler(ctx, msg)
}

func (rt *Runtime[T]) extendLease(ctx context.Context, msgID, receiptHandle string, info MessageInfo) {
	ticker := time.NewTicker(rt.config.LeaseExtensionInterval)
	defer ticker.Stop()

	startedAt := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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

func (rt *Runtime[T]) registerInflight(msgID, receiptHandle string, cancel context.CancelFunc) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.inflight[msgID] = &inflightMsg{
		receiptHandle: receiptHandle,
		startedAt:     time.Now(),
		cancelLease:   cancel,
	}
}

func (rt *Runtime[T]) unregisterInflight(msgID string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	delete(rt.inflight, msgID)
}

func (rt *Runtime[T]) deleteMessage(ctx context.Context, receiptHandle, msgID string) {
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

func (rt *Runtime[T]) backoff(ctx context.Context, consecutiveErrors int) {
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
