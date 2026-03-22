// Package relay is a thin worker runtime for Amazon SQS.
//
// relay standardizes the common concerns of SQS-based background job processing:
// long polling, message decoding, retry/discard control,
// visibility timeout extension, graceful shutdown, and observability.
//
// # Quick Start
//
//	rt := relay.New[SendEmail](sqsClient, relay.Config{
//	    QueueURL:    queueURL,
//	    Concurrency: 8,
//	}, func(ctx context.Context, msg relay.Message[SendEmail]) error {
//	    if err := emailService.Send(ctx, msg.Payload.UserID); err != nil {
//	        if errors.Is(err, domain.ErrInvalidUser) {
//	            return relay.Discard(err)
//	        }
//	        return err // plain error → retry
//	    }
//	    return nil // success → delete
//	})
//
//	if err := rt.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
// # Handler Return Semantics
//
//   - nil → success, message deleted
//   - relay.Discard(err) → message deleted, recorded as discard
//   - relay.Retry(err) → message NOT deleted, redelivered after visibility timeout
//   - any other error → treated as retry
//
// # Design Philosophy
//
// relay is SQS-specific (no multi-broker abstraction), thin (minimal API surface),
// and follows the "1 queue = 1 job type" best practice.
// Handlers should be idempotent (at-least-once delivery).
package relay
