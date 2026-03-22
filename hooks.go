package relay

import "context"

// Hooks provides lifecycle callbacks for the runtime.
// All fields are optional. A nil hook is simply not called.
type Hooks struct {
	// OnReceive is called for each raw SQS message before decoding.
	OnReceive func(ctx context.Context, msg RawMessage)

	// OnHandlerStart is called just before a handler is invoked.
	OnHandlerStart func(ctx context.Context, info MessageInfo)

	// OnHandlerFinish is called after a handler completes (success, retry, or discard).
	// The error is the original handler return value (may be nil).
	OnHandlerFinish func(ctx context.Context, info MessageInfo, err error)

	// OnPanic is called when a handler panics. The recovered value is passed.
	OnPanic func(ctx context.Context, info MessageInfo, recovered any)

	// OnLeaseExtendFail is called when a visibility timeout extension fails.
	OnLeaseExtendFail func(ctx context.Context, info MessageInfo, err error)
}
