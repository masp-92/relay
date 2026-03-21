package relay

import "context"

// HandlerFunc is the signature for a typed message handler.
type HandlerFunc[T any] func(context.Context, Message[T]) error
