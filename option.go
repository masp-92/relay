package relay

import (
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// Option configures a Runtime.
type Option func(*options)

type options struct {
	hooks          Hooks
	logger         *slog.Logger
	tracerProvider trace.TracerProvider
	decoder        any // Decoder[T], type-erased; T is resolved in New[T]
}

// Decoder decodes a raw SQS message body string into T.
// The default decoder unmarshals JSON.
// Use WithDecoder to provide a custom implementation (e.g., protobuf + base64).
type Decoder[T any] func(body string) (T, error)

// WithDecoder sets a custom message body decoder.
//
// Example (protobuf + base64):
//
//	relay.WithDecoder(func(body string) (*pb.SendEmail, error) {
//	    b, err := base64.StdEncoding.DecodeString(body)
//	    if err != nil {
//	        return nil, err
//	    }
//	    msg := &pb.SendEmail{}
//	    return msg, proto.Unmarshal(b, msg)
//	})
func WithDecoder[T any](d Decoder[T]) Option {
	return func(o *options) {
		o.decoder = d
	}
}

// WithHooks sets lifecycle hooks on the runtime.
func WithHooks(h Hooks) Option {
	return func(o *options) {
		o.hooks = h
	}
}

// WithLogger sets the structured logger. Default: slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithTracerProvider sets the OpenTelemetry tracer provider.
// Default: otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) {
		o.tracerProvider = tp
	}
}
