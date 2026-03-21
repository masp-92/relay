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
