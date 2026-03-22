# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run a single test
go test -v -run TestHandleSuccess ./...

# Build
go build ./...

# Lint (uses standard Go tooling)
go vet ./...
```

## Architecture

**relay** is a thin SQS worker runtime for Go. All source files live at the module root (`github.com/masp-92/relay`).

### Core Components

- **`runtime.go`** — The central engine: long-poll loop, semaphore-based concurrency, handler dispatch, visibility timeout lease extension, graceful shutdown with WaitGroup drain.
- **`config.go`** — Configuration with defaults (Concurrency=1, WaitTimeSeconds=20, MaxNumberOfMessages=1, VisibilityTimeoutSeconds=30, ShutdownTimeout=30s).
- **`message.go`** — Wire types: `Envelope` (type + payload + meta), `Message[T]` (parsed), `Metadata`.
- **`error.go`** — `Retry(err)` / `Discard(err)` wrappers; plain `error` defaults to retry.
- **`hooks.go`** — Lifecycle callbacks: `OnReceive`, `OnHandlerStart`, `OnHandlerFinish`, `OnPanic`, `OnLeaseExtendFail`.
- **`sqs_client.go`** — `SQSClient` interface abstracting AWS SDK calls (used for testing).

### Handler Registration

Because Go doesn't support generic methods, handlers are registered via a package-level function:
```go
relay.Handle[T](rt *Runtime, messageType string, fn HandlerFunc[T])
```
This unmarshals `envelope.payload` into `T` before calling `fn`.

### Error Semantics

| Return value | Outcome |
|---|---|
| `nil` | Message deleted (success) |
| `relay.Discard(err)` | Message deleted (permanent failure) |
| `relay.Retry(err)` | Message kept (explicit retry) |
| plain `error` | Message kept (implicit retry) |

### Message Envelope (wire format)

```json
{ "type": "send_email", "payload": {...}, "meta": { "correlation_id": "...", "enqueued_at": "..." } }
```

### Observability

- **Logging**: `log/slog` (stdlib), injected via `relay.WithLogger`.
- **Tracing**: OpenTelemetry — one span per message (`relay.process`), injected via `relay.WithTracerProvider`. Follows OTel Messaging Semantic Conventions.
- **Metrics**: No built-in metrics; implement via Hooks.
