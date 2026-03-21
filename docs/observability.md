# Observability

relay は logging, metrics (hooks 経由), tracing を v1 の必須機能として提供する。

## Logging

`log/slog` ベースの構造化ログ。

```go
rt := relay.New(client, cfg, relay.WithLogger(
    slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
        Level: slog.LevelDebug,
    })),
))
```

省略時は `slog.Default()`。

### ログイベント一覧

| イベント | レベル | タイミング |
|---|---|---|
| `relay runtime starting` | INFO | Run 開始時 |
| `relay runtime shutting down` | INFO | ctx cancel 後 |
| `relay runtime shutdown complete` | INFO | 全 handler drain 完了 |
| `relay runtime shutdown timeout` | WARN | ShutdownTimeout 超過 |
| `relay: message received` | DEBUG | メッセージ受信時 |
| `relay: handler success` | INFO | handler が nil を返した時 |
| `relay: handler retry` | WARN | handler が retry を返した時 |
| `relay: handler discard` | WARN | handler が discard を返した時 |
| `relay: envelope decode failure` | ERROR | JSON decode 失敗 |
| `relay: envelope missing type field` | ERROR | type フィールドなし |
| `relay: unknown message type` | ERROR | 未登録 type |
| `relay: panic recovered` | ERROR | handler panic |
| `relay: receive error` | ERROR | SQS ReceiveMessage 失敗 |
| `relay: delete message failed` | ERROR | SQS DeleteMessage 失敗 |
| `relay: lease extension failed` | ERROR | ChangeMessageVisibility 失敗 |
| `relay: max processing time exceeded` | WARN | 延長上限到達 |

共通フィールド: `message_id`, `message_type`, `error` (該当時)

## Tracing

OpenTelemetry を直接利用。

### セットアップ

```go
tp := sdktrace.NewTracerProvider(
    sdktrace.WithBatcher(exporter),
    sdktrace.WithResource(resource),
)
rt := relay.New(client, cfg, relay.WithTracerProvider(tp))
```

省略時は `otel.GetTracerProvider()`。
OTel SDK 未初期化の場合は noop tracer が使われ、オーバーヘッドなし。

### Span

1 メッセージにつき 1 span を生成。

- **Span name:** `relay.process`
- **Tracer name:** `github.com/yourorg/relay`

### Attributes

[OpenTelemetry Messaging Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/messaging/) に準拠。

| Attribute | 型 | 例 |
|---|---|---|
| `messaging.system` | string | `sqs` |
| `messaging.destination` | string | queue URL |
| `messaging.message.id` | string | SQS MessageId |
| `messaging.operation` | string | `process` |
| `relay.message.type` | string | `send_email` |
| `relay.attempt` | int | `1` |
| `relay.result` | string | `retry` / `discard` (失敗時のみ) |

### Span Status

| handler 結果 | span status | 備考 |
|---|---|---|
| nil (成功) | OK | — |
| Discard(err) | OK | `relay.result = "discard"` |
| Retry / error | Error | `RecordError(err)` |

### Datadog APM との連携

OTel SDK + Datadog exporter の構成で利用可能。

```go
import ddotel "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentelemetry"

tp := ddotel.NewTracerProvider()
rt := relay.New(client, cfg, relay.WithTracerProvider(tp))
```

## Metrics

v1 の runtime 本体には Prometheus registry 等への直接依存はない。
メトリクスは **Hooks 経由** で実装する。

### 推奨メトリクス

| メトリクス名 | 型 | ラベル |
|---|---|---|
| `relay_messages_received_total` | counter | `queue` |
| `relay_messages_processed_total` | counter | `queue`, `message_type`, `result` |
| `relay_messages_succeeded_total` | counter | `queue`, `message_type` |
| `relay_messages_retried_total` | counter | `queue`, `message_type` |
| `relay_messages_discarded_total` | counter | `queue`, `message_type` |
| `relay_messages_decode_failed_total` | counter | `queue` |
| `relay_messages_unknown_type_total` | counter | `queue` |
| `relay_handler_duration_seconds` | histogram | `queue`, `message_type`, `result` |
| `relay_inflight_messages` | gauge | `queue` |
| `relay_receive_errors_total` | counter | `queue` |
| `relay_visibility_extensions_total` | counter | `queue` |
| `relay_visibility_extension_errors_total` | counter | `queue` |
| `relay_handler_panics_total` | counter | `queue`, `message_type` |

### Hook を使ったメトリクス実装例

```go
func newMetricsHooks(reg prometheus.Registerer) relay.Hooks {
    processed := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
        Name: "relay_messages_processed_total",
    }, []string{"message_type", "result"})

    duration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
        Name:    "relay_handler_duration_seconds",
        Buckets: prometheus.DefBuckets,
    }, []string{"message_type"})

    starts := map[string]time.Time{}
    var mu sync.Mutex

    return relay.Hooks{
        OnHandlerStart: func(ctx context.Context, info relay.MessageInfo) {
            mu.Lock()
            starts[info.ID] = time.Now()
            mu.Unlock()
        },
        OnHandlerFinish: func(ctx context.Context, info relay.MessageInfo, err error) {
            result := "success"
            switch {
            case err == nil:
                // success
            case relay.IsDiscard(err):
                result = "discard"
            default:
                result = "retry"
            }
            processed.WithLabelValues(info.Type, result).Inc()

            mu.Lock()
            if start, ok := starts[info.ID]; ok {
                duration.WithLabelValues(info.Type).Observe(time.Since(start).Seconds())
                delete(starts, info.ID)
            }
            mu.Unlock()
        },
    }
}
```
