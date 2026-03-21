# Hooks

relay の `Hooks` は、runtime のライフサイクルイベントにコールバックを差し込む仕組み。
v1 では middleware chain ではなく、単一の hook 方式を採用している。

## 使い方

```go
rt := relay.New(client, cfg, relay.WithHooks(relay.Hooks{
    OnReceive: func(ctx context.Context, msg relay.RawMessage) {
        slog.Debug("received", "message_id", msg.MessageID)
    },
    OnHandlerStart: func(ctx context.Context, info relay.MessageInfo) {
        slog.Info("processing", "type", info.Type, "attempt", info.Attempt)
    },
    OnHandlerFinish: func(ctx context.Context, info relay.MessageInfo, err error) {
        recordMetrics(info, err)
    },
    OnPanic: func(ctx context.Context, info relay.MessageInfo, recovered any) {
        alerting.Send(fmt.Sprintf("panic in %s: %v", info.Type, recovered))
    },
    OnLeaseExtendFail: func(ctx context.Context, info relay.MessageInfo, err error) {
        slog.Warn("lease extend failed", "message_id", info.ID, "error", err)
    },
}))
```

すべてのフィールドはオプション。nil の hook は呼ばれない。

## Hook 一覧

### `OnReceive`

```go
OnReceive func(ctx context.Context, msg RawMessage)
```

SQS からメッセージを受信した直後、decode 前に呼ばれる。
`RawMessage` には生の Body 文字列が含まれる。

用途:
- raw メッセージの監査ログ
- メッセージ受信数のカスタムメトリクス

### `OnHandlerStart`

```go
OnHandlerStart func(ctx context.Context, info MessageInfo)
```

handler 関数を呼び出す直前に呼ばれる。

用途:
- handler 実行の開始ログ
- タイマー開始

### `OnHandlerFinish`

```go
OnHandlerFinish func(ctx context.Context, info MessageInfo, err error)
```

handler 関数が完了した直後に呼ばれる。
`err` は handler の戻り値そのもの（nil / Retry / Discard / plain error）。

用途:
- メトリクス記録（`IsRetry(err)` / `IsDiscard(err)` で分岐）
- handler duration の計測
- カスタムアラート条件

### `OnPanic`

```go
OnPanic func(ctx context.Context, info MessageInfo, recovered any)
```

handler 内で panic が発生し、runtime が recover した後に呼ばれる。

用途:
- panic の外部通知（Sentry, PagerDuty 等）
- panic 発生回数のメトリクス

### `OnLeaseExtendFail`

```go
OnLeaseExtendFail func(ctx context.Context, info MessageInfo, err error)
```

visibility timeout の延長が失敗した時に呼ばれる。
処理自体は継続するが、重複配信のリスクがある。

用途:
- 延長失敗のアラート
- 重複配信リスクの可視化

## 呼び出し順序

正常系:

```
OnReceive → OnHandlerStart → (handler 実行) → OnHandlerFinish
```

panic 時:

```
OnReceive → OnHandlerStart → (handler panic) → OnPanic → OnHandlerFinish
```

## MessageInfo

Hook に渡される `MessageInfo` は読み取り専用のメッセージ要約。

```go
type MessageInfo struct {
    ID            string // SQS MessageId
    Type          string // message type (dispatch key)
    Attempt       int    // ApproximateReceiveCount
    CorrelationID string // envelope の correlation_id
    ReceiptHandle string // SQS receipt handle
}
```

payload は含まない。型情報が消えているため、hook 側では payload にアクセスしない設計。

## メトリクス hook の例

```go
func metricsHooks(m *metrics.Recorder) relay.Hooks {
    return relay.Hooks{
        OnHandlerFinish: func(ctx context.Context, info relay.MessageInfo, err error) {
            result := "success"
            switch {
            case err == nil:
                result = "success"
            case relay.IsDiscard(err):
                result = "discard"
            case relay.IsRetry(err):
                result = "retry"
            default:
                result = "retry" // plain error = implicit retry
            }
            m.Inc("relay_messages_processed_total", info.Type, result)
        },
        OnPanic: func(ctx context.Context, info relay.MessageInfo, recovered any) {
            m.Inc("relay_handler_panics_total", info.Type)
        },
    }
}
```
