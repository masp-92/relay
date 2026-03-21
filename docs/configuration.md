# 設定リファレンス

## Config

`relay.Config` は Runtime の動作を制御する構造体。

```go
rt := relay.New(sqsClient, relay.Config{
    QueueURL:                 "https://sqs.ap-northeast-1.amazonaws.com/123456789012/my-queue",
    Concurrency:              8,
    WaitTimeSeconds:          20,
    MaxNumberOfMessages:      8,
    VisibilityTimeoutSeconds: 60,
    ShutdownTimeout:          30 * time.Second,
    LeaseExtensionInterval:   20 * time.Second,
    MaxProcessingTime:        5 * time.Minute,
})
```

### フィールド一覧

| フィールド | 型 | 必須 | デフォルト | 制約 | 説明 |
|---|---|---|---|---|---|
| `QueueURL` | `string` | ✅ | — | — | SQS キュー URL |
| `Concurrency` | `int` | — | `1` | `>= 1` | 並行実行する handler の goroutine 数 |
| `WaitTimeSeconds` | `int32` | — | `20` | `0..20` | SQS long polling の待機秒数 |
| `MaxNumberOfMessages` | `int32` | — | `1` | `1..10` | 1 回の ReceiveMessage で取得する最大メッセージ数 |
| `VisibilityTimeoutSeconds` | `int32` | — | `30` | SQS 制約内 | メッセージの visibility timeout（秒） |
| `ShutdownTimeout` | `time.Duration` | — | `30s` | — | graceful shutdown の待機上限 |
| `LeaseExtensionInterval` | `time.Duration` | — | `10s` | `< VisibilityTimeoutSeconds` | visibility timeout 延長の間隔。`0` で無効 |
| `MaxProcessingTime` | `time.Duration` | — | `0` (無制限) | — | 延長を続ける上限時間。超過後は延長停止（handler は継続） |

### バリデーション

`Run()` 呼び出し時に以下をチェックする。違反時は error を返す。

- `QueueURL` が空でないこと
- `Concurrency >= 1`
- `MaxNumberOfMessages` が 1〜10
- `WaitTimeSeconds` が 0〜20
- `LeaseExtensionInterval < VisibilityTimeoutSeconds`（両方 > 0 の場合）

## Option

`relay.New` の第 3 引数以降に渡す functional option。

### `WithLogger(l *slog.Logger)`

構造化ロガーを指定する。省略時は `slog.Default()`。

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
rt := relay.New(client, cfg, relay.WithLogger(logger))
```

### `WithTracerProvider(tp trace.TracerProvider)`

OpenTelemetry の TracerProvider を指定する。省略時は `otel.GetTracerProvider()`。

```go
tp := sdktrace.NewTracerProvider(...)
rt := relay.New(client, cfg, relay.WithTracerProvider(tp))
```

### `WithHooks(h Hooks)`

ライフサイクル hook を指定する。詳細は [hooks.md](./hooks.md) を参照。

```go
rt := relay.New(client, cfg, relay.WithHooks(relay.Hooks{
    OnHandlerFinish: func(ctx context.Context, info relay.MessageInfo, err error) {
        metricsRecorder.Record(info.Type, err)
    },
}))
```

## チューニングガイド

### 高スループット

```go
relay.Config{
    Concurrency:         16,
    MaxNumberOfMessages: 10,
}
```

`Concurrency` を上げて並列度を確保し、`MaxNumberOfMessages` で 1 回の poll あたりの取得数を最大化する。

### 長時間処理

```go
relay.Config{
    VisibilityTimeoutSeconds: 300,         // 5 分
    LeaseExtensionInterval:   60 * time.Second,
    MaxProcessingTime:        30 * time.Minute,
}
```

`LeaseExtensionInterval` は `VisibilityTimeoutSeconds` の半分以下を推奨。
余裕を持たせることで、延長 API の一時的な失敗に耐えられる。

### 短い handler（低レイテンシ）

```go
relay.Config{
    VisibilityTimeoutSeconds: 10,
    LeaseExtensionInterval:   0, // 延長不要
}
```

処理が確実に短い場合は lease extension を無効にしてオーバーヘッドを削減。
