# relay

SQS 上で typed message handler を安全に実行する、Go らしい薄い worker runtime。

## なぜ relay?

SQS worker を書くたびに同じボイラープレートを書いていませんか?

- long polling ループ
- JSON decode → dispatch
- retry / discard の判定
- visibility timeout の延長
- graceful shutdown
- panic recovery
- ログ / メトリクス / トレース

relay はこれらを 1 つの runtime に集約し、あなたが書くのは **handler だけ** にします。

## インストール

```
go get github.com/masp-92/relay
```

## クイックスタート

```go
package main

import (
    "context"
    "errors"
    "log"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    "github.com/yourorg/relay"
)

type SendEmail struct {
    UserID int64  `json:"user_id"`
    Kind   string `json:"kind"`
}

func main() {
    ctx := context.Background()
    cfg, _ := config.LoadDefaultConfig(ctx)
    sqsClient := sqs.NewFromConfig(cfg)

    rt := relay.New(sqsClient, relay.Config{
        QueueURL:    "https://sqs.ap-northeast-1.amazonaws.com/123456789012/my-queue",
        Concurrency: 8,
    })

    relay.Handle(rt, "send_email", func(ctx context.Context, msg relay.Message[SendEmail]) error {
        if err := sendEmail(ctx, msg.Payload.UserID, msg.Payload.Kind); err != nil {
            if errors.Is(err, errInvalidUser) {
                return relay.Discard(err) // 永続的な失敗 → 削除
            }
            return err // 一時的な失敗 → リトライ
        }
        return nil // 成功 → 削除
    })

    if err := rt.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

## メッセージフォーマット

SQS の Body に以下の JSON envelope を期待します。

```json
{
  "type": "send_email",
  "payload": {
    "user_id": 123,
    "kind": "welcome"
  },
  "meta": {
    "correlation_id": "req-abc-123",
    "enqueued_at": "2026-03-21T10:00:00Z"
  }
}
```

| フィールド | 必須 | 説明 |
|---|---|---|
| `type` | ✅ | handler dispatch に使う文字列キー |
| `payload` | ✅ | handler の型パラメータ `T` にデコードされる |
| `meta.correlation_id` | — | トレース用の相関 ID |
| `meta.enqueued_at` | — | エンキュー時刻 |

## Handler の戻り値

| 戻り値 | 動作 | SQS メッセージ |
|---|---|---|
| `nil` | 成功 | 削除 |
| `relay.Retry(err)` | 明示リトライ | 削除しない（再配信） |
| `relay.Discard(err)` | 永続的に破棄 | 削除 |
| その他の `error` | 暗黙リトライ | 削除しない（再配信） |

デフォルトポリシー: **plain error はリトライ扱い**。transient failure を最短コードで書けます。

## 設定

```go
relay.Config{
    QueueURL:                 "https://sqs...",  // 必須
    Concurrency:              8,                 // default: 1
    WaitTimeSeconds:          20,                // default: 20 (max 20)
    MaxNumberOfMessages:      10,                // default: 1 (max 10)
    VisibilityTimeoutSeconds: 60,                // default: 30
    ShutdownTimeout:          30 * time.Second,  // default: 30s
    LeaseExtensionInterval:   20 * time.Second,  // default: 10s (0 で無効)
    MaxProcessingTime:        5 * time.Minute,   // default: 0 (無制限)
}
```

## 機能

### Typed Handler (Generics)

Go の generics を使い、payload を型安全にデコードします。

```go
relay.Handle(rt, "sync_inventory", func(ctx context.Context, msg relay.Message[SyncInventory]) error {
    // msg.Payload は SyncInventory 型
    return inventoryService.Sync(ctx, msg.Payload.ProductID)
})
```

> **Note:** Go はジェネリックメソッドをサポートしないため、`Handle` はパッケージレベル関数です。

### Visibility Timeout 延長 (Lease Extension)

長時間処理のメッセージが途中で再配信されるのを防ぎます。

```go
relay.Config{
    VisibilityTimeoutSeconds: 60,
    LeaseExtensionInterval:   20 * time.Second, // 20秒ごとに延長
    MaxProcessingTime:        10 * time.Minute,  // 10分で延長停止
}
```

`MaxProcessingTime` を超えても handler は kill されません。延長が停止するだけです。

### Graceful Shutdown

`ctx` がキャンセルされると:

1. 新規 polling を停止
2. 処理中の handler の完了を待機
3. `ShutdownTimeout` 経過で待機を打ち切り（未完了メッセージは SQS が再配信）

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
defer stop()
rt.Run(ctx)
```

### Panic Recovery

handler 内の panic は runtime が recover し、リトライ扱いにします。runtime 全体はクラッシュしません。

### Hooks

ライフサイクルイベントに hook を差し込めます。

```go
rt := relay.New(client, cfg, relay.WithHooks(relay.Hooks{
    OnReceive:         func(ctx context.Context, msg relay.RawMessage) { /* ... */ },
    OnHandlerStart:    func(ctx context.Context, info relay.MessageInfo) { /* ... */ },
    OnHandlerFinish:   func(ctx context.Context, info relay.MessageInfo, err error) { /* ... */ },
    OnPanic:           func(ctx context.Context, info relay.MessageInfo, recovered any) { /* ... */ },
    OnLeaseExtendFail: func(ctx context.Context, info relay.MessageInfo, err error) { /* ... */ },
}))
```

### OpenTelemetry Tracing

1 メッセージにつき 1 span (`relay.process`) を自動生成します。

| Attribute | 例 |
|---|---|
| `messaging.system` | `sqs` |
| `messaging.destination` | queue URL |
| `messaging.message.id` | SQS MessageId |
| `relay.message.type` | `send_email` |
| `relay.attempt` | `1` |
| `relay.result` | `retry` / `discard` |

```go
rt := relay.New(client, cfg, relay.WithTracerProvider(tp))
```

### 構造化ログ

`log/slog` ベース。カスタムロガーを渡せます。

```go
rt := relay.New(client, cfg, relay.WithLogger(slog.New(handler)))
```

## エッジケースの処理

| ケース | 動作 |
|---|---|
| JSON decode 失敗 | 削除（discard） + エラーログ |
| `type` フィールドなし | 削除（discard） + エラーログ |
| 未登録の message type | 削除（discard） + エラーログ |
| handler panic | recover → リトライ扱い |
| SQS receive エラー | 指数 backoff (上限 30s) → polling 継続 |
| SQS delete エラー | エラーログ → runtime 継続（重複配信の可能性あり） |
| lease 延長エラー | エラーログ → 処理継続（重複配信の可能性あり） |

## 設計判断

| 判断 | 理由 |
|---|---|
| SQS 専用、multi-broker 抽象なし | v1 は specific-driven。使わない抽象化は作らない |
| at-least-once 前提 | SQS の delivery semantics。handler は idempotent に |
| plain error = retry | transient failure が最も多いケース。最短コードで書ける |
| 未登録 type = discard | retry し続けると poison message 化するため |
| handler kill なし | goroutine の強制停止は Go にはない。延長停止のみ |
| `Handle` がパッケージ関数 | Go がジェネリックメソッドをサポートしない制約 |

## Non-Goals

relay は以下を **提供しません**:

- workflow / DAG / job chaining
- result backend
- 管理 UI / cron scheduler
- exactly-once delivery
- multi-broker abstraction
- enqueue API (v1)

## ライセンス

MIT
