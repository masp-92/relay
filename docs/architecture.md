# アーキテクチャ

## 全体フロー

```
Producer ─── SendMessage ───▶ SQS Queue
                                  │
                                  ▼
                          ┌──────────────┐
                          │   Runtime    │
                          │              │
                          │  poll loop   │──── ReceiveMessage (long poll)
                          │      │       │
                          │      ▼       │
                          │   decode     │──── decoder(body) → T
                          │      │       │
                          │   ┌──┴──┐    │
                          │   │ sem │    │──── concurrency control
                          │   └──┬──┘    │
                          │      ▼       │
                          │  handler()   │──── goroutine per message
                          │      │       │
                          │      ▼       │
                          │   outcome    │──── nil/Retry/Discard → delete or leave
                          └──────────────┘
```

## コンポーネント

### Runtime (`runtime.go`)

中核。以下を統合する単一の struct:

- **Poll Loop**: SQS long polling で継続的にメッセージを受信。receive エラー時は指数 backoff。
- **Semaphore**: `chan struct{}` で concurrency を制御。`Config.Concurrency` 数ぶんの goroutine のみ並行実行。
- **Decoder**: `decoder(body) → T` で SQS Body を payload 型にデコード。デフォルトは JSON、`WithDecoder` で差し替え可能。
- **Lease Extender**: 処理中メッセージの visibility timeout を定期延長する goroutine。handler 完了時に cancel。
- **In-flight Tracker**: `sync.Mutex` + `map` で処理中メッセージを管理。shutdown の drain に利用。
- **Shutdown**: `ctx` cancel → polling 停止 → `sync.WaitGroup` で in-flight 完了待ち → timeout で打ち切り。

### Handler

```
New[T any](client, cfg, fn, opts...)
    ↓
Runtime[T]{
    handler: fn,
    decoder: Decoder[T],   ← デフォルト JSON、WithDecoder で差し替え
}
    ↓ processMessage
decoder(body) → T
    ↓
fn(ctx, Message[T]{...})
```

1 queue = 1 handler。`Runtime[T]` は型パラメータ T を持つ。

### Error Flow

```
handler return value
    │
    ├── nil           → deleteMessage()
    ├── Discard(err)  → deleteMessage() + warn log
    ├── Retry(err)    → (何もしない = SQS が再配信)
    └── other error   → (何もしない = SQS が再配信)
```

`Retry` / `Discard` は `errors.As` で判定できる error wrapper。
plain error がリトライになる設計は、transient failure が最も頻出するケースを最短で書けるようにするため。

### Lease Extension

```
handler goroutine         lease goroutine
      │                        │
      ├── start ──────────────▶├── ticker start
      │                        │
      │  (working...)          ├── tick → ChangeMessageVisibility
      │                        ├── tick → ChangeMessageVisibility
      │                        │
      ├── done ───────────────▶├── ctx cancel → exit
      │
      ▼
   outcome
```

- `LeaseExtensionInterval` ごとに `ChangeMessageVisibility` を呼ぶ
- `MaxProcessingTime` 超過で延長を停止（handler は kill しない）
- 延長失敗は hook で通知、処理は継続

### Shutdown Sequence

```
1. ctx cancel
2. pollLoop returns (ReceiveMessage が ctx で中断)
3. wg.Wait() ← in-flight handler の完了を待つ
4. ShutdownTimeout 経過で待機打ち切り
5. 未完了メッセージは SQS が visibility timeout 後に再配信
```

## パッケージ構成

```
relay/
├── doc.go           パッケージドキュメント
├── runtime.go       Runtime[T], New, Run, pollLoop, processMessage
├── config.go        Config, setDefaults, validate
├── message.go       Message[T], Metadata, MessageInfo, RawMessage
├── error.go         Retry, Discard, IsRetry, IsDiscard
├── handler.go       HandlerFunc[T] 型定義
├── hooks.go         Hooks struct
├── option.go        Option, Decoder[T], WithDecoder, WithHooks, WithLogger, WithTracerProvider
├── sqs_client.go    SQSClient interface
└── internal/        (将来の切り出し先)
    ├── lease/
    ├── polling/
    └── decode/
```

現状 `runtime.go` に主要ロジックが集約されている。
ファイルが肥大化した場合、internal 以下に切り出す。
公開 API は root パッケージに残し、internal は実装詳細とする。

## SQS Client Interface

```go
type SQSClient interface {
    ReceiveMessage(...)
    DeleteMessage(...)
    ChangeMessageVisibility(...)
}
```

AWS SDK v2 の `*sqs.Client` がそのまま満たす。
テスト時は mock 実装を渡す。SendMessage は worker runtime の責務外。

## Tracing 設計

OpenTelemetry を直接利用。抽象化しない。

- 1 message = 1 span (`relay.process`)
- span attributes は [OpenTelemetry Messaging Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/messaging/) に準拠
- `WithTracerProvider` で DI。デフォルトは `otel.GetTracerProvider()`
