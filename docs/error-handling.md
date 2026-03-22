# エラーハンドリング

## handler の戻り値

relay の handler は `error` を返す。戻り値によってメッセージの処理結果が決まる。

```
nil           → 成功。メッセージを削除。
Retry(err)    → 明示リトライ。メッセージを削除しない。
Discard(err)  → 永続破棄。メッセージを削除。
その他の error → 暗黙リトライ。メッセージを削除しない。
```

### 成功

```go
return nil
```

メッセージは SQS から削除される。

### 明示リトライ

```go
return relay.Retry(err)
```

メッセージは削除されない。SQS の visibility timeout 経過後に再配信される。
「この error はリトライすべき」と明示的に伝える場合に使う。

### 永続破棄

```go
return relay.Discard(err)
```

メッセージは削除される。discard としてログ・メトリクスに記録される。
修復不能な失敗（不正データ、存在しないリソースへの参照など）に使う。

### 暗黙リトライ（plain error）

```go
return err
```

`Retry` でも `Discard` でもない素の error はリトライ扱い。
これがデフォルトポリシー。transient failure（タイムアウト、一時的な接続エラーなど）を最短で書ける。

## 判定関数

```go
relay.IsRetry(err)   // true if Retry-wrapped
relay.IsDiscard(err) // true if Discard-wrapped
```

`errors.As` ベース。error chain を辿って判定する。

## 典型パターン

### transient failure はそのまま返す

```go
relay.Handle(rt, "send_email", func(ctx context.Context, msg relay.Message[SendEmail]) error {
    return emailService.Send(ctx, msg.Payload.UserID)
    // error → 暗黙リトライ
    // nil → 成功
})
```

### permanent failure を分岐

```go
relay.Handle(rt, "send_email", func(ctx context.Context, msg relay.Message[SendEmail]) error {
    err := emailService.Send(ctx, msg.Payload.UserID)
    if err != nil {
        if errors.Is(err, domain.ErrUserNotFound) {
            return relay.Discard(err) // このユーザーは存在しない → 破棄
        }
        return err // その他 → リトライ
    }
    return nil
})
```

### リトライ回数で判断

```go
relay.Handle(rt, "webhook_retry", func(ctx context.Context, msg relay.Message[Webhook]) error {
    err := httpClient.Post(ctx, msg.Payload.URL, msg.Payload.Body)
    if err != nil {
        if msg.Metadata.Attempt >= 5 {
            return relay.Discard(fmt.Errorf("max retries exceeded: %w", err))
        }
        return relay.Retry(err)
    }
    return nil
})
```

`Metadata.Attempt` は SQS の `ApproximateReceiveCount`。
厳密な回数制限が必要なら DLQ の `maxReceiveCount` を併用する。

### panic

handler 内の panic は runtime が recover する。リトライ扱い（メッセージを削除しない）。

```go
relay.Handle(rt, "risky_job", func(ctx context.Context, msg relay.Message[Risky]) error {
    riskyOperation() // panic しても runtime はクラッシュしない
    return nil
})
```

## Error API

```go
// Retry は err をリトライ指示でラップする。nil を渡すと nil を返す。
func Retry(err error) error

// Discard は err を破棄指示でラップする。nil を渡すと nil を返す。
func Discard(err error) error

// IsRetry は err (またはその chain) がリトライ指示かを判定する。
func IsRetry(err error) bool

// IsDiscard は err (またはその chain) が破棄指示かを判定する。
func IsDiscard(err error) bool
```

`Retry(nil)` と `Discard(nil)` は `nil` を返す。nil を誤ってラップしないための安全策。

## runtime レベルのエラー

handler 以外のエラーは runtime が自動処理する。

| エラー | 動作 | runtime |
|---|---|---|
| SQS receive 失敗 | 指数 backoff (上限 30s) → polling 継続 | 継続 |
| SQS delete 失敗 | エラーログ。重複配信の可能性あり | 継続 |
| lease 延長失敗 | エラーログ + OnLeaseExtendFail hook。処理継続 | 継続 |
| envelope decode 失敗 | discard（削除） + エラーログ | 継続 |
| 未登録 message type | discard（削除） + エラーログ | 継続 |

runtime 自体が停止するのは `ctx` cancel 時のみ。SQS の一時的な障害で runtime が死ぬことはない。
