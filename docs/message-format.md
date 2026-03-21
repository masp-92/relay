# メッセージ仕様

## Envelope フォーマット

relay は SQS メッセージの Body に以下の JSON envelope を期待する。

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

### スキーマ

```go
type Envelope struct {
    Type    string          `json:"type"`
    Payload json.RawMessage `json:"payload"`
    Meta    EnvelopeMeta    `json:"meta,omitempty"`
}

type EnvelopeMeta struct {
    CorrelationID string    `json:"correlation_id,omitempty"`
    EnqueuedAt    time.Time `json:"enqueued_at,omitempty"`
}
```

### フィールド

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| `type` | `string` | ✅ | handler dispatch に使うメッセージ種別。例: `send_email`, `sync_inventory` |
| `payload` | `object` | ✅ | handler の型パラメータ `T` にデコードされるアプリケーションデータ |
| `meta.correlation_id` | `string` | — | リクエストトレーシング用の相関 ID |
| `meta.enqueued_at` | `RFC3339` | — | メッセージがキューに投入された時刻 |

### 型識別

`type` フィールドは明示的な文字列。Go の型名からの自動導出はしない。

推奨命名規則: `snake_case`

```
send_email
sync_inventory
webhook_retry
process_ocr
```

## Handler が受け取る型

```go
type Message[T any] struct {
    ID       string    // SQS MessageId
    Type     string    // envelope の type
    Payload  T         // デコード済み payload
    Metadata Metadata
}

type Metadata struct {
    Attempt        int               // SQS ApproximateReceiveCount
    EnqueuedAt     time.Time         // envelope meta から
    CorrelationID  string            // envelope meta から
    ReceiptHandle  string            // SQS receipt handle（デバッグ用）
    MessageGroupID string            // FIFO queue 用
    MessageDedupID string            // FIFO queue 用
    Attributes     map[string]string // SQS message attributes
}
```

`Attempt` は SQS の `ApproximateReceiveCount` から取得。初回は `1`。

## Producer 側の例

relay v1 には enqueue API がないため、Producer は AWS SDK で直接 SQS に送信する。

```go
payload, _ := json.Marshal(SendEmail{UserID: 123, Kind: "welcome"})

env, _ := json.Marshal(relay.Envelope{
    Type:    "send_email",
    Payload: payload,
    Meta: relay.EnvelopeMeta{
        CorrelationID: requestID,
        EnqueuedAt:    time.Now(),
    },
})

sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
    QueueUrl:    &queueURL,
    MessageBody: aws.String(string(env)),
})
```

> **Tip:** `Envelope` と `EnvelopeMeta` は公開型なので、Producer 側でも import して使える。

## decode 失敗時の挙動

| 失敗パターン | 動作 |
|---|---|
| Body が JSON でない | discard（削除） + エラーログ |
| `type` フィールドがない / 空 | discard（削除） + エラーログ |
| `payload` が `T` にデコードできない | handler 内で error → デフォルトは retry |
| 登録されていない `type` | discard（削除） + エラーログ |

envelope レベルの失敗は discard（修復不能）。
payload レベルの失敗は handler 内の error として扱われ、デフォルト retry。

## FIFO Queue

Standard / FIFO どちらの queue も処理可能。
ただし runtime は ordering を保証しない。FIFO の ordering は SQS 側の semantics に委ねる。

FIFO queue のメッセージは `Metadata.MessageGroupID` と `Metadata.MessageDedupID` に値が入る。
