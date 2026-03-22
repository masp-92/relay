# メッセージ仕様

## Body フォーマット

relay は「1 queue = 1 job type」を前提とする。
SQS メッセージの Body には payload を **直接** 格納する（envelope ラッパー不要）。

```json
{
  "user_id": 123,
  "kind": "welcome"
}
```

## Handler が受け取る型

```go
type Message[T any] struct {
    ID       string    // SQS MessageId
    Payload  T         // デコード済み payload
    Metadata Metadata
}

type Metadata struct {
    Attempt        int               // SQS ApproximateReceiveCount
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
body, _ := json.Marshal(SendEmail{UserID: 123, Kind: "welcome"})

sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
    QueueUrl:    &queueURL,
    MessageBody: aws.String(string(body)),
})
```

## Decoder

デフォルトの decoder は `json.Unmarshal(body, &T{})` 。`WithDecoder` で差し替え可能。

```go
// protobuf + base64
rt := relay.New[*pb.SendEmail](sqsClient, cfg, handler,
    relay.WithDecoder(func(body string) (*pb.SendEmail, error) {
        b, err := base64.StdEncoding.DecodeString(body)
        if err != nil {
            return nil, err
        }
        msg := &pb.SendEmail{}
        return msg, proto.Unmarshal(b, msg)
    }),
)
```

decoder が error を返した場合は discard（メッセージ削除 + エラーログ）。

## decode 失敗時の挙動

| 失敗パターン | 動作 |
|---|---|
| decoder が error を返す | discard（削除） + エラーログ |
| handler panic | recover → リトライ扱い |

## FIFO Queue

Standard / FIFO どちらの queue も処理可能。
ただし runtime は ordering を保証しない。FIFO の ordering は SQS 側の semantics に委ねる。

FIFO queue のメッセージは `Metadata.MessageGroupID` と `Metadata.MessageDedupID` に値が入る。
