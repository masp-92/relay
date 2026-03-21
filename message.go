package relay

// Message is the typed message delivered to a handler.
type Message[T any] struct {
	// ID is the SQS MessageId.
	ID string

	// Payload is the decoded application payload.
	Payload T

	// Metadata contains SQS-specific metadata.
	Metadata Metadata
}

// Metadata holds message metadata beyond the payload.
type Metadata struct {
	// Attempt is the approximate receive count from SQS.
	Attempt int

	// ReceiptHandle is the SQS receipt handle. Exposed for debugging;
	// handlers typically do not need this.
	ReceiptHandle string

	// MessageGroupID is the FIFO queue message group ID, if present.
	MessageGroupID string

	// MessageDedupID is the FIFO queue deduplication ID, if present.
	MessageDedupID string

	// Attributes holds any additional SQS message attributes.
	Attributes map[string]string
}

// MessageInfo is a read-only summary passed to hooks.
// It does not include the decoded payload.
type MessageInfo struct {
	ID            string
	Attempt       int
	ReceiptHandle string
}

// RawMessage is the unparsed SQS message passed to OnReceive hook.
type RawMessage struct {
	MessageID     string
	Body          string
	ReceiptHandle string
}
