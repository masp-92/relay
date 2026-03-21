package relay

import (
	"encoding/json"
	"time"
)

// Message is the typed message delivered to a handler.
type Message[T any] struct {
	// ID is the SQS MessageId.
	ID string

	// Type is the message type string used for dispatch.
	Type string

	// Payload is the decoded application payload.
	Payload T

	// Metadata contains SQS-specific and relay metadata.
	Metadata Metadata
}

// Metadata holds message metadata beyond the payload.
type Metadata struct {
	// Attempt is the approximate receive count from SQS.
	Attempt int

	// EnqueuedAt is the time the message was enqueued (from envelope meta).
	EnqueuedAt time.Time

	// CorrelationID is an optional correlation identifier for tracing.
	CorrelationID string

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

// Envelope is the wire format for SQS message bodies.
type Envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	Meta    EnvelopeMeta    `json:"meta,omitempty"`
}

// EnvelopeMeta holds optional envelope-level metadata.
type EnvelopeMeta struct {
	CorrelationID string    `json:"correlation_id,omitempty"`
	EnqueuedAt    time.Time `json:"enqueued_at,omitempty"`
}

// MessageInfo is a read-only summary passed to hooks.
// It does not include the decoded payload.
type MessageInfo struct {
	ID            string
	Type          string
	Attempt       int
	CorrelationID string
	ReceiptHandle string
}

// RawMessage is the unparsed SQS message passed to OnReceive hook.
type RawMessage struct {
	MessageID     string
	Body          string
	ReceiptHandle string
}
