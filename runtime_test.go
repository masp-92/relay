package relay

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// --- Mock SQS Client ---

type mockSQSClient struct {
	mu       sync.Mutex
	messages []sqstypes.Message
	deleted  []string // receipt handles
	received int32

	receiveErr error
	deleteErr  error
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if m.receiveErr != nil {
		return nil, m.receiveErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.messages) == 0 {
		// Simulate long poll returning empty
		return &sqs.ReceiveMessageOutput{}, nil
	}

	max := int(params.MaxNumberOfMessages)
	if max <= 0 {
		max = 1
	}
	if max > len(m.messages) {
		max = len(m.messages)
	}

	batch := m.messages[:max]
	m.messages = m.messages[max:]
	atomic.AddInt32(&m.received, int32(len(batch)))
	return &sqs.ReceiveMessageOutput{Messages: batch}, nil
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted = append(m.deleted, *params.ReceiptHandle)
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// --- Helpers ---

func ptr(s string) *string { return &s }

func makeSQSMessage(msgID, receiptHandle string, payload any) sqstypes.Message {
	body, _ := json.Marshal(payload)
	return sqstypes.Message{
		MessageId:     ptr(msgID),
		ReceiptHandle: ptr(receiptHandle),
		Body:          ptr(string(body)),
		Attributes: map[string]string{
			"ApproximateReceiveCount": "1",
		},
	}
}

// --- Tests ---

func TestHandleSuccess(t *testing.T) {
	type TestPayload struct {
		UserID int `json:"user_id"`
	}

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeSQSMessage("msg-1", "rh-1", TestPayload{UserID: 42}),
		},
	}

	var called atomic.Bool
	rt := New[TestPayload](mock, Config{
		QueueURL:    "https://sqs.example.com/test",
		Concurrency: 1,
	}, func(ctx context.Context, msg Message[TestPayload]) error {
		if msg.Payload.UserID != 42 {
			t.Errorf("UserID = %d, want 42", msg.Payload.UserID)
		}
		if msg.ID != "msg-1" {
			t.Errorf("ID = %q, want %q", msg.ID, "msg-1")
		}
		called.Store(true)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_ = rt.Run(ctx)

	if !called.Load() {
		t.Error("handler was not called")
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.deleted) != 1 || mock.deleted[0] != "rh-1" {
		t.Errorf("deleted = %v, want [rh-1]", mock.deleted)
	}
}

func TestHandleRetry(t *testing.T) {
	type P struct{}

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeSQSMessage("msg-1", "rh-1", P{}),
		},
	}

	rt := New[P](mock, Config{
		QueueURL:    "https://sqs.example.com/test",
		Concurrency: 1,
	}, func(ctx context.Context, msg Message[P]) error {
		return Retry(errors.New("transient"))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = rt.Run(ctx)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.deleted) != 0 {
		t.Errorf("message should NOT be deleted on retry, deleted = %v", mock.deleted)
	}
}

func TestHandleDiscard(t *testing.T) {
	type P struct{}

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeSQSMessage("msg-1", "rh-1", P{}),
		},
	}

	rt := New[P](mock, Config{
		QueueURL:    "https://sqs.example.com/test",
		Concurrency: 1,
	}, func(ctx context.Context, msg Message[P]) error {
		return Discard(errors.New("bad data"))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = rt.Run(ctx)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.deleted) != 1 {
		t.Errorf("message should be deleted on discard, deleted = %v", mock.deleted)
	}
}

func TestHandlePlainErrorIsRetry(t *testing.T) {
	type P struct{}

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeSQSMessage("msg-1", "rh-1", P{}),
		},
	}

	rt := New[P](mock, Config{
		QueueURL:    "https://sqs.example.com/test",
		Concurrency: 1,
	}, func(ctx context.Context, msg Message[P]) error {
		return errors.New("something went wrong")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = rt.Run(ctx)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	// Plain error = retry = do not delete
	if len(mock.deleted) != 0 {
		t.Errorf("plain error should be retry (no delete), deleted = %v", mock.deleted)
	}
}

func TestPanicRecoveryIsRetry(t *testing.T) {
	type P struct{}

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeSQSMessage("msg-1", "rh-1", P{}),
		},
	}

	var panicHookCalled atomic.Bool
	rt := New[P](mock, Config{
		QueueURL:    "https://sqs.example.com/test",
		Concurrency: 1,
	}, func(ctx context.Context, msg Message[P]) error {
		panic("oh no")
	}, WithHooks(Hooks{
		OnPanic: func(ctx context.Context, info MessageInfo, recovered any) {
			panicHookCalled.Store(true)
		},
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = rt.Run(ctx)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	// Panic = retry = no delete
	if len(mock.deleted) != 0 {
		t.Errorf("panic should be retry (no delete), deleted = %v", mock.deleted)
	}
	if !panicHookCalled.Load() {
		t.Error("OnPanic hook was not called")
	}
}

func TestDecodeFailureIsDiscarded(t *testing.T) {
	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			{
				MessageId:     ptr("msg-bad"),
				ReceiptHandle: ptr("rh-bad"),
				Body:          ptr("this is not json"),
				Attributes:    map[string]string{},
			},
		},
	}

	rt := New[struct{}](mock, Config{
		QueueURL:    "https://sqs.example.com/test",
		Concurrency: 1,
	}, func(ctx context.Context, msg Message[struct{}]) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = rt.Run(ctx)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.deleted) != 1 {
		t.Errorf("decode failure should be discarded (deleted), deleted = %v", mock.deleted)
	}
}

func TestMissingQueueURLReturnsError(t *testing.T) {
	rt := New[struct{}](&mockSQSClient{}, Config{}, func(ctx context.Context, msg Message[struct{}]) error { return nil })
	err := rt.Run(context.Background())
	if err == nil {
		t.Error("expected error for missing QueueURL")
	}
}

func TestConcurrentHandlers(t *testing.T) {
	type P struct {
		N int `json:"n"`
	}

	var msgs []sqstypes.Message
	for i := 0; i < 5; i++ {
		msgs = append(msgs, makeSQSMessage(
			"msg-"+string(rune('0'+i)),
			"rh-"+string(rune('0'+i)),
			P{N: i},
		))
	}

	mock := &mockSQSClient{messages: msgs}

	var count atomic.Int32
	var maxConcurrent atomic.Int32
	var current atomic.Int32

	rt := New[P](mock, Config{
		QueueURL:            "https://sqs.example.com/test",
		Concurrency:         3,
		MaxNumberOfMessages: 5,
	}, func(ctx context.Context, msg Message[P]) error {
		c := current.Add(1)
		count.Add(1)
		for {
			old := maxConcurrent.Load()
			if c <= old || maxConcurrent.CompareAndSwap(old, c) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		current.Add(-1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = rt.Run(ctx)

	if count.Load() != 5 {
		t.Errorf("processed %d messages, want 5", count.Load())
	}
	if maxConcurrent.Load() > 3 {
		t.Errorf("max concurrent = %d, exceeded concurrency limit of 3", maxConcurrent.Load())
	}
}

func TestHooksCalledInOrder(t *testing.T) {
	type P struct{}

	mock := &mockSQSClient{
		messages: []sqstypes.Message{
			makeSQSMessage("msg-1", "rh-1", P{}),
		},
	}

	var order []string
	var mu sync.Mutex
	appendOrder := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
	}

	rt := New[P](mock, Config{
		QueueURL:    "https://sqs.example.com/test",
		Concurrency: 1,
	}, func(ctx context.Context, msg Message[P]) error {
		appendOrder("handler")
		return nil
	}, WithHooks(Hooks{
		OnReceive: func(ctx context.Context, msg RawMessage) {
			appendOrder("receive")
		},
		OnHandlerStart: func(ctx context.Context, info MessageInfo) {
			appendOrder("start")
		},
		OnHandlerFinish: func(ctx context.Context, info MessageInfo, err error) {
			appendOrder("finish")
		},
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = rt.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	expected := []string{"receive", "start", "handler", "finish"}
	if len(order) != len(expected) {
		t.Fatalf("order = %v, want %v", order, expected)
	}
	for i := range expected {
		if order[i] != expected[i] {
			t.Errorf("order[%d] = %q, want %q", i, order[i], expected[i])
		}
	}
}
