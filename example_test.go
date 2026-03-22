package relay_test

import (
	"context"
	"errors"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/masp-92/relay"
)

type SendEmail struct {
	UserID int64  `json:"user_id"`
	Kind   string `json:"kind"`
}

var (
	errInvalidUser = errors.New("invalid user")
)

func Example() {
	// In real code: cfg, _ := config.LoadDefaultConfig(ctx)
	sqsClient := sqs.New(sqs.Options{})
	queueURL := "https://sqs.ap-northeast-1.amazonaws.com/123456789012/my-queue"

	rt := relay.New[SendEmail](sqsClient, relay.Config{
		QueueURL:                 queueURL,
		Concurrency:              8,
		MaxNumberOfMessages:      8,
		VisibilityTimeoutSeconds: 60,
	}, func(ctx context.Context, msg relay.Message[SendEmail]) error {
		err := processSendEmail(ctx, msg.Payload)
		if err != nil {
			if errors.Is(err, errInvalidUser) {
				return relay.Discard(err) // permanent failure → delete
			}
			return err // transient failure → retry
		}
		return nil // success → delete
	})

	ctx := context.Background()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func processSendEmail(_ context.Context, _ SendEmail) error {
	return nil
}
