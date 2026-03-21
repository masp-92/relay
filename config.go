package relay

import (
	"errors"
	"fmt"
	"time"
)

// Config holds the runtime configuration.
type Config struct {
	// QueueURL is the SQS queue URL (required).
	QueueURL string

	// Concurrency is the number of concurrent handler goroutines. Default: 1.
	Concurrency int

	// WaitTimeSeconds for SQS long polling. Default: 20. Max: 20.
	WaitTimeSeconds int32

	// MaxNumberOfMessages per ReceiveMessage call. Default: 1. Max: 10.
	MaxNumberOfMessages int32

	// VisibilityTimeoutSeconds for received messages. Default: 30.
	VisibilityTimeoutSeconds int32

	// ShutdownTimeout is the max duration to wait for in-flight handlers
	// to complete during graceful shutdown. Default: 30s.
	ShutdownTimeout time.Duration

	// LeaseExtensionInterval is how often to extend visibility timeout
	// for in-flight messages. Default: 10s.
	// Set to 0 to disable lease extension.
	LeaseExtensionInterval time.Duration

	// MaxProcessingTime is the absolute upper bound for handler execution.
	// After this duration, lease extension stops but the handler is NOT killed.
	// Default: 0 (no limit — extends indefinitely).
	MaxProcessingTime time.Duration
}

func (c *Config) setDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.WaitTimeSeconds <= 0 {
		c.WaitTimeSeconds = 20
	}
	if c.MaxNumberOfMessages <= 0 {
		c.MaxNumberOfMessages = 1
	}
	if c.VisibilityTimeoutSeconds <= 0 {
		c.VisibilityTimeoutSeconds = 30
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.LeaseExtensionInterval <= 0 && c.MaxProcessingTime > 0 {
		c.LeaseExtensionInterval = 10 * time.Second
	}
	if c.LeaseExtensionInterval < 0 {
		c.LeaseExtensionInterval = 0
	}
}

func (c *Config) validate() error {
	if c.QueueURL == "" {
		return errors.New("relay: QueueURL is required")
	}
	if c.Concurrency < 1 {
		return errors.New("relay: Concurrency must be >= 1")
	}
	if c.MaxNumberOfMessages < 1 || c.MaxNumberOfMessages > 10 {
		return fmt.Errorf("relay: MaxNumberOfMessages must be 1..10, got %d", c.MaxNumberOfMessages)
	}
	if c.WaitTimeSeconds < 0 || c.WaitTimeSeconds > 20 {
		return fmt.Errorf("relay: WaitTimeSeconds must be 0..20, got %d", c.WaitTimeSeconds)
	}
	if c.LeaseExtensionInterval > 0 && c.VisibilityTimeoutSeconds > 0 {
		if c.LeaseExtensionInterval >= time.Duration(c.VisibilityTimeoutSeconds)*time.Second {
			return fmt.Errorf("relay: LeaseExtensionInterval (%v) should be less than VisibilityTimeoutSeconds (%ds)",
				c.LeaseExtensionInterval, c.VisibilityTimeoutSeconds)
		}
	}
	return nil
}
