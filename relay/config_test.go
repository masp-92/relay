package relay

import (
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	c := Config{QueueURL: "https://sqs.ap-northeast-1.amazonaws.com/123/my-queue"}
	c.setDefaults()

	if c.Concurrency != 1 {
		t.Errorf("Concurrency = %d, want 1", c.Concurrency)
	}
	if c.WaitTimeSeconds != 20 {
		t.Errorf("WaitTimeSeconds = %d, want 20", c.WaitTimeSeconds)
	}
	if c.MaxNumberOfMessages != 1 {
		t.Errorf("MaxNumberOfMessages = %d, want 1", c.MaxNumberOfMessages)
	}
	if c.VisibilityTimeoutSeconds != 30 {
		t.Errorf("VisibilityTimeoutSeconds = %d, want 30", c.VisibilityTimeoutSeconds)
	}
	if c.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 30s", c.ShutdownTimeout)
	}
}

func TestConfigValidateMissingQueueURL(t *testing.T) {
	c := Config{}
	c.setDefaults()
	if err := c.validate(); err == nil {
		t.Error("expected error for missing QueueURL")
	}
}

func TestConfigValidateMaxNumberOfMessages(t *testing.T) {
	c := Config{QueueURL: "https://sqs.example.com/q", MaxNumberOfMessages: 11}
	c.setDefaults()
	if err := c.validate(); err == nil {
		t.Error("expected error for MaxNumberOfMessages > 10")
	}
}

func TestConfigValidateLeaseInterval(t *testing.T) {
	c := Config{
		QueueURL:                 "https://sqs.example.com/q",
		VisibilityTimeoutSeconds: 30,
		LeaseExtensionInterval:   60 * time.Second, // >= 30s visibility
	}
	c.setDefaults()
	if err := c.validate(); err == nil {
		t.Error("expected error when LeaseExtensionInterval >= VisibilityTimeoutSeconds")
	}
}
