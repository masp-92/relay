package relay

import (
	"errors"
	"fmt"
)

// retryError wraps an error to signal that the message should be retried.
type retryError struct {
	err error
}

func (e *retryError) Error() string { return fmt.Sprintf("retry: %v", e.err) }
func (e *retryError) Unwrap() error { return e.err }

// discardError wraps an error to signal that the message should be discarded.
type discardError struct {
	err error
}

func (e *discardError) Error() string { return fmt.Sprintf("discard: %v", e.err) }
func (e *discardError) Unwrap() error { return e.err }

// Retry wraps err to indicate the message should be retried.
// The SQS message will NOT be deleted; it will be redelivered
// after the visibility timeout expires.
func Retry(err error) error {
	if err == nil {
		return nil
	}
	return &retryError{err: err}
}

// Discard wraps err to indicate the message should be permanently discarded.
// The SQS message will be deleted and the discard recorded in metrics/logs.
func Discard(err error) error {
	if err == nil {
		return nil
	}
	return &discardError{err: err}
}

// IsRetry reports whether err (or any error in its chain) is a retry error.
func IsRetry(err error) bool {
	var target *retryError
	return errors.As(err, &target)
}

// IsDiscard reports whether err (or any error in its chain) is a discard error.
func IsDiscard(err error) bool {
	var target *discardError
	return errors.As(err, &target)
}
